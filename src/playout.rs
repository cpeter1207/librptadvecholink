//! Serialized source-rate jitter release. PCM timing/resampling remains in the product ring.
use crate::{
    media::MediaError,
    wire::{Codec, VoicePacket},
};
use rptadv_codec_client::{CodecCatalog, Decoder, ProfileId, Status};
use rptadv_jitter_client::{Admission, Decision, Jitter, Kind, Observation};
use std::{
    ffi::c_void,
    time::{Duration, Instant},
};

const FRAMES: usize = 640;
const MAX_PAYLOAD: usize = 252;

/// Ordered source-rate sink. Returning zero is backpressure, not packet loss.
/// Source positions count published PCM/gaps, including adaptive delay insertions.
pub trait PcmSink {
    /// Accept exactly a prefix of normalized 8 kHz PCM, returning its length.
    fn audio(&mut self, source_start: u64, pcm: &[f32]) -> usize;
    /// Accept native codec PLC without presenting it to an in-band DTMF detector.
    /// Ordinary audio-only sinks may consume it identically to received PCM.
    fn concealed(&mut self, source_start: u64, pcm: &[f32]) -> usize {
        self.audio(source_start, pcm)
    }
    /// Accept exactly a prefix of an ordered missing interval for ring fallback PLC.
    fn gap(&mut self, source_start: u64, samples: u32) -> u32;
}

/// Admission preserves ordinary duplicate/late/full outcomes without closing a call.
#[derive(Debug, PartialEq, Eq)]
pub enum Reception {
    /// The jitter capability's actual admission decision.
    Admitted(Admission),
    /// Invalid codec/framing; no source or decoder state was advanced.
    Invalid,
    /// Retained output must drain first; retry this same datagram afterward.
    Busy,
}

/// One bounded owner turn; it never waits for a consumer or spins on an empty ring.
#[derive(Debug, PartialEq, Eq)]
pub enum Progress {
    /// No work due; wait for new media or the supplied monotonic deadline.
    Idle(Option<Instant>),
    /// Consumer accepted no samples; wait for its capacity notification.
    Blocked,
    /// Some ordered media was accepted or one empty decision was retired.
    Advanced,
    /// One final notification after `end` and the entire accepted tail drained.
    Drained,
}

/// Source samples accepted by the product sink, not generated samples retried
/// after backpressure. Counters survive ordinary burst resets.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Concealment {
    /// Native codec PLC samples; never presented to the DTMF detector.
    pub codec_samples: u64,
    /// Ordered unresolved samples handed to shared-ring fallback PLC.
    pub gap_samples: u64,
}

/// One peer worker owns this object and both capabilities. No radio callback calls it.
/// Storage holds only one decoded packet and one fetched decision, never another PCM ring.
pub struct Playout {
    jitter: Jitter,
    decoders: [Option<Decoder>; 2],
    last_codec: Codec,
    gain: f32,
    origin: Instant,
    highest: Option<u64>,
    encoded: [u8; MAX_PAYLOAD + 1],
    pending: Option<Decision>,
    prefix: u32,
    remaining: u32,
    concealed: bool,
    pcm: [f32; FRAMES],
    position: usize,
    count: usize,
    gap: u32,
    output: u64,
    tail_end: Option<u64>,
    released_to: u64,
    skipped: u64,
    ending: bool,
    announced: bool,
    concealment: Concealment,
}

impl Playout {
    /// Copy published concealment provenance on the sole playout owner.
    pub fn concealment(&self) -> Concealment {
        self.concealment
    }
    /// Compose the retained 160/180/500 ms jitter policy and an always-PLC-enabled codec.
    /// Gain is applied once after decode/concealment; all work is off radio callbacks.
    /// `codec` is only the initial PLC hint, not a receive-format restriction. Each
    /// queued packet retains its own format on the single shared jitter timeline.
    /// # Safety
    /// The jitter descriptor must be readable and its DSO retained through this object's drop.
    pub unsafe fn new(
        catalog: &CodecCatalog,
        codec: Codec,
        gain_db: f64,
        descriptor: *const c_void,
        origin: Instant,
    ) -> Result<Self, MediaError> {
        let gain = crate::media::gain(gain_db)?;
        let gsm = catalog
            .decoder(ProfileId::GsmFr, 0, FRAMES as u32, MAX_PAYLOAD)
            .map_err(MediaError::Provider)?;
        let speex = catalog
            .profile(ProfileId::Speex)
            .map(|_| catalog.decoder(ProfileId::Speex, 0, FRAMES as u32, MAX_PAYLOAD))
            .transpose()
            .map_err(MediaError::Provider)?;
        if codec == Codec::Speex && speex.is_none() {
            return Err(MediaError::Invalid);
        }
        let jitter = unsafe { Jitter::new_at_rate(descriptor, 8000, 160, MAX_PAYLOAD + 1) }
            .map_err(MediaError::Jitter)?;
        Ok(Self {
            jitter,
            decoders: [Some(gsm), speex],
            last_codec: codec,
            gain,
            origin,
            highest: None,
            encoded: [0; MAX_PAYLOAD + 1],
            pending: None,
            prefix: 0,
            remaining: 0,
            concealed: false,
            pcm: [0.0; FRAMES],
            position: 0,
            count: 0,
            gap: 0,
            output: 0,
            tail_end: None,
            released_to: 0,
            skipped: 0,
            ending: false,
            announced: false,
            concealment: Concealment::default(),
        })
    }

    /// Apply to future decode/PLC only. Partially published PCM retains its
    /// original gain, source positions and jitter/decoder history.
    pub fn set_gain(&mut self, gain_db: f64) -> Result<(), MediaError> {
        self.gain = crate::media::gain(gain_db)?;
        Ok(())
    }

    /// Inspect framing before requesting first-talker ownership. This does not decode,
    /// advance history, admit a jitter packet or prime a timeline. Invalid wire input
    /// is not a provider failure; endpoint/session authentication remains separate.
    pub fn validate(&self, packet: &VoicePacket<'_>) -> Result<bool, MediaError> {
        if packet.payload.is_empty() || packet.payload.len() > MAX_PAYLOAD {
            return Ok(false);
        }
        let Some(decoder) = self.decoders[codec_index(packet.codec)].as_ref() else {
            return Ok(false);
        };
        match decoder.inspect(packet.payload) {
            Ok(info) => Ok(info.frames == FRAMES as u32),
            Err(error) if matches!(error.status, Status::Malformed | Status::Unsupported) => {
                Ok(false)
            }
            Err(error) => Err(MediaError::Provider(error)),
        }
    }

    /// Inspect before timeline changes; zero RTP timestamp/SSRC is valid, not authentication.
    /// The authenticated router and selected-talker gate must run before this operation.
    pub fn receive(
        &mut self,
        packet: VoicePacket<'_>,
        arrival: Instant,
    ) -> Result<Reception, MediaError> {
        if self.pending.is_some() || self.ending {
            return Ok(Reception::Busy);
        }
        if !self.validate(&packet)? {
            return Ok(Reception::Invalid);
        }
        // Start one epoch above zero so an immediately reordered predecessor remains representable.
        // A non-rewinding high-water mark unwraps 65535 -> 0 independently of unreliable RTP time.
        let sequence = match self.highest {
            None => 65_536 + u64::from(packet.sequence),
            Some(highest) => {
                let distance = i64::from(packet.sequence.wrapping_sub(highest as u16) as i16);
                highest
                    .checked_add_signed(distance)
                    .ok_or(MediaError::Contract)?
            }
        };
        let timestamp = sequence
            .checked_mul(FRAMES as u64)
            .ok_or(MediaError::Contract)?;
        // The neutral jitter capability retains opaque bytes. One private byte
        // preserves codec identity through reordering without another jitter queue.
        let mut tagged = [0; MAX_PAYLOAD + 1];
        tagged[0] = codec_index(packet.codec) as u8;
        tagged[1..1 + packet.payload.len()].copy_from_slice(packet.payload);
        let admission = self
            .jitter
            .put(
                timestamp,
                FRAMES as u32,
                self.ns(arrival),
                &tagged[..1 + packet.payload.len()],
            )
            .map_err(MediaError::Jitter)?;
        if admission == Admission::Accepted {
            self.highest = Some(self.highest.unwrap_or(sequence).max(sequence));
            self.tail_end = Some(self.tail_end.unwrap_or(0).max(timestamp + FRAMES as u64));
        }
        Ok(Reception::Admitted(admission))
    }

    /// Signal end of the selected talker's burst; never discard its buffered final packet.
    pub fn end(&mut self) {
        self.ending = true;
    }

    /// Accepted source tail and all partially published output have drained.
    /// The caller must separately wait for the product ring before RF unkey.
    pub fn drained(&self) -> bool {
        self.pending.is_none() && self.tail_end.is_none_or(|end| self.released_to >= end)
    }

    /// Start another independent talker/burst only after old accepted media drains.
    /// Published output position remains monotonic across resets.
    pub fn reset(&mut self) -> Result<(), MediaError> {
        if !self.drained() {
            return Err(MediaError::Contract);
        }
        self.jitter.reset().map_err(MediaError::Jitter)?;
        for decoder in self.decoders.iter_mut().flatten() {
            decoder.reset().map_err(MediaError::Provider)?;
        }
        self.highest = None;
        self.tail_end = None;
        self.released_to = 0;
        self.skipped = 0;
        self.ending = false;
        self.announced = false;
        Ok(())
    }

    /// Actual jitter diagnostics; ring reserve never feeds a second packet-delay controller.
    pub fn observe(&self) -> Result<Observation, MediaError> {
        self.jitter.observe().map_err(MediaError::Jitter)
    }

    /// Publish one prefix, retaining unaccepted PCM/gaps without repeating decode, PLC or advance.
    pub fn poll(&mut self, now: Instant, sink: &mut dyn PcmSink) -> Result<Progress, MediaError> {
        if self.pending.is_none() {
            if self.drained() {
                return Ok(if self.ending && !self.announced {
                    self.announced = true;
                    Progress::Drained
                } else {
                    Progress::Idle(None)
                });
            }
            let decision = self
                .jitter
                .get(self.ns(now), 160, &mut self.encoded)
                .map_err(MediaError::Jitter)?;
            if decision.kind == Kind::NotDue {
                // An overload reset has discarded the accepted packet timeline.
                // It cannot drain later; do not hold keying or fabricate endless loss.
                if decision.deadline_ns == 0 {
                    self.tail_end = None;
                }
                return Ok(Progress::Idle(
                    (decision.deadline_ns != 0)
                        .then(|| {
                            self.origin
                                .checked_add(Duration::from_nanos(decision.deadline_ns))
                        })
                        .flatten(),
                ));
            }
            if decision.kind == Kind::Packet && decision.span != FRAMES as u32 {
                return Err(MediaError::Contract);
            }
            self.prefix = if decision.kind == Kind::Packet {
                decision.start_offset.max(0) as u32
            } else {
                0
            };
            self.remaining = decision.span;
            self.pending = Some(decision);
        }
        if self.position == self.count && self.gap == 0 {
            self.prepare()?;
        }
        let accepted = if self.gap != 0 {
            let count = sink.gap(self.output, self.gap);
            if count > self.gap {
                return Err(MediaError::Contract);
            }
            self.gap -= count;
            count as usize
        } else {
            let available = self.count - self.position;
            let pcm = &self.pcm[self.position..self.count];
            let count = if self.concealed {
                sink.concealed(self.output, pcm)
            } else {
                sink.audio(self.output, pcm)
            };
            if count > available {
                return Err(MediaError::Contract);
            }
            self.position += count;
            count
        };
        self.output = self
            .output
            .checked_add(accepted as u64)
            .ok_or(MediaError::Contract)?;
        if self.concealed {
            self.concealment.codec_samples = self
                .concealment
                .codec_samples
                .saturating_add(accepted as u64);
        } else if self.count == 0 {
            self.concealment.gap_samples =
                self.concealment.gap_samples.saturating_add(accepted as u64);
        }
        if self.position == self.count && self.gap == 0 && self.prefix == 0 && self.remaining == 0 {
            self.commit(now)?;
            return Ok(Progress::Advanced);
        }
        Ok(if accepted == 0 {
            Progress::Blocked
        } else {
            Progress::Advanced
        })
    }

    fn prepare(&mut self) -> Result<(), MediaError> {
        self.count = 0;
        self.position = 0;
        self.concealed = false;
        let decision = self.pending.as_ref().expect("fetched jitter decision");
        if self.prefix != 0 {
            let span = self.prefix.min(FRAMES as u32);
            self.prefix -= span;
            return self.prepare_gap(span, true);
        }
        // A packet's sole decoded block retires this decision when consumed;
        // only gap decisions may need multiple prepare calls after the prefix.
        if decision.kind == Kind::Packet {
            self.last_codec = match self.encoded[0] {
                0 => Codec::GsmFr,
                1 => Codec::Speex,
                _ => return Err(MediaError::Contract),
            };
            let payload = self
                .encoded
                .get(1..decision.length)
                .ok_or(MediaError::Contract)?;
            let decoder = self.decoders[codec_index(self.last_codec)]
                .as_mut()
                .ok_or(MediaError::Contract)?;
            self.count = match decoder.decode(payload, &mut self.pcm) {
                Err(error) if error.status == Status::Malformed => {
                    // Preflight already established this packet's 640-frame span.
                    // Retain native PLC provenance as well as the recovered samples.
                    self.concealed = true;
                    decoder.conceal(FRAMES as u32, &mut self.pcm)
                }
                result => result,
            }
            .map_err(MediaError::Provider)?;
            let skip = decision.start_offset.saturating_neg().max(0) as usize;
            // Jitter validated span + start_offset > 0, and a packet's span
            // above is exactly FRAMES, so its overlap cannot consume the whole packet.
            if self.count != 0 && self.count != FRAMES {
                return Err(MediaError::Contract);
            }
            if self.count == 0 {
                // Unsupported native PLC produced no PCM; the ring owns this
                // missing span, including its concealment provenance counters.
                self.concealed = false;
                self.gap = (FRAMES - skip) as u32;
            } else {
                self.position = skip;
                for sample in &mut self.pcm {
                    *sample *= self.gain;
                }
            }
            self.remaining = 0;
        } else {
            let span = self.remaining.min(FRAMES as u32);
            self.remaining -= span;
            self.prepare_gap(span, decision.kind == Kind::Missing)?;
        }
        Ok(())
    }

    fn prepare_gap(&mut self, span: u32, missing: bool) -> Result<(), MediaError> {
        // Insertions add playout time without advancing source decoder history.
        // Only genuine missing/damaged source media requests native codec PLC first.
        self.count = if missing {
            self.decoders[codec_index(self.last_codec)]
                .as_mut()
                .ok_or(MediaError::Contract)?
                .conceal(span, &mut self.pcm)
                .map_err(MediaError::Provider)?
        } else {
            0
        };
        if self.count == 0 {
            self.gap = span;
        } else {
            self.concealed = true;
            for sample in &mut self.pcm[..self.count] {
                *sample *= self.gain;
            }
        }
        Ok(())
    }

    fn commit(&mut self, now: Instant) -> Result<(), MediaError> {
        let decision = self.pending.take().expect("delivered jitter decision");
        self.jitter
            .advance(self.ns(now), 0)
            .map_err(MediaError::Jitter)?;
        let observation = self.observe()?;
        self.released_to = decision.timestamp
            + if decision.kind == Kind::Insertion {
                0
            } else {
                u64::from(decision.span)
            }
            + observation.skipped_span.saturating_sub(self.skipped);
        self.skipped = observation.skipped_span;
        Ok(())
    }
    fn ns(&self, time: Instant) -> u64 {
        time.saturating_duration_since(self.origin)
            .as_nanos()
            .min(u128::from(u64::MAX)) as u64
    }
}

fn codec_index(codec: Codec) -> usize {
    match codec {
        Codec::GsmFr => 0,
        Codec::Speex => 1,
    }
}
