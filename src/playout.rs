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

/// One peer worker owns this object and both capabilities. No radio callback calls it.
/// Storage holds only one decoded packet and one fetched decision, never another PCM ring.
pub struct Playout {
    jitter: Jitter,
    decoder: Decoder,
    codec: Codec,
    gain: f32,
    origin: Instant,
    highest: Option<u64>,
    encoded: [u8; MAX_PAYLOAD],
    pending: Option<Decision>,
    prefix: u32,
    remaining: u32,
    decoded: bool,
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
}

impl Playout {
    /// Compose the retained 160/180/500 ms jitter policy and an always-PLC-enabled codec.
    /// Gain is applied once after decode/concealment; all work is off radio callbacks.
    /// # Safety
    /// The jitter descriptor must be readable and its DSO retained through this object's drop.
    pub unsafe fn new(
        catalog: &CodecCatalog,
        codec: Codec,
        gain_db: f64,
        descriptor: *const c_void,
        origin: Instant,
    ) -> Result<Self, MediaError> {
        let gain = 10.0f64.powf(gain_db / 20.0) as f32;
        if !gain_db.is_finite() || !gain.is_finite() {
            return Err(MediaError::Invalid);
        }
        let profile = match codec {
            Codec::GsmFr => ProfileId::GsmFr,
            Codec::Speex => ProfileId::Speex,
        };
        let decoder = catalog
            .decoder(profile, 0, FRAMES as u32, MAX_PAYLOAD)
            .map_err(MediaError::Provider)?;
        let jitter = unsafe { Jitter::new_at_rate(descriptor, 8000, 160, MAX_PAYLOAD) }
            .map_err(MediaError::Jitter)?;
        Ok(Self {
            jitter,
            decoder,
            codec,
            gain,
            origin,
            highest: None,
            encoded: [0; MAX_PAYLOAD],
            pending: None,
            prefix: 0,
            remaining: 0,
            decoded: false,
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
        })
    }

    /// Inspect framing before requesting first-talker ownership. This does not decode,
    /// advance history, admit a jitter packet or prime a timeline. Invalid wire input
    /// is not a provider failure; endpoint/session authentication remains separate.
    pub fn validate(&self, packet: &VoicePacket<'_>) -> Result<bool, MediaError> {
        if packet.codec != self.codec
            || packet.payload.is_empty()
            || packet.payload.len() > MAX_PAYLOAD
        {
            return Ok(false);
        }
        match self.decoder.inspect(packet.payload) {
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
        let admission = self
            .jitter
            .put(timestamp, FRAMES as u32, self.ns(arrival), packet.payload)
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
        self.decoder.reset().map_err(MediaError::Provider)?;
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
            self.decoded = false;
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
            let count = sink.audio(self.output, &self.pcm[self.position..self.count]);
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
        let decision = self.pending.as_ref().expect("fetched jitter decision");
        if self.prefix != 0 {
            let span = self.prefix.min(FRAMES as u32);
            self.prefix -= span;
            return self.prepare_gap(span, true);
        }
        if decision.kind == Kind::Packet && !self.decoded {
            self.count = self
                .decoder
                .decode_recovering(&self.encoded[..decision.length], &mut self.pcm)
                .map_err(MediaError::Provider)?;
            let skip = decision.start_offset.saturating_neg().max(0) as usize;
            if skip > FRAMES || (self.count != 0 && self.count != FRAMES) {
                return Err(MediaError::Contract);
            }
            if self.count == 0 {
                self.gap = (FRAMES - skip) as u32;
            } else {
                self.position = skip;
                for sample in &mut self.pcm {
                    *sample *= self.gain;
                }
            }
            self.decoded = true;
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
            self.decoder
                .conceal(span, &mut self.pcm)
                .map_err(MediaError::Provider)?
        } else {
            0
        };
        if self.count == 0 {
            self.gap = span;
        } else {
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
