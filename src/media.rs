//! Source-rate codec work, never performed in radio callbacks. Product owns resampling.

use crate::wire::{Codec, VoicePacket, WireError, write_voice};
use rptadv_codec_client::{CodecCatalog, Encoder, ProfileId};

const FRAMES: usize = 640;
const MAX_ENCODED: usize = 252;

/// One connection's optional narrowband extension. GSM is always mandatory and initial.
pub struct CodecChoice {
    speex: bool,
    selected: Codec,
}
impl CodecChoice {
    /// Advertise Speex only when its validated bidirectional dynamic provider is composed.
    pub fn new(catalog: &CodecCatalog) -> Result<Self, MediaError> {
        if catalog.profile(ProfileId::GsmFr).is_none() {
            return Err(MediaError::Invalid);
        }
        Ok(Self {
            speex: catalog.profile(ProfileId::Speex).is_some(),
            selected: Codec::GsmFr,
        })
    }
    /// Include the raw SPEEX PRIV item, never wideband or another IAX codec.
    pub fn advertise_speex(&self) -> bool {
        self.speex
    }
    /// Observe only authenticated session-bound SDES, after the account/router gate.
    /// Like EchoLib, later plain heartbeats do not undo a negotiated extension.
    pub fn observe_sdes(&mut self, control: &crate::wire::ControlPacket) {
        if self.speex
            && matches!(
                control,
                crate::wire::ControlPacket::Sdes { speex: true, .. }
            )
        {
            self.selected = Codec::Speex;
        }
    }
    /// Current peer egress choice; receive packets still validate their own payload type.
    pub fn codec(&self) -> Codec {
        self.selected
    }
}

/// Bounded media/provider failure, without PCM or packet contents.
#[derive(Debug)]
pub enum MediaError {
    /// Configuration cannot provide a finite gain or required codec profile.
    Invalid,
    /// Dynamic codec operation failed; preserve its operation/status evidence.
    Provider(rptadv_codec_client::Error),
    /// A provider did not satisfy the fixed four-frame packet contract.
    Contract,
    /// Protocol envelope validation failed.
    Wire(WireError),
    /// Composed jitter capability failed; the account preserves the first cause.
    Jitter(rptadv_jitter_client::Error),
}

/// Immutable prepared bytes retained until sent or explicitly retired by their owner.
pub struct EncodedPacket {
    /// Complete EchoLink RTP envelope; retries reuse these bytes without encoding again.
    pub bytes: Vec<u8>,
    /// Real input position since the current burst reset, excluding wire padding.
    pub source_start: u64,
    /// Real source frames represented, excluding lookahead/padding.
    pub source_frames: u32,
}

/// A single coding block and prepared packet, not another timing/audio queue.
/// Every packet contains exactly four 20 ms units; finite tails pad PCM before
/// the codec, never concatenate independently terminated Speex bitstreams.
pub struct Packetizer {
    encoder: Encoder,
    upgrade: Option<(Encoder, u64)>,
    encoder_base: u64,
    codec: Codec,
    gain: f32,
    pcm: [f32; FRAMES],
    buffered: usize,
    pending: Option<EncodedPacket>,
    finishing: bool,
    lookahead: u64,
    real: u64,
    wire: u64,
    sequence: u16,
    clock: u32,
    source: u32,
}

impl Packetizer {
    /// Open the selected real dynamic provider. Gain is configured by the account,
    /// applied once before encoding; source PCM is always 8 kHz normalized F32.
    pub fn new(
        catalog: &CodecCatalog,
        codec: Codec,
        gain_db: f64,
        sequence: u16,
        source: u32,
    ) -> Result<Self, MediaError> {
        let gain = 10.0f64.powf(gain_db / 20.0) as f32;
        if !gain_db.is_finite() || !gain.is_finite() {
            return Err(MediaError::Invalid);
        }
        let id = match codec {
            Codec::GsmFr => ProfileId::GsmFr,
            Codec::Speex => ProfileId::Speex,
        };
        let profile = catalog.profile(id).ok_or(MediaError::Invalid)?;
        let encoder = catalog
            .encoder(id, 0, FRAMES as u32, MAX_ENCODED)
            .map_err(MediaError::Provider)?;
        Ok(Self {
            encoder,
            upgrade: None,
            encoder_base: 0,
            codec,
            gain,
            pcm: [0.0; FRAMES],
            buffered: 0,
            pending: None,
            finishing: false,
            lookahead: u64::from(profile.lookahead_frames),
            real: 0,
            wire: 0,
            sequence,
            clock: 0,
            source,
        })
    }

    /// Schedule the authenticated, monotonic GSM-to-Speex upgrade at the next
    /// coding boundary. Already prepared bytes, partial PCM and RTP timing survive.
    /// Encoder creation occurs only on the non-audio owner.
    pub fn upgrade_speex(&mut self, catalog: &CodecCatalog) -> Result<(), MediaError> {
        if self.codec == Codec::Speex || self.upgrade.is_some() {
            return Ok(());
        }
        let profile = catalog
            .profile(ProfileId::Speex)
            .ok_or(MediaError::Invalid)?;
        let encoder = catalog
            .encoder(ProfileId::Speex, 0, FRAMES as u32, MAX_ENCODED)
            .map_err(MediaError::Provider)?;
        self.upgrade = Some((encoder, u64::from(profile.lookahead_frames)));
        Ok(())
    }

    /// Accept only the available prefix; a retained packet prevents further acceptance.
    /// The serialized owner retries the suffix after that prepared packet is retired.
    pub fn push(&mut self, pcm: &[f32]) -> Result<usize, MediaError> {
        if self.pending.is_some() || self.finishing {
            return Ok(0);
        }
        let count = pcm.len().min(FRAMES - self.buffered);
        for (target, input) in self.pcm[self.buffered..self.buffered + count]
            .iter_mut()
            .zip(pcm)
        {
            *target = input * self.gain;
        }
        self.buffered += count;
        self.real = self
            .real
            .checked_add(count as u64)
            .ok_or(MediaError::Contract)?;
        if self.buffered == FRAMES {
            self.render()?;
        }
        Ok(count)
    }

    /// Request finite completion. Repeated calls do not append more padding.
    /// Codec lookahead is drained in whole four-unit packets before the burst ends.
    pub fn finish(&mut self) -> Result<(), MediaError> {
        self.finishing = true;
        self.prepare_tail()
    }

    /// Borrow exactly the same prepared bytes across readiness/capacity retries.
    pub fn packet(&self) -> Option<&EncodedPacket> {
        self.pending.as_ref()
    }

    /// Retire one sent or deliberately expired whole packet, then prepare any finite tail.
    pub fn retire(&mut self) -> Result<(), MediaError> {
        self.pending = None;
        self.prepare_tail()
    }

    /// True only after requested completion and all real/lookahead media has been retired.
    pub fn drained(&self) -> bool {
        self.finishing
            && self.pending.is_none()
            && self.buffered == 0
            && (self.real == 0 || self.wire >= self.real.saturating_add(self.lookahead))
    }

    /// Start a new burst; abandon old coding state but retain connection RTP sequence/clock.
    pub fn reset(&mut self) -> Result<(), MediaError> {
        self.encoder.reset().map_err(MediaError::Provider)?;
        self.buffered = 0;
        self.pending = None;
        self.finishing = false;
        self.real = 0;
        self.wire = 0;
        self.encoder_base = 0;
        Ok(())
    }

    fn prepare_tail(&mut self) -> Result<(), MediaError> {
        if self.finishing
            && self.pending.is_none()
            && self.real != 0
            && self.wire < self.real.saturating_add(self.lookahead)
        {
            self.pcm[self.buffered..].fill(0.0);
            self.render()?;
        }
        Ok(())
    }

    fn render(&mut self) -> Result<(), MediaError> {
        if let Some((encoder, lookahead)) = self.upgrade.take() {
            self.encoder = encoder;
            self.lookahead = lookahead;
            self.encoder_base = self.wire;
            self.codec = Codec::Speex;
        }
        let mut encoded = [0; MAX_ENCODED];
        // Wire padding is already explicit PCM. The generic finite-flush operation
        // rounds to codec units, not EchoLink's four-unit packet, so do not use it.
        let result = self
            .encoder
            .encode(&self.pcm, &mut encoded, false)
            .map_err(MediaError::Provider)?;
        if result.consumed_frames != FRAMES
            || result.packet_frames != FRAMES as u32
            || result.encoded_bytes == 0
        {
            return Err(MediaError::Contract);
        }
        let mut bytes = vec![0; 12 + result.encoded_bytes];
        write_voice(
            &VoicePacket {
                codec: self.codec,
                marker: self.codec == Codec::Speex,
                sequence: self.sequence,
                timestamp: self.clock,
                ssrc: self.source,
                payload: &encoded[..result.encoded_bytes],
            },
            &mut bytes,
        )
        .map_err(MediaError::Wire)?;
        let source_start = self
            .encoder_base
            .checked_add(result.source_start_frames)
            .ok_or(MediaError::Contract)?;
        let start = source_start.min(self.real);
        let end = source_start
            .saturating_add(u64::from(result.source_frames))
            .min(self.real);
        self.pending = Some(EncodedPacket {
            bytes,
            source_start: start,
            source_frames: (end - start) as u32,
        });
        self.sequence = self.sequence.wrapping_add(1);
        self.clock = self.clock.wrapping_add(FRAMES as u32);
        self.wire = self
            .wire
            .checked_add(FRAMES as u64)
            .ok_or(MediaError::Contract)?;
        self.buffered = 0;
        Ok(())
    }
}
