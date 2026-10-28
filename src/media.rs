//! Source-rate codec work, never performed in radio callbacks. Product owns resampling.

use crate::wire::{Codec, VoicePacket, WireError, write_voice};
use rptadv_codec_client::{CodecCatalog, Encoder, ProfileId};

const FRAMES: usize = 640;
const MAX_ENCODED: usize = 252;

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
        let start = result.source_start_frames.min(self.real);
        let end = result
            .source_start_frames
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
