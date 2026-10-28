//! RTP uses the released parser/builder; only EchoLink's profile rules are local.
use super::{MAX_DATAGRAM, WireError, copy_output};
use rtp::{header::Header, packet::Packet};
use webrtc_util::marshal::{Marshal, Unmarshal};

/// Encoded four-frame wire profile, not a codec implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Codec {
    /// Four raw 33-byte GSM full-rate units, not WAV49.
    GsmFr,
    /// One continuous narrowband Speex bitstream, inspected by the codec owner.
    Speex,
}
/// Borrowed encoded packet; its zero or nonzero clock/SSRC is not authentication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VoicePacket<'a> {
    /// Explicit encoded profile.
    pub codec: Codec,
    /// Standard RTP marker bit; EchoLink Speex uses it.
    pub marker: bool,
    /// Wrapping RTP sequence number.
    pub sequence: u16,
    /// Received RTP timestamp, which can legitimately be zero.
    pub timestamp: u32,
    /// Received source identifier, which can legitimately be zero.
    pub ssrc: u32,
    /// Complete encoded packet; no partial units.
    pub payload: &'a [u8],
}
fn validate(codec: Codec, bytes: &[u8]) -> Result<(), WireError> {
    if bytes.len() > MAX_DATAGRAM - 12 {
        return Err(WireError::TooLarge);
    }
    match codec {
        Codec::GsmFr if bytes.len() != 132 || bytes.chunks(33).any(|unit| unit[0] >> 4 != 0xd) => {
            Err(WireError::Malformed)
        }
        Codec::Speex if bytes.is_empty() => Err(WireError::Malformed),
        _ => Ok(()),
    }
}
pub(super) fn parse(bytes: &[u8]) -> Result<VoicePacket<'_>, WireError> {
    // No CSRC, extension or padding is defined in the pinned EchoLink media envelope.
    if bytes.first() != Some(&0xc0) || bytes.len() < 12 {
        return Err(WireError::Malformed);
    }
    let mut reader = bytes;
    let packet = Packet::unmarshal(&mut reader).map_err(|_| WireError::Malformed)?;
    let codec = match (packet.header.payload_type, packet.header.marker) {
        (3, _) => Codec::GsmFr,
        (22, true) => Codec::Speex,
        _ => return Err(WireError::Unsupported),
    };
    validate(codec, &bytes[12..])?;
    Ok(VoicePacket {
        codec,
        marker: packet.header.marker,
        sequence: packet.header.sequence_number,
        timestamp: packet.header.timestamp,
        ssrc: packet.header.ssrc,
        payload: &bytes[12..],
    })
}
/// Serialize a complete EchoLink RTP packet; insufficient output consumes nothing.
pub fn write_voice(packet: &VoicePacket<'_>, out: &mut [u8]) -> Result<usize, WireError> {
    validate(packet.codec, packet.payload)?;
    let needed = 12 + packet.payload.len();
    if out.len() < needed {
        return Err(WireError::Capacity(needed));
    }
    if packet.codec == Codec::Speex && !packet.marker {
        return Err(WireError::Malformed);
    }
    let packet = Packet {
        header: Header {
            version: 3,
            marker: packet.marker,
            payload_type: match packet.codec {
                Codec::GsmFr => 3,
                Codec::Speex => 22,
            },
            sequence_number: packet.sequence,
            timestamp: packet.timestamp,
            ssrc: packet.ssrc,
            ..Default::default()
        },
        payload: bytes::Bytes::copy_from_slice(packet.payload),
    };
    let bytes = packet.marshal().map_err(|_| WireError::Malformed)?;
    copy_output(&bytes, out)
}
