//! Bounded EchoLink wire adaptation; network ownership and admission live elsewhere.
mod control;
mod proxy;
mod text;
pub use text::{write_chat, write_station_info};
mod voice;
pub use control::{ControlPacket, write_aprs, write_control};
pub use proxy::{ProxyDecoder, ProxyKind, ProxyRecord};
use std::fmt;
pub use voice::{Codec, VoicePacket, write_voice};

/// Largest IPv4 UDP application payload, checked before parsing/allocation.
pub const MAX_DATAGRAM: usize = 65_507;
/// Existing shared human-text bound; bodies are never silently truncated.
pub const MAX_CHAT_BYTES: usize = 4096;

/// Socket channel, supplied by the transport rather than guessed from source port.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Channel {
    /// Audio and data datagrams.
    Audio,
    /// RTCP connection/heartbeat datagrams.
    Control,
}
/// A pure parse/capacity result, never a request to hang up a QSO.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireError {
    /// Incomplete or structurally invalid packet.
    Malformed,
    /// A well-formed but unimplemented format/type.
    Unsupported,
    /// Implementation or protocol byte bound exceeded.
    TooLarge,
    /// Caller output is unchanged; retry with this many bytes.
    Capacity(usize),
}
impl fmt::Display for WireError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "EchoLink wire error: {self:?}")
    }
}
impl std::error::Error for WireError {}

/// Parsed protocol content. Information/control payloads cannot become human speech.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WirePacket<'a> {
    /// Bounded encoded media; no codec is invoked by the parser.
    Voice(VoicePacket<'a>),
    /// Claimed control identity, not authentication proof.
    Control(ControlPacket),
    /// Literal chat, separately authenticated by its owning account/QSO.
    Chat {
        /// Claimed represented sender, not transport authority.
        sender: &'a str,
        /// Literal bounded UTF-8 text.
        body: &'a str,
    },
    /// Station information, never human chat.
    StationInfo(&'a [u8]),
    /// Unimplemented data/control forms, never guessed to be chat.
    Ignored,
}

/// Parse one complete datagram. Admitted IP/account/channel, not a fixed remote
/// source port or RTP SSRC, establishes its connection identity.
pub fn parse_datagram(channel: Channel, bytes: &[u8]) -> Result<WirePacket<'_>, WireError> {
    if bytes.len() > MAX_DATAGRAM {
        return Err(WireError::TooLarge);
    }
    match channel {
        Channel::Control => control::parse(bytes).map(WirePacket::Control),
        Channel::Audio if bytes.starts_with(b"o") => text::parse(bytes),
        Channel::Audio => voice::parse(bytes).map(WirePacket::Voice),
    }
}

fn copy_output(bytes: &[u8], out: &mut [u8]) -> Result<usize, WireError> {
    if out.len() < bytes.len() {
        return Err(WireError::Capacity(bytes.len()));
    }
    out[..bytes.len()].copy_from_slice(bytes);
    Ok(bytes.len())
}
