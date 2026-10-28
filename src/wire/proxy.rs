//! Published EchoLink Proxy nine-byte framing; not an HTTP proxy or a QSO owner.
use super::{MAX_DATAGRAM, WireError};
use std::net::Ipv4Addr;

/// Standard Proxy record types.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum ProxyKind {
    /// Open a directory TCP connection.
    TcpOpen = 1,
    /// Directory TCP bytes.
    TcpData = 2,
    /// TCP closure, not an EchoLink BYE.
    TcpClose = 3,
    /// Four-byte little-endian TCP result.
    TcpStatus = 4,
    /// UDP audio/data bytes, without a source-port field.
    UdpData = 5,
    /// UDP control bytes, without a source-port field.
    UdpControl = 6,
    /// One-byte authentication/access status.
    System = 7,
}
/// Borrowed complete Proxy record, valid only during emit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProxyRecord<'a> {
    /// Explicit standard type.
    pub kind: ProxyKind,
    /// IPv4 address in network byte order on the wire.
    pub address: Ipv4Addr,
    /// Payload, never retained across the callback.
    pub payload: &'a [u8],
}

impl ProxyRecord<'_> {
    pub(crate) fn header(&self) -> Result<[u8; 9], WireError> {
        let length = u32::try_from(self.payload.len()).map_err(|_| WireError::TooLarge)?;
        let mut bytes = [0; 9];
        bytes[0] = self.kind as u8;
        bytes[1..5].copy_from_slice(&self.address.octets());
        bytes[5..].copy_from_slice(&length.to_le_bytes());
        header(&bytes)?;
        Ok(bytes)
    }
}
/// One bounded transport parser; a bad header clears only its buffered record.
pub struct ProxyDecoder {
    buffer: Vec<u8>,
}
impl Default for ProxyDecoder {
    fn default() -> Self {
        Self {
            buffer: Vec::with_capacity(9 + 65_536),
        }
    }
}
fn header(bytes: &[u8]) -> Result<(ProxyKind, usize), WireError> {
    let length = u32::from_le_bytes(bytes[5..9].try_into().unwrap()) as usize;
    let kind = match bytes[0] {
        1 => ProxyKind::TcpOpen,
        2 => ProxyKind::TcpData,
        3 => ProxyKind::TcpClose,
        4 => ProxyKind::TcpStatus,
        5 => ProxyKind::UdpData,
        6 => ProxyKind::UdpControl,
        7 => ProxyKind::System,
        _ => return Err(WireError::Unsupported),
    };
    let valid = match kind {
        ProxyKind::TcpOpen | ProxyKind::TcpClose => length == 0,
        ProxyKind::TcpStatus => length == 4,
        ProxyKind::System => length == 1,
        ProxyKind::TcpData => (1..=65_536).contains(&length),
        ProxyKind::UdpData | ProxyKind::UdpControl => (1..=MAX_DATAGRAM).contains(&length),
    };
    if !valid {
        return Err(WireError::Malformed);
    }
    Ok((kind, length))
}
impl ProxyDecoder {
    /// Consume split/coalesced records with a single preallocated workspace.
    /// Impossible headers reset this parser and return an error, never a QSO close.
    pub fn push(
        &mut self,
        bytes: &[u8],
        emit: &mut dyn FnMut(ProxyRecord<'_>),
    ) -> Result<usize, WireError> {
        let mut input = bytes;
        while !input.is_empty() || self.buffer.len() >= 9 {
            let needed = if self.buffer.len() < 9 {
                9
            } else {
                match header(&self.buffer) {
                    Ok((_, length)) => 9 + length,
                    Err(error) => {
                        self.buffer.clear();
                        return Err(error);
                    }
                }
            };
            let count = input.len().min(needed - self.buffer.len());
            self.buffer.extend_from_slice(&input[..count]);
            input = &input[count..];
            if self.buffer.len() < 9 {
                break;
            }
            let (kind, length) = match header(&self.buffer) {
                Ok(value) => value,
                Err(error) => {
                    self.buffer.clear();
                    return Err(error);
                }
            };
            if self.buffer.len() == 9 + length {
                emit(ProxyRecord {
                    kind,
                    address: Ipv4Addr::new(
                        self.buffer[1],
                        self.buffer[2],
                        self.buffer[3],
                        self.buffer[4],
                    ),
                    payload: &self.buffer[9..],
                });
                self.buffer.clear();
            } else if input.is_empty() {
                break;
            }
        }
        Ok(bytes.len())
    }
}
