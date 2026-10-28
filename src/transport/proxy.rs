//! Standard Proxy authentication and bounded, cancellation-safe TCP record transport.
//! The account owns its two halves; TCP head-of-line delay remains account-wide.

pub mod directory;

use super::mailbox::{Budget, Charge};
use crate::wire::{ProxyDecoder, ProxyKind, ProxyRecord, WireError};
use md5::{Digest, Md5};
use std::{
    collections::VecDeque,
    io::ErrorKind,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{
        TcpStream,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
};

/// Content-free transport/authentication failure; no credentials or payload bodies.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProxyError {
    /// Connection plus nonce and authentication write exceeded one deadline.
    Timeout,
    /// Remote socket closed; it is not a directory TCP_CLOSE or QSO BYE.
    Closed,
    /// OS failure category, retained for diagnostics.
    Io(ErrorKind),
    /// Locally invalid station identity.
    Invalid,
    /// Standard SYSTEM bad-password rejection.
    BadPassword,
    /// Standard SYSTEM access-denied rejection.
    AccessDenied,
    /// Other SYSTEM status; do not invent a proxy-busy interpretation.
    Rejected(u8),
    /// Framing failed before a claimed oversized body could be allocated.
    Wire(WireError),
}

fn io_error(error: std::io::Error) -> ProxyError {
    if error.kind() == ErrorKind::UnexpectedEof {
        ProxyError::Closed
    } else {
        ProxyError::Io(error.kind())
    }
}

/// Connected and challenge-sent transport. The protocol has no positive auth ACK;
/// a later SYSTEM rejection remains possible until normal Proxy traffic arrives.
pub struct ProxyTransport {
    /// Sole read owner, demultiplexed without waiting for any peer's queue.
    pub reader: ProxyReader<OwnedReadHalf>,
    /// Sole write owner, preserving partially submitted headers and payloads.
    pub writer: ProxyWriter<OwnedWriteHalf>,
}

/// Connect only to the supplied endpoint and send the published legacy challenge response.
/// MD5 is mandated by this wire protocol, never used for administrative secret storage.
pub async fn connect(
    address: SocketAddr,
    callsign: &str,
    password: &str,
    deadline: Duration,
) -> Result<ProxyTransport, ProxyError> {
    if callsign.is_empty()
        || callsign.len() > 15
        || !callsign
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
    {
        return Err(ProxyError::Invalid);
    }
    tokio::time::timeout(deadline, async {
        let mut stream = TcpStream::connect(address).await.map_err(io_error)?;
        stream.set_nodelay(true).map_err(io_error)?;
        let mut nonce = [0; 8];
        stream.read_exact(&mut nonce).await.map_err(io_error)?;
        let password = if password.is_empty() {
            "PUBLIC".to_owned()
        } else {
            password.to_ascii_uppercase()
        };
        let mut digest = Md5::new();
        digest.update(password.as_bytes());
        digest.update(nonce);
        let mut request = callsign.as_bytes().to_vec();
        request.push(b'\n');
        request.extend_from_slice(&digest.finalize());
        stream.write_all(&request).await.map_err(io_error)?;
        let (reader, writer) = stream.into_split();
        Ok(ProxyTransport {
            reader: ProxyReader::new(reader),
            writer: ProxyWriter::new(writer),
        })
    })
    .await
    .map_err(|_| ProxyError::Timeout)?
}

/// Incremental reader; callbacks borrow a bounded complete record only during emission.
pub struct ProxyReader<S> {
    stream: S,
    decoder: ProxyDecoder,
}

impl<S: AsyncRead + Unpin> ProxyReader<S> {
    /// Retain one stream and one maximum-record parser workspace.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            decoder: ProxyDecoder::default(),
        }
    }

    /// Read one bounded chunk. The account must use nonblocking per-peer queue admission
    /// in emit; directory or media backpressure cannot suspend this shared demultiplexer.
    pub async fn read(&mut self, emit: &mut dyn FnMut(ProxyRecord<'_>)) -> Result<(), ProxyError> {
        let mut input = [0; 8192];
        let count = self.stream.read(&mut input).await.map_err(io_error)?;
        if count == 0 {
            return Err(ProxyError::Closed);
        }
        let mut rejection = None;
        self.decoder
            .push(&input[..count], &mut |record| {
                if rejection.is_some() {
                    return;
                }
                if record.kind == ProxyKind::System {
                    rejection = Some(match record.payload[0] {
                        1 => ProxyError::BadPassword,
                        2 => ProxyError::AccessDenied,
                        code => ProxyError::Rejected(code),
                    });
                } else {
                    emit(record);
                }
            })
            .map_err(ProxyError::Wire)?;
        match rejection {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

/// Output admission failure; never interpreted as an individual QSO hangup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueError {
    /// Bounded total/media slot or payload-byte allowance exhausted.
    Full,
    /// Invalid kind-specific frame size; nothing was allocated or queued.
    Invalid,
}

/// One owner turn's result; the caller yields between bounded turns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FlushOutcome {
    /// One complete frame sent.
    Sent,
    /// One complete, never-submitted expired media frame discarded.
    Skipped,
    /// No work; wait for new queued output instead of polling writability.
    Empty,
}

struct Queued {
    wire: Vec<u8>,
    offset: usize,
    media: bool,
    expires: Option<Instant>,
    _charge: Charge,
}

/// Serialized output staging: 64 slots/256 KiB payload, reserving eight slots and
/// 64 KiB for control. No second writer may interleave bytes into a partial frame.
pub struct ProxyWriter<S> {
    stream: S,
    queue: VecDeque<Queued>,
    media_count: usize,
    budget: Arc<Budget>,
}

impl<S: AsyncWrite + Unpin> ProxyWriter<S> {
    /// Prepare bounded accounting outside any real-time callback.
    pub fn new(stream: S) -> Self {
        Self {
            stream,
            queue: VecDeque::with_capacity(64),
            media_count: 0,
            budget: Arc::new(Budget::default()),
        }
    }

    /// Reserve before copying; only UDP data is media. Pass its existing age deadline,
    /// not a new time stamp. Control never expires or replaces an already submitted frame.
    pub fn try_queue(
        &mut self,
        record: ProxyRecord<'_>,
        expires: Option<Instant>,
    ) -> Result<(), QueueError> {
        let header = record.header().map_err(|_| QueueError::Invalid)?;
        let media = record.kind == ProxyKind::UdpData;
        if self.queue.len() == 64 || (media && self.media_count == 56) {
            return Err(QueueError::Full);
        }
        let charge = self
            .budget
            .reserve(record.payload.len(), media)
            .map_err(|_| QueueError::Full)?;
        let mut wire = Vec::with_capacity(header.len() + record.payload.len());
        wire.extend_from_slice(&header);
        wire.extend_from_slice(record.payload);
        self.queue.push_back(Queued {
            wire,
            offset: 0,
            media,
            expires: if media { expires } else { None },
            _charge: charge,
        });
        self.media_count += usize::from(media);
        Ok(())
    }

    /// Queued payload bytes, including the partially submitted frame until completion.
    pub fn queued_bytes(&self) -> usize {
        self.budget.retained_bytes()
    }

    /// Send at most one record. Cancellation retains its exact offset; even an expired
    /// media record must finish if one byte was sent. Other accounts run independently.
    pub async fn flush_one(&mut self, now: Instant) -> Result<FlushOutcome, ProxyError> {
        let Some(front) = self.queue.front_mut() else {
            return Ok(FlushOutcome::Empty);
        };
        if front.offset == 0 && front.expires.is_some_and(|deadline| now >= deadline) {
            let removed = self.queue.pop_front().unwrap();
            self.media_count -= usize::from(removed.media);
            return Ok(FlushOutcome::Skipped);
        }
        while front.offset < front.wire.len() {
            let count = self
                .stream
                .write(&front.wire[front.offset..])
                .await
                .map_err(io_error)?;
            if count == 0 {
                return Err(ProxyError::Io(ErrorKind::WriteZero));
            }
            front.offset += count;
        }
        let removed = self.queue.pop_front().unwrap();
        self.media_count -= usize::from(removed.media);
        Ok(FlushOutcome::Sent)
    }
}
