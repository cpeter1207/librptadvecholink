//! Standard Proxy authentication and bounded, cancellation-safe TCP record transport.
//! The account owns its two halves; TCP head-of-line delay remains account-wide.

pub mod directory;
pub mod session;

use super::mailbox::{Budget, Charge};
use crate::wire::{ProxyDecoder, ProxyKind, ProxyRecord, WireError};
use md5::{Digest, Md5};
use std::{
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
    sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc},
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
    pub async fn read<F: FnMut(ProxyRecord<'_>) + ?Sized>(
        &mut self,
        emit: &mut F,
    ) -> Result<(), ProxyError> {
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
    /// Account output has closed; no packet was copied or admitted.
    Closed,
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
    expires: Option<Instant>,
    _charge: Charge,
    _slot: OwnedSemaphorePermit,
    _media_slot: Option<OwnedSemaphorePermit>,
    // Rust drops fields in declaration order. Notify only after every retained
    // byte/slot charge above is released, not while a waiter still sees it full.
    _retired: CreditProgress,
}

struct CreditProgress(Arc<Notify>);
impl Drop for CreditProgress {
    fn drop(&mut self) {
        self.0.notify_waiters();
    }
}

/// Nonblocking multi-peer handoff to the sole Proxy writer. All clones and the
/// partially submitted frame share 64 slots/256 KiB, with eight slots/64 KiB for control.
#[derive(Clone)]
pub struct ProxyOutput {
    sender: mpsc::Sender<Queued>,
    slots: Arc<Semaphore>,
    media_slots: Arc<Semaphore>,
    budget: Arc<Budget>,
    progress: Arc<Notify>,
}

impl ProxyOutput {
    /// Reserve before copying; only UDP data is media. Pass its existing age deadline,
    /// not a new time stamp. Control never expires or replaces an already submitted frame.
    pub fn try_queue(
        &self,
        record: ProxyRecord<'_>,
        expires: Option<Instant>,
    ) -> Result<(), QueueError> {
        let header = record.header().map_err(|_| QueueError::Invalid)?;
        let media = record.kind == ProxyKind::UdpData;
        // Declare before credits so failed admission releases them before wake.
        // On success this moves into the queued record's last-drop field.
        let retired;
        let permit = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => QueueError::Full,
            mpsc::error::TrySendError::Closed(_) => QueueError::Closed,
        })?;
        retired = CreditProgress(Arc::clone(&self.progress));
        let slot = Arc::clone(&self.slots)
            .try_acquire_owned()
            .map_err(|_| QueueError::Full)?;
        let media_slot = if media {
            Some(
                Arc::clone(&self.media_slots)
                    .try_acquire_owned()
                    .map_err(|_| QueueError::Full)?,
            )
        } else {
            None
        };
        let charge = self
            .budget
            .reserve(record.payload.len(), media)
            .map_err(|_| QueueError::Full)?;
        let mut wire = Vec::with_capacity(header.len() + record.payload.len());
        wire.extend_from_slice(&header);
        wire.extend_from_slice(record.payload);
        permit.send(Queued {
            wire,
            offset: 0,
            expires: if media { expires } else { None },
            _charge: charge,
            _slot: slot,
            _media_slot: media_slot,
            _retired: retired,
        });
        Ok(())
    }

    /// Wait only after a Full send for this same kind/length, without copying
    /// PCM, reserving budget, polling a timer, or blocking another peer. Ready
    /// is advisory: concurrent senders may win its credit before try_queue.
    pub async fn writable(&self, kind: ProxyKind, payload_bytes: usize) -> Result<(), ErrorKind> {
        if !kind.valid_length(payload_bytes) {
            return Err(ErrorKind::InvalidInput);
        }
        loop {
            let progress = self.progress.notified();
            tokio::pin!(progress);
            // Register before checking all four conditions; retirement between
            // this snapshot and await must not be lost.
            progress.as_mut().enable();
            if self.sender.is_closed() {
                return Err(ErrorKind::BrokenPipe);
            }
            let media = kind == ProxyKind::UdpData;
            if self.sender.capacity() != 0
                && self.slots.available_permits() != 0
                && (!media || self.media_slots.available_permits() != 0)
                && self.budget.available_for(payload_bytes, media)
            {
                return Ok(());
            }
            tokio::select! {
                _ = progress => {},
                _ = self.sender.closed() => return Err(ErrorKind::BrokenPipe),
            }
        }
    }
}

/// Sole TCP writer; cancellation retains exact offsets and cannot interleave frames.
pub struct ProxyWriter<S> {
    stream: S,
    output: ProxyOutput,
    receiver: mpsc::Receiver<Queued>,
    current: Option<Queued>,
}

impl<S: AsyncWrite + Unpin> ProxyWriter<S> {
    /// Prepare bounded accounting outside any real-time callback.
    pub fn new(stream: S) -> Self {
        let (sender, receiver) = mpsc::channel(64);
        Self {
            stream,
            receiver,
            current: None,
            output: ProxyOutput {
                sender,
                slots: Arc::new(Semaphore::new(64)),
                media_slots: Arc::new(Semaphore::new(56)),
                budget: Arc::new(Budget::default()),
                progress: Arc::new(Notify::new()),
            },
        }
    }

    /// Clone only queue admission, never the socket or its partial-write cursor.
    pub fn output(&self) -> ProxyOutput {
        self.output.clone()
    }

    /// Admit through the same budget used by concurrent peer workers.
    pub fn try_queue(
        &mut self,
        record: ProxyRecord<'_>,
        expires: Option<Instant>,
    ) -> Result<(), QueueError> {
        self.output.try_queue(record, expires)
    }

    /// Queued payload bytes, including the partially submitted frame until completion.
    pub fn queued_bytes(&self) -> usize {
        self.output.budget.retained_bytes()
    }

    /// Wait for actual queued work, then flush one frame. Both queue receive and
    /// partial writes are cancellation-safe when selected alongside account input.
    pub async fn flush_next(&mut self) -> Result<FlushOutcome, ProxyError> {
        if self.current.is_none() {
            self.current = self.receiver.recv().await;
        }
        self.flush_one(Instant::now()).await
    }

    /// Send at most one record. Cancellation retains its exact offset; even an expired
    /// media record must finish if one byte was sent. Other accounts run independently.
    pub async fn flush_one(&mut self, now: Instant) -> Result<FlushOutcome, ProxyError> {
        if self.current.is_none() {
            self.current = self.receiver.try_recv().ok();
        }
        let Some(front) = self.current.as_mut() else {
            return Ok(FlushOutcome::Empty);
        };
        if front.offset == 0 && front.expires.is_some_and(|deadline| now >= deadline) {
            self.current = None;
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
        self.current = None;
        Ok(FlushOutcome::Sent)
    }
}
