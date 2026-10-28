//! One virtual TCP channel per standard Proxy, independent of peer datagram backpressure.

use crate::{
    transport::{
        ReplyKind, TransactionError,
        mailbox::{Budget, Charge},
        read_reply,
    },
    wire::{ProxyKind, ProxyRecord},
};
use std::{
    future::Future,
    io,
    net::Ipv4Addr,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncRead, BufReader, ReadBuf},
    sync::mpsc,
};

/// Content-free directory-channel failure, distinct from losing the Proxy transport.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TunnelError {
    /// A job is still active, closing or waiting for its completion to be collected.
    Busy,
    /// Invalid request size/deadline or exhausted transaction IDs.
    Invalid,
    /// TCP_STATUS failed with the original unsigned status preserved.
    ConnectStatus(u32),
    /// Directory staging filled; no UDP/control reader was suspended.
    Backpressure,
    /// Unexpected or malformed virtual TCP record.
    Protocol,
    /// Existing shared framing/timeout result.
    Directory(TransactionError),
}

/// Poll this alongside the account's reader/writer, not on a separate thread or per packet.
pub type ReplyFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>, TransactionError>> + Send>>;

struct Chunk {
    bytes: Vec<u8>,
    offset: usize,
    _charge: Charge,
}
struct Input {
    receiver: mpsc::Receiver<Chunk>,
    current: Option<Chunk>,
}

impl AsyncRead for Input {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        output: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if output.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        if self.current.is_none() {
            self.current = match self.receiver.poll_recv(context) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(None) => return Poll::Ready(Ok(())),
                Poll::Ready(Some(chunk)) => Some(chunk),
            };
        }
        let chunk = self.current.as_mut().unwrap();
        let count = output.remaining().min(chunk.bytes.len() - chunk.offset);
        output.put_slice(&chunk.bytes[chunk.offset..chunk.offset + count]);
        chunk.offset += count;
        if chunk.offset == chunk.bytes.len() {
            self.current = None;
        }
        Poll::Ready(Ok(()))
    }
}

#[derive(Clone, Copy)]
enum Phase {
    Open,
    WaitingOpen,
    Request,
    Reading,
    Close,
    WaitingClose,
}

struct Transaction {
    id: u64,
    address: Ipv4Addr,
    request: Vec<u8>,
    phase: Phase,
    sender: Option<mpsc::Sender<Chunk>>,
    budget: Arc<Budget>,
    first_error: Option<TunnelError>,
    remote_closed: bool,
    result: Option<Result<Vec<u8>, TunnelError>>,
}

impl Transaction {
    fn fail(&mut self, error: TunnelError) {
        self.first_error.get_or_insert(error);
        self.sender = None;
        self.close();
    }

    fn close(&mut self) {
        if !matches!(self.phase, Phase::Close | Phase::WaitingClose) {
            self.phase = Phase::Close;
        }
    }
}

/// Sole virtual-directory owner, called only by the serialized account task.
/// Completion cannot release the channel for reuse until TCP_CLOSE quiesces the old stream.
#[derive(Default)]
pub struct DirectoryChannel {
    next: u64,
    current: Option<Transaction>,
    completed: Option<(u64, Result<Vec<u8>, TunnelError>)>,
}

impl DirectoryChannel {
    /// Prepare a bounded operation. The returned future shares direct mode's parser,
    /// while the account continues routing UDP and pumping output independently.
    pub fn begin(
        &mut self,
        address: Ipv4Addr,
        request: &[u8],
        kind: ReplyKind,
        timeout: Duration,
    ) -> Result<(u64, ReplyFuture), TunnelError> {
        if self.current.is_some() || self.completed.is_some() {
            return Err(TunnelError::Busy);
        }
        if request.is_empty() || request.len() > 65_536 || timeout.is_zero() {
            return Err(TunnelError::Invalid);
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or(TunnelError::Invalid)?;
        self.next = self.next.checked_add(1).ok_or(TunnelError::Invalid)?;
        let (sender, receiver) = mpsc::channel(8);
        self.current = Some(Transaction {
            id: self.next,
            address,
            request: request.to_vec(),
            phase: Phase::Open,
            sender: Some(sender),
            budget: Arc::new(Budget::default()),
            first_error: None,
            remote_closed: false,
            result: None,
        });
        let reply = Box::pin(async move {
            tokio::time::timeout_at(
                deadline.into(),
                read_reply(
                    BufReader::new(Input {
                        receiver,
                        current: None,
                    }),
                    kind,
                ),
            )
            .await
            .map_err(|_| TransactionError::Timeout)?
        });
        Ok((self.next, reply))
    }

    /// Retry this exact control record until the bounded Proxy writer admits it.
    pub fn outgoing(&self) -> Option<ProxyRecord<'_>> {
        let transaction = self.current.as_ref()?;
        let (kind, payload) = match transaction.phase {
            Phase::Open => (ProxyKind::TcpOpen, &[][..]),
            Phase::Request => (ProxyKind::TcpData, transaction.request.as_slice()),
            Phase::Close => (ProxyKind::TcpClose, &[][..]),
            _ => return None,
        };
        Some(ProxyRecord {
            kind,
            address: transaction.address,
            payload,
        })
    }

    /// Advance only after outgoing() was accepted in full by the writer's staging queue.
    pub fn output_queued(&mut self) {
        if let Some(transaction) = self.current.as_mut() {
            transaction.phase = match transaction.phase {
                Phase::Open => Phase::WaitingOpen,
                Phase::Request => Phase::Reading,
                Phase::Close => Phase::WaitingClose,
                phase => phase,
            };
        }
    }

    /// Consume only virtual TCP records. False leaves UDP/control with the account router.
    /// A full directory mailbox fails that transaction immediately, without awaiting space.
    pub fn receive(&mut self, record: ProxyRecord<'_>) -> bool {
        if !matches!(
            record.kind,
            ProxyKind::TcpOpen | ProxyKind::TcpData | ProxyKind::TcpStatus | ProxyKind::TcpClose
        ) {
            return false;
        }
        let Some(transaction) = self.current.as_mut() else {
            return true;
        };
        if record.header().is_err() {
            transaction.fail(TunnelError::Protocol);
            return true;
        }
        match record.kind {
            ProxyKind::TcpStatus if matches!(transaction.phase, Phase::WaitingOpen) => {
                let status = u32::from_le_bytes(record.payload.try_into().unwrap());
                if status == 0 {
                    transaction.phase = Phase::Request;
                } else {
                    transaction.fail(TunnelError::ConnectStatus(status));
                    transaction.remote_closed = true;
                }
            }
            ProxyKind::TcpData if matches!(transaction.phase, Phase::Reading) => {
                let accepted = (|| {
                    let permit = transaction
                        .sender
                        .as_ref()
                        .ok_or(())?
                        .try_reserve()
                        .map_err(|_| ())?;
                    let charge = transaction
                        .budget
                        .reserve(record.payload.len(), false)
                        .map_err(|_| ())?;
                    permit.send(Chunk {
                        bytes: record.payload.to_vec(),
                        offset: 0,
                        _charge: charge,
                    });
                    Ok::<_, ()>(())
                })();
                if accepted.is_err() {
                    transaction.fail(TunnelError::Backpressure);
                }
            }
            ProxyKind::TcpClose => {
                transaction.remote_closed = true;
                transaction.sender = None;
            }
            ProxyKind::TcpOpen => transaction.fail(TunnelError::Protocol),
            _ => {}
        }
        self.settle();
        true
    }

    /// Preserve the first channel failure over a later parser EOF/error. Stale results do nothing.
    pub fn finish(&mut self, id: u64, result: Result<Vec<u8>, TransactionError>) {
        let Some(transaction) = self
            .current
            .as_mut()
            .filter(|transaction| transaction.id == id && transaction.result.is_none())
        else {
            return;
        };
        transaction.result = Some(match transaction.first_error {
            Some(error) => Err(error),
            None => result.map_err(TunnelError::Directory),
        });
        transaction.sender = None;
        transaction.close();
        self.settle();
    }

    fn settle(&mut self) {
        if self
            .current
            .as_ref()
            .is_some_and(|transaction| transaction.remote_closed && transaction.result.is_some())
        {
            let mut transaction = self.current.take().unwrap();
            self.completed = Some((transaction.id, transaction.result.take().unwrap()));
        }
    }

    /// Collect once after stream closure; a new OPEN is prohibited until this is consumed.
    pub fn take_completed(&mut self) -> Option<(u64, Result<Vec<u8>, TunnelError>)> {
        self.completed.take()
    }
}
