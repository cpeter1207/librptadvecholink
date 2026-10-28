//! A standard Proxy's single reader/writer and virtual directory channel on shared Tokio.

use super::{
    FlushOutcome, ProxyError, ProxyOutput, ProxyTransport, QueueError,
    directory::{DirectoryChannel, ReplyFuture, TunnelError},
};
use crate::{
    transport::{
        ReplyKind, TransactionError,
        mailbox::{self, Consumer, Datagram},
    },
    wire::{Channel, ProxyKind},
};
use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

struct Request {
    address: Ipv4Addr,
    bytes: Vec<u8>,
    kind: ReplyKind,
    timeout: Duration,
    reply: oneshot::Sender<Result<Vec<u8>, TransactionError>>,
}

/// One bounded virtual-directory request port, independent of peer datagram backpressure.
#[derive(Clone)]
pub struct ProxyDirectory {
    sender: mpsc::Sender<Request>,
}
impl ProxyDirectory {
    /// Execute a framed request with one whole-operation timeout. Dropping the
    /// caller cannot reuse the virtual stream before its CLOSE has quiesced.
    pub async fn exchange(
        &self,
        address: Ipv4Addr,
        request: &[u8],
        kind: ReplyKind,
        timeout: Duration,
    ) -> Result<Vec<u8>, TransactionError> {
        if request.is_empty() || request.len() > 65_536 || timeout.is_zero() {
            return Err(TransactionError::Malformed);
        }
        tokio::time::timeout(timeout, async {
            let permit = self.sender.try_reserve().map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => TransactionError::Limit,
                mpsc::error::TrySendError::Closed(_) => {
                    TransactionError::Io(std::io::ErrorKind::NotConnected)
                }
            })?;
            let (reply, response) = oneshot::channel();
            permit.send(Request {
                address,
                bytes: request.to_vec(),
                kind,
                timeout,
                reply,
            });
            response
                .await
                .unwrap_or(Err(TransactionError::Io(std::io::ErrorKind::NotConnected)))
        })
        .await
        .unwrap_or(Err(TransactionError::Timeout))
    }
}

/// Owns only one transport task, not a runtime or peer drivers. Stop is asynchronous;
/// foreign callers perform their final join off the runtime after peer quiescence.
pub struct ProxySession {
    input: Consumer,
    output: ProxyOutput,
    directory: ProxyDirectory,
    stop: watch::Sender<bool>,
    failure: watch::Receiver<Option<ProxyError>>,
    task: Option<JoinHandle<Result<(), ProxyError>>>,
}
impl ProxySession {
    /// Pump an already-connected/challenge-sent transport. The caller supplies
    /// its configured directory deadline as the finite CLOSE-quiescence bound.
    pub fn start(transport: ProxyTransport, close_timeout: Duration) -> Result<Self, ProxyError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| ProxyError::Invalid)?;
        if close_timeout.is_zero() {
            return Err(ProxyError::Invalid);
        }
        let output = transport.writer.output();
        let (packets, input) = mailbox::channel(1).unwrap();
        let (requests, mut request_input) = mpsc::channel::<Request>(1);
        let (stop, mut stopping) = watch::channel(false);
        let (failed, failure) = watch::channel(None);
        let task = runtime.spawn(async move {
            let ProxyTransport { mut reader, mut writer } = transport;
            let mut channel = DirectoryChannel::default();
            let mut pending: Option<PendingTransaction> = None;
            let outcome = loop {
                if *stopping.borrow() { break Ok(()); }
                if let Some(record) = channel.outgoing() {
                    match writer.try_queue(record, None) {
                        Ok(()) => channel.output_queued(),
                        Err(QueueError::Full) => {},
                        Err(_) => break Err(ProxyError::Closed),
                    }
                }
                if let Some((id, result)) = channel.take_completed() {
                    if let Some(completed) = pending.take().filter(|work| work.id == id) {
                        let _ = completed.reply.send(result.map_err(tunnel_error));
                    }
                }
                let close_deadline = pending.as_ref().and_then(|work| work.close_deadline);
                let event = {
                    let mut emit = |record: crate::wire::ProxyRecord<'_>| {
                        if !channel.receive(record) {
                            let class = match record.kind {
                                ProxyKind::UdpData => Channel::Audio,
                                ProxyKind::UdpControl => Channel::Control,
                                _ => return,
                            };
                            // Full sheds this datagram; neither reader nor another peer waits.
                            let _ = packets.try_push(class, SocketAddr::from((record.address, 0)),
                                Instant::now(), record.payload);
                        }
                    };
                    tokio::select! {
                        _ = stopping.changed() => PumpEvent::Stop,
                        request = request_input.recv(), if pending.is_none() => PumpEvent::Request(request),
                        result = reader.read(&mut emit) => PumpEvent::Read(result),
                        result = writer.flush_next() => PumpEvent::Written(result),
                        (id, result) = async { match pending.as_mut().filter(|work| work.work.is_some()) {
                            Some(pending) => (pending.id, pending.work.as_mut().unwrap().await),
                            None => std::future::pending().await,
                        }} => PumpEvent::Directory(id, result),
                        _ = async { match close_deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                            None => std::future::pending().await,
                        }} => PumpEvent::Expired,
                    }
                };
                match event {
                    PumpEvent::Stop | PumpEvent::Request(None) => break Ok(()),
                    PumpEvent::Expired => break Err(ProxyError::Timeout),
                    PumpEvent::Request(Some(request)) => {
                        if request.reply.is_closed() { continue; }
                        match channel.begin(request.address, &request.bytes, request.kind, request.timeout) {
                            Ok((id, work)) => pending = Some(PendingTransaction { id, work: Some(work), reply: request.reply, close_deadline: None }),
                            Err(error) => { let _ = request.reply.send(Err(tunnel_error(error))); },
                        }
                    },
                    PumpEvent::Read(Err(error)) | PumpEvent::Written(Err(error)) => break Err(error),
                    PumpEvent::Read(Ok(())) | PumpEvent::Written(Ok(_)) => {},
                    PumpEvent::Directory(id, result) => {
                        let pending = pending.as_mut().unwrap();
                        pending.work = None;
                        pending.close_deadline = Instant::now().checked_add(close_timeout);
                        channel.finish(id, result);
                    },
                }
                tokio::task::yield_now().await;
            };
            failed.send_replace(outcome.err());
            outcome
        });
        Ok(Self {
            input,
            output,
            directory: ProxyDirectory { sender: requests },
            stop,
            failure,
            task: Some(task),
        })
    }

    /// Clone admission to the existing bounded TCP writer, never its socket.
    pub fn output(&self) -> ProxyOutput {
        self.output.clone()
    }
    /// Clone one account's virtual-directory request port.
    pub fn directory(&self) -> ProxyDirectory {
        self.directory.clone()
    }
    /// Receive actual framed Proxy UDP without inventing a remote source port.
    pub async fn recv(&mut self) -> Option<Datagram> {
        self.input.recv().await
    }
    /// Original terminal transport category, not a directory transaction failure.
    pub fn failure(&self) -> Option<ProxyError> {
        *self.failure.borrow()
    }
    /// Gate the transport and wait for both socket halves to be released.
    pub async fn stop(mut self) -> Result<(), ProxyError> {
        self.stop.send_replace(true);
        self.task
            .take()
            .unwrap()
            .await
            .unwrap_or(Err(ProxyError::Closed))
    }
}
impl Drop for ProxySession {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}

struct PendingTransaction {
    id: u64,
    work: Option<ReplyFuture>,
    reply: oneshot::Sender<Result<Vec<u8>, TransactionError>>,
    close_deadline: Option<Instant>,
}

enum PumpEvent {
    Stop,
    Request(Option<Request>),
    Read(Result<(), ProxyError>),
    Written(Result<FlushOutcome, ProxyError>),
    Directory(u64, Result<Vec<u8>, TransactionError>),
    Expired,
}

fn tunnel_error(error: TunnelError) -> TransactionError {
    match error {
        TunnelError::Directory(error) => error,
        TunnelError::Backpressure | TunnelError::Busy => TransactionError::Limit,
        TunnelError::ConnectStatus(_) => TransactionError::Rejected,
        TunnelError::Invalid | TunnelError::Protocol => TransactionError::Malformed,
    }
}
