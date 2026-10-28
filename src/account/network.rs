//! One real account I/O owner; directory waits never suspend packet routing or peer turns.

use super::{
    Account, Admission,
    coordination::{Coordinator, TalkerPort},
    directory::{DirectoryControl, DirectoryJob, DirectoryPolicy, DirectoryState, Operation},
    settings::PreparedSettings,
};
use crate::{
    directory::DirectorySnapshot,
    transport::{
        ReplyKind, TransactionError,
        direct::DirectSockets,
        directory_exchange,
        driver::Control,
        routing::{Connection, PendingHandshake, RouteResult, Router},
    },
    wire::{Channel, MAX_DATAGRAM},
};
use std::{
    collections::BTreeMap,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
};

/// Metadata-only account notifications. Incoming admission still requires accept/revalidation.
pub enum NetworkEvent {
    /// One completed directory transaction, independent of existing QSO liveness.
    Directory {
        /// Current registration/refresh health.
        state: DirectoryState,
        /// A usable snapshot has been published; final admission still checks its age.
        fresh: bool,
        /// Failed transactions, not packet-loss accounting.
        failures: u64,
    },
    /// Bounded, directory-verified incoming candidate; no media owner exists yet.
    Pending(PendingHandshake),
    /// The account's direct transport failed with its original OS category.
    TransportLost(io::ErrorKind),
}

/// Explicit admission/control outcome; full control credit is not a hangup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NetworkError {
    /// Invalid account mode or unavailable executor.
    Invalid,
    /// Account shutdown has gated new work.
    Stopped,
    /// This account's bounded command credit is exhausted.
    Full,
    /// Existing directory/account policy rejected the requested generation.
    Admission(Admission),
}

/// Exact route ownership transferred to one peer worker; no driver is retained on Tokio.
pub struct PeerConnection {
    /// Verified identity, initial handshake and unique receive consumer.
    pub connection: Connection,
    /// Nonblocking first-talker requests to this account's serialized owner.
    pub talker: TalkerPort,
    /// Wake/stop signals prepared before the worker or host can reenter.
    pub control: Arc<Control>,
    /// Shared direct sockets; each peer retains its own outgoing packet/readiness state.
    pub sockets: Arc<DirectSockets>,
}

enum Command {
    Dial(u32, oneshot::Sender<Result<PeerConnection, NetworkError>>),
    Accept(u64, oneshot::Sender<Result<PeerConnection, NetworkError>>),
    Remove(u32, u64),
}

/// Owns a task on the existing Tokio runtime, never a new runtime/thread per account or peer.
/// Dropping requests cancellation; stop() is the asynchronous quiescence barrier.
pub struct AccountNetwork {
    commands: mpsc::Sender<Command>,
    stopping: watch::Sender<bool>,
    task: Option<JoinHandle<()>>,
}

impl AccountNetwork {
    /// Start a direct account with already-bound sockets and an off-audio civil-time source.
    /// No login occurs during preparation. All directory I/O begins on the spawned owner.
    pub fn direct(
        settings: Arc<PreparedSettings>,
        sockets: Arc<DirectSockets>,
        civil_time: fn() -> (u8, u8),
    ) -> Result<(Self, mpsc::Receiver<NetworkEvent>), NetworkError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| NetworkError::Invalid)?;
        if settings.as_abi().transport.kind != 0 {
            return Err(NetworkError::Invalid);
        }
        let policy = &settings.as_abi().policy;
        let account = Account::new(
            policy.max_stations,
            settings.allow().map(str::to_owned).collect(),
            settings.deny().map(str::to_owned).collect(),
        )
        .ok_or(NetworkError::Invalid)?;
        let router = Router::new(
            account,
            policy.max_pending_handshakes,
            Duration::from_millis(policy.connect_timeout_ms.into()),
        )
        .ok_or(NetworkError::Invalid)?;
        let coordinator = Coordinator::new(router).ok_or(NetworkError::Invalid)?;
        let directory = DirectoryControl::new(
            settings.directory_servers().map(str::to_owned).collect(),
            DirectoryPolicy {
                operation_timeout: Duration::from_millis(policy.directory_timeout_ms.into()),
                registration_refresh: Duration::from_millis(policy.registration_refresh_ms.into()),
                directory_refresh: Duration::from_millis(policy.directory_refresh_ms.into()),
                retry: Duration::from_millis(policy.directory_retry_ms.into()),
            },
        )
        .ok_or(NetworkError::Invalid)?;
        let snapshot =
            DirectorySnapshot::new(Duration::from_millis(policy.directory_freshness_ms.into()));
        let capacity = usize::try_from(
            u64::from(policy.max_stations) + u64::from(policy.max_pending_handshakes),
        )
        .map_err(|_| NetworkError::Invalid)?;
        let (commands, receiver) = mpsc::channel(capacity);
        let (events, output) = mpsc::channel(capacity);
        let (stopping, stop) = watch::channel(false);
        let owner = Owner {
            settings,
            sockets,
            civil_time,
            coordinator,
            directory,
            snapshot,
            peers: BTreeMap::new(),
            commands: receiver,
            events,
            stop,
        };
        let task = runtime.spawn(owner.run());
        Ok((
            Self {
                commands,
                stopping,
                task: Some(task),
            },
            output,
        ))
    }

    async fn request(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<PeerConnection, NetworkError>>) -> Command,
    ) -> Result<PeerConnection, NetworkError> {
        if *self.stopping.borrow() {
            return Err(NetworkError::Stopped);
        }
        let (sender, receiver) = oneshot::channel();
        self.commands
            .try_send(command(sender))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => NetworkError::Full,
                mpsc::error::TrySendError::Closed(_) => NetworkError::Stopped,
            })?;
        receiver.await.map_err(|_| NetworkError::Stopped)?
    }

    /// Request an outbound route through the fresh directory; never block an executor thread.
    pub async fn dial(&self, node: u32) -> Result<PeerConnection, NetworkError> {
        self.request(|reply| Command::Dial(node, reply)).await
    }

    /// Accept a retained incoming token after the host's policy checks.
    pub async fn accept(&self, token: u64) -> Result<PeerConnection, NetworkError> {
        self.request(|reply| Command::Accept(token, reply)).await
    }

    /// Unregister an exact generation before peer teardown. Full must be retried by control work.
    pub fn remove(&self, node: u32, generation: u64) -> Result<(), NetworkError> {
        self.commands
            .try_send(Command::Remove(node, generation))
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => NetworkError::Full,
                mpsc::error::TrySendError::Closed(_) => NetworkError::Stopped,
            })
    }

    /// Gate commands/routing, signal peers and finish one finite logout before returning.
    pub async fn stop(mut self) -> Result<(), tokio::task::JoinError> {
        self.stopping.send_replace(true);
        self.task.take().unwrap().await
    }
}

impl Drop for AccountNetwork {
    fn drop(&mut self) {
        self.stopping.send_replace(true);
    }
}

type DirectoryWork = Pin<Box<dyn Future<Output = (u64, Result<Vec<u8>, TransactionError>)> + Send>>;

struct Owner {
    settings: Arc<PreparedSettings>,
    sockets: Arc<DirectSockets>,
    civil_time: fn() -> (u8, u8),
    coordinator: Coordinator,
    directory: DirectoryControl,
    snapshot: DirectorySnapshot,
    peers: BTreeMap<(u32, u64), Arc<Control>>,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<NetworkEvent>,
    stop: watch::Receiver<bool>,
}

impl Owner {
    async fn run(mut self) {
        let mut audio = vec![0; MAX_DATAGRAM];
        let mut control = vec![0; MAX_DATAGRAM];
        let mut work: Option<DirectoryWork> = None;
        loop {
            if *self.stop.borrow() {
                break;
            }
            let now = Instant::now();
            self.coordinator.poll(now);
            if work.is_none() {
                match self.directory.poll(&self.snapshot, now) {
                    Ok(Some(job)) => work = Some(self.directory_work(job)),
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
            let deadline = [self.directory.deadline(now), self.coordinator.deadline(now)]
                .into_iter()
                .flatten()
                .min();
            tokio::select! {
                _ = self.stop.changed() => break,
                command = self.commands.recv() => match command {
                    Some(command) => self.command(command),
                    None => break,
                },
                _ = self.coordinator.service() => {},
                result = self.sockets.recv(Channel::Audio, &mut audio) => {
                    if !self.received(Channel::Audio, result, &audio) { break; }
                },
                result = self.sockets.recv(Channel::Control, &mut control) => {
                    if !self.received(Channel::Control, result, &control) { break; }
                },
                (id, result) = async { match work.as_mut() {
                    Some(work) => work.await,
                    None => std::future::pending().await,
                }} => {
                    work = None;
                    self.directory.finish(id, result.as_deref().map_err(|error| *error), &mut self.snapshot, Instant::now());
                    self.health();
                },
                _ = async { match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }} => {},
            }
            // A permanently ready socket/command queue cannot monopolize the executor.
            tokio::task::yield_now().await;
        }
        drop(work);
        self.commands.close();
        for ((node, generation), peer) in std::mem::take(&mut self.peers) {
            self.coordinator.remove(node, generation);
            peer.stop();
        }
        self.coordinator.close();
        if let Ok(Some(job)) = self.directory.stop() {
            let (id, result) = self.directory_work(job).await;
            self.directory.finish(
                id,
                result.as_deref().map_err(|error| *error),
                &mut self.snapshot,
                Instant::now(),
            );
        }
    }

    fn health(&self) {
        let _ = self.events.try_send(NetworkEvent::Directory {
            state: self.directory.state(),
            fresh: self.snapshot.require_fresh(Instant::now()).is_ok(),
            failures: self.directory.failures(),
        });
    }

    fn received(
        &mut self,
        class: Channel,
        result: io::Result<(usize, SocketAddr)>,
        bytes: &[u8],
    ) -> bool {
        let (length, source) = match result {
            Ok(packet) => packet,
            Err(error) => {
                let _ = self
                    .events
                    .try_send(NetworkEvent::TransportLost(error.kind()));
                return false;
            }
        };
        if let RouteResult::Pending(pending) = self.coordinator.router().receive(
            &self.snapshot,
            class,
            source,
            Instant::now(),
            &bytes[..length],
        ) {
            let _ = self.events.try_send(NetworkEvent::Pending(pending));
        }
        true
    }

    fn command(&mut self, command: Command) {
        let (connection, reply) = match command {
            Command::Dial(node, reply) => (
                self.coordinator
                    .router()
                    .dial(&self.snapshot, node, Instant::now()),
                reply,
            ),
            Command::Accept(token, reply) => (
                self.coordinator
                    .router()
                    .accept(&self.snapshot, token, Instant::now()),
                reply,
            ),
            Command::Remove(node, generation) => {
                self.coordinator.remove(node, generation);
                if let Some(peer) = self.peers.remove(&(node, generation)) {
                    peer.stop();
                }
                return;
            }
        };
        let connection = match connection {
            Ok(connection) => connection,
            Err(error) => {
                let _ = reply.send(Err(NetworkError::Admission(error)));
                return;
            }
        };
        let identity = (connection.identity.node_number, connection.generation);
        let control = Arc::new(Control::default());
        let peer = PeerConnection {
            talker: self
                .coordinator
                .port(identity.0, identity.1, Arc::clone(&control)),
            connection,
            control: Arc::clone(&control),
            sockets: Arc::clone(&self.sockets),
        };
        if reply.send(Ok(peer)).is_ok() {
            self.peers.insert(identity, control);
        } else {
            self.coordinator.remove(identity.0, identity.1);
        }
    }

    fn directory_work(&self, job: DirectoryJob) -> DirectoryWork {
        let settings = Arc::clone(&self.settings);
        let (hour, day) = (self.civil_time)();
        Box::pin(async move {
            let result = tokio::time::timeout(job.timeout, async {
                let request = settings
                    .directory_login()
                    .request(&job.operation, hour, day)?;
                let address = match job.server.parse::<SocketAddr>() {
                    Ok(address) if address.is_ipv4() => address,
                    _ => tokio::net::lookup_host((job.server.as_str(), 5200))
                        .await
                        .map_err(|error| TransactionError::Io(error.kind()))?
                        .find(SocketAddr::is_ipv4)
                        .ok_or(TransactionError::Io(io::ErrorKind::AddrNotAvailable))?,
                };
                let kind = if matches!(job.operation, Operation::Snapshot { .. }) {
                    ReplyKind::Directory
                } else {
                    ReplyKind::Login
                };
                directory_exchange(address, &request, kind, job.timeout).await
            })
            .await
            .unwrap_or(Err(TransactionError::Timeout));
            (job.id, result)
        })
    }
}
