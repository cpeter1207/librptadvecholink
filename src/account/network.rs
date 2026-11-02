//! One real account I/O owner; directory waits never suspend packet routing or peer turns.

use super::{
    Account, Admission, aprs,
    coordination::{Coordinator, TalkerPort},
    directory::{DirectoryControl, DirectoryJob, DirectoryPolicy, DirectoryState, Operation},
    settings::PreparedSettings,
};
use crate::{
    directory::{DirectorySnapshot, DirectoryView},
    transport::{
        ReplyKind, TransactionError,
        direct::DirectSockets,
        directory_exchange,
        driver::Control,
        proxy::{ProxyError, ProxyOutput, connect, session::ProxySession},
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
    sync::{Notify, mpsc, oneshot, watch},
    task::JoinHandle,
};

#[cfg(test)]
#[path = "../../tests/unit/account_network.rs"]
pub(crate) mod tests;

/// Metadata-only account notifications. Incoming admission still requires accept/revalidation.
pub enum NetworkEvent {
    /// Selected Proxy is authenticated again; new QSO generations may be admitted.
    TransportReady,
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
    /// Selected Proxy transport failed; its original content-free cause is retained.
    ProxyLost(ProxyError),
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
    /// Direct bind or Proxy-name resolution failed; no transport fallback was attempted.
    Io(io::ErrorKind),
    /// Selected Proxy setup failed with its content-free original cause.
    Proxy(ProxyError),
}

/// Selected account transport; a Proxy peer cannot accidentally send direct UDP.
#[derive(Clone)]
pub enum PeerTransport {
    /// Shared account sockets, with independent per-peer prepared sends/readiness.
    Direct(Arc<DirectSockets>),
    /// Nonblocking handoff to the account's one bounded TCP writer.
    Proxy(ProxyOutput),
}

/// Copied metadata from the existing account owner. No credentials, message
/// bodies, payload bytes or admission authority are exported.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    /// Registration/refresh state, independent of existing QSO liveness.
    pub directory: super::directory::Observation,
    /// Last complete directory publication remains within its allowed age.
    pub fresh: bool,
    /// Age of the last complete directory publication; absent before one exists.
    pub snapshot_age_ms: Option<u64>,
    /// Direct UDP=0, selected standard TCP Proxy=1.
    pub transport: u32,
    /// Currently admitted QSOs, not incoming candidates.
    pub admitted: usize,
    /// Retained, verified but not yet accepted incoming handshakes.
    pub pending: usize,
    /// Exact selected station/generation, including its draining tail.
    pub selected: Option<(u32, u64)>,
    /// Selected raw receive activity, distinct from final drained unkey.
    pub receiving: bool,
    /// Valid competing talker packets suppressed by account policy.
    pub suppressed_packets: u64,
    /// Account talker-control credit failures, not network loss.
    pub control_overflows: u64,
    /// Packets admitted to an exact connection mailbox.
    pub routed_packets: u64,
    /// Unroutable, unverified, malformed or mailbox-rejected packets.
    pub dropped_packets: u64,
    /// Wire-parser failures, a subset of dropped packets.
    pub malformed_packets: u64,
    /// Packets shed by a full peer mailbox, never a hangup.
    pub ingress_full_packets: u64,
    /// Actual Proxy writer metadata; absent in direct mode.
    pub proxy: Option<crate::transport::proxy::ProxyObservation>,
}

enum Transport {
    Direct(Arc<DirectSockets>),
    Proxy(ProxySession),
    Offline(ProxyError),
}
impl Transport {
    fn peer(&self) -> Option<PeerTransport> {
        match self {
            Self::Direct(sockets) => Some(PeerTransport::Direct(Arc::clone(sockets))),
            Self::Proxy(session) => Some(PeerTransport::Proxy(session.output())),
            Self::Offline(_) => None,
        }
    }
}

/// Exact route ownership transferred to one peer worker; no driver is retained on Tokio.
pub struct PeerConnection {
    /// Keep through final peer detach. Dropping retires the exact route even
    /// when a successful dial reply races cancellation; never transfer it to audio.
    pub lease: ConnectionLease,
    /// Verified identity, initial handshake and unique receive consumer.
    pub connection: Connection,
    /// Nonblocking first-talker requests to this account's serialized owner.
    pub talker: TalkerPort,
    /// Wake/stop signals prepared before the worker or host can reenter.
    pub control: Arc<Control>,
    /// Only the selected transport is available to this peer's outgoing owner.
    pub transport: PeerTransport,
}

/// Unique route lifetime, independent of a potentially full command channel.
/// Drop signals the existing account owner; it neither waits nor spawns work.
pub struct ConnectionLease {
    control: Arc<Control>,
    retired: Arc<Notify>,
}
impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.control.stop();
        self.retired.notify_one();
    }
}

type DialReply = oneshot::Sender<Result<PeerConnection, NetworkError>>;
struct PendingDial {
    reply: DialReply,
    deadline: Instant,
}

enum Command {
    Dial(u32, DialReply),
    Accept(u64, DialReply),
    Remove(u32, u64),
    Observe(oneshot::Sender<Result<Observation, NetworkError>>),
    Reconfigure(
        Arc<PreparedSettings>,
        oneshot::Sender<Result<(), NetworkError>>,
    ),
}

/// Owns a task on the existing Tokio runtime, never a new runtime/thread per account or peer.
/// Dropping requests cancellation; stop() is the asynchronous quiescence barrier.
pub struct AccountNetwork {
    control: NetworkControl,
    task: Option<JoinHandle<()>>,
    direct: Option<Arc<DirectSockets>>,
}

/// Cloneable off-audio command port; it owns no join handle or account task.
/// Account shutdown gates every clone before its owner performs the join.
#[derive(Clone)]
pub struct NetworkControl {
    directory: DirectoryView,
    commands: mpsc::Sender<Command>,
    stopping: watch::Sender<bool>,
}

impl AccountNetwork {
    /// Activate the configured transport on the caller's existing Tokio pool.
    /// Direct bind failures reject activation. An unavailable selected Proxy
    /// remains an observable retrying account; it never selects direct UDP.
    pub async fn open(
        settings: Arc<PreparedSettings>,
        civil_time: fn() -> (u8, u8),
    ) -> Result<(Self, mpsc::Receiver<NetworkEvent>), NetworkError> {
        let config = settings.as_abi();
        if config.transport.kind == 0 {
            let ip: std::net::Ipv4Addr = settings
                .bind_address()
                .parse()
                .map_err(|_| NetworkError::Invalid)?;
            let audio = SocketAddr::from((ip, config.transport.audio_port));
            let control = SocketAddr::from((ip, config.transport.audio_port + 1));
            let sockets = DirectSockets::bind(audio, control)
                .await
                .map_err(|error| NetworkError::Io(error.kind()))?;
            return Self::direct(settings, Arc::new(sockets), civil_time);
        }
        let transport = match open_proxy(Arc::clone(&settings)).await {
            Ok(session) => Transport::Proxy(session),
            Err(error) => Transport::Offline(error),
        };
        Self::start(settings, transport, civil_time)
    }

    /// Start a direct account with already-bound sockets and an off-audio civil-time source.
    /// No login occurs during preparation. All directory I/O begins on the spawned owner.
    pub fn direct(
        settings: Arc<PreparedSettings>,
        sockets: Arc<DirectSockets>,
        civil_time: fn() -> (u8, u8),
    ) -> Result<(Self, mpsc::Receiver<NetworkEvent>), NetworkError> {
        Self::start(settings, Transport::Direct(sockets), civil_time)
    }

    fn start(
        settings: Arc<PreparedSettings>,
        transport: Transport,
        civil_time: fn() -> (u8, u8),
    ) -> Result<(Self, mpsc::Receiver<NetworkEvent>), NetworkError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| NetworkError::Invalid)?;
        let mode = match &transport {
            Transport::Direct(_) => 0,
            Transport::Proxy(_) | Transport::Offline(_) => 1,
        };
        if settings.as_abi().transport.kind != mode {
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
            directory_policy(&settings),
        )
        .ok_or(NetworkError::Invalid)?;
        let snapshot =
            DirectorySnapshot::new(Duration::from_millis(policy.directory_freshness_ms.into()));
        let view = DirectoryView::new(&snapshot);
        let capacity = usize::try_from(
            u64::from(policy.max_stations) + u64::from(policy.max_pending_handshakes),
        )
        .map_err(|_| NetworkError::Invalid)?;
        let (commands, receiver) = mpsc::channel(capacity);
        let (events, output) = mpsc::channel(capacity);
        let (stopping, stop) = watch::channel(false);
        let retired = Arc::new(Notify::new());
        let direct = match &transport {
            Transport::Direct(sockets) => Some(Arc::clone(sockets)),
            Transport::Proxy(_) | Transport::Offline(_) => None,
        };
        let owner = Owner {
            aprs_due: aprs::first_due(&settings),
            aprs_work: None,
            settings,
            transport,
            civil_time,
            coordinator,
            directory,
            snapshot,
            view: view.clone(),
            peers: BTreeMap::new(),
            pending_dials: BTreeMap::new(),
            commands: receiver,
            events,
            stop,
            retired,
            routed_packets: 0,
            dropped_packets: 0,
            ingress_full_packets: 0,
        };
        let task = runtime.spawn(owner.run());
        Ok((
            Self {
                control: NetworkControl {
                    directory: view,
                    commands,
                    stopping,
                },
                task: Some(task),
                direct,
            },
            output,
        ))
    }

    /// Borrowable command lifetime is independent of the account's join ownership.
    pub fn control(&self) -> NetworkControl {
        self.control.clone()
    }

    /// Reuse only after this owner's stop barrier. Old immutable QSO handles may
    /// retain the socket allocation, but must never retain an active reader.
    pub(crate) fn direct_sockets(&self) -> Option<Arc<DirectSockets>> {
        self.direct.as_ref().map(Arc::clone)
    }

    /// Read-only current directory; a view alone never authorizes a QSO.
    pub fn directory(&self) -> DirectoryView {
        self.control.directory()
    }

    /// Request a route on the account owner, without blocking the executor.
    pub async fn dial(&self, node: u32) -> Result<PeerConnection, NetworkError> {
        self.control.dial(node).await
    }

    /// Accept a retained incoming token after the host's policy checks.
    pub async fn accept(&self, token: u64) -> Result<PeerConnection, NetworkError> {
        self.control.accept(token).await
    }

    /// Unregister an exact generation; control work must retry Full.
    pub fn remove(&self, node: u32, generation: u64) -> Result<(), NetworkError> {
        self.control.remove(node, generation)
    }

    /// Gate commands/routing, signal peers and finish one finite logout before returning.
    pub async fn stop(mut self) -> Result<(), tokio::task::JoinError> {
        self.control.stopping.send_replace(true);
        self.task.take().unwrap().await
    }
}

impl NetworkControl {
    /// Query on the existing bounded account command channel. This copies on
    /// that owner without a sampler/task, locks, or borrowing any peer state.
    pub async fn observe(&self) -> Result<Observation, NetworkError> {
        self.request(Command::Observe).await
    }
    async fn request<T>(
        &self,
        command: impl FnOnce(oneshot::Sender<Result<T, NetworkError>>) -> Command,
    ) -> Result<T, NetworkError> {
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

    /// Commit nonexclusive settings on this same bounded account owner. Existing
    /// routes, directory jobs and talker state survive; identity/socket changes
    /// require the outer account's quiescent handoff instead.
    pub async fn reconfigure(&self, settings: Arc<PreparedSettings>) -> Result<(), NetworkError> {
        self.request(|reply| Command::Reconfigure(settings, reply))
            .await
    }

    /// Read-only current directory for final provider-bound host admission.
    /// A view is not permission to accept a connection or bypass account policy.
    pub fn directory(&self) -> DirectoryView {
        self.directory.clone()
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
}

impl Drop for AccountNetwork {
    fn drop(&mut self) {
        self.control.stopping.send_replace(true);
    }
}

type DirectoryWork = Pin<Box<dyn Future<Output = (u64, Result<Vec<u8>, TransactionError>)> + Send>>;
type AprsWork = Pin<Box<dyn Future<Output = Result<(), ProxyError>> + Send>>;

/// One selected-Proxy attempt, bounded across DNS, connect and authentication.
async fn open_proxy(settings: Arc<PreparedSettings>) -> Result<ProxySession, ProxyError> {
    let timeout = Duration::from_millis(settings.as_abi().policy.directory_timeout_ms.into());
    tokio::time::timeout(timeout, async {
        let address = tokio::net::lookup_host((
            settings.proxy_host(),
            settings.as_abi().transport.proxy_port,
        ))
        .await
        .map_err(|error| ProxyError::Io(error.kind()))?
        .next()
        .ok_or(ProxyError::Io(io::ErrorKind::AddrNotAvailable))?;
        let transport = connect(
            address,
            settings.callsign(),
            settings.proxy_password(),
            timeout,
        )
        .await?;
        ProxySession::start(transport, timeout)
    })
    .await
    .unwrap_or(Err(ProxyError::Timeout))
}

struct Owner {
    aprs_due: Option<Instant>,
    aprs_work: Option<AprsWork>,
    settings: Arc<PreparedSettings>,
    transport: Transport,
    civil_time: fn() -> (u8, u8),
    coordinator: Coordinator,
    directory: DirectoryControl,
    snapshot: DirectorySnapshot,
    view: DirectoryView,
    peers: BTreeMap<(u32, u64), Arc<Control>>,
    pending_dials: BTreeMap<u32, PendingDial>,
    commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<NetworkEvent>,
    stop: watch::Receiver<bool>,
    retired: Arc<Notify>,
    routed_packets: u64,
    dropped_packets: u64,
    ingress_full_packets: u64,
}

impl Owner {
    fn observe(&mut self) -> Observation {
        let now = Instant::now();
        let router = self.coordinator.router();
        let (admitted, pending, selected, receiving, suppressed_packets, malformed_packets) = (
            router.account.admitted(),
            router.pending_count(),
            router.account.selected(),
            router.account.receiving(),
            router.account.suppressed_packets(),
            router.malformed_packets(),
        );
        let proxy = match &self.transport {
            Transport::Direct(_) | Transport::Offline(_) => None,
            Transport::Proxy(session) => Some(session.output().observe()),
        };
        Observation {
            directory: self.directory.observe(now),
            fresh: self.snapshot.require_fresh(now).is_ok(),
            snapshot_age_ms: self.snapshot.age_ms(now),
            transport: self.settings.as_abi().transport.kind,
            admitted,
            pending,
            selected,
            receiving,
            suppressed_packets,
            malformed_packets,
            control_overflows: self.coordinator.overflows(),
            routed_packets: self.routed_packets,
            dropped_packets: self.dropped_packets,
            ingress_full_packets: self.ingress_full_packets,
            proxy,
        }
    }
    async fn connected(&mut self) -> Option<NetworkEvent> {
        if let Transport::Offline(error) = &self.transport {
            return Some(NetworkEvent::ProxyLost(*error));
        }
        let mut audio = vec![0; MAX_DATAGRAM];
        let mut control = vec![0; MAX_DATAGRAM];
        let sockets = match &self.transport {
            Transport::Direct(sockets) => Some(Arc::clone(sockets)),
            Transport::Proxy(_) | Transport::Offline(_) => None,
        };
        let mut work: Option<DirectoryWork> = None;
        let mut terminal = None;
        loop {
            if *self.stop.borrow() {
                break;
            }
            let now = Instant::now();
            self.coordinator.poll(now);
            if self.aprs_work.is_none() && self.aprs_due.is_some_and(|due| now >= due) {
                self.aprs_work = Some(Box::pin(aprs::publish(
                    Arc::clone(&self.settings),
                    self.transport.peer().expect("connected transport"),
                )));
                self.aprs_due = Some(now + aprs::INTERVAL);
            }
            if work.is_none() {
                match self.directory.poll(&self.snapshot, now) {
                    Ok(Some(job)) => work = Some(self.directory_work(job)),
                    Ok(None) => {}
                    Err(_) => break,
                }
            }
            let deadline = [
                self.directory.deadline(now),
                self.coordinator.deadline(now),
                self.pending_dials.values().map(|dial| dial.deadline).min(),
                self.aprs_due.filter(|_| self.aprs_work.is_none()),
            ]
            .into_iter()
            .flatten()
            .min();
            tokio::select! {
                _ = self.stop.changed() => break,
                _ = self.retired.notified() => self.reap_retired(),
                command = self.commands.recv() => match command {
                    Some(command) => self.command(command),
                    None => break,
                },
                _ = self.coordinator.service() => {},
                result = async { match sockets.as_ref() {
                    Some(sockets) => sockets.recv(Channel::Audio, &mut audio).await,
                    None => std::future::pending().await,
                }} => {
                    if let Err(error) = self.received(Channel::Audio, result, &audio) {
                        terminal = Some(NetworkEvent::TransportLost(error));
                        break;
                    }
                },
                result = async { match sockets.as_ref() {
                    Some(sockets) => sockets.recv(Channel::Control, &mut control).await,
                    None => std::future::pending().await,
                }} => {
                    if let Err(error) = self.received(Channel::Control, result, &control) {
                        terminal = Some(NetworkEvent::TransportLost(error));
                        break;
                    }
                },
                packet = async { match &mut self.transport {
                    Transport::Proxy(session) => session.recv().await
                        .ok_or_else(|| session.failure().unwrap_or(ProxyError::Closed)),
                    Transport::Direct(_) | Transport::Offline(_) => std::future::pending().await,
                }} => match packet {
                    Ok(packet) => self.route(packet.class(), packet.source(), packet.arrived(), packet.payload()),
                    Err(error) => {
                        terminal = Some(NetworkEvent::ProxyLost(error));
                        break;
                    }
                },
                (id, result) = async { match work.as_mut() {
                    Some(work) => work.await,
                    None => std::future::pending().await,
                }} => {
                    work = None;
                    self.directory.finish(id, result.as_deref().map_err(|error| *error), &mut self.snapshot, Instant::now());
                    self.view.publish(&self.snapshot);
                    self.retry_dials(Instant::now());
                    self.health();
                },
                _ = async { match self.aprs_work.as_mut() {
                    Some(work) => work.await,
                    None => std::future::pending().await,
                }} => {
                    // Metadata is best effort, not peer liveness. Retry on the
                    // next reference interval; never terminate or redial a QSO.
                    self.aprs_work = None;
                },
                _ = async { match deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                    None => std::future::pending().await,
                }} => self.retry_dials(Instant::now()),
            }
            // A permanently ready socket/command queue cannot monopolize the executor.
            tokio::task::yield_now().await;
        }
        terminal
    }

    async fn run(mut self) {
        let mut failed = false;
        loop {
            match self.connected().await {
                Some(NetworkEvent::ProxyLost(error)) => {
                    if !self.recover_proxy(error).await {
                        break;
                    }
                }
                Some(event) => {
                    failed = true;
                    tokio::select! {
                        _ = self.events.send(event) => {},
                        _ = self.stop.changed() => {},
                    }
                    break;
                }
                None => break,
            }
        }
        self.commands.close();
        self.aprs_work = None;
        for ((node, generation), peer) in std::mem::take(&mut self.peers) {
            self.coordinator.remove(node, generation);
            if failed {
                peer.transport_lost();
            } else {
                peer.stop();
            }
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
        if let Transport::Proxy(session) = self.transport {
            let _ = session.stop().await;
        }
    }

    /// Recover only account transport/registration. Ordinary QSOs are never redialed.
    /// The existing owner services stop, commands and old finite tails during backoff.
    async fn recover_proxy(&mut self, error: ProxyError) -> bool {
        self.aprs_work = None;
        self.directory.transport_lost();
        self.coordinator.router().clear_pending();
        for (&(node, generation), peer) in &self.peers {
            self.coordinator.unroute(node, generation);
            peer.transport_lost();
        }
        for (_, dial) in std::mem::take(&mut self.pending_dials) {
            let _ = dial.reply.send(Err(NetworkError::Proxy(error)));
        }
        let previous = std::mem::replace(&mut self.transport, Transport::Offline(error));
        if let Transport::Proxy(session) = previous {
            let _ = session.stop().await;
        }
        {
            // This account has stopped routing. Retain its first cause behind
            // ordinary health events, but never prevent lifecycle quiescence.
            tokio::select! {
                _ = self.events.send(NetworkEvent::ProxyLost(error)) => {},
                _ = self.stop.changed() => return false,
            }
        }
        loop {
            let settings = Arc::clone(&self.settings);
            let mut attempt = Box::pin(async move {
                tokio::time::sleep(Duration::from_millis(
                    settings.as_abi().policy.directory_retry_ms.into(),
                ))
                .await;
                open_proxy(settings).await
            });
            loop {
                if *self.stop.borrow() {
                    return false;
                }
                let now = Instant::now();
                self.coordinator.poll(now);
                let deadline = self.coordinator.deadline(now);
                tokio::select! {
                    _ = self.stop.changed() => return false,
                    _ = self.retired.notified() => self.reap_retired(),
                    _ = self.coordinator.service() => {},
                    command = self.commands.recv() => match command {
                        Some(command) => self.command(command),
                        None => return false,
                    },
                    result = &mut attempt => match result {
                        Ok(session) => {
                            self.transport = Transport::Proxy(session);
                            self.aprs_due = aprs::first_due(&self.settings);
                            self.directory.transport_ready();
                            tokio::select! {
                                _ = self.events.send(NetworkEvent::TransportReady) => return true,
                                _ = self.stop.changed() => return false,
                            }
                        }
                        Err(error) => { self.transport = Transport::Offline(error); break; }
                    },
                    _ = async { match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                        None => std::future::pending().await,
                    }} => {},
                }
                tokio::task::yield_now().await;
            }
        }
    }

    fn health(&self) {
        let _ = self.events.try_send(NetworkEvent::Directory {
            state: self.directory.state(),
            fresh: self.snapshot.require_fresh(Instant::now()).is_ok(),
            failures: self.directory.failures(),
        });
    }

    fn reap_retired(&mut self) {
        let coordinator = &mut self.coordinator;
        self.peers.retain(|&(node, generation), control| {
            if control.is_stopped() {
                coordinator.remove(node, generation);
                false
            } else {
                true
            }
        });
    }

    fn received(
        &mut self,
        class: Channel,
        result: io::Result<(usize, SocketAddr)>,
        bytes: &[u8],
    ) -> Result<(), io::ErrorKind> {
        let (length, source) = result.map_err(|error| error.kind())?;
        self.route(class, source, Instant::now(), &bytes[..length]);
        Ok(())
    }

    fn route(&mut self, class: Channel, source: SocketAddr, arrived: Instant, bytes: &[u8]) {
        match self
            .coordinator
            .router()
            .receive(&self.snapshot, class, source, arrived, bytes)
        {
            RouteResult::Pending(pending) => {
                let _ = self.events.try_send(NetworkEvent::Pending(pending));
            }
            RouteResult::Routed => self.routed_packets = self.routed_packets.saturating_add(1),
            RouteResult::RefreshRequired => {
                self.dropped_packets = self.dropped_packets.saturating_add(1);
                self.directory.request_snapshot(arrived);
            }
            outcome => {
                self.dropped_packets = self.dropped_packets.saturating_add(1);
                if matches!(
                    outcome,
                    RouteResult::Backpressure(crate::transport::mailbox::PushError::Full)
                ) {
                    self.ingress_full_packets = self.ingress_full_packets.saturating_add(1);
                }
            }
        }
    }

    fn command(&mut self, command: Command) {
        if let Transport::Offline(error) = &self.transport {
            match command {
                Command::Dial(_, reply) | Command::Accept(_, reply) => {
                    let _ = reply.send(Err(NetworkError::Proxy(*error)));
                    return;
                }
                _ => {}
            }
        }
        let (connection, reply) = match command {
            Command::Dial(node, reply) => {
                let now = Instant::now();
                let result = if self.pending_dials.contains_key(&node) {
                    Err(Admission::Duplicate)
                } else {
                    self.coordinator.router().dial(&self.snapshot, node, now)
                };
                if matches!(result, Err(Admission::Unverified))
                    && self.pending_dials.len()
                        < self.settings.as_abi().policy.max_pending_handshakes as usize
                    && self.directory.request_snapshot(now)
                {
                    self.pending_dials.insert(
                        node,
                        PendingDial {
                            reply,
                            deadline: now
                                + Duration::from_millis(
                                    self.settings.as_abi().policy.connect_timeout_ms.into(),
                                ),
                        },
                    );
                    return;
                }
                (result, reply)
            }
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
            Command::Reconfigure(settings, reply) => {
                let _ = reply.send(self.reconfigure(settings));
                return;
            }
            Command::Observe(reply) => {
                let _ = reply.send(Ok(self.observe()));
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
        self.publish(connection, reply);
    }

    /// Retry only after directory completion/deadline; no task or poll per pending dial.
    fn retry_dials(&mut self, now: Instant) {
        for (node, dial) in std::mem::take(&mut self.pending_dials) {
            if dial.reply.is_closed() {
                continue;
            }
            if now >= dial.deadline {
                let _ = dial
                    .reply
                    .send(Err(NetworkError::Admission(Admission::Unverified)));
                continue;
            }
            match self.coordinator.router().dial(&self.snapshot, node, now) {
                Ok(connection) => self.publish(connection, dial.reply),
                Err(Admission::Unverified) => {
                    self.pending_dials.insert(node, dial);
                }
                Err(error) => {
                    let _ = dial.reply.send(Err(NetworkError::Admission(error)));
                }
            }
        }
    }

    fn publish(&mut self, connection: Connection, reply: DialReply) {
        let identity = (connection.identity.node_number, connection.generation);
        let control = Arc::new(Control::default());
        let peer = PeerConnection {
            lease: ConnectionLease {
                control: Arc::clone(&control),
                retired: Arc::clone(&self.retired),
            },
            talker: self
                .coordinator
                .port(identity.0, identity.1, Arc::clone(&control)),
            connection,
            control: Arc::clone(&control),
            transport: self
                .transport
                .peer()
                .expect("admission requires a live transport"),
        };
        if reply.send(Ok(peer)).is_ok() {
            self.peers.insert(identity, control);
        } else {
            self.coordinator.remove(identity.0, identity.1);
        }
    }

    fn reconfigure(&mut self, settings: Arc<PreparedSettings>) -> Result<(), NetworkError> {
        if self.settings.requires_handoff(&settings) {
            return Err(NetworkError::Invalid);
        }
        let previous = self.settings.directory_login();
        let next = settings.directory_login();
        let refresh = previous.description != next.description || previous.email != next.email;
        // PreparedSettings already validates these positive-u32 timings and
        // servers. Commands close before the owner stops its directory worker.
        self.directory.reconfigure(
            settings.directory_servers().map(str::to_owned).collect(),
            directory_policy(&settings),
            refresh,
        );
        self.coordinator.router().update_policy(&settings);
        self.snapshot.set_freshness(Duration::from_millis(
            settings.as_abi().policy.directory_freshness_ms.into(),
        ));
        self.view.publish(&self.snapshot);
        self.aprs_work = None;
        self.aprs_due = aprs::first_due(&settings);
        self.settings = settings;
        Ok(())
    }

    fn directory_work(&self, job: DirectoryJob) -> DirectoryWork {
        let settings = Arc::clone(&self.settings);
        let (hour, day) = (self.civil_time)();
        let proxy = match &self.transport {
            Transport::Proxy(session) => Some(session.directory()),
            Transport::Direct(_) | Transport::Offline(_) => None,
        };
        Box::pin(async move {
            let result = tokio::time::timeout(job.timeout, async {
                let request = settings
                    .directory_login()
                    .request(&job.operation, hour, day)?;
                // Resolve the protocol's IPv4 endpoint once; the selected
                // transport cannot subsequently receive an IPv6 candidate.
                let address = match job.server.parse::<SocketAddr>() {
                    Ok(SocketAddr::V4(address)) => address,
                    _ => tokio::net::lookup_host((job.server.as_str(), 5200))
                        .await
                        .map_err(|error| TransactionError::Io(error.kind()))?
                        .find_map(|address| match address {
                            SocketAddr::V4(address) => Some(address),
                            SocketAddr::V6(_) => None,
                        })
                        .ok_or(TransactionError::Io(io::ErrorKind::AddrNotAvailable))?,
                };
                let kind = if matches!(job.operation, Operation::Snapshot { .. }) {
                    ReplyKind::Directory
                } else {
                    ReplyKind::Login
                };
                match proxy {
                    Some(proxy) => {
                        proxy
                            .exchange(*address.ip(), &request, kind, job.timeout)
                            .await
                    }
                    None => directory_exchange(address.into(), &request, kind, job.timeout).await,
                }
            })
            .await
            .unwrap_or(Err(TransactionError::Timeout));
            (job.id, result)
        })
    }
}

fn directory_policy(settings: &PreparedSettings) -> DirectoryPolicy {
    let policy = &settings.as_abi().policy;
    DirectoryPolicy {
        operation_timeout: Duration::from_millis(policy.directory_timeout_ms.into()),
        registration_refresh: Duration::from_millis(policy.registration_refresh_ms.into()),
        directory_refresh: Duration::from_millis(policy.directory_refresh_ms.into()),
        retry: Duration::from_millis(policy.directory_retry_ms.into()),
    }
}

#[cfg(test)]
pub(crate) mod test_support {
    /// Cancel only a fixture's actual owner task to exercise join-failure cleanup.
    pub(crate) fn abort_owner(network: &super::AccountNetwork) {
        network.task.as_ref().unwrap().abort();
    }
}
