//! C ownership around the existing account task. No peer borrow spans a host call.

use crate::{
    account::{
        directory::DirectoryState,
        network::{AccountNetwork, ConnectionLease, NetworkControl, NetworkError, NetworkEvent},
        settings::PreparedSettings,
    },
    directory::{AdmissionError, DirectoryEntry},
    ffi::{self, ByteSpan, Event, EventFn, PeerInfo, Status},
    resources::Resources,
};
use std::{
    cell::UnsafeCell,
    ffi::c_void,
    mem::size_of,
    ptr,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
mod peer;
pub(crate) use peer::{
    peer_attach, peer_detach, peer_prepare_audio, peer_read, peer_send_prepared, peer_set_deadline,
    peer_stop, peer_wake,
};
use tokio::{
    runtime::{Handle, Runtime},
    sync::watch,
    task::JoinHandle,
};

pub(crate) struct Account {
    runtime: Arc<Runtime>,
    resources: Option<Arc<Resources>>,
    state: Mutex<State>,
}
struct State {
    settings: Option<Arc<PreparedSettings>>,
    active: Option<Active>,
    generation: u64,
    stopped: bool,
}
struct Active {
    network: AccountNetwork,
    binding: Arc<Binding>,
    stop: watch::Sender<bool>,
    bridge: JoinHandle<()>,
}
struct Binding {
    runtime: Arc<Runtime>,
    _resources: Arc<Resources>,
    settings: Arc<PreparedSettings>,
    control: NetworkControl,
    generation: u64,
    live: AtomicBool,
    first_failure: Mutex<Option<NetworkError>>,
}
struct Peer {
    identity: DirectoryEntry,
    generation: u64,
    control: Arc<crate::transport::driver::Control>,
    _lease: ConnectionLease,
    pending: Mutex<Option<peer::Prepared>>,
    serial: UnsafeCell<Option<peer::Serial>>,
    created: Instant,
    // Fields drop in declaration order: codec/jitter contexts and sockets must
    // retire before their DSO and runtime ownership, including after account drop.
    binding: Arc<Binding>,
}
// Before attach, pending owns the unique endpoints. Attach publishes serial once;
// only its one PeerDriver step and synchronous reentrant host calls access it.
// Identity/control remain independently readable. Detach joins before reclamation.
unsafe impl Send for Peer {}
unsafe impl Sync for Peer {}

#[derive(Clone, Copy)]
struct Callback {
    function: EventFn,
    context: *mut c_void,
}
// The host retains a thread-safe context through the off-executor stop barrier.
// Only this account's one bridge invokes it, never a radio callback.
unsafe impl Send for Callback {}
impl Callback {
    fn emit(self, kind: u32, status: Status, qso: *mut ffi::Qso, generation: u64, taken: *mut u32) {
        let event = Event {
            struct_size: size_of::<Event>() as u32,
            kind,
            qso,
            generation,
            status,
            taken,
        };
        unsafe { (self.function)(self.context, &event) };
    }
}

impl Account {
    pub(crate) fn new(
        runtime: Arc<Runtime>,
        resources: Option<Arc<Resources>>,
        settings: Option<Arc<PreparedSettings>>,
    ) -> Self {
        Self {
            runtime,
            resources,
            state: Mutex::new(State {
                settings,
                active: None,
                generation: 0,
                stopped: false,
            }),
        }
    }

    fn binding(&self) -> Result<Arc<Binding>, Status> {
        let state = self.state.lock().map_err(|_| Status::Failed)?;
        state
            .active
            .as_ref()
            .filter(|active| active.binding.live.load(Ordering::Acquire))
            .map(|active| Arc::clone(&active.binding))
            .ok_or(Status::Closed)
    }
}

fn network_status(error: NetworkError) -> Status {
    match error {
        NetworkError::Invalid | NetworkError::Admission(_) => Status::Invalid,
        NetworkError::Stopped => Status::Closed,
        NetworkError::Full => Status::Full,
        NetworkError::Io(_) | NetworkError::Proxy(_) => Status::Failed,
    }
}

/// The ASL directory stamp uses local civil hour/day-of-month, not elapsed time.
fn civil_time() -> (u8, u8) {
    #[cfg(unix)]
    {
        let seconds = unsafe { libc::time(ptr::null_mut()) };
        let mut local: libc::tm = unsafe { std::mem::zeroed() };
        if !unsafe { libc::localtime_r(&seconds, &mut local) }.is_null() {
            return (local.tm_hour as u8, local.tm_mday as u8);
        }
    }
    // Invalid stamp fails directory serialization; never publish a fabricated date.
    (255, 0)
}

pub(crate) unsafe extern "C" fn account_activate(
    account: *mut ffi::Account,
    callback: Option<EventFn>,
    context: *mut c_void,
) -> Status {
    if Handle::try_current().is_ok() {
        return Status::Invalid;
    }
    let Some(account) = (unsafe { account.cast::<Account>().as_ref() }) else {
        return Status::Invalid;
    };
    let Ok(mut state) = account.state.lock() else {
        return Status::Failed;
    };
    if state.stopped {
        return Status::Closed;
    }
    if state.active.is_some() {
        return Status::Invalid;
    }
    let Some(settings) = state.settings.as_ref().map(Arc::clone) else {
        return Status::Ok;
    };
    let Some(resources) = account.resources.as_ref().map(Arc::clone) else {
        return Status::Unsupported;
    };
    let Some(generation) = state.generation.checked_add(1) else {
        return Status::Failed;
    };
    let (network, events) = match account
        .runtime
        .block_on(AccountNetwork::open(Arc::clone(&settings), civil_time))
    {
        Ok(network) => network,
        Err(error) => return network_status(error),
    };
    let binding = Arc::new(Binding {
        runtime: Arc::clone(&account.runtime),
        _resources: resources,
        settings,
        control: network.control(),
        generation,
        live: AtomicBool::new(true),
        first_failure: Mutex::new(None),
    });
    let (stop, stopping) = watch::channel(false);
    let bridge = account.runtime.spawn(bridge(
        Arc::clone(&binding),
        events,
        stopping,
        callback.map(|function| Callback { function, context }),
    ));
    state.generation = generation;
    state.active = Some(Active {
        network,
        binding,
        stop,
        bridge,
    });
    Status::Ok
}

async fn bridge(
    binding: Arc<Binding>,
    mut events: tokio::sync::mpsc::Receiver<NetworkEvent>,
    mut stopping: watch::Receiver<bool>,
    callback: Option<Callback>,
) {
    loop {
        if *stopping.borrow() {
            break;
        }
        let event = tokio::select! {
            _ = stopping.changed() => break,
            event = events.recv() => match event { Some(event) => event, None => break },
        };
        match event {
            NetworkEvent::Pending(pending) => {
                let Some(callback) = callback else {
                    continue;
                };
                let connection = match binding.control.accept(pending.token()).await {
                    Ok(connection) => connection,
                    Err(error) => {
                        callback.emit(
                            0,
                            network_status(error),
                            ptr::null_mut(),
                            binding.generation,
                            ptr::null_mut(),
                        );
                        continue;
                    }
                };
                let peer = Peer::new(Arc::clone(&binding), connection, pending.arrived);
                if peer.validate() != Status::Ok || *stopping.borrow() {
                    continue;
                }
                let generation = peer.generation;
                let qso = Arc::into_raw(Arc::new(peer)).cast_mut().cast();
                let mut taken = 0;
                callback.emit(1, Status::Pending, qso, generation, &mut taken);
                if taken != 1 {
                    // No host claim: the unique lease retires even a full-command-channel route.
                    drop(unsafe { Arc::from_raw(qso.cast::<Peer>()) });
                }
            }
            NetworkEvent::Directory { state, fresh, .. } => {
                let status = if state == DirectoryState::Retrying {
                    Status::Failed
                } else if fresh && state == DirectoryState::Online {
                    Status::Ok
                } else {
                    Status::Pending
                };
                if let Some(callback) = callback {
                    callback.emit(
                        0,
                        status,
                        ptr::null_mut(),
                        binding.generation,
                        ptr::null_mut(),
                    );
                }
            }
            NetworkEvent::TransportLost(error) => {
                binding.failure(NetworkError::Io(error), callback)
            }
            NetworkEvent::ProxyLost(error) => binding.failure(NetworkError::Proxy(error), callback),
        }
    }
}

impl Binding {
    fn failure(&self, error: NetworkError, callback: Option<Callback>) {
        if let Ok(mut first) = self.first_failure.lock() {
            first.get_or_insert(error);
        }
        self.live.store(false, Ordering::Release);
        if let Some(callback) = callback {
            callback.emit(
                0,
                Status::Failed,
                ptr::null_mut(),
                self.generation,
                ptr::null_mut(),
            );
        }
    }
}

pub(crate) unsafe extern "C" fn account_stop(account: *mut ffi::Account) -> Status {
    if Handle::try_current().is_ok() {
        return Status::Invalid;
    }
    let Some(account) = (unsafe { account.cast::<Account>().as_ref() }) else {
        return Status::Invalid;
    };
    let active = {
        let Ok(mut state) = account.state.lock() else {
            return Status::Failed;
        };
        state.stopped = true;
        let active = state.active.take();
        if let Some(active) = &active {
            active.binding.live.store(false, Ordering::Release);
            active.stop.send_replace(true);
        }
        active
    };
    let Some(active) = active else {
        return Status::Ok;
    };
    account.runtime.block_on(async {
        let notified = active.bridge.await;
        let network = active.network.stop().await;
        if notified.is_ok() && network.is_ok() {
            Status::Ok
        } else {
            Status::Failed
        }
    })
}

pub(crate) unsafe extern "C" fn account_destroy(account: *mut ffi::Account) {
    if !account.is_null() && unsafe { account_stop(account) } == Status::Ok {
        drop(unsafe { Box::from_raw(account.cast::<Account>()) });
    }
}

pub(crate) unsafe extern "C" fn dial(
    account: *mut ffi::Account,
    node: u32,
    output: *mut *mut ffi::Qso,
) -> Status {
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    if node == 0 || Handle::try_current().is_ok() {
        return Status::Invalid;
    }
    let Some(account) = (unsafe { account.cast::<Account>().as_ref() }) else {
        return Status::Invalid;
    };
    let binding = match account.binding() {
        Ok(binding) => binding,
        Err(status) => return status,
    };
    let timeout = Duration::from_millis(binding.settings.as_abi().policy.connect_timeout_ms.into());
    let connection = match binding
        .runtime
        .block_on(async { tokio::time::timeout(timeout, binding.control.dial(node)).await })
    {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => return network_status(error),
        Err(_) => return Status::Failed,
    };
    let peer = Peer::new(binding, connection, Instant::now());
    let valid = peer.validate();
    if valid != Status::Ok {
        return valid;
    }
    *output = Arc::into_raw(Arc::new(peer)).cast_mut().cast();
    Status::Ok
}

impl Peer {
    fn validate(&self) -> Status {
        if !self.binding.live.load(Ordering::Acquire)
            || self.control.is_stopped()
            || self.created.elapsed()
                >= Duration::from_millis(
                    self.binding
                        .settings
                        .as_abi()
                        .policy
                        .connect_timeout_ms
                        .into(),
                )
        {
            return Status::Closed;
        }
        let identity = &self.identity;
        match self.binding.control.directory().lookup(
            &identity.callsign,
            identity.address,
            Instant::now(),
        ) {
            Ok(current) if current.node_number == identity.node_number => Status::Ok,
            Err(AdmissionError::Stale) => Status::Pending,
            _ => Status::Invalid,
        }
    }
}

pub(crate) unsafe extern "C" fn validate_pending(
    qso: *const ffi::Qso,
    output: *mut PeerInfo,
) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    let valid = peer.validate();
    if valid != Status::Ok {
        return valid;
    }
    unsafe { peer_info(qso, output) }
}

pub(crate) unsafe extern "C" fn peer_info(qso: *const ffi::Qso, output: *mut PeerInfo) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if output.is_null()
        || unsafe { ptr::addr_of!((*output).struct_size).read() } < size_of::<PeerInfo>() as u32
        || unsafe { ptr::addr_of!((*output).abi_version).read() } != 1
    {
        return Status::Invalid;
    }
    let identity = &peer.identity;
    let local = peer.binding.settings.local_node();
    let result = PeerInfo {
        struct_size: size_of::<PeerInfo>() as u32,
        abi_version: 1,
        node_number: identity.node_number,
        capabilities: 0,
        account_generation: peer.binding.generation,
        qso_generation: peer.generation,
        callsign: ByteSpan {
            data: identity.callsign.as_ptr(),
            length: identity.callsign.len(),
        },
        local_node: ByteSpan {
            data: local.as_ptr(),
            length: local.len(),
        },
    };
    // The checked record covers writable caller storage; never form a shared output borrow.
    unsafe { ptr::write(output, result) };
    Status::Ok
}

pub(crate) unsafe extern "C" fn peer_destroy(qso: *mut ffi::Qso) {
    // Foreign teardown is control-only/off-executor; never drop a final Runtime
    // or future PeerDriver join on one of its own worker threads.
    if !qso.is_null() && Handle::try_current().is_err() {
        drop(unsafe { Arc::from_raw(qso.cast::<Peer>()) });
    }
}
