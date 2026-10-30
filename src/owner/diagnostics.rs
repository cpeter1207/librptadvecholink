//! Metadata copies on their existing owners, never on a radio callback.
use super::{Account, TransportFailure, ffi, network_status};
use std::{fmt::Debug, mem::size_of, ptr, sync::atomic::Ordering};

/// Validate the readable prefix before forming a reference to the whole output.
pub(super) unsafe fn output<T>(value: *mut T) -> bool {
    !value.is_null()
        && unsafe { value.cast::<u32>().read() } >= size_of::<T>() as u32
        && unsafe { value.cast::<u32>().add(1).read() } == 1
}

/// Only typed local error categories may enter this bounded field, never input bytes.
pub(super) fn detail(error: &impl Debug) -> [u8; 32] {
    let mut result = [0; 32];
    for (out, byte) in result.iter_mut().take(31).zip(format!("{error:?}").bytes()) {
        *out = if byte.is_ascii_graphic() || byte == b' ' {
            byte
        } else {
            b'?'
        };
    }
    result
}
impl TransportFailure {
    /// Stable copied categories; their payloads are typed local errors only.
    fn code(self) -> u64 {
        match self {
            Self::Io(_) => 1,
            Self::Proxy(_) => 2,
        }
    }
}

pub(crate) unsafe extern "C" fn account_observe(
    account: *const ffi::Account,
    output_record: *mut ffi::AccountObservation,
) -> ffi::Status {
    if tokio::runtime::Handle::try_current().is_ok() || !unsafe { output(output_record) } {
        return ffi::Status::Invalid;
    }
    let Some(account) = (unsafe { account.cast::<Account>().as_ref() }) else {
        return ffi::Status::Invalid;
    };
    let (mut result, binding) = {
        let Ok(state) = account.state.lock() else {
            return ffi::Status::Failed;
        };
        let mut result = ffi::AccountObservation {
            struct_size: size_of::<ffi::AccountObservation>() as u32,
            abi_version: 1,
            generation: state.generation,
            registration_age_ms: u64::MAX,
            snapshot_age_ms: u64::MAX,
            retry_in_ms: u64::MAX,
            state: if state.stopped {
                5
            } else if state.settings.is_none() {
                6
            } else if state.activated {
                7
            } else {
                0
            },
            ..Default::default()
        };
        if let Some(settings) = &state.settings {
            result.node_number = settings.as_abi().node_number;
            result.transport = settings.as_abi().transport.kind;
            let callsign = settings.callsign().as_bytes(); // Validated at most15 ASCII bytes.
            result.callsign[..callsign.len()].copy_from_slice(callsign);
        }
        (
            result,
            state
                .active
                .as_ref()
                .map(|active| std::sync::Arc::clone(&active.binding)),
        )
    }; // Never hold account state while querying its network owner.
    if let Some(binding) = binding {
        let first = match binding.first_failure.lock() {
            Ok(first) => *first,
            Err(_) => return ffi::Status::Failed,
        };
        if let Some(error) = first {
            result.state = 7;
            result.first_cause = error.code();
            result.cause_detail = detail(&error);
        } else if binding.live.load(Ordering::Acquire) {
            let observed = match account.runtime.block_on(binding.control.observe()) {
                Ok(observed) => observed,
                Err(error) => return network_status(error),
            };
            // The network owner answers only while running. Shutdown codes
            // remain defined by the directory state, without duplicate branches
            // for states that cannot answer an account observation request.
            result.state = observed.directory.state as u32;
            result.available = 1;
            result.fresh = u32::from(observed.fresh);
            result.registered = u32::from(observed.directory.registered);
            result.directory_failures = observed.directory.failures;
            result.directory_operations = observed.directory.operations;
            result.registration_age_ms = observed.directory.registration_age_ms.unwrap_or(u64::MAX);
            result.snapshot_age_ms = observed.snapshot_age_ms.unwrap_or(u64::MAX);
            result.retry_in_ms = observed.directory.retry_in_ms.unwrap_or(u64::MAX);
            result.admitted = observed.admitted as u64;
            result.pending = observed.pending as u64;
            (result.selected_node, result.selected_generation) = observed
                .selected
                .map_or((0, 0), |(node, generation)| (u64::from(node), generation));
            result.receiving = u64::from(observed.receiving);
            result.suppressed_packets = observed.suppressed_packets;
            result.control_overflows = observed.control_overflows;
            result.routed_packets = observed.routed_packets;
            result.dropped_packets = observed.dropped_packets;
            result.malformed_packets = observed.malformed_packets;
            result.ingress_full_packets = observed.ingress_full_packets;
            if let Some(proxy) = observed.proxy {
                result.proxy = ffi::ProxyObservation {
                    admitted: proxy.admitted,
                    sent: proxy.sent,
                    expired: proxy.expired,
                    stalls: proxy.stalls,
                    stall_ms: proxy.stall_ms,
                    queued_bytes: proxy.queued_bytes,
                    head_age_ms: proxy.head_age_ms,
                };
            }
        }
    }
    unsafe { ptr::write(output_record, result) };
    ffi::Status::Ok
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{account::settings::PreparedSettings, resources::Resources};
    use std::{
        fmt,
        io::{Read, Write},
        net::{TcpListener, UdpSocket},
        sync::Arc,
        time::{Duration, Instant},
    };

    #[test]
    fn network_failures_preserve_public_admission_and_transport_categories() {
        use crate::account::network::NetworkError;
        for (error, status) in [
            (NetworkError::Invalid, ffi::Status::Invalid),
            (
                NetworkError::Admission(crate::account::Admission::Unverified),
                ffi::Status::Invalid,
            ),
            (NetworkError::Stopped, ffi::Status::Closed),
            (NetworkError::Full, ffi::Status::Full),
            (
                NetworkError::Io(std::io::ErrorKind::TimedOut),
                ffi::Status::Failed,
            ),
            (
                NetworkError::Proxy(crate::transport::proxy::ProxyError::Closed),
                ffi::Status::Failed,
            ),
        ] {
            assert_eq!(super::super::network_status(error), status);
        }
    }

    #[test]
    #[cfg(unix)]
    fn unrepresentable_wall_clock_does_not_publish_a_fabricated_directory_stamp() {
        let (hour, day) = super::super::civil_time_at(0);
        assert!(hour < 24 && (1..=31).contains(&day));
        assert_eq!(super::super::civil_time_at(libc::time_t::MAX), (255, 0));
    }

    struct Activated(Box<Account>);
    impl Activated {
        fn pointer(&self) -> *mut ffi::Account {
            ptr::from_ref(self.0.as_ref()).cast_mut().cast()
        }
        fn observation(&self) -> (ffi::Status, ffi::AccountObservation) {
            let mut result = ffi::AccountObservation {
                struct_size: size_of::<ffi::AccountObservation>() as u32,
                abi_version: 1,
                ..Default::default()
            };
            let status = unsafe { account_observe(self.pointer(), &mut result) };
            (status, result)
        }
    }
    impl Drop for Activated {
        fn drop(&mut self) {
            unsafe { super::super::account_stop(self.pointer()) };
        }
    }

    fn active(runtime: Arc<tokio::runtime::Runtime>) -> (Activated, TcpListener) {
        let span = |text: &str| ffi::ByteSpan {
            data: text.as_ptr(),
            length: text.len(),
        };
        let directory = TcpListener::bind("127.0.0.1:0").unwrap();
        let server = directory.local_addr().unwrap().to_string();
        let servers = [span(&server)];
        let audio_port = loop {
            let audio = UdpSocket::bind("127.0.0.1:0").unwrap();
            let port = audio.local_addr().unwrap().port();
            if port < u16::MAX && UdpSocket::bind(("127.0.0.1", port + 1)).is_ok() {
                break port;
            }
        };
        let config = ffi::AccountConfig {
            enabled: 1,
            node_number: 654321,
            local_node: span("524950"),
            callsign: span("K1ABC-R"),
            transport: ffi::TransportConfig {
                bind_address: span("127.0.0.1"),
                audio_port,
                directory_servers: ffi::StringList {
                    items: servers.as_ptr(),
                    count: 1,
                },
                ..Default::default()
            },
            policy: ffi::AccountPolicy {
                max_stations: 1,
                max_pending_handshakes: 1,
                heartbeat_interval_ms: 10_000,
                heartbeat_timeout_ms: 100_000,
                connect_timeout_ms: 30_000,
                directory_timeout_ms: 10_000,
                registration_refresh_ms: 300_000,
                directory_refresh_ms: 240_000,
                directory_retry_ms: 60_000,
                directory_freshness_ms: 900_000,
                ..Default::default()
            },
            ..Default::default()
        };
        let secrets = ffi::AccountSecrets {
            struct_size: size_of::<ffi::AccountSecrets>() as u32,
            abi_version: 1,
            directory_password: span("fixture-password"),
            proxy_password: ffi::ByteSpan::default(),
        };
        let settings = Arc::new(unsafe { PreparedSettings::copy(&config, &secrets) }.unwrap());
        let path = std::env::var("RPTADV_CODEC_DIR").unwrap();
        let gsm = format!("{path}/librptadv_codec_gsm.so.1");
        let jitter = format!("{path}/rpt_advanced/librptadv_jitter_adapter.so.1");
        let resources = unsafe {
            Resources::load(&ffi::RuntimeConfig {
                gsm_library: span(&gsm),
                jitter_library: span(&jitter),
                ..Default::default()
            })
        }
        .unwrap()
        .unwrap();
        let account = Activated(Box::new(Account::new(
            runtime,
            Some(Arc::new(resources)),
            Some(settings),
        )));
        assert_eq!(
            unsafe { super::super::account_activate(account.pointer(), None, ptr::null_mut()) },
            ffi::Status::Ok
        );
        (account, directory)
    }

    #[test]
    fn registering_and_retrying_are_copied_from_the_real_directory_owner() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .unwrap(),
        );
        let (account, directory) = active(runtime);
        let (arrived, arrival) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let server = std::thread::spawn(move || {
            let (mut connection, _) = directory.accept().unwrap();
            connection
                .set_read_timeout(Some(Duration::from_secs(3)))
                .unwrap();
            let mut first = [0];
            connection.read_exact(&mut first).unwrap();
            assert_eq!(first, [b'l']);
            arrived.send(()).unwrap();
            released.recv_timeout(Duration::from_secs(3)).unwrap();
            connection.write_all(b"NO").unwrap();
        });
        arrival.recv_timeout(Duration::from_secs(3)).unwrap();
        let (status, result) = account.observation();
        assert_eq!(status, ffi::Status::Ok);
        assert_eq!(
            (result.state, result.available, result.registered),
            (1, 1, 0)
        );
        release.send(()).unwrap();
        server.join().unwrap();
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let (status, result) = account.observation();
            assert_eq!(status, ffi::Status::Ok);
            if result.state == 3 {
                assert_eq!(result.directory_failures, 1);
                assert!(result.retry_in_ms <= 60_000);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "registration rejection was not observed"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    #[test]
    fn retained_binding_observation_handles_retirement_and_poisoned_metadata() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let (account, _directory) = active(Arc::clone(&runtime));
        let binding = account.0.binding().unwrap();
        let failure = binding.first_failure.lock().unwrap();
        let references = Arc::strong_count(&binding);
        std::thread::scope(|scope| {
            let observed = scope.spawn(|| account.observation());
            let deadline = Instant::now() + Duration::from_secs(3);
            while Arc::strong_count(&binding) == references {
                assert!(
                    Instant::now() < deadline,
                    "observer never retained the binding"
                );
                std::thread::sleep(Duration::from_millis(1));
            }
            assert_eq!(
                unsafe { super::super::account_stop(account.pointer()) },
                ffi::Status::Ok
            );
            drop(failure);
            let (status, result) = observed.join().unwrap();
            assert_eq!(status, ffi::Status::Ok);
            assert_eq!((result.available, result.first_cause), (0, 0));
        });
        let (account, _directory) = active(runtime);
        let binding = account.0.binding().unwrap();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = binding.first_failure.lock().unwrap();
                panic!("intentional test-only poisoned failure metadata");
            }))
            .is_err()
        );
        assert_eq!(account.observation().0, ffi::Status::Failed);
        unsafe extern "C" fn failure_callback(
            context: *mut std::ffi::c_void,
            event: *const ffi::Event,
        ) {
            let event = unsafe { &*event };
            if event.kind == 0 && event.status == ffi::Status::Failed {
                unsafe { &*context.cast::<std::sync::atomic::AtomicUsize>() }
                    .fetch_add(1, Ordering::Release);
            }
        }
        let calls = std::sync::atomic::AtomicUsize::new(0);
        binding.failure(
            TransportFailure::Io(std::io::ErrorKind::BrokenPipe),
            Some(super::super::Callback {
                function: failure_callback,
                context: ptr::from_ref(&calls).cast_mut().cast(),
            }),
        );
        assert!(!binding.available.load(Ordering::Acquire));
        assert!(binding.live.load(Ordering::Acquire));
        assert_eq!(calls.load(Ordering::Acquire), 1);
    }

    #[test]
    fn cancelled_backend_tasks_are_reported_after_account_retirement() {
        for cancel_bridge in [false, true] {
            let runtime = Arc::new(
                tokio::runtime::Builder::new_multi_thread()
                    .worker_threads(1)
                    .enable_all()
                    .build()
                    .unwrap(),
            );
            let (account, directory) = active(runtime);
            drop(directory);
            {
                let state = account.0.state.lock().unwrap();
                let active = state.active.as_ref().unwrap();
                if cancel_bridge {
                    active.bridge.abort();
                } else {
                    crate::account::network::test_support::abort_owner(&active.network);
                }
            }
            assert_eq!(
                unsafe { super::super::account_stop(account.pointer()) },
                ffi::Status::Failed
            );
            assert!(account.0.state.lock().unwrap().active.is_none());
        }
    }

    #[test]
    fn observation_reports_real_control_queue_backpressure_without_waiting() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let (account, _directory) = active(runtime);
        let binding = account.0.binding().unwrap();
        binding.control.remove(1, 1).unwrap();
        binding.control.remove(2, 2).unwrap();
        assert_eq!(account.observation().0, ffi::Status::Full);
    }

    struct Category<'a>(&'a str);
    impl Debug for Category<'_> {
        fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
            output.write_str(self.0)
        }
    }

    #[test]
    fn categories_are_ascii_bounded_and_null_terminated() {
        for (error, code, prefix) in [
            (
                TransportFailure::Io(std::io::ErrorKind::BrokenPipe),
                1,
                b"Io(".as_slice(),
            ),
            (
                TransportFailure::Proxy(crate::transport::proxy::ProxyError::Closed),
                2,
                b"Proxy(".as_slice(),
            ),
        ] {
            assert_eq!(error.code(), code);
            assert!(detail(&error).starts_with(prefix));
        }
        let unusual = detail(&Category("A \n\t\u{7f}é"));
        assert_eq!(&unusual[..7], b"A ?????");
        assert!(unusual[7..].iter().all(|byte| *byte == 0));
        let maximum = detail(&Category(&"X".repeat(50)));
        assert_eq!(&maximum[..31], &[b'X'; 31]);
        assert_eq!(maximum[31], 0);
    }

    #[test]
    fn disabled_stopped_and_poisoned_accounts_have_explicit_observations() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let account = Account::new(Arc::clone(&runtime), None, None);
        let pointer = ptr::from_ref(&account).cast::<ffi::Account>();
        let mut result = ffi::AccountObservation {
            struct_size: size_of::<ffi::AccountObservation>() as u32,
            abi_version: 1,
            ..Default::default()
        };
        assert_eq!(
            unsafe { account_observe(pointer, &mut result) },
            ffi::Status::Ok
        );
        assert_eq!(
            (result.state, result.available, result.generation),
            (6, 0, 0)
        );
        assert_eq!(
            runtime.block_on(async { unsafe { account_observe(pointer, &mut result) } }),
            ffi::Status::Invalid
        );
        account.state.lock().unwrap().stopped = true;
        assert_eq!(
            unsafe { account_observe(pointer, &mut result) },
            ffi::Status::Ok
        );
        assert_eq!((result.state, result.available), (5, 0));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = account.state.lock().unwrap();
            panic!("intentional test-only poisoned account owner");
        }));
        assert!(poisoned.is_err());
        assert_eq!(
            unsafe { account_observe(pointer, &mut result) },
            ffi::Status::Failed
        );
        let peer = Account::new(Arc::clone(&runtime), None, None);
        let pointer = pointer.cast_mut();
        let other = ptr::from_ref(&peer).cast_mut().cast::<ffi::Account>();
        let disabled = ffi::AccountConfig::default();
        assert!(matches!(account.binding(), Err(ffi::Status::Failed)));
        assert_eq!(account.apply_settings(None), ffi::Status::Failed);
        unsafe {
            assert_eq!(
                super::super::account_activate(pointer, None, ptr::null_mut()),
                ffi::Status::Failed
            );
            assert_eq!(
                super::super::account_reconfigure(pointer, &disabled, ptr::null()),
                ffi::Status::Failed
            );
            assert_eq!(super::super::account_stop(pointer), ffi::Status::Failed);
            assert_eq!(
                super::super::account_apply_prepared(other, pointer),
                ffi::Status::Failed
            );
            assert_eq!(
                super::super::account_apply_prepared(pointer, other),
                ffi::Status::Failed
            );
        }
        account.state.clear_poison();
    }

    #[test]
    fn lifecycle_lock_failure_is_reported_before_mutating_account_state() {
        let runtime = Arc::new(
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        );
        let account = Account::new(Arc::clone(&runtime), None, None);
        let prepared = Account::new(runtime, None, None);
        let pointer = ptr::from_ref(&account).cast_mut().cast::<ffi::Account>();
        let other = ptr::from_ref(&prepared).cast_mut().cast::<ffi::Account>();
        assert!(
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _guard = account.operation.lock().unwrap();
                panic!("intentional test-only lifecycle lock failure");
            }))
            .is_err()
        );
        let disabled = ffi::AccountConfig::default();
        unsafe {
            assert_eq!(
                super::super::account_activate(pointer, None, ptr::null_mut()),
                ffi::Status::Failed
            );
            assert_eq!(
                super::super::account_reconfigure(pointer, &disabled, ptr::null()),
                ffi::Status::Failed
            );
            assert_eq!(super::super::account_stop(pointer), ffi::Status::Failed);
            assert_eq!(
                super::super::account_apply_prepared(pointer, other),
                ffi::Status::Failed
            );
            assert_eq!(
                super::super::account_apply_prepared(other, pointer),
                ffi::Status::Failed
            );
        }
        assert!(!account.state.lock().unwrap().activated);
        assert!(!account.state.lock().unwrap().stopped);
        account.operation.clear_poison();
    }
}
