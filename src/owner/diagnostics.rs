//! Metadata copies on their existing owners, never on a radio callback.
use super::{Account, TransportFailure, ffi, network_status};
use crate::account::directory::DirectoryState;
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
            result.state = match observed.directory.state {
                DirectoryState::Registering => 1,
                DirectoryState::Online => 2,
                DirectoryState::Retrying => 3,
                DirectoryState::Stopping => 4,
                DirectoryState::Stopped => 5,
            };
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
    use std::{fmt, sync::Arc};

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
    }
}
