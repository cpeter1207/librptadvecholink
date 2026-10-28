//! Versioned EchoLink provider boundary, independent of Asterisk and radio I/O.
#![deny(missing_docs)]

pub mod account;
pub mod directory;
pub mod ffi;
pub mod session;
pub mod transport;
pub mod wire;
use ffi::{Account, AccountConfig, AccountSecrets, Context, Descriptor, RuntimeConfig, Status};
use std::{mem::size_of, ptr, sync::Arc};

static DESCRIPTOR: Descriptor = Descriptor {
    struct_size: size_of::<Descriptor>() as u32,
    abi_version: 1,
    capability: *b"rptadv.echolink.v1\0\0\0\0\0\0",
    capabilities: 1,
    context_create: Some(context_create),
    context_destroy: Some(context_destroy),
    account_prepare: Some(account_prepare),
    account_activate: None,
    account_reconfigure: None,
    account_stop: Some(account_stop),
    account_destroy: Some(account_destroy),
    dial: None,
    validate_pending: None,
    peer_info: None,
    peer_attach: None,
    peer_stop: None,
    peer_detach: None,
    peer_wake: None,
    peer_set_deadline: None,
};

/// Borrow the immutable ABI-1 table while the shared library remains loaded.
#[unsafe(no_mangle)]
pub extern "C" fn rptadv_echolink_descriptor_v1() -> *const Descriptor {
    &DESCRIPTOR
}

unsafe fn record<'a, T>(pointer: *const T) -> Option<&'a T> {
    // SAFETY: caller supplies at least the readable size prefix for a nonnull pointer.
    let size = unsafe { pointer.cast::<u32>().as_ref() }?;
    if (*size as usize) < size_of::<T>() {
        return None;
    }
    // SAFETY: caller's size prefix promises a complete initialized aligned record.
    unsafe { pointer.as_ref() }
}

unsafe extern "C" fn context_create(
    config: *const RuntimeConfig,
    output: *mut *mut Context,
) -> Status {
    // SAFETY: caller owns writable output when it is nonnull.
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    // SAFETY: configuration follows the sized-record contract.
    let Some(config) = (unsafe { record(config) }) else {
        return Status::Invalid;
    };
    if config.abi_version != 1 || config.worker_threads > 256 {
        return Status::Invalid;
    }
    let workers = if config.worker_threads == 0 {
        std::thread::available_parallelism().map_or(1, |count| count.get().clamp(1, 4))
    } else {
        config.worker_threads as usize
    };
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .thread_name("rpt-echo")
        .enable_all()
        .build();
    let Ok(runtime) = runtime else {
        return Status::Failed;
    };
    *output = Box::into_raw(Box::new(Context {
        runtime: Arc::new(runtime),
    }));
    Status::Ok
}

unsafe extern "C" fn context_destroy(context: *mut Context) {
    if !context.is_null() {
        // SAFETY: caller transfers its once-owned live context allocation.
        drop(unsafe { Box::from_raw(context) });
    }
}

unsafe extern "C" fn account_prepare(
    context: *mut Context,
    config: *const AccountConfig,
    _secrets: *const AccountSecrets,
    output: *mut *mut Account,
) -> Status {
    // SAFETY: caller owns writable output when it is nonnull.
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    // SAFETY: context is borrowed from its live owner for this call.
    let Some(context) = (unsafe { context.as_ref() }) else {
        return Status::Invalid;
    };
    // SAFETY: configuration follows the sized-record contract.
    let Some(config) = (unsafe { record(config) }) else {
        return Status::Invalid;
    };
    if config.abi_version != 1 || config.enabled > 1 {
        return Status::Invalid;
    }
    // Network capability is not published until the account/transport owner exists.
    if config.enabled != 0 {
        return Status::Unsupported;
    }
    *output = Box::into_raw(Box::new(Account {
        _runtime: Arc::clone(&context.runtime),
    }));
    Status::Ok
}

unsafe extern "C" fn account_stop(account: *mut Account) -> Status {
    if account.is_null() {
        Status::Invalid
    } else {
        Status::Ok
    }
}

unsafe extern "C" fn account_destroy(account: *mut Account) {
    if !account.is_null() {
        // SAFETY: caller transfers its once-owned account after callbacks have stopped.
        drop(unsafe { Box::from_raw(account) });
    }
}
