//! Versioned EchoLink provider boundary, independent of Asterisk and radio I/O.
#![deny(missing_docs)]

pub mod account;
pub mod directory;
pub mod ffi;
pub mod media;
mod owner;
pub mod playout;
pub mod receive;
mod resources;
pub mod session;
pub mod transport;
pub mod wire;
use ffi::{AccountConfig, AccountSecrets, Descriptor, RuntimeConfig, Status};
use owner::{
    Account, account_activate, account_destroy, account_reconfigure, account_stop, dial,
    peer_attach, peer_destroy, peer_detach, peer_elapsed_ms, peer_info, peer_prepare_audio,
    peer_read, peer_reset_encoder, peer_send_prepared, peer_send_text, peer_set_deadline,
    peer_stop, peer_wake, validate_pending,
};
use std::{mem::size_of, ptr, sync::Arc};

struct Context {
    runtime: Arc<tokio::runtime::Runtime>,
    resources: Option<Arc<resources::Resources>>,
}

#[repr(C)]
struct RuntimePrefix {
    struct_size: u32,
    abi_version: u32,
    worker_threads: u32,
}

static DESCRIPTOR: Descriptor = Descriptor {
    struct_size: size_of::<Descriptor>() as u32,
    abi_version: 1,
    capability: *b"rptadv.echolink.v1\0\0\0\0\0\0",
    capabilities: 1,
    context_create: Some(context_create),
    context_destroy: Some(context_destroy),
    account_prepare: Some(account_prepare),
    account_activate: Some(account_activate),
    account_reconfigure: Some(account_reconfigure),
    account_stop: Some(account_stop),
    account_destroy: Some(account_destroy),
    dial: Some(dial),
    validate_pending: Some(validate_pending),
    peer_info: Some(peer_info),
    peer_attach: Some(peer_attach),
    peer_stop: Some(peer_stop),
    peer_detach: Some(peer_detach),
    peer_wake: Some(peer_wake),
    peer_set_deadline: Some(peer_set_deadline),
    peer_destroy: Some(peer_destroy),
    peer_read: Some(peer_read),
    peer_prepare_audio: Some(peer_prepare_audio),
    peer_reset_encoder: Some(peer_reset_encoder),
    peer_send_prepared: Some(peer_send_prepared),
    peer_elapsed_ms: Some(peer_elapsed_ms),
    peer_send_text: Some(peer_send_text),
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
    output: *mut *mut ffi::Context,
) -> Status {
    // SAFETY: caller owns writable output when it is nonnull.
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    // SAFETY: configuration follows the sized-record contract.
    let Some(prefix) = (unsafe { record::<RuntimePrefix>(config.cast()) }) else {
        return Status::Invalid;
    };
    if prefix.abi_version != 1 || prefix.worker_threads > 256 {
        return Status::Invalid;
    }
    // Read optional resources only when the complete tail is actually present.
    let resources = if prefix.struct_size as usize >= size_of::<RuntimeConfig>() {
        match unsafe { resources::Resources::load(&*config) } {
            Ok(resources) => resources.map(Arc::new),
            Err(status) => return status,
        }
    } else {
        None
    };
    let workers = if prefix.worker_threads == 0 {
        std::thread::available_parallelism().map_or(1, |count| count.get().clamp(1, 4))
    } else {
        prefix.worker_threads as usize
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
        resources,
    }))
    .cast();
    Status::Ok
}

unsafe extern "C" fn context_destroy(context: *mut ffi::Context) {
    if !context.is_null() {
        // SAFETY: caller transfers its once-owned live context allocation.
        drop(unsafe { Box::from_raw(context.cast::<Context>()) });
    }
}

unsafe extern "C" fn account_prepare(
    context: *mut ffi::Context,
    config: *const AccountConfig,
    secrets: *const AccountSecrets,
    output: *mut *mut ffi::Account,
) -> Status {
    // SAFETY: caller owns writable output when it is nonnull.
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    // SAFETY: context is borrowed from its live owner for this call.
    let Some(context) = (unsafe { context.cast::<Context>().as_ref() }) else {
        return Status::Invalid;
    };
    // Preparation owns configuration and credentials but performs no socket/login
    // operation. Activation/network capability remains separate and unpublished.
    let settings = match unsafe { prepare_settings(config, secrets, context.resources.is_some()) } {
        Ok(settings) => settings,
        Err(status) => return status,
    };
    *output = Box::into_raw(Box::new(Account::new(
        Arc::clone(&context.runtime),
        context.resources.as_ref().map(Arc::clone),
        settings,
    )))
    .cast();
    Status::Ok
}

/// Copy a complete candidate before any account lifecycle or network side effect.
unsafe fn prepare_settings(
    config: *const AccountConfig,
    secrets: *const AccountSecrets,
    resources: bool,
) -> Result<Option<Arc<account::settings::PreparedSettings>>, Status> {
    let config = unsafe { record(config) }.ok_or(Status::Invalid)?;
    if config.abi_version != 1 || config.enabled > 1 {
        return Err(Status::Invalid);
    }
    if config.enabled == 0 {
        return Ok(None);
    }
    if !resources {
        return Err(Status::Unsupported);
    }
    let secrets = unsafe { record(secrets) }.ok_or(Status::Invalid)?;
    unsafe { account::settings::PreparedSettings::copy(config, secrets) }
        .map(|settings| Some(Arc::new(settings)))
}
