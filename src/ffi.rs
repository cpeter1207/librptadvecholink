//! Shared C-compatible records; implementation owners remain private to this DSO.

pub use rptadv_peer_support::echolink::abi::*;
use std::{ffi::c_void, ops::Deref};

/// The released descriptor prefix, retained without a source dependency cycle.
pub type ReleasedDescriptor = rptadv_peer_support::echolink::abi::Descriptor;
/// Optional actual egress operations follow the complete released prefix.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Descriptor {
    /// Released ABI1 table, whose struct_size describes this complete allocation.
    pub released: ReleasedDescriptor,
    /// Retire actual encoder/queued media; Pending clears token, Ok returns nonzero.
    pub peer_retire_egress: Option<unsafe extern "C" fn(*mut Qso, *mut u64) -> Status>,
    /// Retain a metadata-only observer before QSO destruction.
    pub peer_egress_observer:
        Option<unsafe extern "C" fn(*const Qso, *mut EgressObserver) -> Status>,
}
impl Deref for Descriptor {
    type Target = ReleasedDescriptor;
    fn deref(&self) -> &Self::Target {
        &self.released
    }
}
/// Metadata survives QSO destruction without retaining its I/O owners. Caller
/// keeps provider code loaded until release; context is released exactly once.
#[repr(C)]
pub struct EgressObserver {
    /// Complete readable/writable size,32bytes.
    pub struct_size: u32,
    /// Exact revision,one.
    pub abi_version: u32,
    /// Owned metadata reference, never a socket/runtime/QSO reference.
    pub context: *mut c_void,
    /// Zero while live/queued,one only after destroyed and no queued media;-1invalid.
    pub disposed: Option<unsafe extern "C" fn(*const c_void) -> i32>,
    /// Release this context once, including after QSO/account/runtime destruction.
    pub release: Option<unsafe extern "C" fn(*mut c_void)>,
}
