//! Sized optional retirement/metadata operations preserve the released prefix.
use rptadv_echolink::{
    ffi::{Descriptor, EgressObserver, Status},
    rptadv_echolink_descriptor_v1,
};
use std::{
    ffi::c_void,
    mem::{offset_of, size_of},
    ptr,
};

#[test]
fn egress_pair_is_optional_after_the_released_descriptor_and_null_owners_fail_closed() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    assert_eq!(
        offset_of!(Descriptor, peer_retire_egress),
        size_of::<rptadv_peer_support::echolink::abi::Descriptor>()
    );
    assert_eq!(
        size_of::<Descriptor>(),
        offset_of!(Descriptor, peer_egress_observer) + size_of::<usize>()
    );
    assert_eq!(size_of::<EgressObserver>(), 32);
    let old = unsafe { rptadv_peer_support::echolink::Api::open(&api.released) }.unwrap();
    assert!(old.prepared_media_available());
    let mut token = 99;
    assert_eq!(
        unsafe { api.peer_retire_egress.unwrap()(ptr::null_mut(), &mut token) },
        Status::Invalid
    );
    assert_eq!(token, 0);
    assert_eq!(
        unsafe { api.peer_retire_egress.unwrap()(ptr::null_mut(), ptr::null_mut()) },
        Status::Invalid
    );
    let mut observer = EgressObserver {
        struct_size: size_of::<EgressObserver>() as u32,
        abi_version: 1,
        context: ptr::dangling_mut::<c_void>(),
        disposed: None,
        release: None,
    };
    assert_eq!(
        unsafe { api.peer_egress_observer.unwrap()(ptr::null(), &mut observer) },
        Status::Invalid
    );
    assert!(observer.context.is_null());
    assert!(observer.disposed.is_none());
    assert!(observer.release.is_none());
    observer.abi_version = 2;
    assert_eq!(
        unsafe { api.peer_egress_observer.unwrap()(ptr::null(), &mut observer) },
        Status::Invalid
    );
    assert_eq!(
        unsafe { api.peer_egress_observer.unwrap()(ptr::null(), ptr::null_mut()) },
        Status::Invalid
    );
}

#[cfg(target_os = "linux")]
#[test]
fn physical_short_observer_records_never_read_or_write_an_unavailable_tail() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
    let mapping = unsafe {
        libc::mmap(
            ptr::null_mut(),
            page * 2,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    assert_ne!(mapping, libc::MAP_FAILED);
    let guard = unsafe { mapping.cast::<u8>().add(page) };
    assert_eq!(
        unsafe { libc::mprotect(guard.cast(), page, libc::PROT_NONE) },
        0
    );
    for count in [4, 8, 15, 16, 23, 24, 31, 32] {
        let record = unsafe { guard.sub(count) };
        unsafe {
            ptr::write_bytes(record, 0, count);
            record.cast::<u32>().write_unaligned(count as u32);
            if count >= 8 {
                record.add(4).cast::<u32>().write_unaligned(1);
            }
        }
        assert_eq!(
            unsafe { api.peer_egress_observer.unwrap()(ptr::null(), record.cast()) },
            Status::Invalid
        );
    }
    assert_eq!(unsafe { libc::munmap(mapping, page * 2) }, 0);
}
