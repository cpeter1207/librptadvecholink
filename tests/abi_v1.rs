//! Caller-visible lifecycle and fail-closed descriptor tests.
use rptadv_echolink::{
    ffi::{AccountConfig, Descriptor, RuntimeConfig, Status},
    rptadv_echolink_descriptor_v1,
};
use std::{mem::size_of, ptr};

#[test]
fn descriptor_exposes_a_sized_versioned_lifecycle() {
    // SAFETY: descriptor storage has process lifetime.
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    assert_eq!(api.struct_size as usize, size_of::<Descriptor>());
    assert_eq!(api.abi_version, 1);
    assert_eq!(&api.capability[..19], b"rptadv.echolink.v1\0");
    assert_eq!(api.capabilities, 1); // lifecycle only until network work is implemented.
    assert!(api.account_activate.is_none());
}

#[test]
fn context_validation_clears_failed_outputs() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let create = api.context_create.unwrap();
    let mut output = ptr::dangling_mut();
    assert_eq!(unsafe { create(ptr::null(), &mut output) }, Status::Invalid);
    assert!(output.is_null());
    let mut config = RuntimeConfig {
        struct_size: 0,
        ..RuntimeConfig::default()
    };
    output = ptr::dangling_mut();
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Invalid);
    assert!(output.is_null());
    config = RuntimeConfig::default();
    config.abi_version = 99;
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Invalid);
    assert_eq!(
        unsafe { create(&RuntimeConfig::default(), ptr::null_mut()) },
        Status::Invalid
    );
    config = RuntimeConfig {
        worker_threads: 257,
        ..RuntimeConfig::default()
    };
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Invalid);
    config = RuntimeConfig {
        worker_threads: 1,
        ..RuntimeConfig::default()
    };
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Ok);
    unsafe { api.context_destroy.unwrap()(output) };
}

#[test]
fn disabled_accounts_have_no_network_side_effects_and_retain_context() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let mut context = ptr::null_mut();
    let config = RuntimeConfig::default();
    assert_eq!(
        unsafe { api.context_create.unwrap()(&config, &mut context) },
        Status::Ok
    );
    let mut account = ptr::null_mut();
    assert_eq!(
        unsafe {
            api.account_prepare.unwrap()(
                context,
                &AccountConfig::default(),
                ptr::null(),
                &mut account,
            )
        },
        Status::Ok
    );
    assert!(!account.is_null());
    unsafe { api.context_destroy.unwrap()(context) };
    assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
    assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
    unsafe { api.account_destroy.unwrap()(account) };
    unsafe { api.account_destroy.unwrap()(ptr::null_mut()) };
    unsafe { api.context_destroy.unwrap()(ptr::null_mut()) };
}

#[test]
fn unsupported_enabled_accounts_and_invalid_records_fail_closed() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let mut context = ptr::null_mut();
    assert_eq!(
        unsafe { api.context_create.unwrap()(&RuntimeConfig::default(), &mut context) },
        Status::Ok
    );
    let mut output = ptr::dangling_mut();
    let prepare = api.account_prepare.unwrap();
    let mut settings = AccountConfig {
        enabled: 1,
        ..AccountConfig::default()
    };
    assert_eq!(
        unsafe { prepare(context, &settings, ptr::null(), &mut output) },
        Status::Unsupported
    );
    assert!(output.is_null());
    settings.struct_size = 0;
    assert_eq!(
        unsafe { prepare(context, &settings, ptr::null(), &mut output) },
        Status::Invalid
    );
    settings = AccountConfig {
        abi_version: 99,
        ..AccountConfig::default()
    };
    assert_eq!(
        unsafe { prepare(context, &settings, ptr::null(), &mut output) },
        Status::Invalid
    );
    settings = AccountConfig {
        enabled: 2,
        ..AccountConfig::default()
    };
    assert_eq!(
        unsafe { prepare(context, &settings, ptr::null(), &mut output) },
        Status::Invalid
    );
    assert_eq!(
        unsafe {
            prepare(
                ptr::null_mut(),
                &AccountConfig::default(),
                ptr::null(),
                &mut output,
            )
        },
        Status::Invalid
    );
    assert_eq!(
        unsafe { prepare(context, ptr::null(), ptr::null(), &mut output) },
        Status::Invalid
    );
    assert_eq!(
        unsafe {
            prepare(
                context,
                &AccountConfig::default(),
                ptr::null(),
                ptr::null_mut(),
            )
        },
        Status::Invalid
    );
    assert_eq!(
        unsafe { api.account_stop.unwrap()(ptr::null_mut()) },
        Status::Invalid
    );
    unsafe { api.context_destroy.unwrap()(context) };
}
