//! Caller-visible lifecycle and fail-closed descriptor tests.
use rptadv_echolink::{
    ffi::{AccountConfig, ByteSpan, Descriptor, RuntimeConfig, Status},
    rptadv_echolink_descriptor_v1,
};
use std::{mem::size_of, ptr};

#[test]
fn descriptor_exposes_the_complete_sized_network_media_and_text_groups() {
    // SAFETY: descriptor storage has process lifetime.
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    assert_eq!(api.struct_size as usize, size_of::<Descriptor>());
    assert_eq!(api.abi_version, 1);
    assert_eq!(&api.capability[..19], b"rptadv.echolink.v1\0");
    assert_eq!(api.capabilities, 15);
    assert!(api.account_activate.is_some());
    assert!(api.account_apply_prepared.is_some());
    let client = unsafe { rptadv_peer_support::echolink::Api::open(api) }.unwrap();
    assert!(client.network_available());
    assert!(client.prepared_media_available());
    assert!(client.text_available());
}

#[test]
fn peer_operations_reject_missing_owners_and_invalid_output_prefixes() {
    use rptadv_echolink::ffi::{AccountObservation, PeerInfo, PeerObservation, ReadResult};
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let mut info = PeerInfo {
        struct_size: size_of::<PeerInfo>() as u32,
        abi_version: 1,
        node_number: 0,
        capabilities: 0,
        account_generation: 0,
        qso_generation: 0,
        callsign: ByteSpan::default(),
        local_node: ByteSpan::default(),
        source: ByteSpan::default(),
    };
    let mut peer = PeerObservation {
        struct_size: size_of::<PeerObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    let mut account = AccountObservation {
        struct_size: size_of::<AccountObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    let mut read = ReadResult::default();
    let mut encoded: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
    encoded.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
    let mut registration = ptr::dangling_mut();
    let mut qso = ptr::dangling_mut();
    let mut elapsed = 100;
    unsafe {
        for status in [
            api.peer_info.unwrap()(ptr::null(), &mut info),
            api.peer_info.unwrap()(ptr::null(), ptr::null_mut()),
            api.peer_observe.unwrap()(ptr::null(), &mut peer),
            api.peer_observe.unwrap()(ptr::null(), ptr::null_mut()),
            api.account_observe.unwrap()(ptr::null(), &mut account),
            api.account_observe.unwrap()(ptr::null(), ptr::null_mut()),
            api.peer_read.unwrap()(
                ptr::null_mut(),
                0,
                ptr::null(),
                None,
                ptr::null_mut(),
                &mut read,
            ),
            api.peer_prepare_audio.unwrap()(
                ptr::null_mut(),
                ptr::null(),
                0,
                ptr::null_mut(),
                0,
                0,
                ptr::from_mut(&mut encoded).cast(),
            ),
            api.peer_reset_encoder.unwrap()(ptr::null_mut()),
            api.peer_send_prepared.unwrap()(ptr::null_mut(), ByteSpan::default(), 0, 0),
            api.peer_send_text.unwrap()(ptr::null_mut(), ByteSpan::default(), ByteSpan::default()),
            api.peer_set_deadline.unwrap()(ptr::null_mut(), 0),
            api.peer_elapsed_ms.unwrap()(ptr::null(), &mut elapsed),
            api.peer_elapsed_ms.unwrap()(ptr::null(), ptr::null_mut()),
            api.peer_attach.unwrap()(
                ptr::null_mut(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
                &mut registration,
            ),
            api.peer_attach.unwrap()(
                ptr::null_mut(),
                ptr::null_mut(),
                None,
                ptr::null_mut(),
                ptr::null_mut(),
            ),
            api.dial.unwrap()(ptr::null_mut(), 0, &mut qso),
            api.dial.unwrap()(ptr::null_mut(), 1, &mut qso),
            api.dial.unwrap()(ptr::null_mut(), 1, ptr::null_mut()),
            api.validate_pending.unwrap()(ptr::null(), &mut info),
        ] {
            assert_eq!(status, Status::Invalid);
        }
        api.peer_stop.unwrap()(ptr::null_mut());
        api.peer_wake.unwrap()(ptr::null_mut());
        api.peer_detach.unwrap()(ptr::null_mut());
        api.peer_destroy.unwrap()(ptr::null_mut());
    }
    assert_eq!(elapsed, 0);
    assert!(registration.is_null() && qso.is_null());
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
fn runtime_resources_load_real_dsos_and_keep_the_legacy_prefix_usable() {
    let api = unsafe { &*rptadv_echolink_descriptor_v1() };
    let directory = std::env::var("RPTADV_CODEC_DIR").unwrap();
    let gsm = format!("{directory}/librptadv_codec_gsm.so.1");
    let speex = format!("{directory}/librptadv_codec_speex.so.1");
    let jitter = format!("{directory}/rpt_advanced/librptadv_jitter_adapter.so.1");
    let span = |value: &str| ByteSpan {
        data: value.as_ptr(),
        length: value.len(),
    };
    let complete = || RuntimeConfig {
        worker_threads: 1,
        gsm_library: span(&gsm),
        speex_library: span(&speex),
        jitter_library: span(&jitter),
        ..Default::default()
    };
    let mut config = complete();
    let create = api.context_create.unwrap();
    let mut output = ptr::null_mut();
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Ok);
    unsafe { api.context_destroy.unwrap()(output) };
    for (gsm_library, speex_library, jitter_library) in [
        (ByteSpan::default(), ByteSpan::default(), span(&jitter)),
        (span(&gsm), ByteSpan::default(), ByteSpan::default()),
        (
            ByteSpan {
                data: ptr::null(),
                length: 1,
            },
            span(&speex),
            span(&jitter),
        ),
        (
            ByteSpan {
                data: gsm.as_ptr(),
                length: 4097,
            },
            span(&speex),
            span(&jitter),
        ),
        (span("bad\0path"), span(&speex), span(&jitter)),
    ] {
        config = RuntimeConfig {
            gsm_library,
            speex_library,
            jitter_library,
            ..complete()
        };
        output = ptr::dangling_mut();
        assert_eq!(unsafe { create(&config, &mut output) }, Status::Invalid);
        assert!(output.is_null());
    }
    config = RuntimeConfig {
        speex_library: ByteSpan::default(),
        ..complete()
    };
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Ok);
    unsafe { api.context_destroy.unwrap()(output) };
    for library in ["/nonexistent/jitter.so", &gsm] {
        config = RuntimeConfig {
            jitter_library: span(library),
            ..complete()
        };
        output = ptr::dangling_mut();
        assert_eq!(unsafe { create(&config, &mut output) }, Status::Failed);
        assert!(output.is_null());
    }
    config = complete();
    config.gsm_library = ByteSpan::default();
    output = ptr::dangling_mut();
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Invalid);
    assert!(output.is_null());
    config.gsm_library = span("/nonexistent/fixture-gsm.so");
    assert_eq!(unsafe { create(&config, &mut output) }, Status::Failed);
    assert!(output.is_null());
    #[repr(C)]
    struct Legacy {
        size: u32,
        abi: u32,
        workers: u32,
    }
    let legacy = Legacy {
        size: 12,
        abi: 1,
        workers: 1,
    };
    assert_eq!(
        unsafe { create(ptr::from_ref(&legacy).cast(), &mut output) },
        Status::Ok
    );
    unsafe { api.context_destroy.unwrap()(output) };
}

#[test]
fn runtime_resource_exhaustion_cannot_publish_or_abort_a_context() {
    const CHILD: &str = "RPTADV_ECHO_TEST_RESOURCE_FAILURE";
    if let Ok(resource) = std::env::var(CHILD) {
        // Only this dedicated test child loses its resource allowance.
        // Root ignores RLIMIT_NPROC, so drop privileges before imposing it.
        let (kind, original) = unsafe {
            if resource == "threads" && libc::geteuid() == 0 {
                assert_eq!(libc::setgid(65534), 0);
                assert_eq!(libc::setuid(65534), 0);
            }
            let kind = if resource == "threads" {
                libc::RLIMIT_NPROC
            } else {
                libc::RLIMIT_NOFILE
            };
            let mut original = libc::rlimit {
                rlim_cur: 0,
                rlim_max: 0,
            };
            assert_eq!(libc::getrlimit(kind, &mut original), 0);
            let limit = libc::rlimit {
                rlim_cur: 0,
                rlim_max: original.rlim_max,
            };
            assert_eq!(libc::setrlimit(kind, &limit), 0);
            (kind, original)
        };
        let api = unsafe { &*rptadv_echolink_descriptor_v1() };
        let mut context = ptr::dangling_mut();
        let status = unsafe {
            api.context_create.unwrap()(
                &RuntimeConfig {
                    worker_threads: 1,
                    ..Default::default()
                },
                &mut context,
            )
        };
        // Restore the soft limit before diagnostics and LLVM's exit-time file write.
        assert_eq!(unsafe { libc::setrlimit(kind, &original) }, 0);
        assert_eq!(status, Status::Failed);
        assert!(context.is_null());
    } else {
        for resource in ["threads", "files"] {
            let mut command = std::process::Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "runtime_resource_exhaustion_cannot_publish_or_abort_a_context",
                    "--test-threads=1",
                ])
                .env(CHILD, resource);
            if let Some(profile) = std::env::var_os("LLVM_PROFILE_FILE") {
                use std::os::unix::fs::PermissionsExt;
                // The privilege-dropped child must still write its own coverage.
                // Keep the file in llvm-cov's raw-profile directory for collection.
                let profile = std::path::Path::new(&profile)
                    .parent()
                    .unwrap()
                    .join(format!(
                        "echo-runtime-{}-{resource}.profraw",
                        std::process::id()
                    ));
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&profile)
                    .unwrap();
                std::fs::set_permissions(&profile, std::fs::Permissions::from_mode(0o666)).unwrap();
                command.env("LLVM_PROFILE_FILE", profile);
            }
            let child = command.output().unwrap();
            assert!(
                child.status.success(),
                "{resource}: {}\n{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            );
        }
    }
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
#[test]
fn observer_ports_reject_invalid_handles_and_incomplete_records() {
    use rptadv_echolink::ffi::{AccountObservation, PeerObservation, Status};
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let mut account = AccountObservation {
        struct_size: std::mem::size_of::<AccountObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    let mut peer = PeerObservation {
        struct_size: std::mem::size_of::<PeerObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    assert_eq!(
        unsafe {
            api.account_observe.expect("account observer missing")(std::ptr::null(), &mut account)
        },
        Status::Invalid
    );
    assert_eq!(
        unsafe {
            api.peer_observe.expect("serial peer observer missing")(std::ptr::null(), &mut peer)
        },
        Status::Invalid
    );
}
