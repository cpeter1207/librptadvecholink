use super::*;
use crate::{
    account::Account, account::coordination::Coordinator, directory::DirectoryEntry,
    transport::driver::Control, transport::routing::Router,
};
use libloading::Library;
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[test]
fn rejected_policy_does_not_change_existing_session_deadlines() {
    let root = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap());
    let catalog = CodecCatalog::load(&[ProviderSelection {
        path: root.join("librptadv_codec_gsm.so.1"),
        profiles: vec![ProfileId::GsmFr],
    }])
    .unwrap();
    let jitter =
        unsafe { Library::new(root.join("rpt_advanced/librptadv_jitter_adapter.so.1")) }.unwrap();
    let descriptor = unsafe {
        jitter
            .get::<unsafe extern "C" fn() -> *const std::ffi::c_void>(
                b"rptadv_jitter_adapter_descriptor_v1",
            )
            .unwrap()()
    };
    let now = Instant::now();
    let timers = crate::session::Timers {
        heartbeat: Duration::from_secs(10),
        liveness: Duration::from_secs(100),
        connect: Duration::from_secs(30),
    };
    let qso = Qso::new(
        DirectoryEntry {
            node_number: 1,
            callsign: "K1ABC-R".into(),
            description: "Test".into(),
            address: "192.0.2.1".parse().unwrap(),
        },
        1,
        timers,
        now,
    )
    .unwrap();
    let router = Router::new(
        Account::new(1, vec![], vec![]).unwrap(),
        1,
        Duration::from_secs(1),
    )
    .unwrap();
    let coordinator = Coordinator::new(router).unwrap();
    let port = coordinator.port(1, 1, Arc::new(Control::default()));
    let playout = unsafe { Playout::new(&catalog, Codec::GsmFr, 0.0, descriptor, now) }.unwrap();
    let mut receiver = Receiver::new(qso, playout, CodecChoice::new(&catalog).unwrap(), port);
    assert!(matches!(
        receiver.reconfigure(f64::NAN, timers),
        Err(MediaError::Invalid)
    ));
    let invalid = crate::session::Timers {
        heartbeat: Duration::ZERO,
        ..timers
    };
    assert!(matches!(
        receiver.reconfigure(0.0, invalid),
        Err(MediaError::Invalid)
    ));
    assert_eq!(
        receiver.observe(now).unwrap().session.liveness_due_ms,
        30_000
    );
    let changed = crate::session::Timers {
        connect: Duration::from_secs(60),
        ..timers
    };
    receiver.reconfigure(-6.0, changed).unwrap();
    assert_eq!(
        receiver.observe(now).unwrap().session.liveness_due_ms,
        60_000
    );
}
