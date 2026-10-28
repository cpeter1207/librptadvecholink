//! Fake monotonic time proves registration/refresh retry policy without network logins.

use rptadv_echolink::{
    account::directory::{DirectoryControl, DirectoryPolicy, DirectoryState, Operation},
    directory::DirectorySnapshot,
    transport::TransactionError,
};
use std::time::{Duration, Instant};

fn policy() -> DirectoryPolicy {
    DirectoryPolicy {
        operation_timeout: Duration::from_secs(10),
        registration_refresh: Duration::from_secs(300),
        directory_refresh: Duration::from_secs(240),
        retry: Duration::from_secs(20),
    }
}

#[test]
fn ordered_server_failover_obeys_retry_deadlines_and_ignores_old_completions() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    let mut owner = DirectoryControl::new(
        vec![
            "first.example.invalid".into(),
            "second.example.invalid".into(),
        ],
        policy(),
    )
    .unwrap();
    let first = owner.poll(&snapshot, now).unwrap().unwrap();
    assert_eq!(first.server, "first.example.invalid");
    assert_eq!(first.operation, Operation::Register);
    assert_eq!(first.timeout, Duration::from_secs(10));
    assert!(owner.poll(&snapshot, now).unwrap().is_none());
    assert!(
        owner
            .finish(first.id, Err(TransactionError::Timeout), &mut snapshot, now)
            .is_some()
    );
    assert_eq!(owner.state(), DirectoryState::Retrying);
    assert!(
        owner
            .poll(&snapshot, now + Duration::from_secs(19))
            .unwrap()
            .is_none()
    );
    let retry = owner
        .poll(&snapshot, now + Duration::from_secs(20))
        .unwrap()
        .unwrap();
    assert_eq!(retry.server, "second.example.invalid");
    assert_eq!(retry.operation, Operation::Register);
    assert_ne!(retry.id, first.id);
    assert!(
        owner
            .finish(first.id, Ok(b"OK"), &mut snapshot, now)
            .is_none()
    );
    assert_eq!(owner.state(), DirectoryState::Registering);
    assert!(
        owner
            .finish(
                retry.id,
                Ok(b"OK"),
                &mut snapshot,
                now + Duration::from_secs(20)
            )
            .is_none()
    );
    assert_eq!(owner.state(), DirectoryState::Online);
    assert_eq!(owner.failures(), 1);
}

#[test]
fn registration_and_snapshot_refresh_keep_independent_intervals() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    let mut owner =
        DirectoryControl::new(vec!["directory.example.invalid".into()], policy()).unwrap();
    let login = owner.poll(&snapshot, now).unwrap().unwrap();
    owner.finish(login.id, Ok(b"OK"), &mut snapshot, now);
    let refresh = owner.poll(&snapshot, now).unwrap().unwrap();
    assert_eq!(
        refresh.operation,
        Operation::Snapshot {
            snapshot_id: "0".into()
        }
    );
    owner.finish(refresh.id, Ok(b"@@@\n0:first\n+++\n"), &mut snapshot, now);
    assert_eq!(snapshot.snapshot_id(), "first");
    assert!(
        owner
            .poll(&snapshot, now + Duration::from_secs(239))
            .unwrap()
            .is_none()
    );
    let refresh = owner
        .poll(&snapshot, now + Duration::from_secs(240))
        .unwrap()
        .unwrap();
    assert_eq!(
        refresh.operation,
        Operation::Snapshot {
            snapshot_id: "first".into()
        }
    );
    owner.finish(
        refresh.id,
        Ok(b"@@@\n0:second\n+++\n"),
        &mut snapshot,
        now + Duration::from_secs(240),
    );
    let login = owner
        .poll(&snapshot, now + Duration::from_secs(300))
        .unwrap()
        .unwrap();
    assert_eq!(login.operation, Operation::Register);
    owner.finish(
        login.id,
        Ok(b"OK"),
        &mut snapshot,
        now + Duration::from_secs(300),
    );
    assert!(
        owner
            .poll(&snapshot, now + Duration::from_secs(479))
            .unwrap()
            .is_none()
    );
    assert!(matches!(
        owner
            .poll(&snapshot, now + Duration::from_secs(480))
            .unwrap()
            .unwrap()
            .operation,
        Operation::Snapshot { .. }
    ));
}

#[test]
fn outage_retains_good_snapshot_and_live_calls() {
    use rptadv_echolink::session::{Packet, Qso, QsoAction, QsoEvent, QsoState, Timers};
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot
        .update(
            b"@@@\n1:good\nK1ABC-R\nNorth\n123456\n192.0.2.1\n+++\n",
            now,
        )
        .unwrap();
    let identity = snapshot.by_node(123456, now).unwrap();
    let address = identity.address;
    let mut call = Qso::new(
        identity,
        1,
        Timers {
            heartbeat: Duration::from_secs(10),
            liveness: Duration::from_secs(100),
            connect: Duration::from_secs(30),
        },
        now,
    )
    .unwrap();
    assert_eq!(
        call.handle(
            1,
            QsoEvent::Packet {
                address,
                packet: Packet::Sdes {
                    callsign: "K1ABC-R".into(),
                    source: 0
                }
            },
            now
        ),
        Some(QsoAction::Connected)
    );
    let mut owner =
        DirectoryControl::new(vec!["directory.example.invalid".into()], policy()).unwrap();
    let login = owner.poll(&snapshot, now).unwrap().unwrap();
    owner.finish(login.id, Ok(b"OK"), &mut snapshot, now);
    let refresh = owner.poll(&snapshot, now).unwrap().unwrap();
    assert!(
        owner
            .finish(refresh.id, Ok(b"DDD\n1:broken\n"), &mut snapshot, now)
            .is_some()
    );
    assert_eq!(snapshot.snapshot_id(), "good");
    assert_eq!(snapshot.by_node(123456, now).unwrap().callsign, "K1ABC-R");
    assert_eq!(call.state(), QsoState::Connected);
    assert_eq!(owner.state(), DirectoryState::Retrying);
}

#[test]
fn stop_invalidates_pending_work_and_never_schedules_a_reconnect() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    let mut owner =
        DirectoryControl::new(vec!["directory.example.invalid".into()], policy()).unwrap();
    let login = owner.poll(&snapshot, now).unwrap().unwrap();
    owner.finish(login.id, Ok(b"OK"), &mut snapshot, now);
    let refresh = owner.poll(&snapshot, now).unwrap().unwrap();
    let logout = owner.stop().unwrap().unwrap();
    assert_eq!(logout.operation, Operation::Logout);
    assert_eq!(owner.state(), DirectoryState::Stopping);
    owner.finish(refresh.id, Ok(b"@@@\n0:wrong\n+++\n"), &mut snapshot, now);
    assert_eq!(snapshot.snapshot_id(), "0");
    assert!(
        owner
            .finish(
                logout.id,
                Err(TransactionError::Timeout),
                &mut snapshot,
                now
            )
            .is_some()
    );
    assert_eq!(owner.state(), DirectoryState::Stopped);
    assert!(
        owner
            .poll(&snapshot, now + Duration::from_secs(900))
            .unwrap()
            .is_none()
    );
    assert!(owner.stop().unwrap().is_none());
    let mut never_online =
        DirectoryControl::new(vec!["directory.example.invalid".into()], policy()).unwrap();
    assert!(never_online.stop().unwrap().is_none());
}
