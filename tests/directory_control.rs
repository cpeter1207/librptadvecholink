//! Fake monotonic time proves registration/refresh retry policy without network logins.

use rptadv_echolink::{
    account::directory::{DirectoryControl, DirectoryPolicy, DirectoryState, Operation},
    directory::DirectorySnapshot,
    transport::TransactionError,
};
use std::time::{Duration, Instant};

#[test]
fn directory_requests_follow_reference_login_logout_and_snapshot_framing() {
    use rptadv_echolink::account::directory::DirectoryLogin;
    let login = DirectoryLogin {
        callsign: "K1ABC-R",
        password: "not-a-real-password",
        description: "North site",
        email: "operator@example.invalid",
    };
    assert_eq!(
        login.request(&Operation::Register, 9, 3).unwrap(),
        b"lK1ABC-R\xac\xacnot-a-real-password\rONLINE1.00R(9: 3)\rNorth site\roperator@example.invalid\r"
    );
    assert_eq!(
        login.request(&Operation::Logout, 9, 3).unwrap(),
        b"lK1ABC-R\xac\xacnot-a-real-password\rOFF-V3.40\rNorth site\r"
    );
    let conference = DirectoryLogin {
        callsign: "*EXAMPLE*",
        ..login
    };
    assert!(
        conference
            .request(&Operation::Register, 23, 31)
            .unwrap()
            .windows(b"ONLINE1.00B(23:31)\rNor".len())
            .any(|part| part == b"ONLINE1.00B(23:31)\rNor")
    );
    assert_eq!(
        login
            .request(
                &Operation::Snapshot {
                    snapshot_id: "0".into()
                },
                9,
                3
            )
            .unwrap(),
        b"F0\r"
    );
    let token = "a".repeat(49);
    assert_eq!(
        login
            .request(
                &Operation::Snapshot {
                    snapshot_id: token.clone()
                },
                9,
                3
            )
            .unwrap(),
        format!("F{token}\r").as_bytes()
    );
    for bad in ["", "a\rb", "a\nb", &"a".repeat(50)] {
        assert!(
            login
                .request(
                    &Operation::Snapshot {
                        snapshot_id: bad.into()
                    },
                    9,
                    3
                )
                .is_err()
        );
    }
    for (hour, day) in [(24, 3), (9, 0), (9, 32)] {
        assert!(login.request(&Operation::Register, hour, day).is_err());
    }
    for callsign in ["", "K1ABC\r", "K1 ABC", "TOO-LONG-CALLSIGN"] {
        assert!(
            DirectoryLogin { callsign, ..login }
                .request(&Operation::Register, 9, 3)
                .is_err()
        );
    }
    for field in ["one\rtwo", "one\ntwo", "one\0two", &"a".repeat(4097)] {
        for changed in [
            DirectoryLogin {
                password: field,
                ..login
            },
            DirectoryLogin {
                description: field,
                ..login
            },
            DirectoryLogin {
                email: field,
                ..login
            },
        ] {
            assert!(changed.request(&Operation::Register, 9, 3).is_err());
        }
    }
}

#[test]
fn configured_directory_metadata_is_not_limited_by_asls_local_printf_buffer() {
    use rptadv_echolink::account::directory::DirectoryLogin;
    let text = "site ".repeat(400);
    let login = DirectoryLogin {
        callsign: "K1ABC-R",
        password: "fixture-password",
        description: &text,
        email: &text,
    };
    let request = login.request(&Operation::Register, 9, 3).unwrap();
    assert!(
        request
            .windows(text.len())
            .any(|field| field == text.as_bytes())
    );
    assert!(request.len() < 16_384);
}

#[test]
fn scheduled_directory_jobs_execute_against_a_framed_loopback_server() {
    use rptadv_echolink::{
        account::directory::DirectoryLogin,
        transport::{ReplyKind, directory_exchange},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let now = Instant::now();
            let login = DirectoryLogin {
                callsign: "K1ABC-R",
                password: "fixture-password",
                description: "Site",
                email: "",
            };
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut open = Vec::new();
                for (expected, reply) in [
                    (
                        b"lK1ABC-R\xac\xacfixture-password\rONLINE1.00R(9: 3)\rSite\r\r".as_slice(),
                        b"OK".as_slice(),
                    ),
                    (b"F0\r", b"@@@\n0:first\n+++\n"),
                    (b"Ffirst\r", b"DDD\n0:second\n+++\n+++\n"),
                    (
                        b"lK1ABC-R\xac\xacfixture-password\rOFF-V3.40\rSite\r",
                        b"OK",
                    ),
                ] {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let mut request = vec![0; expected.len()];
                    stream.read_exact(&mut request).await.unwrap();
                    assert_eq!(request, expected);
                    for chunk in reply.chunks(1) {
                        stream.write_all(chunk).await.unwrap();
                        tokio::task::yield_now().await;
                    }
                    // Keep the current socket open until the next transaction is accepted.
                    open.push(stream);
                }
            });
            let mut owner = DirectoryControl::new(vec!["127.0.0.1".into()], policy()).unwrap();
            let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
            for elapsed in [0, 0, 240] {
                let time = now + Duration::from_secs(elapsed);
                let job = owner.poll(&snapshot, time).unwrap().unwrap();
                let request = login.request(&job.operation, 9, 3).unwrap();
                let kind = if matches!(job.operation, Operation::Snapshot { .. }) {
                    ReplyKind::Directory
                } else {
                    ReplyKind::Login
                };
                let reply = directory_exchange(address, &request, kind, job.timeout).await;
                assert_eq!(
                    owner.finish(
                        job.id,
                        reply.as_deref().map_err(|error| *error),
                        &mut snapshot,
                        time
                    ),
                    None
                );
            }
            assert_eq!(snapshot.snapshot_id(), "second");
            let logout = owner.stop().unwrap().unwrap();
            let reply = directory_exchange(
                address,
                &login.request(&logout.operation, 9, 3).unwrap(),
                ReplyKind::Login,
                logout.timeout,
            )
            .await;
            assert_eq!(
                owner.finish(
                    logout.id,
                    reply.as_deref().map_err(|error| *error),
                    &mut snapshot,
                    now
                ),
                None
            );
            assert_eq!(owner.state(), DirectoryState::Stopped);
            assert!(
                owner
                    .poll(&snapshot, now + Duration::from_secs(900))
                    .unwrap()
                    .is_none()
            );
            assert!(owner.stop().unwrap().is_none());
            server.await.unwrap();
        });
}

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
