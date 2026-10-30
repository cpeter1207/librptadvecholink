//! Deterministic QSO liveness and generation tests; no sockets or sleeps.

use rptadv_echolink::{
    directory::DirectorySnapshot,
    session::{Cause, Packet, Qso, QsoAction, QsoEvent, QsoState, Timers},
};
use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

fn qso(now: Instant, generation: u64) -> Qso {
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory
        .update(b"@@@\n1\nK1ABC-R\nNorth\n123456\n192.0.2.1\n+++\n", now)
        .unwrap();
    Qso::new(
        directory.by_node(123456, now).unwrap(),
        generation,
        Timers {
            heartbeat: Duration::from_secs(10),
            liveness: Duration::from_secs(100),
            connect: Duration::from_secs(30),
        },
        now,
    )
    .unwrap()
}

fn packet(kind: Packet) -> QsoEvent {
    QsoEvent::Packet {
        address: Ipv4Addr::new(192, 0, 2, 1),
        packet: kind,
    }
}

fn establish(call: &mut Qso, generation: u64, now: Instant) {
    assert_eq!(
        call.handle(
            generation,
            packet(Packet::Sdes {
                callsign: "K1ABC-R".into(),
                source: 0
            }),
            now
        ),
        Some(QsoAction::Connected)
    );
    assert_eq!(call.state(), QsoState::Connected);
}

#[test]
fn timer_reload_preserves_elapsed_history_and_first_close_cause() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    let timers = Timers {
        heartbeat: Duration::from_secs(100),
        liveness: Duration::from_secs(20),
        connect: Duration::from_secs(5),
    };
    assert!(call.set_timers(timers));
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(5)));
    assert!(!call.set_timers(Timers {
        connect: Duration::ZERO,
        ..timers
    }));
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(5)));
    assert!(call.set_timers(Timers {
        connect: Duration::from_secs(60),
        ..timers
    }));
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(10)),
        None
    );
    establish(&mut call, 1, now + Duration::from_secs(10));
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(30)));
    assert!(call.set_timers(Timers {
        heartbeat: Duration::from_secs(12),
        ..timers
    }));
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(12)));
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(12)),
        Some(QsoAction::Heartbeat)
    );
    assert!(call.set_timers(Timers {
        liveness: Duration::from_secs(2),
        ..timers
    }));
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(12)),
        Some(QsoAction::Closing {
            cause: Cause::LivenessExpired,
            send_bye: true
        })
    );
    assert!(call.set_timers(timers));
    assert_eq!(call.deadline(), None);
    assert_eq!(call.first_cause(), Some(Cause::LivenessExpired));
}

#[test]
fn qso_owner_waits_for_heartbeat_or_expiry_and_parks_closed_generations() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(10)));
    for seconds in [10, 20] {
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(seconds));
    }
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(30)));
    establish(&mut call, 1, now + Duration::from_secs(25));
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(30)));
    for seconds in (30..=120).step_by(10) {
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(seconds));
    }
    assert_eq!(call.deadline(), Some(now + Duration::from_secs(125)));
    call.handle(1, QsoEvent::Poll, now + Duration::from_secs(125));
    assert_eq!(call.state(), QsoState::Closing);
    assert_eq!(call.deadline(), None);
    call.handle(1, QsoEvent::Drained, now + Duration::from_secs(126));
    assert_eq!(call.deadline(), None);
}

#[test]
fn receive_release_is_a_real_deadline_not_a_heartbeat_side_effect() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    establish(&mut call, 1, now);
    call.handle(1, packet(Packet::Voice { source: 0 }), now);
    assert_eq!(
        call.receive_deadline(now),
        Some(now + Duration::from_millis(320))
    );
    assert_eq!(
        call.receive_deadline(now + Duration::from_millis(320)),
        None
    );
    call.handle(1, QsoEvent::Stop, now + Duration::from_millis(321));
    assert_eq!(
        call.receive_deadline(now + Duration::from_millis(321)),
        None
    );
}

#[test]
fn delayed_valid_packets_cannot_rewind_media_or_liveness_deadlines() {
    let now = Instant::now();
    for old in [
        Packet::Report { source: 0 },
        Packet::Sdes {
            callsign: "K1ABC-R".into(),
            source: 0,
        },
        Packet::Voice { source: 0 },
    ] {
        let mut call = qso(now, 1);
        establish(&mut call, 1, now);
        assert_eq!(
            call.handle(
                1,
                packet(Packet::Voice { source: 0 }),
                now + Duration::from_secs(20)
            ),
            Some(QsoAction::Voice)
        );
        call.handle(1, packet(old), now + Duration::from_secs(10));
        assert!(call.raw_receiving(now + Duration::from_millis(20_100)));
        assert_eq!(
            call.handle(1, QsoEvent::Poll, now + Duration::from_secs(110)),
            Some(QsoAction::Heartbeat)
        );
        assert_eq!(call.state(), QsoState::Connected);
        assert_eq!(
            call.handle(1, QsoEvent::Poll, now + Duration::from_secs(120)),
            Some(QsoAction::Closing {
                cause: Cause::LivenessExpired,
                send_bye: true
            })
        );
    }
}

#[test]
fn heartbeat_silence_is_not_hangup() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    establish(&mut call, 1, now);
    for seconds in (10..=600).step_by(10) {
        let time = now + Duration::from_secs(seconds);
        assert_eq!(
            call.handle(1, packet(Packet::Report { source: 0 }), time),
            None
        );
        assert_eq!(
            call.handle(1, QsoEvent::Poll, time),
            Some(QsoAction::Heartbeat)
        );
    }
    assert_eq!(call.state(), QsoState::Connected);
    assert!(!call.raw_receiving(now + Duration::from_secs(600)));
}

#[test]
fn single_bad_packet_does_not_close_or_extend_liveness() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    establish(&mut call, 1, now);
    for kind in [Packet::Malformed, Packet::Chat, Packet::Unsupported] {
        assert_eq!(
            call.handle(1, packet(kind), now + Duration::from_secs(99)),
            None
        );
        assert_eq!(call.state(), QsoState::Connected);
    }
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(100)),
        Some(QsoAction::Closing {
            cause: Cause::LivenessExpired,
            send_bye: true
        })
    );
    assert_eq!(call.first_cause(), Some(Cause::LivenessExpired));
}

#[test]
fn first_terminal_cause_is_retained_through_drain_and_later_failures() {
    let now = Instant::now();
    for (event, cause, send_bye) in [
        (QsoEvent::Stop, Cause::LocalRequest, true),
        (packet(Packet::Bye { source: 0 }), Cause::RemoteBye, false),
        (QsoEvent::TransportLost, Cause::TransportLost, false),
        (QsoEvent::OwnerFailed, Cause::OwnerFailed, true),
    ] {
        let mut call = qso(now, 1);
        establish(&mut call, 1, now);
        assert_eq!(
            call.handle(1, event, now),
            Some(QsoAction::Closing { cause, send_bye })
        );
        assert_eq!(call.state(), QsoState::Closing);
        let closing = call.observe(now);
        assert_eq!(
            (closing.heartbeat_due_ms, closing.liveness_due_ms),
            (u64::MAX, u64::MAX)
        );
        assert_eq!(call.handle(1, QsoEvent::TransportLost, now), None);
        assert_eq!(call.handle(1, QsoEvent::Drained, now), None);
        assert_eq!(call.state(), QsoState::Closed);
        assert_eq!(call.first_cause(), Some(cause));
    }
}

#[test]
fn fresh_sdes_endpoint_and_generation_are_required_before_voice() {
    let now = Instant::now();
    let mut call = qso(now, 2);
    assert_eq!(call.generation(), 2);
    assert_eq!(
        call.handle(2, packet(Packet::Voice { source: 0 }), now),
        None
    );
    assert_eq!(
        call.handle(
            1,
            packet(Packet::Sdes {
                callsign: "K1ABC-R".into(),
                source: 0
            }),
            now
        ),
        None
    );
    assert_eq!(
        call.handle(
            2,
            packet(Packet::Sdes {
                callsign: "K9XYZ-R".into(),
                source: 0
            }),
            now
        ),
        None
    );
    assert_eq!(
        call.handle(
            2,
            QsoEvent::Packet {
                address: Ipv4Addr::new(192, 0, 2, 2),
                packet: Packet::Sdes {
                    callsign: "K1ABC-R".into(),
                    source: 0
                }
            },
            now
        ),
        None
    );
    assert_eq!(call.state(), QsoState::Connecting);
    establish(&mut call, 2, now);
    assert_eq!(
        call.handle(2, packet(Packet::Voice { source: 0 }), now),
        Some(QsoAction::Voice)
    );
    assert!(call.raw_receiving(now + Duration::from_millis(319)));
    assert!(!call.raw_receiving(now + Duration::from_millis(320)));
    assert_eq!(call.handle(1, packet(Packet::Bye { source: 0 }), now), None);
    assert_eq!(call.state(), QsoState::Connected);
    // Legacy identical wire BYEs cannot be distinguished after a valid new establishment.
    assert_eq!(
        call.handle(2, packet(Packet::Bye { source: 0 }), now),
        Some(QsoAction::Closing {
            cause: Cause::RemoteBye,
            send_bye: false
        })
    );
}

#[test]
fn available_identifiers_and_connect_deadlines_do_not_authenticate_early_audio() {
    let now = Instant::now();
    let mut call = qso(now, 1);
    assert_eq!(
        call.handle(
            1,
            packet(Packet::Report { source: 123 }),
            now + Duration::from_secs(29)
        ),
        None
    );
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(30)),
        Some(QsoAction::Closing {
            cause: Cause::ConnectExpired,
            send_bye: true
        })
    );
    let mut call = qso(now, 1);
    assert_eq!(
        call.handle(
            1,
            packet(Packet::Sdes {
                callsign: "K1ABC-R".into(),
                source: 123
            }),
            now
        ),
        Some(QsoAction::Connected)
    );
    assert_eq!(
        call.handle(1, packet(Packet::Voice { source: 456 }), now),
        Some(QsoAction::Voice)
    );
    assert_eq!(
        call.handle(
            1,
            packet(Packet::Voice { source: 789 }),
            now + Duration::from_millis(100)
        ),
        None
    );
    assert_eq!(
        call.handle(1, packet(Packet::Bye { source: 789 }), now),
        None
    );
    assert_eq!(call.state(), QsoState::Connected);
}

#[test]
fn setup_validation_and_foreign_control_sources_cannot_extend_a_call() {
    let now = Instant::now();
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory
        .update(b"@@@\n1\nK1ABC-R\nNorth\n123456\n192.0.2.1\n+++\n", now)
        .unwrap();
    let peer = directory.by_node(123456, now).unwrap();
    let valid = Timers {
        heartbeat: Duration::from_secs(10),
        liveness: Duration::from_secs(100),
        connect: Duration::from_secs(30),
    };
    assert!(Qso::new(peer.clone(), 0, valid, now).is_none());
    for timers in [
        Timers {
            heartbeat: Duration::ZERO,
            ..valid
        },
        Timers {
            liveness: Duration::ZERO,
            ..valid
        },
        Timers {
            connect: Duration::ZERO,
            ..valid
        },
    ] {
        assert!(Qso::new(peer.clone(), 1, timers, now).is_none());
    }
    let mut call = qso(now, 1);
    assert_eq!(call.observe(now).liveness_due_ms, 30_000);
    assert_eq!(call.handle(1, QsoEvent::Drained, now), None);
    assert_eq!(
        call.handle(1, packet(Packet::Bye { source: 123 }), now),
        None
    );
    assert_eq!(call.state(), QsoState::Connecting);
    call.handle(
        1,
        packet(Packet::Sdes {
            callsign: "K1ABC-R".into(),
            source: 123,
        }),
        now,
    );
    assert_eq!(call.handle(1, QsoEvent::Drained, now), None);
    for invalid in [
        Packet::Sdes {
            callsign: "K1ABC-R".into(),
            source: 789,
        },
        Packet::Report { source: 789 },
    ] {
        assert_eq!(
            call.handle(1, packet(invalid), now + Duration::from_secs(99)),
            None
        );
        assert_eq!(
            call.observe(now + Duration::from_secs(99))
                .last_valid_age_ms,
            99_000
        );
    }
    assert_eq!(
        call.handle(1, QsoEvent::Poll, now + Duration::from_secs(100)),
        Some(QsoAction::Closing {
            cause: Cause::LivenessExpired,
            send_bye: true,
        })
    );
    call.handle(1, QsoEvent::Drained, now + Duration::from_secs(100));
    assert_eq!(
        call.handle(
            1,
            packet(Packet::Voice { source: 123 }),
            now + Duration::from_secs(101)
        ),
        None
    );
    assert_eq!(call.state(), QsoState::Closed);
    assert_eq!(call.first_cause(), Some(Cause::LivenessExpired));
}
