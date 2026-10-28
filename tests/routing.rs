//! Directory-verified routing and bounded setup, without public directory traffic.
use rptadv_echolink::{
    account::{Account, Admission},
    directory::DirectorySnapshot,
    transport::routing::{RouteResult, Router},
    wire::{Channel, Codec, ControlPacket, VoicePacket, write_control, write_voice},
};
use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

fn directory(now: Instant) -> DirectorySnapshot {
    let mut bytes = b"@@@\n32\n".to_vec();
    for node in 1..=32 {
        bytes.extend(format!("K{node}ABC-R\nStation\n{node}\n192.0.2.{node}\n").bytes());
    }
    bytes.extend(b"+++\n");
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(&bytes, now).unwrap();
    snapshot
}

fn router() -> Router {
    Router::new(
        Account::new(20, vec![], vec![]).unwrap(),
        2,
        Duration::from_secs(30),
    )
    .unwrap()
}

fn sdes(node: u32) -> Vec<u8> {
    let mut output = vec![0; 1024];
    let count = write_control(
        &ControlPacket::Sdes {
            source: node,
            callsign: format!("K{node}ABC-R"),
            station_name: "Station".into(),
            speex: false,
        },
        &mut output,
    )
    .unwrap();
    output.truncate(count);
    output
}

fn endpoint(node: u32, port: u16) -> SocketAddr {
    format!("192.0.2.{node}:{port}").parse().unwrap()
}

fn voice() -> Vec<u8> {
    let mut output = vec![0; 1024];
    let count = write_voice(
        &VoicePacket {
            codec: Codec::GsmFr,
            marker: false,
            sequence: 1,
            timestamp: 0,
            ssrc: 0,
            payload: &[0xd0; 132],
        },
        &mut output,
    )
    .unwrap();
    output.truncate(count);
    output
}

#[test]
fn verified_control_can_alternate_source_ports_without_losing_its_session() {
    let now = Instant::now();
    let directory = directory(now);
    let mut router = router();
    let mut peer = router.dial(&directory, 1, now).unwrap();
    // Issue 826 reports the same named station using both 5198 and 5199 for RTCP.
    for port in [5198, 5199, 5198] {
        assert!(matches!(
            router.receive(
                &directory,
                Channel::Control,
                endpoint(1, port),
                now,
                &sdes(1)
            ),
            RouteResult::Routed
        ));
        assert_eq!(peer.ingress.try_recv().unwrap().source(), endpoint(1, port));
    }
    for control in [
        ControlPacket::Report { source: 1 },
        ControlPacket::Bye {
            source: 1,
            reason: vec![],
        },
    ] {
        let mut bytes = [0; 1024];
        let length = write_control(&control, &mut bytes).unwrap();
        for port in [5199, 5198] {
            assert!(
                matches!(
                    router.receive(
                        &directory,
                        Channel::Control,
                        endpoint(1, port),
                        now,
                        &bytes[..length]
                    ),
                    RouteResult::Routed
                ),
                "control={control:?} port={port} bytes={:?}",
                &bytes[..length]
            );
            assert_eq!(peer.ingress.try_recv().unwrap().source(), endpoint(1, port));
        }
    }
}

#[test]
fn unknown_flood_retains_only_bounded_verified_handshakes() {
    let now = Instant::now();
    let directory = directory(now);
    let mut router = router();
    let mut first = None;
    for node in 1..=32 {
        let result = router.receive(
            &directory,
            Channel::Control,
            endpoint(node, 5199),
            now,
            &sdes(node),
        );
        match result {
            RouteResult::Pending(pending) => {
                assert!(node <= 2);
                first.get_or_insert(pending);
            }
            RouteResult::Dropped => assert!(node > 2),
            _ => panic!("unknown station was published"),
        }
    }
    assert_eq!(router.pending_count(), 2);
    // Repeats neither create another pending event nor prolong this setup deadline.
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(1, 5199),
            now + Duration::from_secs(29),
            &sdes(1)
        ),
        RouteResult::Dropped
    ));
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(3, 5199),
            now + Duration::from_secs(30),
            &sdes(3)
        ),
        RouteResult::Pending(_)
    ));
    assert_eq!(router.pending_count(), 1);
    assert!(matches!(
        router.accept(
            &directory,
            first.unwrap().token(),
            now + Duration::from_secs(30)
        ),
        Err(Admission::Unverified)
    ));
}

#[test]
fn fresh_sdes_identity_and_real_channel_bind_before_media() {
    let now = Instant::now();
    let directory = directory(now);
    let mut router = router();
    let mut peer = router.dial(&directory, 1, now).unwrap();
    assert!(matches!(
        router.receive(&directory, Channel::Audio, endpoint(1, 5198), now, &voice()),
        RouteResult::Dropped
    ));
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(2, 5198),
            now,
            &sdes(1)
        ),
        RouteResult::Dropped
    ));
    assert!(peer.ingress.try_recv().is_none());
    // A control SDES from 5198 is valid on the account's control channel.
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(1, 5198),
            now,
            &sdes(1)
        ),
        RouteResult::Routed
    ));
    let packet = peer.ingress.try_recv().unwrap();
    assert_eq!(packet.source(), endpoint(1, 5198));
    assert_eq!(packet.class(), Channel::Control);
    assert_eq!(packet.generation(), peer.generation);
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Audio,
            endpoint(1, 36000),
            now,
            &voice()
        ),
        RouteResult::Routed
    ));
    assert_eq!(peer.ingress.try_recv().unwrap().payload(), voice());
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Audio,
            endpoint(1, 36001),
            now,
            &voice()
        ),
        RouteResult::Dropped
    ));
}

#[test]
fn accepted_handshake_rechecks_policy_and_retains_original_arrival() {
    let now = Instant::now();
    let directory = directory(now);
    let mut router = router();
    let RouteResult::Pending(pending) = router.receive(
        &directory,
        Channel::Control,
        endpoint(1, 4000),
        now,
        &sdes(1),
    ) else {
        panic!("missing pending identity")
    };
    let peer = router
        .accept(&directory, pending.token(), now + Duration::from_secs(1))
        .unwrap();
    assert_eq!(peer.initial.unwrap().arrived, now);
    assert_eq!(peer.identity.node_number, 1);
    assert_eq!(router.pending_count(), 0);
    assert!(matches!(
        router.accept(&directory, pending.token(), now),
        Err(Admission::Unverified)
    ));
    let mut denied = Router::new(
        Account::new(20, vec![], vec!["K1*".into()]).unwrap(),
        2,
        Duration::from_secs(30),
    )
    .unwrap();
    let RouteResult::Pending(pending) = denied.receive(
        &directory,
        Channel::Control,
        endpoint(1, 4000),
        now,
        &sdes(1),
    ) else {
        panic!("missing pending identity")
    };
    assert!(matches!(
        denied.accept(&directory, pending.token(), now),
        Err(Admission::Denied)
    ));
    assert_eq!(denied.pending_count(), 0);
}

#[test]
fn stalled_peer_queue_does_not_block_routing_or_remove_a_replacement() {
    let now = Instant::now();
    let directory = directory(now);
    let mut router = router();
    let mut old = router.dial(&directory, 1, now).unwrap();
    let mut other = router.dial(&directory, 2, now).unwrap();
    for node in [1, 2] {
        assert!(matches!(
            router.receive(
                &directory,
                Channel::Control,
                endpoint(node, 5199),
                now,
                &sdes(node)
            ),
            RouteResult::Routed
        ));
    }
    for _ in 0..100 {
        router.receive(&directory, Channel::Audio, endpoint(1, 5198), now, &voice());
    }
    assert!(matches!(
        router.receive(&directory, Channel::Audio, endpoint(2, 5198), now, &voice()),
        RouteResult::Routed
    ));
    other.ingress.try_recv().unwrap();
    assert_eq!(other.ingress.try_recv().unwrap().payload(), voice());
    router.remove(1, old.generation);
    let mut replacement = router.dial(&directory, 1, now).unwrap();
    assert_ne!(replacement.generation, old.generation);
    router.remove(1, old.generation);
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(1, 5199),
            now,
            &sdes(1)
        ),
        RouteResult::Routed
    ));
    assert_eq!(
        replacement.ingress.try_recv().unwrap().generation(),
        replacement.generation
    );
    assert!(old.ingress.try_recv().is_some());
}

#[test]
fn shared_ip_is_not_itself_a_unique_station_identity() {
    let now = Instant::now();
    let mut directory = directory(now);
    directory
        .update(
            b"@@@\n2\nK1ABC-R\nA\n1\n192.0.2.1\nK2ABC-R\nB\n2\n192.0.2.1\n+++\n",
            now,
        )
        .unwrap();
    let mut router = router();
    let mut first = router.dial(&directory, 1, now).unwrap();
    let mut second = router.dial(&directory, 2, now).unwrap();
    for node in [1, 2] {
        assert!(matches!(
            router.receive(
                &directory,
                Channel::Control,
                endpoint(1, 5199),
                now,
                &sdes(node)
            ),
            RouteResult::Routed
        ));
    }
    assert_eq!(first.ingress.try_recv().unwrap().payload(), sdes(1));
    assert_eq!(second.ingress.try_recv().unwrap().payload(), sdes(2));
    // Both have zero/unknown voice SSRC. Never duplicate or guess the media owner.
    assert!(matches!(
        router.receive(&directory, Channel::Audio, endpoint(1, 5198), now, &voice()),
        RouteResult::Dropped
    ));
    assert!(first.ingress.try_recv().is_none());
    assert!(second.ingress.try_recv().is_none());
    // A changed control port and zero SSRC cannot identify either shared-IP peer.
    let mut control = [0; 1024];
    let count = write_control(&ControlPacket::Report { source: 0 }, &mut control).unwrap();
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            endpoint(1, 5198),
            now,
            &control[..count]
        ),
        RouteResult::Dropped
    ));
    assert!(first.ingress.try_recv().is_none());
    assert!(second.ingress.try_recv().is_none());
}
