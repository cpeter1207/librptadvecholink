//! Real per-QSO queues prove bounded admission and isolate stalled consumers.

use rptadv_echolink::{
    transport::mailbox::{PushError, channel},
    wire::Channel,
};
use std::{net::SocketAddr, time::Instant};

#[test]
fn copied_ingress_observation_retains_byte_charges_without_keeping_a_route_open() {
    let (sender, mut receiver) = channel(7).unwrap();
    let observer = receiver.observer();
    let source = "192.0.2.1:5198".parse().unwrap();
    let now = Instant::now();
    for _ in 0..56 {
        sender
            .try_push(Channel::Audio, source, now, &[1, 2])
            .unwrap();
    }
    assert_eq!(
        sender.try_push(Channel::Audio, source, now, &[1]),
        Err(PushError::Full)
    );
    let record = observer.observe();
    assert_eq!(
        (
            record.queued_packets,
            record.capacity_packets,
            record.full_drops,
            record.retained_bytes
        ),
        (56, 64, 1, 112)
    );
    let borrowed = receiver.try_recv().unwrap();
    assert_eq!(observer.observe().queued_packets, 55);
    assert_eq!(
        observer.observe().retained_bytes,
        112,
        "dequeue does not release a borrowed packet"
    );
    drop(borrowed);
    assert_eq!(observer.observe().retained_bytes, 110);
    drop(sender);
    assert_eq!(
        observer.observe().capacity_packets,
        0,
        "observer must not keep routing admission open"
    );
    while receiver.try_recv().is_some() {}
    assert_eq!(observer.observe().retained_bytes, 0);
}

#[test]
fn control_survives_media_backpressure_without_reordering_admitted_packets() {
    let (sender, mut receiver) = channel(7).unwrap();
    let source: SocketAddr = "192.0.2.1:5198".parse().unwrap();
    let arrived = Instant::now();
    for number in 0..56 {
        sender
            .try_push(Channel::Audio, source, arrived, &[number])
            .unwrap();
    }
    assert_eq!(
        sender.try_push(Channel::Audio, source, arrived, &[56]),
        Err(PushError::Full)
    );
    for number in 56..64 {
        sender
            .try_push(Channel::Control, source, arrived, &[number])
            .unwrap();
    }
    assert_eq!(
        sender.try_push(Channel::Control, source, arrived, &[64]),
        Err(PushError::Full)
    );
    for number in 0..64 {
        let packet = receiver.try_recv().unwrap();
        assert_eq!(packet.payload(), &[number]);
        assert_eq!(packet.generation(), 7);
        assert_eq!(packet.arrived(), arrived);
        assert_eq!(packet.source(), source);
        assert_eq!(
            packet.class(),
            if number < 56 {
                Channel::Audio
            } else {
                Channel::Control
            }
        );
    }
    assert_eq!(sender.retained_bytes(), 0);
}

#[test]
fn byte_quota_protects_a_maximum_control_datagram_and_releases_on_drop() {
    let (sender, mut receiver) = channel(1).unwrap();
    let source = "192.0.2.1:5198".parse().unwrap();
    let arrived = Instant::now();
    let payload = vec![0; 65_507];
    for _ in 0..3 {
        sender
            .try_push(Channel::Audio, source, arrived, &payload)
            .unwrap();
    }
    assert_eq!(
        sender.try_push(Channel::Audio, source, arrived, &payload),
        Err(PushError::Full)
    );
    sender
        .try_push(Channel::Control, source, arrived, &payload)
        .unwrap();
    assert_eq!(sender.retained_bytes(), 262_028);
    assert_eq!(
        sender.try_push(Channel::Control, source, arrived, &[0; 117]),
        Err(PushError::Full)
    );
    assert_eq!(
        sender.try_push(Channel::Control, source, arrived, &[0; 65_508]),
        Err(PushError::Oversize)
    );
    let packet = receiver.try_recv().unwrap();
    assert_eq!(sender.retained_bytes(), 262_028); // Processing still owns its actual memory.
    drop(packet);
    assert_eq!(sender.retained_bytes(), 196_521);
    drop(receiver);
    assert_eq!(sender.retained_bytes(), 0);
    assert_eq!(
        sender.try_push(Channel::Control, source, arrived, &[0]),
        Err(PushError::Closed)
    );
}

#[test]
fn thousand_independent_queues_do_not_share_a_stalled_peers_budget() {
    let source = "192.0.2.1:5198".parse().unwrap();
    let arrived = Instant::now();
    let mut queues: Vec<_> = (1..=1000)
        .map(|generation| channel(generation).unwrap())
        .collect();
    for _ in 0..56 {
        queues[0]
            .0
            .try_push(Channel::Audio, source, arrived, &[0; 160])
            .unwrap();
    }
    assert_eq!(
        queues[0].0.try_push(Channel::Audio, source, arrived, &[0]),
        Err(PushError::Full)
    );
    for (sender, receiver) in queues.iter_mut().skip(1) {
        sender
            .try_push(Channel::Audio, source, arrived, &[1; 160])
            .unwrap();
        assert_eq!(receiver.try_recv().unwrap().payload(), &[1; 160]);
        assert_eq!(sender.retained_bytes(), 0);
    }
    assert_eq!(queues[0].0.retained_bytes(), 8960);
}

#[test]
fn close_admission_drains_existing_data_and_rejects_all_later_writes() {
    assert!(channel(0).is_none());
    let (sender, mut receiver) = channel(1).unwrap();
    let other = sender.clone();
    let source = "192.0.2.1:5199".parse().unwrap();
    let arrived = Instant::now();
    sender
        .try_push(Channel::Control, source, arrived, &[1])
        .unwrap();
    receiver.close();
    assert_eq!(
        other.try_push(Channel::Control, source, arrived, &[2]),
        Err(PushError::Closed)
    );
    assert_eq!(receiver.try_recv().unwrap().payload(), &[1]);
    assert!(receiver.try_recv().is_none());
    assert_eq!(sender.retained_bytes(), 0);
}

#[test]
fn concurrent_producers_cannot_overbook_or_wrap_the_byte_counts() {
    let (sender, mut receiver) = channel(1).unwrap();
    let source = "192.0.2.1:5198".parse().unwrap();
    let arrived = Instant::now();
    let accepted = std::thread::scope(|scope| {
        let workers: Vec<_> = (0..8)
            .map(|worker| {
                let sender = &sender;
                scope.spawn(move || {
                    let mut accepted = 0;
                    for _ in 0..40 {
                        let class = if worker % 2 == 0 {
                            Channel::Audio
                        } else {
                            Channel::Control
                        };
                        if sender.try_push(class, source, arrived, &[0; 8192]).is_ok() {
                            accepted += 1;
                        }
                        assert!(sender.retained_bytes() <= 262_144);
                    }
                    accepted
                })
            })
            .collect();
        workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .sum::<usize>()
    });
    assert_eq!(sender.retained_bytes(), accepted * 8192);
    let mut received = 0;
    while receiver.try_recv().is_some() {
        received += 1;
    }
    assert_eq!(received, accepted);
    assert_eq!(sender.retained_bytes(), 0);
}
