//! Real shared Tokio tasks prove independent, bounded, callback-safe peer ownership.

use rptadv_echolink::{
    transport::{
        driver::{Control, PeerDriver, PeerEvent, PeerWait},
        mailbox::channel,
    },
    wire::Channel,
};
use std::{
    collections::HashSet,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};

fn runtime() -> Arc<tokio::runtime::Runtime> {
    Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap(),
    )
}

#[test]
fn pending_ingress_parks_instead_of_spinning_and_wake_resumes_it() {
    let runtime = runtime();
    let (sender, receiver) = channel(1).unwrap();
    for _ in 0..40 {
        sender
            .try_push(
                Channel::Audio,
                "192.0.2.1:5198".parse().unwrap(),
                Instant::now(),
                &[1],
            )
            .unwrap();
    }
    let control = Arc::new(Control::default());
    let allowed = Arc::new(AtomicBool::new(false));
    let calls = Arc::new(AtomicUsize::new(0));
    let packets = Arc::new(AtomicUsize::new(0));
    let observed = (
        Arc::clone(&allowed),
        Arc::clone(&calls),
        Arc::clone(&packets),
    );
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::clone(&control),
        receiver,
        move |event| {
            observed.1.fetch_add(1, Ordering::Relaxed);
            if let PeerEvent::Packet(packet) = event {
                assert_eq!(packet.payload(), &[1]);
                observed.2.fetch_add(1, Ordering::Release);
            }
            Some(PeerWait {
                receive: observed.0.load(Ordering::Acquire),
                ..PeerWait::default()
            })
        },
    );
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), async {
            while calls.load(Ordering::Acquire) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert_eq!(calls.load(Ordering::Acquire), 1);
        assert_eq!(sender.retained_bytes(), 40);
        allowed.store(true, Ordering::Release);
        control.wake();
        tokio::time::timeout(Duration::from_secs(2), async {
            while packets.load(Ordering::Acquire) != 40 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    });
    driver.detach();
    assert_eq!(sender.retained_bytes(), 0);
}

#[test]
fn deadline_progresses_without_incoming_packets() {
    let runtime = runtime();
    let (_sender, receiver) = channel(1).unwrap();
    let (done, finished) = tokio::sync::oneshot::channel();
    let mut done = Some(done);
    let deadline = Instant::now() + Duration::from_millis(20);
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::new(Control::default()),
        receiver,
        move |event| {
            if matches!(event, PeerEvent::Deadline) {
                done.take().unwrap().send(()).unwrap();
                return None;
            }
            Some(PeerWait {
                deadline: Some(deadline),
                ..PeerWait::default()
            })
        },
    );
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(2), finished)
            .await
            .unwrap()
            .unwrap();
    });
    driver.detach();
}

#[test]
fn reentrant_graceful_close_keeps_the_owner_until_its_drain_deadline() {
    let runtime = runtime();
    let (_sender, receiver) = channel(1).unwrap();
    let control = Arc::new(Control::default());
    let close = Arc::clone(&control);
    let (done, finished) = std::sync::mpsc::channel();
    let mut deadline = None;
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::clone(&control),
        receiver,
        move |event| {
            match event {
                PeerEvent::Wake => close.request_close(),
                PeerEvent::Close => {
                    assert!(deadline.is_none(), "one coalesced graceful close");
                    deadline = Some(Instant::now() + Duration::from_millis(20));
                }
                PeerEvent::Deadline => {
                    assert!(deadline.is_some());
                    done.send(()).unwrap();
                    return None;
                }
                _ => panic!("unexpected fixture event"),
            }
            Some(PeerWait {
                receive: false,
                deadline,
                ..PeerWait::default()
            })
        },
    );
    finished.recv_timeout(Duration::from_secs(2)).unwrap();
    assert!(
        !control.is_stopped(),
        "graceful close is not a teardown barrier"
    );
    driver.detach();
    assert!(control.is_stopped());
}

#[test]
fn callback_can_stop_before_attach_returns_and_detach_releases_captured_state() {
    struct Retained(Arc<AtomicBool>);
    impl Drop for Retained {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Release);
        }
    }
    let runtime = runtime();
    let (_sender, receiver) = channel(1).unwrap();
    let control = Arc::new(Control::default());
    let stop = Arc::clone(&control);
    let dropped = Arc::new(AtomicBool::new(false));
    let retained = Retained(Arc::clone(&dropped));
    let (called, callback_finished) = std::sync::mpsc::channel();
    let driver = PeerDriver::attach(Arc::clone(&runtime), control, receiver, move |_| {
        let _keep = &retained;
        stop.stop();
        called.send(()).unwrap();
        Some(PeerWait::default())
    });
    callback_finished
        .recv_timeout(Duration::from_secs(2))
        .unwrap();
    drop(runtime); // The driver must retain the pool until final quiescence.
    driver.detach();
    assert!(dropped.load(Ordering::Acquire));
}

#[test]
fn a_stopped_owner_never_calls_back_and_a_closed_ingress_is_reported_once() {
    let runtime = runtime();
    let control = Arc::new(Control::default());
    control.stop();
    let (_sender, receiver) = channel(1).unwrap();
    let driver = PeerDriver::attach(Arc::clone(&runtime), control, receiver, |_| {
        panic!("an already stopped owner must not call its consumer");
    });
    driver.detach();

    let (sender, receiver) = channel(2).unwrap();
    drop(sender);
    let (seen, events) = std::sync::mpsc::channel();
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::new(Control::default()),
        receiver,
        move |event| {
            if let PeerEvent::Io(kind) = event {
                seen.send(kind).unwrap();
                // Leave the owner open: it must park rather than repeatedly
                // reporting the same closed receive queue.
            }
            Some(PeerWait::default())
        },
    );
    assert_eq!(
        events.recv_timeout(Duration::from_secs(2)).unwrap(),
        std::io::ErrorKind::BrokenPipe
    );
    assert!(events.recv_timeout(Duration::from_millis(30)).is_err());
    driver.detach();
}

#[test]
fn thousand_peer_tasks_share_fixed_workers_and_isolate_a_stalled_connection() {
    let runtime = runtime();
    let processed = Arc::new(AtomicUsize::new(0));
    let threads = Arc::new(Mutex::new(HashSet::new()));
    let notify = Arc::new(tokio::sync::Notify::new());
    let mut drivers = Vec::new();
    let mut senders = Vec::new();
    for generation in 1..=1000 {
        let (sender, receiver) = channel(generation).unwrap();
        let observed = (
            Arc::clone(&processed),
            Arc::clone(&threads),
            Arc::clone(&notify),
        );
        let driver = PeerDriver::attach(
            Arc::clone(&runtime),
            Arc::new(Control::default()),
            receiver,
            move |event| {
                observed
                    .1
                    .lock()
                    .unwrap()
                    .insert(std::thread::current().id());
                if let PeerEvent::Packet(packet) = event {
                    assert_eq!(packet.generation(), generation);
                    assert_eq!(packet.payload(), &[1; 160]);
                    observed.0.fetch_add(1, Ordering::Release);
                    observed.2.notify_one();
                    return None;
                }
                Some(PeerWait {
                    receive: generation != 1,
                    ..PeerWait::default()
                })
            },
        );
        sender
            .try_push(
                Channel::Audio,
                "192.0.2.1:5198".parse().unwrap(),
                Instant::now(),
                &[1; 160],
            )
            .unwrap();
        senders.push(sender);
        drivers.push(driver);
    }
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            while processed.load(Ordering::Acquire) != 999 {
                notify.notified().await;
            }
        })
        .await
        .unwrap();
    });
    assert!(threads.lock().unwrap().len() <= 2);
    assert_eq!(senders[0].retained_bytes(), 160);
    for driver in drivers {
        driver.detach();
    }
    assert!(senders.iter().all(|sender| sender.retained_bytes() == 0));
}

#[test]
fn writable_readiness_resumes_the_same_prepared_packet() {
    use rptadv_echolink::transport::direct::{DirectSockets, SendError};
    let runtime = runtime();
    let sockets = Arc::new(
        runtime
            .block_on(DirectSockets::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
            ))
            .unwrap(),
    );
    let peer = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    let destination = peer.local_addr().unwrap();
    let source = sockets.local_addr(Channel::Audio).unwrap();
    let (_sender, receiver) = channel(1).unwrap();
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::new(Control::default()),
        receiver,
        move |event| {
            if matches!(event, PeerEvent::Writable) {
                match sockets.try_send(Channel::Audio, destination, b"prepared once") {
                    Ok(()) => return None,
                    Err(SendError::Pending) => (),
                    Err(error) => panic!("unexpected send failure: {error:?}"),
                }
            }
            Some(PeerWait {
                writable: Some(rptadv_echolink::transport::driver::PeerWritable::Direct(
                    Arc::clone(&sockets),
                    Channel::Audio,
                )),
                ..PeerWait::default()
            })
        },
    );
    let mut output = [0; 64];
    let (count, observed) = peer.recv_from(&mut output).unwrap();
    assert_eq!(&output[..count], b"prepared once");
    assert_eq!(observed, source);
    driver.detach();
}

#[test]
fn proxy_credit_resumes_one_peer_without_delaying_another_and_reports_closure() {
    use rptadv_echolink::{
        transport::{driver::PeerWritable, proxy::ProxyWriter},
        wire::{ProxyKind, ProxyRecord},
    };
    let runtime = runtime();
    let (stream, _reader) = tokio::io::duplex(64);
    let mut writer = ProxyWriter::new(stream);
    let output = writer.output();
    let expired = Instant::now() - Duration::from_secs(1);
    for _ in 0..56 {
        output
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: "192.0.2.1".parse().unwrap(),
                    payload: &[1],
                },
                Some(expired),
            )
            .unwrap();
    }
    let (_sender, receiver) = channel(1).unwrap();
    let (observed, events) = std::sync::mpsc::channel();
    let pending = output.clone();
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::new(Control::default()),
        receiver,
        move |event| {
            match event {
                PeerEvent::Writable => {
                    // Advisory readiness reserved nothing; retry the exact packet.
                    pending
                        .try_queue(
                            ProxyRecord {
                                kind: ProxyKind::UdpData,
                                address: "192.0.2.1".parse().unwrap(),
                                payload: &[2],
                            },
                            Some(expired),
                        )
                        .unwrap();
                    observed.send("ready").unwrap();
                }
                PeerEvent::Io(kind) => {
                    assert_eq!(kind, std::io::ErrorKind::BrokenPipe);
                    observed.send("closed").unwrap();
                    return None;
                }
                _ => (),
            }
            Some(PeerWait {
                receive: false,
                writable: Some(PeerWritable::Proxy {
                    output: pending.clone(),
                    kind: ProxyKind::UdpData,
                    payload_bytes: 1,
                }),
                ..PeerWait::default()
            })
        },
    );
    assert!(events.recv_timeout(Duration::from_millis(20)).is_err());
    runtime.block_on(async {
        // Other work remains runnable while this peer has no media credit.
        tokio::time::timeout(Duration::from_secs(1), tokio::task::yield_now())
            .await
            .unwrap();
        writer.flush_one(Instant::now()).await.unwrap();
    });
    assert_eq!(
        events.recv_timeout(Duration::from_secs(2)).unwrap(),
        "ready"
    );
    drop(writer);
    assert_eq!(
        events.recv_timeout(Duration::from_secs(2)).unwrap(),
        "closed"
    );
    assert_eq!(
        runtime.block_on(output.writable(ProxyKind::UdpData, 1)),
        Err(std::io::ErrorKind::BrokenPipe)
    );
    driver.detach();
}
