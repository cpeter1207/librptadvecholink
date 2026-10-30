//! Real bounded channels arbitrate peer requests without blocking their owner turns.
use rptadv_echolink::{
    account::{
        Account, Selection,
        coordination::{Coordinator, OfferError},
    },
    directory::DirectorySnapshot,
    transport::{
        driver::Control,
        routing::{Connection, Router},
    },
};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

fn run(test: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

fn setup(now: Instant) -> (Coordinator, DirectorySnapshot, Vec<Connection>) {
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory
        .update(
            b"@@@\n2\nK1ABC-R\nFirst\n1\n192.0.2.1\nK2ABC-R\nSecond\n2\n192.0.2.2\n+++\n",
            now,
        )
        .unwrap();
    let mut router = Router::new(
        Account::new(2, vec![], vec![]).unwrap(),
        2,
        Duration::from_secs(30),
    )
    .unwrap();
    let connections = [1, 2]
        .into_iter()
        .map(|node| router.dial(&directory, node, now).unwrap())
        .collect();
    (Coordinator::new(router).unwrap(), directory, connections)
}

#[test]
fn grant_precedes_drain_and_selected_unkey_has_one_consumer() {
    run(async {
        let now = Instant::now();
        let (mut owner, _, connections) = setup(now);
        let mut first = owner.port(1, connections[0].generation, Arc::new(Control::default()));
        let mut second = owner.port(2, connections[1].generation, Arc::new(Control::default()));
        first.offer(now).unwrap();
        assert_eq!(first.selection().unwrap(), None);
        assert_eq!(first.offer(now), Err(OfferError::Busy));
        assert_eq!(first.report_drained(true), Err(OfferError::Busy));
        owner.service().await;
        // No quiet timer can unkey/release a grant before its pending PCM drains.
        assert_eq!(owner.poll(now + Duration::from_millis(320)), None);
        assert_eq!(owner.deadline(now + Duration::from_millis(320)), None);
        assert_eq!(first.selection().unwrap(), Some(Selection::Selected));
        second.offer(now + Duration::from_millis(100)).unwrap();
        owner.service().await;
        assert_eq!(second.selection().unwrap(), Some(Selection::Suppressed));
        second.report_drained(true).unwrap();
        owner.service().await;
        assert_eq!(owner.poll(now + Duration::from_millis(320)), None);
        first.report_drained(true).unwrap();
        owner.service().await;
        assert_eq!(
            owner.poll(now + Duration::from_millis(320)),
            Some((1, connections[0].generation))
        );
        assert!(first.take_unkey());
        assert!(!first.take_unkey());
        assert!(!second.take_unkey());
        assert_eq!(
            owner.deadline(now + Duration::from_millis(320)),
            Some(now + Duration::from_millis(800))
        );
        owner.poll(now + Duration::from_millis(800));
        assert_eq!(owner.deadline(now + Duration::from_millis(800)), None);
        second.offer(now + Duration::from_millis(800)).unwrap();
        owner.service().await;
        assert_eq!(second.selection().unwrap(), Some(Selection::Selected));
    });
}

#[test]
fn stale_ports_and_cancelled_requests_cannot_claim_another_generation() {
    run(async {
        let now = Instant::now();
        let (mut owner, directory, connections) = setup(now);
        let mut cancelled = owner.port(2, connections[1].generation, Arc::new(Control::default()));
        cancelled.offer(now).unwrap();
        drop(cancelled);
        owner.service().await;
        assert_eq!(owner.deadline(now), None);
        let mut old = owner.port(1, connections[0].generation, Arc::new(Control::default()));
        owner.remove(1, connections[0].generation);
        let replacement = owner.router().dial(&directory, 1, now).unwrap();
        let mut current = owner.port(1, replacement.generation, Arc::new(Control::default()));
        current.offer(now).unwrap();
        owner.service().await;
        assert_eq!(current.selection().unwrap(), Some(Selection::Selected));
        old.offer(now).unwrap();
        owner.service().await;
        assert_eq!(old.selection().unwrap(), Some(Selection::Ignored));
        old.report_drained(true).unwrap();
        owner.service().await;
        assert_eq!(owner.poll(now + Duration::from_secs(1)), None);
        owner.remove(1, replacement.generation);
        current.report_drained(true).unwrap();
        owner.service().await;
        assert_eq!(
            owner.poll(now + Duration::from_secs(1)),
            Some((1, replacement.generation))
        );
        assert_eq!(owner.deadline(now), None);
        assert!(!old.take_unkey());
        drop(owner);
        assert_eq!(current.offer(now), Err(OfferError::Closed));
    });
}

#[test]
fn account_shutdown_cancels_a_pending_talker_offer_without_retaining_it() {
    let now = Instant::now();
    let (owner, _, connections) = setup(now);
    let mut port = owner.port(1, connections[0].generation, Arc::new(Control::default()));
    port.offer(now).unwrap();
    drop(owner);
    assert_eq!(port.selection(), Err(OfferError::Closed));
    assert_eq!(port.selection(), Ok(None));
}

#[test]
fn retiring_a_selected_generation_discards_its_grant_without_a_dead_port_report() {
    run(async {
        let now = Instant::now();
        let (mut owner, _, connections) = setup(now);
        let mut first = owner.port(1, connections[0].generation, Arc::new(Control::default()));
        let mut second = owner.port(2, connections[1].generation, Arc::new(Control::default()));
        first.offer(now).unwrap();
        owner.service().await;
        assert_eq!(first.selection().unwrap(), Some(Selection::Selected));
        // A stale teardown must not cancel the active generation's pending PCM.
        owner.remove(1, connections[0].generation + 1);
        second.offer(now).unwrap();
        owner.service().await;
        assert_eq!(second.selection().unwrap(), Some(Selection::Suppressed));
        // Host detach discards that exact connection's audio before its lease is
        // retired. No live serial port remains to report a normal played tail.
        drop(first);
        owner.remove(1, connections[0].generation);
        owner.poll(now + Duration::from_secs(1));
        second.offer(now + Duration::from_secs(1)).unwrap();
        owner.service().await;
        assert_eq!(second.selection().unwrap(), Some(Selection::Selected));
    });
}

#[test]
fn unrepresentable_quiet_deadline_does_not_wrap_or_release_a_selected_talker() {
    run(async {
        let now = Instant::now();
        let (mut owner, _, connections) = setup(now);
        // The public owner accepts caller-supplied Instants. Find this platform's
        // last representable one without assuming its monotonic epoch or width.
        let mut low = 0_u128;
        let mut high = u128::from(u64::MAX) * 1_000_000_000 + 999_999_999;
        while low < high {
            let middle = low + (high - low).div_ceil(2);
            let duration = Duration::new(
                (middle / 1_000_000_000) as u64,
                (middle % 1_000_000_000) as u32,
            );
            if now.checked_add(duration).is_some() {
                low = middle;
            } else {
                high = middle - 1;
            }
        }
        let last = now + Duration::new((low / 1_000_000_000) as u64, (low % 1_000_000_000) as u32);
        let mut port = owner.port(1, connections[0].generation, Arc::new(Control::default()));
        port.offer(last).unwrap();
        owner.service().await;
        assert_eq!(port.selection(), Ok(Some(Selection::Selected)));
        assert_eq!(owner.deadline(last), None);
        assert_eq!(owner.poll(last), None);
    });
}

#[test]
fn full_control_queue_returns_credit_failure_without_waiting_or_closing_peers() {
    run(async {
        let now = Instant::now();
        let (mut owner, _, connections) = setup(now);
        let mut first = owner.port(1, connections[0].generation, Arc::new(Control::default()));
        let mut second = owner.port(2, connections[1].generation, Arc::new(Control::default()));
        assert_eq!(first.selection().unwrap(), None);
        for value in [true, false, true, false] {
            first.report_drained(value).unwrap();
            first.report_drained(value).unwrap(); // Duplicate state must not consume queue credit.
        }
        assert_eq!(second.offer(now), Err(OfferError::Full));
        assert_eq!(owner.overflows(), 1);
        for _ in 0..4 {
            owner.service().await;
        }
        second.offer(now).unwrap();
        owner.service().await;
        assert_eq!(second.selection().unwrap(), Some(Selection::Selected));
        assert!(!second.take_unkey());
        owner.close();
        assert!(!owner.service().await);
    });
}

#[test]
fn freed_account_credit_wakes_a_selected_peer_without_an_idle_retry_timer() {
    use rptadv_echolink::transport::{
        driver::{PeerDriver, PeerWait},
        mailbox,
    };
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );
    let now = Instant::now();
    let (mut owner, _, connections) = setup(now);
    let control = Arc::new(Control::default());
    let mut first = owner.port(1, connections[0].generation, Arc::clone(&control));
    let mut second = owner.port(2, connections[1].generation, Arc::new(Control::default()));
    let (_input, ingress) = mailbox::channel(connections[0].generation).unwrap();
    let (notified, notifications) = std::sync::mpsc::channel();
    let driver = PeerDriver::attach(Arc::clone(&runtime), control, ingress, move |_| {
        notified.send(()).unwrap();
        Some(PeerWait::default())
    });
    notifications.recv_timeout(Duration::from_secs(2)).unwrap();
    first.offer(now).unwrap();
    runtime.block_on(owner.service());
    notifications.recv_timeout(Duration::from_secs(2)).unwrap();
    first.selection().unwrap();
    for value in [true, false, true, false] {
        second.report_drained(value).unwrap();
    }
    assert_eq!(first.report_drained(true), Err(OfferError::Full));
    runtime.block_on(owner.service());
    let wake = notifications.recv_timeout(Duration::from_millis(200));
    driver.detach();
    assert!(
        wake.is_ok(),
        "account progress did not wake the selected owner waiting for credit"
    );
    first.report_drained(true).unwrap();
}
