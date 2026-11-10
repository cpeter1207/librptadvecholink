//! Real bounded Proxy writer records establish eligibility/disposal boundaries.
use super::*;
use crate::{
    transport::proxy::{FlushOutcome, ProxyWriter, QueueError},
    wire::{ProxyKind, ProxyRecord},
};
use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};
use tokio::io::AsyncReadExt;

fn run(test: impl std::future::Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}
fn packet(payload: &[u8]) -> ProxyRecord<'_> {
    ProxyRecord {
        kind: ProxyKind::UdpData,
        address: Ipv4Addr::LOCALHOST,
        payload,
    }
}
fn wire(payload: &[u8]) -> Vec<u8> {
    let mut data = vec![5, 127, 0, 0, 1];
    data.extend((payload.len() as u32).to_le_bytes());
    data.extend(payload);
    data
}

#[test]
fn revoke_drops_only_unsubmitted_old_qso_voice_and_preserves_control_and_other_qso() {
    run(async {
        let first = Arc::new(Lifetime::default());
        let second = Arc::new(Lifetime::default());
        let mut tx = Egress::new(first.clone());
        let other = Egress::new(second.clone());
        let (output, mut input) = tokio::io::duplex(256);
        let mut writer = ProxyWriter::new(output);
        let queue = writer.output();
        queue
            .try_queue_scoped(packet(b"old"), None, tx.epoch())
            .unwrap();
        queue
            .try_queue_scoped(packet(b"other"), None, other.epoch())
            .unwrap();
        let control = ProxyRecord {
            kind: ProxyKind::UdpControl,
            address: Ipv4Addr::LOCALHOST,
            payload: b"control",
        };
        queue.try_queue(control, None).unwrap();
        assert!(tx.begin());
        tx.reset_completed();
        assert_eq!(tx.complete(), Ok(None));
        assert!(tx.epoch().is_none());
        assert!(!first.disposed());
        assert_eq!(writer.flush_next().await.unwrap(), FlushOutcome::Skipped);
        assert_eq!(tx.complete(), Ok(Some(2)));
        assert_eq!(writer.flush_next().await.unwrap(), FlushOutcome::Sent);
        assert_eq!(writer.flush_next().await.unwrap(), FlushOutcome::Sent);
        let mut expected = wire(b"other");
        let mut c = vec![6, 127, 0, 0, 1];
        c.extend(7u32.to_le_bytes());
        c.extend(b"control");
        expected.extend(c);
        let mut heard = vec![0; expected.len()];
        input.read_exact(&mut heard).await.unwrap();
        assert_eq!(heard, expected);
        assert_eq!(second.pending.load(Ordering::Acquire), 0);
        first.close();
        assert!(first.disposed());
        assert!(!second.disposed());
    });
}

#[test]
fn destroying_a_qso_discards_its_unsubmitted_voice_without_a_retirement_call() {
    run(async {
        let lifetime = Arc::new(Lifetime::default());
        let tx = Egress::new(lifetime.clone());
        let (output, _input) = tokio::io::duplex(256);
        let mut writer = ProxyWriter::new(output);
        writer
            .output()
            .try_queue_scoped(packet(b"old"), None, tx.epoch())
            .unwrap();
        lifetime.close();
        assert!(!lifetime.disposed());
        assert_eq!(writer.flush_next().await.unwrap(), FlushOutcome::Skipped);
        assert!(lifetime.disposed());
        assert_eq!(writer.output().observe().sent, 0);
    });
}

#[test]
fn a_partial_frame_stays_pending_through_qso_destruction_until_actual_finish_or_transport_drop() {
    run(async {
        for finish in [true, false] {
            let lifetime = Arc::new(Lifetime::default());
            let mut tx = Egress::new(lifetime.clone());
            let (output, mut input) = tokio::io::duplex(1);
            let mut writer = ProxyWriter::new(output);
            writer
                .output()
                .try_queue_scoped(
                    packet(b"partial"),
                    Some(Instant::now() + Duration::from_millis(1)),
                    tx.epoch(),
                )
                .unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(5), writer.flush_next())
                    .await
                    .is_err()
            );
            assert!(tx.begin());
            tx.reset_completed();
            assert!(!tx.begin());
            assert_eq!(tx.complete(), Ok(None));
            lifetime.close();
            assert!(!lifetime.disposed());
            if finish {
                let reader = tokio::spawn(async move {
                    let mut wire = vec![0; 16];
                    input.read_exact(&mut wire).await.unwrap();
                    wire
                });
                assert_eq!(
                    writer
                        .flush_one(Instant::now() + Duration::from_secs(1))
                        .await
                        .unwrap(),
                    FlushOutcome::Sent
                );
                assert_eq!(reader.await.unwrap(), wire(b"partial"));
                assert!(lifetime.disposed());
            } else {
                drop(writer);
                assert!(lifetime.disposed());
            }
            assert_eq!(tx.complete(), Ok(Some(2)));
        }
    });
}

#[test]
fn bounded_queue_rejection_creates_no_phantom_pending_media_and_epoch_exhaustion_stays_gated() {
    run(async {
        let lifetime = Arc::new(Lifetime::default());
        let mut tx = Egress::new(lifetime.clone());
        let (output, _input) = tokio::io::duplex(1);
        let writer = ProxyWriter::new(output);
        let queue = writer.output();
        for _ in 0..56 {
            queue
                .try_queue_scoped(packet(b"x"), None, tx.epoch())
                .unwrap();
        }
        assert_eq!(
            queue.try_queue_scoped(packet(b"x"), None, tx.epoch()),
            Err(QueueError::Full)
        );
        assert_eq!(lifetime.pending.load(Ordering::Acquire), 56);
        for _ in 0..8 {
            queue
                .try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpControl,
                        address: Ipv4Addr::LOCALHOST,
                        payload: b"c",
                    },
                    None,
                )
                .unwrap();
        }
        assert!(tx.begin());
        tx.reset_completed();
        assert_eq!(tx.complete(), Ok(None));
        drop(writer);
        assert_eq!(lifetime.pending.load(Ordering::Acquire), 0);
        tx.token = u64::MAX;
        assert_eq!(tx.complete(), Err(Exhausted));
        assert!(tx.retiring());
        assert!(tx.epoch().is_none());
        lifetime.close();
        assert!(lifetime.disposed());
    });
}

#[test]
fn a_panicking_writer_cannot_poison_retirement_or_reopen_old_voice() {
    struct Broken;
    impl tokio::io::AsyncWrite for Broken {
        fn poll_write(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            _: &[u8],
        ) -> Poll<io::Result<usize>> {
            panic!("fault at nonblocking writer boundary")
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }
    let lifetime = Arc::new(Lifetime::default());
    let mut tx = Egress::new(lifetime.clone());
    let mut writer = ProxyWriter::new(Broken);
    writer
        .output()
        .try_queue_scoped(packet(b"old"), None, tx.epoch())
        .unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    assert!(
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || runtime.block_on(writer.flush_next())
        ))
        .is_err()
    );
    assert!(tx.begin());
    tx.reset_completed();
    assert_eq!(tx.complete(), Ok(None));
    assert_eq!(
        runtime.block_on(writer.flush_next()).unwrap(),
        FlushOutcome::Skipped
    );
    assert_eq!(tx.complete(), Ok(Some(2)));
}
