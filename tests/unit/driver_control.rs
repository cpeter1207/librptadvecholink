use super::Control;
use std::time::Duration;

#[test]
fn transport_failure_wakes_an_owner_with_no_ingress_credit() {
    use super::{PeerDriver, PeerEvent, PeerWait};
    use std::sync::Arc;
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .unwrap(),
    );
    let control = Arc::new(Control::default());
    let (_producer, consumer) = crate::transport::mailbox::channel(1).unwrap();
    let (started, waiting) = std::sync::mpsc::channel();
    let (failed, observed) = std::sync::mpsc::channel();
    let driver = PeerDriver::attach(
        Arc::clone(&runtime),
        Arc::clone(&control),
        consumer,
        move |event| {
            if let PeerEvent::Io(error) = event {
                failed.send(error).unwrap();
                return None;
            }
            started.send(()).unwrap();
            Some(PeerWait {
                receive: false,
                ..Default::default()
            })
        },
    );
    waiting.recv_timeout(Duration::from_secs(2)).unwrap();
    control.transport_lost();
    let result = observed.recv_timeout(Duration::from_secs(2));
    driver.detach();
    assert_eq!(result.unwrap(), std::io::ErrorKind::BrokenPipe);
}

#[test]
fn graceful_close_wakes_after_a_coalesced_product_wake_is_consumed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let control = Control::default();
        control.wake();
        control.wake();
        control.notify.notified().await;
        // The task has consumed its wake, but has not cleared pending yet.
        // A concurrent close must supply a fresh wake in this exact window.
        control.request_close();
        tokio::time::timeout(Duration::from_millis(20), control.notify.notified())
            .await
            .expect("graceful close lost behind an already consumed product wake");
    });
}
