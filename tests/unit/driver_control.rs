use super::Control;
use std::time::Duration;

#[test]
fn graceful_close_wakes_after_a_coalesced_product_wake_is_consumed() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let control = Control::default();
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
