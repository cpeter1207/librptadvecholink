//! Final admission sees current publication, not a pending callback's old identity copy.
use super::*;

#[test]
fn published_views_observe_replacement_failure_retention_and_freshness() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    let view = DirectoryView::new(&snapshot);
    let reader = view.clone();
    snapshot
        .update(b"@@@\n1\nK1ABC-R\nFirst\n1\n192.0.2.1\n+++\n", now)
        .unwrap();
    view.publish(&snapshot);
    let old = reader.by_node(1, now).unwrap();
    let later = now + Duration::from_secs(1);
    snapshot
        .update(b"@@@\n1\nK1ABC-R\nSecond\n1\n192.0.2.2\n+++\n", later)
        .unwrap();
    view.publish(&snapshot);
    assert_eq!(
        reader.lookup("K1ABC-R", old.address, later),
        Err(AdmissionError::Address)
    );
    let current = reader.by_node(1, later).unwrap();
    assert_eq!(current.address, "192.0.2.2".parse::<Ipv4Addr>().unwrap());
    assert_eq!(current.description, "Second");
    assert_eq!(old.description, "First");
    assert_eq!(old.address, "192.0.2.1".parse::<Ipv4Addr>().unwrap());
    assert!(snapshot.update(b"@@@\n1\ntruncated", later).is_err());
    view.publish(&snapshot);
    assert_eq!(reader.by_node(1, later).unwrap(), current);
    assert_eq!(
        reader.by_node(1, later + Duration::from_secs(900)),
        Err(AdmissionError::Stale)
    );
}
