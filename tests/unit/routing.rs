use super::*;

#[test]
fn exhausted_generation_never_wraps_or_reserves_an_incoming_candidate() {
    let now = Instant::now();
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory
        .update(b"@@@\n1\nK1ABC-R\nStation\n1\n192.0.2.1\n+++\n", now)
        .unwrap();
    let mut router = Router::new(
        Account::new(1, vec![], vec![]).unwrap(),
        1,
        Duration::from_secs(1),
    )
    .unwrap();
    router.next = u64::MAX;
    assert!(matches!(
        router.dial(&directory, 1, now),
        Err(Admission::Full)
    ));
    let mut packet = [0; 1024];
    let count = crate::wire::write_control(
        &ControlPacket::Sdes {
            source: 1,
            callsign: "K1ABC-R".into(),
            station_name: "Station".into(),
            speex: false,
        },
        &mut packet,
    )
    .unwrap();
    assert!(matches!(
        router.receive(
            &directory,
            Channel::Control,
            "192.0.2.1:5199".parse().unwrap(),
            now,
            &packet[..count]
        ),
        RouteResult::Dropped
    ));
    assert_eq!(router.pending_count(), 0);
    assert_eq!(router.next, u64::MAX);
}
