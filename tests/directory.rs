//! Independently framed ASL/EchoLib directory fixtures; no public directory access.

use rptadv_echolink::directory::{AdmissionError, DirectoryError, DirectorySnapshot};
use std::{
    io::Write,
    net::Ipv4Addr,
    time::{Duration, Instant},
};

const FULL: &[u8] = b"@@@\n2:first\nK1ABC-R\nNorth [ON 12:00]\n123456\n192.0.2.1\nK2ABC-L\nSouth [ON 12:00]\n234567\n192.0.2.1\n+++\n";

#[test]
fn snapshot_tokens_keep_the_full_asl_directory_field() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    let token = "a".repeat(49);
    snapshot
        .update(format!("@@@\n0:{token}\n+++\n").as_bytes(), now)
        .unwrap();
    assert_eq!(snapshot.snapshot_id(), token);
    assert_eq!(
        snapshot.update(format!("@@@\n0:{token}a\n+++\n").as_bytes(), now),
        Err(DirectoryError::Malformed)
    );
    assert_eq!(snapshot.snapshot_id(), token);
}

fn compressed(bytes: &[u8]) -> Vec<u8> {
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
    encoder.write_all(bytes).unwrap();
    // ASL consumes and ignores these four bytes before initializing zlib.
    let mut packet = vec![0, 0, 0, 0];
    packet.extend(encoder.finish().unwrap());
    packet
}

#[test]
fn directory_plain_compressed_and_delta_atomic() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(FULL, now).unwrap();
    assert_eq!(snapshot.snapshot_id(), "first");
    assert_eq!(
        snapshot
            .lookup("K1ABC-R", Ipv4Addr::new(192, 0, 2, 1), now)
            .unwrap()
            .node_number,
        123456
    );
    assert_eq!(
        snapshot
            .lookup("K2ABC-L", Ipv4Addr::new(192, 0, 2, 1), now)
            .unwrap()
            .node_number,
        234567
    );
    snapshot.update(&compressed(FULL), now).unwrap();
    let delta = b"DDD\n2:second\nK1ABC-R\nMoved [ON 12:01]\n123456\n192.0.2.9\n+++\nK2ABC-L\n+++\n";
    snapshot.update(&compressed(delta), now).unwrap();
    assert_eq!(snapshot.snapshot_id(), "second");
    assert_eq!(
        snapshot.by_node(123456, now).unwrap().address,
        Ipv4Addr::new(192, 0, 2, 9)
    );
    assert_eq!(snapshot.by_node(234567, now), Err(AdmissionError::Unknown));
    let partial_delta = b"DDD\n1:bad\nK1ABC-R\nChanged\n123456\n192.0.2.6\n+++\n";
    assert_eq!(
        snapshot.update(partial_delta, now),
        Err(DirectoryError::Malformed)
    );
    assert_eq!(snapshot.snapshot_id(), "second");
    assert_eq!(
        snapshot.by_node(123456, now).unwrap().address,
        Ipv4Addr::new(192, 0, 2, 9)
    );
    assert_eq!(snapshot.failures(), 1);
}

#[test]
fn stale_directory_refresh_fails_closed_without_erasing_good_state() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    assert_eq!(
        snapshot.update(b"DDD\n0:new\n+++\n+++\n", now),
        Err(DirectoryError::NoSnapshot)
    );
    snapshot.update(FULL, now).unwrap();
    let admitted = snapshot.by_node(123456, now).unwrap();
    assert!(
        snapshot
            .update(b"not a directory", now + Duration::from_secs(899))
            .is_err()
    );
    assert_eq!(
        snapshot
            .by_node(123456, now + Duration::from_secs(899))
            .unwrap(),
        admitted
    );
    assert_eq!(
        snapshot.by_node(123456, now + Duration::from_secs(900)),
        Err(AdmissionError::Stale)
    );
    assert_eq!(admitted.node_number, 123456); // Existing QSO retains its verified copy.
    assert_eq!(snapshot.snapshot_id(), "first");
}

#[test]
fn callsign_and_address_must_match_even_when_multiple_stations_share_an_ip() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(FULL, now).unwrap();
    assert_eq!(
        snapshot.lookup("K1ABC-R", Ipv4Addr::new(192, 0, 2, 2), now),
        Err(AdmissionError::Address)
    );
    assert_eq!(
        snapshot.lookup("K9XYZ-R", Ipv4Addr::new(192, 0, 2, 1), now),
        Err(AdmissionError::Unknown)
    );
    assert_eq!(
        snapshot
            .lookup("k1abc-r", Ipv4Addr::new(192, 0, 2, 1), now)
            .unwrap()
            .node_number,
        123456
    );
}

#[test]
fn directory_server_notices_do_not_become_stations_or_discard_valid_records() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(b"@@@\n2\n.\nPadding\n0\n0.0.0.0\n \nWelcome\n0\n0.0.0.0\nK1ABC-R\nNorth\n123456\n192.0.2.1\n+++\n", now).unwrap();
    assert_eq!(snapshot.by_node(123456, now).unwrap().callsign, "K1ABC-R");
    assert_eq!(
        snapshot.update(b"@@@\n1\n \nINCORRECT PASSWORD\n0\n0.0.0.0\n+++\n", now),
        Err(DirectoryError::Rejected)
    );
    assert_eq!(snapshot.by_node(123456, now).unwrap().callsign, "K1ABC-R");
}

#[test]
fn conflicting_and_incomplete_snapshots_never_replace_the_previous_snapshot() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(FULL, now).unwrap();
    for invalid in [
        b"@@@\n2\nK1ABC-R\nNorth\n1\n192.0.2.1\nK2ABC-R\nSouth\n1\n192.0.2.2\n+++\n".as_slice(),
        b"@@@\n2\nK1ABC-R\nNorth\n1\n192.0.2.1\nK1ABC-R\nSouth\n2\n192.0.2.2\n+++\n",
        b"@@@\n1\nK1ABC-R\nNorth\n1\n192.0.2.1\n",
        b"@@@\n2\nK1ABC-R\nNorth\n1\n192.0.2.1\n+++\n",
        b"@@@\n1\nK1ABC-R\nNorth\n0\n192.0.2.1\n+++\n",
        b"@@@\n1\nK1ABC-R\nNorth\n1\ninvalid\n+++\n",
        b"@@@\n0\n+++\nextra\n",
    ] {
        assert!(snapshot.update(invalid, now).is_err());
        assert_eq!(snapshot.by_node(123456, now).unwrap().callsign, "K1ABC-R");
    }
    assert_eq!(snapshot.failures(), 7);
}

#[test]
fn bounded_decompression_and_line_limits_reject_before_publication() {
    let now = Instant::now();
    let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
    snapshot.update(FULL, now).unwrap();
    let bomb = compressed(&vec![b'A'; 16 * 1024 * 1024 + 1]);
    assert_eq!(snapshot.update(&bomb, now), Err(DirectoryError::Limit));
    assert_eq!(
        snapshot.update(&vec![b'Z'; 2 * 1024 * 1024 + 1], now),
        Err(DirectoryError::Limit)
    );
    let mut long_line = b"@@@\n1\nK1ABC-R\n".to_vec();
    long_line.extend(vec![b'A'; 1025]);
    long_line.extend(b"\n1\n192.0.2.1\n+++\n");
    assert_eq!(snapshot.update(&long_line, now), Err(DirectoryError::Limit));
    let mut truncated = compressed(FULL);
    truncated.truncate(truncated.len() - 2);
    assert_eq!(
        snapshot.update(&truncated, now),
        Err(DirectoryError::Malformed)
    );
    assert_eq!(snapshot.snapshot_id(), "first");
}
