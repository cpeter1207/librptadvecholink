//! Serialized admission and first-talker policy with explicit downstream drain signals.

use rptadv_echolink::{
    account::{Account, Admission, Selection},
    directory::DirectorySnapshot,
};
use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

fn directory(now: Instant) -> DirectorySnapshot {
    let mut bytes = b"@@@\n32\n".to_vec();
    for node in 1..=32 {
        bytes.extend(format!("K{node}ABC-R\nStation\n{node}\n192.0.2.1\n").bytes());
    }
    bytes.extend(b"+++\n");
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory.update(&bytes, now).unwrap();
    directory
}

#[test]
fn deny_wins_after_directory_validation() {
    let now = Instant::now();
    let directory = directory(now);
    let ip = Ipv4Addr::new(192, 0, 2, 1);
    for (allow, deny, callsign, allowed) in [
        (vec![], vec![], "K1ABC-R", true),
        (vec!["K*"], vec!["K1*"], "K1ABC-R", false),
        (vec!["K?ABC-R"], vec![], "K1ABC-R", true),
        (vec!["K?ABC-R"], vec![], "K12ABC-R", false),
        (vec!["K*ABC-R"], vec!["K*2*"], "K12ABC-R", false),
        (vec!["K1*ABC-R"], vec![], "K12ABC-R", true),
        (vec!["K1*ABC-R"], vec![], "K2ABC-R", false),
    ] {
        let mut account = Account::new(
            20,
            allow.into_iter().map(String::from).collect(),
            deny.into_iter().map(String::from).collect(),
        )
        .unwrap();
        assert_eq!(
            account.admit(&directory, callsign, ip, 1, now).is_ok(),
            allowed
        );
    }
    let mut account = Account::new(20, vec!["*".into()], vec![]).unwrap();
    assert_eq!(
        account.admit(&directory, "K1ABC-R", Ipv4Addr::new(192, 0, 2, 2), 1, now),
        Err(Admission::Unverified)
    );
    assert_eq!(
        account.admit(&directory, "K99ABC-R", ip, 1, now),
        Err(Admission::Unverified)
    );
    assert_eq!(
        account.admit(&directory, "K1ABC-R", ip, 1, now + Duration::from_secs(900)),
        Err(Admission::Unverified)
    );
}

#[test]
fn simultaneous_admission_respects_limit_and_generation_scoped_removal() {
    let now = Instant::now();
    let directory = directory(now);
    let mut account = Account::new(20, vec![], vec![]).unwrap();
    for node in 1..=32 {
        let result = account.admit(
            &directory,
            &format!("K{node}ABC-R"),
            Ipv4Addr::new(192, 0, 2, 1),
            node as u64,
            now,
        );
        assert_eq!(result.is_ok(), node <= 20);
        if node > 20 {
            assert_eq!(result, Err(Admission::Full));
        }
    }
    assert_eq!(account.admitted(), 20);
    assert_eq!(
        account.admit(&directory, "K1ABC-R", Ipv4Addr::new(192, 0, 2, 1), 99, now),
        Err(Admission::Duplicate)
    );
    account.remove(1, 99);
    assert_eq!(account.admitted(), 20);
    account.remove(1, 1);
    assert_eq!(account.admitted(), 19);
    assert!(
        account
            .admit(&directory, "K21ABC-R", Ipv4Addr::new(192, 0, 2, 1), 21, now)
            .is_ok()
    );
    assert_eq!(account.voice(1, 1, now), Selection::Ignored);
}

#[test]
fn first_talker_releases_after_800ms_and_drain_not_contender_activity() {
    let now = Instant::now();
    let directory = directory(now);
    let mut account = Account::new(20, vec![], vec![]).unwrap();
    for node in [1, 2] {
        account
            .admit(
                &directory,
                &format!("K{node}ABC-R"),
                Ipv4Addr::new(192, 0, 2, 1),
                node as u64,
                now,
            )
            .unwrap();
    }
    assert_eq!(account.voice(1, 1, now), Selection::Selected);
    assert_eq!(
        account.voice(2, 2, now + Duration::from_millis(100)),
        Selection::Suppressed
    );
    assert_eq!(
        account.poll(now + Duration::from_millis(320), true),
        Some((1, 1))
    );
    assert!(!account.receiving());
    assert_eq!(account.selected(), Some((1, 1)));
    assert_eq!(
        account.voice(2, 2, now + Duration::from_millis(799)),
        Selection::Suppressed
    );
    assert_eq!(account.poll(now + Duration::from_millis(800), false), None);
    assert_eq!(account.selected(), Some((1, 1)));
    account.poll(now + Duration::from_millis(800), true);
    assert_eq!(account.selected(), None);
    assert_eq!(
        account.voice(2, 2, now + Duration::from_millis(801)),
        Selection::Selected
    );
    assert_eq!(account.selected(), Some((2, 2)));
    assert_eq!(account.suppressed_packets(), 2);
}

#[test]
fn accepted_audio_drains_before_one_final_unkey_without_an_extra_guard() {
    let now = Instant::now();
    let directory = directory(now);
    let mut account = Account::new(20, vec![], vec![]).unwrap();
    account
        .admit(&directory, "K1ABC-R", Ipv4Addr::new(192, 0, 2, 1), 1, now)
        .unwrap();
    account.voice(1, 1, now);
    assert_eq!(account.poll(now + Duration::from_millis(319), true), None);
    assert_eq!(account.poll(now + Duration::from_millis(600), false), None);
    assert!(account.receiving());
    assert_eq!(
        account.poll(now + Duration::from_millis(600), true),
        Some((1, 1))
    );
    assert_eq!(account.poll(now + Duration::from_millis(700), true), None);
    account.remove(1, 1);
    account.poll(now + Duration::from_millis(800), true);
    assert_eq!(account.selected(), None);
    account
        .admit(&directory, "K1ABC-R", Ipv4Addr::new(192, 0, 2, 1), 2, now)
        .unwrap();
    assert_eq!(
        account.voice(1, 1, now + Duration::from_secs(1)),
        Selection::Ignored
    );
    assert_eq!(
        account.voice(1, 2, now + Duration::from_secs(1)),
        Selection::Selected
    );
}
