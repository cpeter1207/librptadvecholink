//! Local Proxy peers exercise challenge bytes, framing and cancellation-safe staging.
use rptadv_echolink::{
    transport::proxy::{FlushOutcome, ProxyError, ProxyReader, ProxyWriter, QueueError, connect},
    wire::{ProxyKind, ProxyRecord},
};
use std::{
    future::Future,
    net::Ipv4Addr,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
};

fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

fn record(kind: u8, ip: [u8; 4], data: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind];
    bytes.extend(ip);
    bytes.extend((data.len() as u32).to_le_bytes());
    bytes.extend(data);
    bytes
}

#[test]
fn proxy_auth_uses_split_nonce_uppercase_password_and_raw_digest_without_waiting_for_ack() {
    run(async {
        for (password, digest) in [
            (
                "sEcReT",
                [
                    172, 22, 219, 253, 5, 142, 205, 242, 178, 133, 31, 61, 120, 188, 58, 54,
                ],
            ),
            (
                "",
                [
                    211, 216, 251, 232, 104, 154, 198, 170, 192, 207, 55, 225, 132, 49, 79, 153,
                ],
            ),
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_all(b"1234").await.unwrap();
                stream.write_all(b"5678").await.unwrap();
                let mut auth = [0; 24];
                stream.read_exact(&mut auth).await.unwrap();
                assert_eq!(&auth[..8], b"K1ABC-R\n");
                assert_eq!(&auth[8..], digest);
                stream
            });
            let _transport = connect(address, "K1ABC-R", password, Duration::from_secs(1))
                .await
                .unwrap();
            let _server = server.await.unwrap();
        }
    });
}

#[test]
fn explicit_proxy_rejection_is_typed_and_incomplete_nonce_obeys_deadline() {
    run(async {
        for (code, expected) in [
            (1, ProxyError::BadPassword),
            (2, ProxyError::AccessDenied),
            (3, ProxyError::Rejected(3)),
        ] {
            let (mut source, input) = tokio::io::duplex(64);
            source.write_all(&record(7, [0; 4], &[code])).await.unwrap();
            let mut reader = ProxyReader::new(input);
            assert_eq!(
                reader.read(&mut |_| panic!("rejection became media")).await,
                Err(expected)
            );
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.write_all(b"1234").await.unwrap();
            tokio::time::sleep(Duration::from_secs(1)).await;
        });
        assert!(matches!(
            connect(
                address,
                "K1ABC-R",
                "not-a-real-secret",
                Duration::from_millis(20)
            )
            .await,
            Err(ProxyError::Timeout)
        ));
        server.abort();
    });
}

#[test]
fn proxy_reader_preserves_remote_ip_and_audio_control_across_every_header_split() {
    run(async {
        let wire = [
            record(5, [192, 0, 2, 1], b"audio"),
            record(6, [192, 0, 2, 2], b"control"),
            record(3, [192, 0, 2, 3], b""),
        ]
        .concat();
        for split in 0..=9 {
            let (mut source, input) = tokio::io::duplex(128);
            let wire = wire.clone();
            let send = tokio::spawn(async move {
                source.write_all(&wire[..split]).await.unwrap();
                tokio::task::yield_now().await;
                source.write_all(&wire[split..]).await.unwrap();
            });
            let mut reader = ProxyReader::new(input);
            let mut received = Vec::new();
            while received.len() < 3 {
                reader
                    .read(&mut |frame| {
                        received.push((frame.kind, frame.address, frame.payload.to_vec()))
                    })
                    .await
                    .unwrap();
            }
            assert_eq!(
                received,
                [
                    (
                        ProxyKind::UdpData,
                        Ipv4Addr::new(192, 0, 2, 1),
                        b"audio".to_vec()
                    ),
                    (
                        ProxyKind::UdpControl,
                        Ipv4Addr::new(192, 0, 2, 2),
                        b"control".to_vec()
                    ),
                    (ProxyKind::TcpClose, Ipv4Addr::new(192, 0, 2, 3), vec![]),
                ]
            );
            send.await.unwrap();
        }
    });
}

#[test]
fn dropping_a_stalled_writer_retires_its_live_stall_without_claiming_a_send() {
    run(async {
        let (stream, _sink) = tokio::io::duplex(1);
        let mut writer = ProxyWriter::new(stream);
        let output = writer.output();
        output
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: Ipv4Addr::LOCALHOST,
                    payload: b"a",
                },
                None,
            )
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(5), writer.flush_next())
                .await
                .is_err()
        );
        drop(writer);
        let retired = output.observe();
        assert_eq!(
            (
                retired.sent,
                retired.expired,
                retired.queued_bytes,
                retired.head_age_ms
            ),
            (0, 0, 0, 0)
        );
        tokio::time::sleep(Duration::from_millis(3)).await;
        assert_eq!(
            output.observe().stall_ms,
            retired.stall_ms,
            "closed socket is no longer a live stall"
        );
    });
}

#[test]
fn canceled_partial_write_keeps_the_header_and_payload_before_later_control() {
    run(async {
        let (output, mut sink) = tokio::io::duplex(7);
        let mut writer = ProxyWriter::new(output);
        let observer = writer.output();
        let now = Instant::now();
        writer
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: Ipv4Addr::new(192, 0, 2, 1),
                    payload: b"audio",
                },
                Some(now + Duration::from_millis(1)),
            )
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), writer.flush_one(now))
                .await
                .is_err()
        );
        let stalled = observer.observe();
        assert_eq!(stalled.admitted, 1);
        assert_eq!(
            stalled.sent, 0,
            "queued/partial bytes are not a sent record"
        );
        assert_eq!(stalled.expired, 0);
        assert_eq!(stalled.stalls, 1);
        assert!(stalled.stall_ms >= 5);
        assert_eq!(stalled.queued_bytes, 5);
        assert!(stalled.head_age_ms >= 5);
        let mut first = [0; 7];
        sink.read_exact(&mut first).await.unwrap();
        writer
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpControl,
                    address: Ipv4Addr::new(192, 0, 2, 2),
                    payload: b"control",
                },
                None,
            )
            .unwrap();
        let read = tokio::spawn(async move {
            let mut all = first.to_vec();
            let mut rest = vec![0; 23];
            sink.read_exact(&mut rest).await.unwrap();
            all.extend(rest);
            all
        });
        assert_eq!(
            writer
                .flush_one(now + Duration::from_secs(1))
                .await
                .unwrap(),
            FlushOutcome::Sent
        );
        assert_eq!(
            writer
                .flush_one(now + Duration::from_secs(1))
                .await
                .unwrap(),
            FlushOutcome::Sent
        );
        let bytes = read.await.unwrap();
        assert_eq!(
            bytes,
            [
                record(5, [192, 0, 2, 1], b"audio"),
                record(6, [192, 0, 2, 2], b"control")
            ]
            .concat()
        );
        assert_eq!(writer.queued_bytes(), 0);
        let completed = observer.observe();
        assert_eq!(
            (completed.admitted, completed.sent, completed.expired),
            (2, 2, 0)
        );
        assert_eq!(completed.queued_bytes, 0);
        assert!(completed.stall_ms >= stalled.stall_ms);
    });
}

#[test]
fn proxy_output_reserves_control_bytes_and_sheds_only_unsent_expired_media() {
    run(async {
        let (output, _sink) = tokio::io::duplex(1);
        let mut writer = ProxyWriter::new(output);
        let now = Instant::now();
        for _ in 0..3 {
            writer
                .try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpData,
                        address: Ipv4Addr::LOCALHOST,
                        payload: &[0; 65507],
                    },
                    Some(now),
                )
                .unwrap();
        }
        assert_eq!(
            writer.try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: Ipv4Addr::LOCALHOST,
                    payload: &[0; 65507]
                },
                None
            ),
            Err(QueueError::Full)
        );
        writer
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpControl,
                    address: Ipv4Addr::LOCALHOST,
                    payload: &[0; 65507],
                },
                None,
            )
            .unwrap();
        assert_eq!(writer.queued_bytes(), 262028);
        for _ in 0..3 {
            assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        }
        assert_eq!(writer.queued_bytes(), 65507);
        let stats = writer.output().observe();
        assert_eq!((stats.admitted, stats.sent, stats.expired), (4, 0, 3));
        assert_eq!(
            writer.try_queue(
                ProxyRecord {
                    kind: ProxyKind::TcpClose,
                    address: Ipv4Addr::LOCALHOST,
                    payload: &[1]
                },
                None
            ),
            Err(QueueError::Invalid)
        );
    });
}

#[test]
fn concurrent_proxy_senders_share_one_slot_budget_and_wake_the_existing_writer() {
    run(async {
        let (output, mut sink) = tokio::io::duplex(4096);
        let mut writer = ProxyWriter::new(output);
        let first = writer.output();
        let second = first.clone();
        let now = Instant::now();
        let media = ProxyRecord {
            kind: ProxyKind::UdpData,
            address: Ipv4Addr::LOCALHOST,
            payload: b"a",
        };
        let control = ProxyRecord {
            kind: ProxyKind::UdpControl,
            address: Ipv4Addr::LOCALHOST,
            payload: b"c",
        };
        for _ in 0..56 {
            first.try_queue(media, Some(now)).unwrap();
        }
        assert_eq!(second.try_queue(media, None), Err(QueueError::Full));
        for _ in 0..8 {
            second.try_queue(control, None).unwrap();
        }
        assert_eq!(writer.try_queue(control, None), Err(QueueError::Full));
        assert_eq!(writer.queued_bytes(), 64);
        assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        first.try_queue(media, Some(now)).unwrap();
        for _ in 0..55 {
            assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        }
        for _ in 0..8 {
            assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Sent);
        }
        assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        assert_eq!(writer.queued_bytes(), 0);
        let mut bytes = [0; 80];
        sink.read_exact(&mut bytes).await.unwrap();
        assert_eq!(bytes.as_slice(), record(6, [127, 0, 0, 1], b"c").repeat(8));

        let peer = tokio::spawn(async move {
            tokio::task::yield_now().await;
            second.try_queue(control, None).unwrap();
        });
        // This waits for producer notification, not an idle writability polling loop.
        assert_eq!(writer.flush_next().await.unwrap(), FlushOutcome::Sent);
        peer.await.unwrap();
        drop(writer);
        assert_eq!(first.try_queue(control, None), Err(QueueError::Closed));
    });
}

#[test]
fn proxy_writability_waits_for_the_same_packet_budget_without_reserving_credit() {
    run(async {
        let (stream, _sink) = tokio::io::duplex(1);
        let mut writer = ProxyWriter::new(stream);
        let output = writer.output();
        let now = Instant::now();
        let large = [0; 65507];
        for _ in 0..3 {
            output
                .try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpData,
                        address: Ipv4Addr::LOCALHOST,
                        payload: &large,
                    },
                    Some(now),
                )
                .unwrap();
        }
        let ready = output.writable(ProxyKind::UdpData, 100);
        tokio::pin!(ready);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut ready)
                .await
                .is_err()
        );
        // Slots remain, but only87 media bytes fit. A slot-only wait spins here.
        assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut ready)
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(writer.queued_bytes(), 131014);
        // Readiness consumes no slot or byte allowance and cancellation is harmless.
        assert_eq!(output.writable(ProxyKind::UdpData, 65507).await, Ok(()));
        output
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: Ipv4Addr::LOCALHOST,
                    payload: &large,
                },
                Some(now),
            )
            .unwrap();
        assert_eq!(writer.queued_bytes(), 196521);
        assert_eq!(
            output.writable(ProxyKind::TcpClose, 1).await,
            Err(std::io::ErrorKind::InvalidInput)
        );
    });
}

#[test]
fn proxy_writability_observes_media_reserve_and_writer_closure() {
    run(async {
        let (stream, _sink) = tokio::io::duplex(1);
        let mut writer = ProxyWriter::new(stream);
        let output = writer.output();
        let now = Instant::now();
        for _ in 0..56 {
            output
                .try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpData,
                        address: Ipv4Addr::LOCALHOST,
                        payload: b"a",
                    },
                    Some(now),
                )
                .unwrap();
        }
        assert_eq!(output.writable(ProxyKind::UdpControl, 1).await, Ok(()));
        let ready = output.writable(ProxyKind::UdpData, 1);
        tokio::pin!(ready);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut ready)
                .await
                .is_err()
        );
        assert_eq!(writer.flush_one(now).await.unwrap(), FlushOutcome::Skipped);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut ready)
                .await
                .unwrap(),
            Ok(())
        );
        output
            .try_queue(
                ProxyRecord {
                    kind: ProxyKind::UdpData,
                    address: Ipv4Addr::LOCALHOST,
                    payload: b"a",
                },
                Some(now),
            )
            .unwrap();
        let closed = output.writable(ProxyKind::UdpData, 1);
        tokio::pin!(closed);
        assert!(
            tokio::time::timeout(Duration::from_millis(5), &mut closed)
                .await
                .is_err()
        );
        drop(writer);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), &mut closed)
                .await
                .unwrap(),
            Err(std::io::ErrorKind::BrokenPipe)
        );
    });
}
