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
fn canceled_partial_write_keeps_the_header_and_payload_before_later_control() {
    run(async {
        let (output, mut sink) = tokio::io::duplex(7);
        let mut writer = ProxyWriter::new(output);
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
