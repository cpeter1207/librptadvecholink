//! A real loopback Proxy pumps virtual directory TCP and peer datagrams independently.
use rptadv_echolink::{
    transport::{
        ReplyKind, TransactionError,
        proxy::{ProxyError, connect, session::ProxySession},
    },
    wire::{Channel, ProxyKind, ProxyRecord},
};
use std::{future::Future, net::Ipv4Addr, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

fn frame(kind: u8, ip: [u8; 4], payload: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind];
    bytes.extend(ip);
    bytes.extend((payload.len() as u32).to_le_bytes());
    bytes.extend(payload);
    bytes
}
async fn read_frame(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 9];
    socket.read_exact(&mut header).await.unwrap();
    assert_eq!(&header[1..5], &[127, 0, 0, 1]);
    let mut payload = vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
    socket.read_exact(&mut payload).await.unwrap();
    (header[0], payload)
}

#[test]
fn proxy_directory_and_udp_share_one_authenticated_connection_without_blocking_each_other() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (release, resume) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                let mut auth = [0; 25]; // N0CALL-R newline plus the raw sixteen-byte digest.
                socket.read_exact(&mut auth).await.unwrap();
                assert_eq!(&auth[..9], b"N0CALL-R\n");
                assert_eq!(read_frame(&mut socket).await, (1, vec![]));
                socket.write_all(&frame(4, [0; 4], &[0; 4])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (2, b"F0\r".to_vec()));
                // Send UDP while withholding the directory result.
                socket
                    .write_all(&frame(6, [127, 0, 0, 1], b"control"))
                    .await
                    .unwrap();
                socket
                    .write_all(&frame(5, [127, 0, 0, 1], b"audio"))
                    .await
                    .unwrap();
                assert_eq!(read_frame(&mut socket).await, (6, b"reply".to_vec()));
                resume.await.unwrap();
                socket
                    .write_all(&frame(2, [0; 4], b"@@@\n0:token\n+++\n"))
                    .await
                    .unwrap();
                assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                socket.write_all(&frame(3, [0; 4], &[])).await.unwrap();
                let mut eof = [0];
                assert_eq!(socket.read(&mut eof).await.unwrap(), 0);
            });
            let transport = connect(
                address,
                "N0CALL-R",
                "fixture-password",
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            let mut session = ProxySession::start(transport, Duration::from_secs(2)).unwrap();
            let directory = session.directory();
            let request = tokio::spawn(async move {
                directory
                    .exchange(
                        Ipv4Addr::LOCALHOST,
                        b"F0\r",
                        ReplyKind::Directory,
                        Duration::from_secs(2),
                    )
                    .await
            });
            let packet = session.recv().await.unwrap();
            assert_eq!(packet.class(), Channel::Control);
            assert_eq!(packet.source(), "127.0.0.1:0".parse().unwrap());
            assert_eq!(packet.payload(), b"control");
            let packet = session.recv().await.unwrap();
            assert_eq!(packet.class(), Channel::Audio);
            assert_eq!(packet.payload(), b"audio");
            session
                .output()
                .try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpControl,
                        address: Ipv4Addr::LOCALHOST,
                        payload: b"reply",
                    },
                    None,
                )
                .unwrap();
            release.send(()).unwrap();
            assert_eq!(request.await.unwrap().unwrap(), b"@@@\n0:token\n+++");
            session.stop().await.unwrap();
            server.await.unwrap();
        })
        .await
        .unwrap();
    });
}

#[test]
fn unacknowledged_virtual_close_has_a_finite_transport_failure_not_a_reused_stream() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(3), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                socket.read_exact(&mut [0; 25]).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (1, vec![]));
                socket.write_all(&frame(4, [0; 4], &[0; 4])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (2, b"login".to_vec()));
                socket.write_all(&frame(2, [0; 4], b"OK")).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                // Never acknowledge CLOSE. Reopening this stream would contaminate a new job.
                assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
            });
            let transport = connect(
                address,
                "N0CALL-R",
                "fixture-password",
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            let mut session = ProxySession::start(transport, Duration::from_millis(20)).unwrap();
            let directory = session.directory();
            let request = tokio::spawn(async move {
                directory
                    .exchange(
                        Ipv4Addr::LOCALHOST,
                        b"login",
                        ReplyKind::Login,
                        Duration::from_secs(1),
                    )
                    .await
            });
            assert!(session.recv().await.is_none());
            assert_eq!(
                session.failure(),
                Some(rptadv_echolink::transport::proxy::ProxyError::Timeout)
            );
            assert!(request.await.unwrap().is_err());
            assert_eq!(
                session.stop().await,
                Err(rptadv_echolink::transport::proxy::ProxyError::Timeout)
            );
            server.await.unwrap();
        })
        .await
        .unwrap();
    });
}

#[test]
fn invalid_and_cancelled_directory_requests_do_not_steal_the_next_reply() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (opened, opening) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let (kept, keep_open) = tokio::sync::oneshot::channel::<()>();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                socket.read_exact(&mut [0; 25]).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (1, vec![]));
                opened.send(()).unwrap();
                resume.await.unwrap();
                socket.write_all(&frame(4, [0; 4], &[0; 4])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (2, b"first".to_vec()));
                socket.write_all(&frame(2, [0; 4], b"OK")).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                socket.write_all(&frame(3, [0; 4], &[])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (1, vec![]));
                socket.write_all(&frame(4, [0; 4], &[0; 4])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (2, b"third".to_vec()));
                socket.write_all(&frame(2, [0; 4], b"NO")).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                socket.write_all(&frame(3, [0; 4], &[])).await.unwrap();
                keep_open.await.unwrap();
            });
            let transport = connect(address, "N0CALL-R", "fixture-password", Duration::from_secs(1)).await.unwrap();
            let session = ProxySession::start(transport, Duration::from_secs(1)).unwrap();
            let directory = session.directory();
            for (request, timeout) in [(vec![], Duration::from_secs(1)), (vec![0; 65_537], Duration::from_secs(1)), (vec![1], Duration::ZERO), (vec![1], Duration::MAX)] {
                assert_eq!(directory.exchange(Ipv4Addr::LOCALHOST, &request, ReplyKind::Login, timeout).await, Err(TransactionError::Malformed));
            }
            let first = directory.exchange(Ipv4Addr::LOCALHOST, b"first", ReplyKind::Login, Duration::from_secs(2));
            tokio::pin!(first);
            tokio::select! { result = &mut first => panic!("premature first reply: {result:?}"), _ = opening => {} }
            let mut cancelled = Box::pin(directory.exchange(Ipv4Addr::LOCALHOST, b"cancelled", ReplyKind::Login, Duration::from_secs(2)));
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(cancelled.as_mut().poll(cx).is_pending())).await);
            assert_eq!(directory.exchange(Ipv4Addr::LOCALHOST, b"full", ReplyKind::Login, Duration::from_secs(2)).await, Err(TransactionError::Limit));
            // Expire the queued caller without cancelling the active transaction.
            drop(cancelled);
            release.send(()).unwrap();
            assert_eq!(first.await.unwrap(), b"OK");
            assert_eq!(directory.exchange(Ipv4Addr::LOCALHOST, b"third", ReplyKind::Login, Duration::from_secs(2)).await, Err(TransactionError::Rejected));
            session.stop().await.unwrap();
            kept.send(()).unwrap();
            server.await.unwrap();
            assert_eq!(directory.exchange(Ipv4Addr::LOCALHOST, b"closed", ReplyKind::Login, Duration::from_secs(1)).await, Err(TransactionError::Io(std::io::ErrorKind::NotConnected)));
        }).await.unwrap();
    });
}

#[test]
fn session_stop_and_invalid_start_release_the_authenticated_socket() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for mode in 0..3 {
        let (transport, server) = runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                socket.read_exact(&mut [0; 25]).await.unwrap();
                assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
            });
            (
                connect(
                    address,
                    "N0CALL-R",
                    "fixture-password",
                    Duration::from_secs(1),
                )
                .await
                .unwrap(),
                server,
            )
        });
        match mode {
            0 => assert!(matches!(
                ProxySession::start(transport, Duration::from_secs(1)),
                Err(ProxyError::Invalid)
            )),
            1 => runtime.block_on(async {
                assert!(matches!(
                    ProxySession::start(transport, Duration::ZERO),
                    Err(ProxyError::Invalid)
                ));
            }),
            _ => runtime.block_on(async {
                ProxySession::start(transport, Duration::from_secs(1))
                    .unwrap()
                    .stop()
                    .await
                    .unwrap();
            }),
        }
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
        });
    }
}

#[test]
fn directory_failure_categories_survive_proxy_close() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        for (response, expected) in [
            (
                frame(4, [0; 4], &1u32.to_le_bytes()),
                TransactionError::Rejected,
            ),
            (frame(1, [0; 4], &[]), TransactionError::Malformed),
        ] {
            tokio::time::timeout(Duration::from_secs(3), async {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let address = listener.local_addr().unwrap();
                let (release, keep_open) = tokio::sync::oneshot::channel::<()>();
                let server = tokio::spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    socket.write_all(b"12345678").await.unwrap();
                    socket.read_exact(&mut [0; 25]).await.unwrap();
                    assert_eq!(read_frame(&mut socket).await, (1, vec![]));
                    socket.write_all(&response).await.unwrap();
                    assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                    socket.write_all(&frame(3, [0; 4], &[])).await.unwrap();
                    keep_open.await.unwrap();
                });
                let transport = connect(
                    address,
                    "N0CALL-R",
                    "fixture-password",
                    Duration::from_secs(1),
                )
                .await
                .unwrap();
                let session = ProxySession::start(transport, Duration::from_secs(1)).unwrap();
                assert_eq!(
                    session
                        .directory()
                        .exchange(
                            Ipv4Addr::LOCALHOST,
                            b"login",
                            ReplyKind::Login,
                            Duration::from_secs(1)
                        )
                        .await,
                    Err(expected)
                );
                assert_eq!(session.failure(), None);
                session.stop().await.unwrap();
                release.send(()).unwrap();
                server.await.unwrap();
            })
            .await
            .unwrap();
        }
    });
}

#[test]
fn a_stalled_proxy_retries_directory_control_after_writer_credit_returns() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(10), async {
            let socket = tokio::net::TcpSocket::new_v4().unwrap();
            socket.set_recv_buffer_size(1024).unwrap();
            socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
            let listener = socket.listen(1).unwrap();
            let address = listener.local_addr().unwrap();
            let (release, resume) = tokio::sync::oneshot::channel();
            let (done, keep_open) = tokio::sync::oneshot::channel::<()>();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                socket.read_exact(&mut [0; 25]).await.unwrap();
                resume.await.unwrap();
                loop {
                    let (kind, payload) = read_frame(&mut socket).await;
                    if kind == 1 { break; }
                    assert_eq!(kind, 6);
                    assert!(!payload.is_empty());
                }
                socket.write_all(&frame(4, [0; 4], &[0; 4])).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (2, b"login".to_vec()));
                socket.write_all(&frame(2, [0; 4], b"OK")).await.unwrap();
                assert_eq!(read_frame(&mut socket).await, (3, vec![]));
                socket.write_all(&frame(3, [0; 4], &[])).await.unwrap();
                keep_open.await.unwrap();
            });
            let transport = connect(address, "N0CALL-R", "fixture-password", Duration::from_secs(1)).await.unwrap();
            let session = ProxySession::start(transport, Duration::from_secs(2)).unwrap();
            let output = session.output();
            let payload = vec![0; 65_507];
            loop {
                while output.try_queue(ProxyRecord { kind: ProxyKind::UdpControl, address: Ipv4Addr::LOCALHOST, payload: &payload }, None).is_ok() {}
                let stats = output.observe();
                if stats.stalls > 0 && stats.head_age_ms >= 20 && stats.queued_bytes > 0 { break; }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            // Exhaust the remaining slots as well as byte credit, while the
            // server is still refusing TCP reads. This is real writer backpressure.
            while output.try_queue(ProxyRecord { kind: ProxyKind::UdpControl, address: Ipv4Addr::LOCALHOST, payload: &[1] }, None).is_ok() {}
            let directory = session.directory();
            let request = directory.exchange(Ipv4Addr::LOCALHOST, b"login", ReplyKind::Login, Duration::from_secs(5));
            tokio::pin!(request);
            tokio::select! { result = &mut request => panic!("reply before TCP drain: {result:?}"), _ = tokio::time::sleep(Duration::from_millis(10)) => {} }
            release.send(()).unwrap();
            assert_eq!(request.await.unwrap(), b"OK");
            assert_eq!(session.failure(), None);
            session.stop().await.unwrap();
            done.send(()).unwrap();
            server.await.unwrap();
        }).await.unwrap();
    });
}
