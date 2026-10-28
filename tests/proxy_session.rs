//! A real loopback Proxy pumps virtual directory TCP and peer datagrams independently.
use rptadv_echolink::{
    transport::{
        ReplyKind,
        proxy::{connect, session::ProxySession},
    },
    wire::{Channel, ProxyKind, ProxyRecord},
};
use std::{net::Ipv4Addr, time::Duration};
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
