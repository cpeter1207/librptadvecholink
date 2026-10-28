//! Real account ownership runs only against kernel-assigned loopback endpoints.
use rptadv_echolink::{
    account::{
        network::{AccountNetwork, NetworkError, NetworkEvent, PeerTransport},
        settings::PreparedSettings,
    },
    ffi::*,
    transport::{
        direct::DirectSockets,
        proxy::{connect, session::ProxySession},
    },
    wire::{Channel, ControlPacket, ProxyKind, ProxyRecord, write_control},
};
use std::{mem::size_of, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, UdpSocket},
};

fn span(value: &str) -> ByteSpan {
    ByteSpan {
        data: value.as_ptr(),
        length: value.len(),
    }
}
fn settings(server: &str, refresh: u32, kind: u32) -> Arc<PreparedSettings> {
    let servers = [span(server)];
    let config = AccountConfig {
        enabled: 1,
        node_number: 999_999,
        local_node: span("524950"),
        callsign: span("N0CALL-R"),
        station: StationMetadata {
            name: span("Test site"),
            email: span("op@example.invalid"),
            ..Default::default()
        },
        transport: TransportConfig {
            kind,
            bind_address: span("127.0.0.1"),
            audio_port: 5198,
            proxy_host: span("127.0.0.1"),
            proxy_port: 8100,
            directory_servers: StringList {
                items: servers.as_ptr(),
                count: 1,
            },
        },
        policy: AccountPolicy {
            max_stations: 2,
            max_pending_handshakes: 2,
            heartbeat_interval_ms: 10_000,
            heartbeat_timeout_ms: 100_000,
            connect_timeout_ms: 30_000,
            directory_timeout_ms: 2_000,
            registration_refresh_ms: refresh,
            directory_refresh_ms: 240_000,
            directory_retry_ms: 20_000,
            directory_freshness_ms: 900_000,
            ..Default::default()
        },
        ..Default::default()
    };
    let secrets = AccountSecrets {
        struct_size: size_of::<AccountSecrets>() as u32,
        abi_version: 1,
        directory_password: span("fixture-password"),
        proxy_password: ByteSpan::default(),
    };
    Arc::new(unsafe { PreparedSettings::copy(&config, &secrets) }.unwrap())
}

#[test]
fn account_owner_registers_routes_verified_packets_and_logs_out_without_public_io() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = listener.local_addr().unwrap().to_string();
            let directory = tokio::spawn(async move {
                for (request, response) in [
                    (b"lN0CALL-R\xac\xacfixture-password\rONLINE1.00R(9: 3)\rTest site\rop@example.invalid\r".as_slice(), b"OK".as_slice()),
                    (b"F0\r", b"@@@\n1\nK1ABC-R\nPeer\n123456\n127.0.0.1\n+++\n"),
                    (b"lN0CALL-R\xac\xacfixture-password\rOFF-V3.40\rTest site\r", b"OK"),
                ] {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut actual = vec![0; request.len()];
                    socket.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, request);
                    socket.write_all(response).await.unwrap();
                }
            });
            let sockets = Arc::new(DirectSockets::bind("127.0.0.1:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap()).await.unwrap());
            let (network, mut events) = AccountNetwork::direct(settings(&server, 300_000, 0), Arc::clone(&sockets), || (9, 3)).unwrap();
            loop {
                if matches!(events.recv().await.unwrap(), NetworkEvent::Directory { fresh: true, .. }) { break; }
            }
            let mut connection = network.dial(123456).await.unwrap();
            assert_eq!(connection.connection.identity.callsign, "K1ABC-R");
            let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut bytes = [0; 1024];
            let count = write_control(&ControlPacket::Sdes { source: 0, callsign: "K1ABC-R".into(), station_name: "Peer".into(), speex: false }, &mut bytes).unwrap();
            peer.send_to(&bytes[..count], sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            let received = connection.connection.ingress.recv().await.unwrap();
            assert_eq!(received.payload(), &bytes[..count]);
            assert!(matches!(network.dial(123456).await, Err(NetworkError::Admission(rptadv_echolink::account::Admission::Duplicate))));
            network.remove(123456, connection.connection.generation).unwrap();
            assert!(connection.connection.ingress.recv().await.is_none());
            // A new inbound candidate must pass the directory/policy check again.
            peer.send_to(&bytes[..count], sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            let pending = loop {
                if let NetworkEvent::Pending(pending) = events.recv().await.unwrap() { break pending; }
            };
            let mut incoming = network.accept(pending.token()).await.unwrap();
            assert_ne!(incoming.connection.generation, connection.connection.generation);
            assert_eq!(incoming.connection.initial.as_ref().unwrap().source, peer.local_addr().unwrap());
            // Cleanup from the old generation cannot delete the new route.
            network.remove(123456, connection.connection.generation).unwrap();
            peer.send_to(&bytes[..count], sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            assert_eq!(incoming.connection.ingress.recv().await.unwrap().payload(), &bytes[..count]);
            network.stop().await.unwrap();
            assert!(incoming.connection.ingress.recv().await.is_none());
            directory.await.unwrap();
        }).await.unwrap();
    });
}

#[test]
fn proxy_account_uses_its_selected_proxy_for_directory_and_peer_traffic() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            async fn frame(socket: &mut tokio::net::TcpStream) -> (u8, Vec<u8>) {
                let mut header = [0; 9];
                socket.read_exact(&mut header).await.unwrap();
                assert_eq!(&header[1..5], &[127, 0, 0, 1]);
                let mut payload = vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
                socket.read_exact(&mut payload).await.unwrap();
                (header[0], payload)
            }
            fn bytes(kind: u8, payload: &[u8]) -> Vec<u8> {
                let mut bytes = vec![kind, 0, 0, 0, 0];
                bytes.extend((payload.len() as u32).to_le_bytes());
                bytes.extend(payload);
                bytes
            }
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (dialed, ready) = tokio::sync::oneshot::channel();
            let server = tokio::spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                socket.write_all(b"12345678").await.unwrap();
                socket.read_exact(&mut [0; 25]).await.unwrap();
                for (request, response) in [
                    (b"lN0CALL-R\xac\xacfixture-password\rONLINE1.00R(9: 3)\rTest site\rop@example.invalid\r".as_slice(), b"OK".as_slice()),
                    (b"F0\r", b"@@@\n1\nK1ABC-R\nPeer\n123456\n127.0.0.1\n+++\n"),
                ] {
                    assert_eq!(frame(&mut socket).await, (1, vec![]));
                    socket.write_all(&bytes(4, &[0; 4])).await.unwrap();
                    assert_eq!(frame(&mut socket).await, (2, request.to_vec()));
                    socket.write_all(&bytes(2, response)).await.unwrap();
                    assert_eq!(frame(&mut socket).await, (3, vec![]));
                    socket.write_all(&bytes(3, &[])).await.unwrap();
                }
                ready.await.unwrap();
                let mut sdes = [0; 1024];
                let count = write_control(&ControlPacket::Sdes { source: 0, callsign: "K1ABC-R".into(), station_name: "Peer".into(), speex: false }, &mut sdes).unwrap();
                let mut packet = bytes(6, &sdes[..count]);
                packet[1..5].copy_from_slice(&[127, 0, 0, 1]);
                socket.write_all(&packet).await.unwrap();
                assert_eq!(frame(&mut socket).await, (6, b"reply".to_vec()));
                assert_eq!(frame(&mut socket).await, (1, vec![]));
                socket.write_all(&bytes(4, &[0; 4])).await.unwrap();
                assert_eq!(frame(&mut socket).await, (2, b"lN0CALL-R\xac\xacfixture-password\rOFF-V3.40\rTest site\r".to_vec()));
                socket.write_all(&bytes(2, b"OK")).await.unwrap();
                assert_eq!(frame(&mut socket).await, (3, vec![]));
                socket.write_all(&bytes(3, &[])).await.unwrap();
                assert_eq!(socket.read(&mut [0]).await.unwrap(), 0);
            });
            let transport = connect(address, "N0CALL-R", "fixture-password", Duration::from_secs(2)).await.unwrap();
            let session = ProxySession::start(transport, Duration::from_secs(2)).unwrap();
            let (network, mut events) = AccountNetwork::proxy(settings("127.0.0.1", 300_000, 1), session, || (9, 3)).unwrap();
            while !matches!(events.recv().await.unwrap(), NetworkEvent::Directory { fresh: true, .. }) {}
            let mut peer = network.dial(123456).await.unwrap();
            dialed.send(()).unwrap();
            let packet = peer.connection.ingress.recv().await.unwrap();
            assert_eq!(packet.class(), Channel::Control);
            assert_eq!(packet.source(), "127.0.0.1:0".parse().unwrap());
            let PeerTransport::Proxy(output) = &peer.transport else { panic!("Proxy account leaked direct UDP"); };
            output.try_queue(ProxyRecord { kind: ProxyKind::UdpControl, address: std::net::Ipv4Addr::LOCALHOST, payload: b"reply" }, None).unwrap();
            network.stop().await.unwrap();
            server.await.unwrap();
        }).await.unwrap();
    });
}

#[test]
fn slow_directory_refresh_does_not_suspend_the_accounts_udp_router() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = listener.local_addr().unwrap().to_string();
            let (waiting, paused) = tokio::sync::oneshot::channel();
            let (release, resume) = tokio::sync::oneshot::channel();
            let directory = tokio::spawn(async move {
                let mut waiting = Some(waiting);
                let mut resume = Some(resume);
                let mut registrations = 0;
                loop {
                    let (socket, _) = listener.accept().await.unwrap();
                    let mut socket = BufReader::new(socket);
                    let mut first = Vec::new();
                    socket.read_until(b'\r', &mut first).await.unwrap();
                    if first[0] == b'F' {
                        socket
                            .get_mut()
                            .write_all(b"@@@\n1\nK1ABC-R\nPeer\n123456\n127.0.0.1\n+++\n")
                            .await
                            .unwrap();
                        continue;
                    }
                    let mut status = Vec::new();
                    socket.read_until(b'\r', &mut status).await.unwrap();
                    let logout = status.starts_with(b"OFF-");
                    for _ in 0..if logout { 1 } else { 2 } {
                        let mut field = Vec::new();
                        socket.read_until(b'\r', &mut field).await.unwrap();
                    }
                    if !logout {
                        registrations += 1;
                        if registrations == 2 {
                            waiting.take().unwrap().send(()).unwrap();
                            resume.take().unwrap().await.unwrap();
                        }
                    }
                    // Shutdown may cancel a transaction whose response has not arrived.
                    let _ = socket.get_mut().write_all(b"OK").await;
                    if logout {
                        break;
                    }
                }
            });
            let sockets = Arc::new(
                DirectSockets::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    "127.0.0.1:0".parse().unwrap(),
                )
                .await
                .unwrap(),
            );
            let (network, mut events) =
                AccountNetwork::direct(settings(&server, 40, 0), Arc::clone(&sockets), || (9, 3))
                    .unwrap();
            while !matches!(
                events.recv().await.unwrap(),
                NetworkEvent::Directory { fresh: true, .. }
            ) {}
            let mut connection = network.dial(123456).await.unwrap();
            paused.await.unwrap();
            let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let mut bytes = [0; 1024];
            let count = write_control(
                &ControlPacket::Sdes {
                    source: 0,
                    callsign: "K1ABC-R".into(),
                    station_name: "Peer".into(),
                    speex: false,
                },
                &mut bytes,
            )
            .unwrap();
            peer.send_to(
                &bytes[..count],
                sockets.local_addr(Channel::Control).unwrap(),
            )
            .await
            .unwrap();
            // This must complete BEFORE the directory server is allowed to respond.
            assert_eq!(
                connection
                    .connection
                    .ingress
                    .recv()
                    .await
                    .unwrap()
                    .payload(),
                &bytes[..count]
            );
            release.send(()).unwrap();
            network.stop().await.unwrap();
            directory.await.unwrap();
        })
        .await
        .unwrap();
    });
}
