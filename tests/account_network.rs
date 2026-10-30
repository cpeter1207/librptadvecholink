//! Real account ownership runs only against kernel-assigned loopback endpoints.
use rptadv_echolink::{
    account::{
        network::{AccountNetwork, NetworkError, NetworkEvent, PeerTransport},
        settings::PreparedSettings,
    },
    ffi::*,
    transport::direct::DirectSockets,
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
    settings_at(server, refresh, kind, 5198, 8100)
}
fn settings_at(
    server: &str,
    refresh: u32,
    kind: u32,
    audio_port: u16,
    proxy_port: u16,
) -> Arc<PreparedSettings> {
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
            audio_port,
            proxy_host: span("127.0.0.1"),
            proxy_port,
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
fn an_ipv6_only_directory_records_failure_without_dialing_an_unverified_peer() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(3), async {
                let sockets = Arc::new(
                    DirectSockets::bind(
                        "127.0.0.1:0".parse().unwrap(),
                        "127.0.0.1:0".parse().unwrap(),
                    )
                    .await
                    .unwrap(),
                );
                let (network, mut events) =
                    AccountNetwork::direct(settings("::1", 300_000, 0), sockets, || (9, 3))
                        .unwrap();
                assert!(matches!(
                    events.recv().await,
                    Some(NetworkEvent::Directory {
                        failures: 1,
                        fresh: false,
                        ..
                    })
                ));
                assert!(matches!(
                    network.dial(123456).await,
                    Err(NetworkError::Admission(
                        rptadv_echolink::account::Admission::Unverified
                    ))
                ));
                assert_eq!(
                    network
                        .control()
                        .observe()
                        .await
                        .unwrap()
                        .directory
                        .failures,
                    1
                );
                network.stop().await.unwrap();
            })
            .await
            .unwrap();
        });
}

#[test]
fn direct_activation_rejects_proxy_settings_and_requires_an_entered_runtime() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let sockets = runtime.block_on(async {
        Arc::new(
            DirectSockets::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap(),
        )
    });
    assert!(matches!(
        AccountNetwork::direct(
            settings("127.0.0.1", 300_000, 0),
            Arc::clone(&sockets),
            || (9, 3)
        ),
        Err(NetworkError::Invalid)
    ));
    runtime.block_on(async {
        assert!(matches!(
            AccountNetwork::direct(settings("127.0.0.1", 300_000, 1), sockets, || (9, 3)),
            Err(NetworkError::Invalid)
        ));
    });
}

#[test]
fn live_policy_and_metadata_update_preserves_the_existing_route() {
    tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap().block_on(async {
        tokio::time::timeout(Duration::from_secs(5), async {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let server = listener.local_addr().unwrap().to_string();
            let (renewed, renewal) = tokio::sync::oneshot::channel();
            let directory = tokio::spawn(async move {
                let mut renewed = Some(renewed);
                for (request, response) in [
                    (b"lN0CALL-R\xac\xacfixture-password\rONLINE1.00R(9: 3)\rTest site\rop@example.invalid\r".as_slice(), b"OK".as_slice()),
                    (b"F0\r", b"@@@\n1\nK1ABC-R\nPeer\n123456\n127.0.0.1\n+++\n"),
                    (b"lN0CALL-R\xac\xacfixture-password\rONLINE1.00R(9: 3)\rUpdated site\rop@example.invalid\r", b"OK"),
                    (b"lN0CALL-R\xac\xacfixture-password\rOFF-V3.40\rUpdated site\r", b"OK"),
                ] {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut actual = vec![0; request.len()];
                    socket.read_exact(&mut actual).await.unwrap();
                    assert_eq!(actual, request);
                    socket.write_all(response).await.unwrap();
                    if request.windows(12).any(|bytes| bytes == b"Updated site") {
                        if let Some(ready) = renewed.take() { ready.send(()).unwrap(); }
                    }
                }
            });
            let sockets = Arc::new(DirectSockets::bind("127.0.0.1:0".parse().unwrap(), "127.0.0.1:0".parse().unwrap()).await.unwrap());
            let initial = settings(&server, 300_000, 0);
            let (network, mut events) = AccountNetwork::direct(Arc::clone(&initial), Arc::clone(&sockets), || (9, 3)).unwrap();
            while !matches!(events.recv().await.unwrap(), NetworkEvent::Directory { fresh: true, .. }) {}
            let commands = network.control();
            let online = commands.observe().await.unwrap();
            assert!(online.fresh);
            assert!(online.directory.registered);
            assert_eq!(online.admitted, 0, "account health exists before a QSO");
            assert_eq!(online.directory.operations, 2);
            let mut connection = commands.dial(123456).await.unwrap();
            let generation = connection.connection.generation;
            let mut candidate = unsafe { std::ptr::read(initial.as_abi()) };
            candidate.station.name = span("Updated site");
            candidate.policy.directory_freshness_ms = 600_000;
            let deny = [span("K*")];
            candidate.policy.deny = StringList { items: deny.as_ptr(), count: deny.len() };
            let secrets = AccountSecrets { struct_size: size_of::<AccountSecrets>() as u32, abi_version: 1,
                directory_password: span("fixture-password"), proxy_password: ByteSpan::default() };
            let changed = Arc::new(unsafe { PreparedSettings::copy(&candidate, &secrets) }.unwrap());
            commands.reconfigure(changed).await.unwrap();
            renewal.await.unwrap();
            let remote = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            remote.send_to(b"malformed", sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            let mut bytes = [0; 1024];
            let count = write_control(&ControlPacket::Sdes { source: 0, callsign: "K1ABC-R".into(), station_name: "Peer".into(), speex: false }, &mut bytes).unwrap();
            remote.send_to(&bytes[..count], sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            assert_eq!(connection.connection.ingress.recv().await.unwrap().payload(), &bytes[..count]);
            let live = commands.observe().await.unwrap();
            assert_eq!((live.admitted, live.routed_packets, live.malformed_packets), (1, 1, 1));
            assert_eq!(live.directory.failures, 0);
            assert!(live.fresh);
            assert_eq!(connection.connection.generation, generation);
            let capacity = connection.connection.ingress.observer().observe().capacity_packets;
            for _ in 0..=capacity {
                remote.send_to(&bytes[..count], sockets.local_addr(Channel::Control).unwrap()).await.unwrap();
            }
            let overloaded = loop {
                let observed = commands.observe().await.unwrap();
                if observed.routed_packets + observed.dropped_packets >= u64::from(capacity) + 3 { break observed; }
                tokio::task::yield_now().await;
            };
            assert_eq!(overloaded.ingress_full_packets, 1);
            while connection.connection.ingress.try_recv().is_some() {}
            candidate.callsign = span("N1OTHER-R");
            let exclusive = Arc::new(unsafe { PreparedSettings::copy(&candidate, &secrets) }.unwrap());
            assert_eq!(commands.reconfigure(exclusive).await, Err(NetworkError::Invalid),
                "live owner updates cannot silently change the account identity");
            assert!(matches!(commands.dial(123456).await, Err(NetworkError::Admission(rptadv_echolink::account::Admission::Denied))));
            assert_eq!(commands.directory().by_node(123456, std::time::Instant::now()).unwrap().callsign, "K1ABC-R");
            assert_eq!(commands.directory().by_node(123456, std::time::Instant::now() + Duration::from_secs(601)),
                Err(rptadv_echolink::directory::AdmissionError::Stale));
            network.stop().await.unwrap();
            assert!(connection.connection.ingress.recv().await.is_none());
            directory.await.unwrap();
        }).await.unwrap();
    });
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
            let commands = network.control();
            let directory_view = network.directory();
            assert_eq!(directory_view.by_node(123456, std::time::Instant::now()), Err(rptadv_echolink::directory::AdmissionError::Stale));
            loop {
                if matches!(events.recv().await.unwrap(), NetworkEvent::Directory { fresh: true, .. }) { break; }
            }
            let verified = directory_view.by_node(123456, std::time::Instant::now()).unwrap();
            assert_eq!(verified.callsign, "K1ABC-R");
            assert_eq!(directory_view.lookup("K1ABC-R", "192.0.2.1".parse().unwrap(), std::time::Instant::now()), Err(rptadv_echolink::directory::AdmissionError::Address));
            // No yield on this current-thread runtime: four commands fill the
            // configured account queue without slowing another account.
            for node in 1..=4 { commands.remove(node, 1).unwrap(); }
            assert_eq!(commands.remove(5, 1), Err(NetworkError::Full));
            assert!(matches!(commands.observe().await, Err(NetworkError::Full)));
            loop {
                match commands.observe().await {
                    Ok(status) => { assert_eq!(status.admitted, 0); break; },
                    Err(NetworkError::Full) => tokio::task::yield_now().await,
                    Err(error) => panic!("failed to recover command credit: {error:?}"),
                }
            }
            // Cancel after command admission but before this account owner
            // runs. Its failed reply delivery must retire the reserved route.
            let mut cancelled = Box::pin(commands.dial(123456));
            assert!(std::future::poll_fn(|cx| std::task::Poll::Ready(cancelled.as_mut().poll(cx).is_pending())).await);
            drop(cancelled);
            let mut connection = commands.dial(123456).await.unwrap();
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
            // A cancelled dial/admission must relinquish its route even when
            // its reply was delivered immediately before cancellation.
            drop(incoming);
            let mut replacement = tokio::time::timeout(Duration::from_millis(100), async {
                loop {
                    match commands.dial(123456).await {
                        Ok(peer) => break peer,
                        Err(NetworkError::Admission(rptadv_echolink::account::Admission::Duplicate)) => tokio::task::yield_now().await,
                        Err(error) => panic!("unexpected admission outcome: {error:?}"),
                    }
                }
            }).await.expect("abandoned route was not retired");
            replacement.talker.offer(std::time::Instant::now()).unwrap();
            while replacement.talker.selection().unwrap().is_none() {
                tokio::task::yield_now().await;
            }
            drop(replacement);
            tokio::time::timeout(Duration::from_secs(2), async {
                while commands.observe().await.unwrap().selected.is_some() {
                    tokio::task::yield_now().await;
                }
            }).await.expect("retiring a selected lease stranded its account grant");
            let mut replacement = network.dial(123456).await.unwrap();
            replacement.talker.offer(std::time::Instant::now()).unwrap();
            loop {
                if let Some(selection) = replacement.talker.selection().unwrap() {
                    assert_eq!(selection, rptadv_echolink::account::Selection::Selected);
                    break;
                }
                tokio::task::yield_now().await;
            }
            network.stop().await.unwrap();
            assert!(matches!(commands.dial(123456).await, Err(NetworkError::Stopped)));
            assert!(replacement.connection.ingress.recv().await.is_none());
            directory.await.unwrap();
        }).await.unwrap();
    });
}

#[test]
fn terminal_proxy_cause_survives_a_full_account_event_queue_and_stop_remains_bounded() {
    use rptadv_echolink::transport::proxy::ProxyError;
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(5), async {
                async fn frame(socket: &mut tokio::net::TcpStream) -> u8 {
                    let mut header = [0; 9];
                    socket.read_exact(&mut header).await.unwrap();
                    let mut payload =
                        vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
                    socket.read_exact(&mut payload).await.unwrap();
                    header[0]
                }
                async fn send(socket: &mut tokio::net::TcpStream, kind: u8, payload: &[u8]) {
                    let mut packet = vec![kind, 0, 0, 0, 0];
                    packet.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                    packet.extend_from_slice(payload);
                    socket.write_all(&packet).await.unwrap();
                }
                for drain in [true, false] {
                    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                    let port = listener.local_addr().unwrap().port();
                    let (close, wait) = tokio::sync::oneshot::channel();
                    let server = tokio::spawn(async move {
                        let (mut socket, _) = listener.accept().await.unwrap();
                        socket.write_all(b"12345678").await.unwrap();
                        socket.read_exact(&mut [0; 25]).await.unwrap();
                        for response in [b"OK".as_slice(), b"@@@\n0\n+++\n"] {
                            assert_eq!(frame(&mut socket).await, 1);
                            send(&mut socket, 4, &[0; 4]).await;
                            assert_eq!(frame(&mut socket).await, 2);
                            send(&mut socket, 2, response).await;
                            assert_eq!(frame(&mut socket).await, 3);
                            send(&mut socket, 3, &[]).await;
                        }
                        wait.await.unwrap(); // Close with both directory health events still queued.
                    });
                    let base = settings_at("127.0.0.1", 300000, 1, 5198, port);
                    // C scalars and borrowed spans only; keep base alive through copy.
                    let mut config = unsafe { std::ptr::read(base.as_abi()) };
                    config.policy.max_stations = 1;
                    config.policy.max_pending_handshakes = 1;
                    let secret = AccountSecrets {
                        struct_size: size_of::<AccountSecrets>() as u32,
                        abi_version: 1,
                        directory_password: span("fixture-password"),
                        proxy_password: ByteSpan::default(),
                    };
                    let config =
                        Arc::new(unsafe { PreparedSettings::copy(&config, &secret) }.unwrap());
                    let (network, mut events) =
                        AccountNetwork::open(config, || (9, 3)).await.unwrap();
                    while events.len() < 2 {
                        tokio::task::yield_now().await;
                    }
                    close.send(()).unwrap();
                    server.await.unwrap();
                    // Let the reader and account owner observe EOF before freeing event credit.
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    if drain {
                        assert!(matches!(
                            events.recv().await,
                            Some(NetworkEvent::Directory { .. })
                        ));
                        assert!(matches!(
                            events.recv().await,
                            Some(NetworkEvent::Directory { .. })
                        ));
                        assert!(
                            matches!(
                                events.recv().await,
                                Some(NetworkEvent::ProxyLost(ProxyError::Closed))
                            ),
                            "terminal cause was silently dropped behind ordinary health events"
                        );
                        let commands = network.control();
                        assert!(matches!(
                            commands.observe().await,
                            Err(NetworkError::Stopped)
                        ));
                        assert_eq!(commands.remove(1, 1), Err(NetworkError::Stopped));
                    }
                    network.stop().await.unwrap();
                }
            })
            .await
            .unwrap();
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
            let (network, mut events) = AccountNetwork::open(settings_at("127.0.0.1", 300_000, 1, 5198, address.port()), || (9, 3)).await.unwrap();
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
fn activation_preserves_the_direct_bind_error_without_trying_another_transport() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let occupied = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let config = settings_at(
            "127.0.0.1:1",
            300_000,
            0,
            occupied.local_addr().unwrap().port(),
            8100,
        );
        assert!(matches!(
            AccountNetwork::open(config, || (9, 3)).await,
            Err(NetworkError::Io(std::io::ErrorKind::AddrInUse))
        ));
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
