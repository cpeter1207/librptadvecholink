//! Terminal account-owner boundaries use real sockets but no directory login.
use super::*;
use crate::settings_fixture::{secrets, settings, span};

/// The actual command/router owner, serviced one turn at a time by its test.
/// No directory worker or test replacement of the command implementation.
pub(crate) struct Harness {
    owner: Owner,
    pub(crate) control: NetworkControl,
    pub(crate) events: mpsc::Receiver<NetworkEvent>,
}

#[tokio::test(flavor = "current_thread")]
async fn aprs_uses_the_selected_transport_and_live_blank_disables_it() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, UdpSocket},
    };
    for proxy in [false, true] {
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let endpoint = if proxy {
            "127.0.0.2:5199".to_owned()
        } else {
            udp.local_addr().unwrap().to_string()
        };
        let servers = [span("127.0.0.1:1")];
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.transport.kind = u32::from(proxy);
        config.aprs_server = span(&endpoint);
        let settings = Arc::new(
            unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap(),
        );
        let mut harness = Harness::new(settings).await;
        let mut server = None;
        if proxy {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            server = Some(tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                stream.write_all(b"12345678").await.unwrap();
                stream.read_exact(&mut [0; 24]).await.unwrap();
                loop {
                    let mut header = [0; 9];
                    stream.read_exact(&mut header).await.unwrap();
                    let mut payload =
                        vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
                    stream.read_exact(&mut payload).await.unwrap();
                    if header[0] == 6 {
                        assert_eq!(&header[1..5], &[127, 0, 0, 2]);
                        return (payload, stream);
                    }
                }
            }));
            let transport = connect(
                address,
                "K1ABC-R",
                "fixture-password",
                Duration::from_secs(1),
            )
            .await
            .unwrap();
            harness.owner.transport =
                Transport::Proxy(ProxySession::start(transport, Duration::from_secs(1)).unwrap());
        }
        assert!(harness.owner.aprs_due.unwrap() > Instant::now() + Duration::from_secs(9));
        harness.owner.aprs_due = Some(Instant::now());
        let mut proxy_socket = None;
        let received = tokio::time::timeout(Duration::from_secs(2), async {
            let run = harness.owner.connected();
            tokio::pin!(run);
            let mut bytes = [0; 512];
            tokio::select! {
                _ = &mut run => panic!("metadata must not stop an account"),
                result = async {
                    match server {
                        Some(server) => {
                            let (payload, stream) = server.await.unwrap();
                            proxy_socket = Some(stream);
                            payload
                        },
                        None => {
                            let (count,_) = udp.recv_from(&mut bytes).await.unwrap();
                            bytes[..count].to_vec()
                        }
                    }
                } => result,
            }
        })
        .await
        .unwrap();
        // Hand-derived RR/SDES fixture, independent of the producer's builders.
        let mut expected = vec![
            0x80, 201, 0, 1, 0, 0, 0, 0, 0x81, 202, 0, 19, 0, 0, 0, 0, 1, 15,
        ];
        expected.extend_from_slice(b"K1ABC-R/K1ABC-R");
        expected.extend_from_slice(&[5, 48]);
        expected.extend_from_slice(b")EL-K1ABC-!4500.00NE09300.00W0PHG3204/146520/114");
        expected.extend_from_slice(&[0; 5]);
        assert_eq!(received, expected);
        assert!(harness.owner.aprs_due.unwrap() > Instant::now() + Duration::from_secs(599));
        config.aprs_server = span("");
        let settings = Arc::new(
            unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap(),
        );
        harness.owner.reconfigure(settings).unwrap();
        assert!(harness.owner.aprs_due.is_none());
        if let Transport::Proxy(session) = harness.owner.transport {
            session.stop().await.unwrap();
        }
        drop(proxy_socket);
    }
}
impl Harness {
    pub(crate) async fn new(settings: Arc<PreparedSettings>) -> Self {
        let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
        snapshot
            .update(
                b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n",
                Instant::now(),
            )
            .unwrap();
        let view = DirectoryView::new(&snapshot);
        let (commands, input) = mpsc::channel(4);
        let (events, output) = mpsc::channel(4);
        let (stopping, stop) = watch::channel(false);
        let sockets = Arc::new(
            DirectSockets::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap(),
        );
        let directory = super::super::directory::tests::at_token(0);
        Self {
            owner: Owner {
                aprs_due: aprs::first_due(&settings),
                aprs_work: None,
                settings,
                transport: Transport::Direct(sockets),
                civil_time: || (9, 3),
                coordinator: Coordinator::new(
                    Router::new(
                        Account::new(1, Vec::new(), Vec::new()).unwrap(),
                        1,
                        Duration::from_secs(30),
                    )
                    .unwrap(),
                )
                .unwrap(),
                directory,
                snapshot,
                view: view.clone(),
                peers: BTreeMap::new(),
                pending_dials: BTreeMap::new(),
                commands: input,
                events,
                stop,
                retired: Arc::new(Notify::new()),
                routed_packets: 0,
                dropped_packets: 0,
                ingress_full_packets: 0,
            },
            control: NetworkControl {
                directory: view,
                commands,
                stopping,
            },
            events: output,
        }
    }

    pub(crate) async fn service(&mut self) {
        let command = self
            .owner
            .commands
            .recv()
            .await
            .expect("an admitted command");
        self.owner.command(command);
    }

    /// Generate a genuine pending token by parsing and routing its SDES.
    pub(crate) fn pending(&mut self) {
        let mut packet = [0; 512];
        let count = crate::wire::write_control(
            &crate::wire::ControlPacket::Sdes {
                source: 123456,
                callsign: "W1XYZ-R".into(),
                station_name: "Peer".into(),
                speex: false,
            },
            &mut packet,
        )
        .unwrap();
        self.owner.route(
            Channel::Control,
            "127.0.0.2:5199".parse().unwrap(),
            Instant::now(),
            &packet[..count],
        );
    }

    pub(crate) fn stale_directory(&mut self) {
        self.owner.snapshot.set_freshness(Duration::ZERO);
        self.owner.view.publish(&self.owner.snapshot);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn pending_dials_share_finite_credit_and_revalidate_before_publication() {
    fn dial(
        owner: &mut Owner,
        node: u32,
    ) -> oneshot::Receiver<Result<PeerConnection, NetworkError>> {
        let (reply, response) = oneshot::channel();
        owner.command(Command::Dial(node, reply));
        response
    }
    let servers = [span("127.0.0.1:1")];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.policy.max_pending_handshakes = 1;
    let keys = secrets("fixture-password");
    let prepared = Arc::new(unsafe { PreparedSettings::copy(&config, &keys) }.unwrap());
    let mut harness = Harness::new(prepared).await;
    let first = dial(&mut harness.owner, 999001);
    assert_eq!(
        dial(&mut harness.owner, 999001).await.unwrap().err(),
        Some(NetworkError::Admission(Admission::Duplicate))
    );
    assert_eq!(
        dial(&mut harness.owner, 999002).await.unwrap().err(),
        Some(NetworkError::Admission(Admission::Unverified))
    );
    harness.owner.retry_dials(Instant::now());
    assert_eq!(
        harness.owner.pending_dials.len(),
        1,
        "missing station remains pending without another task"
    );
    drop(first);
    harness.owner.retry_dials(Instant::now());
    assert!(
        harness.owner.pending_dials.is_empty(),
        "caller cancellation releases its exact credit"
    );
    let expired = dial(&mut harness.owner, 999003);
    let deadline = harness.owner.pending_dials[&999003].deadline;
    harness.owner.retry_dials(deadline);
    assert_eq!(
        expired.await.unwrap().err(),
        Some(NetworkError::Admission(Admission::Unverified))
    );
    harness.stale_directory();
    let denied = dial(&mut harness.owner, 123456);
    let deny = [span("W*")];
    config.policy.deny = crate::ffi::StringList {
        items: deny.as_ptr(),
        count: 1,
    };
    let changed = unsafe { PreparedSettings::copy(&config, &keys) }.unwrap();
    harness.owner.coordinator.router().update_policy(&changed);
    harness
        .owner
        .snapshot
        .set_freshness(Duration::from_secs(900));
    harness.owner.retry_dials(Instant::now());
    assert_eq!(
        denied.await.unwrap().err(),
        Some(NetworkError::Admission(Admission::Denied))
    );
    let pending = dial(&mut harness.owner, 999004);
    harness.owner.transport = Transport::Offline(ProxyError::Closed);
    assert!(harness.owner.transport.peer().is_none());
    for incoming in [false, true] {
        let (reply, response) = oneshot::channel();
        // Both command identities are positive; transport retirement rejects
        // before directory admission or pending-token lookup.
        harness.owner.command(if incoming {
            Command::Accept(1, reply)
        } else {
            Command::Dial(1, reply)
        });
        assert_eq!(
            response.await.unwrap().err(),
            Some(NetworkError::Proxy(ProxyError::Closed))
        );
    }
    harness.owner.commands.close();
    assert_eq!(
        harness.control.remove(123456, 1),
        Err(NetworkError::Stopped)
    );
    drop(harness.control);
    assert!(!harness.owner.recover_proxy(ProxyError::Closed).await);
    assert_eq!(
        pending.await.unwrap().err(),
        Some(NetworkError::Proxy(ProxyError::Closed))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn unavailable_proxy_retries_with_original_transport_failure_until_stopped() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let servers = [span("127.0.0.1:1")];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.transport.kind = 1;
    config.transport.proxy_host = span("127.0.0.1");
    config.transport.proxy_port = port;
    config.policy.directory_retry_ms = 1;
    config.policy.directory_timeout_ms = 10;
    let settings =
        Arc::new(unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap());
    let mut harness = Harness::new(settings).await;
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            harness.owner.recover_proxy(ProxyError::Closed)
        )
        .await
        .is_err()
    );
    assert!(matches!(
        harness.owner.transport,
        Transport::Offline(ProxyError::Io(io::ErrorKind::ConnectionRefused))
    ));
    assert!(matches!(
        harness.events.recv().await,
        Some(NetworkEvent::ProxyLost(ProxyError::Closed))
    ));
    assert!(
        harness.events.try_recv().is_err(),
        "retry failure does not flood account events"
    );
    harness.control.stopping.send_replace(true);
    assert!(!harness.owner.recover_proxy(ProxyError::Closed).await);
}

#[tokio::test(flavor = "current_thread")]
async fn aprs_ipv6_only_resolution_is_a_nonfatal_publication_error() {
    // Debian's loopback-only IPv6 alias: no test station leaves this host.
    let servers = [span("127.0.0.1:1")];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.aprs_server = span("ip6-localhost:5199");
    let settings =
        Arc::new(unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap());
    let harness = Harness::new(Arc::clone(&settings)).await;
    assert_eq!(
        aprs::publish(settings, harness.owner.transport.peer().unwrap()).await,
        Err(ProxyError::Io(io::ErrorKind::AddrNotAvailable))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn aprs_proxy_credit_is_bounded_and_failure_does_not_stop_routing() {
    use crate::{
        transport::proxy::{ProxyWriter, QueueError},
        wire::{ProxyKind, ProxyRecord},
    };
    let servers = [span("127.0.0.1:1")];
    for mode in 0..4 {
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.aprs_server = span("127.0.0.2:5199");
        config.policy.directory_timeout_ms = if mode == 2 { 10 } else { 1000 };
        let settings = Arc::new(
            unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap(),
        );
        let mut writer = Some(ProxyWriter::new(tokio::io::sink()));
        let output = writer.as_ref().unwrap().output();
        let record = ProxyRecord {
            kind: ProxyKind::UdpControl,
            address: std::net::Ipv4Addr::LOCALHOST,
            payload: &[0],
        };
        while output.try_queue(record, None) == Ok(()) {}
        assert_eq!(output.try_queue(record, None), Err(QueueError::Full));
        if mode == 3 {
            drop(writer.take());
        }
        let mut publication = Box::pin(aprs::publish(
            Arc::clone(&settings),
            PeerTransport::Proxy(output.clone()),
        ));
        let expected = match mode {
            0 | 1 => {
                assert!(
                    tokio::time::timeout(Duration::from_millis(20), &mut publication)
                        .await
                        .is_err()
                );
                if mode == 0 {
                    writer
                        .as_mut()
                        .unwrap()
                        .flush_one(Instant::now())
                        .await
                        .unwrap();
                    Ok(())
                } else {
                    drop(writer.take());
                    Err(ProxyError::Io(io::ErrorKind::BrokenPipe))
                }
            }
            2 => Err(ProxyError::Timeout),
            _ => Err(ProxyError::Closed),
        };
        assert_eq!(publication.await, expected);
        assert_eq!(output.observe().admitted, if mode == 0 { 65 } else { 64 });

        // Use the real failed publication as account work, not a synthetic
        // return code: a closed metadata writer must not stop an otherwise
        // available account from answering its control/status port.
        drop(writer);
        let mut harness = Harness::new(Arc::clone(&settings)).await;
        harness.owner.aprs_due = None;
        harness.owner.aprs_work = Some(Box::pin(aprs::publish(
            settings,
            PeerTransport::Proxy(output),
        )));
        let (reply, response) = oneshot::channel();
        harness.owner.command(Command::Observe(reply));
        assert_eq!(response.await.unwrap().unwrap().admitted, 0);
        let run = harness.owner.connected();
        tokio::pin!(run);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut run)
                .await
                .is_err(),
            "APRS failure must leave routing active"
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn finite_directory_tokens_and_closed_commands_quiesce_without_network_work() {
    for exhausted in [false, true] {
        let servers = [span("127.0.0.1:1")];
        let settings = Arc::new(
            unsafe {
                PreparedSettings::copy(
                    &settings("K1ABC-R", "Loopback", &servers, &[]),
                    &secrets("fixture-password"),
                )
            }
            .unwrap(),
        );
        let mut directory =
            super::super::directory::tests::at_token(if exhausted { u64::MAX } else { 0 });
        let snapshot = DirectorySnapshot::new(Duration::from_secs(900));
        if !exhausted {
            // A completed failed registration is in its real retry interval;
            // the closed command port completes before another login is due.
            let now = Instant::now();
            let job = directory.poll(&snapshot, now).unwrap().unwrap();
            let mut unchanged = snapshot.clone();
            directory.finish(
                job.id,
                Err(TransactionError::Io(io::ErrorKind::TimedOut)),
                &mut unchanged,
                now,
            );
        }
        let sockets = Arc::new(
            DirectSockets::bind(
                "127.0.0.1:0".parse().unwrap(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .unwrap(),
        );
        let (commands, input) = mpsc::channel(1);
        let commands = exhausted.then_some(commands);
        let (events, mut output) = mpsc::channel(1);
        let (stopping, stop) = watch::channel(false);
        let owner = Owner {
            aprs_due: aprs::first_due(&settings),
            aprs_work: None,
            settings,
            transport: Transport::Direct(sockets),
            civil_time: || (9, 3),
            coordinator: Coordinator::new(
                Router::new(
                    Account::new(1, Vec::new(), Vec::new()).unwrap(),
                    1,
                    Duration::from_secs(30),
                )
                .unwrap(),
            )
            .unwrap(),
            directory,
            view: DirectoryView::new(&snapshot),
            snapshot,
            peers: BTreeMap::new(),
            pending_dials: BTreeMap::new(),
            commands: input,
            events,
            stop,
            retired: Arc::new(Notify::new()),
            routed_packets: 0,
            dropped_packets: 0,
            ingress_full_packets: 0,
        };
        tokio::time::timeout(Duration::from_secs(1), owner.run())
            .await
            .unwrap();
        assert!(
            !*stopping.borrow(),
            "test must not use the stop notification"
        );
        assert!(output.recv().await.is_none());
        if let Some(commands) = commands {
            assert!(
                commands.is_closed(),
                "finite-token failure must close control admission"
            );
        }
    }
}
