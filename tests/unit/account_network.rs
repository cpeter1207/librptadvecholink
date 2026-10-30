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
