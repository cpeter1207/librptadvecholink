//! One serial virtual directory channel keeps blocked decoding away from Proxy UDP.
use rptadv_echolink::{
    transport::{
        ReplyKind,
        proxy::directory::{DirectoryChannel, TunnelError},
    },
    wire::{ProxyKind, ProxyRecord},
};
use std::{future::Future, net::Ipv4Addr, time::Duration};

fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

fn feed(channel: &mut DirectoryChannel, kind: ProxyKind, payload: &[u8]) {
    // TCP records have no virtual stream ID. Their address field is not a stream ID.
    assert!(channel.receive(ProxyRecord {
        kind,
        address: Ipv4Addr::UNSPECIFIED,
        payload
    }));
}

#[test]
fn serial_tunnel_streams_reply_and_waits_for_close_before_reopening() {
    run(async {
        let mut channel = DirectoryChannel::default();
        let ip = Ipv4Addr::new(192, 0, 2, 10);
        let (token, reply) = channel
            .begin(ip, b"F0\r", ReplyKind::Directory, Duration::from_secs(1))
            .unwrap();
        let output = channel.outgoing().unwrap();
        assert_eq!(
            (output.kind, output.address, output.payload),
            (ProxyKind::TcpOpen, ip, &[][..])
        );
        channel.output_queued();
        assert!(channel.outgoing().is_none());
        feed(&mut channel, ProxyKind::TcpStatus, &[0; 4]);
        assert_eq!(channel.outgoing().unwrap().payload, b"F0\r");
        assert_eq!(channel.outgoing().unwrap().kind, ProxyKind::TcpData);
        channel.output_queued();
        let body = b"@@@\n1\nK1ABC-R\nStation\n123456\n192.0.2.1\n+++";
        for fragment in body.chunks(7) {
            feed(&mut channel, ProxyKind::TcpData, fragment);
        }
        assert!(!channel.receive(ProxyRecord {
            kind: ProxyKind::UdpData,
            address: Ipv4Addr::LOCALHOST,
            payload: b"audio"
        }));
        channel.finish(token, reply.await);
        assert_eq!(channel.outgoing().unwrap().kind, ProxyKind::TcpClose);
        channel.output_queued();
        assert!(
            channel
                .begin(ip, b"next", ReplyKind::Login, Duration::from_secs(1))
                .is_err()
        );
        assert!(channel.take_completed().is_none());
        feed(&mut channel, ProxyKind::TcpClose, &[]);
        assert_eq!(channel.take_completed(), Some((token, Ok(body.to_vec()))));
        assert!(
            channel
                .begin(ip, b"next", ReplyKind::Login, Duration::from_secs(1))
                .is_ok()
        );
    });
}

#[test]
fn full_directory_staging_fails_only_that_transaction_without_blocking_datagrams() {
    run(async {
        let mut channel = DirectoryChannel::default();
        let (token, reply) = channel
            .begin(
                Ipv4Addr::LOCALHOST,
                b"F0\r",
                ReplyKind::Directory,
                Duration::from_secs(1),
            )
            .unwrap();
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpStatus, &[0; 4]);
        channel.output_queued();
        for _ in 0..5 {
            feed(&mut channel, ProxyKind::TcpData, &[0; 65536]);
        }
        assert!(!channel.receive(ProxyRecord {
            kind: ProxyKind::UdpControl,
            address: Ipv4Addr::LOCALHOST,
            payload: b"heartbeat"
        }));
        channel.finish(token, reply.await);
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpClose, &[]);
        assert_eq!(
            channel.take_completed(),
            Some((token, Err(TunnelError::Backpressure)))
        );
    });
}

#[test]
fn status_failure_and_stale_completion_cannot_contaminate_a_new_transaction() {
    run(async {
        let mut channel = DirectoryChannel::default();
        let (old, reply) = channel
            .begin(
                Ipv4Addr::LOCALHOST,
                b"login",
                ReplyKind::Login,
                Duration::from_secs(1),
            )
            .unwrap();
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpStatus, &111u32.to_le_bytes());
        channel.finish(old, reply.await);
        assert_eq!(
            channel.take_completed(),
            Some((old, Err(TunnelError::ConnectStatus(111))))
        );
        let (token, reply) = channel
            .begin(
                Ipv4Addr::LOCALHOST,
                b"login2",
                ReplyKind::Login,
                Duration::from_secs(1),
            )
            .unwrap();
        assert_ne!(old, token);
        channel.finish(old, Ok(b"stale".to_vec()));
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpStatus, &[0; 4]);
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpData, b"OK");
        feed(&mut channel, ProxyKind::TcpClose, &[]);
        channel.finish(token, reply.await);
        assert_eq!(channel.take_completed(), Some((token, Ok(b"OK".to_vec()))));
    });
}

#[test]
fn timeout_requires_close_quiescence_before_another_open() {
    run(async {
        let mut channel = DirectoryChannel::default();
        let (token, reply) = channel
            .begin(
                Ipv4Addr::LOCALHOST,
                b"login",
                ReplyKind::Login,
                Duration::from_millis(1),
            )
            .unwrap();
        channel.output_queued();
        channel.finish(token, reply.await);
        assert_eq!(channel.outgoing().unwrap().kind, ProxyKind::TcpClose);
        channel.output_queued();
        assert!(
            channel
                .begin(
                    Ipv4Addr::LOCALHOST,
                    b"next",
                    ReplyKind::Login,
                    Duration::from_secs(1)
                )
                .is_err()
        );
        feed(&mut channel, ProxyKind::TcpClose, &[]);
        assert!(
            matches!(channel.take_completed(), Some((id, Err(TunnelError::Directory(rptadv_echolink::transport::TransactionError::Timeout)))) if id == token)
        );
    });
}

#[test]
fn parser_completion_does_not_queue_a_second_close_after_staging_failure() {
    run(async {
        let mut channel = DirectoryChannel::default();
        let (token, reply) = channel
            .begin(
                Ipv4Addr::LOCALHOST,
                b"F0\r",
                ReplyKind::Directory,
                Duration::from_secs(1),
            )
            .unwrap();
        channel.output_queued();
        feed(&mut channel, ProxyKind::TcpStatus, &[0; 4]);
        channel.output_queued();
        for _ in 0..5 {
            feed(&mut channel, ProxyKind::TcpData, &[0; 65536]);
        }
        assert_eq!(channel.outgoing().unwrap().kind, ProxyKind::TcpClose);
        channel.output_queued();
        assert!(channel.outgoing().is_none());
        channel.finish(token, reply.await);
        assert!(
            channel.outgoing().is_none(),
            "second CLOSE could tear down a later virtual stream"
        );
        feed(&mut channel, ProxyKind::TcpClose, &[]);
        assert_eq!(
            channel.take_completed(),
            Some((token, Err(TunnelError::Backpressure)))
        );
    });
}
