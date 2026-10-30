//! Raw loopback UDP I/O preserves actual endpoints and does not interpret protocol state.

use rptadv_echolink::{
    transport::direct::{DirectSockets, SendError},
    wire::Channel,
};
use std::{future::Future, io::ErrorKind, net::SocketAddr, time::Duration};
use tokio::net::UdpSocket;

fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

async fn send(account: &DirectSockets, class: Channel, address: SocketAddr, payload: &[u8]) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match account.try_send(class, address, payload) {
                Ok(()) => break,
                Err(SendError::Pending) => account.writable(class).await.unwrap(),
                Err(error) => panic!("unexpected UDP failure: {error:?}"),
            }
        }
    })
    .await
    .unwrap();
}

#[test]
fn direct_accounts_own_independent_audio_and_control_sockets() {
    run(async {
        let account = DirectSockets::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let other = DirectSockets::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let source = peer.local_addr().unwrap();
        let mut buffer = vec![0; 65_507];
        for class in [Channel::Audio, Channel::Control] {
            let destination = account.local_addr(class).unwrap();
            assert_ne!(destination, other.local_addr(class).unwrap());
            peer.send_to(b"packet", destination).await.unwrap();
            let (count, observed) = account.recv(class, &mut buffer).await.unwrap();
            assert_eq!(&buffer[..count], b"packet");
            assert_eq!(observed, source);
            send(&account, class, source, b"reply").await;
            let (count, observed) = peer.recv_from(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..count], b"reply");
            assert_eq!(observed, destination);
        }
    });
}

#[test]
fn direct_minimum_and_maximum_datagrams_are_not_truncated_or_treated_as_hangups() {
    run(async {
        let account = DirectSockets::bind(
            "127.0.0.1:0".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = peer.local_addr().unwrap();
        let mut buffer = vec![0; 65_507];
        for size in [0, 65_507] {
            let input = vec![0x5a; size];
            send(&account, Channel::Audio, address, &input).await;
            let (count, _) = peer.recv_from(&mut buffer).await.unwrap();
            assert_eq!(&buffer[..count], input);
            peer.send_to(&input, account.local_addr(Channel::Control).unwrap())
                .await
                .unwrap();
            let (count, observed) = account.recv(Channel::Control, &mut buffer).await.unwrap();
            assert_eq!(count, size);
            assert_eq!(observed, address);
        }
        assert_eq!(
            account.try_send(Channel::Audio, address, &[0; 65_508]),
            Err(SendError::Failed(ErrorKind::InvalidInput))
        );
        assert_eq!(
            account
                .recv(Channel::Audio, &mut [0; 16])
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::InvalidInput
        );
    });
}

#[test]
fn unsupported_endpoints_and_real_send_failures_remain_distinct_from_backpressure() {
    run(async {
        let ipv4: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let ipv6: SocketAddr = "[::1]:0".parse().unwrap();
        for (audio, control) in [(ipv6, ipv4), (ipv4, ipv6)] {
            assert_eq!(
                DirectSockets::bind(audio, control)
                    .await
                    .err()
                    .unwrap()
                    .kind(),
                ErrorKind::InvalidInput
            );
        }
        let account = DirectSockets::bind(ipv4, ipv4).await.unwrap();
        assert_eq!(
            account.try_send(Channel::Audio, ipv6, b"voice"),
            Err(SendError::Failed(ErrorKind::InvalidInput))
        );
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                match account.try_send(Channel::Audio, ipv4, b"voice") {
                    Err(SendError::Pending) => account.writable(Channel::Audio).await.unwrap(),
                    result => {
                        assert_eq!(result, Err(SendError::Failed(ErrorKind::InvalidInput)));
                        break;
                    }
                }
            }
        })
        .await
        .unwrap();
    });
}
