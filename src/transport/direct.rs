//! Raw IPv4 UDP boundary; protocol admission and packet routing stay with the account.

use crate::wire::{Channel, MAX_DATAGRAM};
use std::{io, net::SocketAddr};
use tokio::net::UdpSocket;

/// Nonblocking send outcome; retain the same prepared packet on Pending.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SendError {
    /// Socket readiness is not available yet, or a send returned WouldBlock.
    Pending,
    /// OS failure category retained for cause-specific diagnostics.
    Failed(io::ErrorKind),
}

/// One account's audio/text and control sockets, registered once with the shared runtime.
pub struct DirectSockets {
    audio: UdpSocket,
    control: UdpSocket,
}

impl DirectSockets {
    /// Bind the already-resolved endpoints. Configuration supplies the adjacent port pair;
    /// zero ports are useful for isolated, kernel-assigned loopback tests.
    pub async fn bind(audio: SocketAddr, control: SocketAddr) -> io::Result<Self> {
        if !audio.is_ipv4() || !control.is_ipv4() {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        let audio = UdpSocket::bind(audio).await?;
        let control = UdpSocket::bind(control).await?;
        Ok(Self { audio, control })
    }

    fn socket(&self, class: Channel) -> &UdpSocket {
        match class {
            Channel::Audio => &self.audio,
            Channel::Control => &self.control,
        }
    }

    /// Actual bound endpoint, including the kernel-selected port in isolated tests.
    pub fn local_addr(&self, class: Channel) -> io::Result<SocketAddr> {
        self.socket(class).local_addr()
    }

    /// Receive one complete datagram into the account's preallocated maximum-sized buffer.
    /// Empty packets reach the wire classifier instead of becoming a transport hangup.
    pub async fn recv(&self, class: Channel, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
        if buffer.len() < MAX_DATAGRAM {
            return Err(io::ErrorKind::InvalidInput.into());
        }
        self.socket(class).recv_from(buffer).await
    }

    /// Try one atomic UDP send. No queue, wait, timestamp change or catch-up burst occurs here.
    pub fn try_send(
        &self,
        class: Channel,
        destination: SocketAddr,
        payload: &[u8],
    ) -> Result<(), SendError> {
        if !destination.is_ipv4() || payload.len() > MAX_DATAGRAM {
            return Err(SendError::Failed(io::ErrorKind::InvalidInput));
        }
        self.socket(class)
            .try_send_to(payload, destination)
            .map(|_| ())
            .map_err(|error| {
                if error.kind() == io::ErrorKind::WouldBlock {
                    SendError::Pending
                } else {
                    SendError::Failed(error.kind())
                }
            })
    }

    /// Wait only after an actual Pending result; other peers retain independent owners.
    pub async fn writable(&self, class: Channel) -> io::Result<()> {
        self.socket(class).writable().await
    }
}

#[cfg(test)]
#[path = "../../tests/support/settings.rs"]
mod settings_fixture;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        account::{
            network::{AccountNetwork, NetworkEvent},
            settings::PreparedSettings,
        },
        transport::direct::settings_fixture::{secrets, settings, span},
    };
    use std::{sync::Arc, time::Duration};

    #[test]
    fn real_udp_receive_errors_retire_the_account_with_the_original_io_cause() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async {
            let _directory = std::net::TcpListener::bind("127.0.0.37:5200").unwrap();
            let servers = [span("127.0.0.37")];
            let config = settings("K1ABC-R", "Loopback", &servers, &[]);
            let settings = Arc::new(
                unsafe { PreparedSettings::copy(&config, &secrets("fixture-password")) }.unwrap(),
            );
            for channel in [Channel::Audio, Channel::Control] {
                let target = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
                let closed = target.local_addr().unwrap();
                drop(target);
                let sockets = Arc::new(
                    DirectSockets::bind(
                        "127.0.0.1:0".parse().unwrap(),
                        "127.0.0.1:0".parse().unwrap(),
                    )
                    .await
                    .unwrap(),
                );
                // Connected loopback UDP gives a genuine asynchronous ICMP error,
                // without closing/replacing a file descriptor owned by Tokio.
                sockets.socket(channel).connect(closed).await.unwrap();
                let (network, mut events) =
                    AccountNetwork::direct(Arc::clone(&settings), Arc::clone(&sockets), || {
                        (12, 30)
                    })
                    .unwrap();
                tokio::task::yield_now().await;
                sockets.socket(channel).send(b"probe").await.unwrap();
                // ICMP alone may expose only writable error readiness. A real
                // loopback datagram makes receive ready without consuming the
                // pending kernel error from the connected socket.
                let wake = std::net::UdpSocket::bind(closed).unwrap();
                wake.send_to(b"readable", sockets.local_addr(channel).unwrap())
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(3), async {
                    loop {
                        match events.recv().await {
                            Some(NetworkEvent::TransportLost(error)) => {
                                assert_eq!(error, io::ErrorKind::ConnectionRefused);
                                break;
                            }
                            Some(_) => (),
                            None => panic!("account retired without its receive error"),
                        }
                    }
                })
                .await
                .unwrap();
                network.stop().await.unwrap();
            }
        });
    }
}
