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
