//! Best-effort RF metadata on the selected account transport, never a QSO worker.
use super::{network::PeerTransport, settings::PreparedSettings};
use crate::{
    transport::proxy::{ProxyError, QueueError},
    wire::{ProxyKind, ProxyRecord, write_aprs},
};
use std::{
    io,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};

pub(super) const INTERVAL: Duration = Duration::from_secs(600);

/// Match the reference's initial ten-second delay; a blank endpoint owns no timer.
pub(super) fn first_due(settings: &PreparedSettings) -> Option<Instant> {
    settings
        .aprs_server()
        .map(|_| Instant::now() + Duration::from_secs(10))
}

/// One bounded publication. DNS and transport credit cannot delay account routing;
/// the account polls this future alongside its ordinary commands and sockets.
pub(super) async fn publish(
    settings: Arc<PreparedSettings>,
    transport: PeerTransport,
) -> Result<(), ProxyError> {
    let (host, port) = settings.aprs_server().expect("enabled APRS job");
    let timeout = Duration::from_millis(settings.as_abi().policy.directory_timeout_ms.into());
    tokio::time::timeout(timeout, async {
        let destination = tokio::net::lookup_host((host, port))
            .await
            .map_err(|error| ProxyError::Io(error.kind()))?
            .find_map(|address| match address {
                SocketAddr::V4(address) => Some(address),
                SocketAddr::V6(_) => None,
            })
            .ok_or(ProxyError::Io(io::ErrorKind::AddrNotAvailable))?;
        // Preparation bounds the identity and all numeric metadata. The empty
        // status suffix keeps this existing serializer below its 255-byte limit.
        let location = settings.aprs_location("").expect("prepared RF metadata");
        let mut bytes = [0; 512];
        let count =
            write_aprs(settings.callsign(), &location, &mut bytes).expect("prepared APRS record");
        loop {
            match &transport {
                PeerTransport::Direct(sockets) => {
                    return sockets
                        .send_control(destination, &bytes[..count])
                        .await
                        .map_err(|error| ProxyError::Io(error.kind()));
                }
                PeerTransport::Proxy(output) => match output.try_queue(
                    ProxyRecord {
                        kind: ProxyKind::UdpControl,
                        address: *destination.ip(),
                        payload: &bytes[..count],
                    },
                    None,
                ) {
                    Ok(()) => return Ok(()),
                    Err(QueueError::Full) => output
                        .writable(ProxyKind::UdpControl, count)
                        .await
                        .map_err(ProxyError::Io)?,
                    Err(_) => return Err(ProxyError::Closed),
                },
            }
        }
    })
    .await
    .unwrap_or(Err(ProxyError::Timeout))
}
