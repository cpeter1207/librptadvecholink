//! RTP/RTCP library adaptation for EchoLib's version-three and surplus-zero SDES.
//! Reference maker: SvxLink 595cf9bb3325be753d05b219bdd39feab6a22b33 rtpacket.cpp.
use super::{MAX_DATAGRAM, WireError, copy_output};
use rtcp::{
    goodbye::Goodbye,
    receiver_report::ReceiverReport,
    source_description::{
        SdesType, SourceDescription, SourceDescriptionChunk, SourceDescriptionItem,
    },
};

/// Logical account control content; packet claims are not directory verification.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlPacket {
    /// A receiver-report heartbeat without station description or BYE.
    Report {
        /// RTP source identifier, not authentication.
        source: u32,
    },
    /// One station description, following its optional receiver report.
    Sdes {
        /// RTP source identifier, including zero.
        source: u32,
        /// Claimed callsign parsed from NAME.
        callsign: String,
        /// Claimed station name, never controller commands.
        station_name: String,
        /// Raw SPEEX private capability item was present.
        speex: bool,
    },
    /// Structurally valid one-source goodbye; endpoint/generation admission is separate.
    Bye {
        /// Source identifier, including zero.
        source: u32,
        /// Opaque bounded reason, never human chat.
        reason: Vec<u8>,
    },
}
type Packets = Vec<Box<dyn rtcp::packet::Packet + Send + Sync>>;

fn normalize_sdes(packet: &mut [u8]) -> Result<(), WireError> {
    let mut cursor = 8; // RTCP header and the single source identifier.
    loop {
        let tag = *packet.get(cursor).ok_or(WireError::Malformed)?;
        if tag == 0 {
            return if packet[cursor..].iter().all(|byte| *byte == 0) {
                Ok(())
            } else {
                Err(WireError::Malformed)
            };
        }
        let length = usize::from(*packet.get(cursor + 1).ok_or(WireError::Malformed)?);
        let end = cursor + 2 + length;
        if end > packet.len() {
            return Err(WireError::Malformed);
        }
        // The released parser maps unknown SDES tags to END. Normalize only
        // their type to NOTE, whose opaque content this adapter ignores. Never
        // convert them to PRIVATE, which could invent a SPEEX capability.
        if tag > 8 {
            packet[cursor] = 7;
        }
        cursor = end;
    }
}

fn packets(wire: &[u8]) -> Result<Packets, WireError> {
    let mut offset = 0;
    let mut packets = Vec::new();
    while offset < wire.len() {
        let header = wire.get(offset..offset + 4).ok_or(WireError::Malformed)?;
        if !matches!(header[0] >> 6, 2 | 3) {
            return Err(WireError::Malformed);
        }
        let length = (u16::from_be_bytes([header[2], header[3]]) as usize + 1) * 4;
        if length > wire.len() - offset {
            return Err(WireError::Malformed);
        }
        let mut packet = wire[offset..offset + length].to_vec();
        packet[0] = (packet[0] & 0x3f) | 0x80;
        if packet[0] & 0x20 != 0 {
            // The released SDES parser discards trailing bytes without validating
            // the RTCP padding count. Validate/strip that final envelope first;
            // SDES END/alignment validation still applies to the unpadded body.
            let padding = usize::from(*packet.last().unwrap());
            if padding == 0 || padding > packet.len() - 4 || offset + length != wire.len() {
                return Err(WireError::Malformed);
            }
            let end = packet.len() - padding;
            if packet[1] == 203 {
                // BYE's optional length-prefixed reason need not end on a word.
                // The released parser needs the aligned envelope but does not
                // exclude padding from its reason bound, so validate that here.
                let reason = 4 + usize::from(packet[0] & 0x1f) * 4;
                if reason > end || (reason < end && reason + 1 + usize::from(packet[reason]) > end)
                {
                    return Err(WireError::Malformed);
                }
            } else {
                if end % 4 != 0 {
                    return Err(WireError::Malformed);
                }
                packet.truncate(end);
                packet[0] &= !0x20;
                packet[2..4].copy_from_slice(&((end / 4 - 1) as u16).to_be_bytes());
            }
        }
        if matches!(packet[1], 202 | 203) && packet[0] & 0x1f != 1 {
            return Err(WireError::Malformed);
        }
        if packet[1] == 202 {
            normalize_sdes(&mut packet)?;
        }
        let first = rtcp::packet::unmarshal(&mut packet.as_slice());
        let parsed = if first.is_err() && packet[1] == 202 && packet.ends_with(&[0; 4]) {
            // The pinned maker can add exactly one whole zero alignment word.
            packet.truncate(packet.len() - 4);
            let words = (packet.len() / 4 - 1) as u16;
            packet[2..4].copy_from_slice(&words.to_be_bytes());
            rtcp::packet::unmarshal(&mut packet.as_slice())
        } else {
            first
        }
        .map_err(|_| WireError::Malformed)?;
        packets.extend(parsed);
        offset += length;
    }
    if packets.is_empty() {
        return Err(WireError::Malformed);
    }
    Ok(packets)
}

pub(super) fn parse(wire: &[u8]) -> Result<ControlPacket, WireError> {
    let mut control = None;
    let mut report = None;
    for packet in packets(wire)? {
        let next = if let Some(sdes) = packet.as_any().downcast_ref::<SourceDescription>() {
            let chunk = &sdes.chunks[0];
            let name = chunk
                .items
                .iter()
                .find(|item| item.sdes_type == SdesType::SdesName)
                .ok_or(WireError::Malformed)?;
            let name = std::str::from_utf8(&name.text).map_err(|_| WireError::Malformed)?;
            let (callsign, station_name) = name.split_once(' ').unwrap_or((name, ""));
            if callsign.is_empty()
                || !callsign
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
                || station_name.chars().any(char::is_control)
            {
                return Err(WireError::Malformed);
            }
            Some(ControlPacket::Sdes {
                source: chunk.source,
                callsign: callsign.to_owned(),
                station_name: station_name.trim_start().to_owned(),
                speex: chunk.items.iter().any(|item| {
                    item.sdes_type == SdesType::SdesPrivate && item.text.as_ref() == b"SPEEX"
                }),
            })
        } else if let Some(bye) = packet.as_any().downcast_ref::<Goodbye>() {
            Some(ControlPacket::Bye {
                source: bye.sources[0],
                reason: bye.reason.to_vec(),
            })
        } else {
            if let Some(rr) = packet.as_any().downcast_ref::<ReceiverReport>() {
                report = Some(ControlPacket::Report { source: rr.ssrc });
            }
            None
        };
        if let Some(next) = next {
            if control.replace(next).is_some() {
                return Err(WireError::Malformed);
            }
        }
    }
    control.or(report).ok_or(WireError::Unsupported)
}

/// Write the standard RR plus EchoLink SDES/BYE envelope, using released builders.
/// Insufficient capacity leaves the caller's output completely unchanged.
pub fn write_control(packet: &ControlPacket, out: &mut [u8]) -> Result<usize, WireError> {
    let mut packets: Packets = vec![Box::new(ReceiverReport::default())];
    match packet {
        ControlPacket::Report { source } => {
            packets[0] = Box::new(ReceiverReport {
                ssrc: *source,
                ..Default::default()
            })
        }
        ControlPacket::Bye { source, reason } => {
            if reason.len() > 255 {
                return Err(WireError::TooLarge);
            }
            packets.push(Box::new(Goodbye {
                sources: vec![*source],
                reason: bytes::Bytes::copy_from_slice(reason),
            }));
        }
        ControlPacket::Sdes {
            source,
            callsign,
            station_name,
            speex,
        } => {
            if callsign.len() > 15 || station_name.len() > 240 {
                return Err(WireError::TooLarge);
            }
            if callsign.is_empty()
                || !callsign
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
                || station_name.chars().any(char::is_control)
            {
                return Err(WireError::Malformed);
            }
            let mut items = vec![
                SourceDescriptionItem {
                    sdes_type: SdesType::SdesCname,
                    text: bytes::Bytes::from_static(b"CALLSIGN"),
                },
                SourceDescriptionItem {
                    sdes_type: SdesType::SdesName,
                    text: format!("{callsign:<15}{station_name}").into(),
                },
                SourceDescriptionItem {
                    sdes_type: SdesType::SdesEmail,
                    text: bytes::Bytes::from_static(b"CALLSIGN"),
                },
                SourceDescriptionItem {
                    sdes_type: SdesType::SdesPhone,
                    text: bytes::Bytes::from_static(b"08:30"),
                },
            ];
            if *speex {
                items.push(SourceDescriptionItem {
                    sdes_type: SdesType::SdesPrivate,
                    text: bytes::Bytes::from_static(b"SPEEX"),
                });
            }
            packets.push(Box::new(SourceDescription {
                chunks: vec![SourceDescriptionChunk {
                    source: *source,
                    items,
                }],
            }));
        }
    }
    write_packets(packets, 3, out)
}

/// Write EchoLink's APRS RR/SDES metadata using version two, zero SSRC,
/// CNAME `call/call` and LOCATION (not the peer-control NAME item).
/// The one-byte SDES item bound is checked before building either packet.
pub fn write_aprs(callsign: &str, location: &str, out: &mut [u8]) -> Result<usize, WireError> {
    if callsign.len() > 15 || location.len() > 255 {
        return Err(WireError::TooLarge);
    }
    if callsign.is_empty()
        || !callsign
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
        || location.chars().any(char::is_control)
    {
        return Err(WireError::Malformed);
    }
    write_packets(
        vec![
            Box::new(ReceiverReport::default()),
            Box::new(SourceDescription {
                chunks: vec![SourceDescriptionChunk {
                    source: 0,
                    items: vec![
                        SourceDescriptionItem {
                            sdes_type: SdesType::SdesCname,
                            text: format!("{callsign}/{callsign}").into(),
                        },
                        SourceDescriptionItem {
                            sdes_type: SdesType::SdesLocation,
                            text: bytes::Bytes::copy_from_slice(location.as_bytes()),
                        },
                    ],
                }],
            }),
        ],
        2,
        out,
    )
}

fn write_packets(packets: Packets, version: u8, out: &mut [u8]) -> Result<usize, WireError> {
    let mut wire = Vec::new();
    for packet in packets {
        let mut part = packet.marshal().map_err(|_| WireError::Malformed)?.to_vec();
        part[0] = (part[0] & 0x3f) | (version << 6);
        if let Some(sdes) = packet.as_any().downcast_ref::<SourceDescription>() {
            let end = 8 + sdes.chunks[0]
                .items
                .iter()
                .map(|item| 2 + item.text.len())
                .sum::<usize>();
            part.resize((end + 2).next_multiple_of(4), 0);
            part[end..].fill(0);
            let length = (part.len() / 4 - 1) as u16;
            part[2..4].copy_from_slice(&length.to_be_bytes());
        }
        wire.extend_from_slice(&part);
    }
    if wire.len() > MAX_DATAGRAM {
        return Err(WireError::TooLarge);
    }
    copy_output(&wire, out)
}
