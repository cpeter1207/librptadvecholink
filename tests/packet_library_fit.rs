//! Characterize the packet crates against the EchoLink wire contract before selection.
use rtcp::{
    goodbye::Goodbye,
    receiver_report::ReceiverReport,
    source_description::{
        SdesType, SourceDescription, SourceDescriptionChunk, SourceDescriptionItem,
    },
};
use rtp::packet::Packet;
use serde::Deserialize;
use webrtc_util::marshal::{Marshal, Unmarshal};

#[derive(Deserialize)]
struct ControlFixture {
    name: String,
    callsign: String,
    station_name: String,
    speex: bool,
    hex: String,
}

fn control_fixtures() -> Vec<ControlFixture> {
    serde_json::from_str(include_str!("fixtures/echolib-control.json")).unwrap()
}

fn hex_bytes(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

type ControlPackets = Vec<Box<dyn rtcp::packet::Packet + Send + Sync>>;

fn parse_control(wire: &[u8]) -> Result<ControlPackets, String> {
    let mut normalized = wire.to_vec();
    let mut offset = 0;
    let mut packets = Vec::new();
    while offset < normalized.len() {
        let header = normalized
            .get_mut(offset..offset + 4)
            .ok_or("truncated header")?;
        if !matches!(header[0] >> 6, 2 | 3) {
            return Err("unsupported version".into());
        }
        header[0] = (header[0] & 0x3f) | 0x80;
        let length = (u16::from_be_bytes([header[2], header[3]]) as usize + 1) * 4;
        if length > normalized.len() - offset {
            return Err("truncated packet".into());
        }
        let mut packet = normalized[offset..offset + length].to_vec();
        if matches!(packet[1], 202 | 203) && packet[0] & 0x1f != 1 {
            return Err("EchoLink station control requires one source".into());
        }
        let first = rtcp::packet::unmarshal(&mut packet.as_slice());
        let parsed = if first.is_err() && packet[1] == 202 && packet.ends_with(&[0; 4]) {
            // Only retry the pinned EchoLib extra zero word, never arbitrary garbage.
            packet.truncate(packet.len() - 4);
            let words = (packet.len() / 4 - 1) as u16;
            packet[2..4].copy_from_slice(&words.to_be_bytes());
            rtcp::packet::unmarshal(&mut packet.as_slice())
        } else {
            first
        }
        .map_err(|error| error.to_string())?;
        for parsed_packet in &parsed {
            if let Some(sdes) = parsed_packet.as_any().downcast_ref::<SourceDescription>() {
                let end = 8 + sdes.chunks[0]
                    .items
                    .iter()
                    .map(|item| 2 + item.text.len())
                    .sum::<usize>();
                let padding = packet.get(end..).ok_or("truncated SDES")?;
                if padding.is_empty() || padding.iter().any(|byte| *byte != 0) {
                    return Err("SDES END and alignment must be zero".into());
                }
            }
        }
        packets.extend(parsed);
        offset += length;
    }
    if packets.is_empty() {
        return Err("empty control".into());
    }
    Ok(packets)
}

fn write_control(fixture: &ControlFixture) -> Vec<u8> {
    let mut packets: ControlPackets = vec![Box::new(ReceiverReport::default())];
    if fixture.name == "bye" {
        packets.push(Box::new(Goodbye {
            sources: vec![0],
            reason: bytes::Bytes::from_static(b"jan2002"),
        }));
    } else {
        let mut items = vec![
            SourceDescriptionItem {
                sdes_type: SdesType::SdesCname,
                text: bytes::Bytes::from_static(b"CALLSIGN"),
            },
            SourceDescriptionItem {
                sdes_type: SdesType::SdesName,
                text: format!("{:<15}{}", fixture.callsign, fixture.station_name).into(),
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
        if fixture.speex {
            items.push(SourceDescriptionItem {
                sdes_type: SdesType::SdesPrivate,
                text: bytes::Bytes::from_static(b"SPEEX"),
            });
        }
        packets.push(Box::new(SourceDescription {
            chunks: vec![SourceDescriptionChunk { source: 0, items }],
        }));
    }
    let mut wire = Vec::new();
    for packet in packets {
        let mut part = packet.marshal().unwrap().to_vec();
        part[0] = (part[0] & 0x3f) | 0xc0;
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
    wire
}

#[test]
fn rtp_version_three_keeps_zero_clock_and_wrapping_sequence() {
    #[derive(Deserialize)]
    struct VoiceFixture {
        header_hex: String,
        sequence: u16,
        payload_byte: u8,
        payload_bytes: usize,
    }
    let fixtures: Vec<VoiceFixture> =
        serde_json::from_str(include_str!("fixtures/echolib-voice.json")).unwrap();
    for fixture in fixtures {
        let mut wire = hex_bytes(&fixture.header_hex);
        wire.extend(std::iter::repeat_n(
            fixture.payload_byte,
            fixture.payload_bytes,
        ));
        let packet = Packet::unmarshal(&mut wire.as_slice()).expect("valid EchoLink RTP");
        assert_eq!(packet.header.version, 3);
        assert_eq!(packet.header.sequence_number, fixture.sequence);
        assert_eq!(packet.header.timestamp, 0);
        assert_eq!(packet.header.ssrc, 0);
        assert_eq!(packet.payload.len(), fixture.payload_bytes);
        assert_eq!(packet.header.marker, wire[1] & 0x80 != 0);
        assert_eq!(packet.header.payload_type, wire[1] & 0x7f);
        assert_eq!(packet.marshal().unwrap().as_ref(), wire.as_slice());
    }
}

#[test]
fn pinned_control_accepts_all_name_residues_and_raw_speex() {
    for fixture in control_fixtures() {
        let wire = hex_bytes(&fixture.hex);
        let packets =
            parse_control(&wire).unwrap_or_else(|error| panic!("{}: {error}", fixture.name));
        assert_eq!(packets.len(), 2, "{}", fixture.name);
        let rr = packets[0]
            .as_any()
            .downcast_ref::<ReceiverReport>()
            .unwrap();
        assert_eq!(rr.ssrc, 0);
        assert!(rr.reports.is_empty());
        if fixture.name == "bye" {
            let bye = packets[1].as_any().downcast_ref::<Goodbye>().unwrap();
            assert_eq!(bye.sources, vec![0]);
            assert_eq!(bye.reason.as_ref(), b"jan2002");
        } else {
            let sdes = packets[1]
                .as_any()
                .downcast_ref::<SourceDescription>()
                .unwrap();
            assert_eq!(sdes.chunks.len(), 1);
            assert_eq!(sdes.chunks[0].source, 0);
            let items = &sdes.chunks[0].items;
            let name = items
                .iter()
                .find(|item| item.sdes_type == SdesType::SdesName)
                .unwrap();
            assert_eq!(
                name.text.as_ref(),
                format!("{:<15}{}", fixture.callsign, fixture.station_name).as_bytes()
            );
            assert_eq!(
                items
                    .iter()
                    .any(|item| item.sdes_type == SdesType::SdesPrivate
                        && item.text.as_ref() == b"SPEEX"),
                fixture.speex
            );
        }
    }
}

#[test]
fn control_writer_matches_pinned_reference_bytes() {
    for fixture in control_fixtures() {
        assert_eq!(
            write_control(&fixture),
            hex_bytes(&fixture.hex),
            "{}",
            fixture.name
        );
    }
}

#[test]
fn mixed_supported_control_versions_are_accepted() {
    let mut wire = hex_bytes(&control_fixtures()[0].hex);
    wire[0] = 0x80;
    assert!(parse_control(&wire).is_ok());
    wire[8] = 0x81;
    assert!(parse_control(&wire).is_ok());
}

#[test]
fn malformed_control_counts_lengths_and_padding_are_rejected() {
    let original = hex_bytes(&control_fixtures()[0].hex);
    let cases: &[(usize, u8)] = &[
        (0, 0x40),
        (2, 0xff),
        (3, 0),
        (8, 0xc0),
        (8, 0xc2),
        (11, 0xff),
        (17, 0xff),
        (63, 0x7f),
    ];
    for &(offset, value) in cases {
        let mut wire = original.clone();
        wire[offset] = value;
        assert!(
            parse_control(&wire).is_err(),
            "accepted bad byte at {offset}"
        );
    }
    for length in 0..original.len() {
        if length == 8 {
            continue;
        } // standalone zero-report RR is structurally legal.
        assert!(
            parse_control(&original[..length]).is_err(),
            "accepted truncation {length}"
        );
    }
    let mut bye = hex_bytes(&control_fixtures().last().unwrap().hex);
    bye[8] = 0xc0;
    assert!(parse_control(&bye).is_err(), "EchoLink BYE needs a source");
}
