use rptadv_echolink::wire::{Channel, ControlPacket, WireError, WirePacket, parse_datagram};
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

fn parse_control(wire: &[u8]) -> Result<ControlPacket, WireError> {
    match parse_datagram(Channel::Control, wire)? {
        WirePacket::Control(packet) => Ok(packet),
        _ => unreachable!("control channel has no media"),
    }
}
fn write_control(fixture: &ControlFixture) -> Vec<u8> {
    let packet = if fixture.name == "bye" {
        ControlPacket::Bye {
            source: 0,
            reason: b"jan2002".to_vec(),
        }
    } else {
        ControlPacket::Sdes {
            source: 0,
            callsign: fixture.callsign.clone(),
            station_name: fixture.station_name.clone(),
            speex: fixture.speex,
        }
    };
    let mut output = vec![0; 512];
    let count = rptadv_echolink::wire::write_control(&packet, &mut output).unwrap();
    output.truncate(count);
    output
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
        let packet = parse_control(&wire).unwrap();
        match packet {
            ControlPacket::Bye { source, reason } => {
                assert_eq!(source, 0);
                assert_eq!(reason, b"jan2002");
            }
            ControlPacket::Sdes {
                source,
                callsign,
                station_name,
                speex,
            } => {
                assert_eq!(source, 0);
                assert_eq!(callsign, fixture.callsign);
                assert_eq!(station_name, fixture.station_name);
                assert_eq!(speex, fixture.speex);
            }
            _ => panic!("missing station control"),
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
