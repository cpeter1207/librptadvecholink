//! Wire tests use independent EchoLib bytes, not our serializer as their oracle.
use rptadv_echolink::wire::{
    Channel, Codec, ControlPacket, ProxyDecoder, ProxyKind, VoicePacket, WireError, WirePacket,
    parse_datagram, write_control, write_voice,
};
use std::net::Ipv4Addr;

fn hex(text: &str) -> Vec<u8> {
    text.as_bytes()
        .chunks_exact(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn outgoing_wire_profiles_reject_oversize_and_missing_identity_fields() {
    use rptadv_echolink::wire::{MAX_CHAT_BYTES, MAX_DATAGRAM, write_chat};
    for error in [
        WireError::Malformed,
        WireError::Unsupported,
        WireError::TooLarge,
        WireError::Capacity(12),
    ] {
        assert_eq!(error.to_string(), format!("EchoLink wire error: {error:?}"));
    }
    let mut output = vec![0xa5; MAX_DATAGRAM + 1];
    let mut voice = VoicePacket {
        codec: Codec::Speex,
        marker: true,
        sequence: 0,
        timestamp: 0,
        ssrc: 0,
        payload: &vec![0; MAX_DATAGRAM],
    };
    assert_eq!(write_voice(&voice, &mut output), Err(WireError::TooLarge));
    voice.payload = &[1];
    voice.marker = false;
    assert_eq!(write_voice(&voice, &mut output), Err(WireError::Malformed));
    for (sender, body, error) in [
        ("", "message", WireError::Malformed),
        (&"X".repeat(65), "message", WireError::Malformed),
        (
            "N0CALL",
            &"x".repeat(MAX_CHAT_BYTES + 1),
            WireError::TooLarge,
        ),
    ] {
        assert_eq!(write_chat(sender, body, &mut output), Err(error));
    }
    assert!(output.iter().all(|byte| *byte == 0xa5));
}

#[test]
fn wire_reference_roundtrip() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/echolib-control.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let bytes = hex(fixture["hex"].as_str().unwrap());
        let WirePacket::Control(packet) = parse_datagram(Channel::Control, &bytes).unwrap() else {
            panic!("control")
        };
        let mut output = [0xa5; 512];
        let count = write_control(&packet, &mut output).unwrap();
        assert_eq!(&output[..count], bytes, "{}", fixture["name"]);
        for length in 0..bytes.len() {
            if length != 8 {
                assert!(
                    parse_datagram(Channel::Control, &bytes[..length]).is_err(),
                    "truncation {length}"
                );
            }
        }
    }
}

#[test]
fn valid_bye_reason_padding_need_not_leave_a_word_aligned_body() {
    // RR followed by a one-source BYE, empty reason and three padding bytes.
    let mut bytes = hex("c0c9000100000000e1cb00020000000100000003");
    assert_eq!(
        parse_datagram(Channel::Control, &bytes),
        Ok(WirePacket::Control(ControlPacket::Bye {
            source: 1,
            reason: vec![],
        }))
    );
    bytes[16] = 1; // A reason cannot claim the envelope's padding as content.
    assert_eq!(
        parse_datagram(Channel::Control, &bytes),
        Err(WireError::Malformed)
    );
}

#[test]
fn gsm_four_units_is_144_bytes() {
    let payload = [0xd0; 132];
    let voice = VoicePacket {
        codec: Codec::GsmFr,
        marker: false,
        sequence: u16::MAX,
        timestamp: 1234,
        ssrc: 524950,
        payload: &payload,
    };
    let mut output = [0xa5; 144];
    assert_eq!(write_voice(&voice, &mut output), Ok(144));
    assert_eq!(
        &output[..12],
        &[0xc0, 3, 255, 255, 0, 0, 4, 210, 0, 8, 2, 150]
    );
    assert_eq!(
        parse_datagram(Channel::Audio, &output),
        Ok(WirePacket::Voice(voice))
    );
    output[1] |= 0x80; // ASL's GSM reader permits the RTP marker bit.
    assert!(matches!(
        parse_datagram(Channel::Audio, &output),
        Ok(WirePacket::Voice(VoicePacket { marker: true, .. }))
    ));
    output[1] = 127;
    assert_eq!(
        parse_datagram(Channel::Audio, &output),
        Err(WireError::Unsupported)
    );
    output[1] = 3;
    for length in 0..144 {
        assert!(parse_datagram(Channel::Audio, &output[..length]).is_err());
    }
    output[12] = 0; // Not raw GSM; do not accept WAV49 or another payload by length alone.
    assert_eq!(
        parse_datagram(Channel::Audio, &output),
        Err(WireError::Malformed)
    );
}

#[test]
fn control_unknown_optional_is_skipped() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/echolib-control.json")).unwrap();
    let mut bytes = hex(fixtures[0]["hex"].as_str().unwrap());
    assert_eq!(bytes[53], 4); // PHONE item, after NAME and EMAIL.
    bytes[53] = 250;
    assert!(matches!(
        parse_datagram(Channel::Control, &bytes),
        Ok(WirePacket::Control(ControlPacket::Sdes {
            speex: false,
            ..
        }))
    ));
    bytes[54] = 255;
    assert_eq!(
        parse_datagram(Channel::Control, &bytes),
        Err(WireError::Malformed)
    );
    let fixture = fixtures
        .as_array()
        .unwrap()
        .iter()
        .find(|item| item["speex"] == true)
        .unwrap();
    let mut bytes = hex(fixture["hex"].as_str().unwrap());
    let private = bytes
        .windows(7)
        .position(|item| item == b"\x08\x05SPEEX")
        .unwrap();
    bytes[private] = 250;
    assert!(matches!(
        parse_datagram(Channel::Control, &bytes),
        Ok(WirePacket::Control(ControlPacket::Sdes {
            speex: false,
            ..
        }))
    ));
}

#[test]
fn issue_826_standard_padding_and_optional_items_are_recognized() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/issue-826-normalized.json")).unwrap();
    let bytes = hex(fixture["hex"].as_str().unwrap());
    assert_eq!(bytes.len(), 96);
    assert_eq!(fixture["source_ports"], serde_json::json!([5199, 5198]));
    assert_eq!(
        parse_datagram(Channel::Control, &bytes),
        Ok(WirePacket::Control(ControlPacket::Sdes {
            source: 0,
            callsign: "HJ4SDR".into(),
            station_name: "Juan".into(),
            speex: false
        }))
    );
    let mut bad = bytes.clone();
    *bad.last_mut().unwrap() = 0;
    assert!(parse_datagram(Channel::Control, &bad).is_err());
    *bad.last_mut().unwrap() = 255;
    assert!(parse_datagram(Channel::Control, &bad).is_err());
}

#[test]
fn proxy_accepts_bounded_maximum_payloads() {
    for (kind, length) in [(2, 65_536), (5, 65_507), (6, 65_507)] {
        let bytes = proxy(kind, &vec![0xa5; length]);
        let mut parser = ProxyDecoder::default();
        let mut emitted = 0;
        for part in bytes.chunks(16384) {
            assert_eq!(
                parser.push(part, &mut |record| {
                    assert_eq!(record.payload, vec![0xa5; length]);
                    emitted += 1;
                }),
                Ok(part.len())
            );
        }
        assert_eq!(emitted, 1);
    }
}

#[test]
fn human_chat_excludes_station_info_and_commands() {
    for bytes in [
        b"oNDATAN0CALL>Hello radio\r\n".as_slice(),
        b"oNDATAN0CALL>Hello radio\r\n\0",
    ] {
        assert_eq!(
            parse_datagram(Channel::Audio, bytes),
            Ok(WirePacket::Chat {
                sender: "N0CALL",
                body: "Hello radio"
            })
        );
    }
    assert_eq!(
        parse_datagram(Channel::Audio, b"oNDATA\rStation information\0"),
        Ok(WirePacket::StationInfo(b"Station information"))
    );
    assert_eq!(
        parse_datagram(Channel::Audio, b"o.CONF admin"),
        Ok(WirePacket::Ignored)
    );
    for bytes in [
        b"oNDATAN0CALLHello\0".as_slice(),
        b"oNDATAN0CALL>Hello\0hidden",
        b"oNDATAN0CALL>\r\n",
        b"oNDATAN0CALL>Hello\r\nextra",
    ] {
        assert!(parse_datagram(Channel::Audio, bytes).is_err());
    }
    let large = format!("oNDATAN0CALL>{}\r\n", "a".repeat(4097));
    assert_eq!(
        parse_datagram(Channel::Audio, large.as_bytes()),
        Err(WireError::TooLarge)
    );
}

fn proxy(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut bytes = vec![kind, 127, 0, 0, 1];
    bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
    bytes.extend_from_slice(body);
    bytes
}

#[test]
fn proxy_split_and_coalesced_records() {
    let mut bytes = proxy(6, b"control");
    bytes.extend(proxy(1, &[]));
    bytes.extend(proxy(7, &[2]));
    for split in 0..=bytes.len() {
        let mut parser = ProxyDecoder::default();
        let mut records = Vec::new();
        let mut emit = |record: rptadv_echolink::wire::ProxyRecord<'_>| {
            records.push((record.kind, record.address, record.payload.to_vec()))
        };
        assert_eq!(parser.push(&bytes[..split], &mut emit), Ok(split));
        assert_eq!(
            parser.push(&bytes[split..], &mut emit),
            Ok(bytes.len() - split)
        );
        assert_eq!(
            records,
            [
                (
                    ProxyKind::UdpControl,
                    Ipv4Addr::LOCALHOST,
                    b"control".to_vec()
                ),
                (ProxyKind::TcpOpen, Ipv4Addr::LOCALHOST, vec![]),
                (ProxyKind::System, Ipv4Addr::LOCALHOST, vec![2])
            ]
        );
    }
}

#[test]
fn proxy_rejects_lengths_before_body_allocation_and_resets() {
    for (kind, length) in [
        (0, 0),
        (1, 1),
        (2, 0),
        (2, 65537),
        (3, 1),
        (4, 3),
        (5, 0),
        (5, 65508),
        (6, 65508),
        (7, 2),
    ] {
        let mut bytes = vec![kind, 0, 0, 0, 0];
        bytes.extend_from_slice(&(length as u32).to_le_bytes());
        let mut parser = ProxyDecoder::default();
        assert!(
            parser
                .push(&bytes, &mut |_| panic!("invalid record emitted"))
                .is_err()
        );
        let valid = proxy(4, &[0; 4]);
        let mut count = 0;
        assert_eq!(parser.push(&valid, &mut |_| count += 1), Ok(valid.len()));
        assert_eq!(count, 1);
    }
}

#[test]
fn malformed_is_not_close_and_short_output_is_unchanged() {
    for bytes in [
        vec![],
        vec![0; 65508],
        vec![0xc0, 203, 0, 1, 0, 0, 0, 0],
        vec![0xc1, 203, 255, 255],
    ] {
        assert!(parse_datagram(Channel::Control, &bytes).is_err());
    }
    let voice = VoicePacket {
        codec: Codec::GsmFr,
        marker: false,
        sequence: 0,
        timestamp: 0,
        ssrc: 0,
        payload: &[0xd0; 132],
    };
    let mut short = [0xa5; 143];
    assert_eq!(
        write_voice(&voice, &mut short),
        Err(WireError::Capacity(144))
    );
    assert_eq!(short, [0xa5; 143]);
}

#[test]
fn outbound_chat_and_station_info_keep_distinct_reference_grammars() {
    use rptadv_echolink::wire::{write_chat, write_station_info};
    let mut output = [0xa5; 256];
    let length = write_chat("K1ABC-R", "Hello, world.", &mut output).unwrap();
    assert_eq!(&output[..length], b"oNDATAK1ABC-R>Hello, world.\r\n\0");
    assert_eq!(
        parse_datagram(Channel::Audio, &output[..length]).unwrap(),
        WirePacket::Chat {
            sender: "K1ABC-R",
            body: "Hello, world."
        }
    );
    let length = write_station_info("K1ABC-R\nRepeater\r\nStatus", &mut output).unwrap();
    assert_eq!(&output[..length], b"oNDATA\rK1ABC-R\rRepeater\rStatus\0");
    assert!(matches!(
        parse_datagram(Channel::Audio, &output[..length]).unwrap(),
        WirePacket::StationInfo(_)
    ));
    let mut short = [0xa5; 3];
    assert!(matches!(
        write_chat("K1ABC-R", "Hello", &mut short),
        Err(WireError::Capacity(_))
    ));
    assert!(matches!(
        write_station_info("Station", &mut short),
        Err(WireError::Capacity(_))
    ));
    assert_eq!(short, [0xa5; 3]);
    for (sender, body) in [
        ("bad>sender", "text"),
        ("K1ABC-R", "text\0bad"),
        ("K1ABC-R", "text\r\ncommand"),
    ] {
        assert_eq!(
            write_chat(sender, body, &mut output),
            Err(WireError::Malformed)
        );
    }
    assert_eq!(
        write_chat("K1ABC-R", &"x".repeat(4097), &mut output),
        Err(WireError::TooLarge)
    );
    assert_eq!(
        write_station_info("text\0hidden", &mut output),
        Err(WireError::Malformed)
    );
    assert_eq!(
        write_station_info(&"x".repeat(65500), &mut output),
        Err(WireError::TooLarge)
    );
}

#[test]
fn aprs_metadata_uses_version_two_cname_and_location_not_peer_identity() {
    use rptadv_echolink::wire::write_aprs;
    use rtcp::{
        receiver_report::ReceiverReport,
        source_description::{SdesType, SourceDescription},
    };
    let location = ")EL-K1ABC-!4500.00NE09300.00W0PHG3274/146520/114 On @ 0903 [2/20]";
    let mut output = [0xa5; 512];
    let length = write_aprs("K1ABC-R", location, &mut output).unwrap();
    assert_eq!(output[0], 0x80);
    assert_eq!(output[8], 0x81);
    let packets = rtcp::packet::unmarshal(&mut &output[..length]).unwrap();
    assert_eq!(packets.len(), 2);
    assert_eq!(
        packets[0]
            .as_any()
            .downcast_ref::<ReceiverReport>()
            .unwrap()
            .ssrc,
        0
    );
    let sdes = packets[1]
        .as_any()
        .downcast_ref::<SourceDescription>()
        .unwrap();
    assert_eq!(sdes.chunks.len(), 1);
    assert_eq!(sdes.chunks[0].source, 0);
    let items = &sdes.chunks[0].items;
    assert_eq!(items.len(), 2);
    assert_eq!(items[0].sdes_type, SdesType::SdesCname);
    assert_eq!(items[0].text.as_ref(), b"K1ABC-R/K1ABC-R");
    assert_eq!(items[1].sdes_type, SdesType::SdesLocation);
    assert_eq!(items[1].text.as_ref(), location.as_bytes());
    let mut short = [0xa5; 3];
    assert_eq!(
        write_aprs("K1ABC-R", location, &mut short),
        Err(WireError::Capacity(length))
    );
    assert_eq!(short, [0xa5; 3]);
    assert_eq!(
        write_aprs("bad>call", location, &mut output),
        Err(WireError::Malformed)
    );
    assert_eq!(
        write_aprs("K1ABC-R", "bad\nlocation", &mut output),
        Err(WireError::Malformed)
    );
    assert_eq!(
        write_aprs("K1ABC-R", &"x".repeat(256), &mut output),
        Err(WireError::TooLarge)
    );
}
