//! Real dynamically loaded codecs prove EchoLink four-frame packetization and finite tails.
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use rptadv_echolink::{
    media::{CodecChoice, Packetizer},
    wire::{Channel, Codec, WirePacket, parse_datagram},
};
use std::path::PathBuf;

fn catalog() -> CodecCatalog {
    let path = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap_or_default());
    CodecCatalog::load(&[
        ProviderSelection {
            path: path.join("librptadv_codec_gsm.so.1"),
            profiles: vec![ProfileId::GsmFr],
        },
        ProviderSelection {
            path: path.join("librptadv_codec_speex.so.1"),
            profiles: vec![ProfileId::Speex],
        },
    ])
    .unwrap()
}

#[test]
fn speex_requires_mutual_raw_priv_and_falls_back_to_the_mandatory_gsm_provider() {
    let catalog = catalog();
    let mut choice = CodecChoice::new(&catalog).unwrap();
    assert!(choice.advertise_speex());
    assert_eq!(choice.codec(), Codec::GsmFr);
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/echolib-control.json")).unwrap();
    for fixture in fixtures.as_array().unwrap() {
        let bytes: Vec<u8> = fixture["hex"]
            .as_str()
            .unwrap()
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let WirePacket::Control(control) = parse_datagram(Channel::Control, &bytes).unwrap() else {
            panic!("control")
        };
        choice.observe_sdes(&control);
        if fixture["speex"].as_bool().unwrap() {
            assert_eq!(choice.codec(), Codec::Speex);
        }
    }
    assert_eq!(
        choice.codec(),
        Codec::Speex,
        "heartbeats do not silently undo a negotiated capability"
    );
    let path = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap_or_default());
    let gsm = CodecCatalog::load(&[ProviderSelection {
        path: path.join("librptadv_codec_gsm.so.1"),
        profiles: vec![ProfileId::GsmFr],
    }])
    .unwrap();
    let mut choice = CodecChoice::new(&gsm).unwrap();
    assert!(!choice.advertise_speex());
    choice.observe_sdes(&rptadv_echolink::wire::ControlPacket::Sdes {
        source: 0,
        callsign: "N0CALL".into(),
        station_name: String::new(),
        speex: true,
    });
    assert_eq!(choice.codec(), Codec::GsmFr);
    assert!(CodecChoice::new(&CodecCatalog::load(&[]).unwrap()).is_err());
}

fn voice(bytes: &[u8]) -> rptadv_echolink::wire::VoicePacket<'_> {
    let WirePacket::Voice(packet) = parse_datagram(Channel::Audio, bytes).unwrap() else {
        panic!("not voice")
    };
    packet
}

fn source(count: usize) -> Vec<f32> {
    (0..count)
        .map(|sample| (sample as f32 * std::f32::consts::TAU * 700.0 / 8000.0).sin() * 0.25)
        .collect()
}

#[test]
fn gsm_80ms_roundtrip_uses_four_raw_units_and_keeps_prepared_bytes_on_retry() {
    let catalog = catalog();
    let mut encoder = Packetizer::new(&catalog, Codec::GsmFr, 0.0, 65535, 123456).unwrap();
    let mut decoder = catalog.decoder(ProfileId::GsmFr, 0, 640, 252).unwrap();
    let pcm = source(640);
    assert_eq!(encoder.push(&pcm[..480]).unwrap(), 480);
    assert!(encoder.packet().is_none());
    assert_eq!(encoder.push(&pcm[480..]).unwrap(), 160);
    let first = encoder.packet().unwrap().bytes.clone();
    assert_eq!(first.len(), 144);
    assert_eq!(encoder.packet().unwrap().source_frames, 640);
    assert_eq!(encoder.push(&pcm).unwrap(), 0);
    encoder.finish().unwrap();
    assert_eq!(encoder.packet().unwrap().bytes, first);
    let packet = voice(&first);
    assert_eq!((packet.sequence, packet.ssrc), (65535, 123456));
    assert_eq!(decoder.inspect(packet.payload).unwrap().frames, 640);
    let mut decoded = [0.0; 640];
    assert_eq!(decoder.decode(packet.payload, &mut decoded).unwrap(), 640);
    assert!(decoded.iter().any(|sample| sample.abs() > 0.05));
    encoder.retire().unwrap();
    assert!(encoder.drained());
    encoder.reset().unwrap();
    encoder.push(&pcm).unwrap();
    assert_eq!(voice(&encoder.packet().unwrap().bytes).sequence, 0);
}

#[test]
fn late_speex_upgrade_preserves_pending_bytes_partial_pcm_and_one_sequence_clock() {
    let catalog = catalog();
    for pending in [false, true] {
        let mut encoder = Packetizer::new(&catalog, Codec::GsmFr, 0.0, 65535, 42).unwrap();
        let mut before = None;
        if pending {
            encoder.push(&source(640)).unwrap();
            before = Some(encoder.packet().unwrap().bytes.clone());
        } else {
            encoder.push(&source(320)).unwrap();
        }
        encoder.upgrade_speex(&catalog).unwrap();
        encoder.upgrade_speex(&catalog).unwrap();
        if let Some(before) = before {
            assert_eq!(encoder.packet().unwrap().bytes, before);
            encoder.retire().unwrap();
            encoder.push(&source(640)).unwrap();
        } else {
            encoder.push(&source(320)).unwrap();
        }
        let first = encoder.packet().unwrap();
        let packet = voice(&first.bytes);
        assert_eq!(
            (packet.codec, packet.sequence, packet.timestamp, packet.ssrc),
            (
                Codec::Speex,
                if pending { 0 } else { 65535 },
                if pending { 640 } else { 0 },
                42
            )
        );
        let base = if pending { 640 } else { 0 };
        // The selected narrowband provider reports forty samples of encoder lookahead.
        assert_eq!((first.source_start, first.source_frames), (base, 600));
        encoder.finish().unwrap();
        encoder.retire().unwrap();
        let tail = encoder.packet().unwrap();
        assert_eq!((tail.source_start, tail.source_frames), (base + 600, 40));
        encoder.retire().unwrap();
        assert!(encoder.drained());
    }
}

#[test]
fn speex_packet_is_one_four_frame_bitstream_and_lookahead_survives_finite_tail() {
    let catalog = catalog();
    for count in [1, 620, 640, 1260] {
        let mut encoder = Packetizer::new(&catalog, Codec::Speex, 0.0, 1, 0).unwrap();
        let mut decoder = catalog.decoder(ProfileId::Speex, 0, 640, 252).unwrap();
        let pcm = source(count);
        let mut accepted = 0;
        let mut represented = 0;
        let mut packets = 0;
        loop {
            if accepted < pcm.len() {
                accepted += encoder.push(&pcm[accepted..]).unwrap();
            }
            if accepted == pcm.len() {
                encoder.finish().unwrap();
            }
            if let Some(prepared) = encoder.packet() {
                let wire = voice(&prepared.bytes);
                assert_eq!((wire.codec, wire.marker), (Codec::Speex, true));
                assert_eq!(decoder.inspect(wire.payload).unwrap().frames, 640);
                assert_eq!(decoder.decode(wire.payload, &mut [0.0; 640]).unwrap(), 640);
                represented += prepared.source_frames;
                packets += 1;
                encoder.retire().unwrap();
            }
            if encoder.drained() {
                break;
            }
            assert!(packets < 5, "finite tail must not generate endless silence");
        }
        assert_eq!(represented, count as u32);
        assert_eq!(packets, (count + 40).div_ceil(640));
    }
}

#[test]
fn empty_burst_has_no_wire_padding_and_partial_gsm_tail_is_one_legal_packet() {
    let catalog = catalog();
    let mut encoder = Packetizer::new(&catalog, Codec::GsmFr, 0.0, 1, 0).unwrap();
    encoder.finish().unwrap();
    encoder.finish().unwrap();
    assert!(encoder.drained());
    assert!(encoder.packet().is_none());
    encoder.reset().unwrap();
    assert_eq!(encoder.push(&source(161)).unwrap(), 161);
    encoder.finish().unwrap();
    let prepared = encoder.packet().unwrap();
    assert_eq!(prepared.source_frames, 161);
    assert_eq!(voice(&prepared.bytes).payload.len(), 132);
    encoder.retire().unwrap();
    assert!(encoder.drained());
}

#[test]
fn transmit_gain_is_applied_once_before_codec_encoding() {
    let catalog = catalog();
    let pcm = source(640);
    let mut energies = Vec::new();
    for gain in [0.0, 6.0] {
        let mut encoder = Packetizer::new(&catalog, Codec::GsmFr, gain, 1, 0).unwrap();
        let mut decoder = catalog.decoder(ProfileId::GsmFr, 0, 640, 252).unwrap();
        encoder.push(&pcm).unwrap();
        let packet = voice(&encoder.packet().unwrap().bytes);
        let mut output = [0.0; 640];
        decoder.decode(packet.payload, &mut output).unwrap();
        energies.push(
            output
                .iter()
                .map(|sample| sample * sample)
                .sum::<f32>()
                .sqrt(),
        );
    }
    let ratio = energies[1] / energies[0];
    assert!(
        (1.7..2.3).contains(&ratio),
        "+6 dB applied more or less than once: {ratio}"
    );
    for gain in [f64::NAN, f64::INFINITY, 10000.0] {
        assert!(Packetizer::new(&catalog, Codec::GsmFr, gain, 1, 0).is_err());
    }
}

#[test]
fn independent_upstream_speex_decoder_and_encoder_agree_on_four_frame_boundaries() {
    use std::{
        io::Write,
        process::{Command, Stdio},
    };
    struct Probe(PathBuf);
    impl Drop for Probe {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let base =
        PathBuf::from(std::env::var_os("CARGO_TARGET_DIR").unwrap_or_else(|| "target".into()));
    std::fs::create_dir_all(&base).unwrap();
    let directory = std::fs::canonicalize(base)
        .unwrap()
        .join(format!("speex-reference-{}", std::process::id()));
    std::fs::create_dir(&directory).unwrap();
    let directory = Probe(directory);
    let executable = directory.0.join("probe");
    let compiled = Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()))
        .args(["-std=c11", "-Wall", "-Wextra", "-Werror"])
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/speex_reference.c"))
        .args(["-lspeex", "-o"])
        .arg(&executable)
        .output()
        .unwrap();
    assert!(
        compiled.status.success(),
        "{}",
        String::from_utf8_lossy(&compiled.stderr)
    );
    let run = |mode: &str, bytes: &[u8]| {
        let mut child = Command::new(&executable)
            .arg(mode)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        child.stdin.take().unwrap().write_all(bytes).unwrap();
        child.wait_with_output().unwrap()
    };
    let catalog = catalog();
    let mut encoder = Packetizer::new(&catalog, Codec::Speex, 0.0, 1, 0).unwrap();
    encoder.push(&source(640)).unwrap();
    let packet = voice(&encoder.packet().unwrap().bytes);
    let reference = run("decode", packet.payload);
    assert!(
        reference.status.success(),
        "the upstream decoder must find exactly four frames then the terminator"
    );
    assert_eq!(reference.stdout.len(), 1280);
    let reference: Vec<f32> = reference
        .stdout
        .chunks_exact(2)
        .map(|pair| f32::from(i16::from_ne_bytes([pair[0], pair[1]])) / 32768.0)
        .collect();
    let mut decoder = catalog.decoder(ProfileId::Speex, 0, 640, 252).unwrap();
    let mut pcm = [0.0; 640];
    decoder.decode(packet.payload, &mut pcm).unwrap();
    assert_eq!(pcm.as_slice(), reference);
    assert!(!run("decode", &packet.payload[..1]).status.success());
    // Peer-side fixture uses EchoLib's documented NB quality/complexity settings,
    // independently of the project's codec provider and packet serializer.
    let input: Vec<u8> = (0..640)
        .flat_map(|i| (((i % 37) * 150 - 2700) as i16).to_ne_bytes())
        .collect();
    let reference = run("encode", &input);
    assert!(reference.status.success());
    decoder.reset().unwrap();
    assert_eq!(decoder.inspect(&reference.stdout).unwrap().frames, 640);
    assert_eq!(decoder.decode(&reference.stdout, &mut pcm).unwrap(), 640);
    assert!(pcm.iter().all(|sample| sample.is_finite()));
}
