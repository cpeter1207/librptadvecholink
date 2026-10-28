//! Real dynamically loaded codecs prove EchoLink four-frame packetization and finite tails.
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use rptadv_echolink::{
    media::Packetizer,
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
