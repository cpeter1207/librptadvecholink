//! The common host prepared-media contract uses real dynamically loaded codecs.
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection, Status};
use rptadv_echolink::{
    media::PreparedMedia,
    wire::{Channel, Codec, WirePacket, parse_datagram},
};
use std::path::PathBuf;

#[path = "support/codec_fault.rs"]
mod codec_fault;

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
fn short_output_preserves_pcm_partial_input_and_the_rtp_sequence() {
    let catalog = catalog();
    let mut media = PreparedMedia::new(&catalog, 0.0, 65535, 42).unwrap();
    let mut short = [0xa5; 143];
    let full = media.prepare(&[0.1; 640], &mut short, false).unwrap();
    assert_eq!(
        (full.status, full.consumed_frames, full.encoded_bytes),
        (Status::OutputFull, 0, 0)
    );
    assert_eq!(short, [0xa5; 143]);
    let mut output = [0xa5; 264];
    let first = media.prepare(&[0.1; 300], &mut output, false).unwrap();
    assert_eq!(
        (first.status, first.consumed_frames, first.encoded_bytes),
        (Status::NeedInput, 300, 0)
    );
    assert_eq!(output, [0xa5; 264]);
    let second = media.prepare(&[0.1; 500], &mut output, false).unwrap();
    assert_eq!(
        (
            second.consumed_frames,
            second.encoded_bytes,
            second.packet_frames
        ),
        (340, 144, 640)
    );
    assert_eq!((second.source_start_frames, second.source_frames), (0, 640));
    let WirePacket::Voice(voice) = parse_datagram(Channel::Audio, &output[..144]).unwrap() else {
        panic!("voice");
    };
    assert_eq!(
        (voice.codec, voice.sequence, voice.timestamp, voice.ssrc),
        (Codec::GsmFr, 65535, 0, 42)
    );
    let mut encoder = catalog.encoder(ProfileId::GsmFr, 0, 640, 252).unwrap();
    let mut expected = [0; 252];
    let encoded = encoder.encode(&[0.1; 640], &mut expected, false).unwrap();
    assert_eq!(voice.payload, &expected[..encoded.encoded_bytes]);
    media.reset().unwrap();
    let next = media.prepare(&[0.2; 640], &mut output, false).unwrap();
    let WirePacket::Voice(voice) =
        parse_datagram(Channel::Audio, &output[..next.encoded_bytes]).unwrap()
    else {
        panic!("voice");
    };
    assert_eq!((voice.sequence, voice.timestamp), (0, 640));
    assert_eq!((next.source_start_frames, next.source_frames), (0, 640));
}

#[test]
fn finite_speex_flush_reports_only_real_input_and_finishes_without_endless_padding() {
    let catalog = catalog();
    let mut media = PreparedMedia::new(&catalog, 0.0, 7, 42).unwrap();
    media.upgrade_speex(&catalog).unwrap();
    let mut output = [0; 264];
    let first = media.prepare(&[0.1; 100], &mut output, true).unwrap();
    assert_eq!(
        (
            first.consumed_frames,
            first.source_start_frames,
            first.source_frames,
            first.packet_frames
        ),
        (100, 0, 100, 640)
    );
    let WirePacket::Voice(voice) =
        parse_datagram(Channel::Audio, &output[..first.encoded_bytes]).unwrap()
    else {
        panic!("voice");
    };
    assert_eq!(
        (voice.codec, voice.sequence, voice.timestamp),
        (Codec::Speex, 7, 0)
    );
    for _ in 0..3 {
        let done = media.prepare(&[], &mut output, true).unwrap();
        assert_eq!(
            (done.status, done.consumed_frames, done.encoded_bytes),
            (Status::Complete, 0, 0)
        );
    }
    media.reset().unwrap();
    let full = media.prepare(&[0.2; 640], &mut output, true).unwrap();
    assert_eq!((full.source_start_frames, full.source_frames), (0, 600));
    let tail = media.prepare(&[], &mut output, true).unwrap();
    assert_eq!(
        (
            tail.source_start_frames,
            tail.source_frames,
            tail.packet_frames
        ),
        (600, 40, 640)
    );
    let done = media.prepare(&[], &mut output, true).unwrap();
    assert_eq!((done.status, done.encoded_bytes), (Status::Complete, 0));
}

#[test]
fn bounded_but_inconsistent_encoder_results_cannot_be_sent_as_voice() {
    use rptadv_echolink::media::MediaError;
    for fault in [10, 11, 12] {
        let fixture = codec_fault::FaultCodec::new(ProfileId::GsmFr);
        let catalog = fixture.catalog();
        let mut media = PreparedMedia::new(&catalog, 0.0, 7, 42).unwrap();
        fixture.select(fault);
        assert!(matches!(
            media.prepare(&[0.1; 640], &mut [0; 264], false),
            Err(MediaError::Contract)
        ));
        assert_eq!(fixture.calls(false), 0);
    }
}
