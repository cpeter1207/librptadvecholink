//! Real dynamic jitter and codec capabilities, with virtual monotonic arrival times.
use libloading::Library;
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use rptadv_echolink::{
    media::Packetizer,
    playout::{PcmSink, Playout, Progress, Reception},
    wire::{Channel, Codec, VoicePacket, WirePacket, parse_datagram},
};
use rptadv_jitter_client::Admission;
use std::{
    ffi::c_void,
    path::PathBuf,
    time::{Duration, Instant},
};

#[path = "support/codec_fault.rs"]
mod codec_fault;

struct Providers {
    catalog: CodecCatalog,
    _jitter: Library,
    descriptor: *const c_void,
}
impl Providers {
    fn new() -> Self {
        let path = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap_or_default());
        let catalog = CodecCatalog::load(&[
            ProviderSelection {
                path: path.join("librptadv_codec_gsm.so.1"),
                profiles: vec![ProfileId::GsmFr],
            },
            ProviderSelection {
                path: path.join("librptadv_codec_speex.so.1"),
                profiles: vec![ProfileId::Speex],
            },
        ])
        .unwrap();
        let jitter =
            unsafe { Library::new(path.join("rpt_advanced/librptadv_jitter_adapter.so.1")) }
                .unwrap();
        let descriptor = unsafe {
            jitter
                .get::<unsafe extern "C" fn() -> *const c_void>(
                    b"rptadv_jitter_adapter_descriptor_v1",
                )
                .unwrap()()
        };
        Self {
            catalog,
            _jitter: jitter,
            descriptor,
        }
    }
    fn playout(&self, codec: Codec, origin: Instant) -> Playout {
        unsafe { Playout::new(&self.catalog, codec, 0.0, self.descriptor, origin) }.unwrap()
    }
    fn packets(&self, codec: Codec, sequence: u16, count: usize) -> Vec<Vec<u8>> {
        let mut encoder = Packetizer::new(&self.catalog, codec, 0.0, sequence, 0).unwrap();
        let mut packets = Vec::new();
        for block in 0..count {
            let pcm: Vec<_> = (0..640)
                .map(|i| ((block * 640 + i) as f32 * 0.47).sin() * 0.25)
                .collect();
            assert_eq!(encoder.push(&pcm).unwrap(), 640);
            packets.push(encoder.packet().unwrap().bytes.clone());
            encoder.retire().unwrap();
        }
        packets
    }
}
fn voice(bytes: &[u8]) -> VoicePacket<'_> {
    let WirePacket::Voice(mut packet) = parse_datagram(Channel::Audio, bytes).unwrap() else {
        panic!("voice")
    };
    packet.timestamp = 0; // Real EchoLink implementations need not supply a useful RTP clock.
    packet
}

#[test]
fn receive_gain_reload_preserves_a_partially_accepted_decoded_packet() {
    let providers = Providers::new();
    let now = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 1, 2);
    let mut changed = providers.playout(Codec::GsmFr, now);
    let mut reference = providers.playout(Codec::GsmFr, now);
    for playout in [&mut changed, &mut reference] {
        for (index, packet) in packets.iter().enumerate() {
            playout
                .receive(
                    voice(packet),
                    now + Duration::from_millis(index as u64 * 80),
                )
                .unwrap();
        }
    }
    let mut output = Sink {
        limit: 73,
        ..Sink::default()
    };
    for millisecond in 180..500 {
        changed
            .poll(now + Duration::from_millis(millisecond), &mut output)
            .unwrap();
        if !output.audio.is_empty() {
            break;
        }
    }
    assert_eq!(output.audio.len(), 73);
    changed.set_gain(6.020599913279624).unwrap();
    for invalid in [f64::NAN, f64::INFINITY, 10000.0] {
        assert!(changed.set_gain(invalid).is_err());
    }
    output.limit = usize::MAX;
    drain(&mut changed, now, &mut output);
    let mut original = Sink {
        limit: usize::MAX,
        ..Sink::default()
    };
    drain(&mut reference, now, &mut original);
    assert_eq!(output.audio.len(), original.audio.len());
    assert_eq!(&output.audio[..640], &original.audio[..640]);
    assert!(!original.audio[640..].is_empty());
    for (sample, reference) in output.audio[640..].iter().zip(&original.audio[640..]) {
        assert_eq!(*sample, *reference * 2.0);
    }
}

#[test]
fn codec_preflight_rejects_contenders_without_priming_a_media_timeline() {
    let providers = Providers::new();
    let now = Instant::now();
    for codec in [Codec::GsmFr, Codec::Speex] {
        let mut playout = providers.playout(codec, now);
        let packets = providers.packets(codec, 1, 1);
        let packet = voice(&packets[0]);
        assert!(playout.validate(&packet).unwrap());
        assert!(playout.validate(&packet).unwrap());
        for payload in [&[][..], &[0][..], &[0; 253][..]] {
            assert!(
                !playout
                    .validate(&VoicePacket { payload, ..packet })
                    .unwrap()
            );
        }
        let other = if codec == Codec::GsmFr {
            Codec::Speex
        } else {
            Codec::GsmFr
        };
        let other_packets = providers.packets(other, 1, 1);
        assert!(playout.validate(&voice(&other_packets[0])).unwrap());
        assert_eq!(playout.observe().unwrap().queued_packets, 0);
        assert_eq!(
            playout.receive(packet, now).unwrap(),
            Reception::Admitted(Admission::Accepted)
        );
        assert_eq!(playout.observe().unwrap().queued_packets, 1);
    }
}
#[derive(Default)]
struct Sink {
    next: u64,
    audio: Vec<f32>,
    gaps: u64,
    concealed: u64,
    limit: usize,
}
impl PcmSink for Sink {
    fn audio(&mut self, source_start: u64, pcm: &[f32]) -> usize {
        assert_eq!(source_start, self.next);
        let count = pcm.len().min(self.limit);
        self.audio.extend_from_slice(&pcm[..count]);
        self.next += count as u64;
        count
    }
    fn gap(&mut self, source_start: u64, samples: u32) -> u32 {
        assert_eq!(source_start, self.next);
        let count = samples.min(self.limit as u32);
        self.gaps += u64::from(count);
        self.next += u64::from(count);
        count
    }
    fn concealed(&mut self, source_start: u64, pcm: &[f32]) -> usize {
        let accepted = self.audio(source_start, pcm);
        self.concealed += accepted as u64;
        accepted
    }
}
fn drain(playout: &mut Playout, origin: Instant, sink: &mut Sink) {
    playout.end();
    let mut completed = 0;
    for millisecond in 180..2500 {
        if playout
            .poll(origin + Duration::from_millis(millisecond), sink)
            .unwrap()
            == Progress::Drained
        {
            completed += 1;
        }
    }
    assert_eq!(
        completed, 1,
        "exactly one final unkey notification after accepted media"
    );
    assert!(playout.drained());
}

#[test]
fn sequence_wrap_zero_timestamp_and_reordering_preserve_every_sample() {
    let providers = Providers::new();
    let origin = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 65534, 3);
    let mut playout = providers.playout(Codec::GsmFr, origin);
    for (index, arrival) in [(0, 0), (2, 80), (1, 90)] {
        assert_eq!(
            playout
                .receive(
                    voice(&packets[index]),
                    origin + Duration::from_millis(arrival)
                )
                .unwrap(),
            Reception::Admitted(Admission::Accepted)
        );
    }
    assert_eq!(
        playout.receive(voice(&packets[1]), origin).unwrap(),
        Reception::Admitted(Admission::Duplicate)
    );
    let mut sink = Sink {
        limit: 37,
        ..Sink::default()
    };
    drain(&mut playout, origin, &mut sink);
    assert_eq!((sink.next, sink.gaps), (1920, 0));
    let mut decoder = providers
        .catalog
        .decoder(ProfileId::GsmFr, 0, 640, 252)
        .unwrap();
    let mut expected = Vec::new();
    for packet in &packets {
        let mut pcm = [0.0; 640];
        decoder.decode(voice(packet).payload, &mut pcm).unwrap();
        expected.extend_from_slice(&pcm);
    }
    assert_eq!(
        sink.audio, expected,
        "partial sink writes must neither decode twice nor reorder audio"
    );
}

#[test]
fn one_receive_timeline_reorders_mixed_codecs_and_uses_each_packets_decoder() {
    let providers = Providers::new();
    let origin = Instant::now();
    let codecs = [Codec::GsmFr, Codec::Speex, Codec::GsmFr, Codec::Speex];
    let packets: Vec<_> = codecs
        .iter()
        .enumerate()
        .map(|(index, codec)| providers.packets(*codec, index as u16, 1).remove(0))
        .collect();
    let mut playout = providers.playout(Codec::GsmFr, origin);
    // Reorder across a codec transition without resetting the shared sequence timeline.
    for (index, millis) in [(0, 0), (2, 80), (1, 90), (3, 160)] {
        assert_eq!(
            playout
                .receive(
                    voice(&packets[index]),
                    origin + Duration::from_millis(millis)
                )
                .unwrap(),
            Reception::Admitted(Admission::Accepted)
        );
    }
    let mut sink = Sink {
        limit: 37,
        ..Default::default()
    };
    drain(&mut playout, origin, &mut sink);
    assert_eq!((sink.next, sink.gaps), (2560, 0));
    let mut gsm = providers
        .catalog
        .decoder(ProfileId::GsmFr, 0, 640, 252)
        .unwrap();
    let mut speex = providers
        .catalog
        .decoder(ProfileId::Speex, 0, 640, 252)
        .unwrap();
    let mut expected = Vec::new();
    for (codec, bytes) in codecs.into_iter().zip(&packets) {
        let decoder = if codec == Codec::GsmFr {
            &mut gsm
        } else {
            &mut speex
        };
        let mut pcm = [0.0; 640];
        decoder.decode(voice(bytes).payload, &mut pcm).unwrap();
        expected.extend_from_slice(&pcm);
    }
    assert_eq!(sink.audio, expected);
}

#[test]
fn missing_audio_uses_native_codec_plc_before_ring_fallback_and_never_runs_past_tail() {
    let providers = Providers::new();
    let origin = Instant::now();
    for codec in [Codec::GsmFr, Codec::Speex] {
        let packets = providers.packets(codec, 1, 3);
        let mut playout = providers.playout(codec, origin);
        playout.receive(voice(&packets[0]), origin).unwrap();
        playout
            .receive(voice(&packets[2]), origin + Duration::from_millis(160))
            .unwrap();
        let mut sink = Sink {
            limit: 43,
            ..Sink::default()
        };
        drain(&mut playout, origin, &mut sink);
        assert_eq!(sink.next, 1920);
        let counts = playout.concealment();
        assert_eq!(
            (counts.codec_samples, counts.gap_samples),
            if codec == Codec::GsmFr {
                (0, 640)
            } else {
                (640, 0)
            }
        );
        playout.reset().unwrap();
        assert_eq!(
            playout.concealment(),
            counts,
            "burst reset must not erase cumulative published counters"
        );
        if codec == Codec::GsmFr {
            assert_eq!(
                sink.gaps, 640,
                "unsupported codec PLC falls back to the ring"
            );
            assert_eq!(sink.concealed, 0);
        } else {
            assert_eq!(
                sink.gaps, 0,
                "Speex PLC must remain enabled for complete coding units"
            );
            assert_eq!(
                sink.concealed, 640,
                "native PLC must be distinguishable from received DTMF"
            );
        }
    }
}

#[test]
fn backpressure_blocks_further_jitter_admission_and_preserves_pending_pcm() {
    let providers = Providers::new();
    let origin = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 0, 2);
    let mut playout = providers.playout(Codec::GsmFr, origin);
    playout.receive(voice(&packets[0]), origin).unwrap();
    let mut sink = Sink::default();
    assert_eq!(
        playout
            .poll(origin + Duration::from_millis(180), &mut sink)
            .unwrap(),
        Progress::Blocked
    );
    assert_eq!(
        playout.receive(voice(&packets[1]), origin).unwrap(),
        Reception::Busy
    );
    assert!(!playout.drained());
    sink.limit = 80;
    for _ in 0..8 {
        playout
            .poll(origin + Duration::from_millis(180), &mut sink)
            .unwrap();
    }
    assert_eq!(sink.next, 640);
    assert_eq!(
        playout
            .receive(voice(&packets[1]), origin + Duration::from_millis(80))
            .unwrap(),
        Reception::Admitted(Admission::Accepted)
    );
    drain(&mut playout, origin, &mut sink);
    assert_eq!(sink.next, 1280);
}

#[test]
fn malformed_or_wrong_codec_does_not_revive_a_burst_or_shift_sequence_epoch() {
    let providers = Providers::new();
    let origin = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 1, 1);
    let mut playout = providers.playout(Codec::GsmFr, origin);
    let mut packet = voice(&packets[0]);
    packet.payload = &[];
    assert_eq!(playout.receive(packet, origin).unwrap(), Reception::Invalid);
    let mut packet = voice(&packets[0]);
    packet.codec = Codec::Speex;
    assert_eq!(playout.receive(packet, origin).unwrap(), Reception::Invalid);
    assert!(playout.drained());
    assert_eq!(
        playout.poll(origin, &mut Sink::default()).unwrap(),
        Progress::Idle(None)
    );
    playout.receive(voice(&packets[0]), origin).unwrap();
    assert!(
        playout.reset().is_err(),
        "never discard accepted tail on a new key"
    );
    let mut sink = Sink {
        limit: 640,
        ..Sink::default()
    };
    drain(&mut playout, origin, &mut sink);
    assert_eq!(
        playout.receive(voice(&packets[0]), origin).unwrap(),
        Reception::Busy,
        "an ended burst cannot accept media before reset"
    );
    playout.reset().unwrap();
    playout
        .receive(voice(&packets[0]), origin + Duration::from_secs(3))
        .unwrap();
    drain(&mut playout, origin + Duration::from_secs(3), &mut sink);
    assert_eq!(
        sink.next, 1280,
        "published positions must not rewind across bursts"
    );
}

#[test]
fn gsm_only_resources_cannot_accept_or_start_a_speex_stream() {
    let path = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap());
    let catalog = CodecCatalog::load(&[ProviderSelection {
        path: path.join("librptadv_codec_gsm.so.1"),
        profiles: vec![ProfileId::GsmFr],
    }])
    .unwrap();
    let providers = Providers::new();
    let origin = Instant::now();
    assert!(matches!(
        unsafe { Playout::new(&catalog, Codec::Speex, 0.0, providers.descriptor, origin) },
        Err(rptadv_echolink::media::MediaError::Invalid)
    ));
    let mut playout =
        unsafe { Playout::new(&catalog, Codec::GsmFr, 0.0, providers.descriptor, origin) }.unwrap();
    let packets = providers.packets(Codec::Speex, 1, 1);
    assert_eq!(
        playout.receive(voice(&packets[0]), origin).unwrap(),
        Reception::Invalid
    );
    assert!(playout.drained());
}

#[test]
fn malformed_gsm_uses_ring_gap_provenance_when_native_plc_is_unsupported() {
    let fixture = codec_fault::FaultCodec::new(ProfileId::GsmFr);
    let catalog = fixture.catalog();
    let providers = Providers::new();
    let origin = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 1, 1);
    let mut playout =
        unsafe { Playout::new(&catalog, Codec::GsmFr, 0.0, providers.descriptor, origin) }.unwrap();
    playout.receive(voice(&packets[0]), origin).unwrap();
    fixture.select(2);
    let mut sink = Sink {
        limit: 73,
        ..Sink::default()
    };
    drain(&mut playout, origin, &mut sink);
    assert!(sink.audio.is_empty());
    assert_eq!(sink.gaps, 640);
    assert_eq!((fixture.calls(false), fixture.calls(true)), (1, 1));
    assert_eq!(playout.concealment().codec_samples, 0);
    assert_eq!(playout.concealment().gap_samples, 640);
}

#[test]
fn native_plc_and_zero_decode_results_keep_exact_accepted_prefix_provenance() {
    for (profile, codec, fault, expected_pcm, expected_gap, expected_plc) in [
        (ProfileId::Speex, Codec::Speex, 2, 640, 0, 1),
        (ProfileId::GsmFr, Codec::GsmFr, 3, 0, 640, 0),
    ] {
        let fixture = codec_fault::FaultCodec::new(profile);
        let catalog = fixture.catalog();
        let providers = Providers::new();
        let origin = Instant::now();
        let packets = providers.packets(codec, 1, 1);
        let mut playout =
            unsafe { Playout::new(&catalog, codec, 0.0, providers.descriptor, origin) }.unwrap();
        playout.receive(voice(&packets[0]), origin).unwrap();
        fixture.select(fault);
        let mut sink = Sink {
            limit: 73,
            ..Sink::default()
        };
        drain(&mut playout, origin, &mut sink);
        assert_eq!((sink.audio.len(), sink.gaps), (expected_pcm, expected_gap));
        assert_eq!(
            (fixture.calls(false), fixture.calls(true)),
            (1, expected_plc)
        );
        assert_eq!(playout.concealment().codec_samples, expected_pcm as u64);
        assert_eq!(playout.concealment().gap_samples, expected_gap);
    }
}

#[test]
fn fatal_inspection_and_short_decode_cannot_publish_pcm() {
    use rptadv_echolink::media::MediaError;
    let fixture = codec_fault::FaultCodec::new(ProfileId::GsmFr);
    let catalog = fixture.catalog();
    let providers = Providers::new();
    let origin = Instant::now();
    let packets = providers.packets(Codec::GsmFr, 1, 1);
    let playout =
        unsafe { Playout::new(&catalog, Codec::GsmFr, 0.0, providers.descriptor, origin) }.unwrap();
    fixture.select(1);
    assert!(matches!(
        playout.validate(&voice(&packets[0])),
        Err(MediaError::Provider(_))
    ));
    assert_eq!((fixture.calls(false), fixture.calls(true)), (0, 0));
    for fault in [4, 5] {
        let mut playout =
            unsafe { Playout::new(&catalog, Codec::GsmFr, 0.0, providers.descriptor, origin) }
                .unwrap();
        fixture.select(fault);
        playout.receive(voice(&packets[0]), origin).unwrap();
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        let mut error = None;
        for millisecond in 180..500 {
            if let Err(found) = playout.poll(origin + Duration::from_millis(millisecond), &mut sink)
            {
                error = Some(found);
                break;
            }
        }
        assert!(matches!(
            (fault, error),
            (4, Some(MediaError::Contract)) | (5, Some(MediaError::Provider(_)))
        ));
        assert!(sink.audio.is_empty());
        assert_eq!(sink.gaps, 0);
    }
}

// Script only the neutral capability's signed-offset decisions. Encoding, decoding and
// native PLC remain the real dynamic Speex provider, so its history is independently checked.
mod offsets {
    use super::*;
    use rptadv_jitter_client::abi::{Config, Descriptor, Observation, Packet, RawDecision};
    use std::{mem::size_of, ptr};
    struct Script {
        positive: bool,
        overload: bool,
        step: u32,
        timestamp: u64,
        payload: Vec<u8>,
    }
    unsafe extern "C" fn create(_: *const Config, out: *mut *mut c_void) -> i32 {
        unsafe {
            *out = Box::into_raw(Box::new(Script {
                positive: false,
                overload: false,
                step: 0,
                timestamp: 0,
                payload: Vec::new(),
            }))
            .cast();
        }
        0
    }
    unsafe extern "C" fn create_positive(config: *const Config, out: *mut *mut c_void) -> i32 {
        unsafe {
            create(config, out);
            (*(*out).cast::<Script>()).positive = true;
        }
        0
    }
    unsafe extern "C" fn create_overload(config: *const Config, out: *mut *mut c_void) -> i32 {
        unsafe {
            create(config, out);
            (*(*out).cast::<Script>()).overload = true;
        }
        0
    }
    unsafe extern "C" fn destroy(context: *mut c_void) {
        unsafe {
            drop(Box::from_raw(context.cast::<Script>()));
        }
    }
    unsafe extern "C" fn reset(_: *mut c_void) -> i32 {
        0
    }
    unsafe extern "C" fn put(context: *mut c_void, packet: *const Packet) -> i32 {
        let (script, packet) = unsafe { (&mut *context.cast::<Script>(), &*packet) };
        script.timestamp = packet.timestamp;
        script.payload =
            unsafe { std::slice::from_raw_parts(packet.payload, packet.length) }.to_vec();
        0
    }
    unsafe extern "C" fn get(
        context: *mut c_void,
        _: u64,
        _: u32,
        out: *mut u8,
        _: usize,
        result: *mut RawDecision,
    ) -> i32 {
        let script = unsafe { &mut *context.cast::<Script>() };
        if script.overload {
            unsafe {
                *result = RawDecision {
                    struct_size: size_of::<RawDecision>() as u32,
                    kind: 3,
                    ..RawDecision::default()
                };
            }
            return 0;
        }
        let (kind, timestamp, span, offset) = if script.positive {
            (0, script.timestamp, 640, 160)
        } else {
            match script.step {
                0 => (2, script.timestamp - 80, 160, 0), // Insertion: no codec-history advance.
                1 => (1, script.timestamp - 80, 160, 0), // Real missing interval: native PLC.
                _ => (0, script.timestamp, 640, -80),    // Decode whole packet, then trim overlap.
            }
        };
        let length = if kind == 0 { script.payload.len() } else { 0 };
        unsafe {
            ptr::copy_nonoverlapping(script.payload.as_ptr(), out, length);
            *result = RawDecision {
                struct_size: size_of::<RawDecision>() as u32,
                kind,
                timestamp,
                span,
                start_offset: offset,
                length,
                deadline_ns: 0,
            };
        }
        0
    }
    unsafe extern "C" fn advance(context: *mut c_void, _: u64, _: u32) -> i32 {
        unsafe {
            (*context.cast::<Script>()).step += 1;
        }
        0
    }
    unsafe extern "C" fn observe(_: *const c_void, result: *mut Observation) -> i32 {
        unsafe {
            *result = Observation {
                struct_size: size_of::<Observation>() as u32,
                ..Observation::default()
            };
        }
        0
    }
    fn descriptor(positive: bool) -> Descriptor {
        Descriptor {
            struct_size: size_of::<Descriptor>() as u32,
            abi_version: 1,
            capability: *b"rptadv.jitter\0\0\0",
            create: Some(if positive { create_positive } else { create }),
            destroy: Some(destroy),
            reset: Some(reset),
            put: Some(put),
            get: Some(get),
            advance: Some(advance),
            observe: Some(observe),
        }
    }
    unsafe extern "C" fn wrong_span(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            (*result).span = 320;
        }
        0
    }
    unsafe extern "C" fn unknown_format(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            *output = 2;
        }
        0
    }
    unsafe extern "C" fn empty_payload(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            (*result).length = 0;
        }
        0
    }
    unsafe extern "C" fn excessive_overlap(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            (*result).start_offset = -641;
        }
        0
    }
    unsafe extern "C" fn long_insertion(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            if (*context.cast::<Script>()).step == 0 {
                (*result).span = 1280;
            }
        }
        0
    }
    unsafe extern "C" fn long_prefix(
        context: *mut c_void,
        now: u64,
        span: u32,
        output: *mut u8,
        capacity: usize,
        result: *mut RawDecision,
    ) -> i32 {
        unsafe {
            get(context, now, span, output, capacity, result);
            (*result).start_offset = 800;
        }
        0
    }
    struct ExcessSink;
    impl PcmSink for ExcessSink {
        fn audio(&mut self, _: u64, samples: &[f32]) -> usize {
            samples.len() + 1
        }
        fn gap(&mut self, _: u64, samples: u32) -> u32 {
            samples + 1
        }
    }
    #[test]
    fn provider_metadata_and_sink_credit_cannot_corrupt_the_playout_cursor() {
        use rptadv_echolink::media::MediaError;
        let providers = Providers::new();
        let origin = Instant::now();
        let packets = providers.packets(Codec::GsmFr, 1, 1);
        let baseline = descriptor(true);
        for (getter, jitter_rejection) in [
            (
                Some(wrong_span as unsafe extern "C" fn(_, _, _, _, _, _) -> _),
                false,
            ),
            (Some(unknown_format), false),
            (Some(empty_payload), false),
            (Some(excessive_overlap), true),
        ] {
            let mut descriptor = baseline;
            descriptor.get = getter;
            let mut playout = unsafe {
                Playout::new(
                    &providers.catalog,
                    Codec::GsmFr,
                    0.0,
                    ptr::from_ref(&descriptor).cast(),
                    origin,
                )
            }
            .unwrap();
            playout.receive(voice(&packets[0]), origin).unwrap();
            let mut sink = Sink {
                limit: 640,
                ..Sink::default()
            };
            // Positive offset first requests PLC; the next turn validates the
            // retained encoded packet before accepting any of its decoded PCM.
            let mut error = None;
            for _ in 0..3 {
                match playout.poll(origin + Duration::from_millis(180), &mut sink) {
                    Ok(_) => {}
                    Err(found) => {
                        error = Some(found);
                        break;
                    }
                }
            }
            assert!(matches!(
                (jitter_rejection, error),
                (false, Some(MediaError::Contract)) | (true, Some(MediaError::Jitter(_)))
            ));
            assert!(sink.audio.is_empty());
        }
        for positive in [false, true] {
            let descriptor = descriptor(positive);
            let mut playout = unsafe {
                Playout::new(
                    &providers.catalog,
                    Codec::Speex,
                    0.0,
                    ptr::from_ref(&descriptor).cast(),
                    origin,
                )
            }
            .unwrap();
            playout.receive(voice(&packets[0]), origin).unwrap();
            assert!(matches!(
                playout.poll(origin + Duration::from_millis(180), &mut ExcessSink),
                Err(MediaError::Contract)
            ));
        }
    }
    #[test]
    fn multi_block_delay_insertion_retains_the_decision_until_all_samples_are_accepted() {
        let providers = Providers::new();
        let origin = Instant::now();
        let packets = providers.packets(Codec::Speex, 1, 1);
        let mut descriptor = descriptor(false);
        descriptor.get = Some(long_insertion);
        let mut playout = unsafe {
            Playout::new(
                &providers.catalog,
                Codec::Speex,
                0.0,
                ptr::from_ref(&descriptor).cast(),
                origin,
            )
        }
        .unwrap();
        playout.receive(voice(&packets[0]), origin).unwrap();
        let mut sink = Sink {
            limit: 73,
            ..Sink::default()
        };
        drain(&mut playout, origin, &mut sink);
        assert_eq!((sink.next, sink.gaps), (2000, 1280));
        assert_eq!(sink.audio.len(), 720);
    }
    #[test]
    fn long_positive_offset_finishes_its_prefix_before_decoding_the_packet() {
        let providers = Providers::new();
        let origin = Instant::now();
        let packets = providers.packets(Codec::Speex, 1, 1);
        let mut descriptor = descriptor(true);
        descriptor.get = Some(long_prefix);
        let mut playout = unsafe {
            Playout::new(
                &providers.catalog,
                Codec::Speex,
                0.0,
                ptr::from_ref(&descriptor).cast(),
                origin,
            )
        }
        .unwrap();
        playout.receive(voice(&packets[0]), origin).unwrap();
        let mut sink = Sink {
            limit: 73,
            ..Sink::default()
        };
        drain(&mut playout, origin, &mut sink);
        assert_eq!((sink.next, sink.gaps, sink.audio.len()), (1440, 0, 1440));
        assert_eq!(playout.concealment().codec_samples, 800);
    }
    #[test]
    fn insertion_preserves_codec_history_and_overlap_is_trimmed_after_decode_once() {
        for positive in [false, true] {
            let providers = Providers::new();
            let origin = Instant::now();
            let packets = providers.packets(Codec::Speex, 1, 1);
            let descriptor = descriptor(positive);
            let mut playout = unsafe {
                Playout::new(
                    &providers.catalog,
                    Codec::Speex,
                    0.0,
                    (&descriptor as *const Descriptor).cast(),
                    origin,
                )
            }
            .unwrap();
            playout.receive(voice(&packets[0]), origin).unwrap();
            let mut sink = Sink {
                limit: 13,
                ..Sink::default()
            };
            drain(&mut playout, origin, &mut sink);
            let mut decoder = providers
                .catalog
                .decoder(ProfileId::Speex, 0, 640, 252)
                .unwrap();
            let mut missing = [0.0; 160];
            assert_eq!(decoder.conceal(160, &mut missing).unwrap(), 160);
            let mut pcm = [0.0; 640];
            decoder
                .decode(voice(&packets[0]).payload, &mut pcm)
                .unwrap();
            let mut expected = missing.to_vec();
            expected.extend_from_slice(if positive { &pcm } else { &pcm[80..] });
            assert_eq!(
                sink.audio, expected,
                "insertion or partial writes advanced Speex history"
            );
            assert_eq!(sink.gaps, if positive { 0 } else { 160 });
            assert_eq!(sink.next, if positive { 800 } else { 880 });
        }
    }

    #[test]
    fn jitter_overload_reset_does_not_hold_a_discarded_tail_forever() {
        let providers = Providers::new();
        let origin = Instant::now();
        let packets = providers.packets(Codec::GsmFr, 1, 1);
        let mut descriptor = descriptor(false);
        descriptor.create = Some(create_overload);
        let mut playout = unsafe {
            Playout::new(
                &providers.catalog,
                Codec::GsmFr,
                0.0,
                (&descriptor as *const Descriptor).cast(),
                origin,
            )
        }
        .unwrap();
        playout.receive(voice(&packets[0]), origin).unwrap();
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        drain(&mut playout, origin, &mut sink);
        assert_eq!(
            sink.next, 0,
            "discarded overload is counted by jitter, not fabricated as new audio"
        );
    }
}
