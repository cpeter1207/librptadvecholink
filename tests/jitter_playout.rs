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
        assert!(
            !playout
                .validate(&VoicePacket {
                    codec: other,
                    ..packet
                })
                .unwrap()
        );
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
        if codec == Codec::GsmFr {
            assert_eq!(
                sink.gaps, 640,
                "unsupported codec PLC falls back to the ring"
            );
        } else {
            assert_eq!(
                sink.gaps, 0,
                "Speex PLC must remain enabled for complete coding units"
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
