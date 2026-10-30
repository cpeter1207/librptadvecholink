//! Real codec/jitter and account channels exercise the composed receive owner.
use libloading::Library;
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use rptadv_echolink::{
    account::{Account, coordination::Coordinator},
    directory::DirectorySnapshot,
    media::{CodecChoice, MediaError, Packetizer},
    playout::{PcmSink, Playout},
    receive::{Incoming, Receiver},
    session::{Cause, Qso, QsoAction, QsoEvent, QsoState, Timers},
    transport::{driver::Control, mailbox, routing::Router},
    wire::{Channel, Codec, ControlPacket, write_control},
};
use std::{
    ffi::c_void,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

struct Providers {
    catalog: CodecCatalog,
    _jitter: Library,
    descriptor: *const c_void,
}

#[path = "support/codec_fault.rs"]
mod codec_fault;
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
}

#[derive(Default)]
struct Sink {
    samples: usize,
    gaps: usize,
    limit: usize,
}
impl PcmSink for Sink {
    fn audio(&mut self, _: u64, pcm: &[f32]) -> usize {
        let count = pcm.len().min(self.limit);
        self.samples += count;
        count
    }
    fn gap(&mut self, _: u64, count: u32) -> u32 {
        let count = (count as usize).min(self.limit);
        self.gaps += count;
        count as u32
    }
}

fn datagram(generation: u64, class: Channel, arrived: Instant, bytes: &[u8]) -> mailbox::Datagram {
    let (sender, mut receiver) = mailbox::channel(generation).unwrap();
    sender
        .try_push(class, "192.0.2.1:5198".parse().unwrap(), arrived, bytes)
        .unwrap();
    receiver.try_recv().unwrap()
}
fn sdes() -> Vec<u8> {
    let mut bytes = vec![0; 512];
    let n = write_control(
        &ControlPacket::Sdes {
            source: 42,
            callsign: "K1ABC-R".into(),
            station_name: "Test".into(),
            speex: true,
        },
        &mut bytes,
    )
    .unwrap();
    bytes.truncate(n);
    bytes
}
fn run(test: impl Future<Output = ()>) {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(test);
}

fn setup(providers: &Providers, now: Instant) -> (Coordinator, Receiver, u64) {
    let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
    directory
        .update(b"@@@\n1\nK1ABC-R\nTest\n1\n192.0.2.1\n+++\n", now)
        .unwrap();
    let mut router = Router::new(
        Account::new(2, vec![], vec![]).unwrap(),
        2,
        Duration::from_secs(30),
    )
    .unwrap();
    let connection = router.dial(&directory, 1, now).unwrap();
    let generation = connection.generation;
    let qso = Qso::new(
        connection.identity,
        generation,
        Timers {
            heartbeat: Duration::from_secs(10),
            liveness: Duration::from_secs(100),
            connect: Duration::from_secs(30),
        },
        now,
    )
    .unwrap();
    let coordinator = Coordinator::new(router).unwrap();
    let talker = coordinator.port(1, generation, Arc::new(Control::default()));
    let playout = unsafe {
        Playout::new(
            &providers.catalog,
            Codec::GsmFr,
            0.0,
            providers.descriptor,
            now,
        )
    }
    .unwrap();
    let receiver = Receiver::new(
        qso,
        playout,
        CodecChoice::new(&providers.catalog).unwrap(),
        talker,
    );
    (coordinator, receiver, generation)
}

#[test]
fn receive_observation_preserves_first_cause_and_separates_raw_from_selected_audio() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut report = [0; 128];
        let count = write_control(&ControlPacket::Report { source: 42 }, &mut report).unwrap();
        assert!(matches!(
            receiver
                .packet(datagram(
                    generation,
                    Channel::Control,
                    now,
                    &report[..count]
                ))
                .unwrap(),
            Incoming::Ignored
        ));
        let chat = receiver
            .packet(datagram(
                generation,
                Channel::Audio,
                now,
                b"oNDATAK1ABC-R>hello\r\n\0",
            ))
            .unwrap();
        assert!(
            matches!(chat, Incoming::Text { sender, body } if sender == "K1ABC-R" && body == "hello")
        );
        receiver
            .packet(datagram(generation + 1, Channel::Audio, now, b"bad"))
            .unwrap();
        receiver
            .packet(datagram(generation, Channel::Audio, now, b"bad"))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        let bytes = &encoder.packet().unwrap().bytes;
        receiver
            .packet(datagram(generation, Channel::Audio, now, bytes))
            .unwrap();
        let pending = receiver.observe(now + Duration::from_millis(25)).unwrap();
        assert_eq!(pending.session.state, QsoState::Connected);
        assert_eq!(pending.session.first_cause, None);
        assert!(pending.session.raw_receiving);
        assert!(!pending.selected);
        assert_eq!((pending.stale_packets, pending.malformed_packets), (1, 1));
        assert_eq!(pending.retained_age_ms, 25);
        assert_eq!(pending.session.last_valid_age_ms, 25);
        assert_eq!(pending.session.heartbeat_due_ms, 9975);
        assert_eq!(pending.session.liveness_due_ms, 99975);
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        receiver.poll(now, false, &mut sink).unwrap();
        coordinator.service().await;
        receiver.poll(now, false, &mut sink).unwrap();
        assert!(receiver.observe(now).unwrap().selected);
        receiver.control(QsoEvent::TransportLost, now);
        receiver.control(QsoEvent::Stop, now);
        let closed = receiver.observe(now + Duration::from_secs(1)).unwrap();
        assert_eq!(closed.session.first_cause, Some(Cause::TransportLost));
        assert_eq!(closed.session.state, QsoState::Closing);
        assert!(!closed.session.raw_receiving);
        assert_eq!(closed.session.heartbeat_due_ms, u64::MAX);
    });
}

#[test]
fn incoming_initial_sdes_establishes_once_without_waiting_for_another_heartbeat() {
    run(async {
        use rptadv_echolink::transport::routing::RouteResult;
        let providers = Providers::new();
        let now = Instant::now();
        let (_coordinator, mut receiver, generation) = setup(&providers, now);
        assert!(matches!(
            receiver
                .packet(datagram(
                    generation,
                    Channel::Audio,
                    now,
                    b"oNDATAK1ABC-R>hello\r\n\0"
                ))
                .unwrap(),
            Incoming::Ignored
        ));
        let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
        directory
            .update(b"@@@\n1\nK1ABC-R\nTest\n1\n192.0.2.1\n+++\n", now)
            .unwrap();
        let mut router = Router::new(
            Account::new(2, vec![], vec![]).unwrap(),
            2,
            Duration::from_secs(30),
        )
        .unwrap();
        let RouteResult::Pending(initial) = router.receive(
            &directory,
            Channel::Control,
            "192.0.2.1:5199".parse().unwrap(),
            now,
            &sdes(),
        ) else {
            panic!("fixture must pass real directory routing");
        };
        let mut wrong = initial.clone();
        wrong.source = "[::1]:5199".parse().unwrap();
        assert_eq!(receiver.initial(&wrong), None);
        wrong = initial.clone();
        wrong.control = ControlPacket::Report { source: 42 };
        assert_eq!(receiver.initial(&wrong), None);
        wrong = initial.clone();
        wrong.source = "192.0.2.2:5199".parse().unwrap();
        assert_eq!(receiver.initial(&wrong), None);
        assert_eq!(receiver.state(), QsoState::Connecting);
        assert_eq!(receiver.codec(), Codec::GsmFr);
        assert_eq!(receiver.initial(&initial), Some(QsoAction::Connected));
        assert_eq!(receiver.state(), QsoState::Connected);
        assert_eq!(receiver.codec(), Codec::Speex);
        assert_eq!(receiver.initial(&initial), None);
    });
}

#[test]
fn selected_audio_keeps_its_tail_until_product_drain_then_unkeys_once() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        assert!(!receiver.product_finish_pending());
        assert!(matches!(
            receiver
                .packet(datagram(generation, Channel::Control, now, &sdes()))
                .unwrap(),
            Incoming::Action(QsoAction::Connected)
        ));
        assert_eq!(receiver.codec(), Codec::Speex);
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        let bytes = &encoder.packet().unwrap().bytes;
        assert!(matches!(
            receiver
                .packet(datagram(generation + 1, Channel::Audio, now, bytes))
                .unwrap(),
            Incoming::Ignored
        ));
        assert!(matches!(
            receiver
                .packet(datagram(generation, Channel::Audio, now, bytes))
                .unwrap(),
            Incoming::Retained
        ));
        assert!(!receiver.can_receive());
        assert!(matches!(
            receiver.packet(datagram(generation, Channel::Audio, now, bytes)),
            Err(MediaError::Contract)
        ));
        assert!(
            matches!(
                receiver
                    .packet(datagram(generation + 1, Channel::Audio, now, bytes))
                    .unwrap(),
                Incoming::Ignored
            ),
            "stale input must not turn backpressure into a terminal error"
        );
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        assert_eq!(receiver.poll(now, true, &mut sink).unwrap().activity, None);
        coordinator.service().await;
        assert_eq!(
            receiver.poll(now, false, &mut sink).unwrap().activity,
            Some(true)
        );
        assert!(receiver.can_receive());
        let later = now + Duration::from_millis(400);
        for _ in 0..8 {
            receiver.poll(later, false, &mut sink).unwrap();
        }
        assert_eq!(sink.samples, 640);
        assert!(
            receiver.product_finish_pending(),
            "finite source end must reach the product before waiting for its ring drain"
        );
        coordinator.service().await;
        assert_eq!(
            coordinator.poll(later),
            None,
            "published but unplayed PCM must still hold receive"
        );
        receiver.poll(later, true, &mut sink).unwrap();
        coordinator.service().await;
        assert_eq!(coordinator.poll(later), Some((1, generation)));
        assert_eq!(
            receiver.poll(later, true, &mut sink).unwrap().activity,
            Some(false)
        );
        assert!(!receiver.product_finish_pending());
        assert_eq!(
            receiver.poll(later, true, &mut sink).unwrap().activity,
            None
        );
        assert_eq!(
            sink.samples, 640,
            "the final packet was replayed or discarded"
        );
        assert_eq!(sink.gaps, 0);
        assert!(
            matches!(
                receiver
                    .packet(datagram(
                        generation,
                        Channel::Audio,
                        later + Duration::from_millis(150),
                        bytes
                    ))
                    .unwrap(),
                Incoming::Ignored
            ),
            "a delayed duplicate after unkey must not open another burst"
        );
        assert_eq!(
            receiver.control(QsoEvent::Stop, later),
            Some(QsoAction::Closing {
                cause: Cause::LocalRequest,
                send_bye: true
            })
        );
        receiver.poll(later, true, &mut sink).unwrap();
        assert_eq!(receiver.state(), QsoState::Closed);
        assert!(matches!(
            receiver
                .packet(datagram(generation, Channel::Audio, later, bytes))
                .unwrap(),
            Incoming::Ignored
        ));
    });
}

#[test]
fn suppressed_and_invalid_media_never_reach_pcm_or_selected_activity() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        let mut directory = DirectorySnapshot::new(Duration::from_secs(900));
        directory
            .update(b"@@@\n1\nK2ABC-R\nOther\n2\n192.0.2.2\n+++\n", now)
            .unwrap();
        let other = coordinator.router().dial(&directory, 2, now).unwrap();
        let mut other_port = coordinator.port(2, other.generation, Arc::new(Control::default()));
        other_port.offer(now).unwrap();
        coordinator.service().await;
        other_port.selection().unwrap();
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        receiver
            .packet(datagram(
                generation,
                Channel::Audio,
                now,
                &encoder.packet().unwrap().bytes,
            ))
            .unwrap();
        coordinator.service().await;
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        assert_eq!(receiver.poll(now, true, &mut sink).unwrap().activity, None);
        assert!(receiver.can_receive());
        assert_eq!(sink.samples, 0);
        let late = now + Duration::from_secs(99);
        for bytes in [&[][..], &[0; 144][..], b"oNDATA\rStation information\0"] {
            assert!(matches!(
                receiver
                    .packet(datagram(generation, Channel::Audio, late, bytes))
                    .unwrap(),
                Incoming::Ignored
            ));
        }
        assert_eq!(
            receiver
                .poll(now + Duration::from_secs(100), true, &mut sink)
                .unwrap()
                .action,
            Some(QsoAction::Closing {
                cause: Cause::LivenessExpired,
                send_bye: true
            })
        );
        assert_eq!(receiver.state(), QsoState::Closed);
        assert_eq!(sink.samples, 0);
        assert_eq!(sink.gaps, 0);
    });
}

#[test]
fn invalid_addresses_and_talker_queue_limits_do_not_publish_receive_audio() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        let (sender, mut input) = mailbox::channel(generation).unwrap();
        sender
            .try_push(
                Channel::Control,
                "[::1]:5199".parse().unwrap(),
                now,
                &sdes(),
            )
            .unwrap();
        assert!(matches!(
            receiver.packet(input.try_recv().unwrap()).unwrap(),
            Incoming::Ignored
        ));
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        let mut ports: Vec<_> = (100..104)
            .map(|node| coordinator.port(node, 1, Arc::new(Control::default())))
            .collect();
        for port in &mut ports {
            port.offer(now).unwrap();
        }
        assert!(matches!(
            receiver
                .packet(datagram(
                    generation,
                    Channel::Audio,
                    now,
                    &encoder.packet().unwrap().bytes
                ))
                .unwrap(),
            Incoming::Ignored
        ));
        coordinator.close();
        assert!(matches!(
            receiver.packet(datagram(
                generation,
                Channel::Audio,
                now,
                &encoder.packet().unwrap().bytes
            )),
            Err(MediaError::Contract)
        ));
        assert!(!receiver.observe(now).unwrap().selected);
    });
}

#[test]
fn lost_account_control_preserves_the_selected_tail_until_product_completion() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        receiver
            .packet(datagram(
                generation,
                Channel::Audio,
                now,
                &encoder.packet().unwrap().bytes,
            ))
            .unwrap();
        coordinator.service().await;
        receiver.control(QsoEvent::TransportLost, now);
        coordinator.close();
        let mut sink = Sink::default();
        assert_eq!(
            receiver.poll(now, false, &mut sink).unwrap().activity,
            Some(true)
        );
        assert_eq!(receiver.poll(now, false, &mut sink).unwrap().activity, None);
        assert_eq!(receiver.state(), QsoState::Closing);
        let later = now + Duration::from_millis(400);
        sink.limit = 37;
        for _ in 0..40 {
            assert_eq!(
                receiver.poll(later, false, &mut sink).unwrap().activity,
                None
            );
        }
        assert_eq!(sink.samples, 640);
        assert_eq!(receiver.state(), QsoState::Closing);
        assert_eq!(
            receiver.poll(later, true, &mut sink).unwrap().activity,
            Some(false)
        );
        assert_eq!(receiver.state(), QsoState::Closed);
        assert_eq!(
            receiver.poll(later, true, &mut sink).unwrap().activity,
            None
        );
    });
}

#[test]
fn vanished_talker_control_returns_a_contract_error_before_or_after_grant() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        for granted in [false, true] {
            let (mut coordinator, mut receiver, generation) = setup(&providers, now);
            receiver
                .packet(datagram(generation, Channel::Control, now, &sdes()))
                .unwrap();
            let mut encoder =
                Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
            encoder.push(&[0.1; 640]).unwrap();
            receiver
                .packet(datagram(
                    generation,
                    Channel::Audio,
                    now,
                    &encoder.packet().unwrap().bytes,
                ))
                .unwrap();
            if granted {
                coordinator.service().await;
            }
            drop(coordinator);
            assert!(matches!(
                receiver.poll(now, false, &mut Sink::default()),
                Err(MediaError::Contract)
            ));
            assert_eq!(receiver.state(), QsoState::Connected);
        }
    });
}

#[test]
fn next_burst_waits_for_the_blocked_source_tail_without_losing_its_packet() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        receiver
            .packet(datagram(
                generation,
                Channel::Audio,
                now,
                &encoder.packet().unwrap().bytes,
            ))
            .unwrap();
        coordinator.service().await;
        let mut sink = Sink::default();
        assert_eq!(
            receiver.poll(now, false, &mut sink).unwrap().activity,
            Some(true)
        );
        assert!(!receiver.product_finish_pending());
        coordinator.service().await;
        let later = now + Duration::from_millis(400);
        receiver.poll(later, false, &mut sink).unwrap();
        assert!(!receiver.product_finish_pending());
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 2, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        assert!(matches!(
            receiver
                .packet(datagram(
                    generation,
                    Channel::Audio,
                    later,
                    &encoder.packet().unwrap().bytes
                ))
                .unwrap(),
            Incoming::Retained
        ));
        assert!(!receiver.product_finish_pending());
        coordinator.service().await;
        receiver.poll(later, false, &mut sink).unwrap();
        assert!(!receiver.can_receive());
        sink.limit = 37;
        for _ in 0..40 {
            receiver.poll(later, false, &mut sink).unwrap();
        }
        assert!(receiver.can_receive());
        for _ in 0..40 {
            receiver
                .poll(later + Duration::from_millis(400), false, &mut sink)
                .unwrap();
        }
        assert_eq!(sink.samples, 1280);
        assert_eq!(sink.gaps, 0);
        assert!(receiver.product_finish_pending());
    });
}

#[test]
fn codec_inspection_failure_never_becomes_selected_audio() {
    run(async {
        let fault = codec_fault::FaultCodec::new(ProfileId::GsmFr);
        let mut providers = Providers::new();
        providers.catalog = fault.catalog();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        let bytes = &encoder.packet().unwrap().bytes;
        fault.select(1);
        assert!(matches!(
            receiver.packet(datagram(generation, Channel::Audio, now, bytes)),
            Err(MediaError::Provider(_))
        ));
        fault.select(6);
        assert!(matches!(
            receiver
                .packet(datagram(generation, Channel::Audio, now, bytes))
                .unwrap(),
            Incoming::Ignored
        ));
        fault.select(0);
        assert!(matches!(
            receiver
                .packet(datagram(generation, Channel::Audio, now, bytes))
                .unwrap(),
            Incoming::Retained
        ));
        coordinator.service().await;
        fault.select(6); // Foreign inspection changed after initial validation.
        assert!(matches!(
            receiver.poll(now, false, &mut Sink::default()),
            Err(MediaError::Contract)
        ));
        assert_eq!(fault.calls(false), 0);
    });
}

#[test]
fn reordered_voice_and_blocked_pcm_keep_the_original_retained_input() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut sink = Sink::default();
        let mut blocked = false;
        for (index, (millis, sequence)) in [(0, 10), (20, 12), (40, 11), (200, 13)]
            .into_iter()
            .enumerate()
        {
            let arrived = now + Duration::from_millis(millis);
            if index == 3 {
                receiver.poll(arrived, false, &mut sink).unwrap();
            }
            let mut encoder =
                Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, sequence, 42).unwrap();
            encoder.push(&[0.1; 640]).unwrap();
            assert!(matches!(
                receiver
                    .packet(datagram(
                        generation,
                        Channel::Audio,
                        arrived,
                        &encoder.packet().unwrap().bytes
                    ))
                    .unwrap(),
                Incoming::Retained
            ));
            coordinator.service().await;
            receiver.poll(arrived, false, &mut sink).unwrap();
            coordinator.service().await; // Each new grant publishes its drain state.
            if !receiver.can_receive() {
                blocked = true;
                sink.limit = 37;
                for _ in 0..64 {
                    receiver.poll(arrived, false, &mut sink).unwrap();
                    if receiver.can_receive() {
                        break;
                    }
                }
                assert!(
                    receiver.can_receive(),
                    "retained packet did not resume: index={index} samples={} gaps={} jitter={:?}",
                    sink.samples,
                    sink.gaps,
                    receiver.observe(arrived).unwrap().jitter
                );
                sink.limit = 0;
            }
        }
        assert!(blocked, "the fixture must exercise actual PCM backpressure");
        assert_eq!(receiver.observe(now).unwrap().out_of_order_packets, 1);
        sink.limit = 37;
        let later = now + Duration::from_millis(700);
        for _ in 0..150 {
            receiver.poll(later, false, &mut sink).unwrap();
        }
        assert!(receiver.can_receive());
        assert_eq!(sink.samples, 2560);
        assert_eq!(sink.gaps, 0);
    });
}

#[test]
fn remote_bye_preserves_blocked_codec_pcm_and_the_first_terminal_cause() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut encoder = Packetizer::new(&providers.catalog, Codec::Speex, 0.0, 1, 42).unwrap();
        encoder.push(&[0.1; 640]).unwrap();
        receiver
            .packet(datagram(
                generation,
                Channel::Audio,
                now,
                &encoder.packet().unwrap().bytes,
            ))
            .unwrap();
        coordinator.service().await;
        let mut sink = Sink::default();
        receiver.poll(now, false, &mut sink).unwrap();
        let later = now + Duration::from_millis(200);
        receiver.poll(later, false, &mut sink).unwrap();
        let mut bye = [0; 128];
        let length = write_control(
            &ControlPacket::Bye {
                source: 42,
                reason: Vec::new(),
            },
            &mut bye,
        )
        .unwrap();
        assert!(matches!(
            receiver
                .packet(datagram(
                    generation,
                    Channel::Control,
                    later,
                    &bye[..length]
                ))
                .unwrap(),
            Incoming::Action(QsoAction::Closing {
                cause: Cause::RemoteBye,
                send_bye: false
            })
        ));
        assert_eq!(receiver.control(QsoEvent::TransportLost, later), None);
        receiver.poll(later, true, &mut sink).unwrap();
        assert_eq!(receiver.state(), QsoState::Closing);
        sink.limit = 37;
        for _ in 0..40 {
            receiver.poll(later, false, &mut sink).unwrap();
        }
        assert_eq!(sink.samples, 640);
        assert_eq!(receiver.state(), QsoState::Closing);
        receiver.poll(later, true, &mut sink).unwrap();
        assert_eq!(receiver.state(), QsoState::Closed);
    });
}

#[test]
fn independent_bursts_keep_rtp_wrap_and_published_pcm_positions() {
    run(async {
        let providers = Providers::new();
        let now = Instant::now();
        let (mut coordinator, mut receiver, generation) = setup(&providers, now);
        receiver
            .packet(datagram(generation, Channel::Control, now, &sdes()))
            .unwrap();
        let mut sink = Sink {
            limit: 640,
            ..Sink::default()
        };
        for (offset, sequence) in [(0, 65535), (900, 0)] {
            let start = now + Duration::from_millis(offset);
            let mut encoder =
                Packetizer::new(&providers.catalog, Codec::GsmFr, 0.0, sequence, 42).unwrap();
            encoder.push(&[0.1; 640]).unwrap();
            assert!(matches!(
                receiver
                    .packet(datagram(
                        generation,
                        Channel::Audio,
                        start,
                        &encoder.packet().unwrap().bytes
                    ))
                    .unwrap(),
                Incoming::Retained
            ));
            coordinator.service().await;
            assert_eq!(
                receiver.poll(start, false, &mut sink).unwrap().activity,
                Some(true)
            );
            let end = start + Duration::from_millis(400);
            for _ in 0..8 {
                receiver.poll(end, false, &mut sink).unwrap();
            }
            coordinator.service().await;
            receiver.poll(end, true, &mut sink).unwrap();
            coordinator.service().await;
            assert_eq!(coordinator.poll(end), Some((1, generation)));
            assert_eq!(
                receiver.poll(end, true, &mut sink).unwrap().activity,
                Some(false)
            );
            coordinator.poll(start + Duration::from_millis(800));
        }
        assert_eq!(sink.samples, 1280);
        assert_eq!(sink.gaps, 0);
    });
}
