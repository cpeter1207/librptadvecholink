//! Serial C registration: one receive owner, no peer borrow across a host callback.
use super::{Binding, Peer};
use crate::{
    account::{
        coordination::TalkerPort,
        network::{PeerConnection, PeerTransport},
    },
    ffi::{self, ReadResult, Status, StepFn},
    media::{CodecChoice, PreparedMedia},
    playout::{PcmSink, Playout, Progress},
    receive::{Incoming, Receiver},
    session::{Qso, QsoAction, QsoEvent, QsoState, Timers},
    transport::{
        driver::{PeerDriver, PeerEvent, PeerWait},
        mailbox::Consumer,
        routing::PendingHandshake,
    },
    wire::{Channel, Codec, ControlPacket, ProxyKind, ProxyRecord, write_control},
};
use std::{
    ffi::c_void,
    mem::size_of,
    net::SocketAddr,
    ptr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::runtime::Handle;

#[cfg(test)]
#[path = "../../tests/unit/owner_peer.rs"]
mod tests;

pub(super) struct Prepared {
    ingress: Consumer,
    initial: Option<PendingHandshake>,
    talker: TalkerPort,
    transport: PeerTransport,
}

/// Only the registered serial executor may access this state, including reentrant calls.
pub(super) struct Serial {
    settings: Arc<crate::account::settings::PreparedSettings>,
    deadline: Option<Instant>,
    receiver: Receiver,
    ingress: crate::transport::mailbox::Observer,
    encoder: PreparedMedia,
    transport: PeerTransport,
    control: Option<Vec<u8>>,
    writable: Option<crate::transport::driver::PeerWritable>,
    text: Option<(String, String)>,
    blocked: bool,
    direct_datagrams: u64,
    blocked_sends: u64,
    proxy_admitted: u64,
    text_admitted: u64,
    text_rejected: u64,
    cause_detail: [u8; 32],
}

struct Registration {
    // Join before dropping the retained peer, whose resources include the runtime.
    driver: Arc<PeerDriver>,
    peer: Arc<Peer>,
}

#[derive(Clone, Copy)]
struct Callback {
    function: StepFn,
    context: *mut c_void,
}
// The host retains its thread-safe context until off-executor detach returns.
unsafe impl Send for Callback {}
impl Callback {
    fn call(self) -> i32 {
        unsafe { (self.function)(self.context) }
    }
}

impl Peer {
    pub(super) fn new(binding: Arc<Binding>, connection: PeerConnection, created: Instant) -> Self {
        let PeerConnection {
            lease,
            connection,
            talker,
            control,
            transport,
        } = connection;
        Self {
            binding,
            identity: connection.identity,
            source: connection
                .initial
                .as_ref()
                .map(|initial| initial.source.to_string())
                .unwrap_or_default(),
            generation: connection.generation,
            control,
            _lease: lease,
            pending: std::sync::Mutex::new(Some(Prepared {
                ingress: connection.ingress,
                initial: connection.initial,
                talker,
                transport,
            })),
            serial: std::cell::UnsafeCell::new(None),
            created,
        }
    }
}

pub(crate) unsafe extern "C" fn peer_attach(
    context: *mut ffi::Context,
    qso: *mut ffi::Qso,
    callback: Option<StepFn>,
    opaque: *mut c_void,
    output: *mut *mut ffi::Registration,
) -> Status {
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = ptr::null_mut();
    let (Some(context), Some(peer), Some(callback)) = (
        unsafe { context.cast::<crate::Context>().as_ref() },
        unsafe { qso.cast::<Peer>().as_ref() },
        callback,
    ) else {
        return Status::Invalid;
    };
    if Handle::try_current().is_ok() || !Arc::ptr_eq(&context.runtime, &peer.binding.runtime) {
        return Status::Invalid;
    }
    // Replacement gates admission and collects every driver under this same
    // control-only lock. The spawned task never acquires it or waits on control.
    let binding = Arc::clone(&peer.binding);
    let Ok(mut drivers) = binding.drivers.lock() else {
        return Status::Failed;
    };
    let valid = peer.validate();
    if valid != Status::Ok {
        return valid;
    }
    let Ok(mut pending) = peer.pending.lock() else {
        return Status::Failed;
    };
    let Some(prepared) = pending.take() else {
        return Status::Invalid;
    };
    let resources = &peer.binding._resources;
    let settings = peer.binding.current.load_full();
    let policy = &settings.as_abi().policy;
    let playout = match unsafe {
        Playout::new(
            resources.catalog(),
            Codec::GsmFr,
            policy.receive_gain_db,
            resources.jitter_descriptor(),
            peer.created,
        )
    } {
        Ok(playout) => playout,
        Err(_) => return Status::Failed,
    };
    let encoder = match PreparedMedia::new(
        resources.catalog(),
        policy.transmit_gain_db,
        0,
        settings.as_abi().node_number,
    ) {
        Ok(encoder) => encoder,
        Err(_) => return Status::Failed,
    };
    // Startup validated the immutable catalog; admission validated the positive
    // generation and prepared settings validated every timer.
    let codecs = CodecChoice::new(resources.catalog()).expect("startup validated GSM");
    let qso_state = Qso::new(
        peer.identity.clone(),
        peer.generation,
        timers(policy),
        peer.created,
    )
    .expect("admission validated generation and timers");
    let mut receiver = Receiver::new(qso_state, playout, codecs, prepared.talker);
    if let Some(initial) = &prepared.initial {
        receiver.initial(initial);
    }
    let mut serial = Serial {
        settings,
        deadline: None,
        receiver,
        ingress: prepared.ingress.observer(),
        encoder,
        transport: prepared.transport,
        control: None,
        writable: None,
        text: None,
        blocked: false,
        direct_datagrams: 0,
        blocked_sends: 0,
        proxy_admitted: 0,
        text_admitted: 0,
        text_rejected: 0,
        cause_detail: [0; 32],
    };
    if serial.queue_control(peer, false).is_err() {
        return Status::Invalid;
    }
    unsafe {
        *peer.serial.get() = Some(serial);
    }
    drop(pending);
    // The caller's QSO ownership remains intact; the registration/task retain another lease.
    unsafe {
        Arc::increment_strong_count(qso.cast::<Peer>());
    }
    let peer = unsafe { Arc::from_raw(qso.cast::<Peer>()) };
    let active = Arc::clone(&peer);
    let callback = Callback {
        function: callback,
        context: opaque,
    };
    let driver = Arc::new(PeerDriver::attach(
        Arc::clone(&peer.binding.runtime),
        Arc::clone(&peer.control),
        prepared.ingress,
        move |event| {
            if active.control.is_stopped() {
                return None;
            }
            {
                let serial =
                    unsafe { (*active.serial.get()).as_mut() }.expect("attached serial owner");
                serial.event(&active, event);
            }
            if callback.call() < 0 {
                return None;
            }
            if active.control.is_stopped() {
                return None;
            }
            // End all prior borrows before callback.call: it may reenter this QSO.
            let serial = unsafe { (*active.serial.get()).as_mut() }.expect("attached serial owner");
            let now = Instant::now();
            serial.flush_control(&active);
            Some(PeerWait {
                receive: !serial.blocked && serial.receiver.can_receive() && serial.text.is_none(),
                deadline: [serial.deadline, serial.receiver.deadline(now)]
                    .into_iter()
                    .flatten()
                    .min(),
                writable: serial.writable.take(),
            })
        },
    ));
    drivers.retain(|driver| driver.strong_count() != 0);
    drivers.push(Arc::downgrade(&driver));
    *output = Box::into_raw(Box::new(Registration { driver, peer })).cast();
    Status::Ok
}

fn timers(policy: &ffi::AccountPolicy) -> Timers {
    Timers {
        heartbeat: Duration::from_millis(policy.heartbeat_interval_ms.into()),
        liveness: Duration::from_millis(policy.heartbeat_timeout_ms.into()),
        connect: Duration::from_millis(policy.connect_timeout_ms.into()),
    }
}

impl Serial {
    fn remember(&mut self, error: &impl std::fmt::Debug) {
        if self.cause_detail[0] == 0
            && matches!(
                self.receiver.state(),
                QsoState::Connecting | QsoState::Connected
            )
        {
            self.cause_detail = super::diagnostics::detail(error);
        }
    }
    fn queue_control(&mut self, peer: &Peer, bye: bool) -> Result<(), Status> {
        let packet = if bye {
            ControlPacket::Bye {
                source: self.settings.as_abi().node_number,
                reason: Vec::new(),
            }
        } else {
            ControlPacket::Sdes {
                source: self.settings.as_abi().node_number,
                callsign: self.settings.callsign().into(),
                station_name: self.settings.directory_login().description.into(),
                speex: peer
                    .binding
                    ._resources
                    .catalog()
                    .profile(rptadv_codec_client::ProfileId::Speex)
                    .is_some(),
            }
        };
        let mut output = [0; 512];
        let count = write_control(&packet, &mut output).map_err(|_| Status::Invalid)?;
        self.control = Some(output[..count].to_vec());
        Ok(())
    }

    fn action(&mut self, peer: &Peer, action: Option<QsoAction>) {
        let bye = match action {
            Some(QsoAction::Heartbeat) if self.control.is_none() => false,
            Some(QsoAction::Closing { send_bye: true, .. }) => true,
            _ => return,
        };
        if self.queue_control(peer, bye).is_err() {
            self.receiver.control(QsoEvent::OwnerFailed, Instant::now());
        }
    }

    fn send(
        &mut self,
        peer: &Peer,
        channel: Channel,
        payload: &[u8],
        expires: Option<Instant>,
    ) -> Status {
        if !peer.live() {
            return Status::Closed;
        }
        use crate::transport::{direct::SendError, driver::PeerWritable, proxy::QueueError};
        match &self.transport {
            PeerTransport::Direct(sockets) => {
                let destination = SocketAddr::new(
                    peer.identity.address.into(),
                    if channel == Channel::Audio {
                        5198
                    } else {
                        5199
                    },
                );
                match sockets.try_send(channel, destination, payload) {
                    Ok(()) => {
                        self.direct_datagrams = self.direct_datagrams.saturating_add(1);
                        Status::Ok
                    }
                    Err(SendError::Pending) => {
                        self.blocked_sends = self.blocked_sends.saturating_add(1);
                        self.writable = Some(PeerWritable::Direct(Arc::clone(sockets), channel));
                        Status::Pending
                    }
                    Err(SendError::Failed(error)) => {
                        self.remember(&error);
                        Status::Failed
                    }
                }
            }
            PeerTransport::Proxy(output) => {
                let kind = if channel == Channel::Audio {
                    ProxyKind::UdpData
                } else {
                    ProxyKind::UdpControl
                };
                match output.try_queue(
                    ProxyRecord {
                        kind,
                        address: peer.identity.address,
                        payload,
                    },
                    expires,
                ) {
                    Ok(()) => {
                        self.proxy_admitted = self.proxy_admitted.saturating_add(1);
                        Status::Ok
                    }
                    Err(QueueError::Full) => {
                        self.blocked_sends = self.blocked_sends.saturating_add(1);
                        self.writable = Some(PeerWritable::Proxy {
                            output: output.clone(),
                            kind,
                            payload_bytes: payload.len(),
                        });
                        Status::Full
                    }
                    // All callers pass serialized, bounded UDP records. Invalid
                    // input cannot occur here; other terminal failures close.
                    Err(_) => Status::Closed,
                }
            }
        }
    }

    fn event(&mut self, peer: &Peer, event: PeerEvent) {
        let settings = peer.binding.current.load_full();
        if !Arc::ptr_eq(&self.settings, &settings) {
            let policy = &settings.as_abi().policy;
            self.receiver
                .reconfigure(policy.receive_gain_db, timers(policy))
                .and_then(|_| self.encoder.set_gain(policy.transmit_gain_db))
                .expect("prepared finite gains and positive timers");
            self.settings = settings;
        }
        if self
            .deadline
            .is_some_and(|deadline| deadline <= Instant::now())
        {
            self.deadline = None;
        }
        let action = match event {
            PeerEvent::Packet(packet) => match self.receiver.packet(packet) {
                Ok(Incoming::Text { sender, body }) => {
                    self.text = Some((sender, body));
                    None
                }
                Ok(Incoming::Action(action)) => Some(action),
                Ok(_) => None,
                Err(error) => {
                    self.remember(&error);
                    self.receiver.control(QsoEvent::OwnerFailed, Instant::now())
                }
            },
            PeerEvent::Close => self.receiver.control(QsoEvent::Stop, Instant::now()),
            PeerEvent::Io(error) => {
                self.remember(&error);
                self.receiver
                    .control(QsoEvent::TransportLost, Instant::now())
            }
            _ => self.receiver.control(QsoEvent::Poll, Instant::now()),
        };
        self.action(peer, action);
        self.flush_control(peer);
    }

    fn flush_control(&mut self, peer: &Peer) {
        if let Some(packet) = self.control.take() {
            match self.send(peer, Channel::Control, &packet, None) {
                Status::Ok => {}
                Status::Pending | Status::Full => self.control = Some(packet),
                _ => {
                    self.receiver
                        .control(QsoEvent::TransportLost, Instant::now());
                }
            }
        }
    }
}

struct Sink<'a>(&'a ffi::PcmSink);
impl PcmSink for Sink<'_> {
    fn audio(&mut self, start: u64, pcm: &[f32]) -> usize {
        unsafe { self.0.audio.unwrap()(self.0.context, start, pcm.as_ptr(), pcm.len(), 0) }
    }
    fn concealed(&mut self, start: u64, pcm: &[f32]) -> usize {
        unsafe { self.0.audio.unwrap()(self.0.context, start, pcm.as_ptr(), pcm.len(), 1) }
    }
    fn gap(&mut self, start: u64, count: u32) -> u32 {
        unsafe { self.0.gap.unwrap()(self.0.context, start, count) }
    }
}

pub(crate) unsafe extern "C" fn peer_read(
    qso: *mut ffi::Qso,
    drained: u32,
    sink: *const ffi::PcmSink,
    text: Option<ffi::TextFn>,
    opaque: *mut c_void,
    output: *mut ReadResult,
) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if drained > 2
        || output.is_null()
        || unsafe { ptr::addr_of!((*output).struct_size).read() } < size_of::<ReadResult>() as u32
        || unsafe { ptr::addr_of!((*output).abi_version).read() } != 1
    {
        return Status::Invalid;
    }
    // A failed account still owes its attached host a final source turn;
    // explicit lifecycle retirement alone makes the serial state unreadable.
    if peer.control.is_stopped() {
        return Status::Closed;
    }
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    let mut result = ReadResult::default();
    if !sink.is_null() {
        if unsafe { ptr::addr_of!((*sink).struct_size).read() } < size_of::<ffi::PcmSink>() as u32
            || unsafe { ptr::addr_of!((*sink).abi_version).read() } != 1
        {
            return Status::Invalid;
        }
        let sink = unsafe { &*sink };
        if sink.audio.is_none() || sink.gap.is_none() {
            return Status::Invalid;
        }
        if let Some(callback) = text {
            if let Some((sender, body)) = serial.text.take() {
                // A conference's represented sender is not its authenticated QSO.
                // Only a fresh, exact directory match supplies a numeric alias.
                result.represented_node_number = peer
                    .binding
                    .control
                    .directory()
                    .by_callsign(&sender, Instant::now())
                    .map_or(0, |entry| entry.node_number);
                unsafe {
                    callback(
                        opaque,
                        ffi::ByteSpan {
                            data: sender.as_ptr(),
                            length: sender.len(),
                        },
                        ffi::ByteSpan {
                            data: body.as_ptr(),
                            length: body.len(),
                        },
                    )
                };
            }
        }
        let turn = match serial
            .receiver
            .poll(Instant::now(), drained != 0, &mut Sink(sink))
        {
            Ok(turn) => turn,
            Err(error) => {
                serial.remember(&error);
                serial
                    .receiver
                    .control(QsoEvent::OwnerFailed, Instant::now());
                return Status::Failed;
            }
        };
        serial.blocked = matches!(turn.progress, Progress::Blocked);
        result.progress = match turn.progress {
            Progress::Idle(_) => 0,
            Progress::Advanced => 1,
            Progress::Blocked => 2,
            Progress::Drained => 3,
        };
        result.activity = match turn.activity {
            Some(true) => 1,
            Some(false) => 2,
            None => 0,
        };
        serial.action(peer, turn.action);
    }
    result.finish_pending = u32::from(serial.receiver.product_finish_pending());
    result.state = match serial.receiver.state() {
        QsoState::Connecting => 0,
        QsoState::Connected => 1,
        QsoState::Closing => 2,
        QsoState::Closed => 3,
    };
    unsafe {
        ptr::write(output, result);
    }
    Status::Ok
}

/// Translate the released codec result record, never the private Rust convenience layout.
pub(crate) unsafe extern "C" fn peer_prepare_audio(
    qso: *mut ffi::Qso,
    pcm: *const f32,
    frames: usize,
    buffer: *mut u8,
    capacity: usize,
    flush: u32,
    output: *mut c_void,
) -> Status {
    let output = output.cast::<rptadv_codec_client::EncodeResultRaw>();
    if output.is_null()
        || unsafe { ptr::addr_of!((*output).struct_size).read() }
            < size_of::<rptadv_codec_client::EncodeResultRaw>() as u32
        || flush > 1
        || frames > isize::MAX as usize / size_of::<f32>()
        || (frames != 0 && pcm.is_null())
        || capacity > isize::MAX as usize
        || (capacity != 0 && buffer.is_null())
    {
        return Status::Invalid;
    }
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if !peer.live() {
        return Status::Closed;
    }
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    let input = if frames == 0 {
        &[]
    } else {
        unsafe { std::slice::from_raw_parts(pcm, frames) }
    };
    let buffer = if capacity == 0 {
        &mut []
    } else {
        unsafe { std::slice::from_raw_parts_mut(buffer, capacity) }
    };
    // SDES agreement is session-bound. Upgrade only when this call can advance
    // coding, preserving short-output no-side-effects and already copied RTP.
    if buffer.len() >= 264
        && serial.receiver.codec() == Codec::Speex
        && serial
            .encoder
            .upgrade_speex(peer.binding._resources.catalog())
            .is_err()
    {
        serial
            .receiver
            .control(QsoEvent::OwnerFailed, Instant::now());
        return Status::Failed;
    }
    let result = match serial.encoder.prepare(input, buffer, flush == 1) {
        Ok(result) => result,
        Err(error) => {
            serial.remember(&error);
            return Status::Failed;
        }
    };
    unsafe {
        ptr::write(
            output,
            rptadv_codec_client::EncodeResultRaw {
                struct_size: size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
                status: result.status.as_raw(),
                consumed_frames: result.consumed_frames,
                encoded_bytes: result.encoded_bytes,
                source_start_frames: result.source_start_frames,
                source_frames: result.source_frames,
                packet_frames: result.packet_frames,
                required_capacity: if result.status == rptadv_codec_client::Status::OutputFull {
                    264
                } else {
                    0
                },
            },
        );
    }
    Status::Ok
}

/// Submit complete prepared voice, preserving retry bytes and original source age.
/// Empty zero-frame markers finish local pacing; EchoLink has no wire key/unkey.
pub(crate) unsafe extern "C" fn peer_send_prepared(
    qso: *mut ffi::Qso,
    packet: ffi::ByteSpan,
    frames: u32,
    timestamp: u32,
) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if !peer.live() {
        return Status::Closed;
    }
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    if frames == 0 && packet.length == 0 {
        return Status::Ok;
    }
    if frames != 640 || packet.data.is_null() || !(12..=264).contains(&packet.length) {
        return Status::Invalid;
    }
    let packet = unsafe { std::slice::from_raw_parts(packet.data, packet.length) };
    if !matches!(
        crate::wire::parse_datagram(Channel::Audio, packet),
        Ok(crate::wire::WirePacket::Voice(_))
    ) {
        return Status::Invalid;
    }
    match serial.receiver.state() {
        QsoState::Connecting => return Status::Pending,
        QsoState::Closing | QsoState::Closed => return Status::Closed,
        QsoState::Connected => {}
    }
    // The shared product expires native media after 200 ms. Preserve that source
    // budget in the Proxy queue rather than granting a new lifetime on admission.
    let now = Instant::now();
    let age = (now.saturating_duration_since(peer.created).as_millis() as u32)
        .wrapping_sub(timestamp) as i32;
    if age >= 200 {
        return Status::Ok;
    }
    let expires = now.checked_add(Duration::from_millis((200 - age.max(0)) as u64));
    serial.send(peer, Channel::Audio, packet, expires)
}

/// Clear segment coding history on its sole owner, not the connection RTP clocks.
pub(crate) unsafe extern "C" fn peer_reset_encoder(qso: *mut ffi::Qso) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if !peer.live() {
        return Status::Closed;
    }
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    match serial.encoder.reset() {
        Ok(()) => Status::Ok,
        Err(_) => Status::Failed,
    }
}

/// Read the connection-relative source clock without changing media or retry age.
pub(crate) unsafe extern "C" fn peer_elapsed_ms(qso: *const ffi::Qso, output: *mut u64) -> Status {
    let Some(output) = (unsafe { output.as_mut() }) else {
        return Status::Invalid;
    };
    *output = 0;
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    *output = peer.created.elapsed().as_millis().min(u64::MAX as u128) as u64;
    Status::Ok
}

/// Serialize bounded human text on the QSO owner; no ASL envelope or new queue.
/// Full/Pending retains no input: the product retries its unchanged sender/body.
pub(crate) unsafe extern "C" fn peer_send_text(
    qso: *mut ffi::Qso,
    sender: ffi::ByteSpan,
    body: ffi::ByteSpan,
) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    if !peer.live() {
        return Status::Closed;
    }
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    let text = |span: ffi::ByteSpan, maximum: usize| {
        if span.data.is_null() || span.length == 0 || span.length > maximum {
            return None;
        }
        std::str::from_utf8(unsafe { std::slice::from_raw_parts(span.data, span.length) }).ok()
    };
    let status = (|| {
        let (Some(sender), Some(body)) =
            (text(sender, 64), text(body, crate::wire::MAX_CHAT_BYTES))
        else {
            return Status::Invalid;
        };
        let mut packet = [0; 4170];
        let Ok(count) = crate::wire::write_chat(sender, body, &mut packet) else {
            return Status::Invalid;
        };
        match serial.receiver.state() {
            QsoState::Connecting => Status::Pending,
            QsoState::Closing | QsoState::Closed => Status::Closed,
            QsoState::Connected => serial.send(peer, Channel::Audio, &packet[..count], None),
        }
    })();
    if status == Status::Ok {
        serial.text_admitted = serial.text_admitted.saturating_add(1);
    } else {
        serial.text_rejected = serial.text_rejected.saturating_add(1);
    }
    status
}

pub(crate) unsafe extern "C" fn peer_observe(
    qso: *const ffi::Qso,
    output: *mut ffi::PeerObservation,
) -> Status {
    if !unsafe { super::diagnostics::output(output) } {
        return Status::Invalid;
    }
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    // This operation shares peer_read's sole serial-owner contract. Closing state
    // remains readable until detach, so do not hide the first cause behind live().
    let Some(serial) = (unsafe { (*peer.serial.get()).as_ref() }) else {
        return Status::Pending;
    };
    let observed = match serial.receiver.observe(Instant::now()) {
        Ok(observed) => observed,
        Err(_) => return Status::Failed,
    };
    let jitter = observed.jitter;
    let ingress = serial.ingress.observe();
    let result = ffi::PeerObservation {
        struct_size: size_of::<ffi::PeerObservation>() as u32,
        abi_version: 1,
        state: match observed.session.state {
            QsoState::Connecting => 0,
            QsoState::Connected => 1,
            QsoState::Closing => 2,
            QsoState::Closed => 3,
        },
        first_cause: match observed.session.first_cause {
            None => 0,
            Some(crate::session::Cause::LocalRequest) => 1,
            Some(crate::session::Cause::RemoteBye) => 2,
            Some(crate::session::Cause::ConnectExpired) => 3,
            Some(crate::session::Cause::LivenessExpired) => 4,
            Some(crate::session::Cause::TransportLost) => 5,
            Some(crate::session::Cause::OwnerFailed) => 6,
        },
        cause_detail: serial.cause_detail,
        codec: u32::from(observed.codec == Codec::Speex),
        raw_receiving: u32::from(observed.session.raw_receiving),
        selected: u32::from(observed.selected),
        transport: u32::from(matches!(serial.transport, PeerTransport::Proxy(_))),
        last_valid_age_ms: observed.session.last_valid_age_ms,
        heartbeat_due_ms: observed.session.heartbeat_due_ms,
        liveness_due_ms: observed.session.liveness_due_ms,
        retained_age_ms: observed.retained_age_ms,
        stale_packets: observed.stale_packets,
        malformed_packets: observed.malformed_packets,
        out_of_order_packets: observed.out_of_order_packets,
        jitter: ffi::JitterObservation {
            queued_packets: jitter.queued_packets,
            delay_samples: jitter.delay,
            buffered_samples: jitter.buffered_span,
            duplicate_drops: jitter.duplicate_drops,
            late_drops: jitter.late_drops,
            full_drops: jitter.full_drops,
            missing_samples: jitter.missing_span,
            insertion_samples: jitter.insertion_span,
            skipped_samples: jitter.skipped_span,
            limit_resets: jitter.limit_resets,
            overload_drops: jitter.overload_drops,
            overload_samples: jitter.overload_span,
        },
        codec_plc_samples: observed.concealment.codec_samples,
        fallback_gap_samples: observed.concealment.gap_samples,
        direct_datagrams: serial.direct_datagrams,
        blocked_sends: serial.blocked_sends,
        proxy_admitted: serial.proxy_admitted,
        text_admitted: serial.text_admitted,
        text_rejected: serial.text_rejected,
        ingress_full_packets: ingress.full_drops,
        ingress_queued_packets: ingress.queued_packets,
        ingress_capacity_packets: ingress.capacity_packets,
        ingress_retained_bytes: ingress.retained_bytes,
    };
    unsafe { ptr::write(output, result) };
    Status::Ok
}

pub(crate) unsafe extern "C" fn peer_stop(registration: *mut ffi::Registration) {
    if let Some(registration) = unsafe { registration.cast::<Registration>().as_ref() } {
        registration.peer.control.request_close();
    }
}

pub(crate) unsafe extern "C" fn peer_wake(registration: *mut ffi::Registration) {
    if let Some(registration) = unsafe { registration.cast::<Registration>().as_ref() } {
        registration.peer.control.wake();
    }
}

pub(crate) unsafe extern "C" fn peer_detach(registration: *mut ffi::Registration) {
    if !registration.is_null() && Handle::try_current().is_err() {
        let registration = unsafe { Box::from_raw(registration.cast::<Registration>()) };
        registration.driver.wait();
        // peer remains retained until the join has completed.
    }
}

pub(crate) unsafe extern "C" fn peer_set_deadline(qso: *mut ffi::Qso, relative: u64) -> Status {
    let Some(peer) = (unsafe { qso.cast::<Peer>().as_ref() }) else {
        return Status::Invalid;
    };
    // Downstream credit/drain deadlines remain valid after transport loss.
    if peer.control.is_stopped() {
        return Status::Closed;
    }
    // Serialized host operation only, never a radio callback or concurrent control call.
    let Some(serial) = (unsafe { (*peer.serial.get()).as_mut() }) else {
        return Status::Invalid;
    };
    serial.deadline = if relative == u64::MAX {
        None
    } else {
        // On supported 64-bit Linux, even u64 nanoseconds (<585 years) fits
        // the signed-seconds monotonic representation.
        Some(Instant::now() + Duration::from_nanos(relative))
    };
    Status::Ok
}
