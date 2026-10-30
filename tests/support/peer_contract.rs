//! Validate real peer ABI records without changing coding or received media.
use rptadv_echolink::ffi::*;
use std::{mem::size_of, ptr};

fn rejected(statuses: impl IntoIterator<Item = Status>) -> usize {
    statuses
        .into_iter()
        .enumerate()
        .filter(|(index, status)| {
            if *status == Status::Invalid {
                return false;
            }
            eprintln!("peer boundary case {index}: {status:?}, expected Invalid");
            true
        })
        .count()
}
fn encoded() -> rptadv_codec_client::EncodeResultRaw {
    let mut value: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
    value.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
    value
}
/// # Safety
/// QSO is live but has not been attached. The caller owns context and QSO.
pub unsafe fn before_attach(api: &Descriptor, context: *mut Context, qso: *mut Qso) -> usize {
    let mut read = ReadResult::default();
    let mut result = encoded();
    let mut registration = ptr::dangling_mut();
    let errors = rejected(unsafe {
        [
            api.peer_read.unwrap()(qso, 0, ptr::null(), None, ptr::null_mut(), &mut read),
            api.peer_prepare_audio.unwrap()(
                qso,
                ptr::null(),
                0,
                ptr::null_mut(),
                0,
                0,
                ptr::from_mut(&mut result).cast(),
            ),
            api.peer_send_prepared.unwrap()(qso, ByteSpan::default(), 0, 0),
            api.peer_reset_encoder.unwrap()(qso),
            api.peer_send_text.unwrap()(qso, ByteSpan::default(), ByteSpan::default()),
            api.peer_set_deadline.unwrap()(qso, 0),
            api.peer_attach.unwrap()(context, qso, None, ptr::null_mut(), &mut registration),
        ]
    });
    let mut observation = PeerObservation {
        struct_size: size_of::<PeerObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    errors
        + usize::from(!registration.is_null())
        + usize::from(
            unsafe { api.peer_observe.unwrap()(qso, &mut observation) } != Status::Pending,
        )
}
/// # Safety
/// Called once by the QSO's sole serial host callback with its live sink.
pub unsafe fn serial(api: &Descriptor, qso: *mut Qso, sink: &PcmSink) -> usize {
    let read = api.peer_read.unwrap();
    let mut errors = 0;
    let mut result = ReadResult::default();
    errors += rejected(unsafe {
        [
            read(qso, 3, sink, None, ptr::null_mut(), &mut result),
            read(qso, 0, sink, None, ptr::null_mut(), ptr::null_mut()),
        ]
    });
    for (size, abi) in [(0, 1), (size_of::<ReadResult>() as u32, 2)] {
        result.struct_size = size;
        result.abi_version = abi;
        errors += rejected([unsafe { read(qso, 0, sink, None, ptr::null_mut(), &mut result) }]);
    }
    result = ReadResult::default();
    for field in 0..4 {
        let mut invalid = PcmSink {
            struct_size: sink.struct_size,
            abi_version: sink.abi_version,
            context: sink.context,
            audio: sink.audio,
            gap: sink.gap,
        };
        match field {
            0 => invalid.struct_size -= 1,
            1 => invalid.abi_version = 2,
            2 => invalid.audio = None,
            _ => invalid.gap = None,
        }
        errors += rejected([unsafe { read(qso, 0, &invalid, None, ptr::null_mut(), &mut result) }]);
    }
    let prepare = api.peer_prepare_audio.unwrap();
    let mut encoded = encoded();
    let mut buffer = [0xa5; 264];
    let pcm = [0.1];
    errors += rejected([unsafe {
        prepare(
            qso,
            ptr::null(),
            0,
            buffer.as_mut_ptr(),
            buffer.len(),
            0,
            ptr::null_mut(),
        )
    }]);
    for (frames, input, capacity, output, flush, size) in [
        (0, ptr::null(), 264, buffer.as_mut_ptr(), 0, 0),
        (
            0,
            ptr::null(),
            264,
            buffer.as_mut_ptr(),
            2,
            size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
        ),
        (
            usize::MAX,
            pcm.as_ptr(),
            264,
            buffer.as_mut_ptr(),
            0,
            size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
        ),
        (
            1,
            ptr::null(),
            264,
            buffer.as_mut_ptr(),
            0,
            size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
        ),
        (
            0,
            ptr::null(),
            usize::MAX,
            buffer.as_mut_ptr(),
            0,
            size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
        ),
        (
            0,
            ptr::null(),
            1,
            ptr::null_mut(),
            0,
            size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
        ),
    ] {
        encoded.struct_size = size;
        errors += rejected([unsafe {
            prepare(
                qso,
                input,
                frames,
                output,
                capacity,
                flush,
                ptr::from_mut(&mut encoded).cast(),
            )
        }]);
    }
    errors += usize::from(buffer != [0xa5; 264]);
    encoded.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
    let status = unsafe {
        prepare(
            qso,
            ptr::null(),
            0,
            ptr::null_mut(),
            0,
            0,
            ptr::from_mut(&mut encoded).cast(),
        )
    };
    errors += usize::from(
        status != Status::Ok
            || encoded.status != 2
            || encoded.consumed_frames != 0
            || encoded.required_capacity != 264,
    );
    let send = api.peer_send_prepared.unwrap();
    for (data, length, frames) in [
        (buffer.as_ptr(), 12, 0),
        (buffer.as_ptr(), 12, 639),
        (ptr::null(), 144, 640),
        (buffer.as_ptr(), 11, 640),
        (buffer.as_ptr(), 265, 640),
        (buffer.as_ptr(), 12, 640),
    ] {
        errors += rejected([unsafe { send(qso, ByteSpan { data, length }, frames, 0) }]);
    }
    errors += usize::from(unsafe { send(qso, ByteSpan::default(), 0, 0) } != Status::Ok);
    let valid = ByteSpan {
        data: b"valid".as_ptr(),
        length: 5,
    };
    let invalid_utf8 = [0xff];
    for invalid in [
        ByteSpan {
            data: ptr::null(),
            length: 1,
        },
        ByteSpan {
            data: buffer.as_ptr(),
            length: 0,
        },
        ByteSpan {
            data: buffer.as_ptr(),
            length: 4097,
        },
        ByteSpan {
            data: invalid_utf8.as_ptr(),
            length: 1,
        },
    ] {
        errors += rejected(unsafe {
            [
                api.peer_send_text.unwrap()(qso, invalid, valid),
                api.peer_send_text.unwrap()(qso, valid, invalid),
            ]
        });
    }
    for (sender, body) in [
        (b"bad>".as_slice(), b"message".as_slice()),
        (b"N0CALL".as_slice(), b"bad\nmessage".as_slice()),
    ] {
        errors += rejected([unsafe {
            api.peer_send_text.unwrap()(
                qso,
                ByteSpan {
                    data: sender.as_ptr(),
                    length: sender.len(),
                },
                ByteSpan {
                    data: body.as_ptr(),
                    length: body.len(),
                },
            )
        }]);
    }
    for (size, abi) in [(0, 1), (size_of::<PeerObservation>() as u32, 2)] {
        let mut observation = PeerObservation {
            struct_size: size,
            abi_version: abi,
            ..Default::default()
        };
        errors += rejected([unsafe { api.peer_observe.unwrap()(qso, &mut observation) }]);
    }
    for deadline in [u64::MAX, u64::MAX - 1, 0] {
        errors +=
            usize::from(unsafe { api.peer_set_deadline.unwrap()(qso, deadline) } != Status::Ok);
    }
    errors
}
