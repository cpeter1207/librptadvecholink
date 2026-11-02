//! Real released codec/jitter capabilities; no radio or account activation.
use super::settings_fixture::span;
use rptadv_echolink::ffi::*;
use std::ptr;
pub fn resource_context(api: &Descriptor) -> *mut Context {
    resource_context_with_threads(api, 1)
}
pub fn resource_context_with_threads(api: &Descriptor, worker_threads: u32) -> *mut Context {
    let directory = std::env::var("RPTADV_CODEC_DIR").unwrap();
    let gsm = format!("{directory}/librptadv_codec_gsm.so.1");
    let speex = format!("{directory}/librptadv_codec_speex.so.1");
    let jitter = format!("{directory}/rpt_advanced/librptadv_jitter_adapter.so.1");
    let runtime = RuntimeConfig {
        worker_threads,
        gsm_library: span(&gsm),
        speex_library: span(&speex),
        jitter_library: span(&jitter),
        ..RuntimeConfig::default()
    };
    let mut context = ptr::null_mut();
    assert_eq!(
        unsafe { api.context_create.unwrap()(&runtime, &mut context) },
        Status::Ok
    );
    context
}
