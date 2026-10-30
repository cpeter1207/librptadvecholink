//! Startup-only dynamic resource ownership, retained through account/QSO shutdown.
use crate::{
    ffi::{ByteSpan, RuntimeConfig, Status},
    media::CodecChoice,
};
use libloading::Library;
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use rptadv_jitter_client::Jitter;
use std::{ffi::c_void, path::PathBuf, slice, str};

pub(crate) struct Resources {
    // Ownership anchors, not new codec/jitter implementations. Keep their code
    // loaded through every account's last serialized media/drain callback.
    catalog: CodecCatalog,
    _jitter: Library,
    jitter_descriptor: *const c_void,
}
// SAFETY: immutable library/catalog descriptors may be read concurrently.
// Mutable codec/jitter instances are separately constructed per exclusive owner;
// these anchors keep all referenced DSO code loaded until their users quiesce.
unsafe impl Send for Resources {}
unsafe impl Sync for Resources {}

impl Resources {
    /// Borrow the startup-validated catalog; peer state retains this resource owner.
    pub(crate) fn catalog(&self) -> &CodecCatalog {
        &self.catalog
    }

    /// Descriptor code stays loaded through every derived peer jitter instance.
    pub(crate) fn jitter_descriptor(&self) -> *const c_void {
        self.jitter_descriptor
    }
    /// Caller spans remain readable for the call; own library handles before publication.
    pub(crate) unsafe fn load(config: &RuntimeConfig) -> Result<Option<Self>, Status> {
        let gsm = unsafe { path(config.gsm_library) }?;
        let speex = unsafe { path(config.speex_library) }?;
        let jitter = unsafe { path(config.jitter_library) }?;
        if gsm.is_empty() && speex.is_empty() && jitter.is_empty() {
            return Ok(None);
        }
        if gsm.is_empty() || jitter.is_empty() {
            return Err(Status::Invalid);
        }
        let mut selections = vec![ProviderSelection {
            path: PathBuf::from(gsm),
            profiles: vec![ProfileId::GsmFr],
        }];
        if !speex.is_empty() {
            selections.push(ProviderSelection {
                path: PathBuf::from(speex),
                profiles: vec![ProfileId::Speex],
            });
        }
        let catalog = CodecCatalog::load(&selections).map_err(|_| Status::Failed)?;
        CodecChoice::new(&catalog).map_err(|_| Status::Failed)?;
        // SAFETY: configured trusted library, loaded off audio; retain its handle
        // through every use of the immutable descriptor and its functions.
        let library = unsafe { Library::new(jitter) }.map_err(|_| Status::Failed)?;
        let descriptor = unsafe {
            library.get::<unsafe extern "C" fn() -> *const c_void>(
                b"rptadv_jitter_adapter_descriptor_v1",
            )
        }
        .map_err(|_| Status::Failed)?;
        let descriptor = unsafe { descriptor() };
        // Validate the complete released capability and actual EchoLink settings
        // before publishing the context, not at the first received voice packet.
        drop(
            unsafe { Jitter::new_at_rate(descriptor, 8000, 160, 253) }
                .map_err(|_| Status::Failed)?,
        );
        Ok(Some(Self {
            catalog,
            _jitter: library,
            jitter_descriptor: descriptor,
        }))
    }
}

unsafe fn path<'a>(span: ByteSpan) -> Result<&'a str, Status> {
    if span.length == 0 {
        return Ok("");
    }
    if span.length > 4096 || span.data.is_null() {
        return Err(Status::Invalid);
    }
    // SAFETY: the sized-record caller promises readable span storage for this call.
    let bytes = unsafe { slice::from_raw_parts(span.data, span.length) };
    let value = str::from_utf8(bytes).map_err(|_| Status::Invalid)?;
    if value.contains('\0') {
        return Err(Status::Invalid);
    }
    Ok(value)
}
