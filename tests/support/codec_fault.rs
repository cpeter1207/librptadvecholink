//! Test-only descriptor wrapper; retain it until all derived codec contexts are dropped.
use libloading::Library;
use rptadv_codec_client::{CodecCatalog, ProfileId, ProviderSelection};
use std::{
    ffi::c_void,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct FaultCodec {
    path: PathBuf,
    profile: ProfileId,
    directory: PathBuf,
    shim: Library,
    _original: Library,
}
impl FaultCodec {
    pub fn new(profile: ProfileId) -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let directory = PathBuf::from(std::env::var_os("RPTADV_CODEC_DIR").unwrap());
        let name = match profile {
            ProfileId::GsmFr => "gsm",
            ProfileId::Speex => "speex",
            _ => panic!("unexpected EchoLink codec"),
        };
        let original =
            unsafe { Library::new(directory.join(format!("librptadv_codec_{name}.so.1"))) }
                .unwrap();
        std::fs::create_dir_all(root.join("target")).unwrap();
        let path = root.join("target").join(format!(
            "codec-fault-{}-{}.so",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let compile =
            std::process::Command::new(std::env::var_os("CC").unwrap_or_else(|| "cc".into()))
                .args([
                    "-std=c11", "-Wall", "-Wextra", "-Werror", "-shared", "-fPIC",
                ])
                .arg(root.join("tests/codec_fault.c"))
                .arg("-o")
                .arg(&path)
                .output()
                .unwrap();
        assert!(
            compile.status.success(),
            "{}",
            String::from_utf8_lossy(&compile.stderr)
        );
        let shim = unsafe { Library::new(&path) }.unwrap();
        unsafe {
            let descriptor = original
                .get::<unsafe extern "C" fn() -> *const c_void>(b"rptadv_codec_descriptor_v1")
                .unwrap()();
            shim.get::<unsafe extern "C" fn(*const c_void)>(b"rptadv_test_configure")
                .unwrap()(descriptor);
        }
        Self {
            path,
            profile,
            directory,
            shim,
            _original: original,
        }
    }
    pub fn catalog(&self) -> CodecCatalog {
        CodecCatalog::load(&[ProfileId::GsmFr, ProfileId::Speex].map(|profile| {
            let name = if profile == ProfileId::GsmFr {
                "gsm"
            } else {
                "speex"
            };
            ProviderSelection {
                path: if profile == self.profile {
                    self.path.clone()
                } else {
                    self.directory.join(format!("librptadv_codec_{name}.so.1"))
                },
                profiles: vec![profile],
            }
        }))
        .unwrap()
    }
    pub fn select(&self, fault: i32) {
        unsafe {
            self.shim
                .get::<unsafe extern "C" fn(i32)>(b"rptadv_test_fault")
                .unwrap()(fault);
        }
    }
    pub fn calls(&self, concealed: bool) -> usize {
        unsafe {
            self.shim
                .get::<unsafe extern "C" fn(u32) -> usize>(b"rptadv_test_calls")
                .unwrap()(u32::from(concealed))
        }
    }
}
impl Drop for FaultCodec {
    fn drop(&mut self) {
        std::fs::remove_file(&self.path).unwrap();
    }
}
