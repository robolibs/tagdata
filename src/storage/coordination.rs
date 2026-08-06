#[cfg(any(
    target_os = "android",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos"
))]
#[path = "coordination/ofd.rs"]
mod platform;

#[cfg(not(any(
    target_os = "android",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos"
)))]
compile_error!("tagdata requires open-file-description lock support");

pub(crate) use platform::{Coordination, GateGuard, MAX_DATA_FILE_BYTES, ReaderRegistration};
