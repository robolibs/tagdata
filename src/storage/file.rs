use std::{fs::File, io};

#[cfg(unix)]
use std::os::fd::AsRawFd;

#[cfg(any(target_os = "linux", target_os = "android"))]
pub(crate) fn allocate(file: &File, len: u64) -> io::Result<()> {
    let len = i64::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64"))?;
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) } == 0 {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

#[cfg(any(
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos"
))]
pub(crate) fn allocate(file: &File, len: u64) -> io::Result<()> {
    let len = i64::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64"))?;
    let mut store = libc::fstore_t {
        fst_flags: libc::F_ALLOCATECONTIG,
        fst_posmode: libc::F_PEOFPOSMODE,
        fst_offset: 0,
        fst_length: len,
        fst_bytesalloc: 0,
    };
    if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &store) } == -1 {
        store.fst_flags = libc::F_ALLOCATEALL;
        if unsafe { libc::fcntl(file.as_raw_fd(), libc::F_PREALLOCATE, &store) } == -1 {
            return Err(io::Error::last_os_error());
        }
    }
    file.set_len(len)
}

#[cfg(windows)]
pub(crate) fn allocate(file: &File, len: u64) -> io::Result<()> {
    use std::{mem, os::windows::io::AsRawHandle};
    use windows_sys::Win32::{
        Foundation::HANDLE,
        Storage::FileSystem::{
            FILE_ALLOCATION_INFO, FileAllocationInfo, SetFileInformationByHandle,
        },
    };

    let allocation_size = i64::try_from(len)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "file size exceeds i64"))?;
    let mut info = FILE_ALLOCATION_INFO {
        AllocationSize: allocation_size,
    };
    let result = unsafe {
        SetFileInformationByHandle(
            file.as_raw_handle() as HANDLE,
            FileAllocationInfo,
            &mut info as *mut _ as *mut _,
            mem::size_of::<FILE_ALLOCATION_INFO>() as u32,
        )
    };
    if result == 0 {
        return Err(io::Error::last_os_error());
    }
    file.set_len(len)
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "android",
    target_os = "macos",
    target_os = "ios",
    target_os = "tvos",
    target_os = "watchos",
    windows
)))]
pub(crate) fn allocate(file: &File, len: u64) -> io::Result<()> {
    file.set_len(len)
}
