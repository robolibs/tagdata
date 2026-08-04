pub(crate) fn hit(stage: &str) {
    #[cfg(feature = "test-hooks")]
    if std::env::var("INSPACE_FAILPOINT").as_deref() == Ok(stage) {
        std::process::abort();
    }

    let _ = stage;
}

pub(crate) fn corrupt_file(
    stage: &str,
    file: &mut std::fs::File,
    offset: u64,
) -> crate::Result<()> {
    #[cfg(feature = "test-hooks")]
    if std::env::var("INSPACE_CORRUPT_STAGE").as_deref() == Ok(stage) {
        use std::io::{Read, Seek, SeekFrom, Write};

        file.seek(SeekFrom::Start(offset))?;
        let mut byte = [0];
        file.read_exact(&mut byte)?;
        byte[0] ^= 1;
        file.seek(SeekFrom::Start(offset))?;
        file.write_all(&byte)?;
        file.flush()?;
        file.sync_all()?;
    }

    let _ = (stage, file, offset);
    Ok(())
}
