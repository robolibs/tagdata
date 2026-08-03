pub(crate) fn hit(stage: &str) {
    #[cfg(feature = "test-hooks")]
    if std::env::var("INSPACE_FAILPOINT").as_deref() == Ok(stage) {
        std::process::abort();
    }

    let _ = stage;
}
