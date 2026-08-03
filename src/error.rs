use std::fmt;
use std::io;

/// An `inspace` operation result.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors returned by `inspace`.
#[derive(Debug)]
pub enum Error {
    /// An operating-system I/O operation failed.
    Io(io::Error),
    /// The database file is not an `inspace` file or contains corrupt data.
    Corrupt(&'static str),
    /// The requested bucket does not exist.
    BucketNotFound,
    /// The requested bucket already exists.
    BucketExists,
    /// A key or bucket name is too large for the on-disk format.
    TooLarge,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "I/O error: {error}"),
            Self::Corrupt(reason) => write!(f, "corrupt database: {reason}"),
            Self::BucketNotFound => f.write_str("bucket not found"),
            Self::BucketExists => f.write_str("bucket already exists"),
            Self::TooLarge => f.write_str("key, value, or transaction is too large"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}
