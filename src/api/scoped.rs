use std::fmt;

use crate::{DB, Error, Tx};

/// Distinguishes database failures from an application's transaction error.
#[derive(Debug)]
pub enum TransactionError<E> {
    Storage(Error),
    Application(E),
}

impl<E> TransactionError<E> {
    pub fn application(error: E) -> Self {
        Self::Application(error)
    }

    pub fn storage(&self) -> Option<&Error> {
        match self {
            Self::Storage(error) => Some(error),
            Self::Application(_) => None,
        }
    }

    pub fn application_ref(&self) -> Option<&E> {
        match self {
            Self::Storage(_) => None,
            Self::Application(error) => Some(error),
        }
    }
}

impl<E> From<Error> for TransactionError<E> {
    fn from(value: Error) -> Self {
        Self::Storage(value)
    }
}

impl<E: fmt::Display> fmt::Display for TransactionError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Storage(error) => write!(f, "transaction storage error: {error}"),
            Self::Application(error) => write!(f, "transaction application error: {error}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for TransactionError<E> {}

impl DB {
    /// Runs a synchronous read transaction while preserving application errors.
    pub fn read<T, E>(
        &self,
        operation: impl FnOnce(&Tx<'_>) -> std::result::Result<T, TransactionError<E>>,
    ) -> std::result::Result<T, TransactionError<E>> {
        let tx = self.read_tx().map_err(TransactionError::Storage)?;
        operation(&tx)
    }

    /// Runs a synchronous write transaction and commits only on success.
    ///
    /// Panics are resumed after the transaction is dropped outside the panicking
    /// state, preventing the database's writer mutex from becoming poisoned.
    pub fn write<T, E>(
        &self,
        operation: impl FnOnce(&Tx<'_>) -> std::result::Result<T, TransactionError<E>>,
    ) -> std::result::Result<T, TransactionError<E>> {
        let tx = self.write_tx().map_err(TransactionError::Storage)?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&tx))) {
            Ok(Ok(result)) => {
                tx.commit().map_err(TransactionError::Storage)?;
                Ok(result)
            }
            Ok(Err(error)) => Err(error),
            Err(payload) => {
                drop(tx);
                std::panic::resume_unwind(payload)
            }
        }
    }

    /// Runs a scoped write after waiting at most `timeout` for the writer slot.
    pub fn write_timeout<T, E>(
        &self,
        timeout: std::time::Duration,
        operation: impl FnOnce(&Tx<'_>) -> std::result::Result<T, TransactionError<E>>,
    ) -> std::result::Result<T, TransactionError<E>> {
        let tx = self
            .write_tx_timeout(timeout)
            .map_err(TransactionError::Storage)?;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| operation(&tx))) {
            Ok(Ok(result)) => {
                tx.commit().map_err(TransactionError::Storage)?;
                Ok(result)
            }
            Ok(Err(error)) => Err(error),
            Err(payload) => {
                drop(tx);
                std::panic::resume_unwind(payload)
            }
        }
    }
}
