//! Raw write-ahead-log byte sinks and durability policy.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

use super::StorageError;

/// A raw append-only destination for already framed WAL bytes.
pub trait WalBackend {
    /// Appends all bytes or returns an error.
    fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError>;

    /// Flushes userspace buffers to the operating system.
    fn flush(&mut self) -> Result<(), StorageError>;

    /// Requests that appended bytes reach stable storage.
    fn sync(&mut self) -> Result<(), StorageError>;
}

/// In-memory WAL sink intended for deterministic tests and development.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct MemoryWal {
    bytes: Vec<u8>,
    append_count: usize,
    flush_count: usize,
    sync_count: usize,
}

impl MemoryWal {
    /// Creates an empty in-memory WAL.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns all bytes appended so far.
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Returns the number of append calls.
    pub fn append_count(&self) -> usize {
        self.append_count
    }

    /// Returns the number of flush calls.
    pub fn flush_count(&self) -> usize {
        self.flush_count
    }

    /// Returns the number of sync calls.
    pub fn sync_count(&self) -> usize {
        self.sync_count
    }
}

impl WalBackend for MemoryWal {
    fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.bytes.extend_from_slice(bytes);
        self.append_count += 1;
        Ok(())
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.flush_count += 1;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.sync_count += 1;
        Ok(())
    }
}

/// File-backed append-only WAL byte sink.
#[derive(Debug)]
pub struct FileWal {
    file: File,
}

impl FileWal {
    /// Creates a new WAL file and fails if `path` already exists.
    ///
    /// # Errors
    ///
    /// Returns an I/O-backed [`StorageError`] when the file cannot be created.
    pub fn create(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let file = OpenOptions::new()
            .append(true)
            .create_new(true)
            .open(path)?;
        Ok(Self { file })
    }

    /// Opens an existing WAL for append and fails if `path` does not exist.
    ///
    /// # Errors
    ///
    /// Returns an I/O-backed [`StorageError`] when the file cannot be opened.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let file = OpenOptions::new().append(true).open(path)?;
        Ok(Self { file })
    }
}

impl WalBackend for FileWal {
    fn append(&mut self, bytes: &[u8]) -> Result<(), StorageError> {
        self.file.write_all(bytes).map_err(StorageError::from)
    }

    fn flush(&mut self) -> Result<(), StorageError> {
        self.file.flush().map_err(StorageError::from)
    }

    fn sync(&mut self) -> Result<(), StorageError> {
        self.file.sync_all().map_err(StorageError::from)
    }
}

/// Durability requested after one WAL append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Durability {
    /// Return after appending to the backend's current buffer.
    None,
    /// Flush userspace buffers after appending.
    Flush,
    /// Request stable-storage synchronization after appending.
    Sync,
}

/// Appends an owned byte vector and applies the requested durability policy.
///
/// Ownership prevents a caller from mutating the submitted buffer while the helper
/// performs the policy operation. `Sync` calls [`WalBackend::sync`] directly;
/// backends are responsible for any flushing required by their sync primitive.
///
/// # Errors
///
/// Returns the first append, flush, or sync error.
pub fn persist(
    backend: &mut impl WalBackend,
    bytes: Vec<u8>,
    durability: Durability,
) -> Result<(), StorageError> {
    backend.append(&bytes)?;
    match durability {
        Durability::None => Ok(()),
        Durability::Flush => backend.flush(),
        Durability::Sync => backend.sync(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_wal_retains_bytes_and_counts_policy_calls() {
        let mut wal = MemoryWal::new();

        persist(&mut wal, b"first".to_vec(), Durability::None).unwrap();
        persist(&mut wal, b"second".to_vec(), Durability::Flush).unwrap();
        persist(&mut wal, b"third".to_vec(), Durability::Sync).unwrap();

        assert_eq!(wal.bytes(), b"firstsecondthird");
        assert_eq!(wal.append_count(), 3);
        assert_eq!(wal.flush_count(), 1);
        assert_eq!(wal.sync_count(), 1);
    }
}
