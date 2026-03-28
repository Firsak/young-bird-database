use std::fs::{File, OpenOptions};
use std::io::Write;

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::wal::wal_entry::WalEntry;

/// Appends WAL entries to the WAL file during normal operation.
///
/// Owns the file handle in append mode. Does not know about transactions —
/// that logic lives on the Executor. Responsibilities:
///   - serialize + append entries
///   - fsync for durability guarantees
///   - truncate after successful commit + page flush
pub struct WalWriter {
    path: String,
    file: File,
}

impl WalWriter {
    /// Opens or creates the WAL file at `path` in append mode.
    ///
    /// If the file already exists (e.g. after a crash), existing entries are
    /// preserved so that `recover_from_wal` can replay them on startup.
    ///
    /// # Errors
    /// Returns `DatabaseError::Io` if the file cannot be opened or created.
    pub fn new(path: String) -> Result<Self, DatabaseError> {
        let file = match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => file,
            Err(error) => {
                println!("Error opening or creating the file {}: {}", &path, error);
                return Err(DatabaseError::Io(error));
            }
        };

        Ok(Self { path, file })
    }

    /// Serializes `entry` and appends it to the WAL file.
    ///
    /// Does not fsync — bytes may sit in the OS buffer until `fsync()` is called.
    /// On COMMIT, call `fsync()` after the last append to guarantee durability.
    ///
    /// # Errors
    /// Returns `DatabaseError::Io` on write failure.
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), DatabaseError> {
        let bytes = entry.to_bytes();
        match self.file.write_all(&bytes) {
            Ok(_) => Ok(()),
            Err(error) => {
                println!(
                    "Error writing WAL entry to the file {}: {error}",
                    &self.path
                );
                Err(DatabaseError::Io(error))
            }
        }
    }

    /// Flushes OS buffers to disk — guarantees entries are durable after this returns.
    ///
    /// Must be called before flushing data pages on COMMIT. Uses `sync_all()`
    /// (Rust's fsync) rather than `flush()` — the latter only flushes Rust's
    /// internal buffers, not the OS kernel buffer.
    ///
    /// # Errors
    /// Returns `DatabaseError::Io` if the fsync syscall fails.
    pub fn fsync(&self) -> Result<(), DatabaseError> {
        match self.file.sync_all() {
            Ok(_) => Ok(()),
            Err(error) => {
                println!("Error syncing WAL file {}: {error}", &self.path);
                Err(DatabaseError::Io(error))
            }
        }
    }

    /// Empties the WAL file. Called after all dirty pages have been flushed to disk.
    ///
    /// Reopens the file twice: once with `.truncate(true)` to clear it, then
    /// again with `.append(true)` to restore the write handle for the next transaction.
    ///
    /// # Errors
    /// Returns `DatabaseError::Io` if truncation or reopen fails.
    pub fn truncate(&mut self) -> Result<(), DatabaseError> {
        match OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.path)
        {
            Ok(_) => {}
            Err(error) => {
                println!("Error truncating the file {}: {}", &self.path, error);
                return Err(DatabaseError::Io(error));
            }
        };
        let file = match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(file) => file,
            Err(error) => {
                println!("Error reopening WAL file {}: {}", &self.path, error);
                return Err(DatabaseError::Io(error));
            }
        };
        self.file = file;
        Ok(())
    }
}
