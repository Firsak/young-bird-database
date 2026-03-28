use std::fs::File;
use std::io::ErrorKind;
use std::os::unix::fs::FileExt;

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::wal::wal_entry::WalEntry;

/// Reads all WAL entries sequentially from the file at `path`.
///
/// Returns an empty Vec if the file does not exist (no recovery needed).
/// Called once on startup before the WalWriter takes over.
///
/// Uses `read_at` with a manual offset rather than sequential `read`/`read_exact`
/// so the file cursor never advances — each read is positioned explicitly.
///
/// # Errors
/// Returns `DatabaseError::Io` on read failures (other than NotFound).
/// Returns `DatabaseError::Serialization` if an entry is truncated or unparseable.
pub fn read_all(path: &str) -> Result<Vec<WalEntry>, DatabaseError> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(DatabaseError::Io(e)),
    };

    let mut entries = Vec::new();
    let mut offset = 0u64;

    loop {
        // Step 1: read 4-byte entry_size prefix
        let mut entry_size_buffer = vec![0u8; 4];
        let read_bytes = match file.read_at(&mut entry_size_buffer, offset) {
            Ok(value) => value,
            Err(error) => return Err(DatabaseError::Io(error)),
        };

        if read_bytes == 0 {
            break; // clean EOF
        }
        if read_bytes < 4 {
            return Err(DatabaseError::Serialization(format!(
                "truncated entry_size prefix in WAL {} at offset {}",
                path, offset
            )));
        }

        let entry_size = u32::from_le_bytes(entry_size_buffer.clone().try_into().unwrap());
        offset += 4;

        // Step 2: read entry_size bytes (the entry body)
        let mut entry_buffer = vec![0u8; entry_size as usize];
        let read_bytes = match file.read_at(&mut entry_buffer, offset) {
            Ok(value) => value,
            Err(error) => return Err(DatabaseError::Io(error)),
        };

        if read_bytes < entry_size as usize {
            return Err(DatabaseError::Serialization(format!(
                "truncated entry body in WAL {} at offset {}",
                path, offset
            )));
        }

        // Step 3: reconstruct full entry bytes (size prefix + body) and parse
        let total_bytes = [entry_size_buffer, entry_buffer].concat();
        let entry = WalEntry::from_bytes(&total_bytes)?;
        offset += entry_size as u64;
        entries.push(entry);
    }

    Ok(entries)
}
