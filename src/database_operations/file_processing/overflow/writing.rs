use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use super::overflow_header::OverflowHeader;
use super::overflow_ref::OverflowRef;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::OVERFLOW_HEADER_SIZE;

/// Creates a new overflow file with a fresh header.
///
/// # Arguments
/// * `filename` - Path to the .overflow file (created if it doesn't exist)
pub fn create_overflow_file(filename: &str) -> Result<(), DatabaseError> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error creating overflow file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let header = OverflowHeader::new(OVERFLOW_HEADER_SIZE as u64, 0);
    file.write_all(&header.to_bytes())?;
    Ok(())
}

/// Appends text to an overflow file and returns a reference to the stored data.
/// Reads the header to find the write cursor, checks that the text fits within
/// `max_file_size`, writes the text bytes, and updates the header's `used_space`.
///
/// # Arguments
/// * `filename` - Path to the .overflow file (must already exist)
/// * `file_index` - Index of this overflow file (stored in the returned OverflowRef)
/// * `text` - The text to store
/// * `max_file_size` - Maximum file size in bytes (overflow_kbytes * 1024)
///
/// # Returns
/// An `OverflowRef` pointing to the stored text
///
/// # Errors
/// * `InvalidArgument` — if the text doesn't fit in this file
/// * `Io` — on file system failure
pub fn append_overflow_text(
    filename: &str,
    file_index: u32,
    text: &str,
    max_file_size: u64,
) -> Result<OverflowRef, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer = [0u8; OVERFLOW_HEADER_SIZE];

    let mut overflow_header = match file.read_exact(&mut buffer) {
        Ok(_) => OverflowHeader::from_bytes(&buffer)?,
        Err(error) => {
            println!("Error reading overflow header in {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    if overflow_header.get_used_space() + text.len() as u64 > max_file_size {
        return Err(DatabaseError::InvalidArgument(
            "Text is too long to insert in the file".to_string(),
        ));
    }

    let overflow_ref = OverflowRef::new(
        file_index,
        overflow_header.get_used_space(),
        text.len() as u32,
    );
    overflow_header.set_used_space(overflow_header.get_used_space() + text.len() as u64);

    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error creating overflow file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let _ = match file.seek(SeekFrom::Start(overflow_ref.get_offset())) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    match file.write_all(text.as_bytes()) {
        Ok(_) => (),
        Err(error) => {
            println!("Error writing overflow reference to the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let _ = match file.seek(SeekFrom::Start(0)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    match file.write_all(&overflow_header.to_bytes()) {
        Ok(_) => (),
        Err(error) => {
            println!("Error writing overflow reference to the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    Ok(overflow_ref)
}

/// Adds `length` bytes to the overflow file's `fragmented_space` header field.
/// Called when a record referencing overflow text is deleted or updated,
/// so the old text bytes are tracked as reclaimable space.
///
/// # Arguments
/// * `filename` - Path to the .overflow file
/// * `length` - Number of bytes to add to fragmented_space
///
/// # Errors
/// * `Io` — on file system failure
pub fn add_fragmented_space(filename: &str, length: u32) -> Result<(), DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening overflow file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer = [0u8; OVERFLOW_HEADER_SIZE];
    let mut header = match file.read_exact(&mut buffer) {
        Ok(_) => OverflowHeader::from_bytes(&buffer)?,
        Err(error) => {
            println!("Error reading overflow header in {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    header.set_fragmented_space(header.get_fragmented_space() + length as u64);

    let mut file = match OpenOptions::new()
        .write(true)
        .truncate(false)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening overflow file for write {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    file.write_all(&header.to_bytes())?;
    Ok(())
}

/// Rewrites an overflow file with only the live entries, eliminating fragmented space.
/// Reads each live entry from the old file, writes them contiguously into a new file,
/// and returns a map of old_offset → new OverflowRef so callers can update records.
///
/// # Arguments
/// * `filename` - Path to the .overflow file to compact
/// * `file_index` - Index of this overflow file (stored in returned OverflowRefs)
/// * `entries` - Vec of (old_offset, length) for each live entry to preserve
///
/// # Returns
/// A `HashMap<u64, OverflowRef>` mapping each old offset to its new OverflowRef
///
/// # Errors
/// * `Io` — on file system failure
pub fn rewrite_overflow_file(
    filename: &str,
    file_index: u32,
    mut entries: Vec<(u64, u32)>,
) -> Result<HashMap<u64, OverflowRef>, DatabaseError> {
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    let mut res_map: HashMap<u64, OverflowRef> = HashMap::new();

    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffers: Vec<Vec<u8>> = vec![];

    for entry in &entries {
        let mut buf: Vec<u8> = vec![0u8; entry.1 as usize];

        let _ = match file.seek(SeekFrom::Start(entry.0)) {
            Ok(pos) => pos,
            Err(error) => {
                println!("Error seeking in the file {filename}: {error}");
                return Err(DatabaseError::Io(error));
            }
        };

        match file.read_exact(&mut buf) {
            Ok(_) => (),
            Err(error) => {
                println!("Error overflow reference in {filename}: {error}");
                return Err(DatabaseError::Io(error));
            }
        };

        buffers.push(buf);
    }

    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error creating overflow file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let header = OverflowHeader::new(
        OVERFLOW_HEADER_SIZE as u64
            + buffers
                .iter()
                .map(|b| b.len())
                .sum::<usize>() as u64,
        0,
    );

    file.write_all(&header.to_bytes())?;

    let mut offset = OVERFLOW_HEADER_SIZE;

    for (buffer, entry) in buffers.iter().zip(entries.iter()) {
        file.write_all(buffer)?;
        let new_ref = OverflowRef::new(file_index, offset as u64, buffer.len() as u32);
        res_map.insert(entry.0, new_ref);
        offset += buffer.len();
    }

    Ok(res_map)
}
