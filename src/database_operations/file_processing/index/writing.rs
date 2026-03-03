use std::fs::OpenOptions;
use std::io::Write;

use super::hash_index::HashIndex;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{INDEX_HEADER_SIZE, INDEX_ENTRY_SIZE};

/// Writes a HashIndex to an .idx file. Creates or overwrites the file.
pub fn write_index(filename: &str, index: &HashIndex) -> Result<(), DatabaseError> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the index file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let bytes_length = INDEX_HEADER_SIZE + INDEX_ENTRY_SIZE * index.get_header().get_bucket_count() as usize; 
    let mut bytes: Vec<u8> = Vec::with_capacity(bytes_length);
    bytes.extend_from_slice(&index.get_header().to_bytes());
    for entry in index.get_buckets().iter() {
       bytes.extend_from_slice(&entry.to_bytes()); 
    }

    file.write_all(&bytes)?;

    Ok(())
}
