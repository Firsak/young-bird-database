use std::fs::OpenOptions;
use std::io::Read;

use super::hash_index::HashIndex;
use super::index_entry::IndexEntry;
use super::index_header::IndexHeader;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{INDEX_ENTRY_SIZE, INDEX_HEADER_SIZE};

/// Reads a HashIndex from an .idx file.
pub fn read_index(filename: &str) -> Result<HashIndex, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening index file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer)?;

    let index_header = IndexHeader::from_bytes(&buffer[0..INDEX_HEADER_SIZE])?;
    let mut buckets: Vec<IndexEntry> = Vec::with_capacity(index_header.get_bucket_count() as usize);

    for bucket_pos in 0..index_header.get_bucket_count() {
        let bucket_bytes_pos = INDEX_HEADER_SIZE + INDEX_ENTRY_SIZE * bucket_pos as usize;
        buckets.push(IndexEntry::from_bytes(
            &buffer[bucket_bytes_pos..bucket_bytes_pos + INDEX_ENTRY_SIZE],
        )?);
    }

    Ok(HashIndex::from_parts(index_header, buckets))
}
