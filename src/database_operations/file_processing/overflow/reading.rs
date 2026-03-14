use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use super::overflow_header::OverflowHeader;
use super::overflow_ref::OverflowRef;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::OVERFLOW_HEADER_SIZE;

/// Reads the overflow header from an overflow file.
///
/// # Arguments
/// * `filename` - Path to the .overflow file
pub fn read_overflow_header(filename: &str) -> Result<OverflowHeader, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer: Vec<u8> = vec![0u8; OVERFLOW_HEADER_SIZE];

    match file.read_exact(&mut buffer) {
        Ok(_) => Ok(OverflowHeader::from_bytes(
            &(buffer[0..OVERFLOW_HEADER_SIZE]),
        )?),
        Err(error) => {
            println!("Error overflow header in {filename}: {error}");
            Err(DatabaseError::Io(error))
        }
    }
}

/// Reads text from an overflow file using an OverflowRef.
///
/// # Arguments
/// * `filename` - Path to the .overflow file
/// * `overflow_ref` - Reference containing offset and length of the stored text
///
/// # Errors
/// * `Io` — on file system failure
/// * `Serialization` — if the stored bytes are not valid UTF-8
pub fn read_overflow_text(
    filename: &str,
    overflow_ref: &OverflowRef,
) -> Result<String, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer: Vec<u8> = vec![0u8; overflow_ref.get_length() as usize];

    let _ = match file.seek(SeekFrom::Start(overflow_ref.get_offset())) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    match file.read_exact(&mut buffer) {
        Ok(_) => Ok(
            String::from_utf8(buffer[0..overflow_ref.get_length() as usize].to_vec()).map_err(
                |error| {
                    DatabaseError::Serialization(format!(
                        "Overflowed string deserialization failed: {}",
                        error
                    ))
                },
            )?,
        ),
        Err(error) => {
            println!("Error overflow reference in {filename}: {error}");
            Err(DatabaseError::Io(error))
        }
    }
}
