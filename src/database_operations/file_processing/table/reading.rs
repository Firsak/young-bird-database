use std::fs::OpenOptions;
use std::io::Read;

use super::table_header::TableHeader;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;

/// Reads a TableHeader from a .meta file.
pub fn read_table_header(filename: &str) -> Result<TableHeader, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening meta file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer)?;

    Ok(TableHeader::from_bytes(&buffer)?)
}
