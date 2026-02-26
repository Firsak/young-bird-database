use std::error::Error;
use std::fs::OpenOptions;
use std::io::Read;

use super::table_header::TableHeader;
use crate::database_operations::file_processing::traits::BinarySerde;

/// Reads a TableHeader from a .meta file.
pub fn read_table_header(filename: &str) -> Result<TableHeader, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening meta file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer)?;

    Ok(TableHeader::from_bytes(&buffer)?)
}
