use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;

use super::table_header::TableHeader;
use crate::database_operations::file_processing::traits::BinarySerde;

/// Writes a TableHeader to a .meta file. Creates or overwrites the file.
pub fn write_table_header(filename: &str, table_header: &TableHeader) -> Result<(), Box<dyn Error>> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let buffer: Vec<u8> = table_header.to_bytes();

    match file.write_all(&buffer) {
        Ok(_) => Ok(()),
        Err(error) => {
            println!("Error writing page to the file {filename}: {error}");
            Err(Box::new(error))
        }
    }
}
