use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::database_operations::file_processing::table::{
    Page, PageHeader, PageRecord, PageRecordContent,
};
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES};

pub fn read_page_header(
    filename: &str,
    page_number: u64,
    page_kbytes: usize,
) -> Result<PageHeader, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes * KBYTES;
    let _ = match file.seek(SeekFrom::Start(page_number * (size as u64))) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut buffer: Vec<u8> = vec![0u8; size];
    match file.read_exact(&mut buffer) {
        Ok(_) => Ok(PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?),
        Err(error) => {
            println!("Error reading page header {page_number} in {filename}: {error}");
            Err(Box::new(error))
        }
    }
}

pub fn read_page(
    filename: &str,
    page_number: u64,
    page_kbytes: usize,
) -> Result<Page, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes * KBYTES;
    let _ = match file.seek(SeekFrom::Start(page_number * (size as u64))) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut buffer: Vec<u8> = vec![0u8; size];
    match file.read_exact(&mut buffer) {
        Ok(_) => Ok(Page::new(
            PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?,
            Vec::new() as Vec<PageRecord>,
            Vec::new() as Vec<PageRecordContent>,
        )),
        Err(error) => {
            println!("Error reading page {page_number} in {filename}: {error}");
            Err(Box::new(error))
        }
    }
}
