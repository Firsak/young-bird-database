use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::database_operations::file_processing::table::{
    PageHeader, PageRecord, PageRecordContent,
};
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES, PAGE_RECORD_SIZE};

pub fn write_page_header(
    filename: &str,
    page_number: u64,
    page_header: PageHeader,
    page_kbytes: usize,
) -> Result<(), Box<dyn Error>> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(filename)
    {
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

    match file.write_all(&page_header.to_bytes()) {
        Ok(_) => Ok(()),
        Err(error) => {
            println!("Error writing to the file {filename}: {error}");
            Err(Box::new(error))
        }
    }
}

pub fn write_new_page(
    filename: &str,
    page_number: u64,
    page_kbytes: usize,
) -> Result<(), Box<dyn Error>> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes * KBYTES;
    let header = PageHeader::new(page_number, 0, (size - HEADER_SIZE) as u16);

    let _ = match file.seek(SeekFrom::Start(page_number * (size as u64))) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    match file.write_all(&header.to_bytes()) {
        Ok(_) => (),
        Err(error) => {
            println!("Error writing to the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let pos = ((page_kbytes * KBYTES) as u64) * (page_number + 1) - 1;
    let _ = match file.seek(SeekFrom::Start(pos)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error expanding the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    match file.write_all(&[0u8]) {
        Ok(_) => Ok(()),
        Err(error) => {
            println!("Error writing ending byte to the file {filename}: {error}");
            Err(Box::new(error))
        }
    }
}

pub fn add_new_record(
    filename: &str,
    page_number: u64,
    page_kbytes: usize,
    record_id: u64,
    record_content: PageRecordContent,
) -> Result<(), Box<dyn Error>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let page_size: usize = page_kbytes * KBYTES;
    let page_pos = page_number * (page_size as u64);

    let _ = match file.seek(SeekFrom::Start(page_pos)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut buffer: Vec<u8> = vec![0u8; page_size];
    let page_header = match file.read_exact(&mut buffer) {
        Ok(_) => PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?,
        Err(error) => {
            println!("Error reading page header {page_number} in {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let next_record = page_header.get_records_count();
    let next_record_pos =
        page_pos + (HEADER_SIZE as u64) + (next_record as u64) * (PAGE_RECORD_SIZE as u64);
    let content_bytes = record_content.to_bytes();
    let content_length = content_bytes.len();
    let content_pos = if next_record_pos == 0 {
        (page_size as u64) - 1 - (content_length as u64)
    } else {
        panic!("Adding record > 0 is not implemented")
    };

    if content_length + PAGE_RECORD_SIZE > page_header.get_free_space() as usize {
        panic!("The behaviour when there is not enough bytes is not implemented");
    };

    let record = PageRecord::new(record_id, content_pos as u32, content_length as u32);
    let record_bytes = record.to_bytes();

    let _ = match file.seek(SeekFrom::Start(next_record_pos)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking for record position in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    match file.write_all(&record_bytes) {
        Ok(_) => (),
        Err(error) => {
            println!("Error writing record to the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let _ = match file.seek(SeekFrom::Start(page_pos + content_pos)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking for record content position in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    match file.write_all(&record_bytes) {
        Ok(_) => Ok(()),
        Err(error) => {
            println!("Error writing record content to the file {filename}: {error}");
            Err(Box::new(error))
        }
    }
}
