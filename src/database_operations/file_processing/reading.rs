use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::database_operations::file_processing::table::{
    Page, PageHeader, PageRecordMetadata, PageRecordContent,
};
use crate::database_operations::file_processing::traits::{BinarySerde, ReadWrite};
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES, PAGE_RECORD_METADATE_SIZE};

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

    PageHeader::read_from_file(&mut file, page_number * (size as u64), size, filename)
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

    let header = match file.read_exact(&mut buffer) {
        Ok(_) => PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?,
        Err(error) => {
            println!("Error reading page {page_number} in {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut page = Page::new(
        header,
        Vec::new() as Vec<PageRecordMetadata>,
        Vec::new() as Vec<PageRecordContent>,
    );

    for pos in 0..page.header.get_records_count() {
        let record_metadata_pos = page_number * size as u64 + HEADER_SIZE as u64 + (pos as u64 * PAGE_RECORD_METADATE_SIZE as u64);
        let record_metadata = PageRecordMetadata::read_from_file(&mut file, record_metadata_pos, PAGE_RECORD_METADATE_SIZE, filename)?;
        let record_content_pos = page_number * size as u64 + record_metadata.get_bytes_offset() as u64;
        let record_content_size = record_metadata.get_bytes_content();
        page.append_record(
            record_metadata,
            PageRecordContent::read_from_file(
                &mut file,
                record_content_pos,
                record_content_size as usize,
                filename,
            )?,
        );
    }

    Ok(page)
}

pub fn read_record_metadata(
    filename: &str,
    page_number: u64,
    record_metadata_index: u64,
    page_kbytes: usize,
) -> Result<PageRecordMetadata, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes * KBYTES;
    let record_metadata_pos: u64 = page_number * (size as u64)
        + (HEADER_SIZE as u64)
        + (PAGE_RECORD_METADATE_SIZE as u64 * record_metadata_index);

    PageRecordMetadata::read_from_file(&mut file, record_metadata_pos, PAGE_RECORD_METADATE_SIZE, filename)
}
