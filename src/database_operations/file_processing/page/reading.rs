use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use super::header::PageHeader;
use super::offsets;
use super::page::Page;
use super::record::{PageRecordContent, PageRecordMetadata};
use crate::database_operations::file_processing::traits::{BinarySerde, ReadWrite};
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES, PAGE_RECORD_METADATE_SIZE};

/// Reads only the page header at the given page number.
pub fn read_page_header(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
) -> Result<PageHeader, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes as usize * KBYTES;

    PageHeader::read_from_file(&mut file, page_number * (size as u64), size, filename)
}

/// Reads a full page: header, all record metadata, and all record content.
pub fn read_page(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
) -> Result<Page, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let page_size: usize = page_kbytes as usize * KBYTES;
    let page_offset = offsets::page_start_offset(page_number, page_size);
    let _ = match file.seek(SeekFrom::Start(page_offset)) {
        Ok(pos) => pos,
        Err(error) => {
            println!("Error seeking in the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let mut page_buffer: Vec<u8> = vec![0u8; page_size];

    match file.read_exact(&mut page_buffer) {
        Ok(_) => {}
        Err(error) => {
            println!("Error reading page {page_number} in {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let header = PageHeader::from_bytes(&(page_buffer[0..HEADER_SIZE]))?;

    let mut page = Page::new(
        header,
        Vec::new() as Vec<PageRecordMetadata>,
        Vec::new() as Vec<PageRecordContent>,
    );

    for index in 0..page.header.get_records_count() {
        let record_metadata_buffer_pos = HEADER_SIZE + index as usize * PAGE_RECORD_METADATE_SIZE;
        let record_metadata = PageRecordMetadata::from_bytes(
            &page_buffer[record_metadata_buffer_pos
                ..record_metadata_buffer_pos + PAGE_RECORD_METADATE_SIZE],
        )?;

        if record_metadata.get_is_deleted() {
            continue;
        }

        let record_content_buffer_pos =
            record_metadata.get_bytes_offset() as usize;
        let record_content_size = record_metadata.get_bytes_content() as usize;
        let record_content = PageRecordContent::from_bytes(&page_buffer[record_content_buffer_pos..record_content_buffer_pos + record_content_size])?;

        page.append_record(
            record_metadata,
            record_content
        );
    }

    Ok(page)
}

/// Reads a single record's metadata by its slot index within the page.
pub fn read_record_metadata(
    filename: &str,
    page_number: u64,
    record_metadata_index: u64,
    page_kbytes: u32,
) -> Result<PageRecordMetadata, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let size: usize = page_kbytes as usize * KBYTES;
    let record_metadata_pos: u64 = page_number * (size as u64)
        + (HEADER_SIZE as u64)
        + (PAGE_RECORD_METADATE_SIZE as u64 * record_metadata_index);

    PageRecordMetadata::read_from_file(
        &mut file,
        record_metadata_pos,
        PAGE_RECORD_METADATE_SIZE,
        filename,
    )
}

/// Reads a record's content using its metadata (which provides offset and size).
pub fn read_record_content(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
    record_metadata: &PageRecordMetadata,
) -> Result<PageRecordContent, Box<dyn Error>> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let page_size: usize = page_kbytes as usize * KBYTES;

    let absolute_file_start_offset = offsets::page_record_content_offset_absolute_file(
        page_number,
        page_size,
        record_metadata.get_bytes_offset() as u64,
    );
    let size = record_metadata.get_bytes_content() as usize;
    PageRecordContent::read_from_file(&mut file, absolute_file_start_offset, size, filename)
}
