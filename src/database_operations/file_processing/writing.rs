use std::error::Error;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};

use crate::database_operations::file_processing::table::{
    PageHeader, PageRecordContent, PageRecordMetadata,
};
use crate::database_operations::file_processing::traits::{BinarySerde, ReadWrite};
use crate::database_operations::file_processing::{
    self, HEADER_SIZE, KBYTES, PAGE_RECORD_METADATE_SIZE,
};

/// Overwrites the header of an existing page. The page must already exist in the file.
pub fn write_page_header(
    filename: &str,
    page_number: u64,
    page_header: PageHeader,
    page_kbytes: u32,
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

    let size: usize = page_kbytes as usize * KBYTES;

    page_header.write_to_file(&mut file, page_number * (size as u64), filename)
}

/// Creates a new empty page at the given page number.
/// Writes an initialized header and expands the file to fit the full page.
pub fn write_new_page(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
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

    let size: usize = page_kbytes as usize * KBYTES;
    let header = PageHeader::new(page_number, 0, 0, (size - HEADER_SIZE) as u32, 0);

    header.write_to_file(&mut file, page_number * (size as u64), filename)?;

    let pos = (size as u64) * (page_number + 1) - 1;
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

/// Appends a new record to a page. Writes metadata after existing metadata slots
/// and content at the end of the page growing backwards. Returns error if not enough free space.
pub fn add_new_record(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
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

    let page_size: usize = page_kbytes as usize * KBYTES;
    let page_pos = page_number * (page_size as u64);

    let mut page_header = PageHeader::read_from_file(
        &mut file,
        page_number * (page_size as u64),
        page_size,
        filename,
    )?;

    let next_record_metadata_index = page_header.get_records_count();
    let next_record_metadata_pos = page_pos
        + (HEADER_SIZE as u64)
        + (next_record_metadata_index as u64) * (PAGE_RECORD_METADATE_SIZE as u64);
    let record_content_bytes = record_content.to_bytes();
    let record_content_length = record_content_bytes.len();
    let record_content_pos = if next_record_metadata_index == 0 {
        (page_size as u64) - (record_content_length as u64)
    } else {
        let last_record_metadata_pos = page_pos
            + (HEADER_SIZE as u64)
            + ((page_header.get_records_count() - 1) as u64) * (PAGE_RECORD_METADATE_SIZE as u64);
        let last_record_metadata = PageRecordMetadata::read_from_file(
            &mut file,
            last_record_metadata_pos,
            PAGE_RECORD_METADATE_SIZE,
            filename,
        )?;
        (last_record_metadata.get_bytes_offset() as u64) - (record_content_length as u64)
    };

    if record_content_length + PAGE_RECORD_METADATE_SIZE > page_header.get_free_space() as usize {
        return Err("Not enough bytes to write in this page".into());
    };

    let record_metadata = PageRecordMetadata::new(
        record_id,
        record_content_pos as u32,
        record_content_length as u32,
        false,
    );

    record_metadata.write_to_file(&mut file, next_record_metadata_pos, filename)?;

    record_content.write_to_file(&mut file, page_pos + record_content_pos, filename)?;

    page_header.update_records_count(page_header.get_records_count() + 1);
    page_header.update_free_space(
        page_header.get_free_space() - (record_content_length + PAGE_RECORD_METADATE_SIZE) as u32,
    );

    page_header.write_to_file(&mut file, page_number * (page_size as u64), filename)?;

    Ok(())
}

/// Scans metadata slots sequentially to find a record by its ID.
/// Returns (metadata, slot_index) or (None, None) if not found.
fn find_record_metadata_by_id(
    file_ref: &mut std::fs::File,
    filename: &str,
    page_number: u64,
    page_size: usize,
    record_id: u64,
    page_header: &PageHeader,
) -> Result<(Option<PageRecordMetadata>, Option<u16>), Box<dyn Error>> {
    let mut found_record_metadata_index: Option<u16> = None;
    let mut found_record_metadata: Option<PageRecordMetadata> = None;

    for index in 0..page_header.get_records_count() {
        let record_metadata_pos = page_number * page_size as u64
            + HEADER_SIZE as u64
            + (index as u64 * PAGE_RECORD_METADATE_SIZE as u64);
        let record_metadata = PageRecordMetadata::read_from_file(
            file_ref,
            record_metadata_pos,
            PAGE_RECORD_METADATE_SIZE,
            filename,
        )?;
        if record_metadata.get_id() == record_id && !record_metadata.get_is_deleted() {
            found_record_metadata = Some(record_metadata);
            found_record_metadata_index = Some(index);
            break;
        }
    }

    Ok((found_record_metadata, found_record_metadata_index))
}

/// Deletes a record by ID. Last record is hard-deleted (slot reclaimed, free_space increases).
/// Non-last records are soft-deleted (marked deleted, fragmented_space increases).
pub fn delete_record(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
    record_id: u64,
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

    let page_size: usize = page_kbytes as usize * KBYTES;
    let page_header_pos: u64 = page_number * (page_size as u64);

    let mut page_header =
        PageHeader::read_from_file(&mut file, page_header_pos, page_size, filename)?;

    let res = find_record_metadata_by_id(
        &mut file,
        filename,
        page_number,
        page_size,
        record_id,
        &page_header,
    )?;

    let found_record_metadata = res.0;
    let found_record_metadata_index = res.1;

    if found_record_metadata.is_none() || found_record_metadata_index.is_none() {
        return Err(Box::from(
            format!(
                "No PageRecordMetadata found with provided record_id {}",
                record_id
            )
            .to_string(),
        ));
    }

    let found_record_metadata_index = found_record_metadata_index.unwrap();
    let mut found_record_metadata = found_record_metadata.unwrap();

    if found_record_metadata_index == (page_header.get_records_count() - 1) {
        page_header.update_records_count(page_header.get_records_count() - 1);
        page_header.update_free_space(
            page_header.get_free_space()
                + found_record_metadata.get_bytes_content() as u32
                + PAGE_RECORD_METADATE_SIZE as u32,
        );
        page_header.write_to_file(&mut file, page_header_pos, filename)
    } else {
        page_header.update_deleted_records_count(page_header.get_deleted_records_count() + 1);
        page_header.update_fragmented_space(
            page_header.get_fragment_space() + found_record_metadata.get_bytes_content() as u32,
        );

        found_record_metadata.set_is_deleted(true);
        let record_metadata_pos = page_number * page_size as u64
            + HEADER_SIZE as u64
            + (found_record_metadata_index as u64 * PAGE_RECORD_METADATE_SIZE as u64);
        found_record_metadata.write_to_file(&mut file, record_metadata_pos, filename)?;

        page_header.write_to_file(&mut file, page_header_pos, filename)
    }
}

/// Updates a record's content by ID. If the new content fits in the old slot, it's written
/// in place. If larger, the old space becomes fragmented and content is written at a new position.
/// Returns error if the page doesn't have enough free space for the larger content.
pub fn update_record(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
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

    let page_size: usize = page_kbytes as usize * KBYTES;
    let page_header_pos: u64 = page_number * (page_size as u64);

    let mut page_header =
        PageHeader::read_from_file(&mut file, page_header_pos, page_size, filename)?;

    let res = find_record_metadata_by_id(
        &mut file,
        filename,
        page_number,
        page_size,
        record_id,
        &page_header,
    )?;

    let found_record_metadata = res.0;
    let found_record_metadata_index = res.1;

    if found_record_metadata.is_none() || found_record_metadata_index.is_none() {
        return Err(Box::from(
            format!(
                "No PageRecordMetadata found with provided record_id {}",
                record_id
            )
            .to_string(),
        ));
    }

    let found_record_metadata_index = found_record_metadata_index.unwrap();
    let mut found_record_metadata = found_record_metadata.unwrap();

    let old_record_content_length = found_record_metadata.get_bytes_content();
    let new_record_content_length = record_content.to_bytes().len();
    let page_pos = page_number * (page_size as u64);

    if old_record_content_length as usize >= new_record_content_length {
        record_content.write_to_file(
            &mut file,
            page_pos + found_record_metadata.get_bytes_offset() as u64,
            filename,
        )?;
        let record_content_length_difference =
            found_record_metadata.get_bytes_content() as usize - new_record_content_length;
        if found_record_metadata_index == (page_header.get_records_count() - 1) {
            page_header.update_free_space(
                page_header.get_free_space() + record_content_length_difference as u32,
            );
        } else {
            page_header.update_fragmented_space(
                page_header.get_fragment_space() + record_content_length_difference as u32,
            );
        }

        let record_metadata_pos = page_number * page_size as u64
            + HEADER_SIZE as u64
            + (found_record_metadata_index as u64 * PAGE_RECORD_METADATE_SIZE as u64);
        found_record_metadata.write_to_file(&mut file, record_metadata_pos, filename)?;

        page_header.write_to_file(&mut file, page_header_pos, filename)
    } else {
        if new_record_content_length > page_header.get_free_space() as usize {
            // Need to create logic to delete record here and create on another page
            return Err(Box::from(
                format!(
                    "Not enough space in page {} to update record {}",
                    page_number, record_id
                )
                .to_string(),
            ));
        }

        page_header.update_fragmented_space(
            page_header.get_fragment_space() + found_record_metadata.get_bytes_content() as u32,
        );
        page_header
            .update_free_space(page_header.get_free_space() - new_record_content_length as u32);
        page_header.write_to_file(&mut file, page_header_pos, filename)?;

        let last_record_metadata_pos = page_pos
            + (HEADER_SIZE as u64)
            + ((page_header.get_records_count() - 1) as u64) * (PAGE_RECORD_METADATE_SIZE as u64);
        let last_record_metadata = PageRecordMetadata::read_from_file(
            &mut file,
            last_record_metadata_pos,
            PAGE_RECORD_METADATE_SIZE,
            filename,
        )?;
        let new_record_content_pos =
            (last_record_metadata.get_bytes_offset() as u64) - (new_record_content_length as u64);

        found_record_metadata.set_bytes_content(new_record_content_length as u32);
        found_record_metadata.set_bytes_offset(new_record_content_pos as u32);

        record_content.write_to_file(&mut file, page_pos + new_record_content_pos, filename)?;

        let record_metadata_pos = page_number * page_size as u64
            + HEADER_SIZE as u64
            + (found_record_metadata_index as u64 * PAGE_RECORD_METADATE_SIZE as u64);

        found_record_metadata.write_to_file(&mut file, record_metadata_pos, filename)
    }
}

/// Compacts a page by rewriting all non-deleted records contiguously,
/// eliminating fragmented space. No-op if fragmented_space is already 0.
pub fn compact_page(
    filename: &str,
    page_number: u64,
    page_kbytes: u32,
) -> Result<(), Box<dyn Error>> {
    let mut file = match OpenOptions::new()
        .read(true)
        .write(true)
        .truncate(false)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening the file {filename}: {error}");
            return Err(Box::new(error));
        }
    };

    let page_size: usize = page_kbytes as usize * KBYTES;
    let page_pos = page_number * (page_size as u64);

    let page_header = PageHeader::read_from_file(&mut file, page_pos, page_size, filename)?;

    if page_header.get_fragment_space() == 0 {
        return Ok(());
    }

    let mut new_header = PageHeader::new(
        page_header.page_id,
        0,
        0,
        (page_size - HEADER_SIZE) as u32,
        0,
    );
    let header_offset = file_processing::table_offsets::page_header_offset(page_number, page_size);

    if page_header.get_records_count() == 0 {
        new_header.write_to_file(&mut file, header_offset, filename)?;
        return Ok(());
    }

    let mut new_records_metadata: Vec<PageRecordMetadata> = vec![];
    let mut new_records_content: Vec<PageRecordContent> = vec![];

    for index in 0..page_header.get_records_count() {
        let record_metadata_offset = file_processing::table_offsets::page_record_metadata_offset(
            page_number,
            page_size,
            index,
        );
        let tmp_record_metadata = PageRecordMetadata::read_from_file(
            &mut file,
            record_metadata_offset,
            PAGE_RECORD_METADATE_SIZE,
            filename,
        )?;

        if !tmp_record_metadata.get_is_deleted() {
            let absolute_file_start_offset =
                file_processing::table_offsets::page_record_content_offset_absolute_file(
                    page_number,
                    page_size,
                    tmp_record_metadata.get_bytes_offset() as u64,
                );
            let size = tmp_record_metadata.get_bytes_content() as usize;
            let tmp_record_content = PageRecordContent::read_from_file(
                &mut file,
                absolute_file_start_offset,
                size,
                filename,
            )?;

            new_records_content.push(tmp_record_content);

            let last_record = {
                if new_records_metadata.is_empty() {
                    None
                } else {
                    Some(&new_records_metadata[new_records_metadata.len() - 1])
                }
            };
            let new_record_offset =
                file_processing::table_offsets::page_record_content_offset_relative_page_end(
                    page_size,
                    last_record,
                    tmp_record_metadata.get_bytes_content() as usize,
                );
            let new_metadata = PageRecordMetadata::new(
                tmp_record_metadata.get_id(),
                new_record_offset as u32,
                tmp_record_metadata.get_bytes_content(),
                false,
            );

            new_records_metadata.push(new_metadata);
        }
    }

    for index in 0..new_records_metadata.len() {
        let metadata_offset = file_processing::table_offsets::page_record_metadata_offset(
            page_number,
            page_size,
            index as u16,
        );
        new_records_metadata[index].write_to_file(&mut file, metadata_offset, filename)?;
        let content_offset =
            file_processing::table_offsets::page_record_content_offset_absolute_file(
                page_number,
                page_size,
                new_records_metadata[index].get_bytes_offset() as u64,
            );
        new_records_content[index].write_to_file(&mut file, content_offset, filename)?;
        new_header.update_records_count(new_header.get_records_count() + 1);
        new_header.update_free_space(
            new_header.get_free_space()
                - PAGE_RECORD_METADATE_SIZE as u32
                - new_records_metadata[index].get_bytes_content(),
        );
    }
    new_header.write_to_file(&mut file, header_offset, filename)?;

    Ok(())
}
