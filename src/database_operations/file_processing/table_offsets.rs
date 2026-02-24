//! Offset calculation helpers for page-based database file operations.
//!
//! This module provides functions to calculate byte offsets for various
//! components within the database file structure:
//!
//! ```text
//! Page Layout (grows in two directions):
//! ┌─────────────────────────────────────┐  ← page_start_offset()
//! │ PageHeader (20 bytes)               │
//! ├─────────────────────────────────────┤  ← page_record_metadata_offset()
//! │ PageRecord 0 metadata (20 bytes)    │
//! ├─────────────────────────────────────┤
//! │ PageRecord 1 metadata (20 bytes)    │
//! ├─────────────────────────────────────┤
//! │ ... more record metadata ...        │  ↓ metadata grows downward
//! ├─────────────────────────────────────┤
//! │                                     │
//! │         Free Space                  │
//! │                                     │
//! ├─────────────────────────────────────┤  ↑ content grows upward
//! │ Record 1 content (variable)         │
//! ├─────────────────────────────────────┤
//! │ Record 0 content (variable)         │  ← content stored from page end
//! └─────────────────────────────────────┘  ← page end
//! ```

use super::table::PageRecordMetadata;
use super::{HEADER_SIZE, PAGE_RECORD_METADATE_SIZE};

/// Calculates the absolute byte offset from file start where a page begins.
///
/// # Arguments
/// * `page_number` - Zero-indexed page number
/// * `page_size_bytes` - Total size of one page in bytes (e.g., 8192 for 8KB pages)
#[inline]
pub fn page_start_offset(page_number: u64, page_size_bytes: usize) -> u64 {
    page_number * (page_size_bytes as u64)
}

/// Calculates the absolute byte offset for the page header.
/// The header is always at the start of the page.
///
/// # Arguments
/// * `page_number` - Zero-indexed page number
/// * `page_size_bytes` - Total size of one page in bytes
#[inline]
pub fn page_header_offset(page_number: u64, page_size_bytes: usize) -> u64 {
    page_start_offset(page_number, page_size_bytes)
}

/// Calculates the absolute byte offset for a record's metadata slot.
///
/// # Arguments
/// * `page_number` - Zero-indexed page number
/// * `page_size_bytes` - Total size of one page in bytes
/// * `record_index` - Zero-indexed position of the record within the page
#[inline]
pub fn page_record_metadata_offset(
    page_number: u64,
    page_size_bytes: usize,
    record_index: u16,
) -> u64 {
    page_start_offset(page_number, page_size_bytes)
        + (HEADER_SIZE as u64)
        + (record_index as u64) * (PAGE_RECORD_METADATE_SIZE as u64)
}

/// Calculates the page-relative offset where record content should be placed.
/// Content is stored from the end of the page, growing backwards.
///
/// # Arguments
/// * `page_size_bytes` - Total size of one page in bytes
/// * `last_record` - Reference to the last record in the page, or None if page is empty
/// * `content_length` - Size in bytes of the new content to be written
///
/// # Returns
/// Offset relative to the page start where the content should begin.
#[inline]
pub fn page_record_content_offset_relative_page_end(
    page_size_bytes: usize,
    last_record: Option<&PageRecordMetadata>,
    content_length: usize,
) -> u64 {
    match last_record {
        None => (page_size_bytes - content_length) as u64,
        Some(record) => (record.get_bytes_offset() as u64) - (content_length as u64),
    }
}

/// Calculates the absolute byte offset for record content in the file.
///
/// # Arguments
/// * `page_number` - Zero-indexed page number
/// * `page_size_bytes` - Total size of one page in bytes
/// * `content_offset_relative_page` - The page-relative offset where content starts
#[inline]
pub fn page_record_content_offset_absolute_file(
    page_number: u64,
    page_size_bytes: usize,
    content_offset_relative_page: u64,
) -> u64 {
    page_start_offset(page_number, page_size_bytes) + content_offset_relative_page
}

/// Calculates the byte position of the last byte in a page (for file expansion).
///
/// # Arguments
/// * `page_number` - Zero-indexed page number
/// * `page_size_bytes` - Total size of one page in bytes
///
/// # Returns
/// The position of the last byte in the page (page_end - 1).
#[inline]
pub fn page_last_byte_offset(page_number: u64, page_size_bytes: usize) -> u64 {
    page_start_offset(page_number + 1, page_size_bytes) - 1
}
