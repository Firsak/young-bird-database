use super::header::PageHeader;
use super::record::{PageRecordContent, PageRecordMetadata};

/// In-memory representation of a full page: header + metadata slots + content.
/// Used by read_page/write_page for buffer-based I/O.
#[derive(Debug)]
pub struct Page {
    pub header: PageHeader,
    records: Vec<PageRecordMetadata>,
    records_content: Vec<PageRecordContent>,
}

impl Page {
    pub fn new(
        header: PageHeader,
        records: Vec<PageRecordMetadata>,
        records_content: Vec<PageRecordContent>,
    ) -> Self {
        Self {
            header,
            records,
            records_content,
        }
    }

    /// Appends a record to this page. Metadata is pushed to the end;
    /// content is inserted at position 0 (reverse order mirrors the on-disk layout
    /// where content grows backward from the page end).
    ///
    /// # Arguments
    /// * `record` - The 20-byte metadata slot to add
    /// * `record_content` - The variable-size column values to add
    pub fn append_record(&mut self, record: PageRecordMetadata, record_content: PageRecordContent) {
        self.records.extend([record]);
        self.records_content.insert(0, record_content);
    }

    pub fn get_records_metadata(&self) -> &Vec<PageRecordMetadata> {
        &self.records
    }

    pub fn get_records_content(&self) -> &Vec<PageRecordContent> {
        &self.records_content
    }

    /// Returns the content paired with the metadata at the given slot index.
    /// Content is stored in reverse order internally (insert(0, ...)),
    /// so `records[i]` pairs with `records_content[len - 1 - i]`.
    ///
    /// # Arguments
    /// * `slot_index` - Zero-indexed metadata position (same index used in `get_records_metadata()`)
    pub fn get_record_content_by_slot_index(
        &self,
        slot_index: usize,
    ) -> &PageRecordContent {
        &self.records_content[&self.records_content.len() - 1 - slot_index]
    }
}
