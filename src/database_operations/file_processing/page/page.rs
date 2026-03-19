use super::header::PageHeader;
use super::offsets;
use super::record::{PageRecordContent, PageRecordMetadata};
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::PAGE_RECORD_METADATA_SIZE;

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

    /// Adds a new record to this page in memory. Calculates the content offset,
    /// creates the metadata slot, appends both, and updates the header.
    ///
    /// # Arguments
    /// * `page_size` - Total page size in bytes (e.g., 8192)
    /// * `record_id` - Unique identifier for the new record
    /// * `record_content` - The column values to store
    ///
    /// # Returns
    /// The assigned slot_index (0-based position in the metadata region).
    ///
    /// # Errors
    /// * `PageFull` - Not enough free space for metadata + content
    pub fn add_record(
        &mut self,
        page_size: usize,
        record_id: u64,
        record_content: PageRecordContent,
    ) -> Result<u16, DatabaseError> {
        let record_content_bytes = record_content.to_bytes();
        let record_content_length = record_content_bytes.len();

        if record_content_length + PAGE_RECORD_METADATA_SIZE > self.header.get_free_space() as usize
        {
            return Err(DatabaseError::PageFull);
        };

        let next_record_index = self.header.get_records_count();

        let last_record = self.records.last();

        let content_pos = offsets::page_record_content_offset_relative_page_end(
            page_size,
            last_record,
            record_content_length,
        );

        let record_metadata = PageRecordMetadata::new(
            record_id,
            content_pos as u32,
            record_content_length as u32,
            false,
        );

        self.append_record(record_metadata, record_content);

        self.header.update_records_count(self.records.len() as u16);
        self.header.update_free_space(
            self.header.get_free_space()
                - PAGE_RECORD_METADATA_SIZE as u32
                - record_content_length as u32,
        );

        Ok(next_record_index)
    }

    /// Deletes a record by slot index **in memory only** (no disk I/O).
    /// Last record is hard-deleted (slot reclaimed, free_space increases).
    /// Non-last records are soft-deleted (marked deleted, fragmented_space increases).
    ///
    /// # Arguments
    /// * `slot_index` - Zero-indexed metadata position of the record to delete
    ///
    /// # Errors
    /// * `RecordNotFound` - slot_index is out of bounds or record is already deleted
    pub fn delete_record(&mut self, slot_index: u16) -> Result<(), DatabaseError> {
        if (slot_index as usize) >= self.records.len() {
            return Err(DatabaseError::RecordNotFound(slot_index as u64));
        }

        let records_length = self.records.len();

        let selected_record = self.records.get_mut(slot_index as usize).unwrap();
        if selected_record.get_is_deleted() {
            return Err(DatabaseError::RecordNotFound(slot_index as u64));
        }
        let content_size = selected_record.get_content_size();

        let is_hard_delete = slot_index as usize == records_length - 1;

        if is_hard_delete {
            self.records.remove(slot_index as usize);
            self.records_content
                .remove(records_length - 1 - slot_index as usize);
            self.header
                .update_records_count(self.header.get_records_count() - 1);
            self.header.update_free_space(
                self.header.get_free_space() + content_size + PAGE_RECORD_METADATA_SIZE as u32,
            );
        } else {
            selected_record.set_is_deleted(true);
            self.header
                .update_deleted_records_count(self.header.get_deleted_records_count() + 1);
            self.header.update_fragmented_space(
                self.header.get_fragment_space() + selected_record.get_content_size(),
            );
        }

        Ok(())
    }

    /// Updates a record's content by slot index **in memory only** (no disk I/O).
    /// If the new content fits in the old slot, it's updated in place. If larger,
    /// the old space becomes fragmented and new content is placed at the end of
    /// the content region.
    ///
    /// # Arguments
    /// * `slot_index` - Zero-indexed metadata position of the record to update
    /// * `record_content` - The new column values to store
    ///
    /// # Errors
    /// * `RecordNotFound` - slot_index is out of bounds or record is deleted
    /// * `PageFull` - New content is larger and the page lacks free space (caller should relocate)
    pub fn update_record(
        &mut self,
        slot_index: u16,
        record_content: PageRecordContent,
    ) -> Result<(), DatabaseError> {
        if (slot_index as usize) >= self.records.len() {
            return Err(DatabaseError::RecordNotFound(slot_index as u64));
        }

        let record = &self.records[slot_index as usize];
        if record.get_is_deleted() {
            return Err(DatabaseError::RecordNotFound(record.get_id()));
        }

        let old_content_size = record.get_content_size() as usize;
        let new_content_bytes = record_content.to_bytes();
        let new_content_size = new_content_bytes.len();
        let content_index = self.records_content.len() - 1 - slot_index as usize;
        let is_last = slot_index as usize == self.records.len() - 1;

        if old_content_size >= new_content_size {
            // In-place update: new content fits in old slot
            self.records_content[content_index] = record_content;

            let size_diff = (old_content_size - new_content_size) as u32;
            if size_diff > 0 {
                self.records[slot_index as usize]
                    .set_content_size(new_content_size as u32);
                if is_last {
                    self.header
                        .update_free_space(self.header.get_free_space() + size_diff);
                } else {
                    self.header
                        .update_fragmented_space(self.header.get_fragment_space() + size_diff);
                }
            }
        } else {
            // Content grew — need new space from free region
            if new_content_size > self.header.get_free_space() as usize {
                return Err(DatabaseError::PageFull);
            }

            // Old content space becomes fragmented
            self.header.update_fragmented_space(
                self.header.get_fragment_space() + old_content_size as u32,
            );
            self.header.update_free_space(
                self.header.get_free_space() - new_content_size as u32,
            );

            // Calculate new content offset (after the last record's content)
            let last_metadata = &self.records[self.records.len() - 1];
            let new_content_offset =
                last_metadata.get_content_offset() as u64 - new_content_size as u64;

            self.records[slot_index as usize].set_content_offset(new_content_offset as u32);
            self.records[slot_index as usize].set_content_size(new_content_size as u32);
            self.records_content[content_index] = record_content;
        }

        Ok(())
    }

    /// Returns a mutable reference to a record's metadata at the given slot index.
    pub fn get_record_metadata_mut(&mut self, slot_index: usize) -> &mut PageRecordMetadata {
        &mut self.records[slot_index]
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
    pub fn get_record_content_by_slot_index(&self, slot_index: usize) -> &PageRecordContent {
        &self.records_content[&self.records_content.len() - 1 - slot_index]
    }
}
