use super::header::PageHeader;
use super::record::{PageRecordContent, PageRecordMetadata};

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
    /// so records[i] pairs with records_content[len - 1 - i].
    pub fn get_record_content_by_slot_index(
        &self,
        slot_index: usize,
    ) -> &PageRecordContent {
        &self.records_content[&self.records_content.len() - 1 - slot_index]
    }
}
