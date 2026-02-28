use std::error::Error;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::database_operations::file_processing::traits::{BinarySerde, ReadWrite};
use crate::database_operations::file_processing::types::ContentTypes;
use crate::database_operations::file_processing::PAGE_RECORD_METADATA_SIZE;

#[derive(Debug)]
pub struct PageRecordMetadata {
    // 20 bytes: [id: 8][content_offset: 4][content_size: 4][is_deleted: 1][padding: 3]
    id: u64,
    content_offset: u32,
    content_size: u32,
    is_deleted: bool,
}

impl PageRecordMetadata {
    pub fn new(id: u64, content_offset: u32, content_size: u32, is_deleted: bool) -> Self {
        Self {
            id,
            content_offset,
            content_size,
            is_deleted,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_content_offset(&self) -> u32 {
        self.content_offset
    }

    pub fn get_content_size(&self) -> u32 {
        self.content_size
    }

    pub fn get_is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn set_is_deleted(&mut self, is_deleted: bool) {
        self.is_deleted = is_deleted;
    }

    pub fn set_content_size(&mut self, new_content_size_length: u32) {
        self.content_size = new_content_size_length;
    }

    pub fn set_content_offset(&mut self, new_content_offset: u32) {
        self.content_offset = new_content_offset;
    }
}

impl BinarySerde for PageRecordMetadata {
    type Output = [u8; PAGE_RECORD_METADATA_SIZE]; // Fixed size array

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; PAGE_RECORD_METADATA_SIZE];
        bytes[0..8].copy_from_slice(&self.id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.content_offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.content_size.to_le_bytes());
        bytes[16..17].copy_from_slice(&[if self.is_deleted { 1u8 } else { 0u8 }]);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("PageRecord deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != PAGE_RECORD_METADATA_SIZE {
            return Err(format!(
                "PageRecord deserialization failed: expected exactly {} bytes (8 for id + 4 for content_offset + 4 for content_size), got {} bytes",
                PAGE_RECORD_METADATA_SIZE, bytes.len()
            ));
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let content_offset = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let content_size = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let is_deleted = bytes[16] == 1u8;

        Ok(Self {
            id,
            content_offset,
            content_size,
            is_deleted,
        })
    }
}

impl ReadWrite for PageRecordMetadata {
    type RWError = Box<dyn Error>;

    fn write_to_file(
        &self,
        file: &mut std::fs::File,
        absolute_file_start_offset: u64,
        filename: &str,
    ) -> Result<(), Self::RWError> {
        let _ = match file.seek(SeekFrom::Start(absolute_file_start_offset)) {
            Ok(pos) => pos,
            Err(error) => {
                println!("Error seeking in the file {filename}: {error}");
                return Err(Box::new(error));
            }
        };

        match file.write_all(&self.to_bytes()) {
            Ok(_) => Ok(()),
            Err(error) => {
                println!("Error writing page record to the file {filename}: {error}");
                Err(Box::new(error))
            }
        }
    }

    fn read_from_file(
        file: &mut std::fs::File,
        absolute_file_start_offset: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized,
    {
        let _ = match file.seek(SeekFrom::Start(absolute_file_start_offset)) {
            Ok(pos) => pos,
            Err(error) => {
                println!("Error seeking in the file {filename}: {error}");
                return Err(Box::new(error));
            }
        };

        let mut buffer: Vec<u8> = vec![0u8; size];
        match file.read_exact(&mut buffer) {
            Ok(_) => Ok(PageRecordMetadata::from_bytes(&(buffer))?),
            Err(error) => {
                println!(
                    "Error reading page record at pos {absolute_file_start_offset} in {filename}: {error}"
                );
                Err(Box::new(error))
            }
        }
    }
}

#[derive(Debug)]
pub struct PageRecordContent {
    content: Vec<ContentTypes>,
}

impl Clone for PageRecordContent {
    fn clone(&self) -> Self {
        PageRecordContent { content: self.get_content().clone() }
    }
}

impl PageRecordContent {
    pub fn new(content: Vec<ContentTypes>) -> Self {
        Self { content }
    }

    pub fn get_content(&self) -> &Vec<ContentTypes> {
        &self.content
    }
}

impl BinarySerde for PageRecordContent {
    type Output = Vec<u8>;

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Vec<u8> = (self.content.len() as u32).to_le_bytes().to_vec();
        for col in &self.content {
            let col_bytes = col.to_bytes();
            bytes.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&col_bytes);
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err(
                "PageRecordContent deserialization failed: byte slice is empty".to_string(),
            );
        }
        if bytes.len() <= 4 {
            return Err(format!(
                "PageRecordContent deserialization failed: expected to be more than {} bytes, got {} bytes",
                4,
                bytes.len()
            ));
        }

        let count = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let mut current_total: usize = 4;
        let mut content: Vec<ContentTypes> = vec![];
        for _ in 0..count {
            let len =
                u32::from_le_bytes(bytes[current_total..current_total + 4].try_into().unwrap())
                    as usize;
            current_total += 4 + len;
            if bytes.len() < current_total {
                return Err(format!(
                "TableHeader deserialization failed during ColumnDef deserialization: bytes length {} expected to be not less than {}",
                bytes.len(), current_total
            ));
            }
            content.push(ContentTypes::from_bytes(
                &bytes[current_total - len..current_total],
            )?);
        }

        Ok(Self { content })
    }
}

impl ReadWrite for PageRecordContent {
    type RWError = Box<dyn Error>;

    fn write_to_file(
        &self,
        file: &mut std::fs::File,
        absolute_file_start_offset: u64,
        filename: &str,
    ) -> Result<(), Self::RWError> {
        let _ = match file.seek(SeekFrom::Start(absolute_file_start_offset)) {
            Ok(pos) => pos,
            Err(error) => {
                println!("Error seeking in the file {filename}: {error}");
                return Err(Box::new(error));
            }
        };

        match file.write_all(&self.to_bytes()) {
            Ok(_) => Ok(()),
            Err(error) => {
                println!("Error writing page record content to the file {filename}: {error}");
                Err(Box::new(error))
            }
        }
    }

    fn read_from_file(
        file: &mut std::fs::File,
        absolute_file_start_offset: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized,
    {
        let _ = match file.seek(SeekFrom::Start(absolute_file_start_offset)) {
            Ok(pos) => pos,
            Err(error) => {
                println!("Error seeking in the file {filename}: {error}");
                return Err(Box::new(error));
            }
        };

        let mut buffer: Vec<u8> = vec![0u8; size];
        match file.read_exact(&mut buffer) {
            Ok(_) => Ok(PageRecordContent::from_bytes(&(buffer))?),
            Err(error) => {
                println!(
                    "Error reading page record at pos {absolute_file_start_offset} in {filename}: {error}"
                );
                Err(Box::new(error))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ══════════════════════════════════════════════════════════
    // PageRecordMetadata tests (20 bytes fixed)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn record_metadata_roundtrip() {
        let meta = PageRecordMetadata::new(99, 4096, 256, false);
        let bytes = meta.to_bytes();
        let restored = PageRecordMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(restored.get_id(), 99);
        assert_eq!(restored.get_content_offset(), 4096);
        assert_eq!(restored.get_content_size(), 256);
    }

    #[test]
    fn record_metadata_deleted_flag() {
        let is_deleted_record = PageRecordMetadata::new(1, 1, 5, true);
        let is_not_deleted_record = PageRecordMetadata::new(1, 1, 5, false);

        assert_eq!(is_deleted_record.to_bytes()[16], 1);
        assert_eq!(is_not_deleted_record.to_bytes()[16], 0);
    }

    #[test]
    fn record_metadata_empty_bytes() {
        assert!(PageRecordMetadata::from_bytes(&[]).is_err());
    }

    #[test]
    fn record_metadata_wrong_size() {
        assert!(PageRecordMetadata::from_bytes(&[0; 10]).is_err());
        assert!(PageRecordMetadata::from_bytes(&[0; 30]).is_err());
    }

    // ══════════════════════════════════════════════════════════
    // PageRecordContent tests (variable size)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn record_content_mixed_types() {
        let content = PageRecordContent::new(vec![
            ContentTypes::Boolean(true),
            ContentTypes::Int32(42),
            ContentTypes::Text("test".to_string()),
        ]);
        let bytes = content.to_bytes();
        let restored = PageRecordContent::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn record_content_single_column() {
        let content = PageRecordContent::new(vec![ContentTypes::Int8(127)]);
        let bytes = content.to_bytes();
        let restored = PageRecordContent::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn record_content_byte_format() {
        let content = PageRecordContent::new(vec![
            ContentTypes::Boolean(true),
            ContentTypes::Int32(42),
            ContentTypes::Text("test".to_string()),
        ]);
        let bytes = content.to_bytes();
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 3);
        let restored = PageRecordContent::from_bytes(&bytes).unwrap();
        assert_eq!(restored.content.len(), 3);
    }

    #[test]
    fn record_content_empty_bytes() {
        assert!(PageRecordContent::from_bytes(&[]).is_err());
    }
}
