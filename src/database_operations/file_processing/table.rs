use std::error::Error;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::database_operations::file_processing::traits::ReadWrite;
use crate::database_operations::file_processing::PAGE_RECORD_METADATE_SIZE;

use super::traits::BinarySerde;
use super::types::{ColumnTypes, ContentTypes};
use super::HEADER_SIZE;

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
}

#[derive(Debug)]
pub struct PageHeader {
    // 20 bytes
    pub(in crate::database_operations::file_processing) page_id: u64,          // 8 bytes
    records_count: u16,    // 2 bytes
    deleted_count: u16,    // 2 bytes
    free_space: u32,       // 4 bytes
    fragmented_space: u32, // 4 bytes
}

impl PageHeader {
    pub fn new(
        page_id: u64,
        records_count: u16,
        deleted_count: u16,
        free_space: u32,
        fragmented_space: u32,
    ) -> Self {
        Self {
            page_id,
            records_count,
            deleted_count,
            free_space,
            fragmented_space,
        }
    }

    pub fn get_records_count(&self) -> u16 {
        self.records_count
    }

    pub fn get_deleted_records_count(&self) -> u16 {
        self.deleted_count
    }

    pub fn get_free_space(&self) -> u32 {
        self.free_space
    }

    pub fn get_fragment_space(&self) -> u32 {
        self.fragmented_space
    }

    pub fn update_records_count(&mut self, new_count: u16) {
        self.records_count = new_count;
    }

    pub fn update_deleted_records_count(&mut self, new_count: u16) {
        self.deleted_count = new_count;
    }

    pub fn update_free_space(&mut self, new_space: u32) {
        self.free_space = new_space;
    }

    pub fn update_fragmented_space(&mut self, new_space: u32) {
        self.fragmented_space = new_space;
    }
}

impl BinarySerde for PageHeader {
    type Output = [u8; HEADER_SIZE]; // Fixed size array

    /// Serializes the PageHeader into a 20-byte array in little-endian format.
    /// Memory layout: [page_id: 8][records_count: 2][deleted_count: 2][free_space: 4][fragmented_space: 4]
    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        bytes[8..10].copy_from_slice(&self.records_count.to_le_bytes());
        bytes[10..12].copy_from_slice(&self.deleted_count.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.free_space.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.fragmented_space.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("PageHeader deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != HEADER_SIZE {
            return Err(format!(
                "PageHeader deserialization failed: expected exactly {} bytes, got {} bytes",
                HEADER_SIZE, bytes.len()
            ));
        }

        let page_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let records_count = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let deleted_count = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
        let free_space = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let fragmented_space = u32::from_le_bytes(bytes[16..20].try_into().unwrap());

        Ok(Self {
            page_id,
            records_count,
            deleted_count,
            free_space,
            fragmented_space,
        })
    }
}

impl ReadWrite for PageHeader {
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
                println!("Error writing page header to the file {filename}: {error}");
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
            Ok(_) => Ok(PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?),
            Err(error) => {
                println!("Error reading page header at pos {absolute_file_start_offset} (look for you page size) in {filename}: {error}");
                Err(Box::new(error))
            }
        }
    }
}

#[derive(Debug)]
pub struct PageRecordMetadata {
    // 20 bytes: [id: 8][bytes_offset: 4][bytes_content: 4][is_deleted: 1][padding: 3]
    id: u64,
    bytes_offset: u32,
    bytes_content: u32,
    is_deleted: bool,
}

impl PageRecordMetadata {
    pub fn new(id: u64, bytes_offset: u32, bytes_content: u32, is_deleted: bool) -> Self {
        Self {
            id,
            bytes_offset,
            bytes_content,
            is_deleted,
        }
    }

    pub fn get_id(&self) -> u64 {
        self.id
    }

    pub fn get_bytes_offset(&self) -> u32 {
        self.bytes_offset
    }

    pub fn get_bytes_content(&self) -> u32 {
        self.bytes_content
    }

    pub fn get_is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn set_is_deleted(&mut self, is_deleted: bool) {
        self.is_deleted = is_deleted;
    }

    pub fn set_bytes_content(&mut self, new_bytes_content_length: u32) {
        self.bytes_content = new_bytes_content_length;
    }

    pub fn set_bytes_offset(&mut self, new_bytes_offset: u32) {
        self.bytes_offset = new_bytes_offset;
    }
}

impl BinarySerde for PageRecordMetadata {
    type Output = [u8; PAGE_RECORD_METADATE_SIZE]; // Fixed size array

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; PAGE_RECORD_METADATE_SIZE];
        bytes[0..8].copy_from_slice(&self.id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.bytes_offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.bytes_content.to_le_bytes());
        bytes[16..17].copy_from_slice(&[if self.is_deleted { 1u8 } else { 0u8 }]);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("PageRecord deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != PAGE_RECORD_METADATE_SIZE {
            return Err(format!(
                "PageRecord deserialization failed: expected exactly {} bytes (8 for id + 4 for bytes_offset + 4 for bytes_content), got {} bytes",
                PAGE_RECORD_METADATE_SIZE, bytes.len()
            ));
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let bytes_offset = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let bytes_content = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let is_deleted = bytes[16] == 1u8;

        Ok(Self {
            id,
            bytes_offset,
            bytes_content,
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

#[derive(Debug)]
pub struct ColumnDef {
    data_type: ColumnTypes, // 1 byte
    nullable: bool,         // 1 byte
    name: String,           // dynamic bytes
}

impl ColumnDef {
    pub fn new(data_type: ColumnTypes, nullable: bool, name: String) -> Self {
        Self {
            data_type,
            nullable,
            name,
        }
    }
}

impl BinarySerde for ColumnDef {
    type Output = Vec<u8>; // Variable size Vec

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Vec<u8> = vec![self.data_type.to_bytes()[0]];
        bytes.push(if self.nullable { 1_u8 } else { 0_u8 });
        let name_bytes = &self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("ColumnDef deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() <= 6 {
            return Err(format!(
                "ColumnDef deserialization failed: expected to be more than {} bytes, got {} bytes",
                6,
                bytes.len()
            ));
        }

        let data_type = ColumnTypes::from_bytes(bytes[0..1].try_into().unwrap())?;
        let nullable_bytes = bytes[1];
        let nullable = match nullable_bytes {
            0 => false,
            1 => true,
            _ => return Err("ColumnDef nullable byte should be 0 or 1".to_string()),
        };
        let len = u32::from_le_bytes(bytes[2..6].try_into().unwrap()) as usize;
        let expected_total = 6 + len;
        if bytes.len() != expected_total {
            return Err(format!(
                "ColumnDef deserialization failed: length name prefix indicates {} bytes of text, expected total {} bytes, got {} bytes",
                len, expected_total, bytes.len()
            ));
        }
        let name = String::from_utf8(bytes[6..].to_vec()).map_err(|e| {
            format!(
                "ColumnDef deserialization failed: invalid UTF-8 encoding: {}",
                e
            )
        })?;

        Ok(Self {
            name,
            data_type,
            nullable,
        })
    }
}

#[derive(Debug)]
pub struct TableHeader {
    // 10 bytes + columns_count * dynamic bytes
    pages_count: u64,       // 8 bytes
    columns_count: u16,     // 2 bytes
    header: Vec<ColumnDef>, // columns_count * dynamic bytes
}

impl TableHeader {
    pub fn new(pages_count: u64, columns_count: u16, header: Vec<ColumnDef>) -> Self {
        Self {
            pages_count,
            columns_count,
            header,
        }
    }
}

impl BinarySerde for TableHeader {
    type Output = Vec<u8>; // Variable size Vec

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Vec<u8> = self.pages_count.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.columns_count.to_le_bytes());
        for col in &self.header {
            let col_bytes = col.to_bytes();
            bytes.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&col_bytes);
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("TableHeader deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() < 10 {
            return Err(format!(
                "TableHeader deserialization failed: expected at least {} bytes (8 for pages_count + 2 for columns_count), got {} bytes",
                10,
                bytes.len()
            ));
        }

        let pages_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let columns_count = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let mut current_total: usize = 10;
        let mut header: Vec<ColumnDef> = vec![];
        for _ in 0..columns_count {
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
            header.push(ColumnDef::from_bytes(
                &bytes[current_total - len..current_total],
            )?);
        }

        Ok(Self {
            pages_count,
            columns_count,
            header,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ══════════════════════════════════════════════════════════
    // PageHeader tests (20 bytes fixed)
    // ══════════════════════════════════════════════════════════

    // EXAMPLE: full roundtrip with field verification
    #[test]
    fn page_header_roundtrip() {
        let header = PageHeader::new(42, 10, 3, 5000, 200);
        let bytes = header.to_bytes();
        let restored = PageHeader::from_bytes(&bytes).unwrap();

        // Verify every field survived the roundtrip
        assert_eq!(restored.get_records_count(), 10);
        assert_eq!(restored.get_deleted_records_count(), 3);
        assert_eq!(restored.get_free_space(), 5000);
        assert_eq!(restored.get_fragment_space(), 200);
    }

    // Verify the exact binary layout matches what we documented:
    // [page_id: 8][records_count: 2][deleted_count: 2][free_space: 4][fragmented_space: 4]
    #[test]
    fn page_header_byte_layout() {
        let header = PageHeader::new(1, 2, 3, 4, 5);
        let bytes = header.to_bytes();

        assert_eq!(bytes.len(), HEADER_SIZE); // should be 20
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 1); // page_id
        assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), 2); // records_count
        assert_eq!(u16::from_le_bytes(bytes[10..12].try_into().unwrap()), 3); // deleted_count
        assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 4); // free_space
        assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), 5); // fragmented_space
    }

    #[test]
    fn page_header_empty_bytes() {
        assert!(PageHeader::from_bytes(&[]).is_err());
    }

    #[test]
    fn page_header_wrong_size() {
        // Too few bytes
        assert!(PageHeader::from_bytes(&[0; 10]).is_err());
        // Too many bytes
        assert!(PageHeader::from_bytes(&[0; 30]).is_err());
    }

    #[test]
    fn page_header_max_values() {
        let header = PageHeader::new(u64::MAX, u16::MAX, u16::MAX, u32::MAX, u32::MAX);
        let bytes = header.to_bytes();
        
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), u64::MAX);
        assert_eq!(u16::from_le_bytes(bytes[8..10].try_into().unwrap()), u16::MAX);
        assert_eq!(u16::from_le_bytes(bytes[10..12].try_into().unwrap()), u16::MAX);
        assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), u32::MAX);
        assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), u32::MAX); 
    }

    // ══════════════════════════════════════════════════════════
    // PageRecordMetadata tests (20 bytes fixed)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn record_metadata_roundtrip() {
        let meta = PageRecordMetadata::new(99, 4096, 256, false);
        let bytes = meta.to_bytes();
        let restored = PageRecordMetadata::from_bytes(&bytes).unwrap();

        assert_eq!(restored.get_id(), 99);
        assert_eq!(restored.get_bytes_offset(), 4096);
        assert_eq!(restored.get_bytes_content(), 256);
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
        assert!(PageRecordMetadata::from_bytes(&[0;10]).is_err());
        assert!(PageRecordMetadata::from_bytes(&[0;30]).is_err());
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
        let content = PageRecordContent::new(vec![
            ContentTypes::Int8(127)
        ]);
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

    // ══════════════════════════════════════════════════════════
    // ColumnDef tests (variable size)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn column_def_roundtrip() {
        let col = ColumnDef::new(ColumnTypes::Int32, true, "age".to_string());
        let bytes = col.to_bytes();
        let restored = ColumnDef::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn column_def_byte_layout() {
        let column = ColumnDef::new(ColumnTypes::Text, false, "email".to_string());

        let bytes = column.to_bytes();

        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2..6], 5u32.to_le_bytes());
        assert_eq!(&bytes[6..], b"email");
    }

    #[test]
    fn column_def_empty_bytes() {
        assert!(ColumnDef::from_bytes(&[]).is_err());
    }

    // ══════════════════════════════════════════════════════════
    // TableHeader tests (variable size)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn table_header_roundtrip() {
        let header = TableHeader::new(
            5, // pages_count
            2, // columns_count
            vec![
                ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
                ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
            ],
        );
        let bytes = header.to_bytes();
        let restored = TableHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn table_header_empty_columns() {
        let header = TableHeader::new(1, 0, vec![]);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 10); // 8 (pages_count) + 2 (columns_count)
        let restored = TableHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn table_header_empty_bytes() {
        assert!(TableHeader::from_bytes(&[]).is_err());
    }

    #[test]
    fn table_header_too_short() {
        assert!(TableHeader::from_bytes(&[0; 5]).is_err());
        assert!(TableHeader::from_bytes(&[0; 9]).is_err());
    }
}
