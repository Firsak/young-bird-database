use crate::database_operations::file_processing::PAGE_RECORD_SIZE;

use super::traits::BinarySerde;
use super::types::{ColumnTypes, ContentTypes};
use super::HEADER_SIZE;

#[derive(Debug)]
pub struct Page {
    pub header: PageHeader,
    records: Vec<PageRecord>,
    records_content: Vec<PageRecordContent>,
}

impl Page {
    pub fn new(
        header: PageHeader,
        records: Vec<PageRecord>,
        records_content: Vec<PageRecordContent>,
    ) -> Self {
        Self {
            header,
            records,
            records_content,
        }
    }
}

#[derive(Debug)]
pub struct PageHeader {
    // 12 bytes
    page_id: u64,       // 8 bytes
    records_count: u16, // 2 bytes
    free_space: u16,    // 2 bytes
}

impl PageHeader {
    pub fn new(page_id: u64, records_count: u16, free_space: u16) -> Self {
        Self {
            page_id,
            records_count,
            free_space,
        }
    }

    pub fn get_records_count(&self) -> u16 {
        self.records_count
    }

    pub fn get_free_space(&self) -> u16 {
        self.free_space
    }

    pub fn update_records_count(&mut self, new_count: u16) -> () {
        self.records_count = new_count;
    }

    pub fn update_free_space(&mut self, new_space: u16) -> () {
        self.free_space = new_space;
    }
}

impl BinarySerde for PageHeader {
    type Output = [u8; HEADER_SIZE]; // Fixed size array

    /// Serializes the PageHeader into a 12-byte array in little-endian format.
    /// Memory layout: [page_id: 8 bytes][records_count: 2 bytes][free_space: 2 bytes]
    /// Uses copy_from_slice for efficient memcpy operation.
    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; HEADER_SIZE];
        // Bytes 0-7: page_id (u64)
        bytes[0..8].copy_from_slice(&self.page_id.to_le_bytes());
        // Bytes 8-9: records_count (u16)
        bytes[8..10].copy_from_slice(&self.records_count.to_le_bytes());
        // Bytes 10-11: free_space (u16)
        bytes[10..12].copy_from_slice(&self.free_space.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("PageHeader deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != HEADER_SIZE {
            return Err(format!(
                "PageHeader deserialization failed: expected exactly {} bytes (8 for page_id + 2 for records_count + 2 for free_space), got {} bytes",
                HEADER_SIZE, bytes.len()
            ));
        }

        // Extract page_id from bytes 0-7
        let page_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        // Extract records_count from bytes 8-9
        let records_count = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        // Extract free_space from bytes 10-11
        let free_space = u16::from_le_bytes(bytes[10..12].try_into().unwrap());

        Ok(Self {
            page_id,
            records_count,
            free_space,
        })
    }
}

#[derive(Debug)]
pub struct PageRecord {
    // 16 bytes
    id: u64,            // 8 bytes
    bytes_offset: u32,  // 4 bytes
    bytes_content: u32, // 4 bytes
}

impl PageRecord {
    pub fn new(id: u64, bytes_offset: u32, bytes_content: u32) -> Self {
        Self {
            id,
            bytes_offset,
            bytes_content,
        }
    }
}

impl BinarySerde for PageRecord {
    type Output = [u8; PAGE_RECORD_SIZE]; // Fixed size array

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; PAGE_RECORD_SIZE];
        bytes[0..8].copy_from_slice(&self.id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.bytes_offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.bytes_content.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("PageRecord deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != PAGE_RECORD_SIZE {
            return Err(format!(
                "PageRecord deserialization failed: expected exactly {} bytes (8 for id + 4 for bytes_offset + 4 for bytes_content), got {} bytes",
                PAGE_RECORD_SIZE, bytes.len()
            ));
        }

        let id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let bytes_offset = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let bytes_content = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

        Ok(Self {
            id,
            bytes_offset,
            bytes_content,
        })
    }
}

#[derive(Debug)]
pub struct PageRecordContent {
    content: Vec<ContentTypes>, // columns_count * 8 bytes (1 or 9 bytes if nullable)
}

impl PageRecordContent {
    pub fn new(content: Vec<ContentTypes>) -> Self {
        Self { content }
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
    // 16 bytes + columns_count * dynamic bytes
    pages_count: u64,       // 8 bytes
    columns_count: u64,     // 8 bytes
    header: Vec<ColumnDef>, // columns_count * dynamic bytes
}

impl TableHeader {
    pub fn new(pages_count: u64, columns_count: u64, header: Vec<ColumnDef>) -> Self {
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
        if bytes.len() <= 16 {
            return Err(format!(
                "TableHeader deserialization failed: expected at least {} bytes, got {} bytes",
                16,
                bytes.len()
            ));
        }

        let pages_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let columns_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let mut current_total: usize = 16;
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
