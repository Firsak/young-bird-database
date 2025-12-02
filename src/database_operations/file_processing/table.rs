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
    // 12 bytes
    page_id: u64,          // 8 bytes
    records_count: u16,    // 2 bytes
    deleted_count: u16,    // 2 bytes
    free_space: u16,       // 2 bytes
    fragmented_space: u16, // 2 bytes
}

impl PageHeader {
    pub fn new(
        page_id: u64,
        records_count: u16,
        deleted_count: u16,
        free_space: u16,
        fragmented_space: u16,
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

    pub fn get_free_space(&self) -> u16 {
        self.free_space
    }

    pub fn get_fragment_space(&self) -> u16 {
        self.fragmented_space
    }

    pub fn update_records_count(&mut self, new_count: u16) {
        self.records_count = new_count;
    }

    pub fn update_deleted_records_count(&mut self, new_count: u16) {
        self.deleted_count = new_count;
    }

    pub fn update_free_space(&mut self, new_space: u16) {
        self.free_space = new_space;
    }

    pub fn update_fragmented_space(&mut self, new_space: u16) {
        self.fragmented_space = new_space;
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
        bytes[10..12].copy_from_slice(&self.deleted_count.to_le_bytes());
        // Bytes 12-13: free_space (u16)
        bytes[12..14].copy_from_slice(&self.free_space.to_le_bytes());
        bytes[14..16].copy_from_slice(&self.fragmented_space.to_le_bytes());
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
        let deleted_count = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
        // Extract free_space from bytes 12-13
        let free_space = u16::from_le_bytes(bytes[12..14].try_into().unwrap());
        let fragmented_space = u16::from_le_bytes(bytes[14..16].try_into().unwrap());

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
        start_pos_bytes: u64,
        filename: &str,
    ) -> Result<(), Self::RWError> {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
        start_pos_bytes: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized,
    {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
                println!("Error reading page header at pos {start_pos_bytes} (look for you page size) in {filename}: {error}");
                Err(Box::new(error))
            }
        }
    }
}

#[derive(Debug)]
pub struct PageRecordMetadata {
    // 16 bytes
    id: u64,            // 8 bytes
    bytes_offset: u32,  // 4 bytes
    bytes_content: u32, // 4 bytes
    is_deleted: bool,   // 1 bytes
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
        start_pos_bytes: u64,
        filename: &str,
    ) -> Result<(), Self::RWError> {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
        start_pos_bytes: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized,
    {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
                    "Error reading page record at pos {start_pos_bytes} in {filename}: {error}"
                );
                Err(Box::new(error))
            }
        }
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

impl ReadWrite for PageRecordContent {
    type RWError = Box<dyn Error>;

    fn write_to_file(
        &self,
        file: &mut std::fs::File,
        start_pos_bytes: u64,
        filename: &str,
    ) -> Result<(), Self::RWError> {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
        start_pos_bytes: u64,
        size: usize,
        filename: &str,
    ) -> Result<Self, Self::RWError>
    where
        Self: Sized,
    {
        let _ = match file.seek(SeekFrom::Start(start_pos_bytes)) {
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
                    "Error reading page record at pos {start_pos_bytes} in {filename}: {error}"
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
