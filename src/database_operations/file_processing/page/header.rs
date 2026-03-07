use std::io::{Read, Seek, SeekFrom, Write};

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::{BinarySerde, ReadWrite};
use crate::database_operations::file_processing::HEADER_SIZE;

/// Fixed 20-byte header at the start of each page.
/// Tracks record count, deletion state, and space availability.
#[derive(Debug, Clone, Copy)]
pub struct PageHeader {
    // 20 bytes
    pub(in crate::database_operations::file_processing) page_number: u64, // 8 bytes
    records_count: u16,                                               // 2 bytes
    deleted_count: u16,                                               // 2 bytes
    free_space: u32,                                                  // 4 bytes
    fragmented_space: u32,                                            // 4 bytes
}

impl PageHeader {
    pub fn new(
        page_number: u64,
        records_count: u16,
        deleted_count: u16,
        free_space: u32,
        fragmented_space: u32,
    ) -> Self {
        Self {
            page_number,
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
    /// Memory layout: [page_number: 8][records_count: 2][deleted_count: 2][free_space: 4][fragmented_space: 4]
    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..8].copy_from_slice(&self.page_number.to_le_bytes());
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
                HEADER_SIZE,
                bytes.len()
            ));
        }

        let page_number = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let records_count = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let deleted_count = u16::from_le_bytes(bytes[10..12].try_into().unwrap());
        let free_space = u32::from_le_bytes(bytes[12..16].try_into().unwrap());
        let fragmented_space = u32::from_le_bytes(bytes[16..20].try_into().unwrap());

        Ok(Self {
            page_number,
            records_count,
            deleted_count,
            free_space,
            fragmented_space,
        })
    }
}

impl ReadWrite for PageHeader {
    type RWError = DatabaseError;

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
                return Err(DatabaseError::Io(error));
            }
        };

        match file.write_all(&self.to_bytes()) {
            Ok(_) => Ok(()),
            Err(error) => {
                println!("Error writing page header to the file {filename}: {error}");
                Err(DatabaseError::Io(error))
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
                return Err(DatabaseError::Io(error));
            }
        };

        let mut buffer: Vec<u8> = vec![0u8; size];
        match file.read_exact(&mut buffer) {
            Ok(_) => Ok(PageHeader::from_bytes(&(buffer[0..HEADER_SIZE]))?),
            Err(error) => {
                println!("Error reading page header at pos {absolute_file_start_offset} (look for you page size) in {filename}: {error}");
                Err(DatabaseError::Io(error))
            }
        }
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
    // [page_number: 8][records_count: 2][deleted_count: 2][free_space: 4][fragmented_space: 4]
    #[test]
    fn page_header_byte_layout() {
        let header = PageHeader::new(1, 2, 3, 4, 5);
        let bytes = header.to_bytes();

        assert_eq!(bytes.len(), HEADER_SIZE); // should be 20
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 1); // page_number
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

        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            u64::MAX
        );
        assert_eq!(
            u16::from_le_bytes(bytes[8..10].try_into().unwrap()),
            u16::MAX
        );
        assert_eq!(
            u16::from_le_bytes(bytes[10..12].try_into().unwrap()),
            u16::MAX
        );
        assert_eq!(
            u32::from_le_bytes(bytes[12..16].try_into().unwrap()),
            u32::MAX
        );
        assert_eq!(
            u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
            u32::MAX
        );
    }
}
