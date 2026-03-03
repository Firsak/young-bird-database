use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::INDEX_ENTRY_SIZE;

/// Status of a bucket slot in the hash index.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BucketStatus {
    Empty = 0,
    Occupied = 1,
    Tombstone = 2,
}

/// A single entry in the hash index.
/// Maps a record_id to its physical location (page_number, slot_index).
///
/// Binary layout (20 bytes, all LE):
/// [record_id: u64 (8)][page_number: u64 (8)][slot_index: u16 (2)][status: u8 (1)][padding: u8 (1)]
#[derive(Debug, Clone)]
pub struct IndexEntry {
    record_id: u64,
    page_number: u64,
    slot_index: u16,
    status: BucketStatus,
}

impl IndexEntry {
    pub fn new(record_id: u64, page_number: u64, slot_index: u16, status: BucketStatus) -> Self {
        IndexEntry {
            record_id,
            page_number,
            slot_index,
            status,
        }
    }

    pub fn empty() -> Self {
        IndexEntry {
            record_id: 0,
            page_number: 0,
            slot_index: 0,
            status: BucketStatus::Empty,
        }
    }

    pub fn get_record_id(&self) -> u64 {
        self.record_id
    }

    pub fn get_page_number(&self) -> u64 {
        self.page_number
    }

    pub fn get_slot_index(&self) -> u16 {
        self.slot_index
    }

    pub fn get_status(&self) -> BucketStatus {
        self.status
    }

    pub fn is_empty(&self) -> bool {
        self.status == BucketStatus::Empty
    }

    pub fn is_occupied(&self) -> bool {
        self.status == BucketStatus::Occupied
    }

    pub fn is_tombstone(&self) -> bool {
        self.status == BucketStatus::Tombstone
    }

    pub fn set_status(&mut self, status: BucketStatus) {
        self.status = status;
    }
}

impl BinarySerde for IndexEntry {
    type Output = [u8; INDEX_ENTRY_SIZE];

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];

        bytes[0..8].copy_from_slice(&self.record_id.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.page_number.to_le_bytes());
        bytes[16..18].copy_from_slice(&self.slot_index.to_le_bytes());
        bytes[18..19].copy_from_slice(&(self.status as u8).to_le_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != INDEX_ENTRY_SIZE {
            return Err(format!(
                "IndexEntry has to be exactly {} bytes long, found {}",
                INDEX_ENTRY_SIZE,
                bytes.len()
            ));
        }

        let record_id = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let page_number = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let slot_index = u16::from_le_bytes(bytes[16..18].try_into().unwrap());
        let status = match bytes[18] {
            0 => BucketStatus::Empty,
            1 => BucketStatus::Occupied,
            2 => BucketStatus::Tombstone,
            other => return Err(format!("Invalid BucketStatus: {}", other)),
        };

        Ok(Self {
            record_id,
            page_number,
            slot_index,
            status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_entry_roundtrip() {
        let entry = IndexEntry::new(42, 5, 10, BucketStatus::Occupied);
        let bytes = entry.to_bytes();
        let restored = IndexEntry::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_record_id(), 42);
        assert_eq!(restored.get_page_number(), 5);
        assert_eq!(restored.get_slot_index(), 10);
        assert_eq!(restored.get_status(), BucketStatus::Occupied);
    }

    #[test]
    fn index_entry_byte_layout() {
        let entry = IndexEntry::new(1, 2, 3, BucketStatus::Tombstone);
        let bytes = entry.to_bytes();
        assert_eq!(bytes.len(), INDEX_ENTRY_SIZE);
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 2);
        assert_eq!(u16::from_le_bytes(bytes[16..18].try_into().unwrap()), 3);
        assert_eq!(bytes[18], 2); // Tombstone
        assert_eq!(bytes[19], 0); // padding
    }

    #[test]
    fn index_entry_empty_constructor() {
        let entry = IndexEntry::empty();
        assert!(entry.is_empty());
        assert_eq!(entry.get_record_id(), 0);
        assert_eq!(entry.get_page_number(), 0);
        assert_eq!(entry.get_slot_index(), 0);
    }

    #[test]
    fn index_entry_all_status_variants() {
        for (status, expected_byte) in [
            (BucketStatus::Empty, 0u8),
            (BucketStatus::Occupied, 1u8),
            (BucketStatus::Tombstone, 2u8),
        ] {
            let entry = IndexEntry::new(1, 1, 1, status);
            let bytes = entry.to_bytes();
            assert_eq!(bytes[18], expected_byte);
            let restored = IndexEntry::from_bytes(&bytes).unwrap();
            assert_eq!(restored.get_status(), status);
        }
    }

    #[test]
    fn index_entry_invalid_status() {
        let mut bytes = [0u8; INDEX_ENTRY_SIZE];
        bytes[18] = 3; // invalid status
        assert!(IndexEntry::from_bytes(&bytes).is_err());
    }

    #[test]
    fn index_entry_wrong_size() {
        assert!(IndexEntry::from_bytes(&[0; 10]).is_err());
        assert!(IndexEntry::from_bytes(&[0; 25]).is_err());
        assert!(IndexEntry::from_bytes(&[]).is_err());
    }
}
