use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::INDEX_HEADER_SIZE;

/// Header for the hash index file.
/// Stores metadata about the hash table: bucket count, entry count, and hash seed.
///
/// Binary layout (24 bytes, all LE):
/// [bucket_count: u64 (8)][entry_count: u64 (8)][seed: u64 (8)]
#[derive(Debug)]
pub struct IndexHeader {
    bucket_count: u64,
    entry_count: u64,
    seed: u64,
}

impl IndexHeader {
    pub fn new(bucket_count: u64, entry_count: u64) -> Self {
        IndexHeader {
            bucket_count,
            entry_count,
            seed: 0,
        }
    }

    pub fn get_bucket_count(&self) -> u64 {
        self.bucket_count
    }

    pub fn get_entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn get_seed(&self) -> u64 {
        self.seed
    }

    pub fn update_entry_count(&mut self, new_count: u64) {
        self.entry_count = new_count;
    }

    pub fn update_bucket_count(&mut self, new_count: u64) {
        self.bucket_count = new_count;
    }
}

impl BinarySerde for IndexHeader {
    type Output = [u8; INDEX_HEADER_SIZE];

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Self::Output = [0u8; INDEX_HEADER_SIZE];

        bytes[0..8].copy_from_slice(&self.bucket_count.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.entry_count.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.seed.to_le_bytes());

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != INDEX_HEADER_SIZE {
            return Err(format!(
                "IndexHeader has to be exactly {} bytes long, found {}",
                INDEX_HEADER_SIZE,
                bytes.len()
            ));
        }

        let bucket_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let entry_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let seed = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

        Ok(Self {
            bucket_count,
            entry_count,
            seed,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_header_roundtrip() {
        let header = IndexHeader::new(16, 5);
        let bytes = header.to_bytes();
        let restored = IndexHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_bucket_count(), 16);
        assert_eq!(restored.get_entry_count(), 5);
        assert_eq!(restored.get_seed(), 0);
    }

    #[test]
    fn index_header_byte_layout() {
        let header = IndexHeader::new(256, 100);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), INDEX_HEADER_SIZE);
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 256);
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 100);
        assert_eq!(u64::from_le_bytes(bytes[16..24].try_into().unwrap()), 0);
    }

    #[test]
    fn index_header_empty_bytes() {
        assert!(IndexHeader::from_bytes(&[]).is_err());
    }

    #[test]
    fn index_header_wrong_size() {
        assert!(IndexHeader::from_bytes(&[0; 10]).is_err());
        assert!(IndexHeader::from_bytes(&[0; 30]).is_err());
    }

    #[test]
    fn index_header_max_values() {
        let header = IndexHeader::new(u64::MAX, u64::MAX);
        let bytes = header.to_bytes();
        let restored = IndexHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_bucket_count(), u64::MAX);
        assert_eq!(restored.get_entry_count(), u64::MAX);
    }
}
