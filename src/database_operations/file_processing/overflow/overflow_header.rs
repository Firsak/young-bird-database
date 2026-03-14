use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::OVERFLOW_HEADER_SIZE;

/// Header at the start of each `.overflow` file (16 bytes).
/// Tracks how much space is used and how much is wasted by deleted/overwritten entries.
///
/// Binary format: [used_space: u64 LE][fragmented_space: u64 LE]
#[derive(Debug, Clone, PartialEq)]
pub struct OverflowHeader {
    /// Total bytes used in this file (header + all text data).
    /// Also serves as the write cursor — next append goes at this offset.
    /// Starts at OVERFLOW_HEADER_SIZE (16) for a fresh file.
    used_space: u64,
    /// Bytes from deleted or overwritten text entries.
    /// Can be reclaimed by compaction.
    fragmented_space: u64,
}

impl OverflowHeader {
    pub fn new(used_space: u64, fragmented_space: u64) -> Self {
        Self {
            used_space,
            fragmented_space,
        }
    }

    pub fn get_used_space(&self) -> u64 {
        self.used_space
    }

    pub fn get_fragmented_space(&self) -> u64 {
        self.fragmented_space
    }

    pub fn set_used_space(&mut self, used_space: u64) {
        self.used_space = used_space;
    }

    pub fn set_fragmented_space(&mut self, fragmented_space: u64) {
        self.fragmented_space = fragmented_space;
    }
}

impl BinarySerde for OverflowHeader {
    type Output = [u8; OVERFLOW_HEADER_SIZE];

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; OVERFLOW_HEADER_SIZE];
        bytes[0..8].copy_from_slice(&self.used_space.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.fragmented_space.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        if bytes.len() != OVERFLOW_HEADER_SIZE {
            return Err(format!(
                "OverflowHeader deserialization failed: expected {} bytes, got {}",
                OVERFLOW_HEADER_SIZE,
                bytes.len()
            ));
        }

        let used_space = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let fragmented_space = u64::from_le_bytes(bytes[8..16].try_into().unwrap());

        Ok(OverflowHeader {
            used_space,
            fragmented_space,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_header_roundtrip() {
        let original = OverflowHeader::new(4096, 128);
        let bytes = original.to_bytes();
        let restored = OverflowHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn overflow_header_fresh_file() {
        let header = OverflowHeader::new(OVERFLOW_HEADER_SIZE as u64, 0);
        let bytes = header.to_bytes();
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            OVERFLOW_HEADER_SIZE as u64
        );
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 0);
    }

    #[test]
    fn overflow_header_wrong_size() {
        assert!(OverflowHeader::from_bytes(&[0; 10]).is_err());
        assert!(OverflowHeader::from_bytes(&[]).is_err());
    }
}
