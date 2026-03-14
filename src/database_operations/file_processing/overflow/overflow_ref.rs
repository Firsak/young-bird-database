use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::OVERFLOW_REF_SIZE;

/// Fixed 16-byte reference to text stored in an overflow file.
/// Stored on-page in place of inline text when `is_file_stored = 1`.
///
/// Binary format: [file_index: u32 LE][offset: u64 LE][length: u32 LE]
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct OverflowRef {
    /// Which overflow file (0-based): {table_name}_{file_index}.overflow
    file_index: u32,
    /// Byte offset within the overflow file where the text starts
    offset: u64,
    /// Length of the stored text in bytes
    length: u32,
}

impl OverflowRef {
    pub fn new(file_index: u32, offset: u64, length: u32) -> Self {
        Self {
            file_index,
            offset,
            length,
        }
    }

    pub fn get_file_index(&self) -> u32 {
        self.file_index
    }

    pub fn get_offset(&self) -> u64 {
        self.offset
    }

    pub fn get_length(&self) -> u32 {
        self.length
    }
}

impl BinarySerde for OverflowRef {
    type Output = [u8; OVERFLOW_REF_SIZE];

    fn to_bytes(&self) -> Self::Output {
        let mut bytes = [0u8; OVERFLOW_REF_SIZE];
        bytes[0..4].copy_from_slice(&self.file_index.to_le_bytes());
        bytes[4..12].copy_from_slice(&self.offset.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.length.to_le_bytes());
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != OVERFLOW_REF_SIZE {
            return Err(format!(
                "OverflowRef deserialization failed: expected {} bytes, got {}",
                OVERFLOW_REF_SIZE,
                bytes.len()
            ));
        }

        let file_index = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        let offset = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let length = u32::from_le_bytes(bytes[12..16].try_into().unwrap());

        Ok(Self {
            file_index,
            offset,
            length,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overflow_ref_roundtrip() {
        let original = OverflowRef::new(2, 8192, 1500);
        let bytes = original.to_bytes();
        let restored = OverflowRef::from_bytes(&bytes).unwrap();
        assert_eq!(restored, original);
    }

    #[test]
    fn overflow_ref_byte_layout() {
        let r = OverflowRef::new(3, 16384, 500);
        let bytes = r.to_bytes();
        assert_eq!(u32::from_le_bytes(bytes[0..4].try_into().unwrap()), 3);
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 16384);
        assert_eq!(u32::from_le_bytes(bytes[12..16].try_into().unwrap()), 500);
    }

    #[test]
    fn overflow_ref_wrong_size() {
        assert!(OverflowRef::from_bytes(&[0; 10]).is_err());
        assert!(OverflowRef::from_bytes(&[]).is_err());
    }
}
