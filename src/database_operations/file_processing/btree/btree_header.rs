use crate::database_operations::file_processing::traits::BinarySerde;

/// Header for the B-tree index file.
/// Stores metadata needed to reconstruct a BTree from disk:
/// root node index, total node count, and the free list of reusable slots.
///
/// Binary layout (24 bytes fixed + variable free list):
/// [root_index: u64 (8)][node_count: u64 (8)][free_list_count: u64 (8)]
/// [free_list[0]: u64 (8)][free_list[1]: u64 (8)]...
#[derive(Debug)]
pub struct BTreeHeader {
    root_index: u64,
    node_count: u64,
    free_list: Vec<u64>,
}

impl BTreeHeader {
    pub fn new(root_index: u64, node_count: u64, free_list: Vec<u64>) -> Self {
        Self {
            root_index,
            node_count,
            free_list,
        }
    }

    pub fn get_root_index(&self) -> u64 {
        self.root_index
    }

    pub fn get_node_count(&self) -> u64 {
        self.node_count
    }

    pub fn get_free_list(&self) -> &Vec<u64> {
        &self.free_list
    }
}

impl BinarySerde for BTreeHeader {
    type Output = Vec<u8>;

    // TODO(human): Implement to_bytes
    // Write the 24-byte fixed portion (root_index, node_count, free_list.len() as u64),
    // then append each free_list entry as u64 LE.
    fn to_bytes(&self) -> Self::Output {
        let mut buffer: Vec<u8> = vec![];

        buffer.extend_from_slice(&self.root_index.to_le_bytes());
        buffer.extend_from_slice(&self.node_count.to_le_bytes());
        buffer.extend_from_slice(&(self.free_list.len() as u64).to_le_bytes());
        for elem in &self.free_list {
            buffer.extend_from_slice(&elem.to_le_bytes());
        }

        buffer
    }

    // TODO(human): Implement from_bytes
    // Read the 24-byte fixed portion, then read free_list_count u64 entries.
    // Return Err if bytes is too short for the fixed portion or the declared free list.
    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        if bytes.len() < 24 {
            return Err(format!(
                "BTreeHeader expects at least 24 bytes, found {}",
                bytes.len()
            ));
        }

        let root_index = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let node_count = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
        let free_list_len = u64::from_le_bytes(bytes[16..24].try_into().unwrap());

        if bytes.len() as u64 - 24 < free_list_len * 8 {
            return Err(format!(
                "BTreeHeader free_list expects {} bytes, found {}",
                free_list_len * 8,
                bytes.len() - 24
            ));
        }

        let mut free_list = vec![];
        for i in 0..free_list_len {
            free_list.push(u64::from_le_bytes(
                bytes[24 + i as usize * 8..24 + (i as usize + 1) * 8]
                    .try_into()
                    .unwrap(),
            ));
        }

        Ok(BTreeHeader {
            root_index,
            node_count,
            free_list,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_free_list() {
        let header = BTreeHeader::new(5, 10, vec![]);
        let bytes = header.to_bytes();
        let restored = BTreeHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_root_index(), 5);
        assert_eq!(restored.get_node_count(), 10);
        assert!(restored.get_free_list().is_empty());
    }

    #[test]
    fn roundtrip_with_free_list() {
        let header = BTreeHeader::new(3, 8, vec![1, 4, 6]);
        let bytes = header.to_bytes();
        let restored = BTreeHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.get_root_index(), 3);
        assert_eq!(restored.get_node_count(), 8);
        assert_eq!(restored.get_free_list(), &vec![1, 4, 6]);
    }

    #[test]
    fn byte_layout_fixed_portion() {
        let header = BTreeHeader::new(42, 100, vec![7]);
        let bytes = header.to_bytes();
        // Fixed portion: 24 bytes + 1 free list entry = 32 bytes total
        assert_eq!(bytes.len(), 32);
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 42);
        assert_eq!(u64::from_le_bytes(bytes[8..16].try_into().unwrap()), 100);
        assert_eq!(u64::from_le_bytes(bytes[16..24].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(bytes[24..32].try_into().unwrap()), 7);
    }

    #[test]
    fn from_bytes_rejects_short_input() {
        assert!(BTreeHeader::from_bytes(&[]).is_err());
        assert!(BTreeHeader::from_bytes(&[0; 16]).is_err());
    }

    #[test]
    fn from_bytes_rejects_truncated_free_list() {
        // Header claims 2 free list entries but only provides bytes for 1
        let mut bytes = vec![0u8; 32]; // 24 fixed + 8 (one entry)
        bytes[16..24].copy_from_slice(&2u64.to_le_bytes()); // free_list_count = 2
        assert!(BTreeHeader::from_bytes(&bytes).is_err());
    }
}
