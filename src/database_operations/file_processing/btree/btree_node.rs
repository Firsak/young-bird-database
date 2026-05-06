// B-tree node: the building block of a B-tree index
//
// Each node is a fixed 8192-byte block stored in a .btree file.
// Leaf nodes store key-value pairs (key → record location).
// Internal nodes store keys + child pointers for routing searches.

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::BTREE_NODE_SIZE;

/// A single node in a B-tree index.
///
/// Two flavors:
/// - **Leaf**: stores keys + record locations. This is where indexed data lives.
///   `keys[i]` maps to `values[i]` = (page_number, slot_index).
/// - **Internal**: stores keys + child pointers for routing searches.
///   `children` has one more entry than `keys` (n keys → n+1 children).
///   Everything < keys[0] goes to children[0], between keys[0]..keys[1] to children[1], etc.
#[derive(Debug)]
pub struct BTreeNode {
    /// true = leaf (has values), false = internal (has children)
    pub is_leaf: bool,
    /// sorted search keys (u64 column values or record IDs)
    pub keys: Vec<u64>,
    /// leaf only: record locations, values[i] corresponds to keys[i]
    pub values: Vec<(u64, u16)>,
    /// internal only: child node indices, always keys.len() + 1 entries
    pub children: Vec<u64>,
}

impl BTreeNode {
    pub fn new_leaf() -> Self {
        Self {
            is_leaf: true,
            keys: vec![],
            values: vec![],
            children: vec![],
        }
    }

    pub fn new_internal() -> Self {
        Self {
            is_leaf: false,
            keys: vec![],
            values: vec![],
            children: vec![],
        }
    }

    /// Number of keys in this node. Derived from `keys.len()` to avoid
    /// dual-bookkeeping drift; the on-disk u16 metadata field is recomputed
    /// from this at serialize time.
    pub fn key_count(&self) -> u16 {
        self.keys.len() as u16
    }

    /// Inserts a key-value pair into a leaf node in sorted order.
    ///
    /// Uses `find_child_index` (binary search) to locate the correct position,
    /// then inserts both the key and value at that position to maintain sort order.
    ///
    /// # Arguments
    /// * `key` — the search key (e.g. column value or record ID)
    /// * `value` — record location as (page_number, slot_index)
    ///
    /// # Errors
    /// Returns `InvalidArgument` if called on an internal node.
    pub fn insert_value(&mut self, key: u64, value: (u64, u16)) -> Result<(), DatabaseError> {
        if !self.is_leaf {
            return Err(DatabaseError::InvalidArgument(
                "Node is not a leaf to add values".to_string(),
            ));
        }

        let pos = self.find_child_index(key);

        self.values.insert(pos, value);
        self.keys.insert(pos, key);
        Ok(())
    }

    /// Finds the insertion position for `key` using binary search (lower bound).
    ///
    /// Returns the index of the first key >= `key`. For internal nodes, this is
    /// the child slot to descend into. For leaf nodes, this is the sorted
    /// insertion position.
    pub fn find_child_index(&self, key: u64) -> usize {
        let mut left = 0;
        let mut right = self.keys.len();

        while left < right {
            let center = left + (right - left) / 2;
            if self.keys[center] < key {
                left = center + 1;
            } else {
                right = center
            }
        }

        left
    }

    /// Removes a key-value pair from a leaf node.
    ///
    /// Uses `find_child_index` (binary search) to locate the key, verifies
    /// it matches, then removes both the key and its corresponding value.
    ///
    /// # Arguments
    /// * `key` — the search key to remove
    ///
    /// # Errors
    /// Returns `InvalidArgument` if called on an internal node, or if the
    /// key does not exist in this leaf.
    pub fn delete(&mut self, key: u64) -> Result<(), DatabaseError> {
        if !self.is_leaf {
            return Err(DatabaseError::InvalidArgument(
                "Node is not a leaf to add values".to_string(),
            ));
        }

        let pos = self.find_child_index(key);

        if self.keys[pos] != key {
            return Err(DatabaseError::InvalidArgument(format!(
                "Key {} does not exist",
                key
            )));
        }

        self.values.remove(pos);
        self.keys.remove(pos);
        Ok(())
    }
}

impl BinarySerde for BTreeNode {
    type Output = [u8; BTREE_NODE_SIZE];

    fn to_bytes(&self) -> Self::Output {
        let mut buffer = [0u8; BTREE_NODE_SIZE];
        buffer[0] = if self.is_leaf { 1u8 } else { 0u8 };
        buffer[1..3].copy_from_slice(&(self.keys.len() as u16).to_le_bytes());
        for (keys_pos, key) in self.keys.iter().enumerate() {
            buffer[3 + keys_pos * 8..3 + (keys_pos + 1) * 8].copy_from_slice(&key.to_le_bytes());
        }
        if self.is_leaf {
            for (value_pos, value) in self.values.iter().enumerate() {
                buffer[3 + 8 * self.keys.len() + 10 * value_pos
                    ..3 + 8 * self.keys.len() + 10 * value_pos + 8]
                    .copy_from_slice(&value.0.to_le_bytes());
                buffer[3 + 8 * self.keys.len() + 10 * value_pos + 8
                    ..3 + 8 * self.keys.len() + 10 * value_pos + 10]
                    .copy_from_slice(&value.1.to_le_bytes());
            }
        } else {
            for (value_pos, value) in self.children.iter().enumerate() {
                buffer[3 + 8 * self.keys.len() + 8 * value_pos
                    ..3 + 8 * self.keys.len() + 8 * (value_pos + 1)]
                    .copy_from_slice(&value.to_le_bytes());
            }
        }

        buffer
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        if bytes.len() < BTREE_NODE_SIZE {
            return Err(format!(
                "Expected bytes length of {}, but found {}",
                BTREE_NODE_SIZE,
                bytes.len()
            ));
        }

        let is_leaf = bytes[0] == 1;
        let key_count = u16::from_le_bytes(bytes[1..3].try_into().unwrap());
        let mut keys: Vec<u64> = vec![];
        for keys_pos in 0..key_count as usize {
            keys.push(u64::from_le_bytes(
                bytes[3 + keys_pos * 8..3 + (keys_pos + 1) * 8]
                    .try_into()
                    .unwrap(),
            ));
        }
        let mut values: Vec<(u64, u16)> = vec![];
        let mut children: Vec<u64> = vec![];
        if is_leaf {
            for value_pos in 0..key_count as usize {
                let record_location = u64::from_le_bytes(
                    bytes[3 + 8 * key_count as usize + 10 * value_pos
                        ..3 + 8 * key_count as usize + 10 * value_pos + 8]
                        .try_into()
                        .unwrap(),
                );
                let value = u16::from_le_bytes(
                    bytes[3 + 8 * key_count as usize + 10 * value_pos + 8
                        ..3 + 8 * key_count as usize + 10 * value_pos + 10]
                        .try_into()
                        .unwrap(),
                );
                values.push((record_location, value));
            }
        } else if key_count > 0 {
            for value_pos in 0..(key_count as usize) + 1 {
                children.push(u64::from_le_bytes(
                    bytes[3 + 8 * key_count as usize + 8 * value_pos
                        ..3 + 8 * key_count as usize + 8 * (value_pos + 1)]
                        .try_into()
                        .unwrap(),
                ));
            }
        }

        Ok(Self {
            is_leaf,
            keys,
            values,
            children,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_empty_leaf() {
        let node = BTreeNode::new_leaf();
        let bytes = node.to_bytes();
        let restored = BTreeNode::from_bytes(&bytes).unwrap();
        assert!(restored.is_leaf);
        assert_eq!(restored.key_count(), 0);
        assert!(restored.keys.is_empty());
        assert!(restored.values.is_empty());
        assert!(restored.children.is_empty());
    }

    #[test]
    fn roundtrip_empty_internal() {
        let node = BTreeNode::new_internal();
        let bytes = node.to_bytes();
        let restored = BTreeNode::from_bytes(&bytes).unwrap();
        assert!(!restored.is_leaf);
        assert_eq!(restored.key_count(), 0);
        assert!(restored.keys.is_empty());
        assert!(restored.values.is_empty());
        assert!(restored.children.is_empty());
    }

    #[test]
    fn roundtrip_leaf_with_data() {
        let node = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20, 30],
            values: vec![(1, 0), (2, 3), (5, 1)],
            children: vec![],
        };
        let bytes = node.to_bytes();
        let restored = BTreeNode::from_bytes(&bytes).unwrap();
        assert!(restored.is_leaf);
        assert_eq!(restored.key_count(), 3);
        assert_eq!(restored.keys, vec![10, 20, 30]);
        assert_eq!(restored.values, vec![(1, 0), (2, 3), (5, 1)]);
        assert!(restored.children.is_empty());
    }

    #[test]
    fn roundtrip_internal_with_data() {
        let node = BTreeNode {
            is_leaf: false,
            keys: vec![30, 60],
            values: vec![],
            children: vec![1, 2, 3],
        };
        let bytes = node.to_bytes();
        let restored = BTreeNode::from_bytes(&bytes).unwrap();
        assert!(!restored.is_leaf);
        assert_eq!(restored.key_count(), 2);
        assert_eq!(restored.keys, vec![30, 60]);
        assert!(restored.values.is_empty());
        assert_eq!(restored.children, vec![1, 2, 3]);
    }

    #[test]
    fn to_bytes_returns_fixed_size() {
        let leaf = BTreeNode::new_leaf();
        assert_eq!(leaf.to_bytes().len(), BTREE_NODE_SIZE);

        let internal = BTreeNode::new_internal();
        assert_eq!(internal.to_bytes().len(), BTREE_NODE_SIZE);
    }

    #[test]
    fn from_bytes_rejects_short_input() {
        let short = [0u8; 100];
        assert!(BTreeNode::from_bytes(&short).is_err());
    }
}
