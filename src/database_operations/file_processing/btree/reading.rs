use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Read;

use super::btree::BTree;
use super::btree_header::BTreeHeader;
use super::btree_node::BTreeNode;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{BTREE_HEADER_BLOCK_SIZE, BTREE_NODE_SIZE};

/// Reads a BTree from a .btree file.
///
/// Layout:
/// [BTreeHeader padded to BTREE_HEADER_BLOCK_SIZE][node_0 (8192)][node_1 (8192)]...
///
/// Header parsing ignores padding bytes after the declared free list.
/// Each node lives at offset `BTREE_HEADER_BLOCK_SIZE + i * BTREE_NODE_SIZE`.
/// Slots in `free_list` hold zero bytes on disk — they get a
/// `BTreeNode::new_leaf()` placeholder (deserialization skipped).
///
/// # Arguments
/// * `filename` — path to the .btree file
pub fn read_btree(filename: &str) -> Result<BTree, DatabaseError> {
    let mut file = match OpenOptions::new().read(true).open(filename) {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening btree file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let mut buffer: Vec<u8> = Vec::new();
    file.read_to_end(&mut buffer)?;

    if buffer.len() < BTREE_HEADER_BLOCK_SIZE {
        return Err(DatabaseError::Serialization(format!(
            "btree file {} bytes smaller than header block {}",
            buffer.len(),
            BTREE_HEADER_BLOCK_SIZE
        )));
    }

    let header = BTreeHeader::from_bytes(&buffer[0..BTREE_HEADER_BLOCK_SIZE])?;
    let free_set: HashSet<u64> = header.get_free_list().iter().copied().collect();
    let node_count = header.get_node_count() as usize;

    let expected_len = BTREE_HEADER_BLOCK_SIZE + node_count * BTREE_NODE_SIZE;
    if buffer.len() < expected_len {
        return Err(DatabaseError::Serialization(format!(
            "btree file {} bytes shorter than expected {} for {} nodes",
            buffer.len(),
            expected_len,
            node_count
        )));
    }

    // TODO: parse nodes
    //   1. let mut nodes: Vec<BTreeNode> = Vec::with_capacity(node_count);
    //   2. for i in 0..node_count:
    //        let offset = BTREE_HEADER_BLOCK_SIZE + i * BTREE_NODE_SIZE;
    //        if free_set contains i as u64: push BTreeNode::new_leaf() (placeholder)
    //        else: push BTreeNode::from_bytes(&buffer[offset..offset + BTREE_NODE_SIZE])?
    //   3. return Ok(BTree::from_parts(header, nodes))
    let mut nodes: Vec<BTreeNode> = Vec::with_capacity(node_count);
    for index in 0..node_count {
        if free_set.contains(&(index as u64)) {
            nodes.push(BTreeNode::new_leaf());
        } else {
            let offset = BTREE_HEADER_BLOCK_SIZE + index * BTREE_NODE_SIZE;
            nodes.push(BTreeNode::from_bytes(
                &buffer[offset..offset + BTREE_NODE_SIZE],
            )?);
        }
    }

    Ok(BTree::from_parts(header, nodes))
}
