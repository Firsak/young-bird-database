use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::Write;

use super::btree::BTree;
use super::btree_header::BTreeHeader;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{BTREE_HEADER_BLOCK_SIZE, BTREE_NODE_SIZE};

/// Writes a BTree to a .btree file. Creates or overwrites the file.
///
/// Layout:
/// [BTreeHeader padded to BTREE_HEADER_BLOCK_SIZE][node_0 (8192)][node_1 (8192)]...
///
/// Header lives in a fixed-size reserved block so that node `i` has a
/// predictable offset: `BTREE_HEADER_BLOCK_SIZE + i * BTREE_NODE_SIZE`.
/// Slots in `free_list` are written as zero bytes to skip `BTreeNode::to_bytes`.
///
/// # Arguments
/// * `filename` — path to the .btree file (created if it doesn't exist)
/// * `tree` — in-memory BTree to serialize
pub fn write_btree(filename: &str, tree: &BTree) -> Result<(), DatabaseError> {
    let mut file = match OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(filename)
    {
        Ok(file) => file,
        Err(error) => {
            println!("Error opening or creating the btree file {filename}: {error}");
            return Err(DatabaseError::Io(error));
        }
    };

    let free_list_u64: Vec<u64> = tree.get_free_list().iter().map(|&s| s as u64).collect();
    let free_set: HashSet<u64> = free_list_u64.iter().copied().collect();

    let header = BTreeHeader::new(
        tree.get_root() as u64,
        tree.get_nodes().len() as u64,
        free_list_u64,
    );

    let header_bytes = header.to_bytes();
    if header_bytes.len() > BTREE_HEADER_BLOCK_SIZE {
        return Err(DatabaseError::Serialization(format!(
            "btree header {} bytes exceeds reserved block {}",
            header_bytes.len(),
            BTREE_HEADER_BLOCK_SIZE
        )));
    }

    let mut buffer: Vec<u8> =
        Vec::with_capacity(BTREE_HEADER_BLOCK_SIZE + tree.get_nodes().len() * BTREE_NODE_SIZE);

    // TODO(human): build the byte buffer
    //   1. extend with header_bytes, then pad with zeros up to BTREE_HEADER_BLOCK_SIZE
    //      (resize works: buffer.resize(BTREE_HEADER_BLOCK_SIZE, 0))
    //   2. for each slot index i in 0..tree.get_nodes().len():
    //        if free_set contains i as u64: extend with [0u8; BTREE_NODE_SIZE]
    //        else:                          extend with tree.get_nodes()[i].to_bytes()
    //   3. file.write_all(&buffer)?;

    buffer.extend_from_slice(&header_bytes);
    buffer.resize(BTREE_HEADER_BLOCK_SIZE, 0);

    for index in 0..tree.get_nodes().len() {
        if free_set.contains(&(index as u64)) {
            buffer.extend_from_slice(&[0u8; BTREE_NODE_SIZE]);
        } else {
            buffer.extend_from_slice(&tree.get_nodes()[index].to_bytes());
        }
    }

    file.write_all(&buffer)?;

    Ok(())
}
