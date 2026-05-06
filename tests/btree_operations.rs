use std::fs;

use young_bird_database::database_operations::file_processing::btree::{
    read_btree, write_btree, BTree, BTreeHeader, BTreeNode,
};

/// Helper: unique temp filename per test.
fn temp_btree(test_name: &str) -> String {
    format!("test_btree_{}.btree", test_name)
}

/// Helper: clean up temp file.
fn cleanup(filename: &str) {
    fs::remove_file(filename).ok();
}

/// Helper: assemble a BTree from pieces using the public from_parts API.
fn tree_from(root: u64, nodes: Vec<BTreeNode>, free_list: Vec<u64>) -> BTree {
    let header = BTreeHeader::new(root, nodes.len() as u64, free_list);
    BTree::from_parts(header, nodes)
}

#[test]
fn roundtrip_empty_tree() {
    let filename = temp_btree("empty");

    let tree = BTree::new();
    write_btree(&filename, &tree).unwrap();

    let loaded = read_btree(&filename).unwrap();
    assert_eq!(loaded.get_root(), 0);
    assert_eq!(loaded.get_nodes().len(), 1);
    assert!(loaded.get_free_list().is_empty());
    // Empty leaf — any search misses
    assert_eq!(loaded.search(42), None);

    cleanup(&filename);
}

#[test]
fn roundtrip_two_level_tree_preserves_search() {
    // Layout:
    //           root (keys=[30])
    //          /                \
    //  leaf A = [10, 20, 30]   leaf B = [40, 50]
    let filename = temp_btree("two_level_search");

    let leaf_a = BTreeNode {
        is_leaf: true,
        keys: vec![10, 20, 30],
        values: vec![(1, 0), (2, 0), (3, 0)],
        children: vec![],
    };
    let leaf_b = BTreeNode {
        is_leaf: true,
        keys: vec![40, 50],
        values: vec![(4, 0), (5, 0)],
        children: vec![],
    };
    let root = BTreeNode {
        is_leaf: false,
        keys: vec![30],
        values: vec![],
        children: vec![0, 1],
    };
    let tree = tree_from(2, vec![leaf_a, leaf_b, root], vec![]);
    write_btree(&filename, &tree).unwrap();

    let loaded = read_btree(&filename).unwrap();
    assert_eq!(loaded.get_root(), 2);
    assert_eq!(loaded.get_nodes().len(), 3);
    // Hits across both leaves
    assert_eq!(loaded.search(10), Some((1, 0)));
    assert_eq!(loaded.search(30), Some((3, 0)));
    assert_eq!(loaded.search(40), Some((4, 0)));
    assert_eq!(loaded.search(50), Some((5, 0)));
    // Misses
    assert_eq!(loaded.search(25), None);
    assert_eq!(loaded.search(100), None);

    cleanup(&filename);
}

#[test]
fn roundtrip_preserves_range_scan() {
    // root([30]) → [10,20,30] | [40,50]
    let filename = temp_btree("range_scan");

    let leaf_a = BTreeNode {
        is_leaf: true,
        keys: vec![10, 20, 30],
        values: vec![(10, 1), (20, 2), (30, 3)],
        children: vec![],
    };
    let leaf_b = BTreeNode {
        is_leaf: true,
        keys: vec![40, 50],
        values: vec![(40, 4), (50, 5)],
        children: vec![],
    };
    let root = BTreeNode {
        is_leaf: false,
        keys: vec![30],
        values: vec![],
        children: vec![0, 1],
    };
    let tree = tree_from(2, vec![leaf_a, leaf_b, root], vec![]);
    write_btree(&filename, &tree).unwrap();

    let loaded = read_btree(&filename).unwrap();
    let result = loaded.range_scan(20, 40);
    assert_eq!(result, vec![(20, 2), (30, 3), (40, 4)]);

    cleanup(&filename);
}

#[test]
fn roundtrip_preserves_free_list_slots() {
    // Tree with a freed slot: the middle of the Vec holds a dead node.
    // After roundtrip, the free list must still mark slot 1 as free,
    // and the surviving nodes must remain searchable.
    //
    // Layout (slot 1 is free):
    //   slot 0: leaf = [10, 20]
    //   slot 1: FREE (placeholder on read, zero bytes on disk)
    //   slot 2: leaf = [30, 40]
    //   slot 3: root, children=[0, 2], keys=[20]
    let filename = temp_btree("free_list");

    let leaf_a = BTreeNode {
        is_leaf: true,
        keys: vec![10, 20],
        values: vec![(10, 0), (20, 0)],
        children: vec![],
    };
    let dead_node = BTreeNode::new_leaf(); // placeholder, on disk = zeros
    let leaf_b = BTreeNode {
        is_leaf: true,
        keys: vec![30, 40],
        values: vec![(30, 0), (40, 0)],
        children: vec![],
    };
    let root = BTreeNode {
        is_leaf: false,
        keys: vec![20],
        values: vec![],
        children: vec![0, 2],
    };
    let tree = tree_from(3, vec![leaf_a, dead_node, leaf_b, root], vec![1]);
    write_btree(&filename, &tree).unwrap();

    let loaded = read_btree(&filename).unwrap();
    assert_eq!(loaded.get_root(), 3);
    assert_eq!(loaded.get_nodes().len(), 4);
    assert_eq!(loaded.get_free_list(), &vec![1usize]);
    // Live leaves still reachable through the parent
    for k in [10, 20, 30, 40] {
        assert_eq!(loaded.search(k), Some((k, 0)), "key {} missing", k);
    }

    cleanup(&filename);
}

#[test]
fn free_slot_on_disk_is_zero_bytes() {
    // Writer must write zeros for free slots so the reader can skip
    // BTreeNode::from_bytes (which would fail on zeroed bytes anyway).
    let filename = temp_btree("free_slot_zeros");

    let leaf = BTreeNode {
        is_leaf: true,
        keys: vec![7],
        values: vec![(7, 0)],
        children: vec![],
    };
    let dead = BTreeNode::new_leaf();
    // Root points at slot 0; slot 1 is free.
    let tree = tree_from(0, vec![leaf, dead], vec![1]);
    write_btree(&filename, &tree).unwrap();

    let bytes = fs::read(&filename).unwrap();
    // File length: header block + 2 nodes
    assert_eq!(bytes.len(), 8192 + 2 * 8192);
    // Slot 1 sits at offset 8192 + 8192 = 16384, should be all zeros
    let slot1 = &bytes[16384..16384 + 8192];
    assert!(
        slot1.iter().all(|&b| b == 0),
        "free slot on disk contains non-zero bytes"
    );

    cleanup(&filename);
}

#[test]
fn overwrite_btree_file() {
    let filename = temp_btree("overwrite");

    // Write first tree with one key.
    let leaf1 = BTreeNode {
        is_leaf: true,
        keys: vec![111],
        values: vec![(1, 0)],
        children: vec![],
    };
    let tree1 = tree_from(0, vec![leaf1], vec![]);
    write_btree(&filename, &tree1).unwrap();

    // Overwrite with a different tree.
    let leaf2 = BTreeNode {
        is_leaf: true,
        keys: vec![222],
        values: vec![(2, 0)],
        children: vec![],
    };
    let tree2 = tree_from(0, vec![leaf2], vec![]);
    write_btree(&filename, &tree2).unwrap();

    let loaded = read_btree(&filename).unwrap();
    assert_eq!(loaded.search(111), None);
    assert_eq!(loaded.search(222), Some((2, 0)));

    cleanup(&filename);
}

#[test]
fn read_missing_file_is_error() {
    let result = read_btree("definitely_does_not_exist_btree_test.btree");
    assert!(result.is_err());
}

#[test]
fn read_truncated_file_is_error() {
    // File shorter than the header block — reader must reject.
    let filename = temp_btree("truncated");
    fs::write(&filename, vec![0u8; 100]).unwrap();

    let result = read_btree(&filename);
    assert!(result.is_err());

    cleanup(&filename);
}

#[test]
fn read_file_missing_node_bytes_is_error() {
    // Header claims node_count=2 but file stops after one node.
    let filename = temp_btree("missing_nodes");

    let leaf = BTreeNode {
        is_leaf: true,
        keys: vec![5],
        values: vec![(5, 0)],
        children: vec![],
    };
    // Fake a "2-node" tree but only supply bytes for one.
    let tree = tree_from(0, vec![leaf], vec![]);
    write_btree(&filename, &tree).unwrap();

    // Patch node_count from 1 → 2 in the header block.
    let mut bytes = fs::read(&filename).unwrap();
    bytes[8..16].copy_from_slice(&2u64.to_le_bytes());
    fs::write(&filename, &bytes).unwrap();

    let result = read_btree(&filename);
    assert!(result.is_err());

    cleanup(&filename);
}
