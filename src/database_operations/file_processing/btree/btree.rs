// B-tree: in-memory tree structure built from BTreeNode blocks
//
// Each node is indexed by position in a Vec<BTreeNode>. `root` holds the
// index of the current root node. Leaf nodes contain key → value pairs;
// internal nodes contain keys + child indices for routing searches.

use crate::database_operations::file_processing::{
    btree::BTreeHeader, errors::DatabaseError, BTREE_MAX_KEYS_PER_NODE,
};

use super::btree_node::BTreeNode;

#[derive(Debug)]
pub struct BTree {
    nodes: Vec<BTreeNode>,
    root: usize,
    free_list: Vec<usize>,
}

impl Default for BTree {
    fn default() -> Self {
        Self::new()
    }
}

impl BTree {
    pub fn new() -> Self {
        let empty_leaf = BTreeNode::new_leaf();
        Self {
            nodes: vec![empty_leaf],
            root: 0,
            free_list: vec![],
        }
    }

    pub fn get_nodes(&self) -> &Vec<BTreeNode> {
        &self.nodes
    }

    pub fn get_root(&self) -> usize {
        self.root
    }

    pub fn get_free_list(&self) -> &Vec<usize> {
        &self.free_list
    }

    pub fn from_parts(header: BTreeHeader, nodes: Vec<BTreeNode>) -> Self {
        Self {
            nodes,
            root: header.get_root_index() as usize,
            free_list: header
                .get_free_list()
                .iter()
                .map(|value| *value as usize)
                .collect(),
        }
    }

    /// Allocates a slot for a new node, reusing a freed slot if available.
    ///
    /// Pops from `free_list` first (reuses slots freed by merges); falls back
    /// to pushing onto the end of `nodes`.
    ///
    /// # Returns
    /// The index of the slot where the node was placed.
    fn alloc_node(&mut self, node: BTreeNode) -> usize {
        if let Some(slot) = self.free_list.pop() {
            self.nodes[slot] = node;
            slot
        } else {
            self.nodes.push(node);
            self.nodes.len() - 1
        }
    }

    /// Searches for a key in the tree, returning its record location if found.
    ///
    /// Descends from root using `find_child_index` at each internal node,
    /// then scans the leaf's keys for an exact match.
    ///
    /// # Returns
    /// `Some((page_number, slot_index))` if the key exists, `None` otherwise.
    pub fn search(&self, key: u64) -> Option<(u64, u16)> {
        let mut cursor_pos = self.root;
        loop {
            if self.nodes[cursor_pos].is_leaf {
                let value_pos = self.nodes[cursor_pos].keys.iter().position(|k| *k == key);
                match value_pos {
                    None => {
                        return None;
                    }
                    Some(p) => {
                        return Some(self.nodes[cursor_pos].values[p]);
                    }
                }
            } else {
                let pos = self.nodes[cursor_pos].find_child_index(key);
                cursor_pos = self.nodes[cursor_pos].children[pos] as usize;
            }
        }
    }

    /// Returns all entries with keys in the inclusive range `[low, high]`.
    ///
    /// Descends to the leaf containing `low` using a path stack, then scans
    /// forward across leaves collecting matching entries until a key exceeds
    /// `high` or the tree is exhausted.
    ///
    /// # Arguments
    /// * `low` — lower bound (inclusive). Use `u64::MIN` for unbounded start.
    /// * `high` — upper bound (inclusive). Use `u64::MAX` for unbounded end.
    ///
    /// # Returns
    /// A `Vec<(page_number, slot_index)>` of record locations, in key order.
    /// Returns an empty `Vec` if no keys fall within the range.
    pub fn range_scan(&self, low: u64, high: u64) -> Vec<(u64, u16)> {
        let mut node = self.root;
        let mut found_value_pos: Vec<(u64, u16)> = vec![];
        let mut stack: Vec<(usize, usize)> = vec![];

        while !self.nodes[node].is_leaf {
            let pos = self.nodes[node].find_child_index(low);
            stack.push((node, pos));
            node = self.nodes[node].children[pos] as usize;
        }

        let mut search_pos = self.nodes[node].find_child_index(low);
        if search_pos == self.nodes[node].keys.len() {
            return found_value_pos;
        }

        while self.nodes[node].keys[search_pos] >= low && self.nodes[node].keys[search_pos] <= high
        {
            found_value_pos.push(self.nodes[node].values[search_pos]);
            if search_pos < self.nodes[node].keys.len() - 1 {
                search_pos += 1;
            } else {
                let mut no_more_leafs = true;
                while let Some((parent_index, child_pos)) = stack.pop() {
                    if child_pos < self.nodes[parent_index].children.len() - 1 {
                        stack.push((parent_index, child_pos + 1));
                        node = self.nodes[parent_index].children[(child_pos) + 1] as usize;
                        no_more_leafs = false;
                        break;
                    }
                }
                if no_more_leafs {
                    break;
                }
                while !self.nodes[node].is_leaf {
                    stack.push((node, 0));
                    node = self.nodes[node].children[0] as usize;
                }
                search_pos = 0;
            }
        }

        found_value_pos
    }

    /// Deletes a key from the tree, rebalancing nodes as needed.
    ///
    /// Descends to the target leaf using a path stack, removes the key,
    /// then restores the B-tree invariant if the leaf underflows
    /// (fewer than `BTREE_MAX_KEYS_PER_NODE / 2` keys). Tries borrowing
    /// from a sibling first (left, then right); if neither can donate,
    /// merges with a sibling and propagates the underflow upward
    /// through internal nodes. Collapses the root when it drops to
    /// a single child.
    ///
    /// # Arguments
    /// * `key` — the search key to remove
    ///
    /// # Errors
    /// Returns `InvalidArgument` if the key does not exist in any leaf.
    pub fn delete(&mut self, key: u64) -> Result<(), DatabaseError> {
        let mut node = self.root;
        let mut stack: Vec<(usize, usize)> = vec![];
        while !self.nodes[node].is_leaf {
            let pos = self.nodes[node].find_child_index(key);
            stack.push((node, pos));
            node = self.nodes[node].children[pos] as usize;
        }

        self.nodes[node].delete(key)?;

        let current_length = self.nodes[node].keys.len();
        if current_length < (BTREE_MAX_KEYS_PER_NODE / 2) && !stack.is_empty() {
            let (parent_index, child_pos) = stack.pop().unwrap();
            if child_pos > 0
                && self.nodes[self.nodes[parent_index].children[child_pos - 1] as usize]
                    .keys
                    .len()
                    > (BTREE_MAX_KEYS_PER_NODE / 2)
            {
                let previous_child_pos = self.nodes[parent_index].children[child_pos - 1] as usize;
                let previous_leaf = &mut self.nodes[previous_child_pos];
                let key_to_move = previous_leaf.keys.pop().unwrap();
                let value_to_move = previous_leaf.values.pop().unwrap();
                let new_separation_key = *previous_leaf.keys.last().unwrap();
                self.nodes[node].keys.insert(0, key_to_move);
                self.nodes[node].values.insert(0, value_to_move);
                self.nodes[parent_index].keys[child_pos - 1] = new_separation_key;
            } else if child_pos < self.nodes[parent_index].children.len() - 1
                && self.nodes[self.nodes[parent_index].children[child_pos + 1] as usize]
                    .keys
                    .len()
                    > (BTREE_MAX_KEYS_PER_NODE / 2)
            {
                let next_child_pos = self.nodes[parent_index].children[child_pos + 1] as usize;
                let next_leaf = &mut self.nodes[next_child_pos];
                let key_to_move = next_leaf.keys.remove(0);
                let value_to_move = next_leaf.values.remove(0);
                let new_separation_key = key_to_move;
                self.nodes[node].keys.push(key_to_move);
                self.nodes[node].values.push(value_to_move);
                self.nodes[parent_index].keys[child_pos] = new_separation_key;
            } else {
                let mut new_leaf = BTreeNode::new_leaf();
                if child_pos > 0 {
                    let neighbour_node = self.nodes[parent_index].children[child_pos - 1];
                    new_leaf.keys = [
                        self.nodes[neighbour_node as usize].keys.clone(),
                        self.nodes[node].keys.clone(),
                    ]
                    .concat();
                    new_leaf.values = [
                        self.nodes[neighbour_node as usize].values.clone(),
                        self.nodes[node].values.clone(),
                    ]
                    .concat();
                    self.nodes[node] = new_leaf;

                    self.nodes[parent_index].keys.remove(child_pos - 1);
                    self.nodes[parent_index].children.remove(child_pos - 1);
                    self.free_list.push(neighbour_node as usize);
                } else {
                    let neighbour_node = self.nodes[parent_index].children[child_pos + 1];
                    new_leaf.keys = [
                        self.nodes[node].keys.clone(),
                        self.nodes[neighbour_node as usize].keys.clone(),
                    ]
                    .concat();
                    new_leaf.values = [
                        self.nodes[node].values.clone(),
                        self.nodes[neighbour_node as usize].values.clone(),
                    ]
                    .concat();
                    self.nodes[node] = new_leaf;

                    self.nodes[parent_index].keys.remove(child_pos);
                    self.nodes[parent_index].children.remove(child_pos + 1);
                    self.free_list.push(neighbour_node as usize);
                }

                while let Some((parent_index, child_pos)) = stack.pop() {
                    node = self.nodes[parent_index].children[child_pos] as usize;
                    if self.nodes[node].children.len() < (BTREE_MAX_KEYS_PER_NODE / 2) {
                        let mut new_leaf = BTreeNode::new_internal();
                        if child_pos > 0 {
                            let parent_key = self.nodes[parent_index].keys.remove(child_pos - 1);
                            let neighbour_node = self.nodes[parent_index].children[child_pos - 1];
                            new_leaf.keys = [
                                self.nodes[neighbour_node as usize].keys.clone(),
                                vec![parent_key],
                                self.nodes[node].keys.clone(),
                            ]
                            .concat();
                            new_leaf.children = [
                                self.nodes[neighbour_node as usize].children.clone(),
                                self.nodes[node].children.clone(),
                            ]
                            .concat();
                            self.nodes[node] = new_leaf;
                            self.nodes[parent_index].children.remove(child_pos - 1);
                            self.free_list.push(neighbour_node as usize);
                        } else {
                            let parent_key = self.nodes[parent_index].keys.remove(child_pos);
                            let neighbour_node = self.nodes[parent_index].children[child_pos + 1];
                            new_leaf.keys = [
                                self.nodes[node].keys.clone(),
                                vec![parent_key],
                                self.nodes[neighbour_node as usize].keys.clone(),
                            ]
                            .concat();
                            new_leaf.children = [
                                self.nodes[node].children.clone(),
                                self.nodes[neighbour_node as usize].children.clone(),
                            ]
                            .concat();
                            self.nodes[node] = new_leaf;
                            self.nodes[parent_index].children.remove(child_pos + 1);
                            self.free_list.push(neighbour_node as usize);
                        }
                    } else {
                        break;
                    }
                }

                if self.nodes[self.root].children.len() == 1 {
                    let new_root = self.nodes[self.root].children[0];
                    self.free_list.push(self.root);
                    self.root = new_root as usize;
                }
            }
        }

        Ok(())
    }

    /// Inserts a key-value pair into the tree, splitting nodes as needed.
    ///
    /// Descends to the target leaf using a path stack, inserts the key in
    /// sorted order, then splits upward if the node exceeds `BTREE_MAX_KEYS_PER_NODE`.
    /// Leaf splits keep the separator in the left child (B+tree style);
    /// internal splits remove the separator from both children.
    /// Creates a new root if the split propagates past the current root.
    ///
    /// # Arguments
    /// * `key` — the search key to insert
    /// * `value` — record location as (page_number, slot_index)
    pub fn insert(&mut self, key: u64, value: (u64, u16)) -> Result<(), DatabaseError> {
        let mut node = self.root;
        let mut stack: Vec<(usize, usize)> = vec![];
        while !self.nodes[node].is_leaf {
            let pos = self.nodes[node].find_child_index(key);
            stack.push((node, pos));
            node = self.nodes[node].children[pos] as usize;
        }

        self.nodes[node].insert_value(key, value)?;
        let current_length = self.nodes[node].keys.len();
        if current_length > BTREE_MAX_KEYS_PER_NODE {
            let mut new_node = BTreeNode::new_leaf();

            let mid = self.nodes[node].keys.len() / 2;
            let mut separator_key = self.nodes[node].keys[mid];
            new_node.keys = self.nodes[node].keys.split_off(mid + 1);
            new_node.values = self.nodes[node].values.split_off(mid + 1);
            let mut new_node_child_pos = self.alloc_node(new_node);
            let mut new_root_needed = true;

            while let Some((parent_index, child_pos)) = stack.pop() {
                new_root_needed = false;
                self.nodes[parent_index]
                    .keys
                    .insert(child_pos, separator_key);
                self.nodes[parent_index]
                    .children
                    .insert(child_pos + 1, new_node_child_pos as u64);

                if self.nodes[parent_index].keys.len() > BTREE_MAX_KEYS_PER_NODE {
                    new_node = BTreeNode::new_internal();
                    let mid = self.nodes[parent_index].keys.len() / 2;
                    separator_key = self.nodes[parent_index].keys[mid];
                    new_node.keys = self.nodes[parent_index].keys.split_off(mid + 1);
                    self.nodes[parent_index].keys.pop();
                    new_node.children = self.nodes[parent_index].children.split_off(mid + 1);
                    new_node_child_pos = self.alloc_node(new_node);
                    new_root_needed = true;
                } else {
                    break;
                }
            }

            if new_root_needed {
                let mut new_root_node = BTreeNode::new_internal();
                new_root_node.keys = vec![separator_key];
                new_root_node.children = vec![self.root as u64, new_node_child_pos as u64];
                let new_root_node_child_pos = self.alloc_node(new_root_node);
                self.root = new_root_node_child_pos;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn search_empty_tree_returns_none() {
        let tree = BTree::new();
        assert_eq!(tree.search(42), None);
    }

    #[test]
    fn search_single_leaf_hits_and_misses() {
        let leaf = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20, 30],
            values: vec![(1, 0), (2, 5), (3, 9)],
            children: vec![],
        };
        let tree = BTree {
            nodes: vec![leaf],
            root: 0,
            free_list: vec![],
        };

        assert_eq!(tree.search(10), Some((1, 0)));
        assert_eq!(tree.search(20), Some((2, 5)));
        assert_eq!(tree.search(30), Some((3, 9)));
        assert_eq!(tree.search(5), None);
        assert_eq!(tree.search(15), None);
        assert_eq!(tree.search(100), None);
    }

    #[test]
    fn search_two_level_tree() {
        // Layout (Option A: equal goes left, so separator 30 lives in leaf A):
        //           root (keys=[30])
        //          /                \
        //  leaf A = [10, 20, 30]   leaf B = [40, 50]
        //
        // nodes[0] = leaf A
        // nodes[1] = leaf B
        // nodes[2] = root
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
        let tree = BTree {
            nodes: vec![leaf_a, leaf_b, root],
            root: 2,
            free_list: vec![],
        };

        // Hits in left leaf
        assert_eq!(tree.search(10), Some((1, 0)));
        assert_eq!(tree.search(20), Some((2, 0)));
        assert_eq!(tree.search(30), Some((3, 0)));
        // Hits in right leaf
        assert_eq!(tree.search(40), Some((4, 0)));
        assert_eq!(tree.search(50), Some((5, 0)));
        // Misses
        assert_eq!(tree.search(5), None);
        assert_eq!(tree.search(25), None);
        assert_eq!(tree.search(45), None);
        assert_eq!(tree.search(100), None);
    }

    #[test]
    fn search_three_level_tree() {
        // Layout:
        //                       root (keys=[50])
        //                      /                \
        //           inner L (keys=[20])      inner R (keys=[70])
        //          /             \           /            \
        //   leaf 0 = [10]   leaf 1 = [20,30,50]   leaf 2 = [60,70]   leaf 3 = [80,90]
        //
        // nodes layout:
        //   0: leaf 0
        //   1: leaf 1
        //   2: leaf 2
        //   3: leaf 3
        //   4: inner L
        //   5: inner R
        //   6: root
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![10],
            values: vec![(10, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![20, 30, 50],
            values: vec![(20, 0), (30, 0), (50, 0)],
            children: vec![],
        };
        let leaf2 = BTreeNode {
            is_leaf: true,
            keys: vec![60, 70],
            values: vec![(60, 0), (70, 0)],
            children: vec![],
        };
        let leaf3 = BTreeNode {
            is_leaf: true,
            keys: vec![80, 90],
            values: vec![(80, 0), (90, 0)],
            children: vec![],
        };
        let inner_l = BTreeNode {
            is_leaf: false,
            keys: vec![20],
            values: vec![],
            children: vec![0, 1],
        };
        let inner_r = BTreeNode {
            is_leaf: false,
            keys: vec![70],
            values: vec![],
            children: vec![2, 3],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![50],
            values: vec![],
            children: vec![4, 5],
        };
        let tree = BTree {
            nodes: vec![leaf0, leaf1, leaf2, leaf3, inner_l, inner_r, root],
            root: 6,
            free_list: vec![],
        };

        // Hits across all four leaves
        assert_eq!(tree.search(10), Some((10, 0)));
        assert_eq!(tree.search(30), Some((30, 0)));
        assert_eq!(tree.search(50), Some((50, 0)));
        assert_eq!(tree.search(60), Some((60, 0)));
        assert_eq!(tree.search(90), Some((90, 0)));
        // Misses
        assert_eq!(tree.search(1), None);
        assert_eq!(tree.search(25), None);
        assert_eq!(tree.search(55), None);
        assert_eq!(tree.search(100), None);
    }

    // ── Insertion tests ──────────────────────────────────────────

    #[test]
    fn insert_into_empty_tree() {
        let mut tree = BTree::new();
        tree.insert(42, (100, 5)).unwrap();
        assert_eq!(tree.search(42), Some((100, 5)));
    }

    #[test]
    fn insert_three_keys_maintains_order() {
        // Insert 3 keys (under MAX_KEYS=4), no split needed
        let mut tree = BTree::new();
        tree.insert(30, (3, 0)).unwrap();
        tree.insert(10, (1, 0)).unwrap();
        tree.insert(20, (2, 0)).unwrap();

        // All three searchable
        assert_eq!(tree.search(10), Some((1, 0)));
        assert_eq!(tree.search(20), Some((2, 0)));
        assert_eq!(tree.search(30), Some((3, 0)));
        // Keys should be sorted in the leaf
        assert_eq!(tree.nodes[tree.root].keys, vec![10, 20, 30]);
    }

    #[test]
    fn insert_causes_leaf_split() {
        // Insert 5 keys into a single leaf (MAX_KEYS=4)
        // The 5th insert should trigger a split and create a new root
        let mut tree = BTree::new();
        tree.insert(10, (10, 0)).unwrap();
        tree.insert(20, (20, 0)).unwrap();
        tree.insert(30, (30, 0)).unwrap();
        tree.insert(40, (40, 0)).unwrap();
        // 4 keys — still one leaf, no split yet
        assert!(tree.nodes[tree.root].is_leaf);

        // 5th key triggers split
        tree.insert(50, (50, 0)).unwrap();

        // Root should now be internal
        assert!(!tree.nodes[tree.root].is_leaf);
        // All 5 keys still searchable
        assert_eq!(tree.search(10), Some((10, 0)));
        assert_eq!(tree.search(20), Some((20, 0)));
        assert_eq!(tree.search(30), Some((30, 0)));
        assert_eq!(tree.search(40), Some((40, 0)));
        assert_eq!(tree.search(50), Some((50, 0)));
    }

    #[test]
    fn insert_split_preserves_structure() {
        // After splitting [10,20,30,40,50] with mid=2, separator=30:
        //   left leaf:  [10, 20, 30]
        //   right leaf: [40, 50]
        //   root: keys=[30], children=[left, right]
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }

        let root = &tree.nodes[tree.root];
        assert!(!root.is_leaf);
        assert_eq!(root.keys, vec![30]);
        assert_eq!(root.children.len(), 2);

        let left = &tree.nodes[root.children[0] as usize];
        let right = &tree.nodes[root.children[1] as usize];
        assert!(left.is_leaf);
        assert!(right.is_leaf);
        assert_eq!(left.keys, vec![10, 20, 30]);
        assert_eq!(right.keys, vec![40, 50]);
    }

    #[test]
    fn insert_second_split_absorbed_by_parent() {
        // Build a 2-level tree: insert 5 keys to get first split
        // Then insert enough to split the right leaf again
        // The parent (root) should absorb the new separator — no new root
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        // Tree: root(keys=[30]) → [10,20,30] | [40,50]
        let root_before = tree.root;

        // Fill right leaf to 5 keys → triggers second split
        for k in [60, 70, 80] {
            tree.insert(k, (k, 0)).unwrap();
        }

        // Root should be the SAME node (no extra root created)
        assert_eq!(tree.root, root_before);
        // All 8 keys searchable
        for k in [10, 20, 30, 40, 50, 60, 70, 80] {
            assert_eq!(tree.search(k), Some((k, 0)));
        }
    }

    #[test]
    fn insert_into_left_leaf_after_split() {
        // After first split: root([30]) → [10,20,30] | [40,50]
        // Insert into the LEFT child until it splits
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        // Insert 5 and 15 into left leaf → [5,10,15,20,30] → split
        tree.insert(5, (5, 0)).unwrap();
        tree.insert(15, (15, 0)).unwrap();

        // All 7 keys searchable
        for k in [5, 10, 15, 20, 30, 40, 50] {
            assert_eq!(tree.search(k), Some((k, 0)));
        }
        // Root should still be internal with 2 separators now
        let root = &tree.nodes[tree.root];
        assert!(!root.is_leaf);
        assert_eq!(root.keys.len(), 2);
        assert_eq!(root.children.len(), 3);
    }

    #[test]
    fn insert_descending_order() {
        // Descending insertion tests different split positions
        let mut tree = BTree::new();
        for k in [50, 40, 30, 20, 10] {
            tree.insert(k, (k, 0)).unwrap();
        }

        // All searchable
        for k in [10, 20, 30, 40, 50] {
            assert_eq!(tree.search(k), Some((k, 0)));
        }
        // Root should be internal after 5th insert
        assert!(!tree.nodes[tree.root].is_leaf);
    }

    #[test]
    fn insert_triggers_root_split() {
        // Insert 17 ascending keys (10,20,...,170)
        // This fills the root to 5 keys and triggers an internal split
        // creating a 3-level tree
        let mut tree = BTree::new();
        for i in 1..=17 {
            tree.insert(i * 10, (i * 10, 0)).unwrap();
        }

        // Root should be internal
        let root = &tree.nodes[tree.root];
        assert!(!root.is_leaf);
        // Root should have exactly 1 key (result of internal split)
        assert_eq!(root.keys.len(), 1);
        // Root should have 2 internal children
        assert_eq!(root.children.len(), 2);
        let left_internal = &tree.nodes[root.children[0] as usize];
        let right_internal = &tree.nodes[root.children[1] as usize];
        assert!(!left_internal.is_leaf);
        assert!(!right_internal.is_leaf);

        // All 17 keys searchable
        for i in 1..=17 {
            assert_eq!(tree.search(i * 10), Some((i * 10, 0)));
        }
    }

    #[test]
    fn insert_mixed_order_stress() {
        // Insert 20 keys in non-sequential order
        let mut tree = BTree::new();
        let keys = [
            50, 20, 80, 10, 60, 30, 90, 40, 70, 100, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95,
        ];
        for &k in &keys {
            tree.insert(k, (k, 0)).unwrap();
        }

        // All 20 keys searchable
        for &k in &keys {
            assert_eq!(tree.search(k), Some((k, 0)));
        }
        // Miss
        assert_eq!(tree.search(999), None);
    }

    // ── Deletion tests ──────────────────────────────────────────

    #[test]
    fn delete_from_single_leaf_root() {
        // Single-leaf root with 3 keys. Delete middle key.
        let mut tree = BTree::new();
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }
        tree.delete(20).unwrap();

        assert_eq!(tree.search(20), None);
        assert_eq!(tree.search(10), Some((10, 0)));
        assert_eq!(tree.search(30), Some((30, 0)));
    }

    #[test]
    fn delete_single_leaf_root_underflow_is_allowed() {
        // Single-leaf root may underflow — stack is empty, so no rebalancing needed.
        let mut tree = BTree::new();
        tree.insert(10, (10, 0)).unwrap();
        tree.insert(20, (20, 0)).unwrap();
        tree.delete(10).unwrap();

        assert_eq!(tree.search(10), None);
        assert_eq!(tree.search(20), Some((20, 0)));
    }

    #[test]
    fn delete_no_underflow_multi_leaf() {
        // After 5 inserts: root([30]) → [10,20,30] | [40,50]
        // Delete 10 → L0=[20,30] (=MIN, no underflow).
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        tree.delete(10).unwrap();

        assert_eq!(tree.search(10), None);
        for k in [20, 30, 40, 50] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
    }

    #[test]
    fn delete_borrow_from_left_sibling() {
        // root([25]) → L0=[5,10,20] (3 keys) | L1=[30,40] (2 keys)
        // Delete 40 → L1=[30] underflows. Left sibling has 3 > MIN → borrow.
        // Expected: L0=[5,10], L1=[20,30], root.keys=[10].
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![5, 10, 20],
            values: vec![(5, 0), (10, 0), (20, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 40],
            values: vec![(30, 0), (40, 0)],
            children: vec![],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![25],
            values: vec![],
            children: vec![0, 1],
        };
        let mut tree = BTree {
            nodes: vec![leaf0, leaf1, root],
            root: 2,
            free_list: vec![],
        };

        tree.delete(40).unwrap();

        assert_eq!(tree.search(40), None);
        for k in [5, 10, 20, 30] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
    }

    #[test]
    fn delete_borrow_from_right_sibling() {
        // root([25]) → L0=[10,20] (2 keys) | L1=[30,40,50] (3 keys)
        // Delete 20 → L0=[10] underflows. Right sibling has 3 > MIN → borrow.
        // Expected: L0=[10,30], L1=[40,50], root.keys=[30].
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20],
            values: vec![(10, 0), (20, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 40, 50],
            values: vec![(30, 0), (40, 0), (50, 0)],
            children: vec![],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![25],
            values: vec![],
            children: vec![0, 1],
        };
        let mut tree = BTree {
            nodes: vec![leaf0, leaf1, root],
            root: 2,
            free_list: vec![],
        };

        tree.delete(20).unwrap();

        assert_eq!(tree.search(20), None);
        for k in [10, 30, 40, 50] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
    }

    #[test]
    fn delete_merge_with_left_sibling() {
        // Convention: separator = max of its left subtree.
        // root([20, 50]) → L0=[10,20] | L1=[30,50] | L2=[60,70]  (all at MIN)
        // Delete 30 → L1=[50] underflows. Neither sibling can donate.
        // Merge L1 with L0 (child_pos=1 > 0). Expected: parent has 1 key, 2 children,
        // merged leaf holds [10,20,50], all remaining keys searchable.
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20],
            values: vec![(10, 0), (20, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 50],
            values: vec![(30, 0), (50, 0)],
            children: vec![],
        };
        let leaf2 = BTreeNode {
            is_leaf: true,
            keys: vec![60, 70],
            values: vec![(60, 0), (70, 0)],
            children: vec![],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![20, 50],
            values: vec![],
            children: vec![0, 1, 2],
        };
        let mut tree = BTree {
            nodes: vec![leaf0, leaf1, leaf2, root],
            root: 3,
            free_list: vec![],
        };

        tree.delete(30).unwrap();

        assert_eq!(tree.search(30), None);
        for k in [10, 20, 50, 60, 70] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
    }

    #[test]
    fn delete_merge_with_right_sibling() {
        // Convention: separator = max of its left subtree.
        // root([15, 40]) → L0=[10,15] | L1=[30,40] | L2=[50,60]  (all at MIN)
        // Delete 10 → L0=[15] underflows. No left sibling; L1 at MIN can't donate.
        // Merge L0 with L1 (child_pos=0). Expected: merged leaf = [15,30,40],
        // parent has 1 key, 2 children, all remaining keys searchable.
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![10, 15],
            values: vec![(10, 0), (15, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 40],
            values: vec![(30, 0), (40, 0)],
            children: vec![],
        };
        let leaf2 = BTreeNode {
            is_leaf: true,
            keys: vec![50, 60],
            values: vec![(50, 0), (60, 0)],
            children: vec![],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![15, 40],
            values: vec![],
            children: vec![0, 1, 2],
        };
        let mut tree = BTree {
            nodes: vec![leaf0, leaf1, leaf2, root],
            root: 3,
            free_list: vec![],
        };

        tree.delete(10).unwrap();

        assert_eq!(tree.search(10), None);
        for k in [15, 30, 40, 50, 60] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
    }

    #[test]
    fn delete_merge_collapses_root_to_leaf() {
        // 2-level tree where merge deletes the only root separator.
        // root([30]) → L0=[10,20] | L1=[30,40]  (both at MIN)
        // Delete 40 → L1=[30] underflows. L0 can't donate → merge.
        // Expected: merged leaf [10,20,30] becomes the new root (leaf), tree height shrinks.
        let leaf0 = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20],
            values: vec![(10, 0), (20, 0)],
            children: vec![],
        };
        let leaf1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 40],
            values: vec![(30, 0), (40, 0)],
            children: vec![],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![30],
            values: vec![],
            children: vec![0, 1],
        };
        let mut tree = BTree {
            nodes: vec![leaf0, leaf1, root],
            root: 2,
            free_list: vec![],
        };

        tree.delete(40).unwrap();

        assert_eq!(tree.search(40), None);
        for k in [10, 20, 30] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
        // After collapse, the new root should be a leaf.
        assert!(tree.nodes[tree.root].is_leaf);
    }

    #[test]
    fn delete_insert_delete_roundtrip() {
        // Build tree via inserts, delete everything, verify empty, reinsert.
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        // Delete all 5 keys
        for k in [30, 10, 50, 20, 40] {
            tree.delete(k).unwrap();
        }
        for k in [10, 20, 30, 40, 50] {
            assert_eq!(tree.search(k), None, "key {} should be gone", k);
        }
        // Reinsert 3 keys — tree should still work
        for k in [100, 200, 300] {
            tree.insert(k, (k, 1)).unwrap();
        }
        for k in [100, 200, 300] {
            assert_eq!(tree.search(k), Some((k, 1)), "key {} after reinsert", k);
        }
    }

    #[test]
    fn delete_all_from_two_level_tree() {
        // Build a 2-level tree (5 keys → split), then delete all keys one by one.
        // Tests merge + root collapse happening multiple times.
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        assert!(!tree.nodes[tree.root].is_leaf);

        // Delete in an order that exercises different merge/borrow paths
        for k in [30, 50, 10, 40, 20] {
            tree.delete(k).unwrap();
            assert_eq!(tree.search(k), None, "key {} still found after delete", k);
        }
        // Tree should be a single empty leaf
        assert!(tree.nodes[tree.root].is_leaf);
        assert_eq!(tree.nodes[tree.root].keys.len(), 0);
    }

    #[test]
    fn delete_from_three_level_tree_causes_internal_merge() {
        // Build a 3-level tree with 17 ascending keys (proven by insert_triggers_root_split).
        // Then delete enough keys from one side to cause leaf merge → internal underflow → internal merge.
        let mut tree = BTree::new();
        for i in 1..=17 {
            tree.insert(i * 10, (i * 10, 0)).unwrap();
        }

        // Verify 3 levels: root is internal, root's children are internal
        let root = &tree.nodes[tree.root];
        assert!(!root.is_leaf);
        let left_internal = &tree.nodes[root.children[0] as usize];
        assert!(!left_internal.is_leaf);

        // Delete keys from the leftmost leaves to force underflows upward
        for k in [10, 20, 30, 40, 50] {
            tree.delete(k).unwrap();
        }

        // All deleted keys should be gone
        for k in [10, 20, 30, 40, 50] {
            assert_eq!(tree.search(k), None, "key {} still found", k);
        }
        // All surviving keys must still be reachable
        for i in 6..=17 {
            assert_eq!(
                tree.search(i * 10),
                Some((i * 10, 0)),
                "key {} missing",
                i * 10
            );
        }
    }

    #[test]
    fn delete_mixed_order_stress() {
        // Insert 20 keys, delete 15 of them in a scrambled order, verify survivors.
        let mut tree = BTree::new();
        let all_keys: Vec<u64> = vec![
            50, 20, 80, 10, 60, 30, 90, 40, 70, 100, 5, 15, 25, 35, 45, 55, 65, 75, 85, 95,
        ];
        for &k in &all_keys {
            tree.insert(k, (k, 0)).unwrap();
        }

        let delete_keys: Vec<u64> = vec![50, 10, 90, 25, 75, 5, 60, 35, 85, 15, 70, 45, 95, 30, 80];
        let survive_keys: Vec<u64> = vec![20, 40, 55, 65, 100];

        for &k in &delete_keys {
            tree.delete(k).unwrap();
            assert_eq!(tree.search(k), None, "key {} still found after delete", k);
        }
        for &k in &survive_keys {
            assert_eq!(tree.search(k), Some((k, 0)), "surviving key {} missing", k);
        }
    }

    #[test]
    fn delete_freed_slots_reused_by_insert() {
        // After deletes cause merges, free_list should have entries.
        // Subsequent inserts that trigger splits should reuse those slots.
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50, 60, 70, 80] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let nodes_before = tree.nodes.len();

        // Delete enough to cause merges (frees slots)
        for k in [10, 20, 30] {
            tree.delete(k).unwrap();
        }
        assert!(!tree.free_list.is_empty(), "merges should have freed slots");

        // Re-insert keys that trigger splits — should reuse freed slots
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }

        // All keys searchable
        for k in [10, 20, 30, 40, 50, 60, 70, 80] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {}", k);
        }
        // Vec should not have grown much — slots were reused
        assert!(
            tree.nodes.len() <= nodes_before + 1,
            "nodes grew from {} to {} — free slots not reused?",
            nodes_before,
            tree.nodes.len()
        );
    }

    #[test]
    fn delete_nonexistent_key_does_not_corrupt() {
        // Deleting a key that doesn't exist should not remove a different key.
        let mut tree = BTree::new();
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }
        // Key 15 doesn't exist in [10, 20, 30]
        let _ = tree.delete(15);

        // All original keys must still be present
        for k in [10, 20, 30] {
            assert_eq!(
                tree.search(k),
                Some((k, 0)),
                "key {} corrupted after deleting nonexistent key",
                k
            );
        }
    }

    #[test]
    fn delete_forces_internal_node_rebalancing() {
        // Hand-built 3-level tree where all nodes are at minimum occupancy.
        // Convention: separator = max of left subtree.
        //
        //              root (keys=[40])           ← 1 key (MIN for internal)
        //             /                \
        //    inner_L (keys=[20])    inner_R (keys=[60])   ← both at MIN
        //    /        \              /        \
        //  L0=[10,20] L1=[30,40]  L2=[50,60] L3=[70,80]  ← all at MIN
        //
        // Delete sequence: 10, 40, 30.
        //   delete(10) → L0 underflows → merge with L1 → inner_L drops to 0 keys/1 child
        //                 (internal merge SHOULD fire to fix inner_L)
        //   delete(40) → merged leaf [20,30,40]→[20,30], no underflow
        //   delete(30) → merged leaf [20,30]→[20], underflow → needs sibling under inner_L
        //                 if inner_L wasn't fixed, it has only 1 child → no sibling → panic
        let l0 = BTreeNode {
            is_leaf: true,
            keys: vec![10, 20],
            values: vec![(10, 0), (20, 0)],
            children: vec![],
        };
        let l1 = BTreeNode {
            is_leaf: true,
            keys: vec![30, 40],
            values: vec![(30, 0), (40, 0)],
            children: vec![],
        };
        let l2 = BTreeNode {
            is_leaf: true,
            keys: vec![50, 60],
            values: vec![(50, 0), (60, 0)],
            children: vec![],
        };
        let l3 = BTreeNode {
            is_leaf: true,
            keys: vec![70, 80],
            values: vec![(70, 0), (80, 0)],
            children: vec![],
        };
        let inner_l = BTreeNode {
            is_leaf: false,
            keys: vec![20],
            values: vec![],
            children: vec![0, 1],
        };
        let inner_r = BTreeNode {
            is_leaf: false,
            keys: vec![60],
            values: vec![],
            children: vec![2, 3],
        };
        let root = BTreeNode {
            is_leaf: false,
            keys: vec![40],
            values: vec![],
            children: vec![4, 5],
        };
        let mut tree = BTree {
            nodes: vec![l0, l1, l2, l3, inner_l, inner_r, root],
            root: 6,
            free_list: vec![],
        };

        // First delete: triggers leaf merge, should also trigger internal rebalancing
        tree.delete(10).unwrap();
        // Second delete: reduces merged leaf, no underflow
        tree.delete(40).unwrap();
        // Third delete: merged leaf underflows again — needs a valid parent structure
        tree.delete(30).unwrap();

        // All deleted keys gone
        for k in [10, 30, 40] {
            assert_eq!(tree.search(k), None, "key {} still found", k);
        }
        // All surviving keys reachable
        for k in [20, 50, 60, 70, 80] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {} missing", k);
        }
        // Structural: every internal node must have children.len() == keys.len() + 1
        for (i, node) in tree.nodes.iter().enumerate() {
            if tree.free_list.contains(&i) {
                continue;
            }
            if !node.is_leaf && node.key_count() > 0 {
                assert_eq!(
                    node.children.len(),
                    node.keys.len() + 1,
                    "internal node {} has {} keys but {} children (expected {})",
                    i,
                    node.keys.len(),
                    node.children.len(),
                    node.keys.len() + 1
                );
            }
        }
    }

    // ── Range scan tests ──────────────────────────────────────

    #[test]
    fn range_scan_empty_tree() {
        let tree = BTree::new();
        let result = tree.range_scan(0, 100);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn range_scan_single_leaf_all_in_range() {
        let mut tree = BTree::new();
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(10, 30);
        assert_eq!(result, vec![(10, 0), (20, 0), (30, 0)]);
    }

    #[test]
    fn range_scan_single_leaf_partial() {
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40] {
            tree.insert(k, (k, 0)).unwrap();
        }
        // 15 and 35 don't exist — only 20 and 30 are in [15, 35]
        let result = tree.range_scan(15, 35);
        assert_eq!(result, vec![(20, 0), (30, 0)]);
    }

    #[test]
    fn range_scan_no_matches() {
        let mut tree = BTree::new();
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(50, 100);
        assert_eq!(result, vec![]);
    }

    #[test]
    fn range_scan_single_key_match() {
        // low == high, exact match on existing key
        let mut tree = BTree::new();
        for k in [10, 20, 30] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(20, 20);
        assert_eq!(result, vec![(20, 0)]);
    }

    #[test]
    fn range_scan_spans_two_leaves() {
        // After 5 inserts: root([30]) → [10,20,30] | [40,50]
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(20, 40);
        assert_eq!(result, vec![(20, 0), (30, 0), (40, 0)]);
    }

    #[test]
    fn range_scan_full_range() {
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(u64::MIN, u64::MAX);
        assert_eq!(result, vec![(10, 0), (20, 0), (30, 0), (40, 0), (50, 0)]);
    }

    #[test]
    fn range_scan_three_level_tree() {
        // 17 keys → 3-level tree
        let mut tree = BTree::new();
        for i in 1..=17 {
            tree.insert(i * 10, (i * 10, 0)).unwrap();
        }
        // Range [50, 120] should return 50,60,70,80,90,100,110,120
        let result = tree.range_scan(50, 120);
        let expected: Vec<(u64, u16)> = (5..=12).map(|i| (i * 10, 0)).collect();
        assert_eq!(result, expected);
    }

    #[test]
    fn range_scan_last_key_in_leaf_included() {
        // Tree: root([30]) → [10,20,30] | [40,50]
        // Range [25,35] should include 30 (last key in left leaf within range)
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40, 50] {
            tree.insert(k, (k, 0)).unwrap();
        }
        let result = tree.range_scan(25, 35);
        assert_eq!(result, vec![(30, 0)]);
    }

    #[test]
    fn range_scan_returns_values_not_positions() {
        // Values are (page_number, slot_index), NOT (node_index, position)
        let mut tree = BTree::new();
        tree.insert(10, (100, 5)).unwrap();
        tree.insert(20, (200, 3)).unwrap();
        tree.insert(30, (300, 7)).unwrap();
        let result = tree.range_scan(10, 30);
        assert_eq!(result, vec![(100, 5), (200, 3), (300, 7)]);
    }

    #[test]
    fn insert_split_reuses_free_slot() {
        // Seed a tree with 4 keys in a single leaf (no split yet)
        let mut tree = BTree::new();
        for k in [10, 20, 30, 40] {
            tree.insert(k, (k, 0)).unwrap();
        }
        assert!(tree.nodes[tree.root].is_leaf);

        // Manually simulate the state delete would leave behind:
        // push two dummy nodes and mark the MIDDLE one (slot 1) as free.
        // The tail dummy at slot 2 ensures `self.nodes.len() - 1` is WRONG.
        tree.nodes.push(BTreeNode::new_leaf()); // slot 1 — about to be "freed"
        tree.nodes.push(BTreeNode::new_leaf()); // slot 2 — stale tail
        tree.free_list.push(1);

        // 5th insert triggers split.
        // alloc_node reuses slot 1 (the free slot) for the new right half.
        // The insert path must use alloc_node's return value, NOT
        // self.nodes.len() - 1, when wiring the new root's child pointer.
        tree.insert(50, (50, 0)).unwrap();

        // All 5 keys must still be reachable through the new root.
        // If the right child pointer were computed as self.nodes.len() - 1,
        // it would point to the stale tail dummy at slot 2 and search(40/50)
        // would return None.
        for k in [10, 20, 30, 40, 50] {
            assert_eq!(tree.search(k), Some((k, 0)), "key {} not found", k);
        }
        // Root's right child must point to slot 1 (the reused slot),
        // not to the stale tail dummy at slot 2.
        let root = &tree.nodes[tree.root];
        assert_eq!(root.children[1], 1);
    }

    #[test]
    fn delete_ascending_after_ascending_inserts_no_panic() {
        // Insert 1..=40 then delete 1..=40 in the same order.
        // Triggers cascading left-side leaf and internal merges as the leftmost
        // path of the tree keeps underflowing. Pre-fix this panicked with
        // "subtract with overflow" on `key_count -= 1` because keys.len() and
        // key_count diverged during internal-merge propagation.
        let mut tree = BTree::new();
        for k in 1u64..=40 {
            tree.insert(k, (k, 0)).unwrap();
        }
        for k in 1u64..=40 {
            tree.delete(k).unwrap();
            assert_eq!(tree.search(k), None, "key {} still found", k);
        }
        // After all deletes, every surviving slot's keys.len() must equal key_count.
        for (i, node) in tree.nodes.iter().enumerate() {
            if tree.free_list.contains(&i) {
                continue;
            }
            assert_eq!(
                node.keys.len() as u16,
                node.key_count(),
                "node {} keys.len/key_count diverged",
                i
            );
        }
    }
}
