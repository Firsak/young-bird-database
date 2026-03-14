use std::collections::HashMap;

/// In-memory reverse index for overflow text entries.
/// Maps (file_index, offset) → (record_id, column_index),
/// enabling O(1) lookup of which record owns a given overflow entry.
///
/// Rebuilt from page data on `Table::open`, kept in sync by
/// `insert_record`, `delete_record`, and `update_record`.
#[derive(Debug, Clone)]
pub struct OverflowReverseIndex {
    /// (file_index, offset) → (record_id, column_index)
    entries: HashMap<(u32, u64), (u64, u16)>,
}

impl OverflowReverseIndex {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
        }
    }

    /// Registers an overflow entry owned by a specific record and column.
    pub fn insert(&mut self, file_index: u32, offset: u64, record_id: u64, column_index: u16) {
        self.entries
            .insert((file_index, offset), (record_id, column_index));
    }

    /// Removes an overflow entry (called when a record is deleted or its overflow text changes).
    pub fn remove(&mut self, file_index: u32, offset: u64) {
        self.entries.remove(&(file_index, offset));
    }

    /// Returns all entries for a given overflow file.
    /// Result: Vec of (offset, record_id, column_index).
    pub fn get_by_file(&self, file_index: u32) -> Vec<(u64, u64, u16)> {
        self.entries
            .iter()
            .filter(|((fi, _), _)| *fi == file_index)
            .map(|((_, offset), (record_id, col_idx))| (*offset, *record_id, *col_idx))
            .collect()
    }

    /// Remaps an entry's offset after overflow file compaction.
    /// The record_id and column_index stay the same; only the file offset changes.
    pub fn update_offset(&mut self, file_index: u32, old_offset: u64, new_offset: u64) {
        if let Some(value) = self.entries.remove(&(file_index, old_offset)) {
            self.entries.insert((file_index, new_offset), value);
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_is_empty() {
        let idx = OverflowReverseIndex::new();
        assert!(idx.is_empty());
        assert_eq!(idx.len(), 0);
    }

    #[test]
    fn insert_and_get_by_file() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        idx.insert(0, 516, 5, 2);
        idx.insert(1, 16, 8, 1);

        let file0 = idx.get_by_file(0);
        assert_eq!(file0.len(), 2);
        assert!(file0.contains(&(16, 5, 1)));
        assert!(file0.contains(&(516, 5, 2)));

        let file1 = idx.get_by_file(1);
        assert_eq!(file1.len(), 1);
        assert!(file1.contains(&(16, 8, 1)));

        assert_eq!(idx.len(), 3);
    }

    #[test]
    fn remove_entry() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        idx.insert(0, 516, 5, 2);

        idx.remove(0, 16);
        assert_eq!(idx.len(), 1);

        let file0 = idx.get_by_file(0);
        assert_eq!(file0.len(), 1);
        assert!(file0.contains(&(516, 5, 2)));
    }

    #[test]
    fn remove_nonexistent_is_no_op() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        idx.remove(0, 999);
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn update_offset_remaps_key() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        idx.insert(0, 516, 5, 2);

        // Compaction moved entry from offset 516 to 200
        idx.update_offset(0, 516, 200);

        let file0 = idx.get_by_file(0);
        assert_eq!(file0.len(), 2);
        assert!(file0.contains(&(16, 5, 1)));   // unchanged
        assert!(file0.contains(&(200, 5, 2)));   // remapped
    }

    #[test]
    fn update_offset_nonexistent_is_no_op() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        idx.update_offset(0, 999, 200);
        assert_eq!(idx.len(), 1);
        assert!(idx.get_by_file(0).contains(&(16, 5, 1)));
    }

    #[test]
    fn get_by_file_no_matches() {
        let mut idx = OverflowReverseIndex::new();
        idx.insert(0, 16, 5, 1);
        assert_eq!(idx.get_by_file(1).len(), 0);
    }
}
