use super::index_entry::{BucketStatus, IndexEntry};
use super::index_header::IndexHeader;
use crate::database_operations::file_processing::errors::DatabaseError;

/// In-memory hash table using open addressing with linear probing.
/// Loaded from / flushed to a `.idx` file.
pub struct HashIndex {
    header: IndexHeader,
    buckets: Vec<IndexEntry>,
}

impl HashIndex {
    /// Creates a new empty hash index with the given number of buckets.
    /// All slots are initialized to Empty.
    pub fn new(initial_bucket_count: u64) -> Self {
        let mut buckets = Vec::with_capacity(initial_bucket_count as usize);
        for _ in 0..initial_bucket_count {
            buckets.push(IndexEntry::empty());
        }
        HashIndex {
            header: IndexHeader::new(initial_bucket_count, 0),
            buckets,
        }
    }

    /// Constructs a HashIndex from an existing header and buckets.
    /// Used by read_index to reconstruct from file data.
    pub fn from_parts(header: IndexHeader, buckets: Vec<IndexEntry>) -> Self {
        HashIndex { header, buckets }
    }

    pub fn get_header(&self) -> &IndexHeader {
        &self.header
    }

    pub fn get_buckets(&self) -> &Vec<IndexEntry> {
        &self.buckets
    }

    /// Computes the starting bucket index for a given record_id.
    fn bucket_index(&self, record_id: u64) -> usize {
        (record_id % self.header.get_bucket_count()) as usize
    }

    /// Returns the current load factor: entry_count / bucket_count.
    pub fn load_factor(&self) -> f64 {
        self.header.get_entry_count() as f64 / self.header.get_bucket_count() as f64
    }

    pub fn rehash(&mut self) {
        let saved_buckets = self.buckets.clone();
        let new_buckets_count = self.header.get_bucket_count() * 2;
        let mut new_buckets = Vec::with_capacity(new_buckets_count as usize);
        for _ in 0..new_buckets_count {
            new_buckets.push(IndexEntry::empty());
        }
        for bucket in saved_buckets.iter() {
            if bucket.is_occupied() {
                let mut new_index_pos = (bucket.get_record_id() % new_buckets_count) as usize;
                let new_index = IndexEntry::new(
                    bucket.get_record_id(),
                    bucket.get_page_number(),
                    bucket.get_slot_index(),
                    BucketStatus::Occupied,
                );
                while !new_buckets[new_index_pos].is_empty() {
                    new_index_pos = (new_index_pos + 1) % new_buckets_count as usize;
                }
                new_buckets[new_index_pos] = new_index;
            }
        }
        self.buckets = new_buckets;
        self.header = IndexHeader::new(new_buckets_count, self.header.get_entry_count());
    }

    fn next_bucket(&self, bucket_index: usize) -> usize {
        (bucket_index + 1) % self.buckets.len()
    }

    pub fn insert_entry(
        &mut self,
        record_id: u64,
        page_number: u64,
        slot_index: u16,
    ) -> Result<(), DatabaseError> {
        let current_load_factor = self.load_factor();
        let future_load_factor = current_load_factor + 1.0 / self.header.get_bucket_count() as f64;

        if future_load_factor >= 0.75 {
            self.rehash();
        }

        let mut bucket_position_slot = self.bucket_index(record_id);
        while self.buckets[bucket_position_slot].is_occupied() {
            if self.buckets[bucket_position_slot].get_record_id() == record_id {
                return Err(DatabaseError::InvalidArgument(format!(
                    "Duplicate record_id {}",
                    record_id
                )));
            }
            bucket_position_slot = self.next_bucket(bucket_position_slot);
        }

        let new_index = IndexEntry::new(record_id, page_number, slot_index, BucketStatus::Occupied);
        self.buckets[bucket_position_slot] = new_index;

        let entry_counts = self.header.get_entry_count();
        self.header.update_entry_count(entry_counts + 1);

        Ok(())
    }

    pub fn lookup(&self, record_id: u64) -> Option<(u64, u16)> {
        let mut bucket_position_slot = self.bucket_index(record_id);
        for _ in 0..self.buckets.len() {
            let current_bucket = &self.buckets[bucket_position_slot];
            if current_bucket.is_empty() {
                return None;
            } else if current_bucket.is_tombstone() {
            } else if current_bucket.get_record_id() == record_id {
                return Some((
                    current_bucket.get_page_number(),
                    current_bucket.get_slot_index(),
                ));
            }
            bucket_position_slot = self.next_bucket(bucket_position_slot);
        }
        None
    }

    pub fn remove_entry(&mut self, record_id: u64) -> Result<(), DatabaseError> {
        let mut bucket_position_slot = self.bucket_index(record_id);
        for _ in 0..self.buckets.len() {
            let current_bucket = &mut self.buckets[bucket_position_slot];
            if current_bucket.is_empty() {
                return Err(DatabaseError::RecordNotFound(record_id));
            } else if current_bucket.is_tombstone() {
            } else if current_bucket.get_record_id() == record_id {
                current_bucket.set_status(BucketStatus::Tombstone);
                self.header
                    .update_entry_count(self.header.get_entry_count() - 1);
                return Ok(());
            }
            bucket_position_slot = self.next_bucket(bucket_position_slot);
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }

    pub fn update_entry(
        &mut self,
        record_id: u64,
        new_page_number: u64,
        new_slot_index: u16,
    ) -> Result<(), DatabaseError> {
        let mut bucket_position_slot = self.bucket_index(record_id);
        for _ in 0..self.buckets.len() {
            let current_bucket = &mut self.buckets[bucket_position_slot];
            if current_bucket.is_empty() {
                return Err(DatabaseError::RecordNotFound(record_id));
            } else if current_bucket.is_tombstone() {
            } else if current_bucket.get_record_id() == record_id {
                let new_entry = IndexEntry::new(
                    record_id,
                    new_page_number,
                    new_slot_index,
                    BucketStatus::Occupied,
                );
                self.buckets[bucket_position_slot] = new_entry;
                return Ok(());
            }
            bucket_position_slot = self.next_bucket(bucket_position_slot);
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_and_lookup_single() {
        let mut index = HashIndex::new(8);
        index.insert_entry(42, 3, 5).unwrap();
        let result = index.lookup(42);
        assert_eq!(result, Some((3, 5)));
    }

    #[test]
    fn insert_and_lookup_multiple() {
        let mut index = HashIndex::new(16);
        index.insert_entry(1, 0, 0).unwrap();
        index.insert_entry(2, 0, 1).unwrap();
        index.insert_entry(3, 1, 0).unwrap();
        assert_eq!(index.lookup(1), Some((0, 0)));
        assert_eq!(index.lookup(2), Some((0, 1)));
        assert_eq!(index.lookup(3), Some((1, 0)));
    }

    #[test]
    fn lookup_nonexistent() {
        let mut index = HashIndex::new(8);
        index.insert_entry(1, 0, 0).unwrap();
        assert_eq!(index.lookup(99), None);
    }

    #[test]
    fn remove_and_lookup() {
        let mut index = HashIndex::new(8);
        index.insert_entry(1, 0, 0).unwrap();
        index.insert_entry(2, 0, 1).unwrap();
        index.remove_entry(1).unwrap();
        assert_eq!(index.lookup(1), None);
        assert_eq!(index.lookup(2), Some((0, 1)));
    }

    #[test]
    fn remove_nonexistent() {
        let mut index = HashIndex::new(8);
        assert!(index.remove_entry(99).is_err());
    }

    #[test]
    fn remove_decrements_entry_count() {
        let mut index = HashIndex::new(8);
        index.insert_entry(1, 0, 0).unwrap();
        index.insert_entry(2, 0, 1).unwrap();
        assert_eq!(index.get_header().get_entry_count(), 2);
        index.remove_entry(1).unwrap();
        assert_eq!(index.get_header().get_entry_count(), 1);
    }

    #[test]
    fn insert_into_empty_index() {
        let mut index = HashIndex::new(4);
        assert_eq!(index.get_header().get_entry_count(), 0);
        index.insert_entry(1, 0, 0).unwrap();
        assert_eq!(index.get_header().get_entry_count(), 1);
        assert_eq!(index.lookup(1), Some((0, 0)));
    }

    #[test]
    fn insert_triggers_rehash() {
        let mut index = HashIndex::new(4);
        // Load factor threshold: 0.75 → 3 entries in 4 buckets triggers rehash
        index.insert_entry(1, 0, 0).unwrap();
        index.insert_entry(2, 0, 1).unwrap();
        index.insert_entry(3, 0, 2).unwrap();
        assert_eq!(index.get_header().get_entry_count(), 3);
        // Should have rehashed — bucket count doubled
        assert!(index.get_header().get_bucket_count() > 4);
        // All entries should still be findable after rehash
        assert_eq!(index.lookup(1), Some((0, 0)));
        assert_eq!(index.lookup(2), Some((0, 1)));
        assert_eq!(index.lookup(3), Some((0, 2)));
    }

    #[test]
    fn rehash_preserves_entries() {
        let mut index = HashIndex::new(4);
        for i in 0..10u64 {
            index.insert_entry(i, i / 3, (i % 3) as u16).unwrap();
        }
        // Multiple rehashes should have happened (4 → 8 → 16)
        assert!(index.get_header().get_bucket_count() >= 16);
        // Every entry must be retrievable
        for i in 0..10u64 {
            assert_eq!(index.lookup(i), Some((i / 3, (i % 3) as u16)));
        }
    }

    #[test]
    fn tombstone_does_not_break_chain() {
        // Two keys that hash to the same bucket
        let mut index = HashIndex::new(8);
        // record_id 3 and 11 both hash to bucket 3 (mod 8)
        index.insert_entry(3, 0, 0).unwrap();
        index.insert_entry(11, 1, 0).unwrap(); // probes to bucket 4
                                               // Remove 3 (creates tombstone at bucket 3)
        index.remove_entry(3).unwrap();
        // 11 should still be findable — probe must skip past the tombstone
        assert_eq!(index.lookup(11), Some((1, 0)));
    }

    #[test]
    fn update_entry_changes_location() {
        let mut index = HashIndex::new(8);
        index.insert_entry(42, 0, 5).unwrap();
        index.update_entry(42, 3, 10).unwrap();
        assert_eq!(index.lookup(42), Some((3, 10)));
    }

    #[test]
    fn update_nonexistent_returns_error() {
        let mut index = HashIndex::new(8);
        assert!(index.update_entry(99, 0, 0).is_err());
    }

    #[test]
    fn insert_duplicate_returns_error() {
        let mut index = HashIndex::new(8);
        index.insert_entry(1, 0, 0).unwrap();
        let result = index.insert_entry(1, 1, 1);
        assert!(result.is_err(), "Inserting duplicate record_id should fail");
    }

    #[test]
    fn load_factor_calculation() {
        let mut index = HashIndex::new(8);
        assert_eq!(index.load_factor(), 0.0);
        index.insert_entry(1, 0, 0).unwrap();
        assert!((index.load_factor() - 0.125).abs() < f64::EPSILON);
        index.insert_entry(2, 0, 1).unwrap();
        assert!((index.load_factor() - 0.25).abs() < f64::EPSILON);
    }
}
