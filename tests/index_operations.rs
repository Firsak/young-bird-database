use std::fs;

use young_bird_database::database_operations::file_processing::index::{
    reading::read_index, writing::write_index, HashIndex,
};

/// Helper: unique temp filename per test.
fn temp_idx(test_name: &str) -> String {
    format!("test_index_{}.idx", test_name)
}

/// Helper: clean up temp file.
fn cleanup(filename: &str) {
    fs::remove_file(filename).ok();
}

#[test]
fn write_and_read_empty_index() {
    let filename = temp_idx("empty");
    let index = HashIndex::new(8);
    write_index(&filename, &index).unwrap();

    let loaded = read_index(&filename).unwrap();
    assert_eq!(loaded.get_header().get_bucket_count(), 8);
    assert_eq!(loaded.get_header().get_entry_count(), 0);
    assert_eq!(loaded.get_buckets().len(), 8);
    // All buckets should be empty
    for bucket in loaded.get_buckets() {
        assert!(bucket.is_empty());
    }

    cleanup(&filename);
}

#[test]
fn write_and_read_populated_index() {
    let filename = temp_idx("populated");
    let mut index = HashIndex::new(16);
    index.insert_entry(1, 0, 0).unwrap();
    index.insert_entry(2, 0, 1).unwrap();
    index.insert_entry(3, 1, 0).unwrap();
    write_index(&filename, &index).unwrap();

    let loaded = read_index(&filename).unwrap();
    assert_eq!(loaded.get_header().get_bucket_count(), 16);
    assert_eq!(loaded.get_header().get_entry_count(), 3);
    // All entries should be retrievable after roundtrip
    assert_eq!(loaded.lookup(1), Some((0, 0)));
    assert_eq!(loaded.lookup(2), Some((0, 1)));
    assert_eq!(loaded.lookup(3), Some((1, 0)));

    cleanup(&filename);
}

#[test]
fn write_and_read_with_tombstones() {
    let filename = temp_idx("tombstones");
    let mut index = HashIndex::new(8);
    index.insert_entry(3, 0, 0).unwrap();
    index.insert_entry(11, 1, 0).unwrap(); // collides with 3 (mod 8)
    index.remove_entry(3).unwrap(); // creates tombstone

    write_index(&filename, &index).unwrap();

    let loaded = read_index(&filename).unwrap();
    assert_eq!(loaded.get_header().get_entry_count(), 1);
    // Tombstone should be preserved — 11 is still findable
    assert_eq!(loaded.lookup(3), None);
    assert_eq!(loaded.lookup(11), Some((1, 0)));

    cleanup(&filename);
}

#[test]
fn overwrite_index() {
    let filename = temp_idx("overwrite");

    // Write first index
    let mut index1 = HashIndex::new(8);
    index1.insert_entry(1, 0, 0).unwrap();
    write_index(&filename, &index1).unwrap();

    // Overwrite with a different index
    let mut index2 = HashIndex::new(16);
    index2.insert_entry(99, 5, 3).unwrap();
    write_index(&filename, &index2).unwrap();

    // Should read back the second index, not the first
    let loaded = read_index(&filename).unwrap();
    assert_eq!(loaded.get_header().get_bucket_count(), 16);
    assert_eq!(loaded.lookup(1), None);
    assert_eq!(loaded.lookup(99), Some((5, 3)));

    cleanup(&filename);
}
