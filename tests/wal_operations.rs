use std::fs;

use young_bird_database::database_operations::file_processing::wal::wal_entry::{
    WalEntry, WalOperation,
};
use young_bird_database::database_operations::file_processing::wal::wal_reader::read_all;
use young_bird_database::database_operations::file_processing::wal::wal_writer::WalWriter;

fn temp_wal(test_name: &str) -> String {
    format!("test_wal_{}.wal", test_name)
}

fn cleanup(path: &str) {
    fs::remove_file(path).ok();
}

#[test]
fn wal_read_nonexistent_file_returns_empty() {
    let result = read_all("definitely_does_not_exist.wal").unwrap();
    assert!(result.is_empty());
}

#[test]
fn wal_write_and_read_single_begin_entry() {
    let path = temp_wal("single_begin");
    cleanup(&path);

    let entry = WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new());
    let mut writer = WalWriter::new(path.clone()).unwrap();
    writer.append(&entry).unwrap();
    writer.fsync().unwrap();

    let entries = read_all(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], entry);

    cleanup(&path);
}

#[test]
fn wal_write_and_read_full_transaction() {
    let path = temp_wal("full_txn");
    cleanup(&path);

    let data = vec![1, 2, 3, 4, 5];
    let entries = vec![
        WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new()),
        WalEntry::new(1, WalOperation::Insert, 100, "users".to_string(), data.clone()),
        WalEntry::new(1, WalOperation::Commit, 0, String::new(), Vec::new()),
    ];

    let mut writer = WalWriter::new(path.clone()).unwrap();
    for entry in &entries {
        writer.append(entry).unwrap();
    }
    writer.fsync().unwrap();

    let recovered = read_all(&path).unwrap();
    assert_eq!(recovered.len(), 3);
    assert_eq!(recovered, entries);

    cleanup(&path);
}

#[test]
fn wal_write_multiple_transactions() {
    let path = temp_wal("multi_txn");
    cleanup(&path);

    let entries = vec![
        WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new()),
        WalEntry::new(1, WalOperation::Insert, 1, "users".to_string(), vec![10, 20]),
        WalEntry::new(1, WalOperation::Commit, 0, String::new(), Vec::new()),
        WalEntry::new(2, WalOperation::Begin, 0, String::new(), Vec::new()),
        WalEntry::new(2, WalOperation::Delete, 1, "users".to_string(), Vec::new()),
        WalEntry::new(2, WalOperation::Commit, 0, String::new(), Vec::new()),
    ];

    let mut writer = WalWriter::new(path.clone()).unwrap();
    for entry in &entries {
        writer.append(entry).unwrap();
    }
    writer.fsync().unwrap();

    let recovered = read_all(&path).unwrap();
    assert_eq!(recovered, entries);

    cleanup(&path);
}

#[test]
fn wal_truncate_clears_all_entries() {
    let path = temp_wal("truncate");
    cleanup(&path);

    let mut writer = WalWriter::new(path.clone()).unwrap();
    writer.append(&WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new())).unwrap();
    writer.append(&WalEntry::new(1, WalOperation::Insert, 1, "t".to_string(), vec![1, 2])).unwrap();
    writer.fsync().unwrap();
    writer.truncate().unwrap();

    let entries = read_all(&path).unwrap();
    assert!(entries.is_empty());

    cleanup(&path);
}

#[test]
fn wal_append_after_truncate() {
    let path = temp_wal("append_after_truncate");
    cleanup(&path);

    let mut writer = WalWriter::new(path.clone()).unwrap();
    writer.append(&WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new())).unwrap();
    writer.fsync().unwrap();
    writer.truncate().unwrap();

    // write a second transaction after truncate
    let entry = WalEntry::new(2, WalOperation::Begin, 0, String::new(), Vec::new());
    writer.append(&entry).unwrap();
    writer.fsync().unwrap();

    let entries = read_all(&path).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0], entry);

    cleanup(&path);
}

#[test]
fn wal_survives_reopen() {
    let path = temp_wal("reopen");
    cleanup(&path);

    // write and close
    {
        let mut writer = WalWriter::new(path.clone()).unwrap();
        writer.append(&WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new())).unwrap();
        writer.append(&WalEntry::new(1, WalOperation::Insert, 5, "orders".to_string(), vec![99])).unwrap();
        writer.fsync().unwrap();
    }

    // reopen and append more
    {
        let mut writer = WalWriter::new(path.clone()).unwrap();
        writer.append(&WalEntry::new(1, WalOperation::Commit, 0, String::new(), Vec::new())).unwrap();
        writer.fsync().unwrap();
    }

    let entries = read_all(&path).unwrap();
    assert_eq!(entries.len(), 3);
    assert_eq!(entries[2].operation, WalOperation::Commit);

    cleanup(&path);
}
