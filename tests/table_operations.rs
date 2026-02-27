use std::fs;

use young_bird_database::database_operations::file_processing::{
    table::{self, ColumnDef, Table, TableHeader},
    traits::BinarySerde,
    types::ColumnTypes,
};

/// Helper: generates a unique temp filename per test.
fn temp_meta(test_name: &str) -> String {
    format!("test_integration_{}.meta", test_name)
}

/// Helper: cleans up the temp file after a test.
fn cleanup(filename: &str) {
    fs::remove_file(filename).ok();
}

/// Helper: creates a unique temp directory for table tests and returns its path.
fn temp_dir(test_name: &str) -> String {
    let path = format!("test_table_{}", test_name);
    fs::create_dir_all(&path).ok();
    path
}

/// Helper: removes a temp directory and all its contents.
fn cleanup_dir(path: &str) {
    fs::remove_dir_all(path).ok();
}

#[test]
fn write_and_read_table_header() {
    let filename = &temp_meta("write_and_read_table_header");

    let header = TableHeader::new(
        10, // pages_count
        3,  // columns_count
        8,  // page_kbytes
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
            ColumnDef::new(ColumnTypes::Boolean, false, "active".to_string()),
        ],
    );

    table::writing::write_table_header(filename, &header).expect("Failed to write table header");

    let restored =
        table::reading::read_table_header(filename).expect("Failed to read table header");

    assert_eq!(restored.to_bytes(), header.to_bytes());

    cleanup(filename);
}

#[test]
fn write_and_read_empty_table_header() {
    let filename = &temp_meta("write_and_read_empty_table_header");

    let header = TableHeader::new(0, 0, 8, vec![]);

    table::writing::write_table_header(filename, &header).expect("Failed to write table header");

    let restored =
        table::reading::read_table_header(filename).expect("Failed to read table header");

    assert_eq!(restored.to_bytes(), header.to_bytes());

    cleanup(filename);
}

#[test]
fn overwrite_table_header() {
    let filename = &temp_meta("overwrite_table_header");

    // Write a header with 2 columns
    let header_v1 = TableHeader::new(
        5,
        2,
        8,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    );
    table::writing::write_table_header(filename, &header_v1).expect("Failed to write v1");

    // Overwrite with a shorter header (1 column)
    let header_v2 = TableHeader::new(
        5,
        1,
        16,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
    );
    table::writing::write_table_header(filename, &header_v2).expect("Failed to write v2");

    // Should read back v2, not corrupted by leftover v1 bytes
    let restored =
        table::reading::read_table_header(filename).expect("Failed to read table header");

    assert_eq!(restored.to_bytes(), header_v2.to_bytes());

    cleanup(filename);
}

// ══════════════════════════════════════════════════════════
// Table::create + Table::open integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn create_table_produces_files() {
    let dir = temp_dir("create_produces_files");

    Table::create(
        "users".to_string(),
        dir.clone(),
        100,
        8,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    )
    .expect("Failed to create table");

    assert!(fs::metadata(format!("{}/users.meta", dir)).is_ok());
    assert!(fs::metadata(format!("{}/users_0.dat", dir)).is_ok());

    cleanup_dir(&dir);
}

#[test]
fn create_table_invalid_pages_per_file() {
    let dir = temp_dir("create_invalid_ppf");

    let result = Table::create(
        "bad".to_string(),
        dir.clone(),
        0,
        8,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
    );

    assert!(result.is_err());
    cleanup_dir(&dir);
}

#[test]
fn create_and_open_table() {
    let dir = temp_dir("create_and_open");


    let table = Table::create(
        "products".to_string(),
        dir.clone(),
        50,
        8,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    )
    .expect("Can not create table");

    let table_read =
        Table::open("products".to_string(), dir.clone(), 50).expect("Can not read table");

    assert_eq!(
        table.get_header().get_pages_count(),
        table_read.get_header().get_pages_count()
    );
    assert_eq!(
        table.get_header().get_page_kbytes(),
        table_read.get_header().get_page_kbytes()
    );
    assert_eq!(
        table.get_header().get_columns_count(),
        table_read.get_header().get_columns_count()
    );
    assert_eq!(table.get_name(), table_read.get_name());

    cleanup_dir(&dir);
}

#[test]
fn open_nonexistent_table() {
    let result = Table::open("ghost".to_string(), "/tmp/no_such_dir".to_string(), 100);
    assert!(result.is_err());
}
