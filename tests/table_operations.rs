use std::fs;

use young_bird_database::database_operations::file_processing::{
    table::{self, ColumnDef, TableHeader},
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

#[test]
fn write_and_read_table_header() {
    let filename = &temp_meta("write_and_read_table_header");

    let header = TableHeader::new(
        10,   // pages_count
        3,    // columns_count
        8,    // page_kbytes
        1000, // pages_per_file
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
            ColumnDef::new(ColumnTypes::Boolean, false, "active".to_string()),
        ],
    );

    table::writing::write_table_header(filename, &header)
        .expect("Failed to write table header");

    let restored = table::reading::read_table_header(filename)
        .expect("Failed to read table header");

    assert_eq!(restored.to_bytes(), header.to_bytes());

    cleanup(filename);
}

#[test]
fn write_and_read_empty_table_header() {
    let filename = &temp_meta("write_and_read_empty_table_header");

    let header = TableHeader::new(0, 0, 8, 1000, vec![]);

    table::writing::write_table_header(filename, &header)
        .expect("Failed to write table header");

    let restored = table::reading::read_table_header(filename)
        .expect("Failed to read table header");

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
        1000,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    );
    table::writing::write_table_header(filename, &header_v1)
        .expect("Failed to write v1");

    // Overwrite with a shorter header (1 column)
    let header_v2 = TableHeader::new(
        5,
        1,
        16,
        500,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
        ],
    );
    table::writing::write_table_header(filename, &header_v2)
        .expect("Failed to write v2");

    // Should read back v2, not corrupted by leftover v1 bytes
    let restored = table::reading::read_table_header(filename)
        .expect("Failed to read table header");

    assert_eq!(restored.to_bytes(), header_v2.to_bytes());

    cleanup(filename);
}
