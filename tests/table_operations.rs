use std::fs;

use young_bird_database::database_operations::file_processing::{
    page::{reading::read_page, PageRecordContent},
    table::{self, ColumnDef, Table, TableHeader},
    traits::BinarySerde,
    types::{ColumnTypes, ContentTypes},
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

// ══════════════════════════════════════════════════════════
// Table::insert_record integration tests
// ══════════════════════════════════════════════════════════

/// Helper: creates a table with one Int64 column and one Text column.
fn create_test_table(test_name: &str) -> (Table, String) {
    let dir = temp_dir(test_name);
    let table = Table::create(
        "items".to_string(),
        dir.clone(),
        5, // pages_per_file = 5 (small, to test multi-file)
        8, // page_kbytes = 8
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    )
    .expect("Failed to create table");
    (table, dir)
}

/// Helper: builds a record with an Int64 and a Text value.
fn make_record(id: i64, name: &str) -> PageRecordContent {
    PageRecordContent::new(vec![
        ContentTypes::Int64(id),
        ContentTypes::Text(name.to_string()),
    ])
}

#[test]
fn insert_single_record() {
    let (mut table, dir) = create_test_table("insert_single");

    table
        .insert_record(1, make_record(1, "apple"))
        .expect("Failed to insert record");

    // Read the first page and verify the record is there
    let resolved = table.resolve_file(0).unwrap();
    let page = read_page(&resolved.filename, resolved.local_page_number, 8).unwrap();

    assert_eq!(page.header.get_records_count(), 1);

    cleanup_dir(&dir);
}

#[test]
fn insert_multiple_records() {
    let (mut table, dir) = create_test_table("insert_multiple");

    for i in 1..=5 {
        table
            .insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("Failed to insert record");
    }

    let resolved = table.resolve_file(0).unwrap();
    let page = read_page(&resolved.filename, resolved.local_page_number, 8).unwrap();

    assert_eq!(page.header.get_records_count(), 5);

    cleanup_dir(&dir);
}

#[test]
fn insert_triggers_new_page() {
    let (mut table, dir) = create_test_table("insert_new_page");

    // Fill the first page with large records until a new page is created
    let big_name = "x".repeat(2000); // ~2KB per record
    for i in 1..=10 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    // Should have more than 1 page now
    assert!(
        table.get_header().get_pages_count() > 1,
        "Expected more than 1 page, got {}",
        table.get_header().get_pages_count()
    );

    cleanup_dir(&dir);
}

#[test]
fn insert_and_reopen_table() {
    let (mut table, dir) = create_test_table("insert_and_reopen");

    table
        .insert_record(1, make_record(1, "persisted"))
        .expect("Failed to insert record");

    // Reopen the table from disk
    let reopened = Table::open("items".to_string(), dir.clone(), 5)
        .expect("Failed to reopen table");

    // The reopened table should see the same pages_count
    assert_eq!(
        table.get_header().get_pages_count(),
        reopened.get_header().get_pages_count(),
    );

    // Read the page and verify the record survived
    let resolved = reopened.resolve_file(0).unwrap();
    let page = read_page(&resolved.filename, resolved.local_page_number, 8).unwrap();

    assert_eq!(page.header.get_records_count(), 1);

    cleanup_dir(&dir);
}

#[test]
fn insert_new_page_and_reopen() {
    let (mut table, dir) = create_test_table("insert_new_page_reopen");

    // Fill with large records to force new page creation
    let big_name = "y".repeat(2000);
    for i in 1..=10 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    let pages_before = table.get_header().get_pages_count();
    assert!(pages_before > 1, "Should have created multiple pages");

    // Reopen and verify pages_count persisted
    let reopened = Table::open("items".to_string(), dir.clone(), 5)
        .expect("Failed to reopen table");

    assert_eq!(
        pages_before,
        reopened.get_header().get_pages_count(),
        "pages_count should match after reopen"
    );

    cleanup_dir(&dir);
}
