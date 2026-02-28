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
    let page = read_page(&resolved.filename, resolved.local_page_index, 8).unwrap();

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
    let page = read_page(&resolved.filename, resolved.local_page_index, 8).unwrap();

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
    let page = read_page(&resolved.filename, resolved.local_page_index, 8).unwrap();

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

// ══════════════════════════════════════════════════════════
// Table::read_record integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn read_single_record() {
    let (mut table, dir) = create_test_table("read_single");

    table
        .insert_record(42, make_record(42, "hello"))
        .expect("Failed to insert");

    let content = table.read_record(42).expect("Failed to read record");
    let values = content.get_content();

    assert_eq!(values.len(), 2);
    assert_eq!(values[0], ContentTypes::Int64(42));
    assert_eq!(values[1], ContentTypes::Text("hello".to_string()));

    cleanup_dir(&dir);
}

#[test]
fn read_nonexistent_record() {
    let (mut table, dir) = create_test_table("read_nonexistent");

    table
        .insert_record(1, make_record(1, "exists"))
        .expect("Failed to insert");

    let result = table.read_record(999);
    assert!(result.is_err(), "Should return error for nonexistent record");

    cleanup_dir(&dir);
}

#[test]
fn read_record_from_second_page() {
    let (mut table, dir) = create_test_table("read_from_second_page");

    // Fill with large records to force multiple pages
    let big_name = "z".repeat(2000);
    for i in 1..=10 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    assert!(table.get_header().get_pages_count() > 1, "Need multiple pages");

    // Read a record that should be on a later page
    let content = table.read_record(10).expect("Failed to read record 10");
    let values = content.get_content();

    assert_eq!(values[0], ContentTypes::Int64(10));

    cleanup_dir(&dir);
}

#[test]
fn read_multiple_records() {
    let (mut table, dir) = create_test_table("read_multiple");

    for i in 1..=5 {
        table
            .insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("Failed to insert");
    }

    // Read each record back and verify content
    for i in 1..=5u64 {
        let content = table.read_record(i).expect(&format!("Failed to read record {}", i));
        let values = content.get_content();
        assert_eq!(values[0], ContentTypes::Int64(i as i64));
        assert_eq!(values[1], ContentTypes::Text(format!("item_{}", i)));
    }

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Table::delete_record integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn delete_single_record() {
    let (mut table, dir) = create_test_table("delete_single");

    table.insert_record(1, make_record(1, "to_delete")).expect("Failed to insert");

    table.delete_record(1).expect("Failed to delete");

    let result = table.read_record(1);
    assert!(result.is_err(), "Deleted record should not be readable");

    cleanup_dir(&dir);
}

#[test]
fn delete_nonexistent_record() {
    let (mut table, dir) = create_test_table("delete_nonexistent");

    table.insert_record(1, make_record(1, "exists")).expect("Failed to insert");

    let result = table.delete_record(999);
    assert!(result.is_err(), "Should fail for nonexistent record");

    // Original record should still be there
    let content = table.read_record(1).expect("Original should survive");
    assert_eq!(content.get_content()[0], ContentTypes::Int64(1));

    cleanup_dir(&dir);
}

#[test]
fn delete_one_of_multiple_records() {
    let (mut table, dir) = create_test_table("delete_one_of_many");

    for i in 1..=3 {
        table.insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("Failed to insert");
    }

    table.delete_record(2).expect("Failed to delete record 2");

    // Records 1 and 3 should still exist
    assert!(table.read_record(1).is_ok());
    assert!(table.read_record(3).is_ok());
    // Record 2 should be gone
    assert!(table.read_record(2).is_err());

    cleanup_dir(&dir);
}

#[test]
fn delete_record_from_second_page() {
    let (mut table, dir) = create_test_table("delete_from_second_page");

    let big_name = "d".repeat(2000);
    for i in 1..=10 {
        table.insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    assert!(table.get_header().get_pages_count() > 1, "Need multiple pages");

    table.delete_record(10).expect("Failed to delete record 10");

    assert!(table.read_record(10).is_err(), "Record 10 should be deleted");
    // Earlier records should still exist
    assert!(table.read_record(1).is_ok());

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Table::update_record integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn update_record_in_place() {
    let (mut table, dir) = create_test_table("update_in_place");

    table.insert_record(1, make_record(1, "original")).expect("Failed to insert");

    // Update with same-size or smaller content (in-place update)
    table.update_record(1, make_record(1, "updated")).expect("Failed to update");

    let content = table.read_record(1).expect("Failed to read after update");
    let values = content.get_content();
    assert_eq!(values[0], ContentTypes::Int64(1));
    assert_eq!(values[1], ContentTypes::Text("updated".to_string()));

    cleanup_dir(&dir);
}

#[test]
fn update_nonexistent_record() {
    let (mut table, dir) = create_test_table("update_nonexistent");

    table.insert_record(1, make_record(1, "exists")).expect("Failed to insert");

    let result = table.update_record(999, make_record(999, "ghost"));
    assert!(result.is_err(), "Should fail for nonexistent record");

    // Original record should be untouched
    let content = table.read_record(1).expect("Original should survive");
    assert_eq!(content.get_content()[1], ContentTypes::Text("exists".to_string()));

    cleanup_dir(&dir);
}

#[test]
fn update_record_larger_content() {
    let (mut table, dir) = create_test_table("update_larger");

    table.insert_record(1, make_record(1, "short")).expect("Failed to insert");

    // Update with larger content (relocate within page)
    let bigger = "a".repeat(500);
    table.update_record(1, make_record(1, &bigger)).expect("Failed to update");

    let content = table.read_record(1).expect("Failed to read after update");
    let values = content.get_content();
    assert_eq!(values[0], ContentTypes::Int64(1));
    assert_eq!(values[1], ContentTypes::Text(bigger));

    cleanup_dir(&dir);
}

#[test]
fn update_preserves_other_records() {
    let (mut table, dir) = create_test_table("update_preserves_others");

    for i in 1..=3 {
        table.insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("Failed to insert");
    }

    table.update_record(2, make_record(200, "changed")).expect("Failed to update");

    // Records 1 and 3 should be untouched
    assert_eq!(
        table.read_record(1).unwrap().get_content()[1],
        ContentTypes::Text("item_1".to_string())
    );
    assert_eq!(
        table.read_record(3).unwrap().get_content()[1],
        ContentTypes::Text("item_3".to_string())
    );

    // Record 2 should have new content
    let updated = table.read_record(2).expect("Failed to read updated record");
    assert_eq!(updated.get_content()[0], ContentTypes::Int64(200));
    assert_eq!(updated.get_content()[1], ContentTypes::Text("changed".to_string()));

    cleanup_dir(&dir);
}

#[test]
fn update_record_cross_page() {
    let (mut table, dir) = create_test_table("update_cross_page");

    // Fill first page with large records
    let big_name = "u".repeat(2000);
    for i in 1..=4 {
        table.insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    // Update record 1 with even bigger content that won't fit in original page
    let huge_name = "U".repeat(4000);
    table.update_record(1, make_record(1, &huge_name)).expect("Failed to cross-page update");

    // Record should still be readable with new content
    let content = table.read_record(1).expect("Failed to read after cross-page update");
    assert_eq!(content.get_content()[0], ContentTypes::Int64(1));
    assert_eq!(content.get_content()[1], ContentTypes::Text(huge_name));

    // Other records should still be readable
    for i in 2..=4 {
        let c = table.read_record(i).expect(&format!("Record {} should survive", i));
        assert_eq!(c.get_content()[0], ContentTypes::Int64(i as i64));
    }

    cleanup_dir(&dir);
}
