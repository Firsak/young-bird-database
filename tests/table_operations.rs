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
        10,   // pages_count
        3,    // columns_count
        8,    // page_kbytes
        0,    // next_record_id
        1000, // pages_per_file
        1024, // overflow_kbytes
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

    let header = TableHeader::new(0, 0, 8, 0, 1000, 1024, vec![]);

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
        0,
        1000,
        1024,
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
        0,
        1000,
        1024,
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
        1024,
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
        1024,
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
        1024,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
        ],
    )
    .expect("Can not create table");

    let table_read =
        Table::open("products".to_string(), dir.clone()).expect("Can not read table");

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
    let result = Table::open("ghost".to_string(), "/tmp/no_such_dir".to_string());
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
        5,    // pages_per_file = 5 (small, to test multi-file)
        8,    // page_kbytes = 8
        1024, // overflow_kbytes
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

    // Fill the first page with inline records until a new page is created
    // Using text under OVERFLOW_THRESHOLD (256) so it stays inline at ~247 bytes/record
    let big_name = "x".repeat(200);
    for i in 1..=40 {
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
    let reopened = Table::open("items".to_string(), dir.clone())
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

    // Fill with inline records to force new page creation
    let big_name = "y".repeat(200);
    for i in 1..=40 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    let pages_before = table.get_header().get_pages_count();
    assert!(pages_before > 1, "Should have created multiple pages");

    // Reopen and verify pages_count persisted
    let reopened = Table::open("items".to_string(), dir.clone())
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

    // Fill with inline records to force multiple pages
    let big_name = "z".repeat(200);
    for i in 1..=40 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    assert!(table.get_header().get_pages_count() > 1, "Need multiple pages");

    // Read a record that should be on a later page
    let content = table.read_record(40).expect("Failed to read record 40");
    let values = content.get_content();

    assert_eq!(values[0], ContentTypes::Int64(40));

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

    let big_name = "d".repeat(200);
    for i in 1..=40 {
        table.insert_record(i, make_record(i as i64, &big_name))
            .expect(&format!("Failed to insert record {}", i));
    }

    assert!(table.get_header().get_pages_count() > 1, "Need multiple pages");

    table.delete_record(40).expect("Failed to delete record 40");

    assert!(table.read_record(40).is_err(), "Record 40 should be deleted");
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

// ══════════════════════════════════════════════════════════
// Schema validation integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn insert_valid_record_passes_validation() {
    let (mut table, dir) = create_test_table("valid_record");

    // Table has [Int64, Text] — this matches
    table.insert_record(1, make_record(1, "valid")).expect("Valid record should insert");

    cleanup_dir(&dir);
}

#[test]
fn insert_wrong_column_count() {
    let (mut table, dir) = create_test_table("wrong_col_count");

    // Table expects 2 columns, provide 1
    let record = PageRecordContent::new(vec![ContentTypes::Int64(1)]);
    let result = table.insert_record(1, record);
    assert!(result.is_err(), "Should reject wrong column count");

    // Provide 3
    let record = PageRecordContent::new(vec![
        ContentTypes::Int64(1),
        ContentTypes::Text("a".to_string()),
        ContentTypes::Boolean(true),
    ]);
    let result = table.insert_record(2, record);
    assert!(result.is_err(), "Should reject extra columns");

    cleanup_dir(&dir);
}

#[test]
fn insert_wrong_type() {
    let (mut table, dir) = create_test_table("wrong_type");

    // Table expects [Int64, Text], provide [Text, Int64] (swapped)
    let record = PageRecordContent::new(vec![
        ContentTypes::Text("oops".to_string()),
        ContentTypes::Int64(42),
    ]);
    let result = table.insert_record(1, record);
    assert!(result.is_err(), "Should reject type mismatch");

    cleanup_dir(&dir);
}

#[test]
fn insert_null_in_non_nullable_column() {
    let (mut table, dir) = create_test_table("null_non_nullable");

    // Table has [Int64(non-nullable), Text(nullable)]
    // Null in first column should fail
    let record = PageRecordContent::new(vec![
        ContentTypes::Null,
        ContentTypes::Text("ok".to_string()),
    ]);
    let result = table.insert_record(1, record);
    assert!(result.is_err(), "Should reject null in non-nullable column");

    cleanup_dir(&dir);
}

#[test]
fn insert_null_in_nullable_column() {
    let (mut table, dir) = create_test_table("null_nullable");

    // Table has [Int64(non-nullable), Text(nullable)]
    // Null in second column (nullable) should succeed
    let record = PageRecordContent::new(vec![
        ContentTypes::Int64(1),
        ContentTypes::Null,
    ]);
    table.insert_record(1, record).expect("Null in nullable column should be accepted");

    cleanup_dir(&dir);
}

#[test]
fn update_with_wrong_schema_rejected() {
    let (mut table, dir) = create_test_table("update_wrong_schema");

    table.insert_record(1, make_record(1, "original")).expect("Insert should work");

    // Try to update with wrong types
    let bad_record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
    ]);
    let result = table.update_record(1, bad_record);
    assert!(result.is_err(), "Should reject schema-violating update");

    // Original record should be untouched
    let content = table.read_record(1).expect("Original should survive");
    assert_eq!(content.get_content()[1], ContentTypes::Text("original".to_string()));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Input validation tests
// ══════════════════════════════════════════════════════════

#[test]
fn create_with_zero_page_kbytes_rejected() {
    let dir = temp_dir("zero_page_kbytes");
    let result = Table::create(
        "test".to_string(),
        dir.clone(),
        10,
        0, // zero page_kbytes
        1024,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
    );
    assert!(result.is_err(), "Should reject page_kbytes of 0");
    cleanup_dir(&dir);
}

#[test]
fn create_with_empty_columns_rejected() {
    let dir = temp_dir("empty_columns");
    let result = Table::create(
        "test".to_string(),
        dir.clone(),
        10,
        8,
        1024,
        vec![], // no columns
    );
    assert!(result.is_err(), "Should reject empty column list");
    cleanup_dir(&dir);
}

#[test]
fn create_with_empty_table_name_rejected() {
    let dir = temp_dir("empty_table_name");
    let result = Table::create(
        "".to_string(),
        dir.clone(),
        10,
        8,
        1024,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
    );
    assert!(result.is_err(), "Should reject empty table name");
    cleanup_dir(&dir);
}

#[test]
fn create_with_whitespace_table_name_rejected() {
    let dir = temp_dir("whitespace_table_name");
    let result = Table::create(
        "   ".to_string(),
        dir.clone(),
        10,
        8,
        1024,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
    );
    assert!(result.is_err(), "Should reject whitespace-only table name");
    cleanup_dir(&dir);
}

#[test]
fn create_with_empty_column_name_rejected() {
    let dir = temp_dir("empty_col_name");
    let result = Table::create(
        "test".to_string(),
        dir.clone(),
        10,
        8,
        1024,
        vec![ColumnDef::new(ColumnTypes::Int64, false, "".to_string())],
    );
    assert!(result.is_err(), "Should reject empty column name");
    cleanup_dir(&dir);
}

#[test]
fn open_with_empty_name_rejected() {
    let result = Table::open(
        "".to_string(),
        "/tmp".to_string(),
    );
    assert!(result.is_err(), "Should reject empty table name on open");
}

// ══════════════════════════════════════════════════════════
// fragmentation_ratio tests
// ══════════════════════════════════════════════════════════

#[test]
fn fragmentation_ratio_fresh_table() {
    let (table, dir) = create_test_table("frag_fresh");

    // Fresh table: 1 page (last page) with no fragmented space → ratio 0.0
    let ratio = table.fragmentation_ratio().expect("Failed to get ratio");
    assert!(
        ratio == 0.0,
        "Fresh empty table should have ratio 0.0, got {}",
        ratio
    );

    cleanup_dir(&dir);
}

#[test]
fn fragmentation_ratio_after_inserts() {
    let (mut table, dir) = create_test_table("frag_after_inserts");

    // Insert several records into single page — no fragmentation, no deleted records
    for i in 1..=10 {
        table
            .insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("insert failed");
    }

    let ratio = table.fragmentation_ratio().expect("Failed to get ratio");
    assert!(
        ratio == 0.0,
        "Single page with no deletes should have ratio 0.0, got {}",
        ratio
    );

    cleanup_dir(&dir);
}

#[test]
fn fragmentation_ratio_after_deletes() {
    let (mut table, dir) = create_test_table("frag_after_deletes");

    for i in 1..=10 {
        table
            .insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("insert failed");
    }
    let ratio_before = table.fragmentation_ratio().expect("ratio before");

    // Delete half the records — creates fragmented space
    for i in 1..=5 {
        table.delete_record(i).expect("delete failed");
    }
    let ratio_after = table.fragmentation_ratio().expect("ratio after");

    assert!(
        ratio_after > ratio_before,
        "Ratio should increase after deletes: before={}, after={}",
        ratio_before,
        ratio_after
    );

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// compact_table tests
// ══════════════════════════════════════════════════════════

#[test]
fn compact_table_no_fragmentation() {
    let (mut table, dir) = create_test_table("compact_no_frag");

    // Insert a few small records into 1 page, no deletes
    for i in 1..=5 {
        table
            .insert_record(i, make_record(i as i64, &format!("item_{}", i)))
            .expect("insert failed");
    }

    let pages_freed = table.compact_table().expect("compact failed");
    assert_eq!(pages_freed, 0, "No pages should be freed when there's no fragmentation");

    // Records should still be readable
    for i in 1..=5 {
        let content = table.read_record(i).expect(&format!("record {} missing after compact", i));
        assert_eq!(content.get_content()[0], ContentTypes::Int64(i as i64));
    }

    cleanup_dir(&dir);
}

#[test]
fn compact_table_consolidates_sparse_pages() {
    let (mut table, dir) = create_test_table("compact_sparse");

    // Insert inline records to force multiple pages
    let big_name = "c".repeat(200);
    for i in 1..=40 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect("insert failed");
    }
    let pages_before = table.get_header().get_pages_count();
    assert!(pages_before > 1, "Need multiple pages for this test");

    // Delete most records, leaving only 2 that fit in 1 page
    for i in 3..=40 {
        table.delete_record(i).expect("delete failed");
    }

    let pages_freed = table.compact_table().expect("compact failed");
    assert!(pages_freed > 0, "Should have freed at least 1 page");

    let pages_after = table.get_header().get_pages_count();
    assert!(
        pages_after < pages_before,
        "Pages count should decrease: before={}, after={}",
        pages_before,
        pages_after
    );

    cleanup_dir(&dir);
}

#[test]
fn compact_table_all_deleted() {
    let (mut table, dir) = create_test_table("compact_all_deleted");

    for i in 1..=5 {
        table
            .insert_record(i, make_record(i as i64, "temp"))
            .expect("insert failed");
    }

    for i in 1..=5 {
        table.delete_record(i).expect("delete failed");
    }

    table.compact_table().expect("compact failed");

    // Should have exactly 1 page (minimum)
    assert_eq!(
        table.get_header().get_pages_count(),
        1,
        "Should keep at least 1 page"
    );

    cleanup_dir(&dir);
}

#[test]
fn compact_table_preserves_data() {
    let (mut table, dir) = create_test_table("compact_preserves");

    let big_name = "p".repeat(2000);
    for i in 1..=8 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect("insert failed");
    }

    // Delete odd-numbered records (creates holes across multiple pages)
    for i in [1, 3, 5, 7] {
        table.delete_record(i).expect("delete failed");
    }

    table.compact_table().expect("compact failed");

    // Surviving even records should have correct content
    for i in [2, 4, 6, 8] {
        let content = table.read_record(i).expect(&format!("record {} missing", i));
        assert_eq!(content.get_content()[0], ContentTypes::Int64(i as i64));
        assert_eq!(
            content.get_content()[1],
            ContentTypes::Text(big_name.clone())
        );
    }

    // Deleted records should remain gone
    for i in [1, 3, 5, 7] {
        assert!(table.read_record(i).is_err(), "record {} should be deleted", i);
    }

    cleanup_dir(&dir);
}

#[test]
fn compact_table_updates_index() {
    let (mut table, dir) = create_test_table("compact_index");

    let big_name = "i".repeat(2000);
    for i in 1..=6 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect("insert failed");
    }

    // Delete first 4 — only records 5 and 6 survive
    for i in 1..=4 {
        table.delete_record(i).expect("delete failed");
    }

    table.compact_table().expect("compact failed");

    // Index-based reads should still work for surviving records
    let c5 = table.read_record(5).expect("record 5 missing");
    assert_eq!(c5.get_content()[0], ContentTypes::Int64(5));

    let c6 = table.read_record(6).expect("record 6 missing");
    assert_eq!(c6.get_content()[0], ContentTypes::Int64(6));

    cleanup_dir(&dir);
}

#[test]
fn compact_table_reopen_after_compact() {
    let (mut table, dir) = create_test_table("compact_reopen");

    let big_name = "r".repeat(2000);
    for i in 1..=6 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect("insert failed");
    }

    for i in 1..=4 {
        table.delete_record(i).expect("delete failed");
    }

    table.compact_table().expect("compact failed");

    // Reopen the table from disk
    let reopened = Table::open("items".to_string(), dir.clone())
        .expect("Failed to reopen after compact");

    // Records 5 and 6 should be readable from reopened table
    let c5 = reopened.read_record(5).expect("record 5 missing after reopen");
    assert_eq!(c5.get_content()[0], ContentTypes::Int64(5));

    let c6 = reopened.read_record(6).expect("record 6 missing after reopen");
    assert_eq!(c6.get_content()[0], ContentTypes::Int64(6));

    cleanup_dir(&dir);
}

#[test]
fn fragmentation_ratio_decreases_after_compact() {
    let (mut table, dir) = create_test_table("frag_after_compact");

    let big_name = "f".repeat(2000);
    for i in 1..=8 {
        table
            .insert_record(i, make_record(i as i64, &big_name))
            .expect("insert failed");
    }

    // Delete most records to create fragmentation
    for i in 1..=6 {
        table.delete_record(i).expect("delete failed");
    }

    let ratio_before = table.fragmentation_ratio().expect("ratio before");

    table.compact_table().expect("compact failed");

    let ratio_after = table.fragmentation_ratio().expect("ratio after");

    assert!(
        ratio_after <= ratio_before,
        "Ratio should decrease after compact: before={}, after={}",
        ratio_before,
        ratio_after
    );

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Auto-increment insert tests
// ══════════════════════════════════════════════════════════

#[test]
fn insert_auto_increment_ids() {
    let (mut table, dir) = create_test_table("insert_auto_ids");

    let id0 = table
        .insert(make_record(10, "alice"))
        .expect("insert 0 failed");
    let id1 = table
        .insert(make_record(20, "bob"))
        .expect("insert 1 failed");
    let id2 = table
        .insert(make_record(30, "carol"))
        .expect("insert 2 failed");

    assert_eq!(id0, 0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    // Records are readable by the returned IDs
    let c0 = table.read_record(id0).expect("read 0 failed");
    assert_eq!(c0.get_content()[0], ContentTypes::Int64(10));

    let c2 = table.read_record(id2).expect("read 2 failed");
    assert_eq!(c2.get_content()[1], ContentTypes::Text("carol".to_string()));

    cleanup_dir(&dir);
}

#[test]
fn insert_auto_increment_persists_across_reopen() {
    let (mut table, dir) = create_test_table("insert_auto_reopen");

    table.insert(make_record(1, "first")).expect("insert failed");
    table.insert(make_record(2, "second")).expect("insert failed");

    // Reopen the table from disk
    let mut reopened = Table::open("items".to_string(), dir.clone())
        .expect("reopen failed");

    let id = reopened
        .insert(make_record(3, "third"))
        .expect("insert after reopen failed");

    // next_record_id was 2 before reopen, so this should be 2
    assert_eq!(id, 2);

    // All three records should be readable
    let c0 = reopened.read_record(0).expect("read 0 failed");
    assert_eq!(c0.get_content()[1], ContentTypes::Text("first".to_string()));

    let c2 = reopened.read_record(2).expect("read 2 failed");
    assert_eq!(c2.get_content()[1], ContentTypes::Text("third".to_string()));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// scan_records tests
// ══════════════════════════════════════════════════════════

#[test]
fn scan_records_all() {
    let (mut table, dir) = create_test_table("scan_all");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    let results = table
        .scan_records(|_id, _cols| true)
        .expect("scan failed");

    assert_eq!(results.len(), 3);

    cleanup_dir(&dir);
}

#[test]
fn scan_records_with_filter() {
    let (mut table, dir) = create_test_table("scan_filter");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    // Filter: only records where the Int64 column > 15
    let results = table
        .scan_records(|_id, cols| {
            if let ContentTypes::Int64(v) = &cols[0] {
                *v > 15
            } else {
                false
            }
        })
        .expect("scan failed");

    assert_eq!(results.len(), 2);

    // Verify the matching records have the right values
    let values: Vec<i64> = results
        .iter()
        .map(|(_id, content)| {
            if let ContentTypes::Int64(v) = &content.get_content()[0] {
                *v
            } else {
                panic!("expected Int64");
            }
        })
        .collect();
    assert!(values.contains(&20));
    assert!(values.contains(&30));

    cleanup_dir(&dir);
}

#[test]
fn scan_records_after_delete() {
    let (mut table, dir) = create_test_table("scan_after_del");

    let id0 = table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    table.delete_record(id0).expect("delete failed");

    let results = table
        .scan_records(|_id, _cols| true)
        .expect("scan failed");

    assert_eq!(results.len(), 2, "deleted record should not appear in scan");

    // Verify the deleted record's ID is not in results
    let ids: Vec<u64> = results.iter().map(|(id, _)| *id).collect();
    assert!(!ids.contains(&id0));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// scan_record_ids + delete tests (two-phase delete)
// ══════════════════════════════════════════════════════════

#[test]
fn scan_delete_all_records() {
    let (mut table, dir) = create_test_table("scan_del_all");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    let ids = table.scan_record_ids(|_id, _cols| true).expect("scan failed");
    assert_eq!(ids.len(), 3);
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 0);

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_with_filter() {
    let (mut table, dir) = create_test_table("scan_del_filter");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    // Delete only records where Int64 column > 15
    let ids = table
        .scan_record_ids(|_id, cols| {
            if let ContentTypes::Int64(v) = &cols[0] {
                *v > 15
            } else {
                false
            }
        })
        .expect("scan failed");
    assert_eq!(ids.len(), 2);
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    // Only alice (10) should remain
    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].1.get_content()[0], ContentTypes::Int64(10));

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_none_matching() {
    let (mut table, dir) = create_test_table("scan_del_none");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");

    let ids = table.scan_record_ids(|_id, _cols| false).expect("scan failed");
    assert_eq!(ids.len(), 0);

    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 2);

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_by_id() {
    let (mut table, dir) = create_test_table("scan_del_by_id");

    let id0 = table.insert(make_record(10, "alice")).expect("insert failed");
    let id1 = table.insert(make_record(20, "bob")).expect("insert failed");
    let id2 = table.insert(make_record(30, "carol")).expect("insert failed");

    let ids = table.scan_record_ids(|id, _cols| id == id1).expect("scan failed");
    assert_eq!(ids, vec![id1]);
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    table.read_record(id0).expect("id0 should exist");
    table.read_record(id2).expect("id2 should exist");
    assert!(table.read_record(id1).is_err(), "id1 should be deleted");

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_across_pages() {
    let (mut table, dir) = create_test_table("scan_del_pages");

    let big_name = "x".repeat(200);
    for _ in 0..40 {
        table.insert(make_record(1, &big_name)).expect("insert failed");
    }
    assert!(table.get_header().get_pages_count() > 1, "Need multiple pages");

    let ids = table.scan_record_ids(|_id, _cols| true).expect("scan failed");
    assert_eq!(ids.len(), 40);
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 0);

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_records_are_not_in_index() {
    let (mut table, dir) = create_test_table("scan_del_index");

    let id0 = table.insert(make_record(10, "alice")).expect("insert failed");
    let id1 = table.insert(make_record(20, "bob")).expect("insert failed");

    let ids = table.scan_record_ids(|id, _cols| id == id0).expect("scan failed");
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    assert!(table.read_record(id0).is_err());
    let content = table.read_record(id1).expect("id1 should exist");
    assert_eq!(content.get_content()[0], ContentTypes::Int64(20));

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_after_previous_deletes() {
    let (mut table, dir) = create_test_table("scan_del_prev_del");

    let _id0 = table.insert(make_record(10, "alice")).expect("insert failed");
    let id1 = table.insert(make_record(20, "bob")).expect("insert failed");
    let _id2 = table.insert(make_record(30, "carol")).expect("insert failed");
    let _id3 = table.insert(make_record(40, "dave")).expect("insert failed");

    // Soft-delete a middle record first
    table.delete_record(id1).expect("delete failed");

    // Now scan and delete remaining records where value > 25
    let ids = table
        .scan_record_ids(|_id, cols| {
            if let ContentTypes::Int64(v) = &cols[0] {
                *v > 25
            } else {
                false
            }
        })
        .expect("scan failed");
    assert_eq!(ids.len(), 2, "should find carol and dave");
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].1.get_content()[0], ContentTypes::Int64(10));

    cleanup_dir(&dir);
}

#[test]
fn scan_delete_cascade_hard_deletes() {
    let (mut table, dir) = create_test_table("scan_del_cascade");

    table.insert(make_record(10, "alice")).expect("insert failed");
    table.insert(make_record(20, "bob")).expect("insert failed");
    table.insert(make_record(30, "carol")).expect("insert failed");

    let ids = table
        .scan_record_ids(|_id, cols| {
            if let ContentTypes::Int64(v) = &cols[0] {
                *v > 15
            } else {
                false
            }
        })
        .expect("scan failed");
    assert_eq!(ids.len(), 2);
    for id in &ids {
        table.delete_record(*id).expect("delete failed");
    }

    let remaining = table.scan_records(|_id, _cols| true).expect("scan failed");
    assert_eq!(remaining.len(), 1);
    assert_eq!(remaining[0].1.get_content()[0], ContentTypes::Int64(10));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Overflow fragmentation tracking tests
// ══════════════════════════════════════════════════════════

#[test]
fn overflow_delete_adds_fragmented_space() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("overflow_del_frag");

    let big_text = "D".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    // Verify fragmented_space starts at 0
    let overflow_path = format!("{}/items_0.overflow", dir);
    let header_before = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_before.get_fragmented_space(), 0);

    // Delete the record — should mark 500 bytes as fragmented
    table.delete_record(1).expect("delete failed");

    let header_after = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_after.get_fragmented_space(), 500);

    // used_space should NOT change (append-only, just tracking dead space)
    assert_eq!(header_before.get_used_space(), header_after.get_used_space());

    cleanup_dir(&dir);
}

#[test]
fn overflow_update_different_text_adds_fragmented_space() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("overflow_upd_diff");

    let text_v1 = "A".repeat(500);
    table
        .insert_record(1, make_record(1, &text_v1))
        .expect("insert failed");

    let overflow_path = format!("{}/items_0.overflow", dir);
    let header_before = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_before.get_fragmented_space(), 0);

    // Update to different overflow text — old 500 bytes fragmented, new 600 bytes appended
    let text_v2 = "B".repeat(600);
    table
        .update_record(1, make_record(1, &text_v2))
        .expect("update failed");

    let header_after = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_after.get_fragmented_space(), 500);
    assert_eq!(
        header_after.get_used_space(),
        header_before.get_used_space() + 600
    );

    // Data still readable
    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(text_v2));

    cleanup_dir(&dir);
}

#[test]
fn overflow_update_same_text_no_fragmentation() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("overflow_upd_same");

    let big_text = "S".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let overflow_path = format!("{}/items_0.overflow", dir);
    let header_before = read_overflow_header(&overflow_path).expect("read header failed");

    // Update with identical text — ref should be reused, no fragmentation
    table
        .update_record(1, make_record(1, &big_text))
        .expect("update failed");

    let header_after = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_after.get_fragmented_space(), 0);
    assert_eq!(header_after.get_used_space(), header_before.get_used_space());

    // Data still correct
    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(big_text));

    cleanup_dir(&dir);
}

#[test]
fn overflow_update_to_inline_releases_ref() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("overflow_upd_inline");

    let big_text = "L".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let overflow_path = format!("{}/items_0.overflow", dir);

    // Update to short text — old overflow ref should be released
    table
        .update_record(1, make_record(1, "short"))
        .expect("update failed");

    let header_after = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header_after.get_fragmented_space(), 500);

    let content = table.read_record(1).expect("read failed");
    assert_eq!(
        content.get_content()[1],
        ContentTypes::Text("short".to_string())
    );

    cleanup_dir(&dir);
}

#[test]
fn overflow_update_from_inline_to_overflow_no_fragmentation() {
    let (mut table, dir) = create_test_table("overflow_upd_to_ovf");

    table
        .insert_record(1, make_record(1, "short"))
        .expect("insert failed");

    // No overflow file yet
    let overflow_path = format!("{}/items_0.overflow", dir);
    assert!(!std::path::Path::new(&overflow_path).exists());

    // Update to large text — creates overflow, no fragmentation since old was inline
    let big_text = "G".repeat(500);
    table
        .update_record(1, make_record(1, &big_text))
        .expect("update failed");

    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;
    let header = read_overflow_header(&overflow_path).expect("read header failed");
    assert_eq!(header.get_fragmented_space(), 0);

    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(big_text));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Overflow text integration tests
// ══════════════════════════════════════════════════════════

#[test]
fn overflow_insert_and_read_large_text() {
    let (mut table, dir) = create_test_table("overflow_insert_read");

    let big_text = "A".repeat(500); // Above OVERFLOW_THRESHOLD (256)
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[0], ContentTypes::Int64(1));
    assert_eq!(content.get_content()[1], ContentTypes::Text(big_text));

    cleanup_dir(&dir);
}

#[test]
fn overflow_small_text_stays_inline() {
    let (mut table, dir) = create_test_table("overflow_inline");

    let small_text = "B".repeat(256); // Exactly at threshold — stays inline
    table
        .insert_record(1, make_record(1, &small_text))
        .expect("insert failed");

    // No overflow file should be created
    let overflow_path = format!("{}/items_0.overflow", dir);
    assert!(
        !std::path::Path::new(&overflow_path).exists(),
        "Overflow file should not exist for inline text"
    );

    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(small_text));

    cleanup_dir(&dir);
}

#[test]
fn overflow_multiple_records_with_large_text() {
    let (mut table, dir) = create_test_table("overflow_multi");

    for i in 1..=5u64 {
        let text = "X".repeat(400 + i as usize * 50);
        table
            .insert_record(i, make_record(i as i64, &text))
            .expect("insert failed");
    }

    // Read back all records and verify text is correct
    for i in 1..=5u64 {
        let expected_text = "X".repeat(400 + i as usize * 50);
        let content = table.read_record(i).expect("read failed");
        assert_eq!(content.get_content()[1], ContentTypes::Text(expected_text));
    }

    cleanup_dir(&dir);
}

#[test]
fn overflow_scan_records_resolves_text() {
    let (mut table, dir) = create_test_table("overflow_scan");

    let big_text = "S".repeat(500);
    table.insert(make_record(1, &big_text)).expect("insert failed");
    table.insert(make_record(2, "small")).expect("insert failed");

    let results = table
        .scan_records(|_id, _cols| true)
        .expect("scan failed");

    assert_eq!(results.len(), 2);

    // Both records should have resolved text (no OverflowText leaking out)
    let texts: Vec<&ContentTypes> = results.iter().map(|(_, c)| &c.get_content()[1]).collect();
    assert!(texts.contains(&&ContentTypes::Text(big_text)));
    assert!(texts.contains(&&ContentTypes::Text("small".to_string())));

    cleanup_dir(&dir);
}

#[test]
fn overflow_update_to_large_text() {
    let (mut table, dir) = create_test_table("overflow_update");

    table
        .insert_record(1, make_record(1, "short"))
        .expect("insert failed");

    // Update to oversized text — should go to overflow
    let big_text = "U".repeat(600);
    table
        .update_record(1, make_record(1, &big_text))
        .expect("update failed");

    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(big_text));

    cleanup_dir(&dir);
}

#[test]
fn overflow_persists_across_reopen() {
    let (mut table, dir) = create_test_table("overflow_reopen");

    let big_text = "R".repeat(1000);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    // Reopen the table from disk
    let reopened =
        Table::open("items".to_string(), dir.clone()).expect("reopen failed");

    let content = reopened.read_record(1).expect("read after reopen failed");
    assert_eq!(content.get_content()[1], ContentTypes::Text(big_text));

    cleanup_dir(&dir);
}

#[test]
fn overflow_file_is_created() {
    let (mut table, dir) = create_test_table("overflow_file_exists");

    let big_text = "F".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let overflow_path = format!("{}/items_0.overflow", dir);
    assert!(
        std::path::Path::new(&overflow_path).exists(),
        "Overflow file should exist after inserting large text"
    );

    cleanup_dir(&dir);
}

#[test]
fn overflow_mixed_inline_and_overflow_columns() {
    let dir = temp_dir("overflow_mixed_cols");
    let table = Table::create(
        "mixed".to_string(),
        dir.clone(),
        5,
        8,
        1024,
        vec![
            ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
            ColumnDef::new(ColumnTypes::Text, false, "short_text".to_string()),
            ColumnDef::new(ColumnTypes::Text, false, "long_text".to_string()),
        ],
    )
    .expect("create failed");

    let mut table = table;
    let short = "inline";
    let long = "O".repeat(500);

    let record = PageRecordContent::new(vec![
        ContentTypes::Int64(42),
        ContentTypes::Text(short.to_string()),
        ContentTypes::Text(long.clone()),
    ]);
    table.insert_record(1, record).expect("insert failed");

    let content = table.read_record(1).expect("read failed");
    assert_eq!(content.get_content()[0], ContentTypes::Int64(42));
    assert_eq!(
        content.get_content()[1],
        ContentTypes::Text(short.to_string())
    );
    assert_eq!(content.get_content()[2], ContentTypes::Text(long));

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// Overflow reverse index tests
// ══════════════════════════════════════════════════════════

#[test]
fn overflow_reverse_index_populated_on_insert() {
    let (mut table, dir) = create_test_table("rev_idx_insert");

    let big_text = "A".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let rev = table.get_overflow_reverse();
    assert_eq!(rev.len(), 1);

    let entries = rev.get_by_file(0);
    assert_eq!(entries.len(), 1);
    // (offset, record_id, column_index)
    assert_eq!(entries[0].1, 1); // record_id
    assert_eq!(entries[0].2, 1); // column_index (column 0 is Int64, column 1 is Text)

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_not_populated_for_inline() {
    let (mut table, dir) = create_test_table("rev_idx_inline");

    table
        .insert_record(1, make_record(1, "short"))
        .expect("insert failed");

    assert!(table.get_overflow_reverse().is_empty());

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_removed_on_delete() {
    let (mut table, dir) = create_test_table("rev_idx_delete");

    let big_text = "D".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");
    assert_eq!(table.get_overflow_reverse().len(), 1);

    table.delete_record(1).expect("delete failed");
    assert!(table.get_overflow_reverse().is_empty());

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_updated_on_update_different_text() {
    let (mut table, dir) = create_test_table("rev_idx_upd_diff");

    let text_v1 = "A".repeat(500);
    table
        .insert_record(1, make_record(1, &text_v1))
        .expect("insert failed");

    let entries_before = table.get_overflow_reverse().get_by_file(0);
    assert_eq!(entries_before.len(), 1);
    let old_offset = entries_before[0].0;

    // Update to different overflow text — offset should change
    let text_v2 = "B".repeat(600);
    table
        .update_record(1, make_record(1, &text_v2))
        .expect("update failed");

    let rev = table.get_overflow_reverse();
    assert_eq!(rev.len(), 1);
    let entries_after = rev.get_by_file(0);
    assert_eq!(entries_after.len(), 1);
    assert_eq!(entries_after[0].1, 1); // same record_id
    assert_ne!(entries_after[0].0, old_offset); // different offset

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_unchanged_on_update_same_text() {
    let (mut table, dir) = create_test_table("rev_idx_upd_same");

    let big_text = "S".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");

    let entries_before = table.get_overflow_reverse().get_by_file(0);
    let old_offset = entries_before[0].0;

    // Update with identical text — offset should stay the same (ref reused)
    table
        .update_record(1, make_record(1, &big_text))
        .expect("update failed");

    let entries_after = table.get_overflow_reverse().get_by_file(0);
    assert_eq!(entries_after.len(), 1);
    assert_eq!(entries_after[0].0, old_offset); // same offset (reused)

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_cleared_on_update_to_inline() {
    let (mut table, dir) = create_test_table("rev_idx_upd_inline");

    let big_text = "L".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");
    assert_eq!(table.get_overflow_reverse().len(), 1);

    // Update to short text — overflow ref removed
    table
        .update_record(1, make_record(1, "short"))
        .expect("update failed");

    assert!(table.get_overflow_reverse().is_empty());

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_rebuilt_on_open() {
    let (mut table, dir) = create_test_table("rev_idx_reopen");

    let big_text = "R".repeat(500);
    table
        .insert_record(1, make_record(1, &big_text))
        .expect("insert failed");
    table
        .insert_record(2, make_record(2, &"Q".repeat(400)))
        .expect("insert failed");

    // Reopen — reverse index should be rebuilt from page scan
    let reopened = Table::open("items".to_string(), dir.clone()).expect("reopen failed");

    let rev = reopened.get_overflow_reverse();
    assert_eq!(rev.len(), 2);

    let entries = rev.get_by_file(0);
    assert_eq!(entries.len(), 2);
    let record_ids: Vec<u64> = entries.iter().map(|(_, rid, _)| *rid).collect();
    assert!(record_ids.contains(&1));
    assert!(record_ids.contains(&2));

    cleanup_dir(&dir);
}

#[test]
fn overflow_reverse_index_multiple_records() {
    let (mut table, dir) = create_test_table("rev_idx_multi");

    for i in 1..=5u64 {
        let text = "X".repeat(300 + i as usize * 50);
        table
            .insert_record(i, make_record(i as i64, &text))
            .expect("insert failed");
    }

    assert_eq!(table.get_overflow_reverse().len(), 5);

    // Delete record 3
    table.delete_record(3).expect("delete failed");
    assert_eq!(table.get_overflow_reverse().len(), 4);

    // Verify record 3 is gone from reverse index
    let entries = table.get_overflow_reverse().get_by_file(0);
    let record_ids: Vec<u64> = entries.iter().map(|(_, rid, _)| *rid).collect();
    assert!(!record_ids.contains(&3));

    cleanup_dir(&dir);
}

// --- compact_overflow_file tests ---

#[test]
fn compact_overflow_reclaims_fragmented_space() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("compact_overflow_reclaim");

    let text1 = "A".repeat(500);
    let text2 = "B".repeat(300);
    table.insert_record(1, make_record(1, &text1)).unwrap();
    table.insert_record(2, make_record(2, &text2)).unwrap();

    // Delete record 1 — 500 bytes become fragmented
    table.delete_record(1).unwrap();

    let overflow_path = format!("{}/items_0.overflow", dir);
    let header_before = read_overflow_header(&overflow_path).unwrap();
    assert_eq!(header_before.get_fragmented_space(), 500);

    // Compact — should eliminate fragmented space
    table.compact_overflow_file(0).unwrap();

    let header_after = read_overflow_header(&overflow_path).unwrap();
    assert_eq!(header_after.get_fragmented_space(), 0);
    // used_space should shrink: header(16) + 300 bytes only
    assert_eq!(header_after.get_used_space(), 16 + 300);

    cleanup_dir(&dir);
}

#[test]
fn compact_overflow_records_still_readable() {
    let (mut table, dir) = create_test_table("compact_overflow_readable");

    let text1 = "A".repeat(500);
    let text2 = "B".repeat(300);
    let text3 = "C".repeat(400);
    table.insert_record(1, make_record(1, &text1)).unwrap();
    table.insert_record(2, make_record(2, &text2)).unwrap();
    table.insert_record(3, make_record(3, &text3)).unwrap();

    // Delete record 2 to create fragmentation
    table.delete_record(2).unwrap();

    table.compact_overflow_file(0).unwrap();

    // Records 1 and 3 should still be readable with correct data
    let content1 = table.read_record(1).unwrap();
    assert_eq!(content1.get_content()[1], ContentTypes::Text(text1));

    let content3 = table.read_record(3).unwrap();
    assert_eq!(content3.get_content()[1], ContentTypes::Text(text3));

    cleanup_dir(&dir);
}

#[test]
fn compact_overflow_reverse_index_updated() {
    let (mut table, dir) = create_test_table("compact_overflow_rev_idx");

    let text1 = "A".repeat(500);
    let text2 = "B".repeat(300);
    table.insert_record(1, make_record(1, &text1)).unwrap();
    table.insert_record(2, make_record(2, &text2)).unwrap();

    table.delete_record(1).unwrap();
    assert_eq!(table.get_overflow_reverse().len(), 1);

    table.compact_overflow_file(0).unwrap();

    // Reverse index should still have 1 entry with updated offset
    assert_eq!(table.get_overflow_reverse().len(), 1);
    let entries = table.get_overflow_reverse().get_by_file(0);
    assert_eq!(entries.len(), 1);
    // Should point to record 2
    assert_eq!(entries[0].1, 2);
    // Offset should be right after header (16)
    assert_eq!(entries[0].0, 16);

    cleanup_dir(&dir);
}

#[test]
fn compact_overflow_no_fragmentation_is_noop() {
    use young_bird_database::database_operations::file_processing::overflow::reading::read_overflow_header;

    let (mut table, dir) = create_test_table("compact_overflow_noop");

    let text = "A".repeat(500);
    table.insert_record(1, make_record(1, &text)).unwrap();

    let overflow_path = format!("{}/items_0.overflow", dir);
    let header_before = read_overflow_header(&overflow_path).unwrap();

    // Compact with no fragmentation
    table.compact_overflow_file(0).unwrap();

    let header_after = read_overflow_header(&overflow_path).unwrap();
    assert_eq!(header_before.get_used_space(), header_after.get_used_space());

    // Record still readable
    let content = table.read_record(1).unwrap();
    assert_eq!(content.get_content()[1], ContentTypes::Text(text));

    cleanup_dir(&dir);
}

#[test]
fn compact_overflow_empty_file_returns_ok() {
    let (mut table, dir) = create_test_table("compact_overflow_empty");

    // No overflow entries — should return Ok immediately
    table.compact_overflow_file(0).unwrap();

    cleanup_dir(&dir);
}

#[test]
fn compact_overflow_survives_reopen() {
    let (mut table, dir) = create_test_table("compact_overflow_reopen");

    let text1 = "A".repeat(500);
    let text2 = "B".repeat(300);
    table.insert_record(1, make_record(1, &text1)).unwrap();
    table.insert_record(2, make_record(2, &text2)).unwrap();
    table.delete_record(1).unwrap();

    table.compact_overflow_file(0).unwrap();

    // Reopen the table — reverse index rebuilt from pages
    let table = Table::open("items".to_string(), dir.clone()).unwrap();

    let content = table.read_record(2).unwrap();
    assert_eq!(content.get_content()[1], ContentTypes::Text(text2));

    assert_eq!(table.get_overflow_reverse().len(), 1);

    cleanup_dir(&dir);
}
