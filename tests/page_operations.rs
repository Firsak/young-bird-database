use std::fs;

use young_bird_database::database_operations::file_processing::{
    self,
    table::{PageHeader, PageRecordContent},
    types::ContentTypes,
};

/// Helper: generates a unique temp filename per test to avoid conflicts.
/// Rust runs tests in parallel by default, so each test needs its own file.
fn temp_file(test_name: &str) -> String {
    format!("test_integration_{}.dat", test_name)
}

/// Helper: cleans up the temp file after a test.
/// Called at the end of each test. Using .ok() so it doesn't panic if file doesn't exist.
fn cleanup(filename: &str) {
    fs::remove_file(filename).ok();
}

// ─────────────────────────────────────────────
// Example: converted from test_create_new_pages.rs bin
// Shows the pattern: create file → write → read back → assert → cleanup
// ─────────────────────────────────────────────

#[test]
fn write_and_read_new_page() {
    let filename = &temp_file("write_and_read_new_page");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    // Read it back
    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page");

    // Verify the header has correct initial values
    assert_eq!(page.header.get_records_count(), 0);
    assert_eq!(page.header.get_deleted_records_count(), 0);
    assert_eq!(page.header.get_fragment_space(), 0);
    // free_space = page_size - HEADER_SIZE = 8*1024 - 20 = 8172
    assert_eq!(page.header.get_free_space(), 8 * 1024 - 20);

    cleanup(filename);
}

// ─────────────────────────────────────────────
// Example: write a custom header, read it back
// Converted from test_write_page_header.rs bin
// ─────────────────────────────────────────────

#[test]
fn write_and_read_page_header() {
    let filename = &temp_file("write_and_read_page_header");
    let page_kbytes: u32 = 8;

    // First create the page so the file has enough space
    file_processing::writing::write_new_page(filename, 0, page_kbytes)
        .expect("Failed to write new page");

    // Overwrite with a custom header
    let custom_header = PageHeader::new(42, 5, 1, 7000, 100);
    file_processing::writing::write_page_header(filename, 0, custom_header, page_kbytes)
        .expect("Failed to write page header");

    // Read just the header back
    let read_header = file_processing::reading::read_page_header(filename, 0, page_kbytes)
        .expect("Failed to read page header");

    assert_eq!(read_header.get_records_count(), 5);
    assert_eq!(read_header.get_deleted_records_count(), 1);
    assert_eq!(read_header.get_free_space(), 7000);
    assert_eq!(read_header.get_fragment_space(), 100);

    cleanup(filename);
}

// ─────────────────────────────────────────────
// Example: add a record, read the full page back
// ─────────────────────────────────────────────

#[test]
fn add_and_read_single_record() {
    let filename = &temp_file("add_and_read_single_record");
    let page_kbytes: u32 = 8;

    file_processing::writing::write_new_page(filename, 0, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    file_processing::writing::add_new_record(filename, 0, page_kbytes, 1, record)
        .expect("Failed to add record");

    // Read the full page — should have 1 record
    let page =
        file_processing::reading::read_page(filename, 0, page_kbytes).expect("Failed to read page");

    assert_eq!(page.header.get_records_count(), 1);
    assert_eq!(page.header.get_deleted_records_count(), 0);

    // Read the record metadata to check its id
    let metadata = file_processing::reading::read_record_metadata(filename, 0, 0, page_kbytes)
        .expect("Failed to read record metadata");
    assert_eq!(metadata.get_id(), 1);

    cleanup(filename);
}

// ─────────────────────────────────────────────
// YOUR TURN: implement the tests below
// ─────────────────────────────────────────────

#[test]
fn add_multiple_records_and_read() {
    // TODO(human): Create a page, add 2-3 records with different content types,
    // then read the page back. Verify:
    // - records_count matches the number added
    // - free_space decreased (it started at 8*1024 - 20)
    // - each record's metadata has the correct id
    // Don't forget cleanup(filename) at the end!
    let filename = &temp_file("add_multiple_records_and_read");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("world".to_string()),
    ]);
    let record_3 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(0),
        ContentTypes::Text("".to_string()),
    ]);

    for (index, record) in [record_1, record_2, record_3].into_iter().enumerate() {
        file_processing::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);
    assert!(page.header.get_free_space() < 8 * 1024 - 20);

    for index in 0..3 {
        let record_metadata = file_processing::reading::read_record_metadata(
            filename,
            page_number,
            index as u64,
            page_kbytes,
        )
        .expect(&format!("Failed reading the record {} metadata", index));
        assert_eq!(record_metadata.get_id(), index);
    }

    cleanup(filename);
}

#[test]
fn delete_last_record() {
    // TODO(human): Create a page, add 2 records, then delete the LAST one (record id 2).
    // Deleting the last record is a "hard delete" — records_count decreases, free_space increases.
    // Verify:
    // - records_count is 1
    // - deleted_records_count is still 0 (hard delete doesn't increment this)
    // - free_space increased back
    let filename = &temp_file("delete_last_record");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("world".to_string()),
    ]);
    let record_3 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(0),
        ContentTypes::Text("".to_string()),
    ]);

    for (index, record) in [record_1, record_2, record_3].into_iter().enumerate() {
        file_processing::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);

    let saved_free_space = page.header.get_free_space();

    file_processing::writing::delete_record(filename, page_number, page_kbytes, 2)
        .expect("Failed to delete the record");

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 2);
    assert!(page.header.get_free_space() > saved_free_space);
    assert_eq!(page.header.get_deleted_records_count(), 0);
    assert_eq!(page.header.get_fragment_space(), 0);

    cleanup(filename);
}

#[test]
fn delete_non_last_record() {
    // TODO(human): Create a page, add 2 records (ids 1 and 2), then delete record id 1.
    // Deleting a non-last record is a "soft delete" — it gets marked as deleted,
    // fragmented_space increases, but the slot stays.
    // Verify:
    // - records_count decreased by 1
    // - deleted_records_count is 1
    // - fragment_space > 0
    let filename = &temp_file("delete_non_last_record");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("world".to_string()),
    ]);
    let record_3 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(0),
        ContentTypes::Text("".to_string()),
    ]);

    for (index, record) in [record_1, record_2, record_3].into_iter().enumerate() {
        file_processing::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);

    let saved_free_space = page.header.get_free_space();
    let saved_fragmented_space = page.header.get_fragment_space();

    file_processing::writing::delete_record(filename, page_number, page_kbytes, 1)
        .expect("Failed to delete the record");

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 2);
    assert_eq!(page.header.get_free_space(), saved_free_space);
    assert!(page.header.get_fragment_space() > saved_fragmented_space);
    assert_eq!(page.header.get_deleted_records_count(), 1);

    cleanup(filename);
}

#[test]
fn update_record_smaller_content() {
    // TODO(human): Create a page, add a record with Text("hello world"),
    // then update it with Text("hi"). The new content is smaller so it fits in place.
    // Verify the header's free_space or fragmented_space changed appropriately.
    let filename = &temp_file("update_record_smaller_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("world".to_string()),
    ]);
    let record_3 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(0),
        ContentTypes::Text("".to_string()),
    ]);

    for (index, record) in [record_1, record_2, record_3].into_iter().enumerate() {
        file_processing::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    let saved_free_space = page.header.get_free_space();
    let saved_fragmented_space = page.header.get_fragment_space();

    let new_record = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("old".to_string()),
    ]);
    file_processing::writing::update_record(filename, page_number, page_kbytes, 1, new_record)
        .expect("Failed updating record");

    let page = file_processing::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_free_space(), saved_free_space);
    assert!(page.header.get_fragment_space() > saved_fragmented_space);

    cleanup(filename);
}

#[test]
fn multiple_pages_independent() {
    let filename = &temp_file("multiple_pages_independent");
    let page_kbytes: u32 = 8;

    for page in 0..3u64 {
        file_processing::writing::write_new_page(filename, page, page_kbytes)
            .expect(&format!("Failed to write page {}", page));
    }

    let record = PageRecordContent::new(vec![
        ContentTypes::Int32(999),
        ContentTypes::Text("only on page 1".to_string()),
    ]);
    file_processing::writing::add_new_record(filename, 1, page_kbytes, 1, record)
        .expect("Failed to add record to page 1");

    let header_0 = file_processing::reading::read_page_header(filename, 0, page_kbytes)
        .expect("Failed to read page 0");
    let header_1 = file_processing::reading::read_page_header(filename, 1, page_kbytes)
        .expect("Failed to read page 1");
    let header_2 = file_processing::reading::read_page_header(filename, 2, page_kbytes)
        .expect("Failed to read page 2");

    assert_eq!(header_0.get_records_count(), 0);
    assert_eq!(header_1.get_records_count(), 1);
    assert_eq!(header_2.get_records_count(), 0);

    cleanup(filename);
}

// ─────────────────────────────────────────────
// read_record_content tests
// ─────────────────────────────────────────────

#[test]
fn read_single_record_content() {
    let filename = &temp_file("read_single_record_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    file_processing::writing::add_new_record(filename, page_number, page_kbytes, 1, record)
        .expect("Failed to add record");

    // Read metadata first, then use it to read content
    let metadata =
        file_processing::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
            .expect("Failed to read metadata");
    let content = file_processing::reading::read_record_content(
        filename,
        page_number,
        page_kbytes,
        &metadata,
    )
    .expect("Failed to read record content");

    let values = content.get_content();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0], ContentTypes::Boolean(true));
    assert_eq!(values[1], ContentTypes::Int32(42));
    assert_eq!(values[2], ContentTypes::Text("hello".to_string()));

    cleanup(filename);
}

#[test]
fn read_multiple_records_content() {
    // TODO(human): Create a page, add 2 records with different content:
    //   record 1: [Null, Int64(999999), Text("first")]
    //   record 2: [Boolean(false), UInt8(255), Text("second")]
    // Read each record's content back using read_record_metadata + read_record_content
    // and verify with get_content() that the values match what was written.
    // Hint: metadata index 0 → record 1, index 1 → record 2
    let filename = &temp_file("read_multiple_records_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("world".to_string()),
    ]);

    for (index, record) in [record_1, record_2].into_iter().enumerate() {
        file_processing::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let metadata_1 =
        file_processing::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
            .expect("Failed to read metadata");
    let content_1 = file_processing::reading::read_record_content(
        filename,
        page_number,
        page_kbytes,
        &metadata_1,
    )
    .expect("Failed to read the record content");

    assert_eq!(content_1.get_content()[0], ContentTypes::Boolean(true));
    assert_eq!(content_1.get_content()[1], ContentTypes::Int32(42));
    assert_eq!(
        content_1.get_content()[2],
        ContentTypes::Text("hello".to_string())
    );

    let metadata_2 =
        file_processing::reading::read_record_metadata(filename, page_number, 1, page_kbytes)
            .expect("Failed to read metadata");
    let content_2 = file_processing::reading::read_record_content(
        filename,
        page_number,
        page_kbytes,
        &metadata_2,
    )
    .expect("Failed to read the record content");

    assert_eq!(content_2.get_content()[0], ContentTypes::Boolean(false));
    assert_eq!(content_2.get_content()[1], ContentTypes::Int32(435));
    assert_eq!(
        content_2.get_content()[2],
        ContentTypes::Text("world".to_string())
    );

    cleanup(filename);
}

#[test]
fn read_record_content_after_update() {
    // TODO(human): Create a page, add a record with Text("original"),
    // update it with Text("updated") using update_record,
    // then read the content back and verify it returns "updated", not "original".
    // Hint: after update, the metadata offset may have changed,
    // so read fresh metadata before reading content.
    let filename = &temp_file("read_record_content_after_update");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    file_processing::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    file_processing::writing::add_new_record(filename, page_number, page_kbytes, 0, record)
        .expect("Failed to add record");

    {
        // Read metadata first, then use it to read content
        let metadata =
            file_processing::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
                .expect("Failed to read metadata");
        let content = file_processing::reading::read_record_content(
            filename,
            page_number,
            page_kbytes,
            &metadata,
        )
        .expect("Failed to read record content");

        let values = content.get_content();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], ContentTypes::Boolean(true));
        assert_eq!(values[1], ContentTypes::Int32(42));
        assert_eq!(values[2], ContentTypes::Text("hello".to_string()));
    }

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(422),
        ContentTypes::Text("hello world".to_string()),
    ]);
    file_processing::writing::update_record(filename, page_number, page_kbytes, 0, record)
        .expect("Failed to update record");

    {
        // Read metadata first, then use it to read content
        let metadata =
            file_processing::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
                .expect("Failed to read metadata");
        let content = file_processing::reading::read_record_content(
            filename,
            page_number,
            page_kbytes,
            &metadata,
        )
        .expect("Failed to read record content");

        let values = content.get_content();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], ContentTypes::Boolean(false));
        assert_eq!(values[1], ContentTypes::Int32(422));
        assert_eq!(values[2], ContentTypes::Text("hello world".to_string()));
    }

    cleanup(filename);
}
