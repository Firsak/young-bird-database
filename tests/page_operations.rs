use std::fs;

use young_bird_database::database_operations::file_processing::{
    page::{self, PageHeader, PageRecordContent},
    traits::BinarySerde,
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

#[test]
fn write_and_read_new_page() {
    let filename = &temp_file("write_and_read_new_page");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    // Read it back
    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page");

    // Verify the header has correct initial values
    assert_eq!(page.header.get_records_count(), 0);
    assert_eq!(page.header.get_deleted_records_count(), 0);
    assert_eq!(page.header.get_fragment_space(), 0);
    // free_space = page_size - HEADER_SIZE = 8*1024 - 20 = 8172
    assert_eq!(page.header.get_free_space(), 8 * 1024 - 20);

    cleanup(filename);
}

#[test]
fn write_and_read_page_header() {
    let filename = &temp_file("write_and_read_page_header");
    let page_kbytes: u32 = 8;

    // First create the page so the file has enough space
    page::writing::write_new_page(filename, 0, page_kbytes)
        .expect("Failed to write new page");

    // Overwrite with a custom header
    let custom_header = PageHeader::new(42, 5, 1, 7000, 100);
    page::writing::write_page_header(filename, 0, custom_header, page_kbytes)
        .expect("Failed to write page header");

    // Read just the header back
    let read_header = page::reading::read_page_header(filename, 0, page_kbytes)
        .expect("Failed to read page header");

    assert_eq!(read_header.get_records_count(), 5);
    assert_eq!(read_header.get_deleted_records_count(), 1);
    assert_eq!(read_header.get_free_space(), 7000);
    assert_eq!(read_header.get_fragment_space(), 100);

    cleanup(filename);
}

#[test]
fn add_and_read_single_record() {
    let filename = &temp_file("add_and_read_single_record");
    let page_kbytes: u32 = 8;

    page::writing::write_new_page(filename, 0, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    page::writing::add_new_record(filename, 0, page_kbytes, 1, record)
        .expect("Failed to add record");

    // Read the full page — should have 1 record
    let page =
        page::reading::read_page(filename, 0, page_kbytes).expect("Failed to read page");

    assert_eq!(page.header.get_records_count(), 1);
    assert_eq!(page.header.get_deleted_records_count(), 0);

    // Read the record metadata to check its id
    let metadata = page::reading::read_record_metadata(filename, 0, 0, page_kbytes)
        .expect("Failed to read record metadata");
    assert_eq!(metadata.get_id(), 1);

    cleanup(filename);
}

#[test]
fn add_multiple_records_and_read() {
    let filename = &temp_file("add_multiple_records_and_read");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    page::writing::write_new_page(filename, page_number, page_kbytes)
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
        page::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);
    assert!(page.header.get_free_space() < 8 * 1024 - 20);

    for index in 0..3 {
        let record_metadata = page::reading::read_record_metadata(
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
    let filename = &temp_file("delete_last_record");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    page::writing::write_new_page(filename, page_number, page_kbytes)
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
        page::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);

    let saved_free_space = page.header.get_free_space();

    page::writing::delete_record(filename, page_number, page_kbytes, 2)
        .expect("Failed to delete the record");

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 2);
    assert!(page.header.get_free_space() > saved_free_space);
    assert_eq!(page.header.get_deleted_records_count(), 0);
    assert_eq!(page.header.get_fragment_space(), 0);

    cleanup(filename);
}

#[test]
fn delete_non_last_record() {
    let filename = &temp_file("delete_non_last_record");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    page::writing::write_new_page(filename, page_number, page_kbytes)
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
        page::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3);

    let saved_free_space = page.header.get_free_space();
    let saved_fragmented_space = page.header.get_fragment_space();

    page::writing::delete_record(filename, page_number, page_kbytes, 1)
        .expect("Failed to delete the record");

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    assert_eq!(page.header.get_records_count(), 3); // stays 3: soft delete doesn't decrement
    assert_eq!(page.header.get_free_space(), saved_free_space);
    assert!(page.header.get_fragment_space() > saved_fragmented_space);
    assert_eq!(page.header.get_deleted_records_count(), 1);

    cleanup(filename);
}

#[test]
fn update_record_smaller_content() {
    let filename = &temp_file("update_record_smaller_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    // Write a new empty page
    page::writing::write_new_page(filename, page_number, page_kbytes)
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
        page::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read the page");

    let saved_free_space = page.header.get_free_space();
    let saved_fragmented_space = page.header.get_fragment_space();

    let new_record = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(435),
        ContentTypes::Text("old".to_string()),
    ]);
    page::writing::update_record(filename, page_number, page_kbytes, 1, new_record)
        .expect("Failed updating record");

    let page = page::reading::read_page(filename, page_number, page_kbytes)
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
        page::writing::write_new_page(filename, page, page_kbytes)
            .expect(&format!("Failed to write page {}", page));
    }

    let record = PageRecordContent::new(vec![
        ContentTypes::Int32(999),
        ContentTypes::Text("only on page 1".to_string()),
    ]);
    page::writing::add_new_record(filename, 1, page_kbytes, 1, record)
        .expect("Failed to add record to page 1");

    let header_0 = page::reading::read_page_header(filename, 0, page_kbytes)
        .expect("Failed to read page 0");
    let header_1 = page::reading::read_page_header(filename, 1, page_kbytes)
        .expect("Failed to read page 1");
    let header_2 = page::reading::read_page_header(filename, 2, page_kbytes)
        .expect("Failed to read page 2");

    assert_eq!(header_0.get_records_count(), 0);
    assert_eq!(header_1.get_records_count(), 1);
    assert_eq!(header_2.get_records_count(), 0);

    cleanup(filename);
}

#[test]
fn read_single_record_content() {
    let filename = &temp_file("read_single_record_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    page::writing::add_new_record(filename, page_number, page_kbytes, 1, record)
        .expect("Failed to add record");

    // Read metadata first, then use it to read content
    let metadata =
        page::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
            .expect("Failed to read metadata");
    let content = page::reading::read_record_content(
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
    let filename = &temp_file("read_multiple_records_content");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
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
        page::writing::add_new_record(
            filename,
            page_number,
            page_kbytes,
            index as u64,
            record,
        )
        .expect(&format!("Failed to add record_{}", index));
    }

    let metadata_1 =
        page::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
            .expect("Failed to read metadata");
    let content_1 = page::reading::read_record_content(
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
        page::reading::read_record_metadata(filename, page_number, 1, page_kbytes)
            .expect("Failed to read metadata");
    let content_2 = page::reading::read_record_content(
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
    let filename = &temp_file("read_record_content_after_update");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(42),
        ContentTypes::Text("hello".to_string()),
    ]);

    page::writing::add_new_record(filename, page_number, page_kbytes, 0, record)
        .expect("Failed to add record");

    {
        // Read metadata first, then use it to read content
        let metadata =
            page::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
                .expect("Failed to read metadata");
        let content = page::reading::read_record_content(
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
    page::writing::update_record(filename, page_number, page_kbytes, 0, record)
        .expect("Failed to update record");

    {
        // Read metadata first, then use it to read content
        let metadata =
            page::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
                .expect("Failed to read metadata");
        let content = page::reading::read_record_content(
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

#[test]
fn compact_page_no_fragmentation() {
    let filename = &temp_file("compact_page_no_fragmentation");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record = PageRecordContent::new(vec![
        ContentTypes::Int32(42),
        ContentTypes::Text("no fragmentation".to_string()),
    ]);
    page::writing::add_new_record(filename, page_number, page_kbytes, 1, record)
        .expect("Failed to add record");

    let header_before =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");

    page::writing::compact_page(filename, page_number, page_kbytes)
        .expect("Failed to compact page");

    let header_after =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");

    assert_eq!(header_after.get_records_count(), header_before.get_records_count());
    assert_eq!(header_after.get_free_space(), header_before.get_free_space());
    assert_eq!(header_after.get_fragment_space(), 0);

    cleanup(filename);
}

#[test]
fn compact_page_all_deleted() {
    let filename = &temp_file("compact_page_all_deleted");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    for i in 0..3u64 {
        let record = PageRecordContent::new(vec![
            ContentTypes::Int32(i as i32),
            ContentTypes::Text(format!("record_{}", i)),
        ]);
        page::writing::add_new_record(filename, page_number, page_kbytes, i, record)
            .expect("Failed to add record");
    }

    // Soft-delete records 0 and 1 (non-last)
    page::writing::delete_record(filename, page_number, page_kbytes, 0)
        .expect("Failed to delete record 0");
    page::writing::delete_record(filename, page_number, page_kbytes, 1)
        .expect("Failed to delete record 1");
    // Hard-delete record 2 (last slot)
    page::writing::delete_record(filename, page_number, page_kbytes, 2)
        .expect("Failed to delete record 2");

    let header_before =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");
    assert!(header_before.get_fragment_space() > 0);
    assert_eq!(header_before.get_deleted_records_count(), 2);

    page::writing::compact_page(filename, page_number, page_kbytes)
        .expect("Failed to compact page");

    let header_after =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");

    assert_eq!(header_after.get_records_count(), 0);
    assert_eq!(header_after.get_deleted_records_count(), 0);
    assert_eq!(header_after.get_fragment_space(), 0);
    assert_eq!(header_after.get_free_space(), (8 * 1024 - 20) as u32);

    cleanup(filename);
}

#[test]
fn compact_page_after_soft_delete() {
    let filename = &temp_file("compact_page_after_soft_delete");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_0 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(100),
        ContentTypes::Text("first".to_string()),
    ]);
    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int32(200),
        ContentTypes::Text("second".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(300),
        ContentTypes::Text("third".to_string()),
    ]);

    for (i, record) in [record_0, record_1, record_2].into_iter().enumerate() {
        page::writing::add_new_record(
            filename, page_number, page_kbytes, i as u64, record,
        )
        .expect("Failed to add record");
    }

    // Soft-delete record 0 (non-last)
    page::writing::delete_record(filename, page_number, page_kbytes, 0)
        .expect("Failed to delete record 0");

    let header_before =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");
    assert!(header_before.get_fragment_space() > 0);

    page::writing::compact_page(filename, page_number, page_kbytes)
        .expect("Failed to compact page");

    let header_after =
        page::reading::read_page_header(filename, page_number, page_kbytes)
            .expect("Failed to read header");

    assert_eq!(header_after.get_records_count(), 2);
    assert_eq!(header_after.get_deleted_records_count(), 0);
    assert_eq!(header_after.get_fragment_space(), 0);
    assert!(header_after.get_free_space() > header_before.get_free_space());

    // Verify record 1 content (now at slot 0 after compaction)
    let metadata_0 =
        page::reading::read_record_metadata(filename, page_number, 0, page_kbytes)
            .expect("Failed to read metadata 0");
    let content_0 = page::reading::read_record_content(
        filename, page_number, page_kbytes, &metadata_0,
    )
    .expect("Failed to read content 0");

    let values_0 = content_0.get_content();
    assert_eq!(values_0[0], ContentTypes::Boolean(false));
    assert_eq!(values_0[1], ContentTypes::Int32(200));
    assert_eq!(values_0[2], ContentTypes::Text("second".to_string()));

    // Verify record 2 content (now at slot 1 after compaction)
    let metadata_1 =
        page::reading::read_record_metadata(filename, page_number, 1, page_kbytes)
            .expect("Failed to read metadata 1");
    let content_1 = page::reading::read_record_content(
        filename, page_number, page_kbytes, &metadata_1,
    )
    .expect("Failed to read content 1");

    let values_1 = content_1.get_content();
    assert_eq!(values_1[0], ContentTypes::Boolean(true));
    assert_eq!(values_1[1], ContentTypes::Int32(300));
    assert_eq!(values_1[2], ContentTypes::Text("third".to_string()));

    cleanup(filename);
}

#[test]
fn write_page_roundtrip_multiple_records() {
    let filename = &temp_file("write_page_roundtrip_multiple");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let record_0 = PageRecordContent::new(vec![
        ContentTypes::Boolean(true),
        ContentTypes::Int32(100),
        ContentTypes::Text("first".to_string()),
    ]);
    let record_1 = PageRecordContent::new(vec![
        ContentTypes::Boolean(false),
        ContentTypes::Int64(999_999),
        ContentTypes::Text("second record".to_string()),
    ]);
    let record_2 = PageRecordContent::new(vec![
        ContentTypes::Float64(3.14),
        ContentTypes::Text("third".to_string()),
    ]);

    for (i, record) in [record_0, record_1, record_2].into_iter().enumerate() {
        page::writing::add_new_record(
            filename, page_number, page_kbytes, i as u64, record,
        )
        .expect("Failed to add record");
    }

    // Read the page built by add_new_record, then write it back with write_page
    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page");

    page::writing::write_page(filename, page_number, page_kbytes, &page)
        .expect("Failed to write page");

    // Read back and verify all content survived the roundtrip
    let page_after = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page after write_page");

    assert_eq!(page_after.header.get_records_count(), 3);
    assert_eq!(page_after.header.get_free_space(), page.header.get_free_space());
    assert_eq!(page_after.header.get_fragment_space(), 0);

    // Verify each record's content via metadata-based read
    for i in 0..3 {
        let meta = page::reading::read_record_metadata(
            filename, page_number, i as u64, page_kbytes,
        )
        .expect("Failed to read metadata");
        let content = page::reading::read_record_content(
            filename, page_number, page_kbytes, &meta,
        )
        .expect("Failed to read content");

        let original_content = page.get_record_content_by_metadata_index(i);
        assert_eq!(content.to_bytes(), original_content.to_bytes());
    }

    cleanup(filename);
}

#[test]
fn write_page_empty() {
    let filename = &temp_file("write_page_empty");
    let page_kbytes: u32 = 8;
    let page_number: u64 = 0;

    page::writing::write_new_page(filename, page_number, page_kbytes)
        .expect("Failed to write new page");

    let page = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page");

    // Write the empty page back
    page::writing::write_page(filename, page_number, page_kbytes, &page)
        .expect("Failed to write page");

    let page_after = page::reading::read_page(filename, page_number, page_kbytes)
        .expect("Failed to read page after write_page");

    assert_eq!(page_after.header.get_records_count(), 0);
    assert_eq!(page_after.header.get_free_space(), 8 * 1024 - 20);
    assert_eq!(page_after.header.get_fragment_space(), 0);

    cleanup(filename);
}

#[test]
fn write_page_to_second_page() {
    let filename = &temp_file("write_page_second");
    let page_kbytes: u32 = 8;

    // Create two pages
    page::writing::write_new_page(filename, 0, page_kbytes)
        .expect("Failed to write page 0");
    page::writing::write_new_page(filename, 1, page_kbytes)
        .expect("Failed to write page 1");

    // Add records to page 0
    let record = PageRecordContent::new(vec![
        ContentTypes::Int32(42),
        ContentTypes::Text("on page zero".to_string()),
    ]);
    page::writing::add_new_record(filename, 0, page_kbytes, 1, record)
        .expect("Failed to add record");

    // Read page 0, write it back
    let page_0 = page::reading::read_page(filename, 0, page_kbytes)
        .expect("Failed to read page 0");
    page::writing::write_page(filename, 0, page_kbytes, &page_0)
        .expect("Failed to write page 0");

    // Verify page 1 was not affected
    let page_1 = page::reading::read_page(filename, 1, page_kbytes)
        .expect("Failed to read page 1");
    assert_eq!(page_1.header.get_records_count(), 0);
    assert_eq!(page_1.header.get_free_space(), 8 * 1024 - 20);

    // Verify page 0 content is intact
    let meta = page::reading::read_record_metadata(filename, 0, 0, page_kbytes)
        .expect("Failed to read metadata");
    let content = page::reading::read_record_content(
        filename, 0, page_kbytes, &meta,
    )
    .expect("Failed to read content");
    assert_eq!(content.get_content()[0], ContentTypes::Int32(42));
    assert_eq!(content.get_content()[1], ContentTypes::Text("on page zero".to_string()));

    cleanup(filename);
}
