use std::fs;
use young_bird_database::database_operations::file_processing::overflow::writing::{
    create_overflow_file, append_overflow_text, rewrite_overflow_file,
};
use young_bird_database::database_operations::file_processing::overflow::reading::{
    read_overflow_header, read_overflow_text,
};
const OVERFLOW_HEADER_SIZE: usize = 16;

#[test]
fn create_overflow_file_has_correct_header() {
    let filename = "/tmp/test_overflow_create.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();

    let bytes = fs::read(filename).unwrap();
    assert_eq!(bytes.len(), OVERFLOW_HEADER_SIZE);
    // used_space should be OVERFLOW_HEADER_SIZE (16)
    let used_space = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    assert_eq!(used_space, OVERFLOW_HEADER_SIZE as u64);
    // fragmented_space should be 0
    let fragmented = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    assert_eq!(fragmented, 0);

    let _ = fs::remove_file(filename);
}

#[test]
fn append_text_writes_correct_bytes() {
    let filename = "/tmp/test_overflow_append_bytes.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let text = "Hello overflow!";
    let overflow_ref = append_overflow_text(filename, 0, text, 1024).unwrap();

    // OverflowRef should point to offset = OVERFLOW_HEADER_SIZE, length = text.len()
    assert_eq!(overflow_ref.get_offset(), OVERFLOW_HEADER_SIZE as u64);
    assert_eq!(overflow_ref.get_length(), text.len() as u32);
    assert_eq!(overflow_ref.get_file_index(), 0);

    // Read raw file and verify the text bytes are at the expected offset
    let bytes = fs::read(filename).unwrap();
    assert_eq!(bytes.len(), OVERFLOW_HEADER_SIZE + text.len());
    let stored_text = std::str::from_utf8(&bytes[OVERFLOW_HEADER_SIZE..]).unwrap();
    assert_eq!(stored_text, text);

    // Header should reflect updated used_space
    let used_space = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    assert_eq!(used_space, (OVERFLOW_HEADER_SIZE + text.len()) as u64);

    let _ = fs::remove_file(filename);
}

#[test]
fn append_multiple_texts_preserves_all() {
    let filename = "/tmp/test_overflow_append_multi.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();

    let text1 = "First text entry";
    let text2 = "Second text entry here";

    let ref1 = append_overflow_text(filename, 0, text1, 4096).unwrap();
    let ref2 = append_overflow_text(filename, 0, text2, 4096).unwrap();

    // ref2 should start right after ref1
    assert_eq!(ref2.get_offset(), ref1.get_offset() + text1.len() as u64);

    // Read raw file and verify both texts are intact
    let bytes = fs::read(filename).unwrap();
    let expected_size = OVERFLOW_HEADER_SIZE + text1.len() + text2.len();
    assert_eq!(bytes.len(), expected_size);

    let stored1 = std::str::from_utf8(
        &bytes[ref1.get_offset() as usize..(ref1.get_offset() + ref1.get_length() as u64) as usize],
    ).unwrap();
    let stored2 = std::str::from_utf8(
        &bytes[ref2.get_offset() as usize..(ref2.get_offset() + ref2.get_length() as u64) as usize],
    ).unwrap();
    assert_eq!(stored1, text1);
    assert_eq!(stored2, text2);

    let _ = fs::remove_file(filename);
}

#[test]
fn append_rejects_when_file_full() {
    let filename = "/tmp/test_overflow_full.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();

    // max_file_size = 32 bytes (16 header + 16 data). Text of 20 bytes won't fit.
    let text = "This is 20 bytes!!!!";
    assert_eq!(text.len(), 20);
    let result = append_overflow_text(filename, 0, text, 32);
    assert!(result.is_err());

    let _ = fs::remove_file(filename);
}

#[test]
fn read_header_from_fresh_file() {
    let filename = "/tmp/test_overflow_read_header.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let header = read_overflow_header(filename).unwrap();

    assert_eq!(header.get_used_space(), OVERFLOW_HEADER_SIZE as u64);
    assert_eq!(header.get_fragmented_space(), 0);

    let _ = fs::remove_file(filename);
}

#[test]
fn read_header_after_append() {
    let filename = "/tmp/test_overflow_read_header_after.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    append_overflow_text(filename, 0, "some text here", 4096).unwrap();

    let header = read_overflow_header(filename).unwrap();
    assert_eq!(header.get_used_space(), (OVERFLOW_HEADER_SIZE + 14) as u64);
    assert_eq!(header.get_fragmented_space(), 0);

    let _ = fs::remove_file(filename);
}

#[test]
fn read_text_single_entry() {
    let filename = "/tmp/test_overflow_read_text.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let text = "Hello from overflow storage!";
    let overflow_ref = append_overflow_text(filename, 0, text, 4096).unwrap();

    let result = read_overflow_text(filename, &overflow_ref).unwrap();
    assert_eq!(result, text);

    let _ = fs::remove_file(filename);
}

#[test]
fn read_text_multiple_entries() {
    let filename = "/tmp/test_overflow_read_multi.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();

    let texts = vec![
        "First entry",
        "Second entry with more data",
        "Third",
    ];

    let refs: Vec<_> = texts.iter()
        .map(|t| append_overflow_text(filename, 0, t, 4096).unwrap())
        .collect();

    // Read back in reverse order to verify random access works
    for (i, overflow_ref) in refs.iter().enumerate().rev() {
        let result = read_overflow_text(filename, overflow_ref).unwrap();
        assert_eq!(result, texts[i]);
    }

    let _ = fs::remove_file(filename);
}

// --- rewrite_overflow_file tests ---

#[test]
fn rewrite_preserves_all_live_entries() {
    let filename = "/tmp/test_overflow_rewrite_all.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let ref1 = append_overflow_text(filename, 0, "Alpha", 4096).unwrap();
    let ref2 = append_overflow_text(filename, 0, "Beta", 4096).unwrap();
    let ref3 = append_overflow_text(filename, 0, "Gamma", 4096).unwrap();

    let entries = vec![
        (ref1.get_offset(), ref1.get_length()),
        (ref2.get_offset(), ref2.get_length()),
        (ref3.get_offset(), ref3.get_length()),
    ];
    let map = rewrite_overflow_file(filename, 0, entries).unwrap();

    // All three should be in the map
    assert_eq!(map.len(), 3);

    // Read back each via its new ref
    let new_ref1 = map.get(&ref1.get_offset()).unwrap();
    let new_ref2 = map.get(&ref2.get_offset()).unwrap();
    let new_ref3 = map.get(&ref3.get_offset()).unwrap();

    assert_eq!(read_overflow_text(filename, new_ref1).unwrap(), "Alpha");
    assert_eq!(read_overflow_text(filename, new_ref2).unwrap(), "Beta");
    assert_eq!(read_overflow_text(filename, new_ref3).unwrap(), "Gamma");

    let _ = fs::remove_file(filename);
}

#[test]
fn rewrite_eliminates_dead_entry() {
    let filename = "/tmp/test_overflow_rewrite_dead.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let ref1 = append_overflow_text(filename, 0, "Keep this", 4096).unwrap();
    let ref2 = append_overflow_text(filename, 0, "Delete this", 4096).unwrap();
    let ref3 = append_overflow_text(filename, 0, "Keep this too", 4096).unwrap();

    // Only pass ref1 and ref3 as live — ref2 is "dead"
    let entries = vec![
        (ref1.get_offset(), ref1.get_length()),
        (ref3.get_offset(), ref3.get_length()),
    ];
    let map = rewrite_overflow_file(filename, 0, entries).unwrap();

    assert_eq!(map.len(), 2);
    assert!(map.get(&ref2.get_offset()).is_none());

    let new_ref1 = map.get(&ref1.get_offset()).unwrap();
    let new_ref3 = map.get(&ref3.get_offset()).unwrap();
    assert_eq!(read_overflow_text(filename, new_ref1).unwrap(), "Keep this");
    assert_eq!(read_overflow_text(filename, new_ref3).unwrap(), "Keep this too");

    // File should be smaller: header + "Keep this" + "Keep this too" only
    let header = read_overflow_header(filename).unwrap();
    assert_eq!(
        header.get_used_space(),
        OVERFLOW_HEADER_SIZE as u64 + "Keep this".len() as u64 + "Keep this too".len() as u64
    );
    assert_eq!(header.get_fragmented_space(), 0);

    let _ = fs::remove_file(filename);
}

#[test]
fn rewrite_packs_contiguously() {
    let filename = "/tmp/test_overflow_rewrite_contiguous.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let _ref1 = append_overflow_text(filename, 0, "AAAA", 4096).unwrap();
    let ref2 = append_overflow_text(filename, 0, "BB", 4096).unwrap();
    let _ref3 = append_overflow_text(filename, 0, "CCCCCC", 4096).unwrap();
    let ref4 = append_overflow_text(filename, 0, "DDD", 4096).unwrap();

    // Keep only ref2 and ref4 (skip ref1 and ref3)
    let entries = vec![
        (ref2.get_offset(), ref2.get_length()),
        (ref4.get_offset(), ref4.get_length()),
    ];
    let map = rewrite_overflow_file(filename, 0, entries).unwrap();

    let new_ref2 = map.get(&ref2.get_offset()).unwrap();
    let new_ref4 = map.get(&ref4.get_offset()).unwrap();

    // Should be packed right after header: BB at 16, DDD at 18
    assert_eq!(new_ref2.get_offset(), OVERFLOW_HEADER_SIZE as u64);
    assert_eq!(new_ref4.get_offset(), OVERFLOW_HEADER_SIZE as u64 + 2);

    // File size should be header + 2 + 3 = 21
    let bytes = fs::read(filename).unwrap();
    assert_eq!(bytes.len(), OVERFLOW_HEADER_SIZE + 5);

    let _ = fs::remove_file(filename);
}

#[test]
fn rewrite_empty_entries_produces_fresh_file() {
    let filename = "/tmp/test_overflow_rewrite_empty.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    append_overflow_text(filename, 0, "Will be removed", 4096).unwrap();

    // Pass no live entries
    let map = rewrite_overflow_file(filename, 0, vec![]).unwrap();
    assert_eq!(map.len(), 0);

    let header = read_overflow_header(filename).unwrap();
    assert_eq!(header.get_used_space(), OVERFLOW_HEADER_SIZE as u64);
    assert_eq!(header.get_fragmented_space(), 0);

    let bytes = fs::read(filename).unwrap();
    assert_eq!(bytes.len(), OVERFLOW_HEADER_SIZE);

    let _ = fs::remove_file(filename);
}

#[test]
fn rewrite_preserves_file_index_in_refs() {
    let filename = "/tmp/test_overflow_rewrite_file_idx.overflow";
    let _ = fs::remove_file(filename);

    create_overflow_file(filename).unwrap();
    let ref1 = append_overflow_text(filename, 3, "test", 4096).unwrap();

    let entries = vec![(ref1.get_offset(), ref1.get_length())];
    let map = rewrite_overflow_file(filename, 3, entries).unwrap();

    let new_ref = map.get(&ref1.get_offset()).unwrap();
    assert_eq!(new_ref.get_file_index(), 3);

    let _ = fs::remove_file(filename);
}
