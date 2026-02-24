use super::traits::BinarySerde;

#[derive(Debug, PartialEq)]
pub enum ContentTypes {
    Null,          //  0
    Boolean(bool), //  1
    Text(String),  //  2
    Int8(i8),      //  3
    Int16(i16),    //  4
    Int32(i32),    //  5
    Int64(i64),    //  6
    UInt8(u8),     //  7
    UInt16(u16),   //  8
    UInt32(u32),   //  9
    UInt64(u64),   // 10
    Float32(f32),  // 11
    Float64(f64),  // 12
}

#[derive(Debug, PartialEq)]
pub enum ColumnTypes {
    Boolean, //  0
    Text,    //  1
    Int8,    //  2
    Int16,   //  3
    Int32,   //  4
    Int64,   //  5
    UInt8,   //  6
    UInt16,  //  7
    UInt32,  //  8
    UInt64,  //  9
    Float32, // 10
    Float64, // 11
}

impl BinarySerde for ColumnTypes {
    type Output = [u8; 1];

    fn to_bytes(&self) -> Self::Output {
        [match self {
            ColumnTypes::Boolean => 0,
            ColumnTypes::Text => 1,
            ColumnTypes::Int8 => 2,
            ColumnTypes::Int16 => 3,
            ColumnTypes::Int32 => 4,
            ColumnTypes::Int64 => 5,
            ColumnTypes::UInt8 => 6,
            ColumnTypes::UInt16 => 7,
            ColumnTypes::UInt32 => 8,
            ColumnTypes::UInt64 => 9,
            ColumnTypes::Float32 => 10,
            ColumnTypes::Float64 => 11,
        }]
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("ColumnTypes deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() != 1 {
            return Err(format!(
                "ColumnTypes deserialization failed: expected exactly 1 byte, got {} bytes",
                bytes.len()
            ));
        }

        match bytes[0] {
            0 => Ok(ColumnTypes::Boolean),
            1 => Ok(ColumnTypes::Text),
            2 => Ok(ColumnTypes::Int8),
            3 => Ok(ColumnTypes::Int16),
            4 => Ok(ColumnTypes::Int32),
            5 => Ok(ColumnTypes::Int64),
            6 => Ok(ColumnTypes::UInt8),
            7 => Ok(ColumnTypes::UInt16),
            8 => Ok(ColumnTypes::UInt32),
            9 => Ok(ColumnTypes::UInt64),
            10 => Ok(ColumnTypes::Float32),
            11 => Ok(ColumnTypes::Float64),
            invalid => Err(format!(
                "ColumnTypes deserialization failed: invalid type tag {}, expected 0-11",
                invalid
            )),
        }
    }
}

impl BinarySerde for ContentTypes {
    type Output = Vec<u8>;

    /// Serializes ContentTypes with format: [type_tag: 1 byte][data: variable]
    /// For Text: [type_tag][is_file_stored: 1 byte][length: 4 bytes][utf8_bytes: length bytes]
    /// Note: is_file_stored is currently always 0 (false) for inline storage
    fn to_bytes(&self) -> Self::Output {
        match self {
            ContentTypes::Null => vec![0],

            ContentTypes::Boolean(val) => {
                vec![1, if *val { 1 } else { 0 }]
            }

            ContentTypes::Text(s) => {
                let mut bytes = vec![2];
                // is_file_stored: always false (0) for now
                let is_file_stored: u8 = 0;
                bytes.push(is_file_stored);
                let str_bytes = s.as_bytes();
                bytes.extend_from_slice(&(str_bytes.len() as u32).to_le_bytes());
                bytes.extend_from_slice(str_bytes);
                bytes
            }

            ContentTypes::Int8(val) => {
                let mut bytes = vec![3];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::Int16(val) => {
                let mut bytes = vec![4];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::Int32(val) => {
                let mut bytes = vec![5];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::Int64(val) => {
                let mut bytes = vec![6];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::UInt8(val) => {
                let mut bytes = vec![7];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::UInt16(val) => {
                let mut bytes = vec![8];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::UInt32(val) => {
                let mut bytes = vec![9];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::UInt64(val) => {
                let mut bytes = vec![10];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::Float32(val) => {
                let mut bytes = vec![11];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }

            ContentTypes::Float64(val) => {
                let mut bytes = vec![12];
                bytes.extend_from_slice(&val.to_le_bytes());
                bytes
            }
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("ContentTypes deserialization failed: byte slice is empty".to_string());
        }

        let type_tag = bytes[0];

        match type_tag {
            0 => {
                if bytes.len() != 1 {
                    return Err(format!(
                        "ContentTypes::Null deserialization failed: expected exactly 1 byte, got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Null)
            }

            1 => {
                if bytes.len() != 2 {
                    return Err(format!(
                        "ContentTypes::Boolean deserialization failed: expected 2 bytes (tag + value), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Boolean(bytes[1] != 0))
            }

            2 => {
                if bytes.len() < 6 {
                    return Err(format!(
                        "ContentTypes::Text deserialization failed: expected at least 6 bytes (tag + is_file_stored + length prefix), got {} bytes",
                        bytes.len()
                    ));
                }
                let is_file_stored = bytes[1];
                if is_file_stored != 0 {
                    return Err(format!(
                        "ContentTypes::Text deserialization failed: file-stored text (is_file_stored={}) is not yet supported",
                        is_file_stored
                    ));
                }
                let len = u32::from_le_bytes(bytes[2..6].try_into().unwrap()) as usize;
                let expected_total = 6 + len;
                if bytes.len() != expected_total {
                    return Err(format!(
                        "ContentTypes::Text deserialization failed: length prefix indicates {} bytes of text, expected total {} bytes, got {} bytes",
                        len, expected_total, bytes.len()
                    ));
                }
                let s = String::from_utf8(bytes[6..].to_vec()).map_err(|e| {
                    format!(
                        "ContentTypes::Text deserialization failed: invalid UTF-8 encoding: {}",
                        e
                    )
                })?;
                Ok(ContentTypes::Text(s))
            }

            3 => {
                if bytes.len() != 2 {
                    return Err(format!(
                        "ContentTypes::Int8 deserialization failed: expected 2 bytes (tag + i8), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Int8(i8::from_le_bytes(
                    bytes[1..2].try_into().unwrap(),
                )))
            }

            4 => {
                if bytes.len() != 3 {
                    return Err(format!(
                        "ContentTypes::Int16 deserialization failed: expected 3 bytes (tag + i16), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Int16(i16::from_le_bytes(
                    bytes[1..3].try_into().unwrap(),
                )))
            }

            5 => {
                if bytes.len() != 5 {
                    return Err(format!(
                        "ContentTypes::Int32 deserialization failed: expected 5 bytes (tag + i32), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Int32(i32::from_le_bytes(
                    bytes[1..5].try_into().unwrap(),
                )))
            }

            6 => {
                if bytes.len() != 9 {
                    return Err(format!(
                        "ContentTypes::Int64 deserialization failed: expected 9 bytes (tag + i64), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Int64(i64::from_le_bytes(
                    bytes[1..9].try_into().unwrap(),
                )))
            }

            7 => {
                if bytes.len() != 2 {
                    return Err(format!(
                        "ContentTypes::UInt8 deserialization failed: expected 2 bytes (tag + u8), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::UInt8(u8::from_le_bytes(
                    bytes[1..2].try_into().unwrap(),
                )))
            }

            8 => {
                if bytes.len() != 3 {
                    return Err(format!(
                        "ContentTypes::UInt16 deserialization failed: expected 3 bytes (tag + u16), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::UInt16(u16::from_le_bytes(
                    bytes[1..3].try_into().unwrap(),
                )))
            }

            9 => {
                if bytes.len() != 5 {
                    return Err(format!(
                        "ContentTypes::UInt32 deserialization failed: expected 5 bytes (tag + u32), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::UInt32(u32::from_le_bytes(
                    bytes[1..5].try_into().unwrap(),
                )))
            }

            10 => {
                if bytes.len() != 9 {
                    return Err(format!(
                        "ContentTypes::UInt64 deserialization failed: expected 9 bytes (tag + u64), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::UInt64(u64::from_le_bytes(
                    bytes[1..9].try_into().unwrap(),
                )))
            }

            11 => {
                if bytes.len() != 5 {
                    return Err(format!(
                        "ContentTypes::Float32 deserialization failed: expected 5 bytes (tag + f32), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Float32(f32::from_le_bytes(
                    bytes[1..5].try_into().unwrap(),
                )))
            }

            12 => {
                if bytes.len() != 9 {
                    return Err(format!(
                        "ContentTypes::Float64 deserialization failed: expected 9 bytes (tag + f64), got {} bytes",
                        bytes.len()
                    ));
                }
                Ok(ContentTypes::Float64(f64::from_le_bytes(
                    bytes[1..9].try_into().unwrap(),
                )))
            }

            invalid => Err(format!(
                "ContentTypes deserialization failed: invalid type tag {}, expected 0-12",
                invalid
            )),
        }
    }
}

// ─────────────────────────────────────────────────────────────
// This block only compiles when running `cargo test`.
// `use super::*` imports everything from the parent scope
// (ColumnTypes, ContentTypes, BinarySerde) so tests can use them.
// ─────────────────────────────────────────────────────────────
#[cfg(test)]
mod tests {
    use super::*;

    // ══════════════════════════════════════════════════════════
    // ColumnTypes tests
    // ══════════════════════════════════════════════════════════

    // EXAMPLE TEST — shows the pattern for all roundtrip tests:
    // 1. Create a value
    // 2. Serialize it with to_bytes()
    // 3. Deserialize it back with from_bytes()
    // 4. Assert the result matches the original
    #[test]
    fn column_types_boolean_roundtrip() {
        let original = ColumnTypes::Boolean;
        let bytes = original.to_bytes(); // serialize: should produce [0]
        assert_eq!(bytes, [0]); // verify raw bytes are correct
        let restored = ColumnTypes::from_bytes(&bytes).unwrap(); // deserialize back
        assert_eq!(restored.to_bytes(), [0]); // verify it matches
    }

    #[test]
    fn column_types_all_variants_roundtrip() {
        let all_colum_types = [
            ColumnTypes::Boolean, //  0
            ColumnTypes::Text,    //  1
            ColumnTypes::Int8,    //  2
            ColumnTypes::Int16,   //  3
            ColumnTypes::Int32,   //  4
            ColumnTypes::Int64,   //  5
            ColumnTypes::UInt8,   //  6
            ColumnTypes::UInt16,  //  7
            ColumnTypes::UInt32,  //  8
            ColumnTypes::UInt64,  //  9
            ColumnTypes::Float32, // 10
            ColumnTypes::Float64, // 11
        ];
        let all_numbers_serde: [u8; 12] = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11];

        for (col_type, number) in all_colum_types.iter().zip(all_numbers_serde.iter()) {
            assert_eq!(col_type.to_bytes(), [*number]);
            assert_eq!(
                col_type.to_bytes(),
                ColumnTypes::from_bytes(&[*number]).unwrap().to_bytes()
            );
        }
    }

    // Test that from_bytes rejects invalid input.
    // from_bytes returns Result<Self, String> — use .is_err() to check it fails.
    #[test]
    fn column_types_invalid_tag() {
        let result = ColumnTypes::from_bytes(&[255]);
        assert!(result.is_err()); // should fail: 255 is not a valid tag (0-11)
    }

    #[test]
    fn column_types_empty_bytes() {
        let result = ColumnTypes::from_bytes(&[]);
        assert!(result.is_err()); // should fail: empty input
    }

    // ══════════════════════════════════════════════════════════
    // ContentTypes tests
    // ══════════════════════════════════════════════════════════

    // Helper: serialize then deserialize, assert full byte-level roundtrip
    fn assert_content_roundtrip(value: ContentTypes) {
        let bytes = value.to_bytes();
        let restored_bytes = ContentTypes::from_bytes(&bytes).unwrap().to_bytes();
        assert_eq!(bytes, restored_bytes);
    }

    #[test]
    fn content_null_roundtrip() {
        let bytes = ContentTypes::Null.to_bytes();
        assert_eq!(bytes, vec![0]);
        assert_content_roundtrip(ContentTypes::Null);
    }

    #[test]
    fn content_boolean_roundtrip() {
        assert_eq!(ContentTypes::Boolean(true).to_bytes(), [1, 1]);
        assert_eq!(ContentTypes::Boolean(false).to_bytes(), [1, 0]);
        assert_content_roundtrip(ContentTypes::Boolean(true));
        assert_content_roundtrip(ContentTypes::Boolean(false));
    }

    #[test]
    fn content_text_roundtrip() {
        let bytes = ContentTypes::Text("Hello world!".to_string()).to_bytes();

        // Verify binary format: [tag][is_file_stored][length LE][utf8]
        assert_eq!(bytes[0], 2);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2..6], 12u32.to_le_bytes());
        assert_eq!(&bytes[6..], b"Hello world!");

        assert_content_roundtrip(ContentTypes::Text("Hello world!".to_string()));
    }

    #[test]
    fn content_text_empty_string() {
        let bytes = ContentTypes::Text("".to_string()).to_bytes();

        assert_eq!(bytes[0], 2);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2..6], 0u32.to_le_bytes());
        assert_eq!(bytes.len(), 6); // no string bytes after the header

        assert_content_roundtrip(ContentTypes::Text("".to_string()));
    }

    #[test]
    fn content_integers_roundtrip() {
        // Signed types: test zero, min, max
        assert_content_roundtrip(ContentTypes::Int8(0));
        assert_content_roundtrip(ContentTypes::Int8(i8::MIN));
        assert_content_roundtrip(ContentTypes::Int8(i8::MAX));
        assert_content_roundtrip(ContentTypes::Int16(i16::MIN));
        assert_content_roundtrip(ContentTypes::Int16(i16::MAX));
        assert_content_roundtrip(ContentTypes::Int32(i32::MIN));
        assert_content_roundtrip(ContentTypes::Int32(i32::MAX));
        assert_content_roundtrip(ContentTypes::Int64(i64::MIN));
        assert_content_roundtrip(ContentTypes::Int64(i64::MAX));

        // Unsigned types: test zero, max
        assert_content_roundtrip(ContentTypes::UInt8(0));
        assert_content_roundtrip(ContentTypes::UInt8(u8::MAX));
        assert_content_roundtrip(ContentTypes::UInt16(u16::MAX));
        assert_content_roundtrip(ContentTypes::UInt32(u32::MAX));
        assert_content_roundtrip(ContentTypes::UInt64(u64::MAX));
    }

    #[test]
    fn content_floats_roundtrip() {
        assert_content_roundtrip(ContentTypes::Float32(3.14));
        assert_content_roundtrip(ContentTypes::Float32(0.0));
        assert_content_roundtrip(ContentTypes::Float32(f32::MIN));
        assert_content_roundtrip(ContentTypes::Float32(f32::MAX));
        assert_content_roundtrip(ContentTypes::Float64(2.71828));
        assert_content_roundtrip(ContentTypes::Float64(0.0));
        assert_content_roundtrip(ContentTypes::Float64(f64::MIN));
        assert_content_roundtrip(ContentTypes::Float64(f64::MAX));
    }

    #[test]
    fn content_invalid_tag() {
        assert!(ContentTypes::from_bytes(&[99]).is_err());
        assert!(ContentTypes::from_bytes(&[255]).is_err());
        assert!(ContentTypes::from_bytes(&[13]).is_err()); // one past last valid tag (12)
    }

    #[test]
    fn content_empty_bytes() {
        assert!(ContentTypes::from_bytes(&[]).is_err());
    }

    #[test]
    fn content_wrong_byte_count() {
        // Boolean: expects exactly 2 bytes
        assert!(ContentTypes::from_bytes(&[1, 0, 0]).is_err());
        // Int8: expects exactly 2 bytes
        assert!(ContentTypes::from_bytes(&[3, 0, 0]).is_err());
        // Int32: expects exactly 5 bytes
        assert!(ContentTypes::from_bytes(&[5, 0, 0]).is_err());
        // Null: expects exactly 1 byte
        assert!(ContentTypes::from_bytes(&[0, 0]).is_err());
    }
}
