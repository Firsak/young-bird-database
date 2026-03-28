// WAL file layout — entries appended sequentially, no gaps:
//
// Example: txn 1 inserts into "users", txn 2 deletes from "orders", txn 3 crashes mid-way
//
// Offset 0:
//   [entry_size=29][txn_id=1][op=0/Begin][rid=0][table_name_len=0][][data_len=0][]
// Offset 33:
//   [entry_size=34][txn_id=1][op=1/Insert][rid=1][table_name_len=5][users][data_len=N][...record bytes...]
// Offset 67+N:
//   [entry_size=29][txn_id=1][op=4/Commit][rid=0][table_name_len=0][][data_len=0][]
// Offset 100+N:
//   [entry_size=30][txn_id=2][op=2/Delete][rid=7][table_name_len=6][orders][data_len=0][]
// Offset 134+N:
//   [entry_size=29][txn_id=2][op=4/Commit][rid=0][table_name_len=0][][data_len=0][]
// Offset 167+N:
//   [entry_size=34][txn_id=3][op=1/Insert][rid=2][table_name_len=5][users][data_len=M][...
//   ^ crash here — no Commit follows
//
// Recovery reads top-to-bottom:
//   txn 1 → has Commit → replay Insert
//   txn 2 → has Commit → replay Delete
//   txn 3 → no Commit  → skip (discard)

use crate::database_operations::file_processing::traits::BinarySerde;

#[derive(Debug, Clone, PartialEq, Copy)]
pub enum WalOperation {
    Begin = 0,
    Insert = 1,
    Delete = 2,
    Update = 3,
    Commit = 4,
    Rollback = 5,
}

/// A single entry in the write-ahead log.
///
/// Binary layout (variable size):
/// ```text
/// ┌─────────────────────────────────────┐
/// │ entry_size: u32 LE  (4 bytes)       │  ← bytes after this field
/// ├─────────────────────────────────────┤
/// │ transaction_id: u64 LE  (8 bytes)   │
/// ├─────────────────────────────────────┤
/// │ operation: u8  (1 byte)             │  ← 0=Begin..5=Rollback
/// ├─────────────────────────────────────┤
/// │ record_id: u64 LE  (8 bytes)        │  ← 0 for Begin/Commit/Rollback
/// ├─────────────────────────────────────┤
/// │ table_name_len: u32 LE  (4 bytes)   │  ← 0 for Begin/Commit/Rollback
/// ├─────────────────────────────────────┤
/// │ table_name: [u8; table_name_len]    │
/// ├─────────────────────────────────────┤
/// │ data_len: u32 LE  (4 bytes)         │  ← 0 for Delete/Begin/Commit/Rollback
/// ├─────────────────────────────────────┤
/// │ data: [u8; data_len]                │  ← record content for Insert/Update
/// └─────────────────────────────────────┘
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct WalEntry {
    pub transaction_id: u64,
    pub operation: WalOperation,
    pub table_name: String,
    pub record_id: u64,
    pub data: Vec<u8>,
}

impl WalEntry {
    pub fn new(
        transaction_id: u64,
        operation: WalOperation,
        record_id: u64,
        table_name: String,
        data: Vec<u8>,
    ) -> Self {
        WalEntry {
            transaction_id,
            operation,
            table_name,
            record_id,
            data,
        }
    }
}

impl BinarySerde for WalEntry {
    type Output = Vec<u8>;

    fn to_bytes(&self) -> Self::Output {
        let table_name_length = self.table_name.len() as u32;
        let data_length = self.data.len() as u32;
        let total_length: u32 = 8 + 1 + 8 + 4 + table_name_length + 4 + data_length;
        let mut bytes: Vec<u8> = Vec::with_capacity(total_length as usize);
        bytes.extend_from_slice(&total_length.to_le_bytes());
        bytes.extend_from_slice(&self.transaction_id.to_le_bytes());
        bytes.push(self.operation as u8);
        bytes.extend_from_slice(&self.record_id.to_le_bytes());
        bytes.extend_from_slice(&table_name_length.to_le_bytes());
        bytes.extend_from_slice(self.table_name.as_bytes());
        bytes.extend_from_slice(&data_length.to_le_bytes());
        bytes.extend_from_slice(&self.data);

        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String>
    where
        Self: Sized,
    {
        if bytes.is_empty() {
            return Err("WalEntry deserialization failed: byte slice is empty".to_string());
        }

        if bytes.len() < 4 {
            return Err(format!(
                "WalEntry deserialization failed: should be at least {} bytes to understand all data bytes length, got {} bytes",
                4,
                bytes.len()
            ));
        }

        let length: u32 = u32::from_le_bytes(bytes[0..4].try_into().unwrap());

        if bytes[4..].len() < length as usize{
            return Err(format!(
                "WalEntry deserialization failed: expected at least {} bytes, got {} bytes",
                length,
                bytes.len()
            ));
        }

        let transaction_id = u64::from_le_bytes(bytes[4..12].try_into().unwrap());
        let operation = match bytes[12] {
            0 => WalOperation::Begin,
            1 => WalOperation::Insert,
            2 => WalOperation::Delete,
            3 => WalOperation::Update,
            4 => WalOperation::Commit,
            5 => WalOperation::Rollback,
            invalid => {
                return Err(format!(
                    "WalOperation deserialization failed: invalid type tag {}, expected 0-5",
                    invalid
                ))
            }
        };
        let record_id = u64::from_le_bytes(bytes[13..21].try_into().unwrap());
        let table_name_length = u32::from_le_bytes(bytes[21..25].try_into().unwrap());
        let table_name = String::from_utf8(bytes[25..25 + table_name_length as usize].to_vec())
            .map_err(|e| {
                format!(
                    "WalOperation Table name deserialization failed: invalid UTF-8 encoding: {}",
                    e
                )
            })?;
        let data_length = u32::from_le_bytes(
            bytes[25 + table_name_length as usize..29 + table_name_length as usize]
                .try_into()
                .unwrap(),
        );
        let data = bytes[29 + table_name_length as usize
            ..29 + table_name_length as usize + data_length as usize]
            .to_vec();

        Ok(WalEntry::new(
            transaction_id,
            operation,
            record_id,
            table_name,
            data,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wal_operation_roundtrip_all_variants() {
        let variants = [
            (WalOperation::Begin, 0u8),
            (WalOperation::Insert, 1),
            (WalOperation::Delete, 2),
            (WalOperation::Update, 3),
            (WalOperation::Commit, 4),
            (WalOperation::Rollback, 5),
        ];
        for (op, expected_tag) in variants {
            assert_eq!(op as u8, expected_tag);
        }
    }

    #[test]
    fn wal_entry_begin_roundtrip() {
        let entry = WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new());
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_commit_roundtrip() {
        let entry = WalEntry::new(42, WalOperation::Commit, 0, String::new(), Vec::new());
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_rollback_roundtrip() {
        let entry = WalEntry::new(99, WalOperation::Rollback, 0, String::new(), Vec::new());
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_insert_roundtrip() {
        let data = vec![10, 20, 30, 40, 50];
        let entry = WalEntry::new(7, WalOperation::Insert, 100, "users".to_string(), data);
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_delete_roundtrip() {
        let entry = WalEntry::new(7, WalOperation::Delete, 55, "orders".to_string(), Vec::new());
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_update_roundtrip() {
        let data = vec![1, 2, 3];
        let entry = WalEntry::new(3, WalOperation::Update, 200, "products".to_string(), data);
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_byte_layout() {
        let entry = WalEntry::new(1, WalOperation::Insert, 42, "t".to_string(), vec![0xFF]);
        let bytes = entry.to_bytes();

        // entry_size: everything after first 4 bytes
        let entry_size = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
        assert_eq!(entry_size as usize, bytes.len() - 4);

        // transaction_id
        assert_eq!(u64::from_le_bytes(bytes[4..12].try_into().unwrap()), 1);
        // operation
        assert_eq!(bytes[12], 1); // Insert
        // record_id
        assert_eq!(u64::from_le_bytes(bytes[13..21].try_into().unwrap()), 42);
        // table_name_len
        assert_eq!(u32::from_le_bytes(bytes[21..25].try_into().unwrap()), 1);
        // table_name
        assert_eq!(&bytes[25..26], b"t");
        // data_len
        assert_eq!(u32::from_le_bytes(bytes[26..30].try_into().unwrap()), 1);
        // data
        assert_eq!(bytes[30], 0xFF);
    }

    #[test]
    fn wal_entry_empty_bytes_error() {
        assert!(WalEntry::from_bytes(&[]).is_err());
    }

    #[test]
    fn wal_entry_too_short_error() {
        assert!(WalEntry::from_bytes(&[0, 0]).is_err());
    }

    #[test]
    fn wal_entry_truncated_data_error() {
        let entry = WalEntry::new(1, WalOperation::Insert, 1, "t".to_string(), vec![1, 2, 3]);
        let bytes = entry.to_bytes();
        // chop off the last few bytes
        assert!(WalEntry::from_bytes(&bytes[..bytes.len() - 3]).is_err());
    }

    #[test]
    fn wal_entry_invalid_operation_tag() {
        let mut bytes = WalEntry::new(1, WalOperation::Begin, 0, String::new(), Vec::new()).to_bytes();
        bytes[12] = 99; // invalid operation tag
        assert!(WalEntry::from_bytes(&bytes).is_err());
    }

    #[test]
    fn wal_entry_large_data_roundtrip() {
        let data = vec![0xAB; 10_000];
        let entry = WalEntry::new(5, WalOperation::Update, 999, "big_table".to_string(), data);
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }

    #[test]
    fn wal_entry_long_table_name_roundtrip() {
        let name = "a".repeat(500);
        let entry = WalEntry::new(1, WalOperation::Delete, 1, name, Vec::new());
        let bytes = entry.to_bytes();
        let recovered = WalEntry::from_bytes(&bytes).unwrap();
        assert_eq!(entry, recovered);
    }
}
