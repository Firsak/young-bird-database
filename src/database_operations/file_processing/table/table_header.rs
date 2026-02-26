use super::column_def::ColumnDef;
use crate::database_operations::file_processing::traits::BinarySerde;

#[derive(Debug)]
pub struct TableHeader {
    // 10 bytes + columns_count * dynamic bytes
    pages_count: u64,       // 8 bytes
    columns_count: u16,     // 2 bytes
    header: Vec<ColumnDef>, // columns_count * dynamic bytes
}

impl TableHeader {
    pub fn new(pages_count: u64, columns_count: u16, header: Vec<ColumnDef>) -> Self {
        Self {
            pages_count,
            columns_count,
            header,
        }
    }
}

impl BinarySerde for TableHeader {
    type Output = Vec<u8>; // Variable size Vec

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Vec<u8> = self.pages_count.to_le_bytes().to_vec();
        bytes.extend_from_slice(&self.columns_count.to_le_bytes());
        for col in &self.header {
            let col_bytes = col.to_bytes();
            bytes.extend_from_slice(&(col_bytes.len() as u32).to_le_bytes());
            bytes.extend_from_slice(&col_bytes);
        }
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("TableHeader deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() < 10 {
            return Err(format!(
                "TableHeader deserialization failed: expected at least {} bytes (8 for pages_count + 2 for columns_count), got {} bytes",
                10,
                bytes.len()
            ));
        }

        let pages_count = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
        let columns_count = u16::from_le_bytes(bytes[8..10].try_into().unwrap());
        let mut current_total: usize = 10;
        let mut header: Vec<ColumnDef> = vec![];
        for _ in 0..columns_count {
            let len =
                u32::from_le_bytes(bytes[current_total..current_total + 4].try_into().unwrap())
                    as usize;
            current_total += 4 + len;
            if bytes.len() < current_total {
                return Err(format!(
                "TableHeader deserialization failed during ColumnDef deserialization: bytes length {} expected to be not less than {}",
                bytes.len(), current_total
            ));
            }
            header.push(ColumnDef::from_bytes(
                &bytes[current_total - len..current_total],
            )?);
        }

        Ok(Self {
            pages_count,
            columns_count,
            header,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_operations::file_processing::types::ColumnTypes;

    // ══════════════════════════════════════════════════════════
    // TableHeader tests (variable size)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn table_header_roundtrip() {
        let header = TableHeader::new(
            5, // pages_count
            2, // columns_count
            vec![
                ColumnDef::new(ColumnTypes::Int64, false, "id".to_string()),
                ColumnDef::new(ColumnTypes::Text, true, "name".to_string()),
            ],
        );
        let bytes = header.to_bytes();
        let restored = TableHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn table_header_empty_columns() {
        let header = TableHeader::new(1, 0, vec![]);
        let bytes = header.to_bytes();
        assert_eq!(bytes.len(), 10); // 8 (pages_count) + 2 (columns_count)
        let restored = TableHeader::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn table_header_empty_bytes() {
        assert!(TableHeader::from_bytes(&[]).is_err());
    }

    #[test]
    fn table_header_too_short() {
        assert!(TableHeader::from_bytes(&[0; 5]).is_err());
        assert!(TableHeader::from_bytes(&[0; 9]).is_err());
    }
}
