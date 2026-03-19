use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::types::ColumnTypes;

/// Column definition: type, nullability, and name.
/// Serialized as [data_type: 1][nullable: 1][name_len: u32 LE][name: UTF-8].
#[derive(Debug, Clone)]
pub struct ColumnDef {
    data_type: ColumnTypes, // 1 byte
    nullable: bool,         // 1 byte
    name: String,           // dynamic bytes
}

impl ColumnDef {
    pub fn new(data_type: ColumnTypes, nullable: bool, name: String) -> Self {
        Self {
            data_type,
            nullable,
            name,
        }
    }

    pub fn get_data_type(&self) -> &ColumnTypes {
        &self.data_type
    }

    pub fn get_nullable(&self) -> bool {
        self.nullable
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }
}

impl BinarySerde for ColumnDef {
    type Output = Vec<u8>; // Variable size Vec

    fn to_bytes(&self) -> Self::Output {
        let mut bytes: Vec<u8> = vec![self.data_type.to_bytes()[0]];
        bytes.push(if self.nullable { 1_u8 } else { 0_u8 });
        let name_bytes = &self.name.as_bytes();
        bytes.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
        bytes.extend_from_slice(name_bytes);
        bytes
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.is_empty() {
            return Err("ColumnDef deserialization failed: byte slice is empty".to_string());
        }
        if bytes.len() <= 6 {
            return Err(format!(
                "ColumnDef deserialization failed: expected to be more than {} bytes, got {} bytes",
                6,
                bytes.len()
            ));
        }

        let data_type = ColumnTypes::from_bytes(bytes[0..1].try_into().unwrap())?;
        let nullable_bytes = bytes[1];
        let nullable = match nullable_bytes {
            0 => false,
            1 => true,
            _ => return Err("ColumnDef nullable byte should be 0 or 1".to_string()),
        };
        let len = u32::from_le_bytes(bytes[2..6].try_into().unwrap()) as usize;
        let expected_total = 6 + len;
        if bytes.len() != expected_total {
            return Err(format!(
                "ColumnDef deserialization failed: length name prefix indicates {} bytes of text, expected total {} bytes, got {} bytes",
                len, expected_total, bytes.len()
            ));
        }
        let name = String::from_utf8(bytes[6..].to_vec()).map_err(|e| {
            format!(
                "ColumnDef deserialization failed: invalid UTF-8 encoding: {}",
                e
            )
        })?;

        Ok(Self {
            name,
            data_type,
            nullable,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ══════════════════════════════════════════════════════════
    // ColumnDef tests (variable size)
    // ══════════════════════════════════════════════════════════

    #[test]
    fn column_def_roundtrip() {
        let col = ColumnDef::new(ColumnTypes::Int32, true, "age".to_string());
        let bytes = col.to_bytes();
        let restored = ColumnDef::from_bytes(&bytes).unwrap();
        assert_eq!(restored.to_bytes(), bytes);
    }

    #[test]
    fn column_def_byte_layout() {
        let column = ColumnDef::new(ColumnTypes::Text, false, "email".to_string());

        let bytes = column.to_bytes();

        assert_eq!(bytes[0], 1);
        assert_eq!(bytes[1], 0);
        assert_eq!(bytes[2..6], 5u32.to_le_bytes());
        assert_eq!(&bytes[6..], b"email");
    }

    #[test]
    fn column_def_empty_bytes() {
        assert!(ColumnDef::from_bytes(&[]).is_err());
    }
}
