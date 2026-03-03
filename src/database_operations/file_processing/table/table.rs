use super::table_header::TableHeader;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::index::reading::read_index;
use crate::database_operations::file_processing::index::writing::write_index;
use crate::database_operations::file_processing::index::HashIndex;
use crate::database_operations::file_processing::page::reading::{
    read_page_header, read_record_content, read_record_metadata,
};
use crate::database_operations::file_processing::page::record::PageRecordContent;
use crate::database_operations::file_processing::page::writing::{
    add_new_record, delete_record as page_delete_record, update_record as page_update_record,
    write_new_page,
};
use crate::database_operations::file_processing::table::reading::read_table_header;
use crate::database_operations::file_processing::table::writing::write_table_header;
use crate::database_operations::file_processing::table::ColumnDef;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::types::{ColumnTypes, ContentTypes};
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES, PAGE_RECORD_METADATA_SIZE};

/// High-level Table API. Wraps a TableHeader and resolves
/// global page numbers to concrete (filename, local_page) pairs.
#[derive(Debug)]
pub struct Table {
    name: String,
    base_path: String,
    pages_per_file: u32,
    header: TableHeader,
    index: HashIndex,
}

/// Result of resolving a global page number: which file and which
/// page within that file.
#[derive(Debug, PartialEq)]
pub struct ResolvedPage {
    pub filename: String,
    pub local_page_index: u64,
}

impl Table {
    pub fn new(
        name: String,
        base_path: String,
        pages_per_file: u32,
        header: TableHeader,
        index: HashIndex,
    ) -> Self {
        Self {
            name,
            base_path,
            pages_per_file,
            header,
            index,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Returns the path to this table's .meta file.
    fn meta_path(&self) -> String {
        format!("{}/{}.meta", self.base_path, self.name)
    }

    /// Returns the path to a .dat file by file index.
    fn dat_path(&self, file_index: u64) -> String {
        format!("{}/{}_{}.dat", self.base_path, self.name, file_index)
    }

    /// Returns the path to this table's .idx file.
    fn idx_path(&self) -> String {
        format!("{}/{}.idx", self.base_path, self.name)
    }

    /// Persists the current in-memory header to the .meta file.
    fn save_header(&self) -> Result<(), DatabaseError> {
        write_table_header(&self.meta_path(), &self.header)
    }

    /// Persists the current in-memory index to the .idx file.
    fn save_index(&self) -> Result<(), DatabaseError> {
        write_index(&self.idx_path(), &self.index)
    }

    /// Opens an existing table by reading its .meta file.
    pub fn open(
        name: String,
        base_path: String,
        pages_per_file: u32,
    ) -> Result<Self, DatabaseError> {
        if name.trim().is_empty() {
            return Err(DatabaseError::InvalidArgument(
                "Table name must not be empty".to_string(),
            ));
        }
        if pages_per_file < 1 {
            return Err(DatabaseError::InvalidArgument(
                "Pages per file should be more than 0".to_string(),
            ));
        }
        let meta_path = format!("{}/{}.meta", base_path, name);
        let header = read_table_header(&meta_path)?;
        let hash_index_path = format!("{}/{}.idx", base_path, name);
        let index = read_index(&hash_index_path)?;
        Ok(Self {
            name,
            base_path,
            pages_per_file,
            header,
            index,
        })
    }

    /// Creates a new table: writes the .meta file and the first .dat file
    /// with one empty page.
    pub fn create(
        name: String,
        base_path: String,
        pages_per_file: u32,
        page_kbytes: u32,
        columns: Vec<super::column_def::ColumnDef>,
    ) -> Result<Self, DatabaseError> {
        if name.trim().is_empty() {
            return Err(DatabaseError::InvalidArgument(
                "Table name must not be empty".to_string(),
            ));
        }
        if pages_per_file < 1 {
            return Err(DatabaseError::InvalidArgument(
                "Pages per file should be more than 0".to_string(),
            ));
        }
        if page_kbytes < 1 {
            return Err(DatabaseError::InvalidArgument(
                "Page size should be at least 1 KB".to_string(),
            ));
        }
        if columns.is_empty() {
            return Err(DatabaseError::InvalidArgument(
                "Table must have at least one column".to_string(),
            ));
        }
        for col in &columns {
            if col.get_name().trim().is_empty() {
                return Err(DatabaseError::InvalidArgument(
                    "Column name must not be empty".to_string(),
                ));
            }
        }
        let table_header = TableHeader::new(1, columns.len() as u16, page_kbytes, columns);
        let index = HashIndex::new(16);
        let table = Table::new(name, base_path, pages_per_file, table_header, index);
        table.save_header()?;
        write_new_page(&table.dat_path(0), 0, page_kbytes)?;
        table.save_index()?;
        Ok(table)
    }

    pub fn get_header(&self) -> &TableHeader {
        &self.header
    }

    pub fn get_header_mut(&mut self) -> &mut TableHeader {
        &mut self.header
    }

    /// Checks that a non-null value's type matches the column definition.
    /// Null values should be handled by the caller (validate_record) before
    /// calling this — reaching Null here is a programming error (InvalidArgument).
    fn compare_column_def_and_value_helper(
        column_def: &ColumnDef,
        value: &ContentTypes,
    ) -> Result<(), DatabaseError> {
        match (value, column_def.get_data_type()) {
            // All valid (ContentTypes variant, ColumnTypes variant) pairs
            (ContentTypes::Boolean(_), ColumnTypes::Boolean) => Ok(()),
            (ContentTypes::Text(_), ColumnTypes::Text) => Ok(()),
            (ContentTypes::Int8(_), ColumnTypes::Int8) => Ok(()),
            (ContentTypes::Int16(_), ColumnTypes::Int16) => Ok(()),
            (ContentTypes::Int32(_), ColumnTypes::Int32) => Ok(()),
            (ContentTypes::Int64(_), ColumnTypes::Int64) => Ok(()),
            (ContentTypes::UInt8(_), ColumnTypes::UInt8) => Ok(()),
            (ContentTypes::UInt16(_), ColumnTypes::UInt16) => Ok(()),
            (ContentTypes::UInt32(_), ColumnTypes::UInt32) => Ok(()),
            (ContentTypes::UInt64(_), ColumnTypes::UInt64) => Ok(()),
            (ContentTypes::Float32(_), ColumnTypes::Float32) => Ok(()),
            (ContentTypes::Float64(_), ColumnTypes::Float64) => Ok(()),
            // Null should not reach here — caller checks nullability first
            (ContentTypes::Null, _) => Err(DatabaseError::InvalidArgument(
                "Nullable values can not be matched".to_string(),
            )),
            // Any other combination is a type mismatch
            _ => Err(DatabaseError::SchemaViolation(format!(
                "Column '{}': expected {}, got {}",
                column_def.get_name(),
                column_def.get_data_type(),
                value,
            ))),
        }
    }

    /// Validates that a record's content matches the table schema.
    /// Checks column count, type compatibility, and nullability.
    fn validate_record(&self, record_content: &PageRecordContent) -> Result<(), DatabaseError> {
        let values = record_content.get_content();
        let column_defs = self.header.get_column_defs();

        if column_defs.len() != values.len() {
            return Err(DatabaseError::SchemaViolation(format!(
                "Expected {} columns, got {}",
                column_defs.len(),
                values.len()
            )));
        }

        for (column_def, value) in column_defs.iter().zip(values.iter()) {
            match value {
                ContentTypes::Null => {
                    if !column_def.get_nullable() {
                        return Err(DatabaseError::SchemaViolation(format!(
                            "Column \"{}\" is not nullable",
                            column_def.get_name()
                        )));
                    }
                }
                _ => Self::compare_column_def_and_value_helper(column_def, value)?,
            }
        }

        Ok(())
    }

    /// Inserts a record into the table. Tries the last page first;
    /// if it doesn't have enough space, creates a new page.
    /// Updates the .meta file after changes.
    pub fn insert_record(
        &mut self,
        record_id: u64,
        record_content: PageRecordContent,
    ) -> Result<(), DatabaseError> {
        self.validate_record(&record_content)?;

        let page_kbytes = self.header.get_page_kbytes();
        let record_size = record_content.to_bytes().len() + PAGE_RECORD_METADATA_SIZE;

        if record_size + HEADER_SIZE > (page_kbytes as usize * KBYTES) {
            return Err(DatabaseError::RecordTooLarge);
        }

        let last_page_number = self.get_header().get_pages_count() - 1;
        let last_page_filename = self.resolve_file(last_page_number)?;

        let last_page_header = read_page_header(
            &last_page_filename.filename,
            last_page_filename.local_page_index,
            page_kbytes,
        )?;

        let (target_page_number, resolved_page) =
            if last_page_header.get_free_space() < record_size as u32 {
                let new_page_count = self.get_header().get_pages_count() + 1;
                self.get_header_mut().update_pages_count(new_page_count);
                self.save_header()?;

                let desired_page_number = last_page_number + 1;
                let filename_for_desired_page = self.resolve_file(desired_page_number)?;
                write_new_page(
                    &filename_for_desired_page.filename,
                    filename_for_desired_page.local_page_index,
                    page_kbytes,
                )?;
                (desired_page_number, filename_for_desired_page)
            } else {
                (last_page_number, last_page_filename)
            };
        let slot_index = add_new_record(
            &resolved_page.filename,
            resolved_page.local_page_index,
            page_kbytes,
            record_id,
            record_content,
        )?;

        self.index
            .insert_entry(record_id, target_page_number, slot_index)?;
        self.save_index()?;

        Ok(())
    }

    /// Reads a record by ID via index lookup.
    /// Returns the record's content, or RecordNotFound if not in the index.
    pub fn read_record(&self, record_id: u64) -> Result<PageRecordContent, DatabaseError> {
        let lookup = self.index.lookup(record_id);
        if let Some(record_pos) = lookup {
            let page_kbytes = self.header.get_page_kbytes();
            let page_number = record_pos.0;
            let slot_index = record_pos.1;
            let resolved_filename = self.resolve_file(page_number)?;
            let record_metadata = read_record_metadata(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                slot_index,
                page_kbytes,
            )?;
            let record_content = read_record_content(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
                &record_metadata,
            )?;
            return Ok(record_content);
        }

        Err(DatabaseError::RecordNotFound(record_id))
    }

    /// Deletes a record by ID via index lookup.
    /// Removes the record from the page and from the index.
    pub fn delete_record(&mut self, record_id: u64) -> Result<(), DatabaseError> {
        let lookup = self.index.lookup(record_id);
        if let Some(record_pos) = lookup {
            let page_kbytes = self.header.get_page_kbytes();
            let page_number = record_pos.0;
            let resolved_filename = self.resolve_file(page_number)?;
            page_delete_record(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
                record_id,
            )?;
            self.index.remove_entry(record_id)?;
            self.save_index()?;
            return Ok(());
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }

    /// Updates a record's content by ID via index lookup.
    pub fn update_record(
        &mut self,
        record_id: u64,
        record_content: PageRecordContent,
    ) -> Result<(), DatabaseError> {
        self.validate_record(&record_content)?;

        let lookup = self.index.lookup(record_id);
        if let Some(record_pos) = lookup {
            let page_kbytes = self.header.get_page_kbytes();
            let page_number = record_pos.0;
            let resolved_filename = self.resolve_file(page_number)?;
            let update_result = page_update_record(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
                record_id,
                record_content.clone(),
            );
            if let Err(error) = update_result {
                match error {
                    DatabaseError::RecordNotFound(_) => {
                        return Err(DatabaseError::RecordNotFound(record_id))
                    }
                    DatabaseError::PageFull => {
                        page_delete_record(
                            &resolved_filename.filename,
                            resolved_filename.local_page_index,
                            page_kbytes,
                            record_id,
                        )?;
                        self.index.remove_entry(record_id)?;
                        self.save_index()?;
                        self.insert_record(record_id, record_content)?;
                        return Ok(());
                    }
                    _ => return Err(error),
                }
            }
            return Ok(());
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }

    /// Given a global page number, returns the .dat filename and
    /// the local page number within that file.
    ///
    /// Formula:
    ///   file_index        = page_number / pages_per_file
    ///   local_page_index = page_number % pages_per_file
    ///   filename          = "{base_path}/{name}_{file_index}.dat"
    ///
    /// Returns Err if page_number >= pages_count.
    pub fn resolve_file(&self, page_number: u64) -> Result<ResolvedPage, DatabaseError> {
        if page_number >= self.header.get_pages_count() {
            return Err(DatabaseError::InvalidArgument(format!(
                "Page number {} is out of bounds (total pages: {})",
                page_number,
                self.header.get_pages_count()
            )));
        }
        let file_index = page_number / self.pages_per_file as u64;
        let local_page_index = page_number % self.pages_per_file as u64;
        let filename = self.dat_path(file_index);
        Ok(ResolvedPage {
            filename,
            local_page_index,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_operations::file_processing::table::ColumnDef;
    use crate::database_operations::file_processing::types::ColumnTypes;

    fn sample_table(pages_count: u64, pages_per_file: u32) -> Table {
        Table::new(
            "users".to_string(),
            "/data/db".to_string(),
            pages_per_file,
            TableHeader::new(
                pages_count,
                1,
                8,
                vec![ColumnDef::new(ColumnTypes::Int64, false, "id".to_string())],
            ),
            HashIndex::new(16),
        )
    }

    #[test]
    fn resolve_first_page() {
        let table = sample_table(10, 5);
        let resolved = table.resolve_file(0).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_0.dat");
        assert_eq!(resolved.local_page_index, 0);
    }

    #[test]
    fn resolve_last_page_in_first_file() {
        let table = sample_table(10, 5);
        let resolved = table.resolve_file(4).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_0.dat");
        assert_eq!(resolved.local_page_index, 4);
    }

    #[test]
    fn resolve_first_page_in_second_file() {
        let table = sample_table(10, 5);
        let resolved = table.resolve_file(5).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_1.dat");
        assert_eq!(resolved.local_page_index, 0);
    }

    #[test]
    fn resolve_page_in_third_file() {
        let table = sample_table(100, 30);
        let resolved = table.resolve_file(67).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_2.dat");
        assert_eq!(resolved.local_page_index, 7);
    }

    #[test]
    fn resolve_out_of_bounds() {
        let table = sample_table(10, 5);
        assert!(table.resolve_file(10).is_err());
        assert!(table.resolve_file(100).is_err());
    }
}
