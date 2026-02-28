use super::table_header::TableHeader;
use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::page::reading::{read_page, read_page_header};
use crate::database_operations::file_processing::page::record::PageRecordContent;
use crate::database_operations::file_processing::page::writing::{
    add_new_record, delete_record as page_delete_record, update_record as page_update_record,
    write_new_page,
};
use crate::database_operations::file_processing::table::reading::read_table_header;
use crate::database_operations::file_processing::table::writing::write_table_header;
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::{HEADER_SIZE, KBYTES, PAGE_RECORD_METADATA_SIZE};

/// High-level Table API. Wraps a TableHeader and resolves
/// global page numbers to concrete (filename, local_page) pairs.
#[derive(Debug)]
pub struct Table {
    name: String,
    base_path: String,
    pages_per_file: u32,
    header: TableHeader,
}

/// Result of resolving a global page number: which file and which
/// page within that file.
#[derive(Debug, PartialEq)]
pub struct ResolvedPage {
    pub filename: String,
    pub local_page_index: u64,
}

impl Table {
    pub fn new(name: String, base_path: String, pages_per_file: u32, header: TableHeader) -> Self {
        Self {
            name,
            base_path,
            pages_per_file,
            header,
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

    /// Opens an existing table by reading its .meta file.
    pub fn open(
        name: String,
        base_path: String,
        pages_per_file: u32,
    ) -> Result<Self, DatabaseError> {
        let meta_path = format!("{}/{}.meta", base_path, name);
        let header = read_table_header(&meta_path)?;
        Ok(Self {
            name,
            base_path,
            pages_per_file,
            header,
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
        if pages_per_file < 1 {
            return Err(DatabaseError::InvalidArgument(
                "Pages per file should be more than 0".to_string(),
            ));
        }
        let table_header = TableHeader::new(1, columns.len() as u16, page_kbytes, columns);
        let table = Table::new(name, base_path, pages_per_file, table_header);
        write_table_header(&table.meta_path(), table.get_header())?;
        write_new_page(&table.dat_path(0), 0, page_kbytes)?;
        Ok(table)
    }

    pub fn get_header(&self) -> &TableHeader {
        &self.header
    }

    pub fn get_header_mut(&mut self) -> &mut TableHeader {
        &mut self.header
    }

    /// Inserts a record into the table. Tries the last page first;
    /// if it doesn't have enough space, creates a new page.
    /// Updates the .meta file after changes.
    pub fn insert_record(
        &mut self,
        record_id: u64,
        record_content: PageRecordContent,
    ) -> Result<(), DatabaseError> {
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

        let filename_for_desired_page = if last_page_header.get_free_space() < record_size as u32 {
            let new_page_count = self.get_header().get_pages_count() + 1;
            self.get_header_mut().update_pages_count(new_page_count);
            write_table_header(&self.meta_path(), self.get_header())?;

            let desired_page_number = last_page_number + 1;
            let filename_for_desired_page = self.resolve_file(desired_page_number)?;
            write_new_page(
                &filename_for_desired_page.filename,
                filename_for_desired_page.local_page_index,
                page_kbytes,
            )?;
            filename_for_desired_page
        } else {
            last_page_filename
        };
        add_new_record(
            &filename_for_desired_page.filename,
            filename_for_desired_page.local_page_index,
            page_kbytes,
            record_id,
            record_content,
        )?;

        Ok(())
    }

    /// Reads a record by ID using a linear scan across all pages.
    /// Returns the record's content, or RecordNotFound if no active
    /// record with that ID exists.
    pub fn read_record(&self, record_id: u64) -> Result<PageRecordContent, DatabaseError> {
        let page_kbytes = self.header.get_page_kbytes();
        let pages_count = self.header.get_pages_count();

        for page_number in 0..pages_count {
            let resolved_filename = self.resolve_file(page_number)?;
            let page = read_page(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
            )?;
            for (slot_index, record_metadata) in page.get_records_metadata().iter().enumerate() {
                if record_metadata.get_id() == record_id {
                    let record_content = page.get_record_content_by_slot_index(slot_index);
                    let new_record = PageRecordContent::new(record_content.get_content().clone());
                    return Ok(new_record);
                }
            }
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }

    /// Deletes a record by ID. Scans pages to find the record,
    /// then delegates to the page-level delete.
    pub fn delete_record(&mut self, record_id: u64) -> Result<(), DatabaseError> {
        let page_kbytes = self.header.get_page_kbytes();
        let pages_count = self.header.get_pages_count();

        for page_number in 0..pages_count {
            let resolved_filename = self.resolve_file(page_number)?;
            let delete_result = page_delete_record(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
                record_id,
            );
            match delete_result {
                Ok(_) => return Ok(()),
                Err(error) => match error {
                    DatabaseError::RecordNotFound(_) => continue,
                    _ => return Err(error),
                },
            }
        }
        Err(DatabaseError::RecordNotFound(record_id))
    }

    /// Updates a record's content by ID. Scans pages to find the record,
    /// then delegates to the page-level update.
    pub fn update_record(
        &mut self,
        record_id: u64,
        record_content: PageRecordContent,
    ) -> Result<(), DatabaseError> {
        let page_kbytes = self.header.get_page_kbytes();
        let pages_count = self.header.get_pages_count();

        for page_number in 0..pages_count {
            let resolved_filename = self.resolve_file(page_number)?;
            let update_result = page_update_record(
                &resolved_filename.filename,
                resolved_filename.local_page_index,
                page_kbytes,
                record_id,
                record_content.clone(),
            );
            match update_result {
                Ok(_) => return Ok(()),
                Err(error) => match error {
                    DatabaseError::RecordNotFound(_) => continue,
                    DatabaseError::PageFull => {
                        page_delete_record(&resolved_filename.filename, resolved_filename.local_page_index, page_kbytes, record_id)?;
                        self.insert_record(record_id, record_content)?;
                        return Ok(());
                    }
                    _ => return Err(error),
                },
            }
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
