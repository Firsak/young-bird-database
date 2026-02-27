use std::error::Error;

use super::table_header::TableHeader;
use crate::database_operations::file_processing::page::writing::write_new_page;
use crate::database_operations::file_processing::table::reading::read_table_header;
use crate::database_operations::file_processing::table::writing::write_table_header;

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
    pub local_page_number: u64,
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
    ) -> Result<Self, Box<dyn Error>> {
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
    ) -> Result<Self, Box<dyn Error>> {
        if pages_per_file < 1 {
            return Err("Pages per file should be more than 0".into());
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

    /// Given a global page number, returns the .dat filename and
    /// the local page number within that file.
    ///
    /// Formula:
    ///   file_index        = global_page_number / pages_per_file
    ///   local_page_number = global_page_number % pages_per_file
    ///   filename          = "{base_path}/{name}_{file_index}.dat"
    ///
    /// Returns Err if global_page_number >= pages_count.
    pub fn resolve_file(&self, global_page_number: u64) -> Result<ResolvedPage, String> {
        if global_page_number >= self.header.get_pages_count() {
            return Err(format!(
                "Page number is greater than total pages in the table: {}",
                self.header.get_pages_count()
            ));
        }
        let file_index = global_page_number / self.pages_per_file as u64;
        let local_page_number = global_page_number % self.pages_per_file as u64;
        let filename = self.dat_path(file_index);
        Ok(ResolvedPage {
            filename,
            local_page_number,
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
        assert_eq!(resolved.local_page_number, 0);
    }

    #[test]
    fn resolve_last_page_in_first_file() {
        let table = sample_table(10, 5);
        let resolved = table.resolve_file(4).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_0.dat");
        assert_eq!(resolved.local_page_number, 4);
    }

    #[test]
    fn resolve_first_page_in_second_file() {
        let table = sample_table(10, 5);
        let resolved = table.resolve_file(5).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_1.dat");
        assert_eq!(resolved.local_page_number, 0);
    }

    #[test]
    fn resolve_page_in_third_file() {
        let table = sample_table(100, 30);
        let resolved = table.resolve_file(67).unwrap();
        assert_eq!(resolved.filename, "/data/db/users_2.dat");
        assert_eq!(resolved.local_page_number, 7);
    }

    #[test]
    fn resolve_out_of_bounds() {
        let table = sample_table(10, 5);
        assert!(table.resolve_file(10).is_err());
        assert!(table.resolve_file(100).is_err());
    }
}
