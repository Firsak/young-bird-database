// Executor: Statement → Table API calls → ExecuteResult
//
// Pipeline:  SQL string → Lexer → Parser → **Executor** → disk I/O
//
// Input:  Statement (parsed AST node)
// Output: ExecuteResult (Created, Dropped, Inserted, Deleted, Updated, Selected)
//
// The executor owns configuration (base_path, pages_per_file, page_kbytes, overflow_kbytes)
// and opens/creates tables as needed per statement.

use std::collections::{HashMap, HashSet};

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::page::record::PageRecordContent;
use crate::database_operations::file_processing::table::{ColumnDef, Table};
use crate::database_operations::file_processing::traits::BinarySerde;
use crate::database_operations::file_processing::types::{
    self as storage_types, ColumnTypes, ContentTypes,
};
use crate::database_operations::file_processing::wal::wal_entry::{self, WalEntry, WalOperation};
use crate::database_operations::file_processing::wal::wal_reader::read_all;
use crate::database_operations::file_processing::wal::wal_writer::WalWriter;
use crate::database_operations::sql::ast::{
    self as ast, CompOp, Expr, Literal, SelectColumns, Statement,
};

/// Result of executing a SQL statement.
#[derive(Debug)]
pub enum ExecuteResult {
    /// CREATE TABLE succeeded.
    Created,
    /// DROP TABLE succeeded.
    Dropped,
    /// INSERT succeeded; contains the auto-assigned record ID.
    Inserted {
        id: u64,
    },
    /// DELETE succeeded; contains the number of records deleted.
    Deleted {
        count: usize,
    },
    /// UPDATE succeeded; contains the number of records updated.
    Updated {
        count: usize,
    },
    /// SELECT succeeded; contains column names and matching rows.
    /// Each row is a Vec<ContentTypes> where the first element is the `id` column.
    Selected {
        columns: Vec<String>,
        rows: Vec<Vec<ContentTypes>>,
    },
    TransactionStarted,
    Committed,
    RolledBack,
}

/// SQL statement executor. Translates AST nodes into Table API calls.
pub struct Executor {
    base_path: String,
    pages_per_file: u32,
    page_kbytes: u32,
    overflow_kbytes: u32,
    cache_size: usize,
    wal: WalWriter,
    active_txn_id: Option<u64>,
    next_txn_id: u64,
    touched_tables: HashSet<String>,
    opened_tables: HashMap<String, Table>,
}

impl Executor {
    pub fn new(
        base_path: String,
        pages_per_file: u32,
        page_kbytes: u32,
        overflow_kbytes: u32,
        cache_size: usize,
        wal_path: String,
    ) -> Result<Self, DatabaseError> {
        let wal = WalWriter::new(wal_path.clone())?;
        let mut executor = Self {
            base_path,
            pages_per_file,
            page_kbytes,
            overflow_kbytes,
            cache_size,
            wal,
            active_txn_id: None,
            next_txn_id: 1,
            touched_tables: HashSet::new(),
            opened_tables: HashMap::new(),
        };
        executor.recover_from_wal(&wal_path)?;
        Ok(executor)
    }

    /// Executes a parsed SQL statement and returns the result.
    ///
    /// # Arguments
    /// - `statement`: a parsed AST `Statement` node
    ///
    /// # Returns
    /// `ExecuteResult` variant matching the statement type.
    ///
    /// # Errors
    /// Returns `DatabaseError` on I/O failures, schema violations,
    /// missing tables, or invalid SQL semantics.
    pub fn execute(&mut self, statement: Statement) -> Result<ExecuteResult, DatabaseError> {
        match statement {
            Statement::CreateTable { table, columns } => self.execute_create_table(&table, columns),
            Statement::DropTable { table } => self.execute_drop_table(&table),
            Statement::Insert { table, values } => self.execute_insert(&table, values),
            Statement::Select {
                columns,
                table,
                where_clause,
            } => self.execute_select(&table, columns, where_clause),
            Statement::Delete {
                table,
                where_clause,
            } => self.execute_delete(&table, where_clause),
            Statement::Update {
                table,
                assignments,
                where_clause,
            } => self.execute_update(&table, assignments, where_clause),
            Statement::Begin => self.execute_begin(),
            Statement::Commit => self.execute_commit(),
            Statement::Rollback => self.execute_rollback(),
        }
    }

    /// Executes a BEGIN statement. Opens an explicit transaction.
    ///
    /// Assigns a new transaction ID, appends a Begin entry to the WAL,
    /// and sets `active_txn_id`. Mutations after this will not flush to
    /// disk until COMMIT.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if a transaction is already active.
    fn execute_begin(&mut self) -> Result<ExecuteResult, DatabaseError> {
        if self.active_txn_id.is_some() {
            return Err(DatabaseError::InvalidArgument(
                "Nested transactions are not supported".to_string(),
            ));
        }
        self.active_txn_id = Some(self.next_txn_id);
        let transaction_id = self.active_txn_id;
        self.next_txn_id += 1;

        self.wal.append(&WalEntry::new(
            transaction_id.unwrap(),
            WalOperation::Begin,
            0,
            "".to_string(),
            vec![],
        ))?;

        Ok(ExecuteResult::TransactionStarted)
    }

    /// Executes a COMMIT statement. Durably persists the active transaction.
    ///
    /// 1. Appends Commit entry to the WAL.
    /// 2. fsyncs the WAL (guarantees durability before touching pages).
    /// 3. Flushes all dirty pages for touched tables.
    /// 4. Truncates the WAL.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if no transaction is active.
    fn execute_commit(&mut self) -> Result<ExecuteResult, DatabaseError> {
        if self.active_txn_id.is_none() {
            return Err(DatabaseError::InvalidArgument(
                "No transaction started".to_string(),
            ));
        }
        self.wal.append(&WalEntry::new(
            self.active_txn_id.unwrap(),
            WalOperation::Commit,
            0,
            "".to_string(),
            vec![],
        ))?;
        self.wal.fsync()?;

        let table_names = self.touched_tables.clone();
        for table_name in table_names {
            let table = self.get_or_open_table(table_name.as_str())?;

            table.flush_all_dirty()?;
        }

        self.wal.truncate()?;
        self.active_txn_id = None;
        self.touched_tables.clear();

        Ok(ExecuteResult::Committed)
    }

    /// Executes a ROLLBACK statement. Discards the active transaction.
    ///
    /// Truncates the WAL and drops all touched tables from `opened_tables`.
    /// Dropping the `Table` struct discards dirty buffer pool pages without
    /// flushing — the redo-only WAL means uncommitted pages never reach disk.
    ///
    /// # Errors
    /// Returns `InvalidArgument` if no transaction is active.
    fn execute_rollback(&mut self) -> Result<ExecuteResult, DatabaseError> {
        if self.active_txn_id.is_none() {
            return Err(DatabaseError::InvalidArgument(
                "No transaction started".to_string(),
            ));
        }

        self.wal.truncate()?;
        self.active_txn_id = None;
        let touched = self.touched_tables.clone();
        self.touched_tables.clear();

        for table_name in &touched {
            self.opened_tables.remove(table_name);
        }

        Ok(ExecuteResult::RolledBack)
    }

    /// Executes a CREATE TABLE statement.
    //
    /// SQL: `CREATE TABLE t (name TEXT, age INT32 NOT NULL)`
    ///
    /// Converts `Vec<ast::ColumnSpec>` → `Vec<ColumnDef>`, then calls `Table::create`
    /// to write the `.meta` and initial `.dat` files.
    ///
    /// # Returns
    /// `ExecuteResult::Created` on success.
    fn execute_create_table(
        &self,
        table_name: &str,
        columns: Vec<ast::ColumnSpec>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let mut table_columns: Vec<ColumnDef> = vec![];
        for ast_column in columns.iter() {
            let table_column_type = column_type_to_storage(&ast_column.data_type);
            table_columns.push(ColumnDef::new(
                table_column_type,
                ast_column.nullable,
                ast_column.name.clone(),
            ));
        }
        Table::create(
            table_name.to_string(),
            self.base_path.clone(),
            self.pages_per_file,
            self.page_kbytes,
            self.overflow_kbytes,
            table_columns,
            self.cache_size,
        )?;
        Ok(ExecuteResult::Created)
    }

    /// Executes a DROP TABLE statement.
    ///
    /// SQL: `DROP TABLE t`
    ///
    /// Deletes `.meta`, `.idx`, all `_N.dat` files, and all `_N.overflow` files.
    /// Silently ignores files that are already missing (NotFound).
    ///
    /// # Returns
    /// `ExecuteResult::Dropped` on success.
    fn execute_drop_table(&mut self, table_name: &str) -> Result<ExecuteResult, DatabaseError> {
        if self.active_txn_id.is_some() {
            return Err(DatabaseError::InvalidArgument(
                "DROP TABLE cannot be used inside a transaction — COMMIT or ROLLBACK first"
                    .to_string(),
            ));
        }
        let meta = format!("{}/{}.meta", self.base_path, table_name);
        let idx = format!("{}/{}.idx", self.base_path, table_name);

        // Delete .meta and .idx — skip if already missing, propagate other I/O errors
        for path in [&meta, &idx] {
            if let Err(e) = std::fs::remove_file(path) && e.kind() != std::io::ErrorKind::NotFound {
                return Err(DatabaseError::Io(e));
            }
        }

        // Delete .dat files: _0.dat, _1.dat, ... until one doesn't exist
        let mut file_index = 0u64;
        loop {
            let dat = format!("{}/{}_{}.dat", self.base_path, table_name, file_index);
            match std::fs::remove_file(&dat) {
                Ok(_) => file_index += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(DatabaseError::Io(e)),
            }
        }

        // Delete .overflow files: _0.overflow, _1.overflow, ... until one doesn't exist
        let mut file_index = 0u64;
        loop {
            let overflow = format!("{}/{}_{}.overflow", self.base_path, table_name, file_index);
            match std::fs::remove_file(&overflow) {
                Ok(_) => file_index += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => break,
                Err(e) => return Err(DatabaseError::Io(e)),
            }
        }

        self.opened_tables.remove(table_name);

        Ok(ExecuteResult::Dropped)
    }

    /// Executes an INSERT statement.
    ///
    /// SQL: `INSERT INTO t VALUES (1, 'alice', true)`
    ///
    /// Opens the table, validates value count against schema, converts each
    /// `Literal` → `ContentTypes` using the column's declared type, then calls
    /// `table.insert()` which auto-assigns a record ID.
    ///
    /// # Returns
    /// `ExecuteResult::Inserted { id }` with the auto-assigned record ID.
    ///
    /// # Errors
    /// `SchemaViolation` if value count doesn't match column count, or if a
    /// literal can't be converted to the target column type.
    fn execute_insert(
        &mut self,
        table_name: &str,
        values: Vec<Literal>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let transaction_id = self.active_txn_id.unwrap_or(self.next_txn_id);

        let table = self.get_or_open_table(table_name)?;
        if values.len() != table.get_header().get_columns_count() as usize {
            return Err(DatabaseError::SchemaViolation(
                "Provided Literals do not match with Column Definitions".to_string(),
            ));
        }
        let mut content_values: Vec<storage_types::ContentTypes> = vec![];
        for (literal, target_type) in values
            .iter()
            .zip(table.get_header().get_column_defs().iter())
        {
            let converted_type = literal_to_content_type(literal, target_type.get_data_type())?;
            content_values.push(converted_type);
        }
        let record_content = PageRecordContent::new(content_values);
        let id = table.insert(record_content.clone())?;

        let wal_entry = WalEntry::new(
            transaction_id,
            wal_entry::WalOperation::Insert,
            id,
            table_name.to_string(),
            record_content.to_bytes(),
        );
        self.wal.append(&wal_entry)?;

        if self.active_txn_id.is_none() {
            // auto-transaction: flush immediately, no tracking needed
            let table = self.get_or_open_table(table_name)?;
            table.flush_all_dirty()?;
            self.wal.truncate()?;
        } else {
            // explicit transaction: track for later COMMIT
            self.touched_tables.insert(table_name.to_string());
        }

        Ok(ExecuteResult::Inserted { id })
    }

    /// Executes a SELECT statement.
    ///
    /// SQL: `SELECT * FROM t WHERE age > 18` or `SELECT name, id FROM t`
    ///
    /// Opens the table, validates column names against schema, scans records
    /// with the WHERE filter, and projects the requested columns. The `id`
    /// column (auto-increment record ID) is always available as a virtual column.
    /// For `SELECT *`, columns are returned as `[id, col1, col2, ...]`.
    /// For named columns, pre-computes column indices before iterating rows.
    ///
    /// # Returns
    /// `ExecuteResult::Selected { columns, rows }` with column names and matching rows.
    ///
    /// # Errors
    /// `SchemaViolation` if a named column doesn't exist in the table schema.
    fn execute_select(
        &self,
        table_name: &str,
        columns: SelectColumns,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let mut table = Table::open(
            table_name.to_string(),
            self.base_path.clone(),
            self.cache_size,
        )?;

        // TODO: validate WHERE column names against schema before scanning
        let column_defs = table.get_header().get_column_defs().clone();
        let column_def_names: Vec<String> = column_defs
            .iter()
            .map(|cd| cd.get_name().to_string())
            .collect();
        if let SelectColumns::Named(names) = &columns {
            for name in names {
                if !column_def_names.contains(name) && name != "id" {
                    return Err(DatabaseError::SchemaViolation(format!(
                        "Column \"{}\" is not presented in the table",
                        name
                    )));
                }
            }
        }
        let column_names = match &columns {
            SelectColumns::All => {
                let mut names: Vec<String> = column_defs
                    .iter()
                    .map(|col| col.get_name().to_string())
                    .collect();
                names.insert(0, "id".to_string());
                names
            }
            SelectColumns::Named(names) => names.clone(),
        };
        let columns_count = match &columns {
            SelectColumns::All => column_defs.len() + 1,
            SelectColumns::Named(names) => names.len(),
        };
        let records = table.scan_records(|record_id, values| match &where_clause {
            None => true,
            Some(expr) => evaluate_expr(expr, record_id, values, &column_defs).unwrap_or(false),
        })?;
        let rows = records
            .iter()
            .map(|record| {
                let mut res: Vec<ContentTypes> = Vec::with_capacity(columns_count);
                if columns == SelectColumns::All {
                    res.push(ContentTypes::UInt64(record.0));
                    for content in record.1.get_content() {
                        res.push(content.clone());
                    }
                }
                if let SelectColumns::Named(names) = &columns {
                    let column_indices: Vec<Option<usize>> = names
                        .iter()
                        .map(|name| {
                            if name == "id" {
                                None // special: comes from record_id, not column_defs
                            } else {
                                Some(column_def_names.iter().position(|col| col == name).unwrap())
                            }
                        })
                        .collect();
                    for index in &column_indices {
                        match index {
                            None => res.push(ContentTypes::UInt64(record.0)), // id
                            Some(i) => res.push(record.1.get_content()[*i].clone()), // column by index
                        }
                    }
                }
                res
            })
            .collect();
        Ok(ExecuteResult::Selected {
            columns: column_names,
            rows,
        })
    }

    /// Executes a DELETE statement using two-phase approach.
    ///
    /// SQL: `DELETE FROM t WHERE id = 3` or `DELETE FROM t` (deletes all)
    ///
    /// Phase 1: scans record IDs matching the WHERE filter via `scan_record_ids`
    /// (collects only IDs, no content cloning).
    /// Phase 2: deletes each matched record by ID via `delete_record`.
    ///
    /// # Returns
    /// `ExecuteResult::Deleted { count }` with the number of records deleted.
    fn execute_delete(
        &mut self,
        table_name: &str,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let transaction_id = self.active_txn_id.unwrap_or(self.next_txn_id);

        let table = self.get_or_open_table(table_name)?;
        // TODO: validate WHERE column names against schema before scanning
        let ids_to_delete = {
            let column_defs = table.get_header().get_column_defs().clone();
            table.scan_record_ids(|record_id, values| match &where_clause {
                None => true,
                Some(expr) => evaluate_expr(expr, record_id, values, &column_defs).unwrap_or(false),
            })?
        };

        let mut wal_entries = vec![];
        for id in &ids_to_delete {
            table.delete_record(*id)?;

            let wal_entry = WalEntry::new(
                transaction_id,
                wal_entry::WalOperation::Delete,
                *id,
                table_name.to_string(),
                vec![],
            );
            wal_entries.push(wal_entry);
        }

        for entry in &wal_entries {
            self.wal.append(entry)?;
        }

        if self.active_txn_id.is_none() {
            // auto-transaction: flush immediately, no tracking needed
            let table = self.get_or_open_table(table_name)?;
            table.flush_all_dirty()?;
            self.wal.truncate()?;
        } else {
            // explicit transaction: track for later COMMIT
            self.touched_tables.insert(table_name.to_string());
        }

        Ok(ExecuteResult::Deleted {
            count: ids_to_delete.len(),
        })
    }

    /// Executes an UPDATE statement using two-phase approach.
    ///
    /// SQL: `UPDATE t SET name = 'bob' WHERE id = 1` or `UPDATE t SET age = 30`
    ///
    /// Pre-computes column positions and converts literal values before scanning.
    /// Phase 1: scans record IDs matching the WHERE filter via `scan_record_ids`.
    /// Phase 2: for each matched record, reads current content, applies the
    /// pre-converted assignment values at pre-computed positions, then calls
    /// `update_record`.
    ///
    /// # Returns
    /// `ExecuteResult::Updated { count }` with the number of records updated.
    ///
    /// # Errors
    /// `SchemaViolation` if an assignment column doesn't exist or the literal
    /// can't be converted to the column's type.
    fn execute_update(
        &mut self,
        table_name: &str,
        assignments: Vec<ast::Assignment>,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let transaction_id = self.active_txn_id.unwrap_or(self.next_txn_id);

        let table = self.get_or_open_table(table_name)?;

        // TODO: validate WHERE column names against schema before scanning
        let column_defs = table.get_header().get_column_defs().clone();

        let column_def_names: Vec<String> = column_defs
            .iter()
            .map(|cd| cd.get_name().to_string())
            .collect();
        let column_def_types: Vec<ColumnTypes> = column_defs
            .iter()
            .map(|cd| cd.get_data_type().clone())
            .collect();
        let mut column_pos_to_update: Vec<usize> = Vec::with_capacity(assignments.len());
        let mut column_values_to_update: Vec<ContentTypes> = Vec::with_capacity(assignments.len());
        for assignment in &assignments {
            match column_def_names
                .iter()
                .position(|col| col == &assignment.column)
            {
                None => {
                    return Err(DatabaseError::SchemaViolation(format!(
                        "Column \"{}\" is not presented in the table",
                        &assignment.column
                    )))
                }
                Some(pos) => {
                    column_pos_to_update.push(pos);
                    column_values_to_update.push(literal_to_content_type(
                        &assignment.value,
                        &column_def_types[pos],
                    )?);
                }
            }
        }

        let ids_to_update = {
            table.scan_record_ids(|record_id, values| match &where_clause {
                None => true,
                Some(expr) => evaluate_expr(expr, record_id, values, &column_defs).unwrap_or(false),
            })?
        };

        if ids_to_update.is_empty() {
            return Ok(ExecuteResult::Updated { count: 0 });
        }

        let mut wal_entries = vec![];
        for record_id in &ids_to_update {
            let record = table.read_record(*record_id)?;
            let mut content = record.get_content().clone();
            for (index, position) in column_pos_to_update.iter().enumerate() {
                content[*position] = column_values_to_update[index].clone();
            }
            table.update_record(*record_id, PageRecordContent::new(content.clone()))?;

            let wal_entry = WalEntry::new(
                transaction_id,
                wal_entry::WalOperation::Update,
                *record_id,
                table_name.to_string(),
                PageRecordContent::new(content).to_bytes(),
            );
            wal_entries.push(wal_entry);
        }

        let count = ids_to_update.len();

        for entry in &wal_entries {
            self.wal.append(entry)?;
        }

        if self.active_txn_id.is_none() {
            // auto-transaction: flush immediately, no tracking needed
            let table = self.get_or_open_table(table_name)?;
            table.flush_all_dirty()?;
            self.wal.truncate()?;
        } else {
            // explicit transaction: track for later COMMIT
            self.touched_tables.insert(table_name.to_string());
        }

        Ok(ExecuteResult::Updated { count })
    }

    /// Returns a mutable reference to an open table, opening it if not already cached.
    ///
    /// Tables stay in `opened_tables` for the lifetime of the executor so that dirty
    /// buffer pool pages persist across statements within a transaction.
    fn get_or_open_table(&mut self, table_name: &str) -> Result<&mut Table, DatabaseError> {
        if !self.opened_tables.contains_key(table_name) {
            let table = Table::open(
                table_name.to_string(),
                self.base_path.clone(),
                self.cache_size,
            )?;
            self.opened_tables.insert(table_name.to_string(), table);
        }
        Ok(self.opened_tables.get_mut(table_name).unwrap())
    }

    /// Replays committed transactions from the WAL file on startup.
    ///
    /// Reads all entries from `wal_path`, groups them by `transaction_id`, and
    /// replays mutations for any group that contains a `Commit` entry. Groups
    /// without a `Commit` (crashed mid-transaction) are silently skipped.
    ///
    /// Each mutation is idempotent:
    /// - Insert: skipped if record already exists (already applied before crash)
    /// - Delete/Update: skipped if record not found (already applied before crash)
    ///
    /// After replay, all touched tables are flushed and the WAL is truncated.
    fn recover_from_wal(&mut self, wal_path: &str) -> Result<(), DatabaseError> {
        let entries = read_all(wal_path)?;
        if entries.is_empty() {
            return Ok(());
        }

        let mut grouped_transactions: HashMap<u64, Vec<WalEntry>> = HashMap::new();
        for entry in entries {
            match grouped_transactions.get_mut(&entry.transaction_id) {
                None => {
                    grouped_transactions.insert(entry.transaction_id, vec![entry]);
                }
                Some(existing_entries) => {
                    existing_entries.push(entry);
                }
            }
        }

        for (_transaction_id, entries) in grouped_transactions {
            if let Some(entry) = entries.last() && entry.operation != WalOperation::Commit {continue;}

            for entry in entries {
                match entry.operation {
                    WalOperation::Insert => {
                        self.touched_tables.insert(entry.table_name.clone());
                        let table = self.get_or_open_table(&entry.table_name)?;
                        match table.read_record(entry.record_id) {
                            Ok(_) => {
                                continue;
                            }
                            Err(DatabaseError::RecordNotFound(_)) => {
                                table.insert_record(
                                    entry.record_id,
                                    PageRecordContent::from_bytes(&entry.data)?,
                                )?;
                            }
                            Err(e) => return Err(e),
                        };
                    }
                    WalOperation::Update => {
                        self.touched_tables.insert(entry.table_name.clone());
                        let table = self.get_or_open_table(&entry.table_name)?;
                        match table.read_record(entry.record_id) {
                            Ok(_) => {
                                table.update_record(
                                    entry.record_id,
                                    PageRecordContent::from_bytes(&entry.data)?,
                                )?;
                            }
                            Err(DatabaseError::RecordNotFound(_)) => {
                                continue;
                            }
                            Err(e) => return Err(e),
                        };
                    }
                    WalOperation::Delete => {
                        self.touched_tables.insert(entry.table_name.clone());
                        let table = self.get_or_open_table(&entry.table_name)?;
                        match table.read_record(entry.record_id) {
                            Ok(_) => {
                                table.delete_record(entry.record_id)?;
                            }
                            Err(DatabaseError::RecordNotFound(_)) => {
                                continue;
                            }
                            Err(e) => return Err(e),
                        };
                    }
                    _ => {
                        continue;
                    }
                }
            }
        }

        let table_names = self.touched_tables.clone();
        for table_name in table_names {
            let table = self.get_or_open_table(table_name.as_str())?;
            table.flush_all_dirty()?;
        }
        self.touched_tables.clear();

        self.wal.truncate()?;

        Ok(())
    }
}

// ── WHERE expression evaluator ───────────────────────────────────

/// Evaluates a WHERE expression against a single record.
///
/// # Arguments
/// - `expr`: the WHERE expression tree
/// - `record_id`: this record's auto-incremented ID (for `WHERE id = N`)
/// - `values`: this record's column values
/// - `column_defs`: table schema (maps column names → indices)
///
/// # Returns
/// `true` if the record matches the expression, `false` otherwise.
///
/// # Errors
/// Returns `DatabaseError::InvalidArgument` if a column name in the
/// expression doesn't exist in the schema.
fn evaluate_expr(
    expr: &Expr,
    record_id: u64,
    values: &[ContentTypes],
    column_defs: &[ColumnDef],
) -> Result<bool, DatabaseError> {
    match expr {
        Expr::And(left, right) => Ok(evaluate_expr(left, record_id, values, column_defs)?
            && evaluate_expr(right, record_id, values, column_defs)?),
        Expr::Or(left, right) => Ok(evaluate_expr(left, record_id, values, column_defs)?
            || evaluate_expr(right, record_id, values, column_defs)?),
        Expr::Not(inner) => Ok(!evaluate_expr(inner, record_id, values, column_defs)?),
        Expr::Comparison { column, op, value } => {
            if *value == Literal::Null {
                return Ok(false);
            }
            if column == "id" {
                Ok(compare_id_with_literal(record_id, op, value)?)
            } else {
                let position = column_defs.iter().position(|cd| cd.get_name() == column);
                if position.is_none() {
                    return Err(DatabaseError::InvalidArgument(format!(
                        "Column \"{}\" not found",
                        column
                    )));
                }
                let position = position.unwrap();
                if values[position] == ContentTypes::Null {
                    return Ok(false);
                }
                Ok(compare_content_with_literal(&values[position], op, value)?)
            }
        }
    }
}

// ── Comparison helpers ───────────────────────────────────────────

/// Applies a comparison operator to an `Ordering`.
/// Maps all six CompOp variants using a single match on the ordering result.
fn apply_op(ordering: std::cmp::Ordering, op: &CompOp) -> bool {
    match op {
        CompOp::Eq => ordering == std::cmp::Ordering::Equal,
        CompOp::Ne => ordering != std::cmp::Ordering::Equal,
        CompOp::Lt => ordering == std::cmp::Ordering::Less,
        CompOp::Gt => ordering == std::cmp::Ordering::Greater,
        CompOp::Le => ordering != std::cmp::Ordering::Greater,
        CompOp::Ge => ordering != std::cmp::Ordering::Less,
    }
}

/// Compares a record_id (u64) against a Literal using the given operator.
/// Used for `WHERE id = 5` style comparisons.
fn compare_id_with_literal(
    record_id: u64,
    op: &CompOp,
    literal: &Literal,
) -> Result<bool, DatabaseError> {
    match literal {
        Literal::Null => Ok(false),
        Literal::Integer(v) => Ok(apply_op(record_id.cmp(v), op)),
        Literal::NegativeInteger(_) => {
            // id is always >= 0, so id > any negative number
            Ok(apply_op(std::cmp::Ordering::Greater, op))
        }
        _ => Err(DatabaseError::InvalidArgument(format!(
            "cannot compare id with {:?}",
            literal
        ))),
    }
}

/// Compares a ContentTypes value against a Literal using the given operator.
/// Returns false if either side is Null (SQL three-valued logic).
fn compare_content_with_literal(
    content: &ContentTypes,
    op: &CompOp,
    literal: &Literal,
) -> Result<bool, DatabaseError> {
    // NULL comparisons always false
    if *content == ContentTypes::Null || *literal == Literal::Null {
        return Ok(false);
    }

    match (content, literal) {
        // Signed integers vs Integer literal
        (ContentTypes::Int8(v), Literal::Integer(lit)) => {
            Ok(apply_op((*v as i64).cmp(&(*lit as i64)), op))
        }
        (ContentTypes::Int16(v), Literal::Integer(lit)) => {
            Ok(apply_op((*v as i64).cmp(&(*lit as i64)), op))
        }
        (ContentTypes::Int32(v), Literal::Integer(lit)) => {
            Ok(apply_op((*v as i64).cmp(&(*lit as i64)), op))
        }
        (ContentTypes::Int64(v), Literal::Integer(lit)) => {
            Ok(apply_op((*v).cmp(&(*lit as i64)), op))
        }

        // Signed integers vs NegativeInteger literal
        (ContentTypes::Int8(v), Literal::NegativeInteger(lit)) => {
            Ok(apply_op((*v as i64).cmp(&-(*lit as i64)), op))
        }
        (ContentTypes::Int16(v), Literal::NegativeInteger(lit)) => {
            Ok(apply_op((*v as i64).cmp(&-(*lit as i64)), op))
        }
        (ContentTypes::Int32(v), Literal::NegativeInteger(lit)) => {
            Ok(apply_op((*v as i64).cmp(&-(*lit as i64)), op))
        }
        (ContentTypes::Int64(v), Literal::NegativeInteger(lit)) => {
            Ok(apply_op((*v).cmp(&-(*lit as i64)), op))
        }

        // Unsigned integers vs Integer literal
        (ContentTypes::UInt8(v), Literal::Integer(lit)) => Ok(apply_op((*v as u64).cmp(lit), op)),
        (ContentTypes::UInt16(v), Literal::Integer(lit)) => Ok(apply_op((*v as u64).cmp(lit), op)),
        (ContentTypes::UInt32(v), Literal::Integer(lit)) => Ok(apply_op((*v as u64).cmp(lit), op)),
        (ContentTypes::UInt64(v), Literal::Integer(lit)) => Ok(apply_op(v.cmp(lit), op)),

        // Unsigned integers vs NegativeInteger — unsigned is always greater
        (ContentTypes::UInt8(_), Literal::NegativeInteger(_)) => {
            Ok(apply_op(std::cmp::Ordering::Greater, op))
        }
        (ContentTypes::UInt16(_), Literal::NegativeInteger(_)) => {
            Ok(apply_op(std::cmp::Ordering::Greater, op))
        }
        (ContentTypes::UInt32(_), Literal::NegativeInteger(_)) => {
            Ok(apply_op(std::cmp::Ordering::Greater, op))
        }
        (ContentTypes::UInt64(_), Literal::NegativeInteger(_)) => {
            Ok(apply_op(std::cmp::Ordering::Greater, op))
        }

        // Floats vs Float/NegativeFloat/Integer/NegativeInteger literals
        (ContentTypes::Float32(v), Literal::Float(lit)) => Ok((*v as f64)
            .partial_cmp(lit)
            .map_or(false, |ord| apply_op(ord, op))),
        (ContentTypes::Float64(v), Literal::Float(lit)) => {
            Ok(v.partial_cmp(lit).map_or(false, |ord| apply_op(ord, op)))
        }
        (ContentTypes::Float32(v), Literal::NegativeFloat(lit)) => Ok((*v as f64)
            .partial_cmp(&-lit)
            .map_or(false, |ord| apply_op(ord, op))),
        (ContentTypes::Float64(v), Literal::NegativeFloat(lit)) => {
            Ok(v.partial_cmp(&-lit).map_or(false, |ord| apply_op(ord, op)))
        }
        (ContentTypes::Float32(v), Literal::Integer(lit)) => Ok((*v as f64)
            .partial_cmp(&(*lit as f64))
            .map_or(false, |ord| apply_op(ord, op))),
        (ContentTypes::Float64(v), Literal::Integer(lit)) => Ok(v
            .partial_cmp(&(*lit as f64))
            .map_or(false, |ord| apply_op(ord, op))),
        (ContentTypes::Float32(v), Literal::NegativeInteger(lit)) => Ok((*v as f64)
            .partial_cmp(&-(*lit as f64))
            .map_or(false, |ord| apply_op(ord, op))),
        (ContentTypes::Float64(v), Literal::NegativeInteger(lit)) => Ok(v
            .partial_cmp(&-(*lit as f64))
            .map_or(false, |ord| apply_op(ord, op))),

        // Text vs String literal
        (ContentTypes::Text(v), Literal::Str(lit)) => {
            Ok(apply_op(v.as_str().cmp(lit.as_str()), op))
        }

        // Boolean vs Boolean literal (only Eq/Ne make sense)
        (ContentTypes::Boolean(v), Literal::Boolean(lit)) => Ok(apply_op(v.cmp(lit), op)),

        // Type mismatch
        (content, literal) => Err(DatabaseError::InvalidArgument(format!(
            "cannot compare {} with {:?}",
            content, literal
        ))),
    }
}

// ── Type conversion helpers ──────────────────────────────────────

/// Converts an AST `ColumnType` to a storage-layer `ColumnTypes`.
/// 1:1 mapping — separate types to keep parser decoupled from storage.
fn column_type_to_storage(ct: &ast::ColumnType) -> storage_types::ColumnTypes {
    match ct {
        ast::ColumnType::Boolean => storage_types::ColumnTypes::Boolean,
        ast::ColumnType::Text => storage_types::ColumnTypes::Text,
        ast::ColumnType::Int8 => storage_types::ColumnTypes::Int8,
        ast::ColumnType::Int16 => storage_types::ColumnTypes::Int16,
        ast::ColumnType::Int32 => storage_types::ColumnTypes::Int32,
        ast::ColumnType::Int64 => storage_types::ColumnTypes::Int64,
        ast::ColumnType::UInt8 => storage_types::ColumnTypes::UInt8,
        ast::ColumnType::UInt16 => storage_types::ColumnTypes::UInt16,
        ast::ColumnType::UInt32 => storage_types::ColumnTypes::UInt32,
        ast::ColumnType::UInt64 => storage_types::ColumnTypes::UInt64,
        ast::ColumnType::Float32 => storage_types::ColumnTypes::Float32,
        ast::ColumnType::Float64 => storage_types::ColumnTypes::Float64,
    }
}

/// Converts an AST `Literal` to a storage-layer `ContentTypes`,
/// guided by the target column's `ColumnTypes`.
///
/// # Arguments
/// - `literal`: the parsed SQL literal value
/// - `target_type`: the column's declared type (from schema)
///
/// # Returns
/// The corresponding `ContentTypes` value.
///
/// # Errors
/// Returns `DatabaseError::SchemaViolation` if the literal cannot be
/// converted to the target type (e.g., string literal for an Int32 column),
/// or if the value overflows the target type's range.
//
// Conversion rules:
//   Literal::Null               + any type       → ContentTypes::Null
//   Literal::Boolean(b)         + Boolean        → ContentTypes::Boolean(b)
//   Literal::Str(s)             + Text           → ContentTypes::Text(s)
//   Literal::Integer(v)         + Int8/16/32/64  → ContentTypes::IntN(v as iN)   — check overflow!
//   Literal::Integer(v)         + UInt8/16/32/64 → ContentTypes::UIntN(v as uN)  — check overflow!
//   Literal::Integer(v)         + Float32/64     → ContentTypes::FloatN(v as fN)
//   Literal::NegativeInteger(v) + Int8/16/32/64  → ContentTypes::IntN(-(v as iN))  — check underflow!
//   Literal::NegativeInteger(v) + Float32/64     → ContentTypes::FloatN(-(v as fN))
//   Literal::Float(v)           + Float32/64     → ContentTypes::FloatN(v)
//   Literal::NegativeFloat(v)   + Float32/64     → ContentTypes::FloatN(-v)
//   anything else               → Err(SchemaViolation)
fn literal_to_content_type(
    literal: &Literal,
    target_type: &storage_types::ColumnTypes,
) -> Result<ContentTypes, DatabaseError> {
    match (literal, target_type) {
        // Null → Null (any column type)
        (Literal::Null, _) => Ok(ContentTypes::Null),

        // Boolean → Boolean
        (Literal::Boolean(b), storage_types::ColumnTypes::Boolean) => Ok(ContentTypes::Boolean(*b)),

        // String → Text
        (Literal::Str(s), storage_types::ColumnTypes::Text) => Ok(ContentTypes::Text(s.clone())),

        // Integer → signed integers (overflow check against iN::MAX)
        (Literal::Integer(v), storage_types::ColumnTypes::Int8) => {
            if *v > i8::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows Int8 (max {})",
                    v,
                    i8::MAX
                )));
            }
            Ok(ContentTypes::Int8(*v as i8))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::Int16) => {
            if *v > i16::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows Int16 (max {})",
                    v,
                    i16::MAX
                )));
            }
            Ok(ContentTypes::Int16(*v as i16))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::Int32) => {
            if *v > i32::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows Int32 (max {})",
                    v,
                    i32::MAX
                )));
            }
            Ok(ContentTypes::Int32(*v as i32))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::Int64) => {
            if *v > i64::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows Int64 (max {})",
                    v,
                    i64::MAX
                )));
            }
            Ok(ContentTypes::Int64(*v as i64))
        }

        // Integer → unsigned integers (overflow check against uN::MAX)
        (Literal::Integer(v), storage_types::ColumnTypes::UInt8) => {
            if *v > u8::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows UInt8 (max {})",
                    v,
                    u8::MAX
                )));
            }
            Ok(ContentTypes::UInt8(*v as u8))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::UInt16) => {
            if *v > u16::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows UInt16 (max {})",
                    v,
                    u16::MAX
                )));
            }
            Ok(ContentTypes::UInt16(*v as u16))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::UInt32) => {
            if *v > u32::MAX as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value {} overflows UInt32 (max {})",
                    v,
                    u32::MAX
                )));
            }
            Ok(ContentTypes::UInt32(*v as u32))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::UInt64) => Ok(ContentTypes::UInt64(*v)),

        // Integer → floats (no range check needed)
        (Literal::Integer(v), storage_types::ColumnTypes::Float32) => {
            Ok(ContentTypes::Float32(*v as f32))
        }
        (Literal::Integer(v), storage_types::ColumnTypes::Float64) => {
            Ok(ContentTypes::Float64(*v as f64))
        }

        // NegativeInteger → signed integers (underflow check against iN::MIN)
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Int8) => {
            if *v > i8::MIN.unsigned_abs() as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value -{} overflows Int8 (min {})",
                    v,
                    i8::MIN
                )));
            }
            Ok(ContentTypes::Int8(-(*v as i8)))
        }
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Int16) => {
            if *v > i16::MIN.unsigned_abs() as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value -{} overflows Int16 (min {})",
                    v,
                    i16::MIN
                )));
            }
            Ok(ContentTypes::Int16(-(*v as i16)))
        }
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Int32) => {
            if *v > i32::MIN.unsigned_abs() as u64 {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value -{} overflows Int32 (min {})",
                    v,
                    i32::MIN
                )));
            }
            Ok(ContentTypes::Int32(-(*v as i32)))
        }
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Int64) => {
            if *v > i64::MIN.unsigned_abs() {
                return Err(DatabaseError::SchemaViolation(format!(
                    "value -{} overflows Int64 (min {})",
                    v,
                    i64::MIN
                )));
            }
            Ok(ContentTypes::Int64(-(*v as i64)))
        }

        // NegativeInteger → floats
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Float32) => {
            Ok(ContentTypes::Float32(-(*v as f32)))
        }
        (Literal::NegativeInteger(v), storage_types::ColumnTypes::Float64) => {
            Ok(ContentTypes::Float64(-(*v as f64)))
        }

        // Float → floats
        (Literal::Float(v), storage_types::ColumnTypes::Float32) => {
            Ok(ContentTypes::Float32(*v as f32))
        }
        (Literal::Float(v), storage_types::ColumnTypes::Float64) => Ok(ContentTypes::Float64(*v)),

        // NegativeFloat → floats
        (Literal::NegativeFloat(v), storage_types::ColumnTypes::Float32) => {
            Ok(ContentTypes::Float32(-(*v as f32)))
        }
        (Literal::NegativeFloat(v), storage_types::ColumnTypes::Float64) => {
            Ok(ContentTypes::Float64(-*v))
        }

        // Type mismatch — catch-all
        (literal, target) => Err(DatabaseError::SchemaViolation(format!(
            "cannot convert {:?} to {}",
            literal, target
        ))),
    }
}

/// Converts a `ContentTypes` value to its display string.
///
/// Used by `pretty_result_print` to render cell values in SELECT output.
/// Null → `"NULL"`, Boolean → `"true"`/`"false"`, numeric types → decimal string,
/// Text → the raw string value.
fn content_type_string(value: &ContentTypes) -> String {
    match value {
        ContentTypes::Null => "NULL".to_string(),
        ContentTypes::Boolean(v) => {
            if *v {
                "true".to_string()
            } else {
                "false".to_string()
            }
        }
        ContentTypes::Text(v) => v.clone(),
        ContentTypes::Int8(v) => v.to_string(),
        ContentTypes::Int16(v) => v.to_string(),
        ContentTypes::Int32(v) => v.to_string(),
        ContentTypes::Int64(v) => v.to_string(),
        ContentTypes::UInt8(v) => v.to_string(),
        ContentTypes::UInt16(v) => v.to_string(),
        ContentTypes::UInt32(v) => v.to_string(),
        ContentTypes::UInt64(v) => v.to_string(),
        ContentTypes::Float32(v) => v.to_string(),
        ContentTypes::Float64(v) => v.to_string(),
        ContentTypes::OverflowText(_) => {
            unreachable!("OverflowText should be resolved by Table before reaching executor")
        }
    }
}

/// Formats an `ExecuteResult` as a human-readable string for REPL/CLI output.
///
/// - Non-SELECT results: single-line status message (e.g., `"Table created"`,
///   `"Inserted record id=5"`, `"Deleted 3 record(s)"`)
/// - SELECT results: psql-style column-aligned table with header, separator,
///   data rows, and row count footer
///
/// # Arguments
/// - `result`: the execution result to format
/// - `max_width`: optional column width limit; values exceeding this are truncated
///   (with `...` suffix when `width >= 4`). `None` means unlimited width.
pub fn pretty_result_print(result: ExecuteResult, max_width: Option<usize>) -> String {
    match result {
        ExecuteResult::Created => "Table created".to_string(),
        ExecuteResult::Dropped => "Table dropped".to_string(),
        ExecuteResult::Inserted { id } => format!("Inserted record id={}", id),
        ExecuteResult::Deleted { count } => format!("Deleted {} record(s)", count),
        ExecuteResult::Updated { count } => format!("Updated {} record(s)", count),
        ExecuteResult::Selected { columns, rows } => {
            let max_width = max_width.unwrap_or(usize::MAX);
            let mut columns_width: Vec<usize> = vec![0; columns.len()];
            // let row_height: Vec<usize> = vec![0; rows.len()];
            for (index, col) in columns.iter().enumerate() {
                let mut col_width = col.len();
                if col_width > max_width {
                    col_width = max_width;
                }
                columns_width[index] = col_width;
            }

            for row in rows.iter() {
                for (index, col) in row.iter().enumerate() {
                    let mut col_width = content_type_string(col).len();
                    if col_width > max_width {
                        col_width = max_width;
                    }
                    if col_width > columns_width[index] {
                        columns_width[index] = col_width;
                    }
                }
            }

            let mut lines_list: Vec<String> = Vec::with_capacity(3 + rows.len());
            let header_line = columns
                .iter()
                .zip(columns_width.iter())
                .map(|(col, size)| {
                    format!(
                        " {:<size$} ",
                        if col.len() > *size && *size >= 4 {
                            &col[..size - 3].to_string()
                        } else {
                            col
                        }
                    )
                })
                .collect::<Vec<String>>()
                .join("|");

            lines_list.push(header_line);

            let separation_line = columns_width
                .iter()
                .map(|width| "-".repeat(*width + 2))
                .collect::<Vec<String>>()
                .join("+");
            lines_list.push(separation_line);

            for row in &rows {
                let row_line = row
                    .iter()
                    .map(content_type_string)
                    .zip(columns_width.iter())
                    .map(|(st, size)| {
                        format!(
                            " {:<size$} ",
                            if st.len() > *size && *size >= 4 {
                                st[..size - 3].to_string()
                            } else {
                                st
                            }
                        )
                    })
                    .collect::<Vec<String>>()
                    .join("|");

                lines_list.push(row_line);
            }

            lines_list.push(format!("{} row(s) returned.", rows.len()));

            lines_list.join("\n")
        }
        ExecuteResult::TransactionStarted => "Transaction started".to_string(),
        ExecuteResult::Committed => "Transaction commited".to_string(),
        ExecuteResult::RolledBack => "Transaction rolled back".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── content_type_string tests ────────────────────────────────

    #[test]
    fn content_string_null() {
        assert_eq!(content_type_string(&ContentTypes::Null), "NULL");
    }

    #[test]
    fn content_string_boolean() {
        assert_eq!(content_type_string(&ContentTypes::Boolean(true)), "true");
        assert_eq!(content_type_string(&ContentTypes::Boolean(false)), "false");
    }

    #[test]
    fn content_string_text() {
        assert_eq!(
            content_type_string(&ContentTypes::Text("hello".to_string())),
            "hello"
        );
    }

    #[test]
    fn content_string_integers() {
        assert_eq!(content_type_string(&ContentTypes::Int8(-42)), "-42");
        assert_eq!(content_type_string(&ContentTypes::Int64(123456)), "123456");
        assert_eq!(content_type_string(&ContentTypes::UInt64(999)), "999");
    }

    #[test]
    fn content_string_floats() {
        assert_eq!(content_type_string(&ContentTypes::Float32(3.14)), "3.14");
        assert_eq!(content_type_string(&ContentTypes::Float64(2.718)), "2.718");
    }

    // ── apply_op tests ───────────────────────────────────────────

    #[test]
    fn apply_op_eq() {
        assert!(apply_op(std::cmp::Ordering::Equal, &CompOp::Eq));
        assert!(!apply_op(std::cmp::Ordering::Less, &CompOp::Eq));
        assert!(!apply_op(std::cmp::Ordering::Greater, &CompOp::Eq));
    }

    #[test]
    fn apply_op_ne() {
        assert!(!apply_op(std::cmp::Ordering::Equal, &CompOp::Ne));
        assert!(apply_op(std::cmp::Ordering::Less, &CompOp::Ne));
        assert!(apply_op(std::cmp::Ordering::Greater, &CompOp::Ne));
    }

    #[test]
    fn apply_op_lt_gt_le_ge() {
        assert!(apply_op(std::cmp::Ordering::Less, &CompOp::Lt));
        assert!(!apply_op(std::cmp::Ordering::Equal, &CompOp::Lt));

        assert!(apply_op(std::cmp::Ordering::Greater, &CompOp::Gt));
        assert!(!apply_op(std::cmp::Ordering::Equal, &CompOp::Gt));

        assert!(apply_op(std::cmp::Ordering::Less, &CompOp::Le));
        assert!(apply_op(std::cmp::Ordering::Equal, &CompOp::Le));
        assert!(!apply_op(std::cmp::Ordering::Greater, &CompOp::Le));

        assert!(apply_op(std::cmp::Ordering::Greater, &CompOp::Ge));
        assert!(apply_op(std::cmp::Ordering::Equal, &CompOp::Ge));
        assert!(!apply_op(std::cmp::Ordering::Less, &CompOp::Ge));
    }

    // ── pretty_result_print tests ────────────────────────────────

    #[test]
    fn pretty_print_simple_variants() {
        assert_eq!(
            pretty_result_print(ExecuteResult::Created, None),
            "Table created"
        );
        assert_eq!(
            pretty_result_print(ExecuteResult::Dropped, None),
            "Table dropped"
        );
        assert_eq!(
            pretty_result_print(ExecuteResult::Inserted { id: 5 }, None),
            "Inserted record id=5"
        );
        assert_eq!(
            pretty_result_print(ExecuteResult::Deleted { count: 3 }, None),
            "Deleted 3 record(s)"
        );
        assert_eq!(
            pretty_result_print(ExecuteResult::Updated { count: 0 }, None),
            "Updated 0 record(s)"
        );
    }

    #[test]
    fn pretty_print_select_empty() {
        let result = ExecuteResult::Selected {
            columns: vec!["id".to_string(), "name".to_string()],
            rows: vec![],
        };
        let output = pretty_result_print(result, None);
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("0 row(s) returned."));
    }

    #[test]
    fn pretty_print_select_with_rows() {
        let result = ExecuteResult::Selected {
            columns: vec!["id".to_string(), "age".to_string()],
            rows: vec![
                vec![ContentTypes::UInt64(0), ContentTypes::Int64(25)],
                vec![ContentTypes::UInt64(1), ContentTypes::Int64(30)],
            ],
        };
        let output = pretty_result_print(result, None);
        assert!(output.contains("id"));
        assert!(output.contains("age"));
        assert!(output.contains("25"));
        assert!(output.contains("30"));
        assert!(output.contains("2 row(s) returned."));
    }

    #[test]
    fn pretty_print_select_separator() {
        let result = ExecuteResult::Selected {
            columns: vec!["id".to_string()],
            rows: vec![],
        };
        let output = pretty_result_print(result, None);
        let lines: Vec<&str> = output.lines().collect();
        // Second line should be the separator
        assert!(lines[1].contains("---"));
    }

    // ── column_type_to_storage tests ─────────────────────────────

    #[test]
    fn column_type_conversion_all_types() {
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::Boolean),
            storage_types::ColumnTypes::Boolean
        );
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::Text),
            storage_types::ColumnTypes::Text
        );
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::Int8),
            storage_types::ColumnTypes::Int8
        );
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::Int64),
            storage_types::ColumnTypes::Int64
        );
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::UInt64),
            storage_types::ColumnTypes::UInt64
        );
        assert_eq!(
            column_type_to_storage(&ast::ColumnType::Float64),
            storage_types::ColumnTypes::Float64
        );
    }

    // ── literal_to_content_type tests ────────────────────────────

    #[test]
    fn literal_null_converts_to_null() {
        let result = literal_to_content_type(&Literal::Null, &ColumnTypes::Int64).unwrap();
        assert_eq!(result, ContentTypes::Null);
    }

    #[test]
    fn literal_integer_overflow_rejected() {
        let result = literal_to_content_type(&Literal::Integer(200), &ColumnTypes::Int8);
        assert!(result.is_err());
    }

    #[test]
    fn literal_type_mismatch_rejected() {
        let result =
            literal_to_content_type(&Literal::Str("hello".to_string()), &ColumnTypes::Int64);
        assert!(result.is_err());
    }
}
