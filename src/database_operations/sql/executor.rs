// Executor: Statement → Table API calls → ExecuteResult
//
// Pipeline:  SQL string → Lexer → Parser → **Executor** → disk I/O
//
// Input:  Statement (parsed AST node)
// Output: ExecuteResult (Created, Dropped, Inserted, Deleted, Updated, Selected)
//
// The executor owns configuration (base_path, pages_per_file, page_kbytes)
// and opens/creates tables as needed per statement.

use crate::database_operations::file_processing::errors::DatabaseError;
use crate::database_operations::file_processing::page::record::PageRecordContent;
use crate::database_operations::file_processing::table::{ColumnDef, Table};
use crate::database_operations::file_processing::types::{
    self as storage_types, ColumnTypes, ContentTypes,
};
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
    Inserted { id: u64 },
    /// DELETE succeeded; contains the number of records deleted.
    Deleted { count: usize },
    /// UPDATE succeeded; contains the number of records updated.
    Updated { count: usize },
    /// SELECT succeeded; contains column names and matching rows.
    /// Each row is a Vec<ContentTypes> where the first element is the `id` column.
    Selected {
        columns: Vec<String>,
        rows: Vec<Vec<ContentTypes>>,
    },
}

/// SQL statement executor. Translates AST nodes into Table API calls.
pub struct Executor {
    base_path: String,
    pages_per_file: u32,
    page_kbytes: u32,
}

impl Executor {
    pub fn new(base_path: String, pages_per_file: u32, page_kbytes: u32) -> Self {
        Self {
            base_path,
            pages_per_file,
            page_kbytes,
        }
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
    pub fn execute(&self, statement: Statement) -> Result<ExecuteResult, DatabaseError> {
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
        }
    }

    // ── CREATE TABLE ─────────────────────────────────────────────
    // SQL: CREATE TABLE t (name TEXT, age INT32 NOT NULL)
    // → convert Vec<ast::ColumnSpec> to Vec<ColumnDef>, call Table::create

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
            table_columns,
        )?;
        Ok(ExecuteResult::Created)
    }

    // ── DROP TABLE ───────────────────────────────────────────────
    // SQL: DROP TABLE t
    // → delete .meta, .idx, and all _N.dat files

    fn execute_drop_table(&self, table_name: &str) -> Result<ExecuteResult, DatabaseError> {
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

        Ok(ExecuteResult::Dropped)
    }

    // ── INSERT ───────────────────────────────────────────────────
    // SQL: INSERT INTO t VALUES (1, 'alice', true)
    // → open table, convert literals to ContentTypes using schema, call table.insert()

    fn execute_insert(
        &self,
        table_name: &str,
        values: Vec<Literal>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let mut table = Table::open(
            table_name.to_string(),
            self.base_path.clone(),
            self.pages_per_file,
        )?;
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
        let id = table.insert(record_content)?;
        Ok(ExecuteResult::Inserted { id })
    }

    // ── SELECT ───────────────────────────────────────────────────
    // SQL: SELECT * FROM t WHERE age > 18
    // → open table, scan with WHERE filter, project columns, prepend id

    fn execute_select(
        &self,
        table_name: &str,
        columns: SelectColumns,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let table = Table::open(
            table_name.to_string(),
            self.base_path.clone(),
            self.pages_per_file,
        )?;

        // TODO: validate WHERE column names against schema before scanning
        let column_defs = table.get_header().get_column_defs();
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
        let columns_count = match &columns {
            SelectColumns::All => column_defs.len() + 1,
            SelectColumns::Named(names) => names.len(),
        };
        let records = table.scan_records(|record_id, values| match &where_clause {
            None => true,
            Some(expr) => evaluate_expr(expr, record_id, values, column_defs).unwrap_or(false),
        })?;
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

    // ── DELETE ───────────────────────────────────────────────────
    // SQL: DELETE FROM t WHERE id = 3
    // → open table, scan with WHERE filter, delete each match

    fn execute_delete(
        &self,
        table_name: &str,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let mut table = Table::open(
            table_name.to_string(),
            self.base_path.clone(),
            self.pages_per_file,
        )?;
        // TODO: validate WHERE column names against schema before scanning
        let ids_to_delete = {
            let column_defs = table.get_header().get_column_defs();
            table.scan_record_ids(|record_id, values| match &where_clause {
                None => true,
                Some(expr) => evaluate_expr(expr, record_id, values, column_defs).unwrap_or(false),
            })?
        };
        for id in &ids_to_delete {
            table.delete_record(*id)?;
        }
        Ok(ExecuteResult::Deleted {
            count: ids_to_delete.len(),
        })
    }

    // ── UPDATE ───────────────────────────────────────────────────
    // SQL: UPDATE t SET name = 'bob' WHERE id = 1
    // → open table, scan with WHERE filter, apply assignments, update each match

    fn execute_update(
        &self,
        table_name: &str,
        assignments: Vec<ast::Assignment>,
        where_clause: Option<Expr>,
    ) -> Result<ExecuteResult, DatabaseError> {
        let mut table = Table::open(
            table_name.to_string(),
            self.base_path.clone(),
            self.pages_per_file,
        )?;

        // TODO: validate WHERE column names against schema before scanning
        let column_defs = table.get_header().get_column_defs();

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
                Some(expr) => evaluate_expr(expr, record_id, values, column_defs).unwrap_or(false),
            })?
        };

        if ids_to_update.is_empty() {
            return Ok(ExecuteResult::Updated { count: 0 });
        }

        for record_id in &ids_to_update {
            let record = table.read_record(*record_id)?;
            let mut content = record.get_content().clone();
            for (index, position) in column_pos_to_update.iter().enumerate() {
                content[*position] = column_values_to_update[index].clone();
            }
            table.update_record(*record_id, PageRecordContent::new(content))?;
        }

        let count = ids_to_update.len();

        Ok(ExecuteResult::Updated { count })
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
    }
}

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
        assert_eq!(pretty_result_print(ExecuteResult::Created, None), "Table created");
        assert_eq!(pretty_result_print(ExecuteResult::Dropped, None), "Table dropped");
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
        assert_eq!(column_type_to_storage(&ast::ColumnType::Boolean), storage_types::ColumnTypes::Boolean);
        assert_eq!(column_type_to_storage(&ast::ColumnType::Text), storage_types::ColumnTypes::Text);
        assert_eq!(column_type_to_storage(&ast::ColumnType::Int8), storage_types::ColumnTypes::Int8);
        assert_eq!(column_type_to_storage(&ast::ColumnType::Int64), storage_types::ColumnTypes::Int64);
        assert_eq!(column_type_to_storage(&ast::ColumnType::UInt64), storage_types::ColumnTypes::UInt64);
        assert_eq!(column_type_to_storage(&ast::ColumnType::Float64), storage_types::ColumnTypes::Float64);
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
        let result = literal_to_content_type(&Literal::Str("hello".to_string()), &ColumnTypes::Int64);
        assert!(result.is_err());
    }
}
