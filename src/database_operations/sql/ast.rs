/// A parsed SQL statement.
#[derive(Debug, PartialEq, Clone)]
pub enum Statement {
    Select {
        columns: SelectColumns,
        table: String,
        where_clause: Option<Expr>,
    },
    Insert {
        table: String,
        values: Vec<Literal>,
    },
    Update {
        table: String,
        assignments: Vec<Assignment>,
        where_clause: Option<Expr>,
    },
    Delete {
        table: String,
        where_clause: Option<Expr>,
    },
    CreateTable {
        table: String,
        columns: Vec<ColumnSpec>,
    },
    DropTable {
        table: String,
    },
}

/// Which columns a SELECT returns.
#[derive(Debug, PartialEq, Clone)]
pub enum SelectColumns {
    /// SELECT *
    All,
    /// SELECT col1, col2, ...
    Named(Vec<String>),
}

/// A SET assignment: `column = value`.
#[derive(Debug, PartialEq, Clone)]
pub struct Assignment {
    pub column: String,
    pub value: Literal,
}

/// A column definition in CREATE TABLE.
#[derive(Debug, PartialEq, Clone)]
pub struct ColumnSpec {
    pub name: String,
    pub data_type: ColumnType,
    /// true by default, false if NOT NULL specified
    pub nullable: bool,
}

/// SQL type names (AST-level, decoupled from storage ColumnTypes).
#[derive(Debug, PartialEq, Clone)]
pub enum ColumnType {
    Boolean,
    Text,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
}

/// A literal value in SQL.
#[derive(Debug, PartialEq, Clone)]
pub enum Literal {
    Integer(u64),
    Float(f64),
    Str(String),
    Boolean(bool),
    Null,
    NegativeInteger(u64),
    NegativeFloat(f64),
}

/// Comparison operators in WHERE clauses.
#[derive(Debug, PartialEq, Clone)]
pub enum CompOp {
    Eq, // =
    Ne, // != or <>
    Lt, // <
    Gt, // >
    Le, // <=
    Ge, // >=
}

/// A WHERE clause expression.
#[derive(Debug, PartialEq, Clone)]
pub enum Expr {
    Comparison {
        column: String,
        op: CompOp,
        value: Literal,
    },
    And(Box<Expr>, Box<Expr>),
    Or(Box<Expr>, Box<Expr>),
    Not(Box<Expr>),
}
