/// SQL keywords — reserved words that have special meaning in SQL statements.
#[derive(Debug, PartialEq, Clone)]
pub enum Keyword {
    // Statement starters
    Select,
    Insert,
    Update,
    Delete,
    Create,
    Drop,
    Table,

    // Clause words
    From,
    Into,
    Values,
    Set,
    Where,

    // Logical operators
    And,
    Or,
    Not,

    // Null
    Null,

    // Type names (matching our ColumnTypes enum)
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

impl Keyword {
    /// Maps an uppercase string to a Keyword, or None if it's not a keyword.
    pub fn parse_keyword(s: &str) -> Option<Keyword> {
        match s.to_uppercase().as_str() {
            // Statements
            "SELECT" => Some(Keyword::Select),
            "INSERT" => Some(Keyword::Insert),
            "UPDATE" => Some(Keyword::Update),
            "DELETE" => Some(Keyword::Delete),
            "CREATE" => Some(Keyword::Create),
            "DROP" => Some(Keyword::Drop),
            "TABLE" => Some(Keyword::Table),

            // Clauses
            "FROM" => Some(Keyword::From),
            "INTO" => Some(Keyword::Into),
            "VALUES" => Some(Keyword::Values),
            "SET" => Some(Keyword::Set),
            "WHERE" => Some(Keyword::Where),

            // Logical
            "AND" => Some(Keyword::And),
            "OR" => Some(Keyword::Or),
            "NOT" => Some(Keyword::Not),

            // Null
            "NULL" => Some(Keyword::Null),

            // Type names
            "BOOLEAN" => Some(Keyword::Boolean),
            "TEXT" => Some(Keyword::Text),
            "INT8" => Some(Keyword::Int8),
            "INT16" => Some(Keyword::Int16),
            "INT32" => Some(Keyword::Int32),
            "INT64" => Some(Keyword::Int64),
            "UINT8" => Some(Keyword::UInt8),
            "UINT16" => Some(Keyword::UInt16),
            "UINT32" => Some(Keyword::UInt32),
            "UINT64" => Some(Keyword::UInt64),
            "FLOAT32" => Some(Keyword::Float32),
            "FLOAT64" => Some(Keyword::Float64),

            _ => None,
        }
    }
}

/// A token — one classified piece of SQL text.
#[derive(Debug, PartialEq, Clone)]
pub enum Token {
    // A reserved SQL keyword
    Keyword(Keyword),

    // A user-chosen name (table name, column name)
    Identifier(String),

    // Literal values
    IntegerLiteral(u64),   // 42, 0, 999
    FloatLiteral(f64),     // 3.14, 0.5
    StringLiteral(String), // 'hello', 'it''s'
    BooleanLiteral(bool),  // TRUE, FALSE

    // Comparison operators
    Equals,       // =
    NotEquals,    // != or <>
    LessThan,     // <
    GreaterThan,  // >
    LessEqual,    // <=
    GreaterEqual, // >=

    // Punctuation
    Comma,      // ,
    LeftParen,  // (
    RightParen, // )
    Semicolon,  // ;
    Asterisk,   // *
    Minus,      // -
}
