use young_bird_database::database_operations::sql::{
    lexer::Lexer,
    token::{Keyword, Token},
};

#[test]
fn tokenize_select_statement() {
    let mut lexer = Lexer::new("SELECT name FROM users");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
        ]
    );
}

#[test]
fn tokenize_select_star() {
    let mut lexer = Lexer::new("SELECT * FROM users");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
        ]
    );
}

#[test]
fn tokenize_where_clause() {
    let mut lexer = Lexer::new("WHERE age >= 18");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Where),
            Token::Identifier("age".to_string()),
            Token::GreaterEqual,
            Token::IntegerLiteral(18),
        ]
    );
}

#[test]
fn tokenize_insert() {
    let mut lexer = Lexer::new("INSERT INTO users VALUES (1, 'alice')");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Insert),
            Token::Keyword(Keyword::Into),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Values),
            Token::LeftParen,
            Token::IntegerLiteral(1),
            Token::Comma,
            Token::StringLiteral("alice".to_string()),
            Token::RightParen,
        ]
    );
}

#[test]
fn tokenize_create_table() {
    let mut lexer = Lexer::new("CREATE TABLE users (id INT64 NOT NULL, name TEXT)");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Keyword(Keyword::Int64),
            Token::Keyword(Keyword::Not),
            Token::Keyword(Keyword::Null),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::Text),
            Token::RightParen,
        ]
    );
}

#[test]
fn tokenize_string_with_escaped_quote() {
    let mut lexer = Lexer::new("'it''s'");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![Token::StringLiteral("it's".to_string())]
    );
}

#[test]
fn tokenize_all_operators() {
    let mut lexer = Lexer::new("= != <> < > <= >=");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Equals,
            Token::NotEquals,
            Token::NotEquals,
            Token::LessThan,
            Token::GreaterThan,
            Token::LessEqual,
            Token::GreaterEqual,
        ]
    );
}

#[test]
fn tokenize_float_literal() {
    let mut lexer = Lexer::new("3.14");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(tokens, vec![Token::FloatLiteral(3.14)]);
}

#[test]
fn tokenize_boolean_literals() {
    let mut lexer = Lexer::new("TRUE FALSE");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::BooleanLiteral(true),
            Token::BooleanLiteral(false),
        ]
    );
}

#[test]
fn tokenize_case_insensitive() {
    let mut lexer = Lexer::new("select FROM Where");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Select),
            Token::Keyword(Keyword::From),
            Token::Keyword(Keyword::Where),
        ]
    );
}

#[test]
fn tokenize_unknown_char_error() {
    let mut lexer = Lexer::new("SELECT @");
    let result = lexer.tokenize();
    assert!(result.is_err());
}

#[test]
fn tokenize_unterminated_string_error() {
    let mut lexer = Lexer::new("'hello");
    let result = lexer.tokenize();
    assert!(result.is_err());
}

#[test]
fn tokenize_drop_table() {
    let mut lexer = Lexer::new("DROP TABLE users");
    let tokens = lexer.tokenize().unwrap();
    assert_eq!(
        tokens,
        vec![
            Token::Keyword(Keyword::Drop),
            Token::Keyword(Keyword::Table),
            Token::Identifier("users".to_string()),
        ]
    );
}
