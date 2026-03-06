// Parser: Vec<Token> → Statement (AST)
//
// [Keyword(Select), Asterisk, Keyword(From), Identifier("users"),
//  Keyword(Where), Identifier("age"), GreaterEqual, IntegerLiteral(18)]
//  → Statement::Select {
//        columns: All,
//        table: "users",
//        where_clause: Some(Comparison { column: "age", op: Ge, value: Integer(18) })
//    }
//
// Input: Vec<Token> (from Lexer — we work with tokens, NOT characters)
// Output: Statement enum (structured AST ready for execution)

use super::ast::*;
use super::token::{Keyword, Token};

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Parser {
        Parser {
            tokens,
            position: 0,
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn advance(&mut self) -> Option<&Token> {
        let value = self.tokens.get(self.position);
        self.position += 1;
        value
    }

    fn at_end(&self) -> bool {
        self.position >= self.tokens.len()
    }

    // How expect helpers work — example: parsing `DROP TABLE users`
    //
    // expect_keyword(Keyword::Drop)?    → advance, token is Keyword(Drop) → match! Ok(())
    // expect_keyword(Keyword::Table)?   → advance, token is Keyword(Table) → match! Ok(())
    // expect_identifier()?              → advance, token is Identifier("users") → Ok("users")
    //
    // If input was `DROP users` instead:
    // expect_keyword(Keyword::Drop)?    → advance, token is Keyword(Drop) → match! Ok(())
    // expect_keyword(Keyword::Table)?   → advance, token is Identifier("users") → NOT a keyword → Err!
    //
    // Each expect call: advance one token, check if it matches, error if not.
    // expect_keyword / expect_token return Ok(()) — just confirmation
    // expect_identifier returns Ok(name) — you need the actual string for the AST

    /// Consume the next token if it is the expected keyword, otherwise return an error.
    fn expect_keyword(&mut self, keyword: Keyword) -> Result<(), String> {
        match self.advance() {
            Some(Token::Keyword(k)) if *k == keyword => Ok(()),
            Some(other) => Err(format!("Expected {:?}, found {:?}", keyword, other)),
            None => Err(format!("Expected {:?}, found end of input", keyword)),
        }
    }

    /// Consume the next token if it matches exactly, otherwise return an error.
    fn expect_token(&mut self, expected: &Token) -> Result<(), String> {
        match self.advance() {
            Some(t) if t == expected => Ok(()),
            Some(other) => Err(format!("Expected {:?}, found {:?}", expected, other)),
            None => Err(format!("Expected {:?}, found end of input", expected)),
        }
    }

    /// Consume the next token if it is an identifier, returning the name.
    fn expect_identifier(&mut self) -> Result<String, String> {
        match self.advance() {
            Some(Token::Identifier(name)) => Ok(name.clone()),
            Some(other) => Err(format!("Expected identifier, found {:?}", other)),
            None => Err("Expected identifier, found end of input".to_string()),
        }
    }

    /// Entry point: parse a single SQL statement with optional trailing semicolon.
    pub fn parse(&mut self) -> Result<Statement, String> {
        let statement = match self.peek() {
            Some(Token::Keyword(token)) => match token {
                Keyword::Select => self.parse_select(),
                Keyword::Insert => self.parse_insert(),
                Keyword::Drop => self.parse_drop_table(),
                Keyword::Delete => self.parse_delete(),
                Keyword::Update => self.parse_update(),
                Keyword::Create => self.parse_create_table(),
                _ => Err("Starting token should be one of the statement tokens: SELECT, INSERT, DROP, DELETE, UPDATE, CREATE".to_string())
            },
            _ => Err("No tokens found while parsing".to_string()),
        }?;
        if let Some(Token::Semicolon) = self.peek() {
            self.advance();
        }
        if !self.at_end() {
            return Err("Unexpected tokens after statement".to_string());
        }
        Ok(statement)
    }

    // CREATE TABLE name (col1 TYPE, col2 TYPE NOT NULL, ...)
    // Tokens: [Create, Table, Identifier, LeftParen, Identifier, Keyword(type), Comma, ..., RightParen]
    fn parse_create_table(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Create)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.expect_identifier()?;
        self.expect_token(&Token::LeftParen)?;
        let column_name = self.expect_identifier()?;
        let column_type = self.parse_column_type()?;
        let mut is_null = true;
        if let Some(Token::Keyword(Keyword::Not)) = self.peek() {
            self.advance();
            self.expect_keyword(Keyword::Null)?;
            is_null = false;
        }
        let mut column_specs = vec![ColumnSpec {
            name: column_name,
            data_type: column_type,
            nullable: is_null,
        }];
        while let Some(Token::Comma) = self.peek() {
            self.advance();
            let column_name = self.expect_identifier()?;
            let column_type = self.parse_column_type()?;
            let mut is_null = true;
            if let Some(Token::Keyword(Keyword::Not)) = self.peek() {
                self.advance();
                self.expect_keyword(Keyword::Null)?;
                is_null = false;
            }
            column_specs.push(ColumnSpec {
                name: column_name,
                data_type: column_type,
                nullable: is_null,
            });
        }
        self.expect_token(&Token::RightParen)?;
        Ok(Statement::CreateTable {
            table: table_name,
            columns: column_specs,
        })
    }

    /// Map a type keyword token to a ColumnType AST node.
    fn parse_column_type(&mut self) -> Result<ColumnType, String> {
        match self.advance() {
            Some(Token::Keyword(Keyword::Boolean)) => Ok(ColumnType::Boolean),
            Some(Token::Keyword(Keyword::Text)) => Ok(ColumnType::Text),
            Some(Token::Keyword(Keyword::Int8)) => Ok(ColumnType::Int8),
            Some(Token::Keyword(Keyword::Int16)) => Ok(ColumnType::Int16),
            Some(Token::Keyword(Keyword::Int32)) => Ok(ColumnType::Int32),
            Some(Token::Keyword(Keyword::Int64)) => Ok(ColumnType::Int64),
            Some(Token::Keyword(Keyword::UInt8)) => Ok(ColumnType::UInt8),
            Some(Token::Keyword(Keyword::UInt16)) => Ok(ColumnType::UInt16),
            Some(Token::Keyword(Keyword::UInt32)) => Ok(ColumnType::UInt32),
            Some(Token::Keyword(Keyword::UInt64)) => Ok(ColumnType::UInt64),
            Some(Token::Keyword(Keyword::Float32)) => Ok(ColumnType::Float32),
            Some(Token::Keyword(Keyword::Float64)) => Ok(ColumnType::Float64),
            Some(other) => Err(format!("Expected type name, found {:?}", other)),
            None => Err("Expected type name, found end of input".to_string()),
        }
    }

    // UPDATE table SET col = val, col2 = val2 WHERE ...
    // Tokens: [Update, Identifier, Set, Identifier, Equals, Literal, Comma, ..., Where?, ...]
    fn parse_update(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Update)?;
        let table_name = self.expect_identifier()?;
        self.expect_keyword(Keyword::Set)?;
        let column_name = self.expect_identifier()?;
        self.expect_token(&Token::Equals)?;
        let column_value = self.parse_literal()?;
        let mut assignments = vec![Assignment {
            column: column_name,
            value: column_value,
        }];
        while let Some(Token::Comma) = self.peek() {
            self.advance();
            let column_name = self.expect_identifier()?;
            self.expect_token(&Token::Equals)?;
            let column_value = self.parse_literal()?;
            assignments.push(Assignment {
                column: column_name,
                value: column_value,
            });
        }
        let where_expected = self.peek();
        let where_expr = match where_expected {
            Some(Token::Keyword(Keyword::Where)) => {
                self.expect_keyword(Keyword::Where)?;
                Some(self.parse_where()?)
            }
            _ => None,
        };
        Ok(Statement::Update {
            table: table_name,
            assignments,
            where_clause: where_expr,
        })
    }

    // SELECT * FROM table WHERE ...   or   SELECT col1, col2 FROM table WHERE ...
    // Tokens: [Select, Asterisk|Identifier, ..., From, Identifier, Where?, ...]
    fn parse_select(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Select)?;
        let all_or_left_identifier = self.peek();
        let select_columns: SelectColumns = match all_or_left_identifier {
            Some(Token::Asterisk) => {
                self.advance();
                SelectColumns::All
            }
            _ => {
                let mut values: Vec<String> = vec![];
                values.push(self.expect_identifier()?);
                while self.peek() == Some(&Token::Comma) {
                    self.advance();
                    values.push(self.expect_identifier()?);
                }
                SelectColumns::Named(values)
            }
        };
        self.expect_keyword(Keyword::From)?;
        let table_name = self.expect_identifier()?;
        let where_expected = self.peek();
        let where_expr = match where_expected {
            Some(Token::Keyword(Keyword::Where)) => {
                self.expect_keyword(Keyword::Where)?;
                Some(self.parse_where()?)
            }
            _ => None,
        };
        Ok(Statement::Select {
            columns: select_columns,
            table: table_name,
            where_clause: where_expr,
        })
    }

    // INSERT INTO table VALUES (val1, val2, ...)
    // Tokens: [Insert, Into, Identifier, Values, LeftParen, Literal, Comma, ..., RightParen]
    fn parse_insert(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Insert)?;
        self.expect_keyword(Keyword::Into)?;
        let table_name = self.expect_identifier()?;
        self.expect_keyword(Keyword::Values)?;
        self.expect_token(&Token::LeftParen)?;
        let first_literal = self.parse_literal()?;
        let mut values: Vec<Literal> = vec![first_literal];
        while self.peek() == Some(&Token::Comma) {
            self.advance();
            values.push(self.parse_literal()?);
        }
        self.expect_token(&Token::RightParen)?;
        Ok(Statement::Insert {
            table: table_name,
            values,
        })
    }

    // DROP TABLE name
    // Tokens: [Drop, Table, Identifier]
    fn parse_drop_table(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Drop)?;
        self.expect_keyword(Keyword::Table)?;
        let table_name = self.expect_identifier()?;
        Ok(Statement::DropTable { table: table_name })
    }

    // DELETE FROM table WHERE ...
    // Tokens: [Delete, From, Identifier, Where?, ...]
    fn parse_delete(&mut self) -> Result<Statement, String> {
        self.expect_keyword(Keyword::Delete)?;
        self.expect_keyword(Keyword::From)?;
        let table_name = self.expect_identifier()?;
        let where_expected = self.peek();
        let where_expr = match where_expected {
            Some(Token::Keyword(Keyword::Where)) => {
                self.expect_keyword(Keyword::Where)?;
                Some(self.parse_where()?)
            }
            _ => None,
        };
        Ok(Statement::Delete {
            table: table_name,
            where_clause: where_expr,
        })
    }

    // WHERE clause entry point — delegates to precedence chain.
    // Precedence (low → high): OR → AND → NOT → comparison
    // Lower-precedence operators sit higher in the call chain,
    // so they become the outermost nodes in the AST.
    fn parse_where(&mut self) -> Result<Expr, String> {
        self.parse_or_expr()
    }

    // a = 1 OR b = 2 OR c = 3
    // Tokens: [Identifier, Op, Literal, Or, Identifier, Op, Literal, Or, ...]
    fn parse_or_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_and_expr()?;
        while let Some(Token::Keyword(Keyword::Or)) = self.peek() {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::Or(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // a = 1 AND b = 2 AND c = 3
    // Tokens: [Identifier, Op, Literal, And, Identifier, Op, Literal, And, ...]
    fn parse_and_expr(&mut self) -> Result<Expr, String> {
        let mut left = self.parse_not_expr()?;
        while let Some(Token::Keyword(Keyword::And)) = self.peek() {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::And(Box::new(left), Box::new(right));
        }
        Ok(left)
    }

    // NOT age > 18   or just   age > 18 (no NOT, falls through to comparison)
    // Tokens: [Not?, Identifier, Op, Literal]
    fn parse_not_expr(&mut self) -> Result<Expr, String> {
        if let Some(Token::Keyword(Keyword::Not)) = self.peek() {
            self.advance();
            Ok(Expr::Not(Box::new(self.parse_comparison()?)))
        } else {
            self.parse_comparison()
        }
    }

    // age >= 18   or   name = 'alice'
    // Tokens: [Identifier, CompOp, Literal]
    fn parse_comparison(&mut self) -> Result<Expr, String> {
        let column = self.expect_identifier()?;
        let op = self.parse_comp_op()?;
        let value = self.parse_literal()?;
        Ok(Expr::Comparison { column, op, value })
    }

    /// Consume one comparison operator token and return the corresponding CompOp.
    fn parse_comp_op(&mut self) -> Result<CompOp, String> {
        match self.advance() {
            Some(Token::Equals) => Ok(CompOp::Eq),
            Some(Token::NotEquals) => Ok(CompOp::Ne),
            Some(Token::LessThan) => Ok(CompOp::Lt),
            Some(Token::GreaterThan) => Ok(CompOp::Gt),
            Some(Token::LessEqual) => Ok(CompOp::Le),
            Some(Token::GreaterEqual) => Ok(CompOp::Ge),
            Some(other) => Err(format!("Expected comparison operator, found {:?}", other)),
            None => Err("Expected comparison operator, found end of input".to_string()),
        }
    }

    fn parse_literal(&mut self) -> Result<Literal, String> {
        match self.peek() {
            Some(Token::IntegerLiteral(n)) => {
                let value = *n; // copy u64 out (Copy type)
                self.advance(); // safe — no reference held
                Ok(Literal::Integer(value))
            }
            Some(Token::FloatLiteral(n)) => {
                let value = *n;
                self.advance();
                Ok(Literal::Float(value))
            }
            Some(Token::StringLiteral(s)) => {
                let value = s.clone(); // clone String out (not Copy, needs clone)
                self.advance();
                Ok(Literal::Str(value))
            }
            Some(Token::BooleanLiteral(b)) => {
                let value = *b;
                self.advance();
                Ok(Literal::Boolean(value))
            }
            Some(Token::Keyword(Keyword::Null)) => {
                self.advance();
                Ok(Literal::Null)
            }
            Some(Token::Minus) => {
                self.advance();
                match self.peek() {
                    Some(Token::IntegerLiteral(n)) => {
                        let value = *n;
                        self.advance();
                        Ok(Literal::NegativeInteger(value))
                    }
                    Some(Token::FloatLiteral(n)) => {
                        let value = *n;
                        self.advance();
                        Ok(Literal::NegativeFloat(value))
                    }
                    _ => Err("Numerical literal expected after minus".to_string()),
                }
            }
            None => Err("No token found".to_string()),
            _ => Err("Unexpected token, should be Literal".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::database_operations::sql::token::{Keyword, Token};

    #[test]
    fn parse_empty_tokens() {
        let mut parser = Parser::new(vec![]);
        assert!(parser.parse().is_err());
    }

    #[test]
    fn parse_unknown_start() {
        let mut parser = Parser::new(vec![Token::Identifier("foo".to_string())]);
        assert!(parser.parse().is_err());
    }

    #[test]
    fn parse_drop_table_basic() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Drop),
            Token::Keyword(Keyword::Table),
            Token::Identifier("users".to_string()),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::DropTable {
                table: "users".to_string()
            }
        );
    }

    #[test]
    fn parse_drop_table_missing_name() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Drop),
            Token::Keyword(Keyword::Table),
        ]);
        assert!(parser.parse().is_err());
    }

    #[test]
    fn parse_literal_integer() {
        let mut parser = Parser::new(vec![Token::IntegerLiteral(42)]);
        assert_eq!(parser.parse_literal().unwrap(), Literal::Integer(42));
    }

    #[test]
    fn parse_literal_string() {
        let mut parser = Parser::new(vec![Token::StringLiteral("hello".to_string())]);
        assert_eq!(
            parser.parse_literal().unwrap(),
            Literal::Str("hello".to_string())
        );
    }

    #[test]
    fn parse_literal_negative() {
        let mut parser = Parser::new(vec![Token::Minus, Token::IntegerLiteral(5)]);
        assert_eq!(parser.parse_literal().unwrap(), Literal::NegativeInteger(5));
    }

    #[test]
    fn parse_literal_null() {
        let mut parser = Parser::new(vec![Token::Keyword(Keyword::Null)]);
        assert_eq!(parser.parse_literal().unwrap(), Literal::Null);
    }

    #[test]
    fn parse_literal_unexpected() {
        let mut parser = Parser::new(vec![Token::Asterisk]);
        assert!(parser.parse_literal().is_err());
    }

    #[test]
    fn parse_delete_basic() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Delete),
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Delete {
                table: "users".to_string(),
                where_clause: None,
            }
        );
    }

    #[test]
    fn parse_delete_missing_from() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Delete),
            Token::Identifier("users".to_string()),
        ]);
        assert!(parser.parse().is_err());
    }

    #[test]
    fn parse_insert_single() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Insert),
            Token::Keyword(Keyword::Into),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Values),
            Token::LeftParen,
            Token::IntegerLiteral(42),
            Token::RightParen,
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Insert {
                table: "t".to_string(),
                values: vec![Literal::Integer(42)],
            }
        );
    }

    #[test]
    fn parse_insert_multiple() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Insert),
            Token::Keyword(Keyword::Into),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Values),
            Token::LeftParen,
            Token::IntegerLiteral(1),
            Token::Comma,
            Token::StringLiteral("alice".to_string()),
            Token::Comma,
            Token::Keyword(Keyword::Null),
            Token::RightParen,
        ]);
        let result = parser.parse().unwrap();
        assert_eq!(
            result,
            Statement::Insert {
                table: "t".to_string(),
                values: vec![
                    Literal::Integer(1),
                    Literal::Str("alice".to_string()),
                    Literal::Null,
                ],
            }
        );
    }

    #[test]
    fn parse_insert_missing_paren() {
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Insert),
            Token::Keyword(Keyword::Into),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Values),
            Token::IntegerLiteral(1),
        ]);
        assert!(parser.parse().is_err());
    }

    // --- UPDATE tests ---

    #[test]
    fn parse_update_single_assignment() {
        // UPDATE users SET name = 'bob'
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Update),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Set),
            Token::Identifier("name".to_string()),
            Token::Equals,
            Token::StringLiteral("bob".to_string()),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Update {
                table: "users".to_string(),
                assignments: vec![Assignment {
                    column: "name".to_string(),
                    value: Literal::Str("bob".to_string()),
                }],
                where_clause: None,
            }
        );
    }

    #[test]
    fn parse_update_multiple_assignments() {
        // UPDATE users SET name = 'bob', age = 30
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Update),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Set),
            Token::Identifier("name".to_string()),
            Token::Equals,
            Token::StringLiteral("bob".to_string()),
            Token::Comma,
            Token::Identifier("age".to_string()),
            Token::Equals,
            Token::IntegerLiteral(30),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Update {
                table: "users".to_string(),
                assignments: vec![
                    Assignment {
                        column: "name".to_string(),
                        value: Literal::Str("bob".to_string()),
                    },
                    Assignment {
                        column: "age".to_string(),
                        value: Literal::Integer(30),
                    },
                ],
                where_clause: None,
            }
        );
    }

    #[test]
    fn parse_update_with_where() {
        // UPDATE users SET active = TRUE WHERE id = 5
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Update),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Set),
            Token::Identifier("active".to_string()),
            Token::Equals,
            Token::BooleanLiteral(true),
            Token::Keyword(Keyword::Where),
            Token::Identifier("id".to_string()),
            Token::Equals,
            Token::IntegerLiteral(5),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Update {
                table: "users".to_string(),
                assignments: vec![Assignment {
                    column: "active".to_string(),
                    value: Literal::Boolean(true),
                }],
                where_clause: Some(Expr::Comparison {
                    column: "id".to_string(),
                    op: CompOp::Eq,
                    value: Literal::Integer(5),
                }),
            }
        );
    }

    #[test]
    fn parse_update_missing_set() {
        // UPDATE users name = 'bob' (missing SET)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Update),
            Token::Identifier("users".to_string()),
            Token::Identifier("name".to_string()),
            Token::Equals,
            Token::StringLiteral("bob".to_string()),
        ]);
        assert!(parser.parse().is_err());
    }

    // --- CREATE TABLE tests ---

    #[test]
    fn parse_create_table_single_column() {
        // CREATE TABLE users (name TEXT)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::Text),
            Token::RightParen,
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::CreateTable {
                table: "users".to_string(),
                columns: vec![ColumnSpec {
                    name: "name".to_string(),
                    data_type: ColumnType::Text,
                    nullable: true,
                }],
            }
        );
    }

    #[test]
    fn parse_create_table_multiple_columns() {
        // CREATE TABLE users (name TEXT, age INT32, active BOOLEAN)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("users".to_string()),
            Token::LeftParen,
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::Text),
            Token::Comma,
            Token::Identifier("age".to_string()),
            Token::Keyword(Keyword::Int32),
            Token::Comma,
            Token::Identifier("active".to_string()),
            Token::Keyword(Keyword::Boolean),
            Token::RightParen,
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::CreateTable {
                table: "users".to_string(),
                columns: vec![
                    ColumnSpec {
                        name: "name".to_string(),
                        data_type: ColumnType::Text,
                        nullable: true,
                    },
                    ColumnSpec {
                        name: "age".to_string(),
                        data_type: ColumnType::Int32,
                        nullable: true,
                    },
                    ColumnSpec {
                        name: "active".to_string(),
                        data_type: ColumnType::Boolean,
                        nullable: true,
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_create_table_not_null() {
        // CREATE TABLE t (id INT64 NOT NULL, name TEXT)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("t".to_string()),
            Token::LeftParen,
            Token::Identifier("id".to_string()),
            Token::Keyword(Keyword::Int64),
            Token::Keyword(Keyword::Not),
            Token::Keyword(Keyword::Null),
            Token::Comma,
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::Text),
            Token::RightParen,
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::CreateTable {
                table: "t".to_string(),
                columns: vec![
                    ColumnSpec {
                        name: "id".to_string(),
                        data_type: ColumnType::Int64,
                        nullable: false,
                    },
                    ColumnSpec {
                        name: "name".to_string(),
                        data_type: ColumnType::Text,
                        nullable: true,
                    },
                ],
            }
        );
    }

    #[test]
    fn parse_create_table_missing_paren() {
        // CREATE TABLE t name TEXT (missing open paren)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Create),
            Token::Keyword(Keyword::Table),
            Token::Identifier("t".to_string()),
            Token::Identifier("name".to_string()),
            Token::Keyword(Keyword::Text),
        ]);
        assert!(parser.parse().is_err());
    }

    // --- WHERE clause tests ---

    #[test]
    fn parse_where_simple_comparison() {
        // WHERE age = 18
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("age".to_string()),
            Token::Equals,
            Token::IntegerLiteral(18),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Select {
                columns: SelectColumns::All,
                table: "users".to_string(),
                where_clause: Some(Expr::Comparison {
                    column: "age".to_string(),
                    op: CompOp::Eq,
                    value: Literal::Integer(18),
                }),
            }
        );
    }

    #[test]
    fn parse_where_and() {
        // WHERE a = 1 AND b = 2
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("a".to_string()),
            Token::Equals,
            Token::IntegerLiteral(1),
            Token::Keyword(Keyword::And),
            Token::Identifier("b".to_string()),
            Token::Equals,
            Token::IntegerLiteral(2),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Select {
                columns: SelectColumns::All,
                table: "t".to_string(),
                where_clause: Some(Expr::And(
                    Box::new(Expr::Comparison {
                        column: "a".to_string(),
                        op: CompOp::Eq,
                        value: Literal::Integer(1),
                    }),
                    Box::new(Expr::Comparison {
                        column: "b".to_string(),
                        op: CompOp::Eq,
                        value: Literal::Integer(2),
                    }),
                )),
            }
        );
    }

    #[test]
    fn parse_where_or() {
        // WHERE a = 1 OR b = 2
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("a".to_string()),
            Token::Equals,
            Token::IntegerLiteral(1),
            Token::Keyword(Keyword::Or),
            Token::Identifier("b".to_string()),
            Token::Equals,
            Token::IntegerLiteral(2),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Select {
                columns: SelectColumns::All,
                table: "t".to_string(),
                where_clause: Some(Expr::Or(
                    Box::new(Expr::Comparison {
                        column: "a".to_string(),
                        op: CompOp::Eq,
                        value: Literal::Integer(1),
                    }),
                    Box::new(Expr::Comparison {
                        column: "b".to_string(),
                        op: CompOp::Eq,
                        value: Literal::Integer(2),
                    }),
                )),
            }
        );
    }

    #[test]
    fn parse_where_not() {
        // WHERE NOT active = TRUE
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Keyword(Keyword::Not),
            Token::Identifier("active".to_string()),
            Token::Equals,
            Token::BooleanLiteral(true),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Select {
                columns: SelectColumns::All,
                table: "t".to_string(),
                where_clause: Some(Expr::Not(Box::new(Expr::Comparison {
                    column: "active".to_string(),
                    op: CompOp::Eq,
                    value: Literal::Boolean(true),
                }),)),
            }
        );
    }

    #[test]
    fn parse_where_precedence() {
        // WHERE a = 1 AND b = 2 OR c = 3
        // Should parse as: OR(AND(a=1, b=2), c=3)
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Select),
            Token::Asterisk,
            Token::Keyword(Keyword::From),
            Token::Identifier("t".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("a".to_string()),
            Token::Equals,
            Token::IntegerLiteral(1),
            Token::Keyword(Keyword::And),
            Token::Identifier("b".to_string()),
            Token::Equals,
            Token::IntegerLiteral(2),
            Token::Keyword(Keyword::Or),
            Token::Identifier("c".to_string()),
            Token::Equals,
            Token::IntegerLiteral(3),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Select {
                columns: SelectColumns::All,
                table: "t".to_string(),
                where_clause: Some(Expr::Or(
                    Box::new(Expr::And(
                        Box::new(Expr::Comparison {
                            column: "a".to_string(),
                            op: CompOp::Eq,
                            value: Literal::Integer(1),
                        }),
                        Box::new(Expr::Comparison {
                            column: "b".to_string(),
                            op: CompOp::Eq,
                            value: Literal::Integer(2),
                        }),
                    )),
                    Box::new(Expr::Comparison {
                        column: "c".to_string(),
                        op: CompOp::Eq,
                        value: Literal::Integer(3),
                    }),
                )),
            }
        );
    }

    #[test]
    fn parse_delete_with_where() {
        // DELETE FROM users WHERE id = 5
        let mut parser = Parser::new(vec![
            Token::Keyword(Keyword::Delete),
            Token::Keyword(Keyword::From),
            Token::Identifier("users".to_string()),
            Token::Keyword(Keyword::Where),
            Token::Identifier("id".to_string()),
            Token::Equals,
            Token::IntegerLiteral(5),
        ]);
        assert_eq!(
            parser.parse().unwrap(),
            Statement::Delete {
                table: "users".to_string(),
                where_clause: Some(Expr::Comparison {
                    column: "id".to_string(),
                    op: CompOp::Eq,
                    value: Literal::Integer(5),
                }),
            }
        );
    }
}
