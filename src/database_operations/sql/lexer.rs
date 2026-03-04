use super::token::{Keyword, Token};

// TODO(human): Implement read_identifier_or_keyword() method
pub struct Lexer {
    input: Vec<char>,
    position: usize,
}

impl Lexer {
    pub fn new(input: &str) -> Lexer {
        Lexer {
            input: input.chars().collect(),
            position: 0,
        }
    }

    fn peek(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    fn advance(&mut self) -> Option<char> {
        let value = self.input.get(self.position).copied();
        self.position += 1;
        value
    }

    fn skip_whitespace(&mut self) {
        while match self.peek() {
            Some(x) => x.is_whitespace(),
            None => false,
        } {
            self.advance();
        }
    }

    pub fn tokenize(&mut self) -> Result<Vec<Token>, String> {
        let mut tokens: Vec<Token> = vec![];
        let mut char_value = self.peek();
        if char_value.is_none() {
            return Err("Empty input".to_string());
        }

        while let Some(c) = char_value {
            match c {
                c if c.is_alphabetic() || c == '_' => {
                    let token = self.read_identifier_or_keyword();
                    tokens.push(token);
                }
                c if c.is_ascii_digit() => todo!(),
                '\'' => todo!(),
                '<' | '>' | '!' => todo!(),
                '=' => {
                    self.advance();
                    tokens.push(Token::Equals);
                }
                '(' => {
                    self.advance();
                    tokens.push(Token::LeftParen);
                }
                ')' => {
                    self.advance();
                    tokens.push(Token::RightParen);
                }
                ',' => {
                    self.advance();
                    tokens.push(Token::Comma);
                }
                ';' => {
                    self.advance();
                    tokens.push(Token::Semicolon);
                }
                '*' => {
                    self.advance();
                    tokens.push(Token::Asterisk);
                }
                '-' => {
                    self.advance();
                    tokens.push(Token::Minus);
                }
                _ => return Err(format!("Unexpected character: {}", c)),
            }
            self.skip_whitespace();
            char_value = self.peek();
        }

        Ok(tokens)
    }

    fn read_identifier_or_keyword(&mut self) -> Token {
        let mut future_token: Vec<char> = vec![];
        while let Some(c) = self.peek() && (c.is_alphanumeric() || c == '_'){
            future_token.push(c);
            self.advance();
        }

        let token_string: String = future_token.iter().collect();
        if token_string.to_uppercase().eq("TRUE") {
            return Token::BooleanLiteral(true);
        }
        if token_string.to_uppercase().eq("FALSE") {
            return Token::BooleanLiteral(false);
        }
        if let Some(k) = Keyword::parse_keyword(&token_string) {
            Token::Keyword(k)
        } else {
            Token::Identifier(token_string)
        }
    }
}
