// Lexer: SQL string → Vec<Token>
//
// "SELECT * FROM users WHERE age >= 18"
//  → [Keyword(Select), Asterisk, Keyword(From), Identifier("users"),
//     Keyword(Where), Identifier("age"), GreaterEqual, IntegerLiteral(18)]
//
// Input: &str (raw SQL text, character by character)
// Output: Vec<Token> (classified pieces — keywords, identifiers, literals, operators)

use super::token::{Keyword, Token};

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

    /// Returns the character at the current position without advancing.
    fn peek(&self) -> Option<char> {
        self.input.get(self.position).copied()
    }

    /// Returns the character at the current position and advances by one.
    fn advance(&mut self) -> Option<char> {
        let value = self.input.get(self.position).copied();
        self.position += 1;
        value
    }

    /// Returns the character one position ahead without advancing (lookahead).
    /// Used for two-character operators like `<=`, `>=`, `!=`, `<>`.
    fn peek_next(&self) -> Option<char> {
        self.input.get(self.position + 1).copied()
    }

    /// Advances past all whitespace characters at the current position.
    fn skip_whitespace(&mut self) {
        while match self.peek() {
            Some(x) => x.is_whitespace(),
            None => false,
        } {
            self.advance();
        }
    }

    /// Tokenizes the full input string into a `Vec<Token>`.
    ///
    /// Consumes characters left-to-right, dispatching to specialized readers
    /// based on the first character: alphabetic → `read_identifier_or_keyword`,
    /// digit → `read_number`, `'` → `read_string`, operators handled inline.
    /// Whitespace between tokens is skipped automatically.
    ///
    /// # Returns
    /// The complete token stream for the input.
    ///
    /// # Errors
    /// Returns `String` on unexpected characters, unterminated strings,
    /// or unparseable numbers.
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
                c if c.is_ascii_digit() => {
                    let token = self.read_number()?;
                    tokens.push(token);
                }
                '\'' => {
                    let token = self.read_string()?;
                    tokens.push(token);
                }
                '<' => {
                    let possible_next_char = self.peek_next();
                    if let Some(c) = possible_next_char && c == '=' {
                        self.advance();
                        tokens.push(Token::LessEqual);
                    } else if let Some(c) = possible_next_char && c == '>'  {
                        self.advance();
                        tokens.push(Token::NotEquals);
                    } else {
                        tokens.push(Token::LessThan);
                    }
                    self.advance();
                }
                '>' => {
                    let possible_next_char = self.peek_next();
                    if let Some(c) = possible_next_char && c == '=' {
                        self.advance();
                        tokens.push(Token::GreaterEqual);
                    } else {
                        tokens.push(Token::GreaterThan);
                    }
                    self.advance();
                }
                '!' => {
                    let possible_next_char = self.peek_next();
                    if let Some(c) = possible_next_char && c == '=' {
                        self.advance();
                        tokens.push(Token::NotEquals);
                    } else {
                        return Err("Bare \"!\" is not valid".to_string());
                    }
                    self.advance();
                }
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

    /// Reads an identifier or keyword starting at the current position.
    ///
    /// Consumes alphanumeric characters and underscores. The collected string
    /// is matched case-insensitively: `TRUE`/`FALSE` → `BooleanLiteral`,
    /// SQL keywords (SELECT, FROM, etc.) → `Keyword`, otherwise → `Identifier`.
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

    /// Reads a numeric literal (integer or float) starting at the current position.
    ///
    /// Collects digits first. If a `.` followed by a digit is found, collects
    /// the fractional part and returns `FloatLiteral`. Otherwise returns
    /// `IntegerLiteral`. A trailing `.` without a digit (e.g., `"3."`) leaves
    /// the dot unconsumed.
    ///
    /// # Errors
    /// Returns `String` if the collected characters can't be parsed as u64 or f64.
    fn read_number(&mut self) -> Result<Token, String> {
        let mut future_token: Vec<char> = vec![];
        // Current problem: the single loop collects digits AND dots together.
        // When we hit '.', we push it into future_token and advance BEFORE checking
        // if the next char is a digit. So on input "3.", the dot is already consumed
        // and we can't "undo" it — we return an error even though "3" alone is valid.
        //
        // Fix: use two separate loops.
        // Loop 1: collect only digits (stop when peek is NOT a digit)
        // Then:   if peek is '.' AND the char at position+2 is a digit,
        //         consume the dot + run loop 2 for fractional digits
        //         if peek is '.' but next is NOT a digit, leave dot alone — "3" is valid
        // Finally: parse collected chars as u64 (no dot) or f64 (has dot)
        while let Some(c) = self.peek() && (c.is_ascii_digit() || c == '.'){
            if c == '.' {
                if let Some(char_after_dot) = self.peek_next() && char_after_dot.is_ascii_digit() {
                    future_token.push(c);
                    self.advance();
                    future_token.push(char_after_dot);
                    self.advance();
                    while let Some(inner_c) = self.peek() && inner_c.is_ascii_digit() {
                        future_token.push(inner_c);
                        self.advance();
                    }
                    break;
                } else {
                    break;
                }
            } else {
                future_token.push(c);
                self.advance();
            }
        }

        let token_string: String = future_token.iter().collect();
        let token_u64_res = token_string.parse::<u64>();
        if let Ok(token_u64) = token_u64_res {
            return Ok(Token::IntegerLiteral(token_u64));
        }
        let token_f64_res = token_string.parse::<f64>();
        if let Ok(token_f64) = token_f64_res {
            return Ok(Token::FloatLiteral(token_f64));
        }

        Err(format!(
            "Could not convert {} into number",
            token_string.as_str()
        ))
    }

    /// Reads a string literal enclosed in single quotes.
    ///
    /// Starts after the opening `'`. Escaped quotes use SQL convention:
    /// `''` inside a string produces a single `'` character (e.g., `'it''s'` → `it's`).
    /// Consumes the closing `'`.
    ///
    /// # Errors
    /// Returns `String` if the input ends before a closing `'` is found.
    fn read_string(&mut self) -> Result<Token, String> {
        let mut future_token: Vec<char> = vec![];
        loop {
            self.advance();
            let current_char = self.peek();
            match current_char {
                None => return Err("No closing \"'\" symbol".to_string()),
                Some(c) => {
                    if c != '\'' {
                        future_token.push(c);
                        continue;
                    }
                    if let Some(maybe_extra) = self.peek_next() && maybe_extra == '\'' {
                        future_token.push('\'');
                        self.advance();
                    } else {
                        self.advance();
                        break;
                    }
                }
            }
        }
        let token_string: String = future_token.iter().collect();
        Ok(Token::StringLiteral(token_string))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_identifier() {
        let mut lexer = Lexer::new("users");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::Identifier("users".to_string())]);
    }

    #[test]
    fn read_keyword() {
        let mut lexer = Lexer::new("SELECT");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::Keyword(Keyword::Select)]);
    }

    #[test]
    fn read_boolean_true() {
        let mut lexer = Lexer::new("TRUE");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::BooleanLiteral(true)]);
    }

    #[test]
    fn read_integer() {
        let mut lexer = Lexer::new("42");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::IntegerLiteral(42)]);
    }

    #[test]
    fn read_float() {
        let mut lexer = Lexer::new("3.14");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::FloatLiteral(3.14)]);
    }

    #[test]
    fn read_float_no_fraction() {
        let mut lexer = Lexer::new("3.");
        // "3." should parse as integer 3, dot left unconsumed (causes error)
        let result = lexer.tokenize();
        assert!(result.is_err());
    }

    #[test]
    fn read_string_simple() {
        let mut lexer = Lexer::new("'hello'");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::StringLiteral("hello".to_string())]);
    }

    #[test]
    fn read_string_empty() {
        let mut lexer = Lexer::new("''");
        let tokens = lexer.tokenize().unwrap();
        assert_eq!(tokens, vec![Token::StringLiteral("".to_string())]);
    }
}
