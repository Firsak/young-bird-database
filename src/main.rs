use young_bird_database::database_operations::sql::executor::{pretty_result_print, Executor};
use young_bird_database::database_operations::sql::lexer::Lexer;
use young_bird_database::database_operations::sql::parser::Parser;
use std::io::{self, Write};

fn main() {
    let pages_per_file = 1000;
    let page_kbytes = 8;
    let base_path = "data";
    let max_width: Option<usize> = Some(20);
    std::fs::create_dir_all(base_path).unwrap();

    let executor = Executor::new(base_path.to_string(), pages_per_file, page_kbytes);

    loop {
        let mut buffer = String::new();

        print!("sql> ");
        io::stdout().flush().unwrap();

        let bytes_read = match io::stdin().read_line(&mut buffer) {
            Ok(bl) => bl,
            Err(error) => {
                println!("Error reading: {}", error);
                continue;
            }
        };

        if bytes_read == 0 {
            break;
        }

        let input = buffer.trim();

        if input == "exit" || input == "exit()" {
            break;
        }

        if input.is_empty() {
            continue;
        }

        let mut lexer = Lexer::new(input);
        let tokens = match lexer.tokenize() {
            Ok(tokens) => tokens,
            Err(error) => {
                println!("Error tokenizing your input: {}", error);
                continue;
            }
        };

        let mut parser = Parser::new(tokens);
        let statement = match parser.parse() {
            Ok(st) => st,
            Err(error) => {
                println!("Error parsing tokens: {}", error);
                continue;
            }
        };

        let res = match executor.execute(statement) {
            Ok(res) => res,
            Err(error) => {
                println!("Error executing statement: {}", error);
                continue;
            }
        };

        println!("{}", pretty_result_print(res, max_width));
    }
}
