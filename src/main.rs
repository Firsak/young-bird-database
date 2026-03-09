use young_bird_database::database_operations::sql::executor::{
    pretty_result_print, ExecuteResult, Executor,
};
use young_bird_database::database_operations::sql::lexer::Lexer;
use young_bird_database::database_operations::sql::parser::Parser;
use std::io::{self, Write};

fn main() {
    let args = std::env::args().collect::<Vec<String>>();

    let mut max_width: Option<usize> = None;
    let mut buffer = String::new();
    let mut base_path = "data";
    let pages_per_file = 1000;
    let page_kbytes = 8;

    let interactive_mode = args.len() == 1;

    if !interactive_mode {
        let mut index = 1;
        loop {
            if index >= args.len() {
                break;
            }

            match &args[index] {
                s if s.as_str() == "--help" => {
                    println!("Usage: young_bird_database [OPTIONS] [SQL]");
                    println!();
                    println!("Arguments:");
                    println!("  [SQL]               SQL statement to execute (non-interactive mode)");
                    println!();
                    println!("Options:");
                    println!("  --max-width <N>     Set column width limit for output");
                    println!("  --base-path <PATH>  Set data directory (default: data)");
                    println!("  --help              Show this help message");
                    println!();
                    println!("If no SQL argument is provided, enters interactive REPL mode.");
                    std::process::exit(0);
                }
                s if s.as_str() == "--max-width" => {
                    index += 1;
                    let string_value = match args.get(index) {
                        Some(v) => v,
                        None => {
                            eprintln!("max-width value not provided");
                            std::process::exit(1);
                        }
                    };
                    match string_value.parse::<usize>() {
                        Ok(v) => {
                            max_width = Some(v);
                        }
                        Err(error) => {
                            eprintln!("provided max-width value is not a valid number: {}", error);
                            std::process::exit(1);
                        }
                    };
                }
                s if s.as_str() == "--base-path" => {
                    index += 1;
                    base_path = match args.get(index) {
                        Some(v) => v,
                        None => {
                            eprintln!("base-path value not provided");
                            std::process::exit(1);
                        }
                    };
                }
                s => {
                    buffer = s.clone();
                }
            }

            index += 1;
        }
    }

    std::fs::create_dir_all(base_path).unwrap();

    let executor = Executor::new(base_path.to_string(), pages_per_file, page_kbytes);

    if interactive_mode {
        loop {
            print!("sql> ");
            io::stdout().flush().unwrap();

            buffer.clear();
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

            let res = match process_execution(&executor, input) {
                Ok(value) => value,
                Err(error) => {
                    println!("{}", error);
                    continue;
                }
            };

            println!("{}", pretty_result_print(res, max_width));
        }
    } else {
        let input = buffer.trim();

        if input.is_empty() {
            eprintln!("Empty input");
            std::process::exit(1);
        }

        let res = match process_execution(&executor, input) {
            Ok(value) => value,
            Err(error) => {
                eprintln!("{}", error);
                std::process::exit(1);
            }
        };

        println!("{}", pretty_result_print(res, max_width));
    }
}

fn process_execution(executor: &Executor, input: &str) -> Result<ExecuteResult, String> {
    let mut lexer = Lexer::new(input);
    let tokens = lexer
        .tokenize()
        .map_err(|e| format!("Error tokenizing input: {}", e))?;
    let mut parser = Parser::new(tokens);
    let statement = parser
        .parse()
        .map_err(|e| format!("Error parsing tokens: {}", e))?;
    executor
        .execute(statement)
        .map_err(|e| format!("Error executing statement: {}", e))
}
