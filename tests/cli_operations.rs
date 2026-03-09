use std::fs;
use std::process::Command;

fn binary_path() -> String {
    // Build first to ensure binary is up to date
    let build = Command::new("cargo")
        .args(["build"])
        .output()
        .expect("failed to build");
    assert!(build.status.success(), "cargo build failed");

    "target/debug/young_bird_database".to_string()
}

fn temp_dir(test_name: &str) -> String {
    let path = format!("test_cli_{}", test_name);
    fs::create_dir_all(&path).ok();
    path
}

fn cleanup_dir(path: &str) {
    fs::remove_dir_all(path).ok();
}

fn run_sql(base_path: &str, sql: &str) -> std::process::Output {
    Command::new(binary_path())
        .args(["--base-path", base_path, sql])
        .output()
        .expect("failed to execute binary")
}

fn stdout_str(output: &std::process::Output) -> String {
    String::from_utf8(output.stdout.clone()).unwrap()
}

fn stderr_str(output: &std::process::Output) -> String {
    String::from_utf8(output.stderr.clone()).unwrap()
}

// --- Help and args ---

#[test]
fn cli_help_flag() {
    let output = Command::new(binary_path())
        .args(["--help"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = stdout_str(&output);
    assert!(stdout.contains("Usage:"));
    assert!(stdout.contains("--max-width"));
    assert!(stdout.contains("--base-path"));
}

#[test]
fn cli_empty_sql_exits_with_error() {
    let dir = temp_dir("empty_sql");
    let output = Command::new(binary_path())
        .args(["--base-path", &dir, ""])
        .output()
        .unwrap();
    // Empty input should fail — but the arg parser treats "" as the SQL string
    // which is empty, so it should exit with code 1
    assert!(!output.status.success());
    assert!(stderr_str(&output).contains("Empty input"));
    cleanup_dir(&dir);
}

#[test]
fn cli_missing_max_width_value() {
    let output = Command::new(binary_path())
        .args(["--max-width"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(stderr_str(&output).contains("max-width value not provided"));
}

#[test]
fn cli_invalid_max_width() {
    let output = Command::new(binary_path())
        .args(["--max-width", "abc"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(stderr_str(&output).contains("not a valid number"));
}

// --- SQL execution ---

#[test]
fn cli_create_table() {
    let dir = temp_dir("create");
    let output = run_sql(&dir, "CREATE TABLE users (name TEXT, age INT64)");
    assert!(output.status.success());
    assert!(stdout_str(&output).contains("Table created"));
    cleanup_dir(&dir);
}

#[test]
fn cli_sql_error_exits_nonzero() {
    let dir = temp_dir("sql_error");
    // SELECT from nonexistent table
    let output = run_sql(&dir, "SELECT * FROM nonexistent");
    assert!(!output.status.success());
    assert!(!stderr_str(&output).is_empty());
    cleanup_dir(&dir);
}

#[test]
fn cli_max_width_flag() {
    let dir = temp_dir("max_width");
    run_sql(&dir, "CREATE TABLE items (description TEXT)");
    run_sql(&dir, "INSERT INTO items VALUES ('this is a very long description that should be truncated')");
    let output = Command::new(binary_path())
        .args(["--base-path", &dir, "--max-width", "10", "SELECT * FROM items"])
        .output()
        .unwrap();
    assert!(output.status.success());
    let stdout = stdout_str(&output);
    // The long text should be truncated, so full string should NOT appear
    assert!(!stdout.contains("this is a very long description that should be truncated"));
    cleanup_dir(&dir);
}

// TODO(human): Implement a test that chains multiple SQL operations and verifies
// the full lifecycle works end-to-end via the CLI binary.
//
// Test name: cli_full_lifecycle
//
// Steps:
//   1. CREATE TABLE people (name TEXT, age INT64)
//   2. INSERT three records
//   3. SELECT * — verify all 3 rows appear in stdout
//   4. DELETE WHERE age > 25
//   5. SELECT * — verify only the remaining rows appear
//   6. DROP TABLE people
//   7. SELECT * FROM people — verify it fails (table gone)
//
// Use run_sql() and stdout_str()/stderr_str() helpers.
// Remember cleanup_dir() at the end.
#[test]
fn cli_full_lifecycle() {
    let dir = temp_dir("lifecycle");
    cleanup_dir(&dir);
}
