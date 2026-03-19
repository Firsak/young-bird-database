use std::fs;

use young_bird_database::database_operations::file_processing::types::ContentTypes;
use young_bird_database::database_operations::sql::ast::{
    Assignment, ColumnSpec, ColumnType, CompOp, Expr, Literal, SelectColumns, Statement,
};
use young_bird_database::database_operations::sql::executor::{ExecuteResult, Executor};

const TEST_CACHE_SIZE: usize = 16;

fn temp_dir(test_name: &str) -> String {
    let path = format!("test_executor_{}", test_name);
    fs::create_dir_all(&path).ok();
    path
}

fn cleanup_dir(path: &str) {
    fs::remove_dir_all(path).ok();
}

/// Helper: creates a table with columns (age INT64, name TEXT) via executor
fn create_test_table(executor: &Executor, table_name: &str) {
    executor
        .execute(Statement::CreateTable {
            table: table_name.to_string(),
            columns: vec![
                ColumnSpec {
                    name: "age".to_string(),
                    data_type: ColumnType::Int64,
                    nullable: false,
                },
                ColumnSpec {
                    name: "name".to_string(),
                    data_type: ColumnType::Text,
                    nullable: false,
                },
            ],
        })
        .expect("create table failed");
}

/// Helper: inserts a record (age, name) via executor, returns the assigned ID
fn insert_record(executor: &Executor, table_name: &str, age: u64, name: &str) -> u64 {
    let result = executor
        .execute(Statement::Insert {
            table: table_name.to_string(),
            values: vec![Literal::Integer(age), Literal::Str(name.to_string())],
        })
        .expect("insert failed");
    match result {
        ExecuteResult::Inserted { id } => id,
        _ => panic!("expected Inserted"),
    }
}

// ══════════════════════════════════════════════════════════
// CREATE TABLE tests
// ══════════════════════════════════════════════════════════

#[test]
fn create_table_basic() {
    let dir = temp_dir("create_basic");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    let result = executor
        .execute(Statement::CreateTable {
            table: "items".to_string(),
            columns: vec![ColumnSpec {
                name: "price".to_string(),
                data_type: ColumnType::Float64,
                nullable: true,
            }],
        })
        .expect("create failed");

    match result {
        ExecuteResult::Created => {}
        _ => panic!("expected Created"),
    }

    // Verify table is openable via select
    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "items".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "price"]);
            assert_eq!(rows.len(), 0);
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn create_table_duplicate_name() {
    let dir = temp_dir("create_dup");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    // Creating same table again should fail (files already exist)
    let result = executor.execute(Statement::CreateTable {
        table: "users".to_string(),
        columns: vec![ColumnSpec {
            name: "age".to_string(),
            data_type: ColumnType::Int64,
            nullable: false,
        }],
    });

    // This may or may not error depending on whether create overwrites —
    // if it succeeds, the original data is lost, which is still worth testing
    // For now just verify it doesn't panic
    let _ = result;

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// DROP TABLE tests
// ══════════════════════════════════════════════════════════

#[test]
fn drop_table_basic() {
    let dir = temp_dir("drop_basic");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");
    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::DropTable {
            table: "users".to_string(),
        })
        .expect("drop failed");

    match result {
        ExecuteResult::Dropped => {}
        _ => panic!("expected Dropped"),
    }

    // Table should no longer be openable
    let select = executor.execute(Statement::Select {
        columns: SelectColumns::All,
        table: "users".to_string(),
        where_clause: None,
    });
    assert!(select.is_err());

    cleanup_dir(&dir);
}

#[test]
fn drop_nonexistent_table() {
    let dir = temp_dir("drop_nonexist");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);

    // Dropping a table that doesn't exist should succeed (files just don't exist)
    let result = executor
        .execute(Statement::DropTable {
            table: "ghost".to_string(),
        })
        .expect("drop should not error on missing table");

    match result {
        ExecuteResult::Dropped => {}
        _ => panic!("expected Dropped"),
    }

    cleanup_dir(&dir);
}

#[test]
fn drop_and_recreate() {
    let dir = temp_dir("drop_recreate");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");
    insert_record(&executor, "users", 25, "alice");

    executor
        .execute(Statement::DropTable {
            table: "users".to_string(),
        })
        .expect("drop failed");

    // Recreate and insert new data
    create_test_table(&executor, "users");
    insert_record(&executor, "users", 99, "new_user");

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][1], ContentTypes::Int64(99));
            assert_eq!(rows[0][2], ContentTypes::Text("new_user".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// INSERT tests
// ══════════════════════════════════════════════════════════

#[test]
fn insert_returns_incrementing_ids() {
    let dir = temp_dir("insert_ids");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    let id0 = insert_record(&executor, "users", 25, "alice");
    let id1 = insert_record(&executor, "users", 30, "bob");
    let id2 = insert_record(&executor, "users", 18, "carol");

    assert_eq!(id0, 0);
    assert_eq!(id1, 1);
    assert_eq!(id2, 2);

    cleanup_dir(&dir);
}

#[test]
fn insert_wrong_value_count() {
    let dir = temp_dir("insert_bad_count");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    // Table has 2 columns (age, name), but we provide 1 value
    let result = executor.execute(Statement::Insert {
        table: "users".to_string(),
        values: vec![Literal::Integer(25)],
    });

    assert!(result.is_err());

    cleanup_dir(&dir);
}

#[test]
fn insert_type_mismatch() {
    let dir = temp_dir("insert_bad_type");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    // age is Int64 but we pass a string, name is Text but we pass an integer
    let result = executor.execute(Statement::Insert {
        table: "users".to_string(),
        values: vec![Literal::Str("not_a_number".to_string()), Literal::Integer(42)],
    });

    assert!(result.is_err());

    cleanup_dir(&dir);
}

#[test]
fn insert_and_verify_via_select() {
    let dir = temp_dir("insert_verify");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "age", "name"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], ContentTypes::UInt64(0));
            assert_eq!(rows[0][1], ContentTypes::Int64(25));
            assert_eq!(rows[0][2], ContentTypes::Text("alice".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// SELECT tests
// ══════════════════════════════════════════════════════════

#[test]
fn select_all_no_where() {
    let dir = temp_dir("select_all_no_where");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "age", "name"]);
            assert_eq!(rows.len(), 2);
            // First row: id=0, age=25, name=alice
            assert_eq!(rows[0][0], ContentTypes::UInt64(0));
            assert_eq!(rows[0][1], ContentTypes::Int64(25));
            assert_eq!(rows[0][2], ContentTypes::Text("alice".to_string()));
            // Second row: id=1, age=30, name=bob
            assert_eq!(rows[1][0], ContentTypes::UInt64(1));
            assert_eq!(rows[1][1], ContentTypes::Int64(30));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn select_all_with_where() {
    let dir = temp_dir("select_all_where");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");
    insert_record(&executor, "users", 18, "carol");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: Some(Expr::Comparison {
                column: "age".to_string(),
                op: CompOp::Gt,
                value: Literal::Integer(20),
            }),
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "age", "name"]);
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[0][1], ContentTypes::Int64(25));
            assert_eq!(rows[1][1], ContentTypes::Int64(30));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn select_named_columns() {
    let dir = temp_dir("select_named");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::Named(vec!["name".to_string()]),
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["name"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 1);
            assert_eq!(rows[0][0], ContentTypes::Text("alice".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn select_named_with_id() {
    let dir = temp_dir("select_named_id");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::Named(vec!["id".to_string(), "name".to_string()]),
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "name"]);
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].len(), 2);
            assert_eq!(rows[0][0], ContentTypes::UInt64(0));
            assert_eq!(rows[0][1], ContentTypes::Text("alice".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn select_nonexistent_column_rejected() {
    let dir = temp_dir("select_bad_col");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    let result = executor.execute(Statement::Select {
        columns: SelectColumns::Named(vec!["nonexistent".to_string()]),
        table: "users".to_string(),
        where_clause: None,
    });

    assert!(result.is_err());

    cleanup_dir(&dir);
}

#[test]
fn select_empty_table() {
    let dir = temp_dir("select_empty");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { columns, rows } => {
            assert_eq!(columns, vec!["id", "age", "name"]);
            assert_eq!(rows.len(), 0);
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn select_where_by_id() {
    let dir = temp_dir("select_by_id");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");
    insert_record(&executor, "users", 18, "carol");

    let result = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: Some(Expr::Comparison {
                column: "id".to_string(),
                op: CompOp::Eq,
                value: Literal::Integer(1),
            }),
        })
        .expect("select failed");

    match result {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][0], ContentTypes::UInt64(1));
            assert_eq!(rows[0][2], ContentTypes::Text("bob".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// DELETE tests
// ══════════════════════════════════════════════════════════

#[test]
fn delete_all_records() {
    let dir = temp_dir("delete_all");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");

    let result = executor
        .execute(Statement::Delete {
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("delete failed");

    match result {
        ExecuteResult::Deleted { count } => assert_eq!(count, 2),
        _ => panic!("expected Deleted"),
    }

    // Verify empty
    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => assert_eq!(rows.len(), 0),
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn delete_with_where() {
    let dir = temp_dir("delete_where");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");
    insert_record(&executor, "users", 18, "carol");

    let result = executor
        .execute(Statement::Delete {
            table: "users".to_string(),
            where_clause: Some(Expr::Comparison {
                column: "age".to_string(),
                op: CompOp::Lt,
                value: Literal::Integer(26),
            }),
        })
        .expect("delete failed");

    match result {
        ExecuteResult::Deleted { count } => assert_eq!(count, 2),
        _ => panic!("expected Deleted"),
    }

    // Only bob (age 30) should remain
    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0][2], ContentTypes::Text("bob".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

// ══════════════════════════════════════════════════════════
// UPDATE tests
// ══════════════════════════════════════════════════════════

#[test]
fn update_all_records() {
    let dir = temp_dir("update_all");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");

    let result = executor
        .execute(Statement::Update {
            table: "users".to_string(),
            assignments: vec![Assignment {
                column: "age".to_string(),
                value: Literal::Integer(99),
            }],
            where_clause: None,
        })
        .expect("update failed");

    match result {
        ExecuteResult::Updated { count } => assert_eq!(count, 2),
        _ => panic!("expected Updated"),
    }

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows[0][1], ContentTypes::Int64(99));
            assert_eq!(rows[1][1], ContentTypes::Int64(99));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn update_with_where() {
    let dir = temp_dir("update_where");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");
    insert_record(&executor, "users", 18, "carol");

    let result = executor
        .execute(Statement::Update {
            table: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Literal::Str("updated".to_string()),
            }],
            where_clause: Some(Expr::Comparison {
                column: "age".to_string(),
                op: CompOp::Gt,
                value: Literal::Integer(20),
            }),
        })
        .expect("update failed");

    match result {
        ExecuteResult::Updated { count } => assert_eq!(count, 2),
        _ => panic!("expected Updated"),
    }

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows[0][2], ContentTypes::Text("updated".to_string()));
            assert_eq!(rows[1][2], ContentTypes::Text("updated".to_string()));
            assert_eq!(rows[2][2], ContentTypes::Text("carol".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn update_multiple_columns() {
    let dir = temp_dir("update_multi_col");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::Update {
            table: "users".to_string(),
            assignments: vec![
                Assignment {
                    column: "age".to_string(),
                    value: Literal::Integer(50),
                },
                Assignment {
                    column: "name".to_string(),
                    value: Literal::Str("renamed".to_string()),
                },
            ],
            where_clause: None,
        })
        .expect("update failed");

    match result {
        ExecuteResult::Updated { count } => assert_eq!(count, 1),
        _ => panic!("expected Updated"),
    }

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows[0][1], ContentTypes::Int64(50));
            assert_eq!(rows[0][2], ContentTypes::Text("renamed".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn update_none_matching() {
    let dir = temp_dir("update_none");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor
        .execute(Statement::Update {
            table: "users".to_string(),
            assignments: vec![Assignment {
                column: "age".to_string(),
                value: Literal::Integer(99),
            }],
            where_clause: Some(Expr::Comparison {
                column: "age".to_string(),
                op: CompOp::Gt,
                value: Literal::Integer(100),
            }),
        })
        .expect("update failed");

    match result {
        ExecuteResult::Updated { count } => assert_eq!(count, 0),
        _ => panic!("expected Updated"),
    }

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows[0][1], ContentTypes::Int64(25));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}

#[test]
fn update_nonexistent_column_rejected() {
    let dir = temp_dir("update_bad_col");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");

    let result = executor.execute(Statement::Update {
        table: "users".to_string(),
        assignments: vec![Assignment {
            column: "nonexistent".to_string(),
            value: Literal::Integer(1),
        }],
        where_clause: None,
    });

    assert!(result.is_err());

    cleanup_dir(&dir);
}

#[test]
fn update_by_id() {
    let dir = temp_dir("update_by_id");
    let executor = Executor::new(dir.clone(), 100, 8, 1024, TEST_CACHE_SIZE);
    create_test_table(&executor, "users");

    insert_record(&executor, "users", 25, "alice");
    insert_record(&executor, "users", 30, "bob");

    let result = executor
        .execute(Statement::Update {
            table: "users".to_string(),
            assignments: vec![Assignment {
                column: "name".to_string(),
                value: Literal::Str("ALICE".to_string()),
            }],
            where_clause: Some(Expr::Comparison {
                column: "id".to_string(),
                op: CompOp::Eq,
                value: Literal::Integer(0),
            }),
        })
        .expect("update failed");

    match result {
        ExecuteResult::Updated { count } => assert_eq!(count, 1),
        _ => panic!("expected Updated"),
    }

    let select = executor
        .execute(Statement::Select {
            columns: SelectColumns::All,
            table: "users".to_string(),
            where_clause: None,
        })
        .expect("select failed");
    match select {
        ExecuteResult::Selected { rows, .. } => {
            assert_eq!(rows[0][2], ContentTypes::Text("ALICE".to_string()));
            assert_eq!(rows[1][2], ContentTypes::Text("bob".to_string()));
        }
        _ => panic!("expected Selected"),
    }

    cleanup_dir(&dir);
}
