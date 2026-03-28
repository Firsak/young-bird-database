# Young Bird Database

A page-based database engine built from scratch in Rust with zero external dependencies. Everything — binary serialization, slotted page layout, hash index, SQL parser, and query executor — is implemented by hand to understand how databases work internally.

## Features

- **Binary storage engine** with custom serialization for all data types (integers, floats, text, booleans, null)
- **Slotted page layout** — metadata grows forward, content grows backward, with soft/hard delete and intra-page compaction
- **Multi-file tables** — schema in `.meta`, pages in `_N.dat` files, hash index in `.idx`
- **Overflow text** — large text values stored in separate `.overflow` files with fragmentation tracking and compaction
- **Hash index** with open addressing and linear probing for O(1) record lookup by ID
- **Buffer pool cache** — LRU page cache with write-back dirty tracking, configurable size via `--cache-size`
- **Inter-page compaction** — streaming two-page algorithm with O(page_size) memory
- **Full SQL pipeline** — Lexer → Parser (recursive descent) → Executor with WHERE clause support (AND/OR/NOT with precedence)
- **Transaction support** — `BEGIN`/`COMMIT`/`ROLLBACK` with write-ahead log (WAL); crash recovery replays committed transactions on startup
- **Interactive REPL** and single-command CLI mode

## Quick Start

```bash
# Build
cargo build

# Run interactive REPL
cargo run

# Execute a single SQL statement
cargo run -- "CREATE TABLE users (name TEXT, age INT64)"
cargo run -- "INSERT INTO users VALUES ('alice', 25)"
cargo run -- "SELECT * FROM users"

# Run all tests
cargo test
```

## CLI Options

```
Usage: young_bird_database [OPTIONS] [SQL]

Arguments:
  [SQL]               SQL statement to execute (non-interactive mode)

Options:
  --max-width <N>     Set column width limit for output
  --base-path <PATH>  Set data directory (default: data)
  --cache-size <N>    Set buffer pool size in pages (default: 64)
  --help              Show this help message
```

## Supported SQL

```sql
CREATE TABLE t (name TEXT, age INT64 NOT NULL, score FLOAT64)
DROP TABLE t
INSERT INTO t VALUES ('alice', 25, 98.5)
SELECT * FROM t WHERE age > 18 AND name = 'alice'
SELECT name, id FROM t WHERE id = 1
UPDATE t SET age = 26 WHERE name = 'alice'
DELETE FROM t WHERE age < 18
BEGIN
COMMIT
ROLLBACK
```

Supported types: `BOOLEAN`, `TEXT`, `INT8`, `INT16`, `INT32`, `INT64`, `UINT8`, `UINT16`, `UINT32`, `UINT64`, `FLOAT32`, `FLOAT64`

## Architecture

```
{base_path}/
├── {table}.meta         ← Schema (columns, types, page config)
├── {table}_0.dat        ← Pages 0..999
├── {table}_1.dat        ← Pages 1000..1999
├── {table}.idx          ← Hash index (record_id → page, slot)
├── {table}_0.overflow   ← Overflow text for large values
└── {table}_1.overflow   ← More overflow files as needed
```

Each page uses a **slotted page layout**:
```
┌──────────────────────────────┐
│ PageHeader (20 bytes)        │
├──────────────────────────────┤
│ Record 0 metadata (20 bytes) │  ↓ grows downward
│ Record 1 metadata (20 bytes) │
├──────────────────────────────┤
│         Free Space           │
├──────────────────────────────┤
│ Record 1 content (variable)  │  ↑ grows upward
│ Record 0 content (variable)  │
└──────────────────────────────┘
```

## Project Structure

```
src/
  lib.rs                              # Library root
  main.rs                             # REPL + CLI entry point
  database_operations/
    file_processing.rs                # Constants (KBYTES, HEADER_SIZE, etc.)
    file_processing/
      errors.rs      # DatabaseError enum
      types.rs       # ContentTypes, ColumnTypes enums + serialization
      traits.rs      # BinarySerde, ReadWrite traits
      buffer_pool/
        cached_page.rs   # CachedPage (Page + dirty flag)
        buffer_pool.rs   # BufferPool (LRU cache, HashMap + VecDeque)
      page/
        header.rs    # PageHeader (20 bytes)
        record.rs    # PageRecordMetadata, PageRecordContent
        page.rs      # Page struct (header + metadata + content + in-memory mutations)
        offsets.rs   # Offset calculation helpers
        reading.rs   # read_page, read_page_all, read_page_header, read_record_*
        writing.rs   # write_page, write_new_page, add/delete/update, compact_page
      table/
        column_def.rs    # ColumnDef struct
        table_header.rs  # TableHeader struct
        table.rs         # Table struct (create, open, CRUD, scan, compact)
        reading.rs       # read_table_header (.meta file)
        writing.rs       # write_table_header (.meta file)
      overflow/
        overflow_header.rs # OverflowHeader (16 bytes)
        overflow_ref.rs    # OverflowRef (16 bytes)
        reverse_index.rs   # OverflowReverseIndex (in-memory)
        reading.rs         # read_overflow_header, read_overflow_text
        writing.rs         # create/append/rewrite overflow files
      index/
        index_header.rs  # IndexHeader (24 bytes)
        index_entry.rs   # IndexEntry (20 bytes)
        hash_index.rs    # HashIndex (open addressing, linear probing)
        reading.rs       # read_index (.idx file)
        writing.rs       # write_index (.idx file)
      wal/
        wal_entry.rs     # WalOperation enum + WalEntry (variable size)
        wal_writer.rs    # WalWriter (append, fsync, truncate)
        wal_reader.rs    # read_all (sequential WAL file reader)
    sql/
      token.rs           # Token types and SQL keywords
      lexer.rs           # SQL string → token stream
      ast.rs             # AST types (Statement, Expr, Literal, etc.)
      parser.rs          # Recursive descent parser
      executor.rs        # AST → Table API calls → results
tests/
  page_operations.rs       # 17 page-level I/O tests
  table_operations.rs      # 87 table-level operation tests
  index_operations.rs      # 4 index file I/O tests
  overflow_operations.rs   # 13 overflow file I/O tests
  sql_lexer.rs             # 13 SQL lexer tests
  executor_operations.rs   # 37 SQL executor + transaction tests
  wal_operations.rs        # 7 WAL reader/writer tests
  cli_operations.rs        # 8 CLI integration tests
```

## Test Coverage

346 tests total (160 unit + 186 integration) covering serialization, page I/O, table CRUD, buffer pool caching, overflow text, index operations, SQL parsing, query execution, transactions, crash recovery, and CLI behavior.

```bash
cargo test
```

## Roadmap

See [ROADMAP.md](ROADMAP.md) for the full development plan. Phases 1–7 and 6.1–6.3 are complete. Next up: B-Tree index (Phase 8) or SQL SET/GET config commands (Phase 6.4).

## License

MIT
