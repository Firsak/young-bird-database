# Development Roadmap

## Completed

### Phase 1: Page Layer
- Binary serialization for all types (ContentTypes, ColumnTypes)
- Page structures (PageHeader, PageRecordMetadata, PageRecordContent)
- BinarySerde and ReadWrite traits
- Page file I/O: create page, read page/header/metadata/content
- Record operations: add, delete (hard + soft), update (in-place + relocate)
- Buffer-based read_page and write_page (single I/O call per page)
- Intra-page compaction (defragment within a single page)

### Phase 2: Table Layer
- Table schema structures (ColumnDef, TableHeader) with .meta file I/O
- Table struct with multi-file page addressing (resolve_file)
- Table::create + Table::open
- Table-level CRUD: insert (tries last page, creates new if full), read, delete, update (in-place, relocate, or cross-page)
- Auto-increment record IDs

### Phase 3: Validation & Errors
- Custom DatabaseError enum (Io, PageFull, RecordTooLarge, RecordNotFound, InvalidArgument, SchemaViolation, Serialization)
- Input validation on create/open (table name, page_kbytes, columns, column names, pages_per_file)
- Schema validation on insert/update (column count, types, nullability)

### Phase 4: Hash Index
- Open addressing with linear probing, stored in .idx file
- IndexHeader (24 bytes) + IndexEntry (20 bytes) with BinarySerde
- Core operations: insert, lookup, remove, update, rehash
- Table integration: all CRUD uses index for O(1) record lookup by ID

### Phase 5: Cross-Page Operations
- Inter-page compaction: streaming two-page algorithm with O(page_size) memory
- Fragmentation ratio stats helper
- Record migration on update (delete+insert with index update)

### Phase 7: SQL Pipeline
- Lexer: SQL string → token stream (keywords, identifiers, literals, operators)
- Parser: recursive descent, all 6 statement types (CREATE/DROP/INSERT/SELECT/DELETE/UPDATE)
- AST: Statement, Expr, Literal, ColumnSpec, Assignment types
- WHERE clause support with AND/OR/NOT and operator precedence
- Executor: AST → Table API calls, type conversion, two-phase delete, pre-computed column indices
- Interactive REPL and single-command CLI mode

### Phase 6.1: Overflow Text
- Overflow file storage for large text values (OverflowHeader, OverflowRef, `.overflow` files)
- Fragmentation tracking on delete/update
- Column-by-column update comparison (reuse refs for unchanged text)
- In-memory reverse index for overflow entries (rebuilt on Table::open)
- Overflow file compaction (rewrite with only live entries, patch records)

### Phase 6.2: Page Caching (Buffer Pool)
- BufferPool struct with LRU eviction (HashMap + VecDeque)
- CachedPage with write-back dirty tracking
- In-memory Page mutations (add_record, delete_record, update_record — no disk I/O)
- read_page_all (includes deleted records for cache slot-index consistency)
- Table CRUD and scan methods rewired through cache
- Compaction: flush-before + cache-invalidate-after approach
- CLI `--cache-size` flag with configurable buffer pool size

### Phase 6.3: Transaction Support (Write-Ahead Log)
- WAL module: WalEntry with binary layout (entry_size prefix + txn_id + op + rid + table_name + data), WalWriter (append/fsync/truncate), WalReader (read_all with read_at)
- `BEGIN` / `COMMIT` / `ROLLBACK` SQL statements
- Executor WAL integration: log before mutate; dirty pages only flush on COMMIT
- Auto-transaction for single statements without explicit BEGIN
- Crash recovery: replay committed transactions idempotently on startup, discard incomplete ones

### Phase 6.4: Persistent Runtime Config
- DatabaseConfig struct with text-file persistence (`database.conf`)
- `GET <key>`, `GET ALL`, `SET <key> = <value>` SQL statements through full pipeline (token / AST / parser / executor)
- CLI override support; executor reads from centralized config

### Phase 8: B-Tree Index
- BTreeNode (leaf + internal) with BinarySerde; `key_count` derived from `keys.len()` (no dual-bookkeeping field)
- BTree struct: search, insert with leaf and internal node splitting, delete with borrow / merge / rebalance, range_scan with stack-based leaf traversal
- B-tree file I/O: BTreeHeader (root, node_count, free_list) in a fixed 8192-byte reserved header block; nodes at predictable offsets `BTREE_HEADER_BLOCK_SIZE + i * BTREE_NODE_SIZE`; free slots written as zero bytes and read as placeholder leaves
- Table integration: `.btree` index on `record_id` persisted alongside `.idx` on every insert / delete; `compact_table` rebuilds the B-tree from active records
- Query planner v0: `classify_id_range(expr)` recognizes id-range WHERE clauses (`id =`, `<`, `<=`, `>`, `>=`, AND of two id-bounds); executor dispatches `Table::scan_records_by_id_range` when applicable, falls back to `scan_records` + `evaluate_expr` for OR / NOT / non-id columns

## Planned

### Phase 8.7: Secondary B-Tree Indexes
- B-tree on arbitrary columns (not just `record_id`)
- Multiple `.btree` files per table, one per indexed column
- Column-aware key extraction at insert / delete / update time

### Phase 8.8 v1+: Query Planner Extensions
- Use Phase 8.7 indexes for non-id WHERE columns
- DNF expansion for OR (each disjunct → separate index lookup, union results)
- Cost model (when to skip an index even if available)

### Phase 9: Concurrency Control
- Page-level locking (read / write locks on cached pages)
- MVCC or lock-based concurrency (design decision TBD)
- Concurrent access tests
- Per-object WAL records (replace table-op replay with finer-grained log entries to support concurrent transactions)

## Dependency Chain

```
Page Layer (1)
  └─→ Table Layer (2) — wraps page functions with multi-file addressing
        └─→ Validation (3) — schema/input checks on Table API
        └─→ Hash Index (4) — O(1) lookups, integrated into Table CRUD
              └─→ Cross-Page Ops (5) — compaction + migration need index
        └─→ SQL Pipeline (7) — translates SQL into Table API calls
              └─→ B-Tree Index (8) — query planner uses B-tree for range queries
  └─→ Overflow Text (6.1) — large text in separate files with compaction
  └─→ Page Caching (6.2) — LRU buffer pool with write-back dirty tracking
  └─→ Transactions / WAL (6.3) — log before mutate, COMMIT flushes, ROLLBACK discards
  └─→ Persistent Config (6.4) — database.conf + SQL GET/SET
  └─→ B-Tree Index (8) — depends on Page (1), Table (2), Hash Index (4); persisted on every mutation
        └─→ Query Planner v0 (8.8) — depends on B-Tree + SQL (7); id-range fast-path
        └─→ Secondary Indexes (8.7) — depends on Table API for column access
  └─→ Concurrency (9) — depends on Buffer Pool (6.2), WAL (6.3), and B-Tree (8)
```
