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

## Planned

### Phase 6: Advanced Features (continued)
- Transaction support (write-ahead log)
- SQL SET/GET commands for runtime config (e.g. cache_size)

### Phase 8: B-Tree Index
- B-tree node struct (leaf + internal nodes, BinarySerde)
- B-tree search (single key lookup)
- B-tree insertion (with node splitting)
- B-tree deletion (with node merging/rebalancing)
- Range scan (return all entries where key is in [low, high])
- B-tree file I/O (persist to .btree file)
- Secondary index support (index on any column, not just record_id)
- Query planner integration (WHERE clauses use B-tree when available)

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
  └─→ Advanced Features (6.3+) — transactions (WAL), concurrency control
```
