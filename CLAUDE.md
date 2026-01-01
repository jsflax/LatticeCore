# LatticeCpp - Core C++ ORM Implementation

## Overview
LatticeCpp is the C++ core of the Lattice ORM system. It provides the database layer that Swift (and eventually Kotlin) frontends delegate to. Built on SQLite with support for sync, change tracking, and observation.

---

## ⚠️ CRITICAL: Coding Principle

**All heavy lifting happens here, not in Swift.**

Priority order:
1. **SQL** - Do it in a query (WHERE, ORDER BY, LIMIT, JOINs, aggregates)
2. **C++ functions** - If SQL alone can't do it, add C++ code
3. **Never in Swift** - Swift is thin wrapper only

Example: `combinedNearestQuery()` does vector search + geo filtering + bounds in one SQL query, returning only K results. Swift just hydrates the results.

---

## Project Structure

```
LatticeCpp/
├── Sources/
│   ├── LatticeCpp/                    # Core C++ implementation
│   │   ├── include/
│   │   │   ├── LatticeCpp.hpp         # Main header (includes all)
│   │   │   └── lattice/
│   │   │       ├── db.hpp             # Database wrapper
│   │   │       ├── lattice.hpp        # lattice_db, managed<T>, query<T>
│   │   │       ├── schema.hpp         # Schema types, registry
│   │   │       ├── sync.hpp           # Synchronizer
│   │   │       └── network.hpp        # HTTP/WebSocket client
│   │   └── src/
│   │       ├── db.cpp                 # SQLite operations
│   │       ├── schema.cpp             # Schema registry
│   │       ├── sync.cpp               # Sync implementation
│   │       └── network.cpp            # Network implementation
│   └── LatticeSwiftCppBridge/         # Swift interop layer
│       ├── include/
│       │   └── lattice.hpp            # Swift-specific types
│       └── src/
│           └── lattice.cpp            # Retain/release functions
└── Package.swift
```

## Key Architecture

### 1. **Core Classes**

#### `database` (db.hpp)
- Low-level SQLite wrapper
- Prepared statements, transactions
- `execute()`, `query()`, `insert()`, `update()`, `remove()`
- Table creation and migration

#### `lattice_db` (lattice.hpp)
- Main ORM class for C++ usage
- Template methods: `add<T>()`, `find<T>()`, `remove<T>()`
- Query building: `objects<T>()` returns `query<T>`
- Transaction support: `begin_transaction()`, `commit()`
- Observer system for change notifications

#### `managed<T>` (lattice.hpp)
- Smart wrapper for database-backed objects
- Tracks dirty state for property changes
- `id()`, `global_id()` for identification
- `is_valid()` to check if still in database

#### `query<T>` / `results<T>` (lattice.hpp)
- Type-safe query building
- `.where()`, `.order_by()`, `.limit()`, `.offset()`
- Lazy execution via `execute()`

### 2. **Swift Bridge Types** (LatticeSwiftCppBridge)

#### `swift_dynamic_object`
- Type-erased model for Swift interop
- `table_name`, `properties` (schema), `values` (data)
- Convenience getters/setters: `get_string()`, `set_int()`, etc.

#### `swift_lattice`
- Inherits from `lattice_db`
- Swift-specific methods using `swift_dynamic_object`
- `add()`, `objects()`, `count()`, `delete_where()`
- Observer API with C function pointers

#### `swift_lattice_ref`
- Wrapper with `SWIFT_SHARED_REFERENCE` for Swift memory management
- Factory: `create(config, schemas)`
- Caches instances by config path using `weak_ptr`
- `ref_count_` starts at 0 (Swift calls retain on creation)

### 3. **Schema System**

#### `property_descriptor`
```cpp
struct property_descriptor {
    std::string name;
    column_type type;      // integer, real, text, blob
    property_kind kind;    // primitive, link, link_list
    std::string target_table;  // For links
    std::string link_table;    // For link lists
    bool nullable;
};
```

#### `swift_schema_entry`
```cpp
struct swift_schema_entry {
    std::string table_name;
    SwiftSchema properties;  // unordered_map<string, property_descriptor>
};
```

#### `SchemaVector`
- `std::vector<swift_schema_entry>`
- Passed from Swift at init time
- Used to create tables before any queries

### 4. **Table Creation Flow**

1. Swift `Lattice.init` builds `SchemaVector` from model types
2. Calls `swift_lattice_ref::create(config, schemas)`
3. `get_or_create_shared()` creates `swift_lattice(config, schemas)`
4. `swift_lattice` constructor calls `ensure_swift_tables(schemas)`
5. For each schema, calls `create_model_table_public()` (protected in `lattice_db`)
6. `lattice_db::create_model_table()` creates table + audit triggers

### 5. **Memory Management**

```cpp
// swift_lattice_ref with SWIFT_SHARED_REFERENCE
class swift_lattice_ref {
    std::shared_ptr<swift_lattice> impl_;
    std::atomic<int> ref_count_{0};  // Starts at 0!

    void retain() { ref_count_++; }
    bool release() { return --ref_count_ == 0; }
};

// Global retain/release for Swift
void retainSwiftLatticeRef(swift_lattice_ref* ptr);
void releaseSwiftLatticeRef(swift_lattice_ref* ptr);
```

## Important Implementation Details

### Reserved Columns
- `id` and `globalId` are auto-added by `create_model_table()`
- Swift filters these out in `cxxPropertyDescriptor()` to avoid duplicates
- `globalId` uses SQL-generated UUID default

### Audit Triggers
Created for every model table:
- `AuditLog_Update_<table>` - Tracks field changes
- `Audit<table>Insert` - Tracks inserts
- `Audit<table>Delete` - Tracks deletes

All check `sync_disabled()` function to allow bulk operations without audit.

### Observer System
```cpp
// Table-level observers (for Results)
uint64_t add_table_observer(table_name, callback);
void remove_table_observer(table_name, observer_id);

// Object-level observers (for individual objects)
uint64_t add_object_observer(table_name, row_id, callback);
```

## Building

```bash
swift build                    # Build everything
swift test                     # Run tests (in Lattice project)
```

## Key Files to Understand

1. **lattice.hpp:lattice_db** - Core ORM implementation
2. **lattice.hpp:swift_lattice** - Swift bridge layer
3. **lattice.hpp:swift_lattice_ref** - Memory-safe wrapper for Swift
4. **db.hpp/db.cpp** - SQLite operations
5. **LatticeSwiftCppBridge/include/lattice.hpp** - All Swift interop types

## Current State

### Working
- ✅ `swift_lattice_ref` with `SWIFT_SHARED_REFERENCE`
- ✅ Schema passing from Swift to C++
- ✅ Table creation at init time
- ✅ Basic CRUD: `add()`, `add_bulk()`, `objects()`, `count()`, `delete_where()`, `remove()`
- ✅ Connection caching by config path
- ✅ Migration support with row callbacks
- ✅ WHERE clause support in queries
- ✅ ORDER BY support with SortDescriptor
- ✅ LIMIT/OFFSET pagination
- ✅ UNION queries for polymorphic types (`union_objects()`)
- ✅ Link tables for List<Model> relationships
- ✅ Foreign key constraints for Optional<Model> links
- ✅ Vector search via sqlite-vec (`combinedNearestQuery()`)
- ✅ Spatial queries via R*Tree (`objectsWithinBBox()`)
- ✅ Combined proximity queries (vector + geo + bounds)
- ✅ Table observers with C function callbacks
- ✅ Database attachment (`attach()`)
- ✅ Transaction support (`begin_transaction()`, `commit()`)
- ✅ WebSocket client integration via `generic_websocket_client`
- ✅ Scheduler injection for observer dispatch

### TODO
- ⏳ AuditLog compaction
- ⏳ Full sync protocol implementation
