# LatticeCore

C++ backend for [Lattice](https://github.com/jsflax/lattice), a Swift ORM built on SQLite with sync support.

LatticeCore provides the core database engine — query execution, schema management, vector search (via [sqlite-vec](https://github.com/asg017/sqlite-vec)), spatial indexing (R\*Tree), sync protocol (WSS + IPC), and the Swift-C++ bridge layer.

## Features

- **SQLite Engine** — CRUD, transactions, WAL mode, prepared statements
- **Schema Management** — Table creation, migrations, audit triggers
- **Sync Protocol** — Upload/download/reconcile with AuditLog-based change tracking
- **Filtered Sync** — Per-table upload filtering with SQL predicates
- **IPC Transport** — Cross-process sync via Unix domain sockets with length-prefix framing
- **Per-Synchronizer State** — Independent sync tracking per transport (`_lattice_sync_state`)
- **Cloud Relay** — IPC-received entries automatically forwarded to WSS
- **Vector Search** — ANN similarity search via sqlite-vec (L2, Cosine, L1)
- **Spatial Indexing** — R\*Tree bounding box and proximity queries
- **Full-Text Search** — FTS5 with porter tokenizer and external content tables

## Structure

```
Sources/
├── LatticeCore/             # Core C++ database engine
│   ├── include/lattice/
│   │   ├── db.hpp           # SQLite wrapper
│   │   ├── lattice.hpp      # ORM core (lattice_db, managed<T>, query<T>)
│   │   ├── schema.hpp       # Schema types
│   │   ├── sync.hpp         # Synchronizer, sync_transport interface
│   │   ├── network.hpp      # WebSocket client (generic_websocket_client)
│   │   └── ipc.hpp          # IPC server/client (Unix domain sockets)
│   └── src/
│       ├── db.cpp           # SQLite operations
│       ├── sync.cpp         # Sync protocol, per-synchronizer state
│       ├── ipc.cpp          # IPC transport, length-prefix framing
│       └── ...
├── LatticeSwiftCppBridge/   # Swift-C++ interop layer
├── LatticeSwiftModule/      # Swift module overlay
├── LatticeCAPI/             # C API wrapper
└── SqliteVec/               # sqlite-vec extension
```

## Requirements

- Swift 6.2+ / C++20
- macOS 14+ / iOS 17+ / Linux (Ubuntu 24.04+)
- SQLite3

## Usage

This package is consumed as a dependency by the [Lattice](https://github.com/jsflax/lattice) Swift ORM. Add it to your `Package.swift`:

```swift
.package(url: "https://github.com/jsflax/LatticeCore.git", from: "0.6.0")
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
