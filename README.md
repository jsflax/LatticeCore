# LatticeCore

C++ backend for [Lattice](https://github.com/jsflax/lattice), a Swift ORM built on SQLite with sync support.

LatticeCore provides the core database engine — query execution, schema management, vector search (via [sqlite-vec](https://github.com/asg017/sqlite-vec)), spatial indexing (R\*Tree), and the Swift-C++ bridge layer.

## Structure

```
Sources/
├── LatticeCpp/              # Core C++ database engine
├── LatticeSwiftCppBridge/   # Swift-C++ interop layer
├── LatticeSwiftModule/      # Swift module overlay
├── LatticeCAPI/             # C API wrapper
└── SqliteVec/               # sqlite-vec extension
```

## Requirements

- Swift 5.9+ / C++20
- macOS 14+ / iOS 17+
- SQLite3

## Usage

This package is consumed as a dependency by the [Lattice](https://github.com/jsflax/lattice) Swift ORM. Add it to your `Package.swift`:

```swift
.package(url: "https://github.com/jsflax/LatticeCore.git", branch: "main")
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
