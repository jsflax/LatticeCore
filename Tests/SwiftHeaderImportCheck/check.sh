#!/usr/bin/env bash
set -euo pipefail

# A consuming Swift target uses its own Clang language mode, independently of
# Core's C++20 target settings. Compile the real public module map with Swift's
# PCM frontend in C++17, the same stage that a consuming Swift build invokes.
# No link, wrapper protocol declarations, database or network operation is needed.
test "$#" -eq 1
check_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
check_cache="$1"
mkdir -p "$check_cache"
swiftc -frontend -emit-pcm -module-name LatticeSwiftCppBridge \
  -cxx-interoperability-mode=default -Xcc -std=c++17 \
  -Xcc -DSQLITE_CORE -module-cache-path "$check_cache" \
  -I "$check_root/Sources/LatticeSwiftCppBridge/include" \
  -I "$check_root/Sources/LatticeCore/include" \
  -I "$check_root/Sources/SqliteVec/include" \
  -o "$check_cache/LatticeSwiftCppBridge.pcm" \
  "$check_root/Sources/LatticeSwiftCppBridge/include/module.modulemap"

swiftc -emit-module -parse-as-library -module-name LatticeSwiftModule \
  -module-cache-path "$check_cache" \
  -emit-module-path "$check_cache/LatticeSwiftModule.swiftmodule" \
  "$check_root/Sources/LatticeSwiftModule/LatticeSwiftModule.swift"

swiftc -typecheck -module-name Lattice -cxx-interoperability-mode=default -Xcc -std=c++17 \
  -Xcc -DSQLITE_CORE -module-cache-path "$check_cache" \
  -I "$check_cache" \
  -I "$check_root/Sources/LatticeSwiftCppBridge/include" \
  -I "$check_root/Sources/LatticeCore/include" \
  -I "$check_root/Sources/SqliteVec/include" \
  "$check_root/Tests/SwiftHeaderImportCheck/BackgroundSync.swift"
