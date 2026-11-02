#!/usr/bin/env bash
set -euo pipefail

# Compile-only regression for Xcode's explicit Clang modules: Swift targets
# iOS 15, but its PCM can target the current SDK. Both must choose the same
# handle ABI as native C++ objects. The tiny wrapper uses the REAL compatibility
# header; older deployment-based selection passes implicit import and fails
# explicit import with the iOS 16.4 foreign-reference availability diagnostic.
# Usage: check-ios-layout.sh /absolute/path/to/isolated-output
test "$#" -eq 1
check_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
check_output="$1"
mkdir -p "$check_output/module" "$check_output/tmp"
check_output="$(cd "$check_output" && pwd)"
export TMPDIR="$check_output/tmp"
check_sdk="$(xcrun --sdk iphoneos --show-sdk-path)"
check_sdk_version="$(xcrun --sdk iphoneos --show-sdk-version)"
check_header_dir="$check_root/Sources/LatticeSwiftCppBridge/include"

cat > "$check_output/module/module.modulemap" <<'EOF'
module LatticePlatformLayoutProbe { header "probe.hpp" export * }
EOF
cat > "$check_output/module/probe.hpp" <<'EOF'
#include <bridging.hpp>
#if LATTICE_HAS_FRT
struct ProbeRef;
void retainProbe(ProbeRef*);
void releaseProbe(ProbeRef*);
struct ProbeRef { int value; int ref_count; }
    SWIFT_SHARED_REFERENCE(retainProbe, releaseProbe);
#else
struct ProbeRef { int value; };
#endif
EOF
cat > "$check_output/probe.swift" <<'EOF'
import LatticePlatformLayoutProbe
public func acceptsHandle(_ value: ProbeRef) {}
EOF
cat > "$check_output/native.cpp" <<'EOF'
#include "module/probe.hpp"
#ifndef EXPECTED_FRT
#define EXPECTED_FRT 0
#endif
static_assert(LATTICE_HAS_FRT == EXPECTED_FRT, "Unexpected handle ABI");
static_assert(sizeof(ProbeRef) == (EXPECTED_FRT ? 2 : 1) * sizeof(int),
              "Unexpected native handle layout");
EOF

for check_version in 15.0 16.4 "$check_sdk_version"; do
  xcrun clang++ -fsyntax-only -std=c++20 \
    -target "arm64-apple-ios$check_version" -isysroot "$check_sdk" \
    -I "$check_header_dir" "$check_output/native.cpp"
  for check_mode in implicit explicit; do
    check_flags=()
    if [[ "$check_mode" == explicit ]]; then
      check_flags=(-explicit-module-build)
    fi
    xcrun swiftc -typecheck -cxx-interoperability-mode=default \
      -target "arm64-apple-ios$check_version" -sdk "$check_sdk" \
      -I "$check_output/module" -Xcc "-I$check_header_dir" \
      -module-cache-path "$check_output/$check_mode-cache" \
      ${check_flags[@]+"${check_flags[@]}"} "$check_output/probe.swift"
  done
done

# A consistent custom native build can still explicitly choose FRT.
xcrun clang++ -fsyntax-only -std=c++20 -target arm64-apple-ios15.0 \
  -isysroot "$check_sdk" -I "$check_header_dir" \
  -DLATTICE_HAS_FRT=1 -DEXPECTED_FRT=1 "$check_output/native.cpp"

# The supported macOS floor remains on FRT.
xcrun clang++ -fsyntax-only -std=c++20 -target arm64-apple-macos14.0 \
  -isysroot "$(xcrun --sdk macosx --show-sdk-path)" -I "$check_header_dir" \
  -DEXPECTED_FRT=1 "$check_output/native.cpp"
echo "PASS: iOS native/implicit/explicit layout matrix; explicit override and macOS FRT"
