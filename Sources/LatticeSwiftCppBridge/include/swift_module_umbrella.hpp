// Swift-module anchor for LatticeSwiftCppBridge — see module.modulemap.
//
// This is the ONLY non-textual header of the module: Swift's ClangImporter
// compiles it (and, through it, every bridge header — textually inlined) to
// build the `LatticeSwiftCppBridge` clang module that `import
// LatticeSwiftCppBridge` consumes from the Lattice Swift package.
//
// C++ code must NEVER #include this header. Every other bridge header is
// declared `textual` so plain C++ compiles (SwiftPM passes each dependency's
// module map via -fmodule-map-file even with clang modules disabled) never
// "need" a module nobody builds — the Linux `swift build` failure of
// docs/capi-gap-audit.md V1. A C++ TU including THIS header would recreate
// exactly that failure on Linux.

#ifndef lattice_swift_module_umbrella_hpp
#define lattice_swift_module_umbrella_hpp

// Same headers, same (alphabetical) order as the umbrella SwiftPM used to
// synthesize for this target. The order is LOAD-BEARING: several headers are
// not self-contained (e.g. error.hpp, geo_bounds.hpp) and rely on
// LatticeBridge.hpp → lattice.hpp having textually pulled in their
// dependencies first, exactly as the synthesized umbrella did.
#include <LatticeBridge.hpp>
#include <bridging.hpp>
#include <dynamic_object.hpp>
#include <error.hpp>
#include <geo_bounds.hpp>
#include <lattice.hpp>
#include <list.hpp>
#include <managed_object.hpp>
#include <unmanaged_object.hpp>
#include <util.hpp>

#endif /* lattice_swift_module_umbrella_hpp */
