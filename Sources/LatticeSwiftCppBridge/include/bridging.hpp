// Compatibility header for building without Swift interop.
//
// Uses __has_include to check if the real Swift bridging header is available.
// If available (building with Swift toolchain), include it.
// Otherwise, provide stub definitions.

#ifndef SWIFT_BRIDGING_COMPAT_H
#define SWIFT_BRIDGING_COMPAT_H

#ifdef __cplusplus

// LATTICE_HAS_FRT: whether SWIFT_SHARED_REFERENCE foreign-reference types are
// available. They require iOS 16.4 / macOS 13.3 / tvOS 16.4 / watchOS 9.4. Below
// that floor (iOS 15) the *_ref handle types are compiled as plain Swift value
// types whose inner std::shared_ptr provides the same shared ownership — so the
// existing C++-interop Swift code can drive them without a separate C backend.
//
// Overridable (#ifndef): targets that never cross Swift interop and depend on
// the heap-ref model (e.g. LatticeCAPI) force the FRT layout with
// -DLATTICE_HAS_FRT=1.
//
// Order matters: tvOS/watchOS define __IPHONE_OS_VERSION_MIN_REQUIRED too (as
// their base 9.0 alias), so their checks must come before the iOS check or
// they'd always take the iOS branch.
#ifndef LATTICE_HAS_FRT
#if defined(__APPLE__)
#  include <Availability.h>
#endif
#if defined(__TV_OS_VERSION_MIN_REQUIRED) && __TV_OS_VERSION_MIN_REQUIRED < 160400
#  define LATTICE_HAS_FRT 0
#elif defined(__WATCH_OS_VERSION_MIN_REQUIRED) && __WATCH_OS_VERSION_MIN_REQUIRED < 90400
#  define LATTICE_HAS_FRT 0
#elif defined(__IPHONE_OS_VERSION_MIN_REQUIRED) && __IPHONE_OS_VERSION_MIN_REQUIRED < 160400
#  define LATTICE_HAS_FRT 0
#elif defined(__MAC_OS_X_VERSION_MIN_REQUIRED) && __MAC_OS_X_VERSION_MIN_REQUIRED < 130300
#  define LATTICE_HAS_FRT 0
#else
#  define LATTICE_HAS_FRT 1
#endif
#endif  // !defined(LATTICE_HAS_FRT)

#if __has_include(<swift/bridging.hpp>)
    // Real Swift bridging available (macOS Swift Package Manager build)
    #include <swift/bridging.hpp>
#elif __has_include(<swift/bridging>)
    // Real Swift bridging available (Linux Swift toolchain)
    #include <swift/bridging>
#else
    // Stub definitions for non-Swift builds (CMake, etc.)
    #ifndef SWIFT_SHARED_REFERENCE
        #define SWIFT_SHARED_REFERENCE(retain, release)
    #endif
    #ifndef SWIFT_UNSAFE_REFERENCE
        #define SWIFT_UNSAFE_REFERENCE
    #endif
    #ifndef SWIFT_CONFORMS_TO_PROTOCOL
        #define SWIFT_CONFORMS_TO_PROTOCOL(x)
    #endif
    #ifndef SWIFT_NAME
        #define SWIFT_NAME(x)
    #endif
    #ifndef SWIFT_COMPUTED_PROPERTY
        #define SWIFT_COMPUTED_PROPERTY
    #endif
    #ifndef SWIFT_RETURNS_INDEPENDENT_VALUE
        #define SWIFT_RETURNS_INDEPENDENT_VALUE
    #endif
    #ifndef SWIFT_RETURNS_RETAINED
        #define SWIFT_RETURNS_RETAINED
    #endif
    #ifndef SWIFT_RETURNS_UNRETAINED
        #define SWIFT_RETURNS_UNRETAINED
    #endif
#endif

#endif // __cplusplus

#endif // SWIFT_BRIDGING_COMPAT_H
