// Compatibility header for building without Swift interop.
//
// Uses __has_include to check if the real Swift bridging header is available.
// If available (building with Swift toolchain), include it.
// Otherwise, provide stub definitions.

#ifndef SWIFT_BRIDGING_COMPAT_H
#define SWIFT_BRIDGING_COMPAT_H

#ifdef __cplusplus

#if __has_include(<swift/bridging.hpp>)
    // Real Swift bridging available (Swift Package Manager build)
    #include <swift/bridging.hpp>
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
#endif

#endif // __cplusplus

#endif // SWIFT_BRIDGING_COMPAT_H
