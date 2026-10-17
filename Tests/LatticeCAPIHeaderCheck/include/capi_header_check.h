#ifndef LATTICE_CAPI_HEADER_CHECK_H
#define LATTICE_CAPI_HEADER_CHECK_H

#ifdef __cplusplus
extern "C" {
#endif

// Probe exported by capi_header_compiles.c (compiled as pure C11).
// Returns 1 when the representative surface checks in that TU hold.
// Calling it from the gtest suite proves the C TU built and linked.
int lattice_capi_header_compiles_as_c11(void);

#ifdef __cplusplus
}
#endif

#endif  // LATTICE_CAPI_HEADER_CHECK_H
