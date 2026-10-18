#ifndef SQLITE_VEC_LATTICE_REGISTRATION_H
#define SQLITE_VEC_LATTICE_REGISTRATION_H
#include "sqlite-vec.h"
#include <stdint.h>
#ifdef __cplusplus
extern "C" {
#endif
/* Closed Core initializer, not a SQL/module query or a Native ownership grant.
 * Each supplied cookie is consumed by the ACTUAL v2 registration call, whose
 * real destructor owns ending on success, failure, overload, drop or close.
 * Module ending may be deferred by live vtabs: cookie liveness ALONE never
 * proves the current name binding. Core must also retain its closed mutator
 * frontier and refuse raw Native escape/move/retire. No builtin-module claim. */
#define SQLITE_VEC_LATTICE_REGISTRATION_COUNT 20
struct sqlite_vec_lattice_cookie {
  uint64_t creator; /* fixed DATA checked before inherited payload */
  void *payload;
  void (*retire)(void *);
  const char *text; /* only the two actual static text functions use userdata */
};
struct sqlite_vec_lattice_issuer {
  void *payload;
  struct sqlite_vec_lattice_cookie *(*take)(void *, unsigned int);
  int (*returned)(void *, unsigned int, int); /* actual rc, then DATA currentness */
};
SQLITE_VEC_API int sqlite3_vec_init_with_lattice_receipts(sqlite3 *,char **,
    const sqlite3_api_routines *,const struct sqlite_vec_lattice_issuer *);
#ifdef __cplusplus
}
#endif
#endif
