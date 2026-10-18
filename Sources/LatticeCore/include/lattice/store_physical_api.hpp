#pragma once
#include <sqlite3.h>

namespace lattice::detail {
// Source-internal return-scope adapters. They preserve actual SQLite results;
// an argument is never turned into SQL/ownership authority. No public SDK API.
int store_physical_finalize(sqlite3_stmt*)noexcept;
int store_physical_step(sqlite3_stmt*)noexcept;
int store_physical_reset(sqlite3_stmt*)noexcept;
int store_physical_exec(sqlite3*,const char*,int(*)(void*,int,char**,char**),void*,char**)noexcept;
int store_physical_wal_checkpoint(sqlite3*,const char*,int,int*,int*)noexcept;
int store_physical_blob_close(sqlite3_blob*,sqlite3* actual_owner)noexcept;
int store_physical_close(sqlite3*)noexcept;
int store_physical_close_v2(sqlite3*)noexcept;
// Actual successful/unpublished unresourced open producer only. Supersedes
// stale address associations, never creates a physical resource permission.
void store_physical_unresourced_open_returned(sqlite3*)noexcept;
}
