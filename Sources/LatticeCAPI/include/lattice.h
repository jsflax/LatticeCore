#ifndef LATTICE_C_API_H
#define LATTICE_C_API_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// =============================================================================
// Export annotation (docs/CAPI-STABILITY.md rule 8)
// =============================================================================
// The shared library builds with default-hidden visibility; every public
// function carries LATTICE_EXPORT so the export list is exactly the declared
// surface (enforced by the nm diff against lattice_capi.symbols in CI).

#if defined(_WIN32)
  #define LATTICE_EXPORT __declspec(dllexport)
#elif defined(__GNUC__) || defined(__clang__)
  #define LATTICE_EXPORT __attribute__((visibility("default")))
#else
  #define LATTICE_EXPORT
#endif

// =============================================================================
// Opaque Types
// =============================================================================

typedef struct lattice_db lattice_db_t;
typedef struct lattice_object lattice_object_t;
typedef struct lattice_results lattice_results_t;
typedef struct lattice_link_list lattice_link_list_t;

// =============================================================================
// Versioning & Introspection
// =============================================================================

// The C ABI version tracks the LatticeCore release train (see CHANGELOG.md).
// The surface becomes additive-only at 1.0 (see docs/CAPI-STABILITY.md).
#define LATTICE_CAPI_VERSION_MAJOR 1
#define LATTICE_CAPI_VERSION_MINOR 0
#define LATTICE_CAPI_VERSION_PATCH 0

// Single-integer encoding: MAJOR*1000000 + MINOR*1000 + PATCH (sqlite-style).
#define LATTICE_CAPI_VERSION \
    (LATTICE_CAPI_VERSION_MAJOR * 1000000 + \
     LATTICE_CAPI_VERSION_MINOR * 1000 + \
     LATTICE_CAPI_VERSION_PATCH)

#define LATTICE_CAPI_VERSION_STRING "1.0.0"

// Runtime version of the linked library (compare against LATTICE_CAPI_VERSION
// to detect header/library skew).
LATTICE_EXPORT uint32_t lattice_capi_version(void);

// Runtime version string of the linked library ("MAJOR.MINOR.PATCH", static
// storage - do NOT free).
LATTICE_EXPORT const char* lattice_capi_version_string(void);

// The on-disk schema-format epoch this library writes/expects
// (lattice_db::kLatticeSchemaFormatEpoch). Bumped when internal DDL templates
// change; consumers sharing database files must agree on the epoch.
LATTICE_EXPORT int32_t lattice_schema_format_epoch(void);

// Query whether a named feature is available in this build of the C ABI.
// Returns false for unknown/NULL names, so callers can probe features that
// do not exist yet.
//
// Feature strings that return true today:
//   "attach", "checkpoint", "cross_process_observation", "detach", "fts",
//   "geo_query", "ipc", "knn", "migration", "observation", "rollback",
//   "row_cache", "statement_counters", "sync", "sync_filter",
//   "sync_progress", "sync_tuning", "to_json", "transactions"
// Reserved strings that will return true when the feature lands (planned):
//   "read_generations", "unified_open" (deferred to 1.1 —
//   docs/design-unified-open.md)
LATTICE_EXPORT bool lattice_capi_has_feature(const char* feature);

// =============================================================================
// Logging
// =============================================================================

/// Set global log level: 0=off, 1=error, 2=warn, 3=info, 4=debug
LATTICE_EXPORT void lattice_set_log_level(int level);

// =============================================================================
// Error Handling
// =============================================================================

typedef enum {
    LATTICE_OK = 0,
    LATTICE_ERROR_NULL_POINTER = -1,
    LATTICE_ERROR_INVALID_ARGUMENT = -2,
    LATTICE_ERROR_NOT_FOUND = -3,
    LATTICE_ERROR_DATABASE = -4,
} lattice_status_t;

// Get the last error message (thread-local)
LATTICE_EXPORT const char* lattice_last_error(void);

// =============================================================================
// Schema Types (must be defined before database creation functions)
// =============================================================================

typedef enum {
    LATTICE_TYPE_INTEGER = 0,
    LATTICE_TYPE_REAL = 1,
    LATTICE_TYPE_TEXT = 2,
    LATTICE_TYPE_BLOB = 3,
} lattice_column_type_t;

typedef enum {
    LATTICE_KIND_PRIMITIVE = 0,
    LATTICE_KIND_LINK = 1,
    LATTICE_KIND_LINK_LIST = 2,
    LATTICE_KIND_VIRTUAL_LIST = 3,
    LATTICE_KIND_VIRTUAL_LINK = 4,
} lattice_property_kind_t;

typedef struct {
    const char* name;
    lattice_column_type_t type;
    lattice_property_kind_t kind;
    const char* target_table;   // For links (NULL if primitive)
    const char* link_table;     // For link lists (NULL otherwise)
    bool nullable;
    // Index and constraint flags
    bool is_indexed;            // Create non-unique index on this column
    bool is_unique;             // Create unique constraint on this column
    bool is_full_text;          // Create FTS5 virtual table for this column
    bool is_vector;             // Create vec0 virtual table for this column
    bool is_geo_bounds;         // Create R*Tree virtual table for this column
    const char* column_name;    // Custom column name mapping (NULL = use field name)
} lattice_property_t;

// Compound unique constraint (across multiple columns)
typedef struct {
    const char* table_name;
    const char** property_names;
    size_t property_count;
    bool allows_upsert;
} lattice_unique_constraint_t;

typedef struct {
    const char* table_name;
    const lattice_property_t* properties;
    size_t property_count;
} lattice_schema_t;

// =============================================================================
// Database Creation
// =============================================================================

// Create an in-memory database (no schema - use for simple key-value style)
LATTICE_EXPORT lattice_db_t* lattice_db_create_in_memory(void);

// Create/open a database at the given path (no schema)
LATTICE_EXPORT lattice_db_t* lattice_db_create_at_path(const char* path);

// Create a database with schemas (tables are created/migrated automatically)
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_schemas(
    const char* path,           // NULL or ":memory:" for in-memory
    const lattice_schema_t* schemas,
    size_t schema_count
);

// Retain a reference to the database
LATTICE_EXPORT void lattice_db_retain(lattice_db_t* db);

// Release a reference to the database (frees when ref_count hits 0)
LATTICE_EXPORT void lattice_db_release(lattice_db_t* db);

// Explicitly close database connections and stop background services.
// Call before release to ensure clean shutdown. Safe to call multiple times.
LATTICE_EXPORT void lattice_db_close(lattice_db_t* db);

// Check if the sync WebSocket connection is established.
LATTICE_EXPORT bool lattice_db_is_sync_connected(lattice_db_t* db);

// =============================================================================
// Object Lifecycle
// =============================================================================

// Create a new object for a table (without schema - not recommended)
LATTICE_EXPORT lattice_object_t* lattice_object_create(const char* table_name);

// Create a new object with explicit schema (like Swift's _defaultCxxLatticeObject)
// This is the preferred way to create standalone objects without a db reference
LATTICE_EXPORT lattice_object_t* lattice_object_create_with_schema(
    const char* table_name,
    const lattice_property_t* properties,
    size_t property_count
);

// Create a new object with schema from the database's registered schemas
LATTICE_EXPORT lattice_object_t* lattice_db_create_object(lattice_db_t* db, const char* table_name);

// Retain a reference
LATTICE_EXPORT void lattice_object_retain(lattice_object_t* obj);

// Release a reference
LATTICE_EXPORT void lattice_object_release(lattice_object_t* obj);

// =============================================================================
// Object Field Access
// =============================================================================

// Getters (return default value if field not set or wrong type)
LATTICE_EXPORT int64_t lattice_object_get_int(lattice_object_t* obj, const char* field);
LATTICE_EXPORT double lattice_object_get_double(lattice_object_t* obj, const char* field);
LATTICE_EXPORT const char* lattice_object_get_string(lattice_object_t* obj, const char* field);
LATTICE_EXPORT size_t lattice_object_get_blob(lattice_object_t* obj, const char* field,
                                uint8_t* buffer, size_t buffer_size);

// Check if field has a value
LATTICE_EXPORT bool lattice_object_has_value(lattice_object_t* obj, const char* field);

// Setters
LATTICE_EXPORT void lattice_object_set_int(lattice_object_t* obj, const char* field, int64_t value);
LATTICE_EXPORT void lattice_object_set_double(lattice_object_t* obj, const char* field, double value);
LATTICE_EXPORT void lattice_object_set_string(lattice_object_t* obj, const char* field, const char* value);
LATTICE_EXPORT void lattice_object_set_blob(lattice_object_t* obj, const char* field,
                             const uint8_t* data, size_t size);
LATTICE_EXPORT void lattice_object_set_null(lattice_object_t* obj, const char* field);

// Object identity (for managed objects)
LATTICE_EXPORT int64_t lattice_object_get_id(lattice_object_t* obj);
LATTICE_EXPORT const char* lattice_object_get_global_id(lattice_object_t* obj);
LATTICE_EXPORT const char* lattice_object_get_table_name(lattice_object_t* obj);

// =============================================================================
// CRUD Operations
// =============================================================================

// Add an object to the database (returns managed object, caller must release)
LATTICE_EXPORT lattice_object_t* lattice_db_add(lattice_db_t* db, lattice_object_t* obj);

// Find an object by primary key (returns NULL if not found, caller must release)
LATTICE_EXPORT lattice_object_t* lattice_db_find(lattice_db_t* db, const char* table_name, int64_t id);

// Find an object by global ID (returns NULL if not found, caller must release)
LATTICE_EXPORT lattice_object_t* lattice_db_find_by_global_id(lattice_db_t* db, const char* table_name,
                                                const char* global_id);

// Remove an object from the database
LATTICE_EXPORT lattice_status_t lattice_db_remove(lattice_db_t* db, lattice_object_t* obj);

// =============================================================================
// Query Operations
// =============================================================================

// Query objects from a table
// where_clause: SQL WHERE clause (NULL for all objects)
// order_by: SQL ORDER BY clause (NULL for default order)
// limit: Maximum number of results (0 for unlimited)
// offset: Number of results to skip (0 for none)
LATTICE_EXPORT lattice_results_t* lattice_db_query(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause,
    const char* order_by,
    int64_t limit,
    int64_t offset
);

// Get count of objects matching query
LATTICE_EXPORT size_t lattice_db_count(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
);

// Delete objects matching where clause
LATTICE_EXPORT size_t lattice_db_delete_where(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
);

// =============================================================================
// Results Iteration
// =============================================================================

// Get number of results
LATTICE_EXPORT size_t lattice_results_count(lattice_results_t* results);

// Get object at index (caller must release the returned object)
LATTICE_EXPORT lattice_object_t* lattice_results_get(lattice_results_t* results, size_t index);

// Free results (does not free individual objects you've retrieved)
LATTICE_EXPORT void lattice_results_free(lattice_results_t* results);

// =============================================================================
// Transactions
// =============================================================================

LATTICE_EXPORT lattice_status_t lattice_db_begin_transaction(lattice_db_t* db);
LATTICE_EXPORT lattice_status_t lattice_db_commit(lattice_db_t* db);
LATTICE_EXPORT lattice_status_t lattice_db_rollback(lattice_db_t* db);

// =============================================================================
// Scheduler (required for observer callbacks)
// =============================================================================

typedef struct lattice_scheduler lattice_scheduler_t;

// Callback type for scheduler invoke - called when observer notifications fire
typedef void (*lattice_invoke_fn)(void* context, void (*callback)(void*), void* callback_context);

// Create a scheduler with custom invoke function
// invoke_fn: Called to dispatch observer callbacks. The function should call
//            callback(callback_context) on the appropriate thread/context.
// context: User context passed to invoke_fn
// destroy_fn: Called when scheduler is destroyed (can be NULL)
LATTICE_EXPORT lattice_scheduler_t* lattice_scheduler_create(
    void* context,
    lattice_invoke_fn invoke_fn,
    void (*destroy_fn)(void*)
);

// Release a scheduler
LATTICE_EXPORT void lattice_scheduler_release(lattice_scheduler_t* scheduler);

// Create a database with schemas and scheduler
// The scheduler is used to dispatch observer callbacks
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_scheduler(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler
);

// Create a database with schemas, scheduler, and sync configuration
// wss_endpoint: WebSocket URL for sync (e.g., "wss://api.example.com/sync")
// authorization_token: Bearer token for sync authentication
// Pass NULL for wss_endpoint to disable sync
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_sync(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char* wss_endpoint,
    const char* authorization_token
);

// =============================================================================
// Observation
// =============================================================================

typedef void (*lattice_table_observer_fn)(
    void* context,
    const char* operation,  // "INSERT", "UPDATE", or "DELETE"
    int64_t row_id,
    const char* global_id
);

typedef void (*lattice_object_observer_fn)(void* context);

// Add a table observer (returns observer token, 0 on failure)
LATTICE_EXPORT uint64_t lattice_db_observe_table(
    lattice_db_t* db,
    const char* table_name,
    void* context,
    lattice_table_observer_fn callback
);

// Add an object observer
LATTICE_EXPORT uint64_t lattice_db_observe_object(
    lattice_db_t* db,
    const char* table_name,
    int64_t row_id,
    void* context,
    lattice_object_observer_fn callback
);

// Remove observers
LATTICE_EXPORT void lattice_db_remove_table_observer(lattice_db_t* db, const char* table_name, uint64_t token);
LATTICE_EXPORT void lattice_db_remove_object_observer(lattice_db_t* db, const char* table_name,
                                        int64_t row_id, uint64_t token);

// =============================================================================
// Object Relationships (same as Swift's set_object/get_object)
// =============================================================================

// Set an object link - wraps dynamic_object_ref::set_object()
LATTICE_EXPORT void lattice_object_set_object(lattice_object_t* obj, const char* field, lattice_object_t* value);

// Get a linked object - wraps dynamic_object_ref::get_object()
// Returns NULL if no link. Caller must release.
LATTICE_EXPORT lattice_object_t* lattice_object_get_object(lattice_object_t* obj, const char* field);

// =============================================================================
// Link Lists (one-to-many relationships, same as Swift's List<T>)
// =============================================================================

// Get a link list from an object - wraps dynamic_object_ref::get_link_list()
// Returns NULL on error. Caller must release with lattice_link_list_release.
LATTICE_EXPORT lattice_link_list_t* lattice_object_get_link_list(lattice_object_t* obj, const char* field);

// Release a link list
LATTICE_EXPORT void lattice_link_list_release(lattice_link_list_t* list);

// Get number of elements in the list
LATTICE_EXPORT size_t lattice_link_list_size(lattice_link_list_t* list);

// Get element at index (caller must release the returned object)
LATTICE_EXPORT lattice_object_t* lattice_link_list_get(lattice_link_list_t* list, size_t index);

// Append an element to the list
LATTICE_EXPORT void lattice_link_list_push_back(lattice_link_list_t* list, lattice_object_t* obj);

// Remove element at index
LATTICE_EXPORT void lattice_link_list_erase(lattice_link_list_t* list, size_t index);

// Remove all elements
LATTICE_EXPORT void lattice_link_list_clear(lattice_link_list_t* list);

// =============================================================================
// Sync Operations
// =============================================================================

// Opaque type for audit log entry
typedef struct lattice_audit_log_entry lattice_audit_log_entry_t;

// Opaque type for audit log entry vector
typedef struct lattice_audit_log_vector lattice_audit_log_vector_t;

// Receive sync data from server (JSON-encoded ServerSentEvent)
// Applies entries with per-entry error isolation (same core path the Swift
// bridge uses): an entry that fails to apply is skipped and the remaining
// entries are still applied.
// Returns the array of global IDs that were SUCCESSFULLY applied (JSON array
// string, caller must free with lattice_string_free) - entries missing from
// the returned array were not applied and must not be acked.
// Returns NULL on total failure (malformed payload, database-level error);
// see lattice_last_error() for details.
LATTICE_EXPORT char* lattice_db_receive_sync_data(
    lattice_db_t* db,
    const uint8_t* data,
    size_t data_size
);

// Get pending (unsynchronized) audit log entries as JSON
// Returns JSON array of audit log entries (caller must free)
// Returns NULL on error
LATTICE_EXPORT char* lattice_db_get_pending_audit_log(lattice_db_t* db);

// Get audit log entries after a checkpoint (for server-side sync)
// checkpoint_global_id: NULL to get all entries, or the globalId to get entries after
// Returns JSON array of audit log entries (caller must free)
LATTICE_EXPORT char* lattice_db_events_after(lattice_db_t* db, const char* checkpoint_global_id);

// Create a sync message from audit log entries (for server-side sync)
// entries_json: JSON array of audit log entries (from events_after)
// Returns properly formatted server_sent_event JSON: {"kind":"auditLog","auditLog":[...]}
// Returns NULL on error or if entries array is empty (caller must free)
LATTICE_EXPORT char* lattice_create_sync_message(const char* entries_json);

// Create an ack message for sync (for server-side sync)
// global_ids_json: JSON array of globalId strings
// Returns properly formatted server_sent_event JSON: {"kind":"ack","ack":[...]}
// Returns NULL on error (caller must free)
LATTICE_EXPORT char* lattice_create_ack_message(const char* global_ids_json);

// Mark audit log entries as synchronized
// global_ids_json: JSON array of globalId strings to mark as synced
LATTICE_EXPORT void lattice_db_mark_synced(lattice_db_t* db, const char* global_ids_json);

// Compact the audit log by replacing all entries with INSERT records
// representing the current state of all objects. Drops all history.
// Returns number of INSERT entries created.
LATTICE_EXPORT int64_t lattice_db_compact_audit_log(lattice_db_t* db);

// Generate audit log INSERT entries for objects not already in the audit log.
// Preserves existing entries and only adds entries for missing objects.
// Useful for initial migration when enabling sync on existing data.
// Returns number of INSERT entries created.
LATTICE_EXPORT int64_t lattice_db_generate_history(lattice_db_t* db);

// Free a string returned by sync operations
LATTICE_EXPORT void lattice_string_free(char* str);

// =============================================================================
// WebSocket Client (for platform-specific WebSocket injection)
// =============================================================================

// Opaque type for WebSocket client
typedef struct lattice_websocket_client lattice_websocket_client_t;

// WebSocket state enum (matches C++ websocket_state)
typedef enum {
    LATTICE_WS_CONNECTING = 0,
    LATTICE_WS_OPEN = 1,
    LATTICE_WS_CLOSING = 2,
    LATTICE_WS_CLOSED = 3
} lattice_websocket_state_t;

// WebSocket message type
typedef enum {
    LATTICE_WS_MSG_TEXT = 0,
    LATTICE_WS_MSG_BINARY = 1
} lattice_websocket_msg_type_t;

// Callback function pointers for operations (platform calls these)
// Note: headers is passed as JSON object string "{\"key\":\"value\",...}"
typedef void (*lattice_ws_connect_fn)(void* user_data, const char* url, const char* headers_json);
typedef void (*lattice_ws_disconnect_fn)(void* user_data);
typedef lattice_websocket_state_t (*lattice_ws_state_fn)(void* user_data);
typedef void (*lattice_ws_send_fn)(void* user_data, lattice_websocket_msg_type_t type,
                                    const uint8_t* data, size_t data_size);

// Callback function pointers for events (C++ calls these when events happen)
typedef void (*lattice_ws_on_open_fn)(void* user_data);
typedef void (*lattice_ws_on_message_fn)(void* user_data, lattice_websocket_msg_type_t type,
                                          const uint8_t* data, size_t data_size);
typedef void (*lattice_ws_on_error_fn)(void* user_data, const char* error);
typedef void (*lattice_ws_on_close_fn)(void* user_data, int code, const char* reason);

// Create a WebSocket client with platform-provided callbacks
// Returns opaque pointer that must be released with lattice_websocket_client_release
// user_data: Context passed to all callbacks
// connect_fn: Called when C++ wants to connect
// disconnect_fn: Called when C++ wants to disconnect
// state_fn: Called when C++ queries current state
// send_fn: Called when C++ wants to send a message
LATTICE_EXPORT lattice_websocket_client_t* lattice_websocket_client_create(
    void* user_data,
    lattice_ws_connect_fn connect_fn,
    lattice_ws_disconnect_fn disconnect_fn,
    lattice_ws_state_fn state_fn,
    lattice_ws_send_fn send_fn
);

// Release a WebSocket client
LATTICE_EXPORT void lattice_websocket_client_release(lattice_websocket_client_t* client);

// Set event handlers (C++ will call these when it receives events from the network layer)
// These are called by C++ code when it wants to notify the platform of events
LATTICE_EXPORT void lattice_websocket_client_set_on_open(lattice_websocket_client_t* client,
                                           void* user_data, lattice_ws_on_open_fn fn);
LATTICE_EXPORT void lattice_websocket_client_set_on_message(lattice_websocket_client_t* client,
                                              void* user_data, lattice_ws_on_message_fn fn);
LATTICE_EXPORT void lattice_websocket_client_set_on_error(lattice_websocket_client_t* client,
                                            void* user_data, lattice_ws_on_error_fn fn);
LATTICE_EXPORT void lattice_websocket_client_set_on_close(lattice_websocket_client_t* client,
                                            void* user_data, lattice_ws_on_close_fn fn);

// Trigger events from platform to C++ (platform calls these when WebSocket events occur)
// Call these from your platform WebSocket implementation when events happen
LATTICE_EXPORT void lattice_websocket_client_trigger_on_open(lattice_websocket_client_t* client);
LATTICE_EXPORT void lattice_websocket_client_trigger_on_message(lattice_websocket_client_t* client,
                                                  lattice_websocket_msg_type_t type,
                                                  const uint8_t* data, size_t data_size);
LATTICE_EXPORT void lattice_websocket_client_trigger_on_error(lattice_websocket_client_t* client, const char* error);
LATTICE_EXPORT void lattice_websocket_client_trigger_on_close(lattice_websocket_client_t* client,
                                                int code, const char* reason);

// =============================================================================
// Network Factory (registers WebSocket implementation with database)
// =============================================================================

// Opaque type for network factory
typedef struct lattice_network_factory lattice_network_factory_t;

// Callback to create a new WebSocket client instance
// Returns raw pointer that will be managed by C++
typedef lattice_websocket_client_t* (*lattice_create_websocket_fn)(void* user_data);

// Create a network factory with a custom WebSocket creator
// user_data: Context passed to creator callback
// create_ws_fn: Called when C++ needs a new WebSocket client
// destroy_fn: Called when factory is destroyed (can be NULL)
LATTICE_EXPORT lattice_network_factory_t* lattice_network_factory_create(
    void* user_data,
    lattice_create_websocket_fn create_ws_fn,
    void (*destroy_fn)(void* user_data)
);

// Release a network factory
LATTICE_EXPORT void lattice_network_factory_release(lattice_network_factory_t* factory);

// Register a network factory globally (used by all databases)
LATTICE_EXPORT void lattice_set_network_factory(lattice_network_factory_t* factory);

// Attach a network factory to a specific database
LATTICE_EXPORT void lattice_db_set_network_factory(lattice_db_t* db, lattice_network_factory_t* factory);

// =============================================================================
// Query Extensions (FTS5, Vector Search, Geospatial, Distinct/Group)
// =============================================================================

// Distance metric for vector search
typedef enum {
    LATTICE_DISTANCE_L2 = 0,       // Euclidean distance (default)
    LATTICE_DISTANCE_COSINE = 1,   // Cosine distance
    LATTICE_DISTANCE_L1 = 2,       // Manhattan distance
} lattice_distance_metric_t;

// KNN result: globalId + distance
typedef struct {
    const char* global_id;  // Caller must free with lattice_string_free()
    double distance;
} lattice_knn_result_t;

// Perform KNN (K-Nearest Neighbors) vector search
// Returns array of results, caller must free each global_id and the array itself
// result_count is set to the number of results returned
LATTICE_EXPORT lattice_knn_result_t* lattice_db_query_nearest(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    const uint8_t* query_vector,
    size_t vector_size,
    int k,
    lattice_distance_metric_t metric,
    const char* where_clause        // NULL for no filtering
);
// Get count from last nearest query
LATTICE_EXPORT size_t lattice_knn_results_count(lattice_knn_result_t* results);
// Free KNN results
LATTICE_EXPORT void lattice_knn_results_free(lattice_knn_result_t* results, size_t count);

// Full-text search query
// Returns results matching the FTS5 MATCH expression on the given column
LATTICE_EXPORT lattice_results_t* lattice_db_query_fts(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    const char* match_expression,
    const char* order_by,           // NULL for rank order
    int64_t limit                   // 0 for unlimited
);

// Geospatial bounding box query (R*Tree)
// Returns objects within the given bounding box
LATTICE_EXPORT lattice_results_t* lattice_db_query_within_bounds(
    lattice_db_t* db,
    const char* table_name,
    const char* column_name,
    double min_lat,
    double max_lat,
    double min_lon,
    double max_lon,
    const char* where_clause,       // NULL for no additional filtering
    int64_t limit                   // 0 for unlimited
);

// Query with DISTINCT or GROUP BY
LATTICE_EXPORT lattice_results_t* lattice_db_query_distinct(
    lattice_db_t* db,
    const char* table_name,
    const char* distinct_by,        // Column name for DISTINCT (GROUP BY)
    const char* where_clause,
    const char* order_by,
    int64_t limit,
    int64_t offset
);

// Count with DISTINCT/GROUP BY support
LATTICE_EXPORT size_t lattice_db_count_distinct(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause,
    const char* group_by,
    const char* distinct_by
);

// =============================================================================
// Database Attachment (cross-database queries)
// =============================================================================

// Attach another database for cross-DB queries (UNION ALL)
LATTICE_EXPORT lattice_status_t lattice_db_attach(lattice_db_t* db, lattice_db_t* other);

// =============================================================================
// Migration Support
// =============================================================================

// Opaque migration context
typedef struct lattice_migration_context lattice_migration_context_t;

// Migration callback: called for each row during migration
// old_values/new_values are JSON objects mapping column names to values
typedef void (*lattice_migration_row_fn)(
    void* context,
    const char* old_values_json,
    char** new_values_json           // Caller sets this to transformed JSON (must be malloc'd)
);

// Create a database with schema version and migration support
//
// LIMITATION - BLOB columns: the JSON row round-trip used by
// lattice_migration_row_fn cannot represent BLOB values (including vector
// columns, which are stored as BLOBs). Requesting a row migration
// (migration_fn + migration_table) for a table that contains BLOB columns
// FAILS EXPLICITLY: this function returns NULL and lattice_last_error()
// describes the offending column. Previously such migrations silently
// dropped the BLOB data. BLOB-capable row migration arrives with the
// unified-open migration ABI (see docs/CAPI-STABILITY.md).
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_migration(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    int32_t target_schema_version,
    void* migration_context,
    lattice_migration_row_fn migration_fn,
    const char* migration_table      // Table name to migrate (NULL = no row migration)
);

// =============================================================================
// Add with preserved global ID (for sync / cross-DB identity)
// =============================================================================

// Add an object with a specific globalId (preserving identity across databases)
LATTICE_EXPORT lattice_object_t* lattice_db_add_with_global_id(
    lattice_db_t* db,
    lattice_object_t* obj,
    const char* global_id
);

// =============================================================================
// IPC Sync Configuration
// =============================================================================

// Create a database with IPC sync targets
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_ipc(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char** ipc_channels,
    size_t ipc_channel_count
);

// =============================================================================
// Sync Filter
// =============================================================================

// Set sync filter for upload (per-table predicates)
// filter_json: JSON array of {"table": "...", "predicate": "..."} objects.
// "predicate" is a SQL WHERE-clause fragment; omit it (or pass "") to sync
// ALL rows of the table. The key "where_clause" is accepted as a legacy
// alias for "predicate" (older callers targeted the core field name; both
// keys are parsed, "predicate" wins when both are present and non-empty).
LATTICE_EXPORT lattice_status_t lattice_db_set_sync_filter(lattice_db_t* db, const char* filter_json);

// Clear sync filter (upload everything)
LATTICE_EXPORT lattice_status_t lattice_db_clear_sync_filter(lattice_db_t* db);

// =============================================================================
// Cross-Process Observation
// =============================================================================

// Register a cross-process observer (fires when another process modifies the DB).
//
// Delivery rides the core cross-process notifier: writes committed by OTHER
// processes are detected by tailing the audit log and dispatched through the
// same batched change pipeline as local flushes. Consequently the callback
// also fires for writes made through THIS handle - callers needing
// other-process-only events should filter out their own writes (e.g. by
// global_id). Callbacks are dispatched via the database's scheduler, once per
// changed row.
//
// Returns an observer token (0 on failure). Remove with
// lattice_db_remove_cross_process_observer.
LATTICE_EXPORT uint64_t lattice_db_observe_cross_process(
    lattice_db_t* db,
    const char* table_name,
    void* context,
    lattice_table_observer_fn callback
);

// Remove a cross-process observer previously registered with
// lattice_db_observe_cross_process (no-op for unknown tokens).
LATTICE_EXPORT void lattice_db_remove_cross_process_observer(lattice_db_t* db, uint64_t token);

// =============================================================================
// Sync Options (size-prefixed struct; docs/CAPI-STABILITY.md rule 3)
// =============================================================================

// Sync-tuning + sync-configuration options for
// lattice_db_create_with_sync_options.
//
// SIZE-PREFIXED: struct_size is the first member and MUST be initialized —
// always via lattice_sync_options_init(), which also fills every knob with
// its "keep the library default" sentinel:
//
//   lattice_sync_options_t opts;
//   lattice_sync_options_init(&opts);
//   opts.chunk_size = 512;                 // override only what you need
//
// The implementation reads min(struct_size, sizeof(lattice_sync_options_t))
// bytes, so binaries compiled against an older (shorter) version of this
// struct keep working as fields are appended at the tail.
//
// VALIDATION mirrors the Swift bridge setters: nonsensical values are
// IGNORED (the knob keeps the library default) rather than failing the open
// — chunk_size <= 0, delays <= 0, negative windows/intervals. 0 stays valid
// where it means "disabled": max_reconnect_attempts 0 = retry forever,
// upload_coalesce_ms 0 = legacy immediate dispatch, checkpoint interval
// 0 = that checkpoint cadence off.
typedef struct {
    size_t struct_size;                      // set by lattice_sync_options_init()

    // Sync-tuning knobs (sentinel = keep the library default).
    int64_t chunk_size;                      // audit entries per upload chunk; applies when > 0 (sentinel 0)
    int32_t max_reconnect_attempts;          // applies when >= 0 (0 = retry forever); sentinel -1
    double  base_delay_seconds;              // reconnect backoff base; applies when > 0 (sentinel 0)
    double  max_delay_seconds;               // reconnect backoff cap; applies when > 0 (sentinel 0)
    int64_t stable_connection_ms;            // stable-connection window before backoff reset; applies when >= 0; sentinel -1
    int32_t upload_coalesce_ms;              // applies when >= 0 (0 = immediate dispatch); sentinel -1
    int32_t checkpoint_passive_interval_ms;  // applies when >= 0 (0 = off); sentinel -1
    int32_t checkpoint_truncate_interval_ms; // applies when >= 0 (0 = off); sentinel -1
    int32_t use_upload_floor;                // tri-state: 1 = on, 0 = off, -1 = keep default

    // Upload sync filter, same JSON contract as lattice_db_set_sync_filter:
    // array of {"table": "...", "predicate": "..."} objects ("where_clause"
    // accepted as legacy alias). NULL = sync everything. NOTE: an empty
    // array means sync NOTHING (explicit empty whitelist).
    const char* sync_filter_json;

    // RESERVED for the unified-open work: must be NULL. Sync ids are
    // currently always derived by the library ("wss:<wss_endpoint>",
    // "ipc:<channel>" — see lattice_db_read_upload_floor); a non-NULL value
    // fails the open with LATTICE_ERROR_INVALID_ARGUMENT so that a future
    // override cannot be silently ignored by older libraries.
    const char* sync_id;
} lattice_sync_options_t;

// Fill defaults: struct_size = sizeof(lattice_sync_options_t), every knob at
// its "keep the library default" sentinel, pointers NULL. No-op on NULL.
LATTICE_EXPORT void lattice_sync_options_init(lattice_sync_options_t* options);

// Create a database with schemas, scheduler, sync endpoint AND sync options.
// Identical to lattice_db_create_with_sync (which stays as-is per the
// additive policy) plus a trailing options struct. options may be NULL
// (all defaults — then this behaves exactly like create_with_sync).
// Pass NULL for wss_endpoint to disable WSS sync (tuning still applies to
// any IPC synchronizers created through other configuration).
// Returns NULL on failure; see lattice_last_error().
LATTICE_EXPORT lattice_db_t* lattice_db_create_with_sync_options(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler,
    const char* wss_endpoint,
    const char* authorization_token,
    const lattice_sync_options_t* options
);

// =============================================================================
// Sync Progress & Sync Observers
// =============================================================================

// Snapshot of sync progress, aggregated over ALL synchronizers attached to
// the database's path (WSS + IPC; the synchronizer may live on a sibling
// instance).
//
// SIZE-PREFIXED OUT-STRUCT: set struct_size to
// sizeof(lattice_sync_progress_t) before calling; the implementation writes
// min(struct_size, its own sizeof) bytes and sets struct_size to the number
// of bytes written.
typedef struct {
    size_t struct_size;
    int64_t pending_upload;  // entries awaiting upload in the current batch
    int64_t total_upload;    // total-upload snapshot at start of the batch
    int64_t acked;           // entries acknowledged by the peer
    int64_t received;        // cumulative downloaded entries applied
} lattice_sync_progress_t;

// Read current sync progress. A database with no sync configured returns
// LATTICE_OK with all counters zero.
LATTICE_EXPORT lattice_status_t lattice_db_get_sync_progress(lattice_db_t* db,
                                              lattice_sync_progress_t* progress);

// Sync observer callbacks. All fire on the synchronizer's own thread — do
// not block; marshal to your own thread/queue. The progress pointer is only
// valid for the duration of the callback (copy it out).
typedef void (*lattice_sync_progress_fn)(void* context,
                                         const lattice_sync_progress_t* progress);
typedef void (*lattice_sync_state_fn)(void* context, bool connected);
typedef void (*lattice_sync_error_fn)(void* context, const char* message);

// Register sync observers (established context + fn-pointer + destroy
// pattern). Returns an observer token (0 on failure). Multiple observers may
// be registered per database; tokens are shared across the three kinds and
// are removed with lattice_db_remove_sync_observer. destroy(context) is
// called when the observer is removed or the database is closed (it is NOT
// called if the last handle is released without lattice_db_close — remove
// observers or close the database before the final release).
//
// NOTE: sync callbacks are a single slot per underlying database instance;
// the C observers multiplex over that slot. Registering C observers replaces
// any callback installed directly on the same instance through another
// binding layer.
LATTICE_EXPORT uint64_t lattice_db_observe_sync_progress(
    lattice_db_t* db,
    void* context,
    lattice_sync_progress_fn callback,
    void (*destroy)(void*)
);

// connected=true when the sync transport connects, false when it drops.
LATTICE_EXPORT uint64_t lattice_db_observe_sync_state(
    lattice_db_t* db,
    void* context,
    lattice_sync_state_fn callback,
    void (*destroy)(void*)
);

// message is NUL-terminated and only valid for the duration of the callback.
LATTICE_EXPORT uint64_t lattice_db_observe_sync_error(
    lattice_db_t* db,
    void* context,
    lattice_sync_error_fn callback,
    void (*destroy)(void*)
);

// Remove a sync observer registered by any of the three functions above
// (no-op for unknown tokens). Calls the observer's destroy(context).
LATTICE_EXPORT void lattice_db_remove_sync_observer(lattice_db_t* db, uint64_t token);

// =============================================================================
// Database Detachment & Attach Errors
// =============================================================================

// Detach a previously attached database (inverse of lattice_db_attach).
// Idempotent: detaching a database that is not attached succeeds. Returns
// LATTICE_ERROR_DATABASE on failure with the reason in lattice_last_error()
// (also readable via lattice_db_last_attach_error).
LATTICE_EXPORT lattice_status_t lattice_db_detach(lattice_db_t* db, lattice_db_t* other);

// Reason for the most recent FAILED attach/detach on this database (a copy
// of the core's mutex-guarded accessor). Returns NULL when the most recent
// attach/detach succeeded (success clears it) or none has been attempted.
// The returned buffer is thread-local — copy it before the next C-API call
// on the same thread.
LATTICE_EXPORT const char* lattice_db_last_attach_error(lattice_db_t* db);

// =============================================================================
// Row Cache & Atomic Field Increment (per-object)
// =============================================================================

// Row cache: one-statement materialization of a MANAGED object's full row —
// field reads hit the snapshot instead of issuing one SELECT per column.
// The snapshot is as-of hydration/refresh; concurrent writers are invisible
// until lattice_object_refresh_row_cache. Writes through this object are
// write-through (read-your-writes holds). Any cache miss or type mismatch
// falls through to the live read — slower, never wrong. Enabling on an
// UNMANAGED object is a no-op (unmanaged objects are already value
// snapshots; is_row_cache_enabled stays false).
LATTICE_EXPORT lattice_status_t lattice_object_enable_row_cache(lattice_object_t* obj);
LATTICE_EXPORT lattice_status_t lattice_object_disable_row_cache(lattice_object_t* obj);
// Re-fetch the full row in one statement (the staleness escape hatch).
LATTICE_EXPORT lattice_status_t lattice_object_refresh_row_cache(lattice_object_t* obj);
LATTICE_EXPORT bool lattice_object_is_row_cache_enabled(lattice_object_t* obj);

// SQL-side atomic increment of an integer field: `SET field = field + delta`
// under the write lock — no read-modify-write race between concurrent
// writers. On an unmanaged object the in-memory value is incremented
// instead. `field` must be a schema column name, not user input (same
// interpolation convention as the query layer's filter clauses).
LATTICE_EXPORT lattice_status_t lattice_object_increment_int(lattice_object_t* obj,
                                              const char* field,
                                              int64_t delta);

// =============================================================================
// Statement Counters
// =============================================================================

// Process-global count of SQL statements issued through the database layer's
// public funnels, across ALL connections and databases. For statement-budget
// regression tests in bindings.
LATTICE_EXPORT uint64_t lattice_db_total_statement_count(void);

// Thread-local twin — exact budgets on single-threaded read paths.
LATTICE_EXPORT uint64_t lattice_db_thread_statement_count(void);

// =============================================================================
// WAL Checkpoint
// =============================================================================

// Checkpoint the WAL file. mode: 0 = PASSIVE (checkpoint what can be done
// without blocking readers/writers), 1 = TRUNCATE (checkpoint everything and
// truncate the WAL; silently partial under concurrent readers — the outcome
// is logged). Any other mode returns LATTICE_ERROR_INVALID_ARGUMENT.
LATTICE_EXPORT lattice_status_t lattice_db_checkpoint(lattice_db_t* db, int32_t mode);

// =============================================================================
// Sync Upload Floor (conformance/debug hook)
// =============================================================================

// Read the per-slot upload floor for a sync id from
// _lattice_replication_slots — the scan bound with the invariant that no
// entry pending for this sync_id has id <= floor. Sync ids are derived by
// the library: "wss:<wss_endpoint>" for the WSS synchronizer,
// "ipc:<channel>" for IPC synchronizers. Returns 0 when the slot does not
// exist (no floor advanced yet), -1 on invalid arguments or database error
// (see lattice_last_error()).
LATTICE_EXPORT int64_t lattice_db_read_upload_floor(lattice_db_t* db, const char* sync_id);

// =============================================================================
// Object-graph -> JSON (Detached snapshot backing)
// =============================================================================

// Serialize an object's data view - and its link graph, followed to
// max_depth hops - to a JSON string. This is the primitive a binding's
// Detached/detached(maxDepth) is built on.
//
// The output contract is PINNED to Swift's
// DynamicObject.jsonObject(maxDepth:) so all SDKs produce the same shape:
// - "globalId" / "id" keys when present.
// - primitives by declared column type: INTEGER -> number (bools are 0/1),
//   REAL -> number, BLOB -> base64 string, TEXT -> inlined as JSON when the
//   string's first character is '[' or '{' and it parses, else the raw
//   string. NULL/absent values omit the key. Vector columns are OMITTED.
// - geo_bounds: {"lat","lon"} for a point, else
//   {"minLat","maxLat","minLon","maxLon"}.
// - to-one links: recursed while max_depth > 0; at the boundary (or on a
//   revisit of an already-serialized object) they collapse to a
//   {"globalId": "..."} stub; absent links omit the key.
// - to-many lists: arrays of recursed elements while max_depth > 0, else a
//   {"count": n} summary; geo-bounds lists are {"geoBoundsCount": n}.
// - unions: {"unionRef": "<union row globalId>"}.
// - cycle-safe: revisited objects (keyed table:globalId) become stubs.
// - key ORDER within objects is unspecified; parse, don't string-compare.
//
// max_depth <= 0 serializes scalars only (links/lists collapse as above).
// Returns a malloc'd NUL-terminated string - free with lattice_string_free.
// Returns NULL on error (see lattice_last_error()).
LATTICE_EXPORT char* lattice_object_to_json(lattice_object_t* obj, int32_t max_depth);

#ifdef __cplusplus
}
#endif

#endif // LATTICE_C_API_H
