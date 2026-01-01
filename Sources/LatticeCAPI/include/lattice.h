#ifndef LATTICE_C_API_H
#define LATTICE_C_API_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

// =============================================================================
// Opaque Types
// =============================================================================

typedef struct lattice_db lattice_db_t;
typedef struct lattice_object lattice_object_t;
typedef struct lattice_results lattice_results_t;
typedef struct lattice_link_list lattice_link_list_t;

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
const char* lattice_last_error(void);

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
} lattice_property_kind_t;

typedef struct {
    const char* name;
    lattice_column_type_t type;
    lattice_property_kind_t kind;
    const char* target_table;   // For links (NULL if primitive)
    const char* link_table;     // For link lists (NULL otherwise)
    bool nullable;
} lattice_property_t;

typedef struct {
    const char* table_name;
    const lattice_property_t* properties;
    size_t property_count;
} lattice_schema_t;

// =============================================================================
// Database Creation
// =============================================================================

// Create an in-memory database (no schema - use for simple key-value style)
lattice_db_t* lattice_db_create_in_memory(void);

// Create/open a database at the given path (no schema)
lattice_db_t* lattice_db_create_at_path(const char* path);

// Create a database with schemas (tables are created/migrated automatically)
lattice_db_t* lattice_db_create_with_schemas(
    const char* path,           // NULL or ":memory:" for in-memory
    const lattice_schema_t* schemas,
    size_t schema_count
);

// Retain a reference to the database
void lattice_db_retain(lattice_db_t* db);

// Release a reference to the database (frees when ref_count hits 0)
void lattice_db_release(lattice_db_t* db);

// =============================================================================
// Object Lifecycle
// =============================================================================

// Create a new object for a table (without schema - not recommended)
lattice_object_t* lattice_object_create(const char* table_name);

// Create a new object with explicit schema (like Swift's _defaultCxxLatticeObject)
// This is the preferred way to create standalone objects without a db reference
lattice_object_t* lattice_object_create_with_schema(
    const char* table_name,
    const lattice_property_t* properties,
    size_t property_count
);

// Create a new object with schema from the database's registered schemas
lattice_object_t* lattice_db_create_object(lattice_db_t* db, const char* table_name);

// Retain a reference
void lattice_object_retain(lattice_object_t* obj);

// Release a reference
void lattice_object_release(lattice_object_t* obj);

// =============================================================================
// Object Field Access
// =============================================================================

// Getters (return default value if field not set or wrong type)
int64_t lattice_object_get_int(lattice_object_t* obj, const char* field);
double lattice_object_get_double(lattice_object_t* obj, const char* field);
const char* lattice_object_get_string(lattice_object_t* obj, const char* field);
size_t lattice_object_get_blob(lattice_object_t* obj, const char* field,
                                uint8_t* buffer, size_t buffer_size);

// Check if field has a value
bool lattice_object_has_value(lattice_object_t* obj, const char* field);

// Setters
void lattice_object_set_int(lattice_object_t* obj, const char* field, int64_t value);
void lattice_object_set_double(lattice_object_t* obj, const char* field, double value);
void lattice_object_set_string(lattice_object_t* obj, const char* field, const char* value);
void lattice_object_set_blob(lattice_object_t* obj, const char* field,
                             const uint8_t* data, size_t size);
void lattice_object_set_null(lattice_object_t* obj, const char* field);

// Object identity (for managed objects)
int64_t lattice_object_get_id(lattice_object_t* obj);
const char* lattice_object_get_global_id(lattice_object_t* obj);
const char* lattice_object_get_table_name(lattice_object_t* obj);

// =============================================================================
// CRUD Operations
// =============================================================================

// Add an object to the database (returns managed object, caller must release)
lattice_object_t* lattice_db_add(lattice_db_t* db, lattice_object_t* obj);

// Find an object by primary key (returns NULL if not found, caller must release)
lattice_object_t* lattice_db_find(lattice_db_t* db, const char* table_name, int64_t id);

// Find an object by global ID (returns NULL if not found, caller must release)
lattice_object_t* lattice_db_find_by_global_id(lattice_db_t* db, const char* table_name,
                                                const char* global_id);

// Remove an object from the database
lattice_status_t lattice_db_remove(lattice_db_t* db, lattice_object_t* obj);

// =============================================================================
// Query Operations
// =============================================================================

// Query objects from a table
// where_clause: SQL WHERE clause (NULL for all objects)
// order_by: SQL ORDER BY clause (NULL for default order)
// limit: Maximum number of results (0 for unlimited)
// offset: Number of results to skip (0 for none)
lattice_results_t* lattice_db_query(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause,
    const char* order_by,
    int64_t limit,
    int64_t offset
);

// Get count of objects matching query
size_t lattice_db_count(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
);

// Delete objects matching where clause
size_t lattice_db_delete_where(
    lattice_db_t* db,
    const char* table_name,
    const char* where_clause
);

// =============================================================================
// Results Iteration
// =============================================================================

// Get number of results
size_t lattice_results_count(lattice_results_t* results);

// Get object at index (caller must release the returned object)
lattice_object_t* lattice_results_get(lattice_results_t* results, size_t index);

// Free results (does not free individual objects you've retrieved)
void lattice_results_free(lattice_results_t* results);

// =============================================================================
// Transactions
// =============================================================================

lattice_status_t lattice_db_begin_transaction(lattice_db_t* db);
lattice_status_t lattice_db_commit(lattice_db_t* db);
lattice_status_t lattice_db_rollback(lattice_db_t* db);

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
lattice_scheduler_t* lattice_scheduler_create(
    void* context,
    lattice_invoke_fn invoke_fn,
    void (*destroy_fn)(void*)
);

// Release a scheduler
void lattice_scheduler_release(lattice_scheduler_t* scheduler);

// Create a database with schemas and scheduler
// The scheduler is used to dispatch observer callbacks
lattice_db_t* lattice_db_create_with_scheduler(
    const char* path,
    const lattice_schema_t* schemas,
    size_t schema_count,
    lattice_scheduler_t* scheduler
);

// Create a database with schemas, scheduler, and sync configuration
// wss_endpoint: WebSocket URL for sync (e.g., "wss://api.example.com/sync")
// authorization_token: Bearer token for sync authentication
// Pass NULL for wss_endpoint to disable sync
lattice_db_t* lattice_db_create_with_sync(
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
uint64_t lattice_db_observe_table(
    lattice_db_t* db,
    const char* table_name,
    void* context,
    lattice_table_observer_fn callback
);

// Add an object observer
uint64_t lattice_db_observe_object(
    lattice_db_t* db,
    const char* table_name,
    int64_t row_id,
    void* context,
    lattice_object_observer_fn callback
);

// Remove observers
void lattice_db_remove_table_observer(lattice_db_t* db, const char* table_name, uint64_t token);
void lattice_db_remove_object_observer(lattice_db_t* db, const char* table_name,
                                        int64_t row_id, uint64_t token);

// =============================================================================
// Object Relationships (same as Swift's set_object/get_object)
// =============================================================================

// Set an object link - wraps dynamic_object_ref::set_object()
void lattice_object_set_object(lattice_object_t* obj, const char* field, lattice_object_t* value);

// Get a linked object - wraps dynamic_object_ref::get_object()
// Returns NULL if no link. Caller must release.
lattice_object_t* lattice_object_get_object(lattice_object_t* obj, const char* field);

// =============================================================================
// Link Lists (one-to-many relationships, same as Swift's List<T>)
// =============================================================================

// Get a link list from an object - wraps dynamic_object_ref::get_link_list()
// Returns NULL on error. Caller must release with lattice_link_list_release.
lattice_link_list_t* lattice_object_get_link_list(lattice_object_t* obj, const char* field);

// Release a link list
void lattice_link_list_release(lattice_link_list_t* list);

// Get number of elements in the list
size_t lattice_link_list_size(lattice_link_list_t* list);

// Get element at index (caller must release the returned object)
lattice_object_t* lattice_link_list_get(lattice_link_list_t* list, size_t index);

// Append an element to the list
void lattice_link_list_push_back(lattice_link_list_t* list, lattice_object_t* obj);

// Remove element at index
void lattice_link_list_erase(lattice_link_list_t* list, size_t index);

// Remove all elements
void lattice_link_list_clear(lattice_link_list_t* list);

// =============================================================================
// Sync Operations
// =============================================================================

// Opaque type for audit log entry
typedef struct lattice_audit_log_entry lattice_audit_log_entry_t;

// Opaque type for audit log entry vector
typedef struct lattice_audit_log_vector lattice_audit_log_vector_t;

// Receive sync data from server (JSON-encoded ServerSentEvent)
// Returns array of global IDs that were applied (JSON array string, caller must free)
// Returns NULL on error
char* lattice_db_receive_sync_data(
    lattice_db_t* db,
    const uint8_t* data,
    size_t data_size
);

// Get pending (unsynchronized) audit log entries as JSON
// Returns JSON array of audit log entries (caller must free)
// Returns NULL on error
char* lattice_db_get_pending_audit_log(lattice_db_t* db);

// Get audit log entries after a checkpoint (for server-side sync)
// checkpoint_global_id: NULL to get all entries, or the globalId to get entries after
// Returns JSON array of audit log entries (caller must free)
char* lattice_db_events_after(lattice_db_t* db, const char* checkpoint_global_id);

// Create a sync message from audit log entries (for server-side sync)
// entries_json: JSON array of audit log entries (from events_after)
// Returns properly formatted server_sent_event JSON: {"kind":"auditLog","auditLog":[...]}
// Returns NULL on error or if entries array is empty (caller must free)
char* lattice_create_sync_message(const char* entries_json);

// Create an ack message for sync (for server-side sync)
// global_ids_json: JSON array of globalId strings
// Returns properly formatted server_sent_event JSON: {"kind":"ack","ack":[...]}
// Returns NULL on error (caller must free)
char* lattice_create_ack_message(const char* global_ids_json);

// Mark audit log entries as synchronized
// global_ids_json: JSON array of globalId strings to mark as synced
void lattice_db_mark_synced(lattice_db_t* db, const char* global_ids_json);

// Compact the audit log by replacing all entries with INSERT records
// representing the current state of all objects. Drops all history.
// Returns number of INSERT entries created.
int64_t lattice_db_compact_audit_log(lattice_db_t* db);

// Generate audit log INSERT entries for objects not already in the audit log.
// Preserves existing entries and only adds entries for missing objects.
// Useful for initial migration when enabling sync on existing data.
// Returns number of INSERT entries created.
int64_t lattice_db_generate_history(lattice_db_t* db);

// Free a string returned by sync operations
void lattice_string_free(char* str);

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
lattice_websocket_client_t* lattice_websocket_client_create(
    void* user_data,
    lattice_ws_connect_fn connect_fn,
    lattice_ws_disconnect_fn disconnect_fn,
    lattice_ws_state_fn state_fn,
    lattice_ws_send_fn send_fn
);

// Release a WebSocket client
void lattice_websocket_client_release(lattice_websocket_client_t* client);

// Set event handlers (C++ will call these when it receives events from the network layer)
// These are called by C++ code when it wants to notify the platform of events
void lattice_websocket_client_set_on_open(lattice_websocket_client_t* client,
                                           void* user_data, lattice_ws_on_open_fn fn);
void lattice_websocket_client_set_on_message(lattice_websocket_client_t* client,
                                              void* user_data, lattice_ws_on_message_fn fn);
void lattice_websocket_client_set_on_error(lattice_websocket_client_t* client,
                                            void* user_data, lattice_ws_on_error_fn fn);
void lattice_websocket_client_set_on_close(lattice_websocket_client_t* client,
                                            void* user_data, lattice_ws_on_close_fn fn);

// Trigger events from platform to C++ (platform calls these when WebSocket events occur)
// Call these from your platform WebSocket implementation when events happen
void lattice_websocket_client_trigger_on_open(lattice_websocket_client_t* client);
void lattice_websocket_client_trigger_on_message(lattice_websocket_client_t* client,
                                                  lattice_websocket_msg_type_t type,
                                                  const uint8_t* data, size_t data_size);
void lattice_websocket_client_trigger_on_error(lattice_websocket_client_t* client, const char* error);
void lattice_websocket_client_trigger_on_close(lattice_websocket_client_t* client,
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
lattice_network_factory_t* lattice_network_factory_create(
    void* user_data,
    lattice_create_websocket_fn create_ws_fn,
    void (*destroy_fn)(void* user_data)
);

// Release a network factory
void lattice_network_factory_release(lattice_network_factory_t* factory);

// Register a network factory globally (used by all databases)
void lattice_set_network_factory(lattice_network_factory_t* factory);

// Attach a network factory to a specific database
void lattice_db_set_network_factory(lattice_db_t* db, lattice_network_factory_t* factory);

#ifdef __cplusplus
}
#endif

#endif // LATTICE_C_API_H
