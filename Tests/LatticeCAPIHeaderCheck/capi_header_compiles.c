// capi_header_compiles.c — pins the C-ABI header to pure C11.
//
// lattice.h is consumed by ctypes (Python) and JNI/cinterop (Kotlin) callers
// that compile it as plain C. This translation unit is compiled as C11 (NOT
// C++) in its own C-only target (LatticeCAPIHeaderCheck — SwiftPM applies a
// C++ target's -std=c++20 unsafeFlags to C sources too, so the pin cannot
// live inside LatticeCoreTests directly); if anyone adds C++-isms, a
// C++-only type, or a missing include to the public header, the test suite
// stops building. The CAPISurface gtest calls the probe so a silently-dropped
// target would also fail at link time.
//
// (docs/CAPI-STABILITY.md — "header compiles as pure C11" policy.)

#include "capi_header_check.h"
#include "lattice.h"

#ifdef __cplusplus
#error "capi_header_compiles.c must be compiled as C, not C++"
#endif

#if !defined(__STDC_VERSION__) || __STDC_VERSION__ < 201112L
#error "lattice.h must compile under C11 (or newer)"
#endif

int lattice_capi_header_compiles_as_c11(void) {
    // Touch representative public surface so the compiler fully type-checks
    // it. No lattice_* function is CALLED — this target does not link
    // LatticeCAPI; the header alone must be self-contained.
    lattice_property_t prop = {0};
    lattice_unique_constraint_t constraint = {0};
    lattice_schema_t schema = {0};
    lattice_knn_result_t knn = {0};

    lattice_status_t status = LATTICE_OK;
    lattice_column_type_t col = LATTICE_TYPE_BLOB;
    lattice_property_kind_t kind = LATTICE_KIND_VIRTUAL_LINK;
    lattice_distance_metric_t metric = LATTICE_DISTANCE_L1;
    lattice_websocket_state_t ws_state = LATTICE_WS_CLOSED;
    lattice_websocket_msg_type_t ws_msg = LATTICE_WS_MSG_BINARY;

    lattice_invoke_fn invoke = 0;
    lattice_table_observer_fn table_obs = 0;
    lattice_object_observer_fn object_obs = 0;
    lattice_migration_row_fn migration = 0;
    lattice_create_websocket_fn create_ws = 0;
    lattice_ws_on_message_fn on_message = 0;

    lattice_db_t* db = 0;
    lattice_object_t* obj = 0;
    lattice_results_t* results = 0;
    lattice_link_list_t* list = 0;
    lattice_scheduler_t* sched = 0;
    lattice_websocket_client_t* ws = 0;
    lattice_network_factory_t* factory = 0;

    (void)prop; (void)constraint; (void)schema; (void)col; (void)kind;
    (void)metric; (void)ws_state; (void)ws_msg; (void)db; (void)obj;
    (void)results; (void)list; (void)sched; (void)ws; (void)factory;

    int ok = (status == 0)
        && (LATTICE_ERROR_DATABASE == -4)
        && (LATTICE_CAPI_VERSION ==
            LATTICE_CAPI_VERSION_MAJOR * 1000000 +
            LATTICE_CAPI_VERSION_MINOR * 1000 +
            LATTICE_CAPI_VERSION_PATCH)
        && (sizeof(LATTICE_CAPI_VERSION_STRING) > 1)
        && !invoke && !table_obs && !object_obs && !migration
        && !create_ws && !on_message
        && (schema.property_count == 0)
        && (knn.distance == 0.0);
    return ok;
}
