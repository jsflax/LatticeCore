cmake_minimum_required(VERSION 3.16)
project(GenuineOldReceiptCompanion VERSION 0.1.0 LANGUAGES C CXX)
if(NOT APPLE)
  message(FATAL_ERROR "This exact prior dependency map is qualified for the same macOS host as the current candidate")
endif()
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
option(LATTICE_SYNC_COMMIT_PROBE "Original old product option" OFF)
# Product targets below are copied literally from the genuine old CMake family.
# For Android, we need to bundle SQLite3
if(ANDROID)
    include(FetchContent)
    FetchContent_Declare(
        sqlite3
        URL https://www.sqlite.org/2024/sqlite-amalgamation-3450000.zip
        URL_HASH SHA256=bde30d13ebdf84926ddd5e8b6df145be03a577a48fd075a087a5dd815bcdf740
    )
    FetchContent_MakeAvailable(sqlite3)

    # Create SQLite3 library from amalgamation
    add_library(sqlite3_bundled STATIC ${sqlite3_SOURCE_DIR}/sqlite3.c)
    target_include_directories(sqlite3_bundled PUBLIC ${sqlite3_SOURCE_DIR})
    target_compile_definitions(sqlite3_bundled PRIVATE
        SQLITE_ENABLE_FTS5
        SQLITE_ENABLE_JSON1
        SQLITE_ENABLE_RTREE
    )
    # SQLITE_CORE must be PUBLIC so all consumers (including SqliteVec) use sqlite3.h not sqlite3ext.h
    target_compile_definitions(sqlite3_bundled PUBLIC SQLITE_CORE)

    # Create alias to match FindSQLite3 target name
    add_library(SQLite::SQLite3 ALIAS sqlite3_bundled)
else()
    # Find system SQLite3
    find_package(SQLite3 REQUIRED)
endif()

# =============================================================================
# SqliteVec - SQLite vector extension (C library)
# =============================================================================
add_library(SqliteVec STATIC
    Sources/SqliteVec/src/sqlite-vec.c
)
target_include_directories(SqliteVec PUBLIC
    Sources/SqliteVec/include
    PRIVATE Sources/SqliteVec/src
)
target_compile_definitions(SqliteVec PRIVATE
    SQLITE_CORE
    SQLITE_VEC_STATIC
    SQLITE_VEC_EXPERIMENTAL_IVF_ENABLE
)
# Enable NEON on ARM — Clang only: sqlite-vec's NEON kernels rely on Clang's
# lax int8x16_t/uint8x16_t conversions and do not compile under GCC's strict
# intrinsic signatures (seen on aarch64 Linux). The SwiftPM build always uses
# Clang; keep the CMake path aligned rather than forking behavior.
if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64"
   AND CMAKE_C_COMPILER_ID MATCHES "Clang")
    target_compile_definitions(SqliteVec PRIVATE SQLITE_VEC_ENABLE_NEON)
endif()
target_link_libraries(SqliteVec PRIVATE SQLite::SQLite3)

# =============================================================================
# LatticeCore - Core C++ ORM implementation
# =============================================================================
add_library(LatticeCore STATIC
    Sources/LatticeCore/src/db.cpp
    Sources/LatticeCore/src/ordinary_export.cpp
    Sources/LatticeCore/src/ordinary_target_staging_io.cpp
    Sources/LatticeCore/src/ordinary_target_staging_connection.cpp
    Sources/LatticeCore/src/ordinary_schema_validation.cpp
    Sources/LatticeCore/src/ordinary_internal_schema.cpp
    Sources/LatticeCore/src/ordinary_wire.cpp
    Sources/LatticeCore/src/ordinary_receiver_session.cpp
    Sources/LatticeCore/src/ordinary_v2_original.cpp
    Sources/LatticeCore/src/ordinary_source_upload.cpp
    Sources/LatticeCore/src/ordinary_v2_audit_json.cpp
    Sources/LatticeCore/src/ordinary_vector_integrity.cpp
    Sources/LatticeCore/src/ordinary_store_admission.cpp
    Sources/LatticeCore/src/ordinary_owned_launch.cpp
    Sources/LatticeCore/src/managed_observation.cpp
    Sources/LatticeCore/src/scheduler.cpp
    Sources/LatticeCore/src/schema.cpp
    Sources/LatticeCore/src/derived_profile.cpp
    Sources/LatticeCore/src/sync.cpp
    Sources/LatticeCore/src/sync_commit_probe.cpp
    Sources/LatticeCore/src/recovery_export_adapter.cpp
    Sources/LatticeCore/src/recovery_server_export.cpp
    Sources/LatticeCore/src/sync_callback_lifetime.cpp
    Sources/LatticeCore/src/configured_retirement.cpp
    Sources/LatticeCore/src/configured_attempt_custody.cpp
    Sources/LatticeCore/src/configured_platform.cpp
    Sources/LatticeCore/src/configured_recovery_connection.cpp
    Sources/LatticeCore/src/scoped_recovery_install.cpp
    Sources/LatticeCore/src/recovery_witness.cpp
    Sources/LatticeCore/src/recovery_refresh.cpp
    Sources/LatticeCore/src/receive_install_state.cpp
    Sources/LatticeCore/src/recovery_obligation_store.cpp
    Sources/LatticeCore/src/recovery_local_producer.cpp
    Sources/LatticeCore/src/receive_ledger.cpp
    Sources/LatticeCore/src/receive_delivery_guard.cpp
    Sources/LatticeCore/src/recovery_staging.cpp
    Sources/LatticeCore/src/recovery_install_transaction.cpp
    Sources/LatticeCore/src/canonical_change_store.cpp
    Sources/LatticeCore/src/recovery_receipt_coverage.cpp
    Sources/LatticeCore/src/canonical_receipt_coverage.cpp
    Sources/LatticeCore/src/canonical_writer_adapter.cpp
    Sources/LatticeCore/src/recovery_authenticated_session.cpp
    Sources/LatticeCore/src/recovery_receiver_source.cpp
    Sources/LatticeCore/src/recovery_receiver_controller.cpp
    Sources/LatticeCore/src/recovery_unknown_reconciliation.cpp
    Sources/LatticeCore/src/recovery_request_store.cpp
    Sources/LatticeCore/src/sync_recovery_protocol.cpp
    Sources/LatticeCore/src/sync_canonical_range.cpp
    Sources/LatticeCore/src/canonical_range_staging.cpp
    Sources/LatticeCore/src/sync_recovery_values.cpp
    Sources/LatticeCore/src/sync_recovery_outbox.cpp
    Sources/LatticeCore/src/sync_snapshot_source.cpp
    Sources/LatticeCore/src/network.cpp
    Sources/LatticeCore/src/ipc.cpp
    Sources/LatticeCore/src/lattice.cpp
    Sources/LatticeCore/src/projection.cpp
    Sources/LatticeCore/src/owned_catalog.cpp
    Sources/LatticeCore/src/projection_memory.cpp
    Sources/LatticeCore/src/projection_capture_policy.cpp
    Sources/LatticeCore/src/cross_process_notifier_darwin.cpp
    Sources/LatticeCore/src/cross_process_notifier_linux.cpp
    Sources/LatticeCore/src/cross_process_notifier_noop.cpp
    Sources/LatticeCore/src/ordinary_cohort_attachment.cpp
    Sources/LatticeCore/src/ordinary_cohort_attachment_qualification.cpp
    Sources/LatticeCore/src/ordinary_installation_cohort.cpp
    Sources/LatticeCore/src/ordinary_installation_files.cpp
    Sources/LatticeCore/src/ordinary_installation_manifest.cpp
    Sources/LatticeCore/src/ordinary_installation_seed.cpp
    Sources/LatticeCore/src/ordinary_ipc_frame_io.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_attempt.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_connection.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_endpoint.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_route.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_worker_owner.cpp
    Sources/LatticeCore/src/ordinary_managed_ipc_workers.cpp
    Sources/LatticeCore/src/ordinary_managed_product_route.cpp
    Sources/LatticeCore/src/ordinary_managed_wss_binding.cpp
    Sources/LatticeCore/src/ordinary_namespace_retirement.cpp
    Sources/LatticeCore/src/ordinary_normal_startup.cpp
    Sources/LatticeCore/src/ordinary_observed_process.cpp
)
target_include_directories(LatticeCore PUBLIC
    Sources/LatticeCore/include
)
# Parity with Package.swift's LatticeCore cxxSettings: without SQLITE_CORE,
# sqlite-vec.h (included by db.cpp) switches to sqlite3ext.h extension mode
# and every sqlite3_* call expands to the undefined sqlite3_api thunk table
# (bites on Linux, where libsqlite3-dev ships sqlite3ext.h).
target_compile_definitions(LatticeCore PRIVATE SQLITE_CORE)
if(LATTICE_SYNC_COMMIT_PROBE)
    target_compile_definitions(LatticeCore PUBLIC LATTICE_SYNC_COMMIT_PROBE)
endif()
target_link_libraries(LatticeCore PUBLIC
    SqliteVec
    SQLite::SQLite3
)

# =============================================================================
# LatticeSwiftCppBridge - Bridge layer (used by CAPI, despite "Swift" name)
# =============================================================================
add_library(LatticeSwiftCppBridge STATIC
    Sources/LatticeSwiftCppBridge/src/lattice.cpp
    Sources/LatticeSwiftCppBridge/src/bulk_mutation.cpp
    Sources/LatticeSwiftCppBridge/src/projection.cpp
    Sources/LatticeSwiftCppBridge/src/recovery_continuity.cpp
    Sources/LatticeSwiftCppBridge/src/recovery_export.cpp
    Sources/LatticeSwiftCppBridge/src/recovery_relay.cpp
    Sources/LatticeSwiftCppBridge/src/dynamic_object.cpp
    Sources/LatticeSwiftCppBridge/src/managed_object.cpp
    Sources/LatticeSwiftCppBridge/src/unmanaged_object.cpp
    Sources/LatticeSwiftCppBridge/src/list.cpp
)
target_include_directories(LatticeSwiftCppBridge PUBLIC
    Sources/LatticeSwiftCppBridge/include
)
target_link_libraries(LatticeSwiftCppBridge PUBLIC LatticeCore)

# For non-Apple platforms, we need to stub out Swift bridging macros
if(NOT APPLE)
    target_compile_definitions(LatticeSwiftCppBridge PUBLIC
        "SWIFT_SHARED_REFERENCE(retain,release)="
        "SWIFT_UNSAFE_REFERENCE="
        "SWIFT_CONFORMS_TO_PROTOCOL(x)="
        "SWIFT_NAME(x)="
        "SWIFT_COMPUTED_PROPERTY="
        "SWIFT_RETURNS_INDEPENDENT_VALUE="
    )
endif()


# The old CAPI target is intentionally not built in this two-helper image.
# Preserve every visibility property on the three actual retained product targets.
# Default-hidden visibility (docs/CAPI-STABILITY.md rule 8): only the
# LATTICE_EXPORT-annotated C surface leaves the shared library. Applied to the
# static libs it absorbs too — their default-visibility symbols would
# otherwise re-export through the .so.
set(_lattice_hidden_targets LatticeCore LatticeSwiftCppBridge SqliteVec)
if(TARGET sqlite3_bundled)
    list(APPEND _lattice_hidden_targets sqlite3_bundled)
endif()
foreach(tgt ${_lattice_hidden_targets})
    set_target_properties(${tgt} PROPERTIES
        C_VISIBILITY_PRESET hidden
        CXX_VISIBILITY_PRESET hidden
        VISIBILITY_INLINES_HIDDEN ON
    )
endforeach()

add_library(gtest STATIC Sources/GoogleTest/src/gtest-all.cc)
target_include_directories(gtest PUBLIC Sources/GoogleTest/include)
target_include_directories(gtest PRIVATE Sources/GoogleTest)
target_link_libraries(gtest PRIVATE pthread)

add_library(gtest_main STATIC Sources/GoogleTest/src/gtest_main.cc)
target_link_libraries(gtest_main PUBLIC gtest)

# =============================================================================
# LatticeTests - GoogleTest-based test suite

foreach(kind IN ITEMS V1 V1Partial)
  if(kind STREQUAL "V1")
    set(helper_source Tests/LatticeCoreTests/RecoveryReceiptCoverageControllerTests.cpp)
  else()
    set(helper_source Tests/LatticeCoreTests/RecoveryReceiptCoveragePartialControllerTests.cpp)
  endif()
  add_executable(LatticeReceipt${kind}Helper "${helper_source}")
  target_compile_definitions(LatticeReceipt${kind}Helper PRIVATE LATTICE_RECEIPT_RETENTION_LEGACY_WRITER=1)
  target_include_directories(LatticeReceipt${kind}Helper PRIVATE
    Tests/LatticeCoreTests Tests/LatticeCAPIHeaderCheck/include Sources/LatticeCAPI/include)
  target_link_libraries(LatticeReceipt${kind}Helper PRIVATE LatticeCore LatticeSwiftCppBridge gtest_main)
endforeach()
# No automatic discovery/process launch at link time. Inventories are separate owned stages.
