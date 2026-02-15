cmake_minimum_required(VERSION 3.16)
project(LatticeCore VERSION 0.1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

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
)
target_compile_definitions(SqliteVec PRIVATE
    SQLITE_CORE
    SQLITE_VEC_STATIC
)
# Enable NEON on ARM (macOS/iOS)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
    target_compile_definitions(SqliteVec PRIVATE SQLITE_VEC_ENABLE_NEON)
endif()
target_link_libraries(SqliteVec PRIVATE SQLite::SQLite3)

# =============================================================================
# LatticeCore - Core C++ ORM implementation
# =============================================================================
add_library(LatticeCore STATIC
    Sources/LatticeCore/src/db.cpp
    Sources/LatticeCore/src/schema.cpp
    Sources/LatticeCore/src/sync.cpp
    Sources/LatticeCore/src/network.cpp
    Sources/LatticeCore/src/lattice.cpp
    Sources/LatticeCore/src/cross_process_notifier_darwin.cpp
    Sources/LatticeCore/src/cross_process_notifier_linux.cpp
    Sources/LatticeCore/src/cross_process_notifier_noop.cpp
)
target_include_directories(LatticeCore PUBLIC
    Sources/LatticeCore/include
)
target_link_libraries(LatticeCore PUBLIC
    SqliteVec
    SQLite::SQLite3
)

# =============================================================================
# LatticeSwiftCppBridge - Bridge layer (used by CAPI, despite "Swift" name)
# =============================================================================
add_library(LatticeSwiftCppBridge STATIC
    Sources/LatticeSwiftCppBridge/src/lattice.cpp
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

# =============================================================================
# LatticeCAPI - C API shared library (main output for Python/other bindings)
# =============================================================================
add_library(LatticeCAPI SHARED
    Sources/LatticeCAPI/src/lattice.cpp
)
target_include_directories(LatticeCAPI PUBLIC
    Sources/LatticeCAPI/include
)
target_link_libraries(LatticeCAPI PRIVATE LatticeSwiftCppBridge)

# Set output name and properties
set_target_properties(LatticeCAPI PROPERTIES
    OUTPUT_NAME "LatticeCAPI"
    VERSION ${PROJECT_VERSION}
    SOVERSION 0
)

# On macOS, set install name for proper dylib loading
if(APPLE)
    set_target_properties(LatticeCAPI PROPERTIES
        INSTALL_RPATH "@loader_path"
        BUILD_WITH_INSTALL_RPATH TRUE
    )
endif()

# =============================================================================
# Installation
# =============================================================================
install(TARGETS LatticeCAPI
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin  # For Windows DLLs
)
install(FILES Sources/LatticeCAPI/include/lattice.h
    DESTINATION include
)
