// swift-tools-version: 6.2

import PackageDescription

// The behavioral C-ABI test target links the LatticeCAPI library. Built on
// every platform since C1 slice 3 fixed the Linux SwiftPM modulemap gap
// (docs/capi-gap-audit.md V1 — bridge + LatticeCAPIHeaderCheck maps); it is
// a separate product, so the Linux `swift run LatticeCoreTests` closure
// still never grows the CAPI library. Also built via CMake against the
// SHARED library (the true export surface) in the capi.yml CI leg.
let capiTestProducts: [Product] = [
    .executable(
        name: "LatticeCAPITests",
        targets: ["LatticeCAPITests"]
    ),
]
let capiTestTargets: [Target] = [
    .executableTarget(
        name: "LatticeCAPITests",
        dependencies: ["LatticeCAPI", "GoogleTest"],
        path: "Tests/LatticeCAPITests",
        cxxSettings: [
            .headerSearchPath("../../Sources/LatticeCAPI/include"),
            .headerSearchPath("../../Sources/GoogleTest/include"),
            // Test-side JSON verifier only (nlohmann): to_json output has
            // unspecified key order, so tests parse instead of string-compare.
            .headerSearchPath("../../Sources/LatticeCore/include"),
            .unsafeFlags(["-std=c++20"]),
            .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS])),
        ],
        linkerSettings: [
            .linkedLibrary("sqlite3")
        ]
    ),
]

let package = Package(
    name: "LatticeCore",
    platforms: [
        .macOS(.v14),
        .iOS(.v15)
    ],
    products: [
        .library(
            name: "LatticeCore",
            targets: ["LatticeCore"]
        ),
        .library(
            name: "LatticeSwiftCppBridge",
            targets: ["LatticeSwiftCppBridge"]
        ),
        // Explicit test support only; no ordinary library/runtime target depends on it.
        .library(name: "LatticeServerExportTestSupport", targets: ["LatticeServerExportTestSupport"]),
        .library(
            name: "LatticeSwiftModule",
            targets: ["LatticeSwiftModule"]
        ),
        .library(name: "LatticeInstallationChannel", targets: ["LatticeInstallationChannel"]),
        .library(
            name: "LatticeCAPI",
            targets: ["LatticeCAPI"]
        ),
        .executable(
            name: "LatticeCoreTests",
            targets: ["LatticeCoreTests"]
        ),
    ] + capiTestProducts,
    targets: [
        .target(
            name: "SqliteVec",
            path: "Sources/SqliteVec",
            sources: ["src/sqlite-vec.c"],
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("include"),
                .headerSearchPath("src"),
                .define("SQLITE_CORE"),
                .define("SQLITE_VEC_STATIC"),
                .define("SQLITE_VEC_ENABLE_NEON", .when(platforms: [.macOS, .iOS])),
                .define("SQLITE_VEC_EXPERIMENTAL_IVF_ENABLE"),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3"),
                .linkedLibrary("m", .when(platforms: [.linux])),
            ]
        ),
        .target(
            name: "LatticeCore",
            dependencies: ["SqliteVec"],
            path: "Sources/LatticeCore",
            // Included by their owning .cpp files, never standalone translation units.
            exclude: ["src/vendor/picosha2/LICENSE", "src/vendor/picosha2/README.md", "src/canonical_transfer_retention.inc", "src/canonical_durable_ready.inc", "src/recovery_producer_continuity_state.inc", "src/recovery_producer_continuity.inc",
                "src/ordinary_delivery_storage.inc",
                "src/ordinary_history_capacity.inc",
                "src/ordinary_history_capacity_declarations.inc",
                "src/ordinary_history_capture.inc",
                "src/ordinary_history_decode.inc",
                "src/ordinary_history_final_install.inc",
                "src/ordinary_history_lineage_relation_decode.inc",
                "src/ordinary_history_owner.inc",
                "src/ordinary_history_receive_session.inc",
                "src/ordinary_history_receive_storage.inc",
                "src/ordinary_history_stage.inc",
                "src/ordinary_managed_accepted_baseline.inc",
                "src/ordinary_managed_baseline_paging.inc",
                "src/ordinary_managed_cohort_link.inc",
                "src/ordinary_managed_contribution_ledger.inc",
                "src/ordinary_managed_history_receiver.inc",
                "src/ordinary_managed_history_state.inc",
                "src/ordinary_managed_namespace_restart.inc",
                "src/ordinary_managed_peer_delivery.inc",
                "src/ordinary_managed_peer_owner.inc",
                "src/ordinary_managed_peer_request.inc",
                "src/ordinary_managed_peer_retry.inc",
                "src/ordinary_managed_product_policy.inc",
                "src/ordinary_managed_profile.inc",
                "src/ordinary_managed_profile_pair.inc",
                "src/ordinary_managed_receiver_preservation.inc",
                "src/ordinary_managed_receiver_wss.inc",
                "src/ordinary_managed_upstream_capture.inc",
                "src/ordinary_managed_wss_binding.inc",
                "src/ordinary_namespace_provisional.inc",
                "src/ordinary_receiver_restart_access.inc",
                "src/ordinary_source_upload_storage.inc",
                "src/ordinary_group_target_catalog_program.inc",
                "src/ordinary_group_target_import_suspension_program.inc",
                "src/ordinary_group_target_projection_program.inc",
                "src/ordinary_group_target_receiving_owner.inc",
                "src/ordinary_group_target_row_import_program.inc",
                "src/ordinary_receiver_history_lifecycle_fixture.inc",
                "src/ordinary_target_abandoned_raw.inc",
                "src/ordinary_target_abandonment_database.inc",
                "src/ordinary_target_catalog_database.inc",
                "src/ordinary_target_descriptor_callbacks.inc",
                "src/ordinary_target_descriptor_ledger.inc",
                "src/ordinary_target_general_files.inc",
                "src/ordinary_target_history_chain_database.inc",
                "src/ordinary_target_history_header_database.inc",
                "src/ordinary_target_history_page_database.inc",
                "src/ordinary_target_history_page_reader.inc",
                "src/ordinary_target_history_projection_database.inc",
                "src/ordinary_target_history_row_import_database.inc",
                "src/ordinary_target_history_spool.inc",
                "src/ordinary_target_import_suspension_database.inc",
                "src/ordinary_target_reclamation_fixture.inc",
                "src/ordinary_target_staging_database.inc",
                "src/ordinary_target_terminal_call_evidence.inc",
                "src/ordinary_target_terminal_decoder_database.inc",
                "src/ordinary_target_terminal_outer_database.inc",
                "src/ordinary_target_terminal_outer_tail_database.inc",
                "src/ordinary_target_terminal_page_database.inc",
                "src/ordinary_target_terminal_page_tail_database.inc",
                "src/ordinary_target_terminal_phase_database.inc",
                "src/ordinary_target_terminal_phase_tail_database.inc",
                "src/ordinary_target_terminal_programme_database.inc",
                "src/ordinary_target_terminal_programmes.inc",
                "src/ordinary_target_terminal_raw_objects.inc",
                "src/ordinary_target_terminal_schema_roots.inc",
                "src/ordinary_target_terminal_scope.inc",
                "src/ordinary_target_terminal_scope_roots_database.inc",
                "src/ordinary_target_terminal_shell_database.inc",
                "src/ordinary_target_terminal_receiving_payloads.inc",
                "src/ordinary_target_terminal_collection.inc",
                "src/ordinary_target_terminal_scope_roots.inc",
                "src/ordinary_target_scope_environment_history.inc",
                "src/ordinary_target_terminal_static_database.inc",
                "src/ordinary_target_terminal_static_tail_database.inc",
                "src/ordinary_target_terminal_vfs.inc",
            ],
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .define("SQLITE_CORE"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS])),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3"),
                .linkedLibrary("m", .when(platforms: [.linux])),
            ]
        ),
        .target(name: "LatticeSwiftModule"),
        .target(
            name: "LatticeSwiftCppBridge",
            dependencies: ["LatticeCore", "LatticeSwiftModule"],
            path: "Sources/LatticeSwiftCppBridge",
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .headerSearchPath("../LatticeCore/include"),
                .define("SQLITE_VEC_EXPERIMENTAL_IVF_ENABLE"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS])),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        // SDK qualification tests use genuine producer enrollment through the
        // actual retained ref. Never a production route or authority issuer.
        .target(
            name: "LatticeServerExportTestSupport",
            dependencies: ["LatticeSwiftCppBridge"],
            path: "Sources/LatticeServerExportTestSupport",
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .headerSearchPath("../LatticeSwiftCppBridge/include"),
                .headerSearchPath("../LatticeCore/include"),
                .define("SQLITE_VEC_EXPERIMENTAL_IVF_ENABLE"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS])),
            ]
        ),
        .target(
            name: "LatticeInstallationChannel", dependencies: ["LatticeCore"],
            path: "Sources/LatticeInstallationChannel", sources: ["src/channel.cpp"],
            publicHeadersPath: "include",
            cxxSettings: [.headerSearchPath("include"), .define("SQLITE_CORE"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS]))],
            linkerSettings: [.linkedLibrary("sqlite3")]
        ),
        .target(
            name: "LatticeCAPI",
            dependencies: ["LatticeSwiftCppBridge"],
            path: "Sources/LatticeCAPI",
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .headerSearchPath("../LatticeSwiftCppBridge/include"),
                .headerSearchPath("../LatticeCore/include"),
                // The C API is plain C++ (never crosses Swift interop) and is
                // built around the heap-ref/retain-release model, so it always
                // uses the FRT layout regardless of the deployment floor.
                // CONSTRAINT: LatticeCAPI must never be linked into the same
                // image as a below-the-floor (LATTICE_HAS_FRT=0) build of the
                // bridge — the *_ref class layouts differ between the two
                // modes. Nothing does today (the Lattice Swift package links
                // only the bridge), and the Swift package needs no C API.
                .define("LATTICE_HAS_FRT", to: "1"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS]))
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .target(
            name: "GoogleTest",
            path: "Sources/GoogleTest",
            sources: ["src/gtest-all.cc", "src/gtest_main.cc"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("."),
                .unsafeFlags(["-std=c++20"]),
            ]
        ),
        // Compiles the public C ABI header (lattice.h) as pure C11
        // (docs/CAPI-STABILITY.md). Lives in its own C-only target because
        // SwiftPM applies a C++ target's -std=c++20 unsafeFlags to C sources
        // too. Only needs the header — the C API *library* is deliberately
        // NOT in the test closure (its SwiftPM build is broken on Linux; see
        // docs/capi-gap-audit.md V1).
        .target(
            name: "LatticeCAPIHeaderCheck",
            path: "Tests/LatticeCAPIHeaderCheck",
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("../../Sources/LatticeCAPI/include"),
                .unsafeFlags(["-std=c11"]),
            ]
        ),
        .executableTarget(
            name: "LatticeCoreTests",
            dependencies: ["LatticeCore", "LatticeSwiftCppBridge", "GoogleTest", "LatticeCAPIHeaderCheck"],
            path: "Tests/LatticeCoreTests",
            exclude: ["vendor", "LatticeCoreTests_legacy.cpp.bak", "IPCTests.hpp", "SyncIntegrationTests.hpp"],
            cxxSettings: [
                .headerSearchPath("vendor"),
                .headerSearchPath("../../Sources/LatticeSwiftCppBridge/include"),
                .headerSearchPath("../../Sources/GoogleTest/include"),
                .define("ASIO_STANDALONE"),
                .define("_WEBSOCKETPP_CPP11_THREAD_"),
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS])),
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        // LatticeSwiftCppTests disabled: requires Lattice module for CxxObject protocol
        // .testTarget(name: "LatticeSwiftCppTests",
        //             dependencies: ["LatticeSwiftCppBridge"],
        //             cxxSettings: [],
        //             swiftSettings: [.interoperabilityMode(.Cxx)])
    ] + capiTestTargets,
    cxxLanguageStandard: .cxx20
)
