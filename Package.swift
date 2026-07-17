// swift-tools-version: 6.2

import PackageDescription

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
        .library(
            name: "LatticeSwiftModule",
            targets: ["LatticeSwiftModule"]
        ),
        .library(
            name: "LatticeCAPI",
            targets: ["LatticeCAPI"]
        ),
        .executable(
            name: "LatticeCoreTests",
            targets: ["LatticeCoreTests"]
        ),
    ],
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
    ],
    cxxLanguageStandard: .cxx20
)
