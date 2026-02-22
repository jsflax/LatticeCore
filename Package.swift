// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "LatticeCore",
    platforms: [
        .macOS(.v14),
        .iOS(.v17)
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
            sources: ["src"],
            publicHeadersPath: "include",
            cSettings: [
                .headerSearchPath("include"),
                .define("SQLITE_CORE"),
                .define("SQLITE_VEC_STATIC"),
                .define("SQLITE_VEC_ENABLE_NEON", .when(platforms: [.macOS, .iOS])),
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
                .unsafeFlags(["-std=c++20"]),
                .unsafeFlags(["-fno-implicit-module-maps"], .when(platforms: [.macOS, .iOS]))
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .executableTarget(
            name: "LatticeCoreTests",
            dependencies: ["LatticeCore"],
            path: "Tests/LatticeCoreTests",
            exclude: ["vendor"],
            cxxSettings: [
                .headerSearchPath("vendor"),
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
