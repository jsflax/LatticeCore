// swift-tools-version: 6.2

import PackageDescription

let package = Package(
    name: "LatticeCpp",
    platforms: [
        .macOS(.v14),
        .iOS(.v17)
    ],
    products: [
        .library(
            name: "LatticeCpp",
            targets: ["LatticeCpp"]
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
            name: "LatticeCppTests",
            targets: ["LatticeCppTests"]
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
                .linkedLibrary("sqlite3")
            ]
        ),
        .target(
            name: "LatticeCpp",
            dependencies: ["SqliteVec"],
            path: "Sources/LatticeCpp",
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .unsafeFlags(["-std=c++20"])
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .target(name: "LatticeSwiftModule"),
        .target(
            name: "LatticeSwiftCppBridge",
            dependencies: ["LatticeCpp", "LatticeSwiftModule"],
            path: "Sources/LatticeSwiftCppBridge",
            sources: ["src"],
            publicHeadersPath: "include",
            cxxSettings: [
                .headerSearchPath("include"),
                .unsafeFlags(["-std=c++20"])
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
                .headerSearchPath("../LatticeCpp/include"),
                .unsafeFlags(["-std=c++20"])
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .executableTarget(
            name: "LatticeCppTests",
            dependencies: ["LatticeCpp"],
            path: "Tests/LatticeCppTests",
            exclude: ["vendor"],
            cxxSettings: [
                .headerSearchPath("vendor"),
                .define("ASIO_STANDALONE"),
                .define("_WEBSOCKETPP_CPP11_THREAD_"),
                .unsafeFlags(["-std=c++20"])
            ],
            linkerSettings: [
                .linkedLibrary("sqlite3")
            ]
        ),
        .testTarget(name: "LatticeSwiftCppTests",
                    dependencies: ["LatticeSwiftCppBridge"],
                    cxxSettings: [],
                    swiftSettings: [.interoperabilityMode(.Cxx)])
    ],
    cxxLanguageStandard: .cxx20
)
