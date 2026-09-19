cmake_minimum_required(VERSION 3.16)
project(CheckpointOwnershipQualification LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT IS_DIRECTORY "${CHECKPOINT_SOURCE_DIR}")
    message(FATAL_ERROR "CHECKPOINT_SOURCE_DIR must be the verified owned source tree")
endif()
add_subdirectory("${CHECKPOINT_SOURCE_DIR}" core EXCLUDE_FROM_ALL)
add_executable(CheckpointOwnershipQualificationTests
    "${CHECKPOINT_SOURCE_DIR}/Tests/LatticeCoreTests/CheckpointOwnershipTests.cpp")
target_link_libraries(CheckpointOwnershipQualificationTests PRIVATE LatticeCore gtest_main)
target_include_directories(CheckpointOwnershipQualificationTests PRIVATE
    "${CHECKPOINT_SOURCE_DIR}/Tests/LatticeCoreTests")
# No discovery command: the one explicit runner invocation retains the first
# native result. Unrelated targets/suites are configured but never built/run.
