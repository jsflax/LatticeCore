#pragma once
// Explicit target-only passive raw locator. The source/capture/generic receive
// BLOB recipe is unchanged. Coordinates are DATA, never an IO or SQL permit.
namespace lattice::detail {
struct ordinary_target_raw_history_storage_recipe final {
    static constexpr const char* page_name="_lattice_ordered_history_raw_page_v1";
    static constexpr const char* page="CREATE TABLE _lattice_ordered_history_raw_page_v1 (stage TEXT NOT NULL,page INTEGER NOT NULL,previous TEXT NOT NULL,digest TEXT NOT NULL,raw_offset INTEGER NOT NULL CHECK(raw_offset>=0),raw_length INTEGER NOT NULL CHECK(raw_length>0),rows INTEGER NOT NULL,known INTEGER NOT NULL,unknown INTEGER NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(stage,page))";
    static constexpr const char* seal_domain="ordinary-target-raw-page-v1";
};
}
