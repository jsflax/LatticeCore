#pragma once
// Exact existing passive history DDL, shared by its two actual private writers.
// These constant bytes grant no native/target/current/source-cut authority.
namespace lattice::detail {
struct ordinary_history_storage_recipe final {
    static constexpr const char* stage="CREATE TABLE _lattice_ordered_history_stage_v1 (stage TEXT PRIMARY KEY,role INTEGER NOT NULL CHECK(role IN (1,2)),source TEXT NOT NULL,epoch INTEGER NOT NULL,cut INTEGER NOT NULL,scope TEXT NOT NULL,schema TEXT NOT NULL,device TEXT NOT NULL,inode TEXT NOT NULL,state INTEGER NOT NULL CHECK(state IN (0,1)),header BLOB NOT NULL,rows INTEGER NOT NULL,known INTEGER NOT NULL,unknown INTEGER NOT NULL,pages INTEGER NOT NULL,bytes INTEGER NOT NULL,content TEXT NOT NULL,seal TEXT NOT NULL)";
    static constexpr const char* page="CREATE TABLE _lattice_ordered_history_page_v1 (stage TEXT NOT NULL,page INTEGER NOT NULL,previous TEXT NOT NULL,digest TEXT NOT NULL,payload BLOB NOT NULL,rows INTEGER NOT NULL,known INTEGER NOT NULL,unknown INTEGER NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(stage,page))";
    static constexpr const char* meta="CREATE TABLE _lattice_ordered_history_receive_v1 (stage TEXT PRIMARY KEY,channel TEXT NOT NULL,records_limit INTEGER NOT NULL,pages_limit INTEGER NOT NULL,page_limit INTEGER NOT NULL,records_per_page INTEGER NOT NULL,bytes_limit INTEGER NOT NULL,last_table INTEGER NOT NULL,seal TEXT NOT NULL)";
    static constexpr const char* key="CREATE TABLE _lattice_ordered_history_receive_key_v1 (stage TEXT NOT NULL,role INTEGER NOT NULL,model TEXT NOT NULL,gid TEXT COLLATE NOCASE NOT NULL,page INTEGER NOT NULL,ordinal INTEGER NOT NULL,seal TEXT NOT NULL,PRIMARY KEY(stage,role,model,gid),UNIQUE(stage,page,ordinal))";
};
}
