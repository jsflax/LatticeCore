#include "TestHelpers.hpp"
#if !defined(__EMSCRIPTEN__) && (defined(__APPLE__) || defined(__linux__))
#include "../../Sources/LatticeCore/src/wal_extent_vfs.hpp"
#include <sys/wait.h>
#include <unistd.h>

namespace {
using wal_access=lattice::detail::wal_extent_test_access;
constexpr sqlite3_int64 test_limit=4*1024*1024; // Test input, never a production default.
struct area {
    TempDB file{"wal-requested-extent"};
    area(){file.path=std::filesystem::canonical(file.path.parent_path())/file.path.filename();}
    std::string path()const{return file.str();}
    std::string wal()const{return path()+"-wal";}
    lattice::configuration selected(sqlite3_int64 limit=test_limit)const {
        lattice::configuration config(path());config.wal_extent_limit_bytes=limit;return config;
    }
};
int64_t scalar(lattice::database& db,const std::string& sql) {
    const auto rows=db.query(sql);if(rows.size()!=1||rows.front().size()!=1)throw std::runtime_error("one real scalar required");
    return std::get<int64_t>(rows.front().begin()->second);
}
void seed(lattice::database& db) {
    db.execute("PRAGMA wal_autocheckpoint=0");
    db.execute("CREATE TABLE wal_budget_row(id INTEGER PRIMARY KEY,globalId TEXT NOT NULL UNIQUE,value BLOB NOT NULL)");
    db.execute("INSERT INTO wal_budget_row(id,globalId,value) VALUES(1,?,zeroblob(65536))",{lattice::uuid_t::generate().to_string()});
}
}

TEST(WalExtentBudget, ForeignPinnedReaderRefusesBeforeWriteAndPreservesCommittedSnapshot) {
    area files;lattice::lattice_db owner(files.selected(),true);seed(owner.db());
    const auto empty=owner.db().wal_checkpoint(true,0);ASSERT_EQ(empty.rc,SQLITE_OK);ASSERT_EQ(empty.busy,0);
    lattice::database reader(files.path(),lattice::database::open_mode::read_only);
    reader.execute("BEGIN");ASSERT_EQ(scalar(reader,"SELECT value=zeroblob(65536) FROM wal_budget_row"),1);
    auto original=wal_access::original(owner.db());ASSERT_TRUE(original);
    bool full=false;int successes=0;
    for(int n=0;n<128;++n) {
        try{owner.db().execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1");++successes;}
        catch(const lattice::db_error&){full=true;break;}
    }
    ASSERT_TRUE(full);ASSERT_GT(successes,0);
    const auto proof=original->inspect();EXPECT_GT(proof.refused_growth,0u);EXPECT_EQ(proof.first_error,SQLITE_FULL);
    EXPECT_LE(proof.maximum_delegated_end,test_limit);EXPECT_LE(std::filesystem::file_size(files.wal()),static_cast<uint64_t>(test_limit));
    EXPECT_EQ(scalar(reader,"SELECT value=zeroblob(65536) FROM wal_budget_row"),1);
    EXPECT_EQ(scalar(owner.db(),"SELECT count(*) FROM wal_budget_row"),1);
    EXPECT_EQ(scalar(owner.db(),"SELECT value=zeroblob(65536) FROM wal_budget_row"),0);
    const auto blocked=owner.db().wal_checkpoint(true,0);EXPECT_EQ(blocked.busy,1);
    reader.execute("ROLLBACK");
    const auto reclaimed=owner.db().wal_checkpoint(true,0);ASSERT_EQ(reclaimed.rc,SQLITE_OK);ASSERT_EQ(reclaimed.busy,0);
    ASSERT_EQ(std::filesystem::file_size(files.wal()),0u);
    EXPECT_NO_THROW(owner.db().execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1"));
    EXPECT_EQ(original->inspect().first_error,SQLITE_FULL);
}

TEST(WalExtentBudget, UncommittedSpillRefusesBeforeGrowthAndPreservesPriorCommit) {
    area files;lattice::lattice_db owner(files.selected(),true);seed(owner.db());
    const auto empty=owner.db().wal_checkpoint(true,0);ASSERT_EQ(empty.busy,0);
    owner.db().execute("PRAGMA cache_size=8");owner.db().execute("BEGIN");
    bool full=false,flush_full=false;int inserted=0;
    for(int n=0;n<128;++n) {
        try{owner.db().execute("INSERT INTO wal_budget_row(globalId,value) VALUES(?,randomblob(65536))",{lattice::uuid_t::generate().to_string()});++inserted;}
        catch(const lattice::db_error&){full=true;break;}
        const int flushed=sqlite3_db_cacheflush(owner.db().handle());
        if(flushed==SQLITE_FULL){full=true;flush_full=true;break;}
        ASSERT_EQ(flushed,SQLITE_OK);
    }
    ASSERT_TRUE(full);ASSERT_TRUE(flush_full);EXPECT_GT(inserted,0);
    if(!sqlite3_get_autocommit(owner.db().handle()))owner.db().execute("ROLLBACK");
    EXPECT_EQ(scalar(owner.db(),"SELECT count(*) FROM wal_budget_row"),1);
    const auto proof=wal_access::original(owner.db())->inspect();EXPECT_GT(proof.refused_growth,0u);
    EXPECT_EQ(proof.first_error,SQLITE_FULL);EXPECT_LE(proof.maximum_delegated_end,test_limit);
    EXPECT_LE(std::filesystem::file_size(files.wal()),static_cast<uint64_t>(test_limit));
}

TEST(WalExtentBudget, SizeHintChunkAndProjectedTruncateCannotAllocateAroundGate) {
    area files;lattice::lattice_db owner(files.selected(),true);seed(owner.db());
    auto original=wal_access::original(owner.db());ASSERT_TRUE(original);
    const auto before=std::filesystem::file_size(files.wal());const auto observed=original->inspect();
    EXPECT_EQ(wal_access::file_call(owner.db(),SQLITE_FCNTL_SIZE_HINT,test_limit+1),SQLITE_FULL);
    EXPECT_EQ(wal_access::file_call(owner.db(),SQLITE_FCNTL_CHUNK_SIZE,test_limit*2),SQLITE_OK);
    EXPECT_EQ(wal_access::file_call(owner.db(),-1,test_limit+1),SQLITE_FULL);
    EXPECT_EQ(std::filesystem::file_size(files.wal()),before);
    const auto after=original->inspect();EXPECT_EQ(after.writes,observed.writes);EXPECT_EQ(after.truncates,observed.truncates);
    EXPECT_GE(after.refused_growth-observed.refused_growth,2u);
    EXPECT_NO_THROW(owner.db().execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1"));
    EXPECT_LE(std::filesystem::file_size(files.wal()),static_cast<uint64_t>(test_limit));
}

TEST(WalExtentBudget, ReopenObservesRealRetainedWalRatherThanResettingItsExtent) {
    area files;auto owner=std::make_unique<lattice::lattice_db>(files.selected(),true);seed(owner->db());
    lattice::database reader(files.path(),lattice::database::open_mode::read_only);
    reader.execute("BEGIN");ASSERT_EQ(scalar(reader,"SELECT count(*) FROM wal_budget_row"),1);
    owner->db().execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1");
    const auto original_size=std::filesystem::file_size(files.wal());ASSERT_GT(original_size,0u);owner.reset();
    const auto retained_size=std::filesystem::file_size(files.wal());
    ASSERT_GE(retained_size,original_size);ASSERT_LE(retained_size,static_cast<uint64_t>(test_limit));
    auto next=std::make_unique<lattice::lattice_db>(files.selected(),true);
    ASSERT_TRUE(wal_access::original(next->db()));
    EXPECT_EQ(scalar(next->db(),"SELECT count(*) FROM wal_budget_row"),1);
    EXPECT_GE(std::filesystem::file_size(files.wal()),retained_size);
    EXPECT_LE(std::filesystem::file_size(files.wal()),static_cast<uint64_t>(test_limit));reader.execute("ROLLBACK");
}

TEST(WalExtentBudget, ExistingOversizedWalRefusesWithoutTruncationOrDiscardingRows) {
    area files;lattice::database legacy(files.path());seed(legacy);
    lattice::database reader(files.path(),lattice::database::open_mode::read_only);
    reader.execute("BEGIN");ASSERT_EQ(scalar(reader,"SELECT count(*) FROM wal_budget_row"),1);
    for(int n=0;n<80;++n)legacy.execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1");
    const auto size=std::filesystem::file_size(files.wal());ASSERT_GT(size,static_cast<uint64_t>(test_limit));
    EXPECT_THROW({lattice::lattice_db refused(files.selected(),true);},lattice::db_error);
    EXPECT_EQ(std::filesystem::file_size(files.wal()),size);
    EXPECT_EQ(scalar(legacy,"SELECT count(*) FROM wal_budget_row"),1);
    EXPECT_EQ(scalar(reader,"SELECT value=zeroblob(65536) FROM wal_budget_row"),1);
    reader.execute("ROLLBACK");
}

TEST(WalExtentBudget, FirstBusyCloseRetainsOriginalEvenAfterActualZombieFilesClose) {
    area files;auto owner=std::make_unique<lattice::lattice_db>(files.selected(),true);seed(owner->db());
    auto original=wal_access::original(owner->db());ASSERT_TRUE(original);sqlite3_stmt* statement=nullptr;
    ASSERT_EQ(sqlite3_prepare_v2(owner->db().handle(),"SELECT value FROM wal_budget_row",-1,&statement,nullptr),SQLITE_OK);
    ASSERT_NE(statement,nullptr);EXPECT_TRUE(wal_access::attachment_guard_active(original));owner.reset();
    const auto before=original->inspect();EXPECT_TRUE(before.close_observed);EXPECT_EQ(before.checked_close,SQLITE_BUSY);
    EXPECT_EQ(before.fallback_close,SQLITE_OK);EXPECT_TRUE(before.registered);EXPECT_FALSE(before.ending_known);
    EXPECT_FALSE(wal_access::attachment_guard_active(original));
    ASSERT_EQ(sqlite3_finalize(statement),SQLITE_OK);
    const auto after=original->inspect();EXPECT_EQ(after.checked_close,SQLITE_BUSY);EXPECT_TRUE(after.registered);EXPECT_FALSE(after.ending_known);
    EXPECT_FALSE(wal_access::attachment_guard_active(original));
}

TEST(WalExtentBudget, ExplicitUnsupportedUriAttachmentAndInheritedWriterRefuse) {
    area files;auto uri=files.selected();uri.path="file:"+files.path()+"?vfs=unix";
    EXPECT_THROW({lattice::lattice_db refused(uri,true);},lattice::db_error);
    lattice::lattice_db owner(files.selected(),true);seed(owner.db());
    TempDB attached{"wal-budget-unsupported-attach"};
    EXPECT_THROW(owner.db().execute("ATTACH DATABASE ? AS other",{attached.str()}),lattice::db_error);
    EXPECT_THROW(owner.db().execute("ATTACH DATABASE ? AS escaped",{"file:"+attached.str()+"?vfs=unix"}),lattice::db_error);
    EXPECT_THROW(owner.db().execute("ATTACH DATABASE ':memory:' AS memory"),lattice::db_error);
    const pid_t child=::fork();ASSERT_GE(child,0);
    if(child==0){try{owner.db().execute("UPDATE wal_budget_row SET value=zeroblob(65536)");::_exit(2);}catch(...){::_exit(0);}}
    int status=0;ASSERT_EQ(::waitpid(child,&status,0),child);ASSERT_TRUE(WIFEXITED(status));EXPECT_EQ(WEXITSTATUS(status),0);
    EXPECT_NO_THROW(owner.db().execute("UPDATE wal_budget_row SET value=randomblob(65536) WHERE id=1"));
}

namespace {
std::string attached_alias(lattice::lattice_db& owner,const std::string& path) {
    const auto rows=owner.db().query("SELECT name FROM pragma_database_list WHERE seq>1 AND file=?",{path});
    if(rows.size()!=1)throw std::runtime_error("one actual attached native file required");
    return std::get<std::string>(rows.front().at("name"));
}
std::string quoted_alias(const std::string& alias) {
    std::string out="\"";for(char c:alias){out+=c;if(c=='\"')out+='\"';}return out+"\"";
}
}

TEST(WalExtentBudget, TypedNativeAttachmentUsesTargetCapWithPinnedReaderAndReadonlyView) {
    area recipient,target;lattice::lattice_db parent(recipient.selected(test_limit*2),true),child(target.selected(),true);
    seed(parent.db());seed(child.db());ASSERT_EQ(child.db().wal_checkpoint(true,0).busy,0);
    lattice::database held(target.path(),lattice::database::open_mode::read_only);
    held.execute("BEGIN");ASSERT_EQ(scalar(held,"SELECT value=zeroblob(65536) FROM wal_budget_row"),1);
    ASSERT_NO_THROW(parent.attach(child));const auto alias=attached_alias(parent,target.path());const auto selected=quoted_alias(alias);
    ASSERT_EQ(sqlite3_db_readonly(parent.read_db().handle(),alias.c_str()),1);
    auto original=wal_access::original(parent.db());ASSERT_TRUE(original);
    bool full=false;int successes=0;
    for(int n=0;n<128;++n){
        try{parent.db().execute("UPDATE "+selected+".wal_budget_row SET value=randomblob(65536) WHERE id=1");++successes;}
        catch(const lattice::db_error&){full=true;break;}
    }
    ASSERT_TRUE(full);ASSERT_GT(successes,0);
    const auto proof=original->inspect_member(target.path());ASSERT_EQ(proof.limit,test_limit);
    EXPECT_GT(proof.refused_growth,0u);EXPECT_EQ(proof.first_error,SQLITE_FULL);
    EXPECT_LE(proof.maximum_delegated_end,test_limit);EXPECT_LE(std::filesystem::file_size(target.wal()),static_cast<uint64_t>(test_limit));
    EXPECT_EQ(scalar(held,"SELECT value=zeroblob(65536) FROM wal_budget_row"),1);
    EXPECT_EQ(scalar(child.db(),"SELECT value=zeroblob(65536) FROM wal_budget_row"),0);
    EXPECT_THROW(parent.read_db().execute("UPDATE "+selected+".wal_budget_row SET value=zeroblob(65536) WHERE id=1"),lattice::db_error);
    EXPECT_EQ(scalar(child.db(),"SELECT value=zeroblob(65536) FROM wal_budget_row"),0);
    ASSERT_NO_THROW(parent.detach(child));EXPECT_TRUE(original->inspect_member(target.path()).ending_known);
    held.execute("ROLLBACK");ASSERT_EQ(child.db().wal_checkpoint(true,0).busy,0);
    EXPECT_EQ(std::filesystem::file_size(target.wal()),0u);
}

TEST(WalExtentBudget, TypedNativeCleanDetachAndReattachUseNewSerialAndKeepRawActionsRefused) {
    area recipient,target;lattice::lattice_db parent(recipient.selected(),true),child(target.selected(test_limit*2),true);
    seed(parent.db());seed(child.db());auto original=wal_access::original(parent.db());ASSERT_TRUE(original);
    ASSERT_NO_THROW(parent.attach(child));auto alias=attached_alias(parent,target.path());
    const auto first=original->inspect_member(target.path());ASSERT_GT(first.member_serial,0u);ASSERT_EQ(first.limit,test_limit*2);
    EXPECT_THROW(parent.db().execute("DETACH DATABASE "+quoted_alias(alias)),lattice::db_error);
    EXPECT_THROW(parent.db().execute("ATTACH DATABASE ? AS arbitrary",{target.path()}),lattice::db_error);
    EXPECT_THROW(parent.db().execute("ATTACH DATABASE ? AS overridden",{"file:"+target.path()+"?vfs=unix"}),lattice::db_error);
    EXPECT_EQ(scalar(parent.db(),"SELECT count(*) FROM "+quoted_alias(alias)+".wal_budget_row"),1);
    lattice::lattice_db changed(target.selected(),true); // Same real file, different newly requested target cap.
    EXPECT_THROW(parent.attach(changed),lattice::db_error);
    EXPECT_EQ(original->inspect_member(target.path()).member_serial,first.member_serial);
    EXPECT_EQ(original->inspect_member(target.path()).limit,test_limit*2);
    EXPECT_EQ(scalar(parent.db(),"SELECT count(*) FROM "+quoted_alias(alias)+".wal_budget_row"),1);
    EXPECT_NO_THROW(parent.attach(child)); // Exact original policy still admits idempotent reuse.
    ASSERT_NO_THROW(parent.detach(child));ASSERT_TRUE(original->inspect_member(target.path()).ending_known);
    ASSERT_NO_THROW(parent.attach(child));alias=attached_alias(parent,target.path());
    const auto second=original->inspect_member(target.path());EXPECT_GT(second.member_serial,first.member_serial);
    EXPECT_FALSE(second.ending_known);EXPECT_EQ(wal_access::original(parent.db()),original);
    EXPECT_NO_THROW(parent.db().execute("UPDATE "+quoted_alias(alias)+".wal_budget_row SET value=randomblob(65536) WHERE id=1"));
    EXPECT_EQ(scalar(child.db(),"SELECT value=zeroblob(65536) FROM wal_budget_row"),0);
    EXPECT_EQ(sqlite3_db_readonly(parent.read_db().handle(),alias.c_str()),1);
    ASSERT_NO_THROW(parent.reopen_read_db());
    EXPECT_EQ(sqlite3_db_readonly(parent.read_db().handle(),alias.c_str()),1);
    EXPECT_EQ(scalar(parent.read_db(),"SELECT count(*) FROM "+quoted_alias(alias)+".wal_budget_row"),1);
    ASSERT_NO_THROW(parent.reopen_write_db());
    auto reopened=wal_access::original(parent.db());ASSERT_TRUE(reopened);EXPECT_NE(reopened,original);
    EXPECT_EQ(reopened->inspect_member(target.path()).limit,test_limit*2);
    EXPECT_EQ(scalar(parent.db(),"SELECT count(*) FROM "+quoted_alias(alias)+".wal_budget_row"),1);
    EXPECT_EQ(sqlite3_db_readonly(parent.read_db().handle(),alias.c_str()),1);
    EXPECT_NO_THROW(parent.db().execute("UPDATE "+quoted_alias(alias)+".wal_budget_row SET value=randomblob(65536) WHERE id=1"));
    EXPECT_LE(std::filesystem::file_size(target.wal()),static_cast<uint64_t>(test_limit*2));
    ASSERT_NO_THROW(parent.detach(child));EXPECT_TRUE(reopened->inspect_member(target.path()).ending_known);
}

TEST(WalExtentBudget, TypedNativeAttachmentRefusesMissingPeerOrRecipientPolicyBeforePublication) {
    area bounded,legacy;lattice::lattice_db selected(bounded.selected(),true),unselected(legacy.path());
    seed(selected.db());seed(unselected.db());
    EXPECT_THROW(selected.attach(unselected),lattice::db_error);
    EXPECT_TRUE(selected.db().query("SELECT name FROM pragma_database_list WHERE seq>1").empty());
    EXPECT_THROW(unselected.attach(selected),lattice::db_error);
    EXPECT_TRUE(unselected.db().query("SELECT name FROM pragma_database_list WHERE seq>1").empty());
    EXPECT_EQ(scalar(selected.db(),"SELECT count(*) FROM wal_budget_row"),1);
    EXPECT_EQ(scalar(unselected.db(),"SELECT count(*) FROM wal_budget_row"),1);
}
#endif
