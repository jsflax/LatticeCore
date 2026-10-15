#include "TestHelpers.hpp"
#include <lattice.hpp>          // swift_lattice, swift_lattice_ref (schemas with per-property flags)
#include <lattice/sync.hpp>
#include <lattice/ordinary_sync.hpp>
#include "../../Sources/LatticeCore/src/ordinary_source_upload.hpp"
#include "../../Sources/LatticeCore/src/ordinary_v2_original.hpp"
#include <cstring>

// ============================================================================
// property_descriptor::no_history (1.5.0) — "record that it changed, not what
// it became" for a streamed/growing column.
//
// Every UPDATE audit row used to carry the full new value of each changed
// column; a row rewritten ~10×/s while a 325 KB text grew left ~1.5 GB of
// history for ONE turn. With no_history the UPDATE trigger writes NULL for the
// column's value (it still gates the trigger and appears in changedFieldsNames,
// so live observers fire); INSERT and DELETE keep full values (once per row).
// Sync fills the CURRENT value in at upload/push time; a receiver never binds
// a null for such a column.
// ============================================================================

namespace {

lattice::swift_schema_entry make_schema(
    const std::string& table_name,
    std::initializer_list<std::pair<std::string, lattice::property_descriptor>> props) {
    lattice::swift_schema_entry entry;
    entry.table_name = table_name;
    for (const auto& [name, desc] : props) entry.properties[name] = desc;
    return entry;
}

lattice::property_descriptor text_prop(const std::string& name, bool no_history = false) {
    lattice::property_descriptor d;
    d.name = name; d.type = lattice::column_type::text; d.kind = lattice::property_kind::primitive;
    d.no_history = no_history;
    return d;
}

lattice::SchemaVector doc_schema(bool body_no_history) {
    return { make_schema("NhDoc", {{"title", text_prop("title")}, {"body", text_prop("body", body_no_history)}}) };
}

struct audit_row { std::string fields, names; };

audit_row last_update(lattice::database& db) {
    auto rows = db.query(
        "SELECT changedFields, changedFieldsNames FROM AuditLog "
        "WHERE tableName = 'NhDoc' AND operation = 'UPDATE' ORDER BY id DESC LIMIT 1", {});
    if (rows.empty()) return {};
    return {std::get<std::string>(rows[0].at("changedFields")),
            std::get<std::string>(rows[0].at("changedFieldsNames"))};
}

std::string meta(lattice::database& db, const std::string& key) {
    auto rows = db.query("SELECT value FROM _lattice_meta WHERE key = ?", {key});
    return rows.empty() ? std::string("<none>") : std::get<std::string>(rows[0].at("value"));
}

} // namespace

TEST(NoHistory, UpdateRecordsTheColumnButNotItsValue) {
    TempDB tmp{"nohistory_update"};
    auto schemas = doc_schema(/*body_no_history=*/true);
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();

    db.db().execute("INSERT INTO NhDoc(globalId, title, body) VALUES('d1', 'T', 'first body')");
    // INSERT keeps the value (a fresh peer needs it once).
    auto ins = db.db().query("SELECT changedFields FROM AuditLog WHERE tableName='NhDoc' AND operation='INSERT'", {});
    ASSERT_EQ(ins.size(), 1u);
    EXPECT_NE(std::get<std::string>(ins[0].at("changedFields")).find("first body"), std::string::npos);

    // UPDATE of the no_history column: named, valueless.
    db.db().execute("UPDATE NhDoc SET body = 'second body that is much longer' WHERE globalId = 'd1'");
    auto u = last_update(db.db());
    EXPECT_NE(u.names.find("\"body\""), std::string::npos) << u.names;
    EXPECT_NE(u.fields.find("\"body\":null"), std::string::npos) << u.fields;
    EXPECT_EQ(u.fields.find("second body"), std::string::npos) << "the value must not be in the audit row";

    // UPDATE of an ordinary column still carries its value; the untouched
    // no_history column is not named.
    db.db().execute("UPDATE NhDoc SET title = 'T2' WHERE globalId = 'd1'");
    u = last_update(db.db());
    EXPECT_NE(u.fields.find("\"title\":\"T2\""), std::string::npos) << u.fields;
    EXPECT_EQ(u.names.find("body"), std::string::npos) << u.names;

    // DELETE keeps the old values (observe(where:) evaluates membership on them).
    db.db().execute("DELETE FROM NhDoc WHERE globalId = 'd1'");
    auto del = db.db().query("SELECT changedFields FROM AuditLog WHERE tableName='NhDoc' AND operation='DELETE'", {});
    ASSERT_EQ(del.size(), 1u);
    EXPECT_NE(std::get<std::string>(del[0].at("changedFields")).find("second body"), std::string::npos);

    EXPECT_EQ(meta(db.db(), "trigger_flags:NhDoc"), "body");
}

TEST(NoHistory, GainingTheFlagRecreatesTriggersOnAnExistingStore) {
    TempDB tmp{"nohistory_upgrade"};
    {
        // "Older binary": body is an ordinary column; its value lands in history.
        auto schemas = doc_schema(/*body_no_history=*/false);
        auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
        auto& db = *ref->get();
        db.db().execute("INSERT INTO NhDoc(globalId, title, body) VALUES('d1', 'T', 'v1')");
        db.db().execute("UPDATE NhDoc SET body = 'v2' WHERE globalId = 'd1'");
        EXPECT_NE(last_update(db.db()).fields.find("\"body\":\"v2\""), std::string::npos);
        EXPECT_EQ(meta(db.db(), "trigger_flags:NhDoc"), "");
        db.close();
    }
    {
        // Same file reopened by a model that gained @NoHistory on body: no
        // column changed, so only the marker comparison can rebuild the trigger.
        auto schemas = doc_schema(/*body_no_history=*/true);
        auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
        auto& db = *ref->get();
        EXPECT_EQ(meta(db.db(), "trigger_flags:NhDoc"), "body") << "reopen must rewrite the marker";
        db.db().execute("UPDATE NhDoc SET body = 'v3' WHERE globalId = 'd1'");
        auto u = last_update(db.db());
        EXPECT_NE(u.fields.find("\"body\":null"), std::string::npos) << u.fields;
        EXPECT_EQ(u.fields.find("v3"), std::string::npos);
        db.close();
    }
}

TEST(NoHistory, UploadAndPushLateBindTheLiveValue) {
    TempDB tmp{"nohistory_latebind"};
    auto schemas = doc_schema(true);
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& db = *ref->get();
    db.db().execute("INSERT INTO NhDoc(globalId, title, body) VALUES('d1', 'T', 'v1')");
    for (int i = 2; i <= 5; ++i) {
        db.db().execute("UPDATE NhDoc SET body = ? WHERE globalId = 'd1'", {std::string("v") + std::to_string(i)});
    }

    // Upload path: every UPDATE entry ships the CURRENT value (latest at upload time).
    auto upload = lattice::query_audit_log_for_sync(db.db(), "wss:test", std::nullopt, 0, 100);
    int updates = 0;
    for (const auto& e : upload) {
        if (e.operation != "UPDATE") continue;
        ++updates;
        auto it = e.changed_fields.find("body");
        ASSERT_NE(it, e.changed_fields.end());
        ASSERT_TRUE(std::holds_alternative<std::string>(it->second.value));
        EXPECT_EQ(std::get<std::string>(it->second.value), "v5");
    }
    EXPECT_EQ(updates, 4);

    // Push/catch-up path: same fill.
    auto push = lattice::events_after(db.db(), std::nullopt);
    for (const auto& e : push) {
        if (e.operation != "UPDATE") continue;
        auto it = e.changed_fields.find("body");
        ASSERT_NE(it, e.changed_fields.end());
        EXPECT_EQ(std::get<std::string>(it->second.value), "v5");
    }

    // The per-row helper the Swift relay uses for rows it serializes itself.
    auto j = lattice::no_history_live_values_json(db.db(), "NhDoc", "d1", {"body"});
    EXPECT_NE(j.find("\"body\""), std::string::npos) << j;
    EXPECT_NE(j.find("\"v5\""), std::string::npos) << j;
    EXPECT_EQ(lattice::no_history_live_values_json(db.db(), "NhDoc", "missing", {"body"}), "{}");

    // Row gone: the column is DROPPED from the entry (never shipped as null);
    // its DELETE entry follows.
    db.db().execute("DELETE FROM NhDoc WHERE globalId = 'd1'");
    auto after = lattice::events_after(db.db(), std::nullopt);
    bool saw_delete = false;
    for (const auto& e : after) {
        if (e.operation == "UPDATE") {
            EXPECT_EQ(e.changed_fields.count("body"), 0u);
            EXPECT_EQ(std::find(e.changed_fields_names.begin(), e.changed_fields_names.end(), "body"),
                      e.changed_fields_names.end());
        }
        if (e.operation == "DELETE") saw_delete = true;
    }
    EXPECT_TRUE(saw_delete);
}

TEST(NoHistory, ReceiverNeverBindsANullForANoHistoryColumn) {
    TempDB tmp{"nohistory_receiver"};
    auto schemas = doc_schema(true);
    auto* ref = lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()), schemas);
    auto& peer = *ref->get();
    peer.db().execute("INSERT INTO NhDoc(globalId, title, body) VALUES('d1', 'T', 'kept')");

    // An UPDATE from an older sender that shipped the raw audit row: body named, value null.
    lattice::audit_log_entry e;
    e.global_id = "entry-1";
    e.table_name = "NhDoc";
    e.operation = "UPDATE";
    e.row_id = 1;
    e.global_row_id = "d1";
    e.changed_fields_names = {"title", "body"};
    e.changed_fields["title"] = lattice::any_property(std::string("T2"));
    e.changed_fields["body"] = lattice::any_property(nullptr);

    auto applied = lattice::apply_remote_changes(peer, {e});
    EXPECT_EQ(applied.size(), 1u) << "the entry must be applied (and acked), not wedged";
    auto row = peer.db().query("SELECT title, body FROM NhDoc WHERE globalId = 'd1'", {});
    ASSERT_EQ(row.size(), 1u);
    EXPECT_EQ(std::get<std::string>(row[0].at("title")), "T2");
    EXPECT_EQ(std::get<std::string>(row[0].at("body")), "kept") << "the null must not overwrite the value";
}


TEST(NoHistory, PassiveUploadAndDeliveryFactsDoNotEnterLegacyJSONOrOriginalEnvelope) {
    lattice::audit_log_entry entry;entry.id=3;entry.global_id=fake_uuid(7301);entry.table_name="NhDoc";
    entry.operation="UPDATE";entry.row_id=7;entry.global_row_id=fake_uuid(7302);entry.timestamp="123.25";
    entry.changed_fields.emplace("title",lattice::any_property(std::string("T")));entry.changed_fields_names={"title"};
    entry.original_identity=lattice::audit_original_identity{1,{"title"},std::string(64,'a')};
    const auto json=entry.to_json();size_t charged=0;
    const auto charge=[&](size_t n){charged+=n;};
    const auto encoded=lattice::detail::encode_ordinary_v2_original(entry,charge);
    const auto retained=lattice::detail::encode_ordinary_v2_retained_entry(entry,charge);
    auto original=std::make_shared<lattice::ordinary_upload_original>();original->fields={"body","title"};original->names={"title","body"};
    entry.upload_original=original;entry.ordinary_delivery_version=std::string(64,'b');
    EXPECT_EQ(entry.to_json(),json);
    EXPECT_EQ(lattice::detail::encode_ordinary_v2_original(entry,charge),encoded);
    EXPECT_EQ(lattice::detail::encode_ordinary_v2_retained_entry(entry,charge),retained);
    const auto decoded=lattice::audit_log_entry::from_json(entry.to_json());ASSERT_TRUE(decoded);
    EXPECT_EQ(decoded->original_identity,entry.original_identity);EXPECT_FALSE(decoded->upload_original);EXPECT_FALSE(decoded->ordinary_delivery_version);
    auto copied=entry;EXPECT_EQ(copied.upload_original.get(),original.get());EXPECT_EQ(copied.ordinary_delivery_version,entry.ordinary_delivery_version);
    EXPECT_GT(charged,0u);
}

TEST(NoHistory, OrdinaryQueryKeepsActualRawMembershipAfterTargetDeletion) {
    TempDB tmp{"ordinary_current_raw_membership"};auto schemas=doc_schema(true);
    auto* ref=lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()),schemas);auto& owner=*ref->get();
    owner.db().execute("INSERT INTO NhDoc(globalId,title,body) VALUES('d1','T','v1')");
    owner.db().execute("UPDATE NhDoc SET body='v2' WHERE globalId='d1'");
    owner.db().execute("DELETE FROM NhDoc WHERE globalId='d1'");
    const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    const auto input=lattice::detail::query_ordinary_upload_entries(owner.db(),"ordinary-current-observation",std::nullopt,0,64);
    ASSERT_EQ(input.size(),3u);const auto update=std::find_if(input.begin(),input.end(),[](const auto& e){return e.operation=="UPDATE";});ASSERT_NE(update,input.end());
    EXPECT_TRUE(update->changed_fields.empty());EXPECT_TRUE(update->changed_fields_names.empty());ASSERT_TRUE(update->upload_original);
    EXPECT_EQ(update->upload_original->fields,(std::vector<std::string>{"body"}));EXPECT_EQ(update->upload_original->names,(std::vector<std::string>{"body"}));
    EXPECT_EQ(update->ordinary_delivery_version,(std::optional<std::string>{"0"}));
    EXPECT_EQ(owner.db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    const auto legacy=lattice::query_audit_log_for_sync(owner.db(),"ordinary-current-observation",std::nullopt,0,64);
    ASSERT_EQ(legacy.size(),3u);const auto old_update=std::find_if(legacy.begin(),legacy.end(),[](const auto& e){return e.operation=="UPDATE";});ASSERT_NE(old_update,legacy.end());
    EXPECT_TRUE(old_update->changed_fields.empty());EXPECT_TRUE(old_update->changed_fields_names.empty());EXPECT_FALSE(old_update->upload_original);EXPECT_FALSE(old_update->ordinary_delivery_version);
}

TEST(NoHistory, OrdinaryQueryRefusesUnboundedAndActualNativeReadFailureWithoutMutation) {
    TempDB tmp{"ordinary_current_read_refusal"};auto schemas=doc_schema(true);
    auto* ref=lattice::swift_lattice_ref::create(lattice::swift_configuration(tmp.str()),schemas);auto& owner=*ref->get();
    owner.db().execute("INSERT INTO NhDoc(globalId,title,body) VALUES('d1','T','v1')");
    const auto before=owner.db().query("SELECT * FROM AuditLog ORDER BY id");
    EXPECT_THROW((void)lattice::detail::query_ordinary_upload_entries(owner.db(),"ordinary-current-refusal",std::nullopt,0,0),lattice::db_error);
    EXPECT_THROW((void)lattice::detail::query_ordinary_upload_entries(owner.db(),"ordinary-current-refusal",std::nullopt,0,lattice::ordinary_sync_limits::originals+1),lattice::db_error);
    auto* handle=owner.db().handle();struct clear_authorizer {sqlite3* handle;~clear_authorizer(){sqlite3_set_authorizer(handle,nullptr,nullptr);}} clear{handle};int denied=0;
    ASSERT_EQ(sqlite3_set_authorizer(handle,[](void* state,int action,const char* table,const char*,const char*,const char*)noexcept {
        if(action==SQLITE_READ&&table&&std::strcmp(table,"AuditLog")==0){++*static_cast<int*>(state);return SQLITE_DENY;}return SQLITE_OK;
    },&denied),SQLITE_OK);
    EXPECT_THROW((void)lattice::detail::query_ordinary_upload_entries(owner.db(),"ordinary-current-refusal",std::nullopt,0,64),lattice::db_error);
    ASSERT_EQ(sqlite3_set_authorizer(handle,nullptr,nullptr),SQLITE_OK);EXPECT_GT(denied,0);
    EXPECT_EQ(owner.db().query("SELECT * FROM AuditLog ORDER BY id"),before);
    EXPECT_FALSE(owner.db().is_in_transaction());
}
