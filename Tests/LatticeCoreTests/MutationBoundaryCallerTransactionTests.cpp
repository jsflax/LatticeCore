// PROPOSED stronger operation-atomicity control; UNCOMPILED/UNRUN.
// Existing bulk tests and product bodies are untouched. See TEST-PLAN.json.
#include "TestHelpers.hpp"
#ifndef __linux__
#include <lattice.hpp>
#include <dynamic_object.hpp>
#include <memory>

namespace {
using namespace lattice;

SchemaVector boundary_schemas() {
    swift_schema_entry item;
    item.table_name = "BoundaryCallerItem";
    property_descriptor value;
    value.name = "value";
    value.type = column_type::integer;
    item.properties["value"] = value;
    return {item};
}

std::unique_ptr<swift_lattice_ref> boundary_owner(const swift_configuration& config,
                                                const SchemaVector& schemas) {
#if LATTICE_HAS_FRT
    return std::unique_ptr<swift_lattice_ref>(swift_lattice_ref::create(config, schemas));
#else
    return std::make_unique<swift_lattice_ref>(swift_lattice_ref::create(config, schemas));
#endif
}

dynamic_object boundary_item(const SchemaVector& schemas, int64_t value) {
    swift_dynamic_object source;
    source.table_name = "BoundaryCallerItem";
    source.properties = schemas[0].properties;
    source.values["value"] = value;
    return dynamic_object(source);
}
}

TEST(MutationBoundaryProposal, FailedBulkDoesNotEndCallerOwnedTransaction) {
    TempDB path{"boundary_caller_owned_bulk"};
    const auto schemas = boundary_schemas();
    auto owner = boundary_owner(swift_configuration(path.str()), schemas);
    ASSERT_TRUE(owner);
    ASSERT_NE(owner->get(), nullptr);
    auto& core = *owner->get();
    core.db().execute("CREATE TRIGGER boundary_late BEFORE INSERT ON BoundaryCallerItem "
                      "WHEN NEW.value = 2 AND EXISTS(SELECT 1 FROM BoundaryCallerItem WHERE value = 1) "
                      "BEGIN SELECT RAISE(ABORT, 'boundary-late-real-prefix'); END");
    core.begin_transaction();
    bool caller_committed = false;
    // Cleanup belongs to this test only while its own original transaction lives.
    struct EndCaller {
        lattice::swift_lattice& core;
        bool& committed;
        ~EndCaller() { if (!committed && core.db().is_in_transaction()) { try { core.rollback(); } catch (...) {} } }
    } ending{core, caller_committed};
    core.db().execute("INSERT INTO BoundaryCallerItem(globalId,value) VALUES('caller-before',41)");
    auto one = boundary_item(schemas, 1);
    auto two = boundary_item(schemas, 2);
    std::vector<dynamic_object*> offered{&one, &two};
    std::string first_error;
    try { core.add_bulk(offered); }
    catch (const std::exception& error) { first_error = error.what(); }
    EXPECT_NE(first_error.find("boundary-late-real-prefix"), std::string::npos);
    // These are explicitly PROPOSED stronger semantics, not claimed existing API.
    ASSERT_TRUE(core.db().is_in_transaction());
    const auto rows = core.db().query("SELECT value FROM BoundaryCallerItem ORDER BY value");
    ASSERT_EQ(rows.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(rows[0].at("value")), 41);
    EXPECT_FALSE(one.lattice);
    EXPECT_FALSE(two.lattice);
    core.commit();
    caller_committed = true;
    EXPECT_FALSE(core.db().is_in_transaction());
    const auto persisted = core.db().query("SELECT value FROM BoundaryCallerItem");
    ASSERT_EQ(persisted.size(), 1u);
    EXPECT_EQ(std::get<int64_t>(persisted[0].at("value")), 41);
}
#endif
