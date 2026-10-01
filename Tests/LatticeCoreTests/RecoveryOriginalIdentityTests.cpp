#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/recovery_receipt_coverage.hpp"
#include <algorithm>
#include <limits>

namespace {
using namespace lattice;
using namespace lattice::detail;

class RecoveryOriginalIdentity : public ::testing::Test {
protected:
    const std::unordered_map<std::string,column_type> columns{
        {"body",column_type::text},{"count",column_type::integer},
        {"label",column_type::text},{"payload",column_type::blob},{"ratio",column_type::real}};
    const std::set<std::string> no_history{"body"};
    const std::string schema=std::string(64,'a');
    const recovery_producer_registration producer{"registered-device","90000000-0000-4000-8000-000000000001"};

    audit_log_entry update() const {
        audit_log_entry e;
        e.id=7;e.row_id=9;
        e.global_id="a0000000-0000-4000-8000-000000000002";
        e.global_row_id="b0000000-0000-4000-8000-000000000003";
        e.table_name="IdentityRow";e.operation="UPDATE";e.timestamp="1789819200.125";
        e.changed_fields_names={"label","body","count"};
        // This is the generated NoHistory UPDATE shape, before export fills
        // or omits the late-bound body projection.
        e.changed_fields={{"body",any_property(nullptr)},{"count",any_property(int64_t{42})},{"label",any_property("original")}};
        return e;
    }
    audit_original_identity identity(const audit_log_entry& e) const {
        return make_original_identity(e,columns,no_history,schema,producer);
    }
    audit_log_entry signed_update() const {
        auto e=update();e.original_identity=identity(e);return e;
    }
    void verify(const audit_log_entry& e) const {
        verify_original_identity(e,columns,no_history,schema,producer);
    }
    static void omit(audit_log_entry& e,const std::string& name) {
        e.changed_fields.erase(name);
        e.changed_fields_names.erase(std::remove(e.changed_fields_names.begin(),e.changed_fields_names.end(),name),e.changed_fields_names.end());
    }
};

TEST_F(RecoveryOriginalIdentity, NoHistoryUpdateMarkerSurvivesChangedAndOmittedProjection) {
    const auto generated=signed_update();ASSERT_TRUE(generated.original_identity);
    EXPECT_EQ(generated.original_identity->changed_fields_names,(std::vector<std::string>{"body","count","label"}));
    EXPECT_EQ(generated.original_identity->digest.size(),64u);
    EXPECT_NO_THROW(verify(generated));
    auto projected=generated;projected.changed_fields["body"]=any_property("current body at first export");
    EXPECT_EQ(identity(projected),*generated.original_identity);
    EXPECT_NO_THROW(verify(projected));
    projected.changed_fields["body"]=any_property("later current body");
    EXPECT_NO_THROW(verify(projected));
    omit(projected,"body");
    EXPECT_NE(identity(projected),*generated.original_identity);
    EXPECT_EQ(make_original_identity(projected,columns,no_history,schema,producer,&generated.original_identity->changed_fields_names),*generated.original_identity);
    EXPECT_NO_THROW(verify(projected));
}

TEST_F(RecoveryOriginalIdentity, NoHistoryOnlyUpdateMayCarryEmptyProjectionWithoutLosingOriginalNames) {
    auto e=update();e.changed_fields_names={"body"};e.changed_fields={{"body",any_property(nullptr)}};
    e.original_identity=identity(e);const auto saved=*e.original_identity;
    omit(e,"body");ASSERT_TRUE(e.changed_fields.empty());ASSERT_TRUE(e.changed_fields_names.empty());
    EXPECT_NO_THROW(verify(e));
    EXPECT_EQ(make_original_identity(e,columns,no_history,schema,producer,&saved.changed_fields_names),saved);
    e.original_identity->changed_fields_names.clear();
    EXPECT_THROW(verify(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, OrdinaryChangedValuesAndOmissionsRefuseTheSavedIdentity) {
    const auto original=signed_update();
    auto e=original;e.changed_fields["count"]=any_property(int64_t{43});
    EXPECT_THROW(verify(e),db_error);
    e=original;e.changed_fields["label"]=any_property("different ordinary value");
    EXPECT_THROW(verify(e),db_error);
    e=original;omit(e,"count");
    EXPECT_THROW(verify(e),db_error);
    e=original;e.changed_fields.erase("label");
    EXPECT_THROW(verify(e),db_error);
    e=original;e.changed_fields["count"]=any_property(42.0);
    EXPECT_THROW(verify(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, InsertAndDeleteBindRetainedNoHistoryValuesAsOrdinaryValues) {
    for(const auto* operation:{"INSERT","DELETE"}) {
        SCOPED_TRACE(operation);
        auto e=update();e.operation=operation;e.changed_fields["body"]=any_property("retained full value");
        e.changed_fields_names.push_back("payload");e.changed_fields["payload"]=any_property(std::vector<uint8_t>{0,1,255});
        e.original_identity=identity(e);const auto original=e;
        EXPECT_NO_THROW(verify(e));
        e.changed_fields["body"]=any_property("replacement full value");
        EXPECT_THROW(verify(e),db_error);
        e=original;omit(e,"body");
        EXPECT_THROW(verify(e),db_error);
        e=original;e.changed_fields["payload"]=any_property(std::vector<uint8_t>{0,2,255});
        EXPECT_THROW(verify(e),db_error);
    }
}

TEST_F(RecoveryOriginalIdentity, ProducerSchemaOriginalTargetOperationAndTimestampRemainBound) {
    const auto original=signed_update();
    auto other=producer;other.registration_id="another-registered-device";
    EXPECT_THROW(verify_original_identity(original,columns,no_history,schema,other),db_error);
    other=producer;other.incarnation="90000000-0000-4000-8000-000000000009";
    EXPECT_THROW(verify_original_identity(original,columns,no_history,schema,other),db_error);
    EXPECT_THROW(verify_original_identity(original,columns,no_history,std::string(64,'b'),producer),db_error);
    auto e=original;e.global_id="a0000000-0000-4000-8000-000000000004";
    EXPECT_THROW(verify(e),db_error);
    e=original;e.global_row_id="b0000000-0000-4000-8000-000000000005";
    EXPECT_THROW(verify(e),db_error);
    e=original;e.table_name="OtherIdentityRow";
    EXPECT_THROW(verify(e),db_error);
    e=original;e.timestamp="1789819200.126";
    EXPECT_THROW(verify(e),db_error);
    e=original;e.operation="DELETE";
    EXPECT_THROW(verify(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, FieldOrderAndUuidSpellingNormalizeButMetadataOrderIsStrict) {
    const auto original=signed_update();auto e=original;
    std::reverse(e.changed_fields_names.begin(),e.changed_fields_names.end());
    std::transform(e.global_id.begin(),e.global_id.end(),e.global_id.begin(),[](char c){return c>='a'&&c<='f'?static_cast<char>(c-'a'+'A'):c;});
    std::transform(e.global_row_id.begin(),e.global_row_id.end(),e.global_row_id.begin(),[](char c){return c>='a'&&c<='f'?static_cast<char>(c-'a'+'A'):c;});
    EXPECT_EQ(identity(e),*original.original_identity);
    EXPECT_NO_THROW(verify(e));
    std::reverse(e.original_identity->changed_fields_names.begin(),e.original_identity->changed_fields_names.end());
    EXPECT_THROW(verify(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, BlobHexAndBytesHaveOneTypedIdentityWithoutTextAliasing) {
    auto e=update();e.changed_fields_names={"payload"};e.changed_fields={{"payload",any_property("00aBff")}};
    e.original_identity=identity(e);const auto original=e;
    e.changed_fields["payload"]=any_property(std::vector<uint8_t>{0,171,255});
    EXPECT_EQ(identity(e),*original.original_identity);
    EXPECT_NO_THROW(verify(e));
    e.changed_fields["payload"]=any_property("00ABFF");
    EXPECT_NO_THROW(verify(e));
    e.changed_fields["payload"]=any_property("00abfe");
    EXPECT_THROW(verify(e),db_error);
    for(const auto* malformed:{"0","0g","0x00"}) {
        SCOPED_TRACE(malformed);e=original;e.changed_fields["payload"]=any_property(malformed);
        EXPECT_THROW(identity(e),db_error);
    }
    auto text_columns=columns;text_columns["payload"]=column_type::text;
    EXPECT_THROW(verify_original_identity(original,text_columns,no_history,schema,producer),db_error);
}

TEST_F(RecoveryOriginalIdentity, RealIntegerRepresentationsAndSignedZeroNormalize) {
    auto e=update();e.changed_fields_names={"ratio"};e.changed_fields={{"ratio",any_property(int64_t{42})}};
    e.original_identity=identity(e);e.changed_fields["ratio"]=any_property(42.0);
    EXPECT_NO_THROW(verify(e));
    e.changed_fields["ratio"]=any_property(42.5);
    EXPECT_THROW(verify(e),db_error);
    e.changed_fields["ratio"]=any_property(-0.0);e.original_identity=identity(e);
    e.changed_fields["ratio"]=any_property(0.0);
    EXPECT_NO_THROW(verify(e));
    e.changed_fields["ratio"]=any_property(int64_t{0});
    EXPECT_NO_THROW(verify(e));
    e.changed_fields["ratio"]=any_property(std::numeric_limits<double>::infinity());
    EXPECT_THROW(identity(e),db_error);
    e.changed_fields["ratio"]=any_property(std::numeric_limits<double>::quiet_NaN());
    EXPECT_THROW(identity(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, MissingMalformedAndDuplicateMetadataCannotVerify) {
    const auto original=signed_update();auto e=original;e.original_identity.reset();
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->version=2;
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->digest=std::string(64,'A');
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->digest.pop_back();
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->changed_fields_names.push_back("body");
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->changed_fields_names.assign(33,"body");
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->changed_fields_names={std::string(65,'n')};
    EXPECT_THROW(verify(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, ExtraOrdinaryNamesAndReservedOrUnknownFieldsCannotForgeProjection) {
    const auto original=signed_update();auto e=original;
    e.changed_fields_names.push_back("ratio");e.changed_fields["ratio"]=any_property(1.0);
    EXPECT_THROW(verify(e),db_error);
    e=original;e.original_identity->changed_fields_names.push_back("ratio");e.changed_fields["ratio"]=any_property(1.0);
    EXPECT_THROW(verify(e),db_error);
    e=original;e.changed_fields_names.push_back("count");
    EXPECT_THROW(identity(e),db_error);
    for(const auto* name:{"id","globalId","unknown"}) {
        SCOPED_TRACE(name);e=original;e.changed_fields_names.push_back(name);e.changed_fields[name]=any_property("forged");
        EXPECT_THROW(identity(e),db_error);
    }
    e=original;e.changed_fields["unknown"]=any_property("unlisted payload");
    EXPECT_THROW(identity(e),db_error);
}

TEST_F(RecoveryOriginalIdentity, GeneratedOperationShapeAndFiniteInputBoundsAreRequired) {
    const auto original=signed_update();auto e=original;e.synthesized=true;
    EXPECT_THROW(identity(e),db_error);
    e=original;e.operation="UPSERT";
    EXPECT_THROW(identity(e),db_error);
    e=original;e.global_id="not-a-global-uuid";
    EXPECT_THROW(identity(e),db_error);
    e=original;e.table_name=std::string(65,'t');
    EXPECT_THROW(identity(e),db_error);
    e=original;e.timestamp.clear();
    EXPECT_THROW(identity(e),db_error);
    e=original;e.timestamp=std::string("1\0hidden",8);
    EXPECT_THROW(identity(e),db_error);
    e=original;e.changed_fields_names.assign(33,"body");
    EXPECT_THROW(identity(e),db_error);
    e=original;e.changed_fields["label"]=any_property(std::string(4194304,'x'));
    EXPECT_THROW(identity(e),db_error);
    EXPECT_THROW(make_original_identity(original,columns,no_history,"short",producer),db_error);
}

TEST_F(RecoveryOriginalIdentity, RegistrationValidationUsesBoundedExplicitNormalizedIdentity) {
    auto value=producer;value.registration_id=std::string(256,'r');
    EXPECT_NO_THROW(value.validate());
    value.registration_id.push_back('r');
    EXPECT_THROW(value.validate(),db_error);
    value=producer;value.registration_id.clear();
    EXPECT_THROW(value.validate(),db_error);
    value=producer;value.registration_id=std::string("a\0b",3);
    EXPECT_THROW(value.validate(),db_error);
    value=producer;value.incarnation="A0000000-0000-4000-8000-000000000001";
    EXPECT_THROW(value.validate(),db_error);
    value=producer;value.incarnation="90000000-0000-4000-8000-00000000000";
    EXPECT_THROW(value.validate(),db_error);
}
}
