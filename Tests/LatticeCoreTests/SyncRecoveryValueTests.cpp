#include <gtest/gtest.h>
#include <lattice/sync.hpp>
#include "../../Sources/LatticeCore/src/sync_recovery_values.hpp"
#include <cmath>
#include <limits>

namespace sr = lattice::detail::sync_recovery;
namespace {
sr::value_limits budget() { return {65536, 32, 128, 8192, 16384}; }
void refuses(std::string_view s) { EXPECT_THROW((void)sr::decode_values(s, budget()), sr::protocol_error); }
}

TEST(SyncRecoveryValues, ExactSQLiteScalarsRoundTripIncludingNullEmptyAndEmbeddedNul) {
    sr::row_values values{{"null", nullptr}, {"emptyText", std::string{}},
        {"text", std::string("left\0right", 10)}, {"unicode", std::string("\xc3\xa9")},
        {"emptyBlob", std::vector<uint8_t>{}}, {"blob", std::vector<uint8_t>{0, 1, 127, 128, 255}},
        {"min", std::numeric_limits<int64_t>::min()}, {"max", std::numeric_limits<int64_t>::max()},
        {"real", std::nextafter(1.0, 2.0)}};
    const auto encoded = sr::encode_values(values, budget());
    EXPECT_EQ(sr::decode_values(encoded, budget()), values);
    EXPECT_EQ(sr::encode_values(sr::decode_values(encoded, budget()), budget()), encoded);
    EXPECT_EQ(sr::decode_values("{}", budget()), sr::row_values{});
}

TEST(SyncRecoveryValues, ProducerAnyPropertyScalarsUseTheSameStrictSnapshotGrammar) {
    lattice::audit_log_entry source;
    source.changed_fields.emplace("integer", lattice::any_property(int64_t{42}));
    source.changed_fields.emplace("real", lattice::any_property(42.0));
    source.changed_fields.emplace("null", lattice::any_property(nullptr));
    source.changed_fields.emplace("text", lattice::any_property(std::string("a\0b", 3)));
    source.changed_fields.emplace("blob", lattice::any_property(std::vector<uint8_t>{0, 255}));
    const auto decoded = sr::decode_values(source.changed_fields_to_json(), budget());
    EXPECT_EQ(decoded.at("integer"), lattice::column_value_t(int64_t{42}));
    EXPECT_EQ(decoded.at("real"), lattice::column_value_t(42.0));
    EXPECT_EQ(decoded.at("text"), lattice::column_value_t(std::string("a\0b", 3)));
    EXPECT_EQ(decoded.at("blob"), lattice::column_value_t(std::vector<uint8_t>{0, 255}));
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(decoded.at("null")));
}

TEST(SyncRecoveryValues, DoubleSignsSubnormalsAndBoundariesRemainExact) {
    for (double number : {0.0, -0.0, std::numeric_limits<double>::denorm_min(),
                          std::numeric_limits<double>::min(), std::numeric_limits<double>::max(),
                          -std::numeric_limits<double>::max(), std::nextafter(1.0, 2.0)}) {
        const auto decoded = sr::decode_values(sr::encode_values({{"n", number}}, budget()), budget());
        ASSERT_TRUE(std::holds_alternative<double>(decoded.at("n")));
        const double actual = std::get<double>(decoded.at("n"));
        EXPECT_EQ(actual, number); EXPECT_EQ(std::signbit(actual), std::signbit(number));
    }
    for (double number : {std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity(),
                          std::numeric_limits<double>::quiet_NaN()})
        EXPECT_THROW((void)sr::encode_values({{"n", number}}, budget()), sr::protocol_error);
    refuses(R"({"n":{"kind":7,"value":1e999}})");
}

TEST(SyncRecoveryValues, DuplicateKeysAndWrongShapesNeverBecomeAcceptedValues) {
    for (auto text : {R"({"x":{"kind":1,"value":1},"x":{"kind":1,"value":2}})",
         R"({"x":{"kind":1,"kind":2,"value":"s"}})",
         R"({"x":{"kind":1,"value":1,"value":2}})",
         R"({"x":{"kind":1}})", R"({"x":{"value":1}})",
         R"({"x":{"kind":1,"value":1,"extra":null}})", R"({"x":null})",
         R"({"x":{"kind":1,"value":{}}})", R"({"x":{"kind":1,"value":[]}})",
         "[]", "null", "{}{}", "{broken"}) refuses(text);
    EXPECT_EQ(std::get<int64_t>(sr::decode_values(R"({"x":{"value":1,"kind":1}})", budget()).at("x")), 1);
}

TEST(SyncRecoveryValues, KindsAreStrictAndIntegersNeverWrapOrBecomeBooleans) {
    for (auto text : {R"({"x":{"kind":0,"value":1}})", R"({"x":{"kind":3,"value":1.0}})",
         R"({"x":{"kind":5,"value":1.0}})", R"({"x":{"kind":8,"value":1}})",
         R"({"x":{"kind":1.0,"value":1}})", R"({"x":{"kind":true,"value":1}})",
         R"({"x":{"kind":1,"value":9223372036854775808}})",
         R"({"x":{"kind":1,"value":-9223372036854775809}})",
         R"({"x":{"kind":1,"value":1.0}})", R"({"x":{"kind":1,"value":true}})",
         R"({"x":{"kind":1,"value":null}})", R"({"x":{"kind":2,"value":1}})",
         R"({"x":{"kind":4,"value":""}})", R"({"x":{"kind":7,"value":1}})"}) refuses(text);
}

TEST(SyncRecoveryValues, BlobHexMustBeCompleteLowercaseBytes) {
    for (auto text : {R"({"x":{"kind":6,"value":"0"}})", R"({"x":{"kind":6,"value":"0g"}})",
         R"({"x":{"kind":6,"value":"FF"}})", R"({"x":{"kind":6,"value":" 0"}})",
         R"({"x":{"kind":6,"value":null}})", R"({"x":{"kind":6,"value":255}})"}) refuses(text);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(sr::decode_values(R"({"x":{"kind":6,"value":"00ff"}})", budget()).at("x")),
              (std::vector<uint8_t>{0,255}));
}

TEST(SyncRecoveryValues, RawFieldNameValueAndAggregateBudgetsHavePositiveBoundaryControls) {
    const sr::row_values values{{"a", std::string("abc")}, {"b", std::vector<uint8_t>{1,2,3}}};
    const auto encoded = sr::encode_values(values, budget());
    auto b = budget(); b.raw_bytes = encoded.size(); b.value_bytes = 3; b.decoded_bytes = 10;
    b.fields = 2; b.name_bytes = 1;
    EXPECT_EQ(sr::decode_values(encoded, b), values); EXPECT_EQ(sr::encode_values(values, b), encoded);
    auto smaller = b; --smaller.raw_bytes;
    EXPECT_THROW((void)sr::decode_values(encoded, smaller), sr::protocol_error);
    EXPECT_THROW((void)sr::encode_values(values, smaller), sr::protocol_error);
    smaller = b; --smaller.fields;
    EXPECT_THROW((void)sr::decode_values(encoded, smaller), sr::protocol_error);
    smaller = b; --smaller.value_bytes;
    EXPECT_THROW((void)sr::decode_values(encoded, smaller), sr::protocol_error);
    smaller = b; --smaller.decoded_bytes;
    EXPECT_THROW((void)sr::decode_values(encoded, smaller), sr::protocol_error);
    EXPECT_THROW((void)sr::encode_values(values, smaller), sr::protocol_error);
    EXPECT_THROW((void)sr::decode_values(R"({"ab":{"kind":4,"value":null}})", b), sr::protocol_error);
}

TEST(SyncRecoveryValues, EscapedBytesAndInvalidNamesCannotBypassLimits) {
    const sr::row_values values{{"x", std::string(100, '\0')}};
    const auto encoded = sr::encode_values(values, budget());
    auto b = budget(); b.raw_bytes = encoded.size() - 1; b.value_bytes = b.decoded_bytes = 101;
    EXPECT_THROW((void)sr::encode_values(values, b), sr::protocol_error);
    EXPECT_EQ(std::get<std::string>(sr::decode_values(R"({"x":{"kind":2,"value":"\u0000"}})", budget()).at("x")), std::string(1,'\0'));
    refuses(R"({"":{"kind":4,"value":null}})");
    refuses(R"({"a\u0000b":{"kind":4,"value":null}})");
    EXPECT_THROW((void)sr::encode_values({{std::string("a\0b", 3), nullptr}}, budget()), sr::protocol_error);
    EXPECT_THROW((void)sr::encode_values({{"x", std::string(1, static_cast<char>(0xff))}}, budget()), sr::protocol_error);
    refuses(std::string("{\"x\":{\"kind\":2,\"value\":\"") + static_cast<char>(0xff) + "\"}}");
}
