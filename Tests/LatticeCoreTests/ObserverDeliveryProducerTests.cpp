#include "TestHelpers.hpp"
#include <LatticeCore.hpp>
#include <lattice/detail/observer_delivery_values.hpp>
#include <array>
#include <atomic>

struct DeliveryBorrowedRow {
    int64_t sequence;
    std::string payload;
    std::vector<uint8_t> bytes;
    std::optional<int64_t> optional;
    lattice::timestamp_t timestamp;
    lattice::uuid_t uuid;
    lattice::geo_bounds bounds;
};
LATTICE_SCHEMA(DeliveryBorrowedRow,sequence,payload,bytes,optional,timestamp,uuid,bounds);

struct DeliveryTableConversionRow {int64_t sequence;};
struct DeliveryOpaqueTable {
    inline static std::atomic<unsigned> conversions{0};
    operator std::string_view()const {++conversions;return "DeliveryTableConversionRow";}
};
struct DeliveryOpaqueValue {
    inline static std::atomic<unsigned> conversions{0};
    operator int64_t()const {++conversions;return 1;}
};
struct DeliveryOpaqueValueRow {DeliveryOpaqueValue value;};
struct DeliveryManualConstructorRow {int64_t sequence;};
inline std::atomic<unsigned> delivery_manual_constructors{0};
namespace lattice {
template<>struct managed<DeliveryManualConstructorRow,void>:model_base {
    managed<int64_t> sequence;
    inline static constexpr std::string_view observer_delivery_table="DeliveryManualConstructorRow";
    inline static constexpr auto observer_delivery_members=std::tuple{
        detail::observer_delivery_member<DeliveryManualConstructorRow,int64_t>{
            "sequence",&DeliveryManualConstructorRow::sequence,{}}};
    inline static constexpr auto observer_delivery_outputs=std::tuple{
        detail::observer_delivery_output_member<managed<DeliveryManualConstructorRow>,int64_t>{
            "sequence",&managed<DeliveryManualConstructorRow>::sequence,{}}};
    managed(){++delivery_manual_constructors;}
};
template<>struct managed<DeliveryTableConversionRow,void> {
    inline static const DeliveryOpaqueTable observer_delivery_table{};
    inline static constexpr auto observer_delivery_members=std::tuple{
        detail::observer_delivery_member<DeliveryTableConversionRow,int64_t>{
            "sequence",&DeliveryTableConversionRow::sequence,{}}};
};
template<>struct managed<DeliveryOpaqueValueRow,void> {
    inline static constexpr std::string_view observer_delivery_table="DeliveryOpaqueValueRow";
    inline static constexpr auto observer_delivery_members=std::tuple{
        detail::observer_delivery_member<DeliveryOpaqueValueRow,DeliveryOpaqueValue>{
            "value",&DeliveryOpaqueValueRow::value,{}}};
};
}

namespace {
using Values=lattice::detail::observer_delivery_values;
const Values::value_view& column(Values::row_view row,std::string_view name) {
    return lattice::detail::observer_delivery_value_detail::field(row,name);
}
TEST(ObserverDeliveryProducer, GeneratedBuiltinCursorBorrowsBytesAndReusesOneScratch) {
    const auto time=lattice::timestamp_t(std::chrono::milliseconds(1234567));
    DeliveryBorrowedRow source{9,std::string(2048,'x'),{1,2,3},std::nullopt,time,
        lattice::uuid_t{},lattice::geo_bounds{1,2,3,4}};
    auto cursor=Values::cursor::typed(std::span<const DeliveryBorrowedRow>(&source,1));
    Values::field_buffer scratch;
    const auto first=cursor.read(0,scratch);
    EXPECT_EQ(first.table,"DeliveryBorrowedRow");
    ASSERT_EQ(first.fields.size(),10u);
    EXPECT_EQ(std::get<std::string_view>(column(first,"payload")).data(),source.payload.data());
    EXPECT_EQ(std::get<std::span<const uint8_t>>(column(first,"bytes")).data(),source.bytes.data());
    EXPECT_TRUE(std::holds_alternative<std::nullptr_t>(column(first,"optional")));
    EXPECT_DOUBLE_EQ(std::get<double>(column(first,"timestamp")),1234.567);
    EXPECT_EQ(std::get<std::string_view>(column(first,"uuid")).size(),36u);
    EXPECT_DOUBLE_EQ(std::get<double>(column(first,"bounds_minLat")),1);
    EXPECT_DOUBLE_EQ(std::get<double>(column(first,"bounds_maxLon")),4);
    const auto* fixed=first.fields.data();
    EXPECT_EQ(cursor.read(0,scratch).fields.data(),fixed);
    EXPECT_EQ(source.payload.size(),2048u);
}

TEST(ObserverDeliveryProducer, NonBuiltinTableConversionRefusesWithoutCallingIt) {
    DeliveryOpaqueTable::conversions=0;
    const DeliveryTableConversionRow source{1};
    EXPECT_THROW(Values::cursor::typed(std::span<const DeliveryTableConversionRow>(&source,1)),
                 std::invalid_argument);
    EXPECT_EQ(DeliveryOpaqueTable::conversions.load(),0u);
}

TEST(ObserverDeliveryProducer, NonBuiltinValueRefusesBeforeImplicitConversion) {
    DeliveryOpaqueValue::conversions=0;
    const DeliveryOpaqueValueRow source{};
    auto cursor=Values::cursor::typed(std::span<const DeliveryOpaqueValueRow>(&source,1));
    Values::field_buffer scratch;
    EXPECT_THROW(cursor.read(0,scratch),std::invalid_argument);
    EXPECT_EQ(DeliveryOpaqueValue::conversions.load(),0u);
}

TEST(ObserverDeliveryProducer, SameSpelledManualMetadataDoesNotIssueOutputConstructor) {
    delivery_manual_constructors=0;
    using Output=lattice::managed<DeliveryManualConstructorRow>;
    static_assert(lattice::detail::observer_delivery_value_detail::is_outputs<
        std::remove_cvref_t<decltype(Output::observer_delivery_outputs)>>::value);
    static_assert(!lattice::detail::observer_delivery_generated_schema<DeliveryManualConstructorRow>::value);
    std::vector<Output> outputs;
    EXPECT_THROW(lattice::detail::observer_delivery_value_detail::emplace_generated_output<
        DeliveryManualConstructorRow>(outputs),std::invalid_argument);
    EXPECT_TRUE(outputs.empty());
    EXPECT_EQ(delivery_manual_constructors.load(),0u);
}

TEST(ObserverDeliveryProducer, MaterializedBuiltinOutputUsesAdmittedImage) {
    lattice::managed<DeliveryBorrowedRow> output;
    const std::string admitted="the admitted value";
    std::array<Values::field_view,10> columns{{
        {"sequence",int64_t(41)},{"payload",std::string_view(admitted)},
        {"bytes",std::span<const uint8_t>{}},{"optional",int64_t(7)},
        {"timestamp",1.25},{"uuid",std::string_view("00000000-0000-0000-0000-000000000000")},
        {"bounds_minLat",1.0},{"bounds_maxLat",2.0},{"bounds_minLon",3.0},{"bounds_maxLon",4.0}}};
    std::apply([&](const auto&... member){
        (lattice::detail::observer_delivery_value_detail::materialize_output(
            output,Values::row_view{"DeliveryBorrowedRow",columns},member),...);
    },lattice::managed<DeliveryBorrowedRow>::observer_delivery_outputs);
    EXPECT_EQ(output.sequence.unmanaged_value,41);
    EXPECT_EQ(output.payload.unmanaged_value,admitted);
    EXPECT_EQ(output.optional.unmanaged_value,std::optional<int64_t>(7));
    EXPECT_EQ(output.timestamp.unmanaged_value,lattice::timestamp_t(std::chrono::milliseconds(1250)));
    EXPECT_DOUBLE_EQ(output.bounds.unmanaged_value.max_lon,4);
}
}

#ifndef __linux__
#include <lattice.hpp>
#include <observer_delivery_input.hpp>
namespace {
TEST(ObserverDeliveryProducer, SwiftFlatCursorReadsActualRawValuesWithoutRootCopies) {
    lattice::swift_dynamic_object raw;raw.table_name="DeliverySwift";
    lattice::property_descriptor descriptor;descriptor.name="payload";
    descriptor.type=lattice::column_type::text;descriptor.kind=lattice::property_kind::primitive;
    raw.properties.emplace("payload",descriptor);raw.values.emplace("payload",std::string(2048,'p'));
    auto input=lattice::detail::observer_delivery_swift_input::unmanaged({&raw,1});
    auto cursor=lattice::detail::observer_delivery_swift_input::cursor(input);
    Values::field_buffer scratch;const auto row=cursor.read(0,scratch);
    ASSERT_EQ(row.fields.size(),1u);
    EXPECT_EQ(std::get<std::string_view>(row.fields[0].contents).data(),
              std::get<std::string>(raw.values.at("payload")).data());
    EXPECT_FALSE(input.has_originals());
}

TEST(ObserverDeliveryProducer, SwiftUnsupportedDescriptorRefusesAtBorrowedPass) {
    lattice::swift_dynamic_object raw;raw.table_name="DeliverySwift";
    lattice::property_descriptor descriptor;descriptor.name="vector";
    descriptor.type=lattice::column_type::blob;descriptor.is_vector=true;
    raw.properties.emplace("vector",descriptor);raw.values.emplace("vector",std::vector<uint8_t>{1,2,3,4});
    auto input=lattice::detail::observer_delivery_swift_input::unmanaged({&raw,1});
    auto cursor=lattice::detail::observer_delivery_swift_input::cursor(input);
    Values::field_buffer scratch;
    EXPECT_THROW(cursor.read(0,scratch),std::invalid_argument);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(raw.values.at("vector")).size(),4u);
}
}
#endif
