#ifndef LatticeBridge_hpp
#define LatticeBridge_hpp

#include <lattice.hpp>

namespace lattice {

inline OptionalString string_to_optional(const std::string& s) {
    return s;
}

inline OptionalInt64 int64_to_optional(int64_t v) {
    return v;
}

using int64_t = int64_t;


using LinkListPtr = link_list*;

using StringVector = std::vector<std::string>;

#define get_field_(type, name) \
type get_##name(const std::string& name) const SWIFT_NAME(get_##name(named:)) { \
    return get_field<type>(name); \
}

// Type aliases for Swift C++ interop

using ManagedString = managed<std::string>;
using ManagedOptionalString = managed<std::optional<std::string>>;
using ManagedStringList = managed<std::vector<std::string>>;
using ManagedOptionalStringList = managed<std::optional<std::vector<std::string>>>;

using ManagedInt = managed<int64_t>;
using ManagedOptionalInt = managed<std::optional<int64_t>>;
using ManagedIntList = managed<std::vector<int>>;

using ManagedDouble = managed<double>;
using ManagedOptionalDouble = managed<std::optional<double>>;
using ManagedDoubleList = managed<std::vector<double>>;

using ManagedBool = managed<bool>;
using ManagedOptionalBool = managed<std::optional<bool>>;
using ManagedBoolList = managed<std::vector<bool>>;

using ManagedFloat = managed<float>;
using ManagedOptionalFloat = managed<std::optional<float>>;
using ManagedListFloat = managed<std::vector<float>>;

using ManagedTimestamp = managed<timestamp_t>;
using ManagedOptionalTimestamp = managed<std::optional<timestamp_t>>;
using ManagedListTimestamp = managed<std::optional<timestamp_t>>;

using ManagedData = managed<ByteVector>;
using ManagedOptionalData = managed<std::optional<ByteVector>>;
using ManagedListData = managed<std::vector<ByteVector>>;

using ManagedUUID = managed<uuid_t>;
using ManagedOptionalUUID = managed<std::optional<uuid_t>>;
using ManagedListUUID = managed<std::vector<uuid_t>>;

using ManagedGeobounds = managed<geo_bounds>;
using ManagedListGeobounds = managed<std::vector<geo_bounds>>;

using ManagedLink = managed<swift_dynamic_object*>;

using ManagedFloatList = managed<std::vector<float>>;
using ManagedLinkList = managed<std::vector<swift_dynamic_object*>>;
using ManagedModel = managed<swift_dynamic_object>;

using ManagedOptionalModel = managed<std::optional<swift_dynamic_object>>;
using ModelVector = std::vector<swift_dynamic_object>;
using DynamicObjectVector = std::vector<dynamic_object>;
using DynamicObjectPtrVector = std::vector<dynamic_object*>;
using DynamicObjectRefPtrVector = std::vector<dynamic_object_ref*>;

using OptionalManagedModel = std::optional<managed<swift_dynamic_object>>;

static OptionalManagedModel from_nonoptional(managed<swift_dynamic_object> o) {
    return o;
}
static ManagedModel from_optional(std::optional<managed<swift_dynamic_object>> o) {
    return *o;
}

template <typename T>
static std::optional<T> to_optional(const T& t);
template <>
std::optional<swift_configuration::SchemaPair> to_optional<swift_configuration::SchemaPair>(const swift_configuration::SchemaPair& p) {
    return p;
}
// Forward declarations
class swift_lattice;
class swift_lattice_ref;

} // namespace lattice

#endif /* LatticeBridge_hpp */
