#pragma once
#include <bridging.hpp>
#include <cstddef>
#include <cstdint>
#include <string_view>

namespace lattice {
class dynamic_object_ref;
// Swift value copies share one real one-use UPDATE token. This is deliberately
// a value handle on BOTH FRT and older platforms; its intrusive DATA original
// checks its creator before even touching inherited reference counts.
class swift_delivery_setter final {
    struct payload;payload* value_=nullptr;
    explicit swift_delivery_setter(payload*)noexcept;
    static bool retain(payload*)noexcept;
    static void release(payload*)noexcept;
    static swift_delivery_setter prepare(const dynamic_object_ref&,
        std::string_view,bool text,uint64_t maximum_bytes);
    void apply(const uint8_t*,int64_t,bool text)const;
    friend class dynamic_object_ref;
public:
    swift_delivery_setter()noexcept=default;
    swift_delivery_setter(const swift_delivery_setter&)noexcept;
    swift_delivery_setter(swift_delivery_setter&&)noexcept;
    swift_delivery_setter& operator=(const swift_delivery_setter&)noexcept;
    swift_delivery_setter& operator=(swift_delivery_setter&&)noexcept;
    ~swift_delivery_setter()noexcept;
    bool prepared()const noexcept SWIFT_NAME(isPrepared());
    void apply_text(const uint8_t*,int64_t)const SWIFT_NAME(applyText(_:count:));
    void apply_bytes(const uint8_t*,int64_t)const SWIFT_NAME(applyBytes(_:count:));
};
}
