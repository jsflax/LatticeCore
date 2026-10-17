#pragma once
#include <lattice/ordinary_sync.hpp>
#include <string_view>

namespace lattice::detail {
// A bounded top-level namespace discriminator, never a JSON validator or an
// authority grant. Escaped occurrences and mixed/duplicate envelope keys must
// be kept out of every legacy audit/ACK dispatcher.
bool reserved_ordinary_wire_frame(std::string_view) noexcept;
class ordinary_wire_access final {
    static ordinary_sync_export capture_impl(lattice_db&,std::vector<std::string> (*)(const void*),const void*);
public:
    static ordinary_sync_export capture(lattice_db&,const std::vector<std::string>&);
    static ordinary_sync_export capture(lattice_db&,const char* const*,size_t);
    static std::vector<std::string> apply(lattice_db&,const ordinary_sync_export&);
};
}
