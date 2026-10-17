#pragma once
#include "ordinary_history_receive.hpp"
#include <string_view>
namespace lattice::detail {
class ordinary_group_target_receiving_owner;
class ordinary_receiver_history_expectation;
// Private forwarding of the actual codec. Its return values are bounded DATA;
// only the mounted receiving worker correlates its retained original inputs.
class ordinary_group_history_codec final {
    friend class ordinary_group_target_receiving_owner;
    static void offer(std::string_view,const std::string&);
    static ordinary_history_lineage_relation relation(std::string_view,
        const ordinary_receiver_history_expectation&,const std::string&,const std::string&,const std::string&);
    static ordinary_history_descriptor header(std::string_view,const ordinary_history_limits&);
    static ordinary_history_decoded_page page(std::string_view,const ordinary_history_descriptor&,const ordinary_history_progress&,const ordinary_history_limits&);
    static void match(const ordinary_history_lineage_relation&,const ordinary_history_descriptor&);
};
}
