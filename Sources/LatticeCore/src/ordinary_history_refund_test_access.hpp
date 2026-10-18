#pragma once
#include "ordinary_export.hpp"
#include <lattice/owned_catalog.hpp>

namespace lattice::detail {
// Source-private Native tests only; no Bridge/public Swift API is enrolled.
// Registration still runs the original actual Swift-catalog/owner issuer.
struct ordinary_history_refund_test_access {
    static ordinary_history_refund_observation observe(const ordinary_history_export& original) {
        return ordinary_export_access::observe_history_refund(original);
    }
    static ordinary_history_refund_observation observe(const ordinary_mounted_history_source& original) {
        return ordinary_export_access::observe_history_refund(original);
    }
    static bool has_observation(const ordinary_history_export& original)noexcept {
        return ordinary_export_access::history_has_refund_observation(original);
    }
    static ordinary_mounted_history_source register_actual_swift_mount(
        std::shared_ptr<lattice_db> original,const std::vector<std::string>& models) {
        return ordinary_export_access::register_mounted_history(std::move(original),models,{});
    }
    // Calls the real owned child's close API for the SQLITE_BUSY failure
    // control. It neither injects nor substitutes a close/retirement result.
    static lattice_close_result close_actual_child(const ordinary_history_export& original) {
        return ordinary_export_access::close_history_child_for_test(original);
    }
    static std::shared_ptr<database> borrow_actual_xproc(const ordinary_history_export& original) {
        return ordinary_export_access::history_child_xproc_for_test(original);
    }
    static catalog_stream_operation catalog_actual_child(const ordinary_history_export& original,
        const std::string& request,int64_t budget) {
        return ordinary_export_access::history_child_catalog_for_test(original,request,budget);
    }
};
}
