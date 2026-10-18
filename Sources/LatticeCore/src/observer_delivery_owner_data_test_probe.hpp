#pragma once
#include <cstdint>
namespace lattice::detail {
// Nil-default passive fixed DATA observation. It grants no owner/Native/SQL
// access and owns no callback target. Used only at the real prepared seam.
struct observer_delivery_owner_data_test_probe final {
    struct observation final {
        std::uint64_t creator=0;
        bool writer_absent=false,reader_absent=false,cross_process_reader_absent=false;
        bool catalog_present=false,scheduler_present=false,guard_present=false,callback_mutex_present=false;
        bool migration_callback_present=false,core_registries_empty=false;
    };
    static inline thread_local void(*after_data)(const observation&)=nullptr;
};
}
