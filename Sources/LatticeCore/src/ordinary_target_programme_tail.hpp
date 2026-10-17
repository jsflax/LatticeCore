#pragma once
#include "ordinary_owned_launch.hpp"
#include <memory>
#include <thread>

namespace lattice::detail {
class ordinary_group_target_schema_original;
class ordinary_target_staging_connection;
struct ordinary_target_programme_record;
// Actual first-member tail. It cannot create a programme or a Native result.
class ordinary_target_programme_tail final {
    friend class ordinary_group_target_schema_original;
    friend class ordinary_target_staging_connection;
    const ordinary_launch::process_identity process_=ordinary_launch::current_process();
    const std::thread::id thread_=std::this_thread::get_id();
    const void* const original_;
    const ordinary_group_target_schema_original* const schema_;
    const unsigned kind_;
    bool construction_returned_=false;
    std::shared_ptr<ordinary_target_programme_record> first_;
    ordinary_target_programme_tail(const void*,const ordinary_group_target_schema_original*,unsigned)noexcept;
    bool bind(const std::shared_ptr<ordinary_target_programme_record>&)noexcept;
public:
    ~ordinary_target_programme_tail()noexcept;
    ordinary_target_programme_tail(const ordinary_target_programme_tail&)=delete;
    ordinary_target_programme_tail& operator=(const ordinary_target_programme_tail&)=delete;
};
}
