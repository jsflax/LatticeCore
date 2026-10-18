#pragma once
#include "ordinary_owned_launch.hpp"
#include <memory>
#include <thread>
namespace lattice::detail {
class ordinary_group_target_schema_original;
class ordinary_target_staging_connection;
struct ordinary_target_schema_roots_ending;
struct ordinary_target_schema_root_record;
// Original first member, not a factory or a completion setter.
class ordinary_target_schema_root_tail final {
    friend class ordinary_group_target_schema_original;
    friend class ordinary_target_staging_connection;
    const ordinary_launch::process_identity process_=ordinary_launch::current_process();
    const std::thread::id thread_=std::this_thread::get_id();
    const void* const original_;
    const unsigned kind_;
    bool construction_returned_=false;
    std::shared_ptr<ordinary_target_schema_root_record> first_;
    ordinary_target_schema_root_tail(const void*,unsigned)noexcept;
    bool bind(const std::shared_ptr<ordinary_target_schema_root_record>&)noexcept;
public:
    ~ordinary_target_schema_root_tail()noexcept;
    ordinary_target_schema_root_tail(const ordinary_target_schema_root_tail&)=delete;
    ordinary_target_schema_root_tail& operator=(const ordinary_target_schema_root_tail&)=delete;
};
}
