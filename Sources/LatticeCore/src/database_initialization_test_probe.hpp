#pragma once
namespace lattice { class database; }
namespace lattice::detail {
struct database_initialization_test_probe {
    // Passive DATA only: no callback, admission, Native access or permission.
    static inline thread_local unsigned* read_admission_attempts=nullptr;
    enum class allocation_stage {none,request,transaction,episode,bootstrap_record,bootstrap_control,bootstrap_handoff};
    static inline thread_local allocation_stage fail_once=allocation_stage::none;
    static bool consume(allocation_stage expected)noexcept {
        if(fail_once!=expected)return false;
        fail_once=allocation_stage::none;return true;
    }
    static inline thread_local void(*after_pending)(database&)=nullptr;
    static inline thread_local void(*after_functions)(database&)=nullptr;
};
}
