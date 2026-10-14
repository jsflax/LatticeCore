#pragma once
#if defined(LATTICE_MANAGED_ATTACHMENT_QUALIFICATION)
#include <cstddef>
#include <functional>
#include <memory>
namespace lattice { class lattice_db; }
namespace lattice::detail {
// Dedicated qualification image only. This observes a real completed managed
// ATTACH; it cannot supply an identity, statement, alias, profile or deadline.
// The public operation retains the copied shared callback before taking leaves
// and destroys that copy only after those leaves have unwound.
struct ordinary_cohort_attachment_test_hooks final {
    using callback=std::function<void(lattice_db& recipient,lattice_db& target,std::size_t handle_index)>;
    static inline thread_local std::shared_ptr<const callback> after_handle;
};
struct ordinary_cohort_attachment_qualification_result final {
    bool issued=false,throw_cleanup=false,writer_mismatch=false,reader_mismatch=false;
    bool closed_target=false,raw_denied=false,reentrant_denied=false,cleanup_complete=false;
    int failed_stage=0;
};
// Real positive fixture only: both arguments must already own actual managed
// profiles. This creates native children of those actual retained owners; no
// fixture profile factory, raw fd, replacement identity or caller grant exists.
ordinary_cohort_attachment_qualification_result qualify_managed_cohort_attachment(lattice_db&,lattice_db&);
class ordinary_cohort_attachment_observation final {
    const std::shared_ptr<const ordinary_cohort_attachment_test_hooks::callback> callback_;
    ordinary_cohort_attachment_observation* const previous_;
    static inline thread_local ordinary_cohort_attachment_observation* current_=nullptr;
public:
    ordinary_cohort_attachment_observation()
        :callback_(ordinary_cohort_attachment_test_hooks::after_handle),previous_(current_){current_=this;}
    ~ordinary_cohort_attachment_observation(){current_=previous_;}
    ordinary_cohort_attachment_observation(const ordinary_cohort_attachment_observation&)=delete;
    static void completed(lattice_db& recipient,lattice_db& target,std::size_t index){
        if(current_&&current_->callback_)(*current_->callback_)(recipient,target,index);
    }
};
}
#endif
