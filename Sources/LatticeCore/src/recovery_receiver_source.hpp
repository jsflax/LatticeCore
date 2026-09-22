#pragma once
#include <lattice/network.hpp>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>

namespace lattice { class lattice_db; class synchronizer_base; }
namespace lattice::detail {
class sync_callback_lifetime;
bool reserved_recovery_source_frame(std::string_view);
struct receiver_source_test_access;
// No public constructor or authority conversion. Only the actual synchronizer
// can start a describe on its owned, system-TLS verified physical attempt.
// This object is deliberately incomplete for READY, UNSENT and installation.
class receiver_source_binding {
    friend class ::lattice::synchronizer_base;
    friend struct receiver_source_test_access;
    struct policy;
    struct record;
    std::shared_ptr<const policy> policy_;
    std::weak_ptr<lattice_db> owner_;
    std::weak_ptr<sync_callback_lifetime> lifetime_;
    mutable std::mutex mutex_;
    std::shared_ptr<const record> current_;
    receiver_source_binding(const std::shared_ptr<lattice_db>&,
        const std::shared_ptr<sync_callback_lifetime>&,const std::string&,const std::string&);
    std::string dial_url()const;
    void opened(const platform_transport_callbacks&,uint64_t,owned_platform_sync_transport&);
    bool receive(const platform_transport_callbacks&,uint64_t,const transport_message&);
    void invalidate(const std::shared_ptr<const record>&);
    bool live(const std::shared_ptr<const record>&)const;
    bool described()const;
public:
    ~receiver_source_binding();
    receiver_source_binding(const receiver_source_binding&)=delete;
    static constexpr bool ready_authority=false;
    static constexpr bool install_authority=false;
    static constexpr bool automatic_recovery=false;
};
}
