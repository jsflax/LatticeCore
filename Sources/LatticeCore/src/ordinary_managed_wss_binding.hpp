#pragma once
#include <lattice/network.hpp>
#include <atomic>
#include <array>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>

namespace lattice {class lattice_db;}
namespace lattice::detail {
class ordinary_managed_product_credentials;
class ordinary_managed_profile;
class ordinary_receiver_wss_request;
class ordinary_receiver_history_expectation;
class ordinary_receiver_session;
class ordinary_export_access;
class sync_callback_lifetime;
struct ordinary_managed_wss_epoch;
// Output of the actual accepted cut, never decoded transport authority. The
// constructor is closed and only the authenticated receiver publishes it after
// apply_session returned a clean owned COMMIT, including off-lock delivery.
class ordinary_managed_wss_receipt final {
    friend class ordinary_managed_wss_binding;
    const std::shared_ptr<ordinary_managed_wss_epoch> epoch_;
    std::atomic<bool> retired_{false};
    ordinary_managed_wss_receipt(std::shared_ptr<ordinary_managed_wss_epoch>,
        std::string,std::string,std::string,std::string,std::string,std::int64_t,std::int64_t);
public:
    const std::string channel,scope,source,schema_digest,receipt_digest;
    const std::int64_t source_epoch,cut;
    bool current()const noexcept;
};
struct ordinary_managed_wss_acceptance final {
    std::string scope,source,schema_digest,receipt_digest;
    std::int64_t epoch=0,cut=0;
};
// Separate application and owned sync-child wrappers must share the exact
// privately issued enrolled profile. Matching a path/inode or credential text
// alone cannot register a managed owner. No public factory/DTO constructor.
class ordinary_managed_wss_binding final {
    friend class ordinary_receiver_session;
    friend class ordinary_history_receive_state;
    friend class ordinary_managed_product_credentials;
    struct reservation;
    struct retained;
    struct dispose_retained {void operator()(retained*)const noexcept;};
    std::unique_ptr<retained,dispose_retained> held_; // guards construction and teardown
    const std::shared_ptr<reservation>& reservation_;
    const std::uint64_t creator_;
    const std::shared_ptr<ordinary_managed_product_credentials>& credentials_;
    const std::shared_ptr<const ordinary_managed_profile>& profile_;
    const std::shared_ptr<const ordinary_receiver_wss_request>& receiver_;
    const std::shared_ptr<lattice_db>& owner_;
    const std::shared_ptr<sync_callback_lifetime>& lifetime_;
    const std::string url_,channel_;
    const std::shared_ptr<std::atomic<bool>>& live_;
    std::mutex& leaf_;
    std::shared_ptr<ordinary_managed_wss_epoch>& epoch_;
    std::array<std::shared_ptr<ordinary_managed_wss_receipt>,16>& receipts_;
    ordinary_managed_wss_binding(const std::shared_ptr<reservation>&,const std::shared_ptr<ordinary_managed_product_credentials>&,
        const std::shared_ptr<const ordinary_managed_profile>&,const std::shared_ptr<lattice_db>&,
        const std::shared_ptr<sync_callback_lifetime>&,std::string,std::string,
        std::shared_ptr<const ordinary_receiver_wss_request> = {});
    static std::shared_ptr<ordinary_managed_wss_binding> attach(
        const std::shared_ptr<ordinary_managed_product_credentials>&,
        const std::shared_ptr<lattice_db>&,const std::shared_ptr<sync_callback_lifetime>&,
        const std::string&,const std::string&,const std::string&);
    bool configuration_current()const noexcept;
    // Borrow only from this retained current receive-only native issuer.
    const ordinary_receiver_history_expectation& expected_history_relation()const;
    bool has_receiver_history_expectation()const noexcept;
    void opened(const platform_transport_callbacks&,std::uint64_t);
    void closed(const platform_transport_callbacks&,std::uint64_t)noexcept;
    void retire()noexcept;
    void accepted(const platform_transport_callbacks&,std::uint64_t,ordinary_managed_wss_acceptance&&);
    std::shared_ptr<const ordinary_managed_wss_receipt> receipt(const std::string&);
    std::shared_ptr<const ordinary_managed_wss_receipt> try_receipt(const std::string&);
public:
    ~ordinary_managed_wss_binding();
    ordinary_managed_wss_binding(const ordinary_managed_wss_binding&)=delete;
};
}
