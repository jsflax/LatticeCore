#pragma once
#include <lattice/network.hpp>
#include <atomic>
#include <chrono>
#include <limits>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace lattice {class lattice_db;class synchronizer_base;}
namespace lattice::detail {
class sync_callback_lifetime;
class ordinary_managed_product_credentials;
class ordinary_managed_wss_binding;
struct ordinary_received_cut;
class ordinary_history_receive_operation;
struct ordinary_receiver_test_access;struct ordinary_upload_ack;
// Issuance is private to the actual configured synchronizer. A URL, decoded
// source, frame buffer, or copied endpoint cannot construct this receiver.
class ordinary_receiver_session final {
    friend class ::lattice::synchronizer_base;
    friend struct ordinary_receiver_test_access;
    friend struct ordinary_received_cut;
    struct record;
    std::weak_ptr<lattice_db> owner_;
    std::weak_ptr<sync_callback_lifetime> lifetime_;
    const std::string channel_;
    const bool history_profile_;
    // Selector only. Actual receiver issuance, immutable Swift store catalog,
    // system-TLS attempt and managed route still supply every owning authority.
    const std::string model_profile_;
    const std::vector<std::string> expected_models_;
    std::shared_ptr<ordinary_managed_wss_binding> managed_binding_;
    mutable std::mutex mutex_;
    std::shared_ptr<record> current_;
    // A wake hint only. The actual record and physical lifetime remain the
    // authority; the pacer never acquires this leaf while holding its leaf.
    std::atomic<int64_t> deadline_millis_{INT64_MAX};
    ordinary_receiver_session(const std::shared_ptr<lattice_db>&,
        const std::shared_ptr<sync_callback_lifetime>&,std::string,const std::string&,
        const std::shared_ptr<ordinary_managed_product_credentials>& = {},const std::string& = {},bool history_profile=false,const std::string& model_profile={});
    void opened(const platform_transport_callbacks&,uint64_t,owned_platform_sync_transport&);
    void closed(const platform_transport_callbacks&,uint64_t);
    bool receive_model_offer(const platform_transport_callbacks&,uint64_t,std::string_view);
    void advance_model_negotiation(const platform_transport_callbacks&,uint64_t,owned_platform_sync_transport&);
    static std::vector<std::string> issued_model_scope(const std::shared_ptr<lattice_db>&,const std::string&);
    bool live(const std::shared_ptr<record>&)const;
    void begin_pre_header_admission(const std::shared_ptr<record>&);
    std::chrono::steady_clock::time_point deadline()const noexcept;
    bool expire();
    bool uploads_ready()const;
    // The returned completed cut retains its own independently charged pool
    // slot through scheduler admission, cancellation, commit and receipt send.
    bool receive(const platform_transport_callbacks&,uint64_t,const transport_message&,
        std::shared_ptr<ordinary_received_cut>&,std::vector<ordinary_upload_ack>* acknowledgment=nullptr,
        std::shared_ptr<ordinary_history_receive_operation>* history=nullptr);
    bool receive_history(const platform_transport_callbacks&,uint64_t,std::string_view,std::shared_ptr<ordinary_history_receive_operation>*);
    void install_history(const std::shared_ptr<ordinary_history_receive_operation>&,owned_platform_sync_transport&);
    void install(const std::shared_ptr<ordinary_received_cut>&,owned_platform_sync_transport&);
public:
    ~ordinary_receiver_session();
    ordinary_receiver_session(const ordinary_receiver_session&)=delete;
};
}
