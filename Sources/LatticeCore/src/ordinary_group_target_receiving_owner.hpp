#pragma once
#ifndef __EMSCRIPTEN__
#include <memory>
#include <exception>
#include <chrono>
#include <string>
namespace lattice { class database; class platform_transport_callbacks; struct transport_message; struct lattice_close_result; }
namespace lattice::detail {
template<bool Receiving> class ordinary_resource_reservation;
class ordinary_receiver_wss_request;
class ordinary_receiver_wss_access;
class ordinary_managed_wss_handle;
class ordinary_target_staging_connection;
class ordinary_unpublished_group_target;
class ordinary_target_staging_io;
class ordinary_group_target_schema_original;
struct ordinary_history_descriptor;
struct ordinary_history_decoded_page;
struct ordinary_history_progress;
// Private mounted receiving owner, not a generic target/path/SQL factory.
class ordinary_group_target_receiving_owner final {
    friend class ordinary_receiver_wss_access;
    friend class ordinary_target_staging_connection;
    friend class ::lattice::database;
    friend class ordinary_managed_wss_handle;
#if defined(LATTICE_MANAGED_TARGET_RECLAMATION_OWNER_FIXTURE)
    friend class ordinary_receiver_reclamation_fixture;
#endif
    struct state;
    std::shared_ptr<state> state_;
    explicit ordinary_group_target_receiving_owner(std::shared_ptr<state>);
    static void prepare(const std::shared_ptr<const ordinary_receiver_wss_request>&,
        std::shared_ptr<ordinary_group_target_receiving_owner>&);
    void start(const std::string&);
    void require_worker_current()const;
    std::shared_ptr<const ordinary_group_target_receiving_owner> terminal_abandonment_original(
        const ordinary_target_staging_connection&,const std::shared_ptr<const ordinary_unpublished_group_target>&,
        const std::shared_ptr<ordinary_target_staging_io>&)const;
    const ordinary_history_descriptor& require_header_original(const ordinary_target_staging_connection&)const;
    // SAME actual receive slot; lifetime custody only, never SQL/import authority.
    const std::shared_ptr<ordinary_resource_reservation<true>>& require_decoder_charge(
        const ordinary_target_staging_connection&)const;
    const std::string& require_header_channel(const ordinary_target_staging_connection&)const;
    const std::shared_ptr<ordinary_group_target_schema_original>& require_header_schema(const ordinary_target_staging_connection&)const;
    const ordinary_history_decoded_page& require_page_original(const ordinary_target_staging_connection&)const;
    std::shared_ptr<const ordinary_history_decoded_page> require_page_input_pin(const ordinary_target_staging_connection&)const;
    void end_page_input_pin(const ordinary_target_staging_connection&,std::shared_ptr<const ordinary_history_decoded_page>&)const;
    void receive_page(const platform_transport_callbacks&,const transport_message&);
    void send_page_progress(const platform_transport_callbacks&);
    void send_header_progress(const platform_transport_callbacks&);
    void prepare_on_worker();
    void opened_on_worker(const platform_transport_callbacks&);
    void message_on_worker(const platform_transport_callbacks&,const transport_message&);
    void enqueue_event(const platform_transport_callbacks&,const transport_message*,bool,bool,
        const std::string* = nullptr,int = 0,bool = false)noexcept;
    // Actual owned input captures only; no SQLite/files/worker/registry join.
    enum class terminal_input_retirement_result {not_held,captures_ended,retained_debt};
    terminal_input_retirement_result retire_terminal_inputs(const ordinary_target_staging_connection&)noexcept;
    bool terminal_inputs_ended(const ordinary_target_staging_connection&)const noexcept;
    bool terminal_decoder_charge_identity_matches(const ordinary_target_staging_connection&,const void*)const noexcept;
    void retire_on_worker()noexcept;
    void collect_on_worker()noexcept;
    void schedule_collection()noexcept;
    void finish_never_started()noexcept;
    void release_service_custody()noexcept;
    void remember_failure(std::exception_ptr)noexcept;
public:
    ~ordinary_group_target_receiving_owner();
    void request_close()noexcept;
    bool cleanup_complete()const noexcept;
    lattice_close_result close(std::chrono::steady_clock::time_point)noexcept;
};
}
#endif
