#pragma once
#include "ordinary_owned_launch.hpp"
#include <memory>
#include <cstdint>
#include <utility>
#include <string>
#include <vector>
#include <cstddef>

namespace lattice { class database; }
namespace lattice::detail {
class ordinary_target_staging_connection;
class ordinary_group_target_receiving_owner;
class ordinary_target_history_spool;
class ordinary_target_history_page_reader;
template<bool Receiving> class ordinary_resource_reservation;
struct ordinary_history_decoded_page;

// Coordinates extracted only from the SAME active Native phase's retained,
// sealed row. This DATA object grants no read or descriptor operation.
struct ordinary_target_history_read_span final {
    std::uint64_t page,offset,length,total;
};
class ordinary_target_history_page_reader final {
    friend class ordinary_target_history_spool;
    friend class ordinary_target_staging_connection;
    friend class ::lattice::database;
    struct state;
    const std::shared_ptr<state> state_;
    explicit ordinary_target_history_page_reader(std::shared_ptr<state>);
    static std::size_t retained_bytes(std::uint64_t);
    void require_current(const ::lattice::database&,const void*,ordinary_launch::deadline)const;
    const std::vector<std::uint8_t>& bytes(const ::lattice::database&,const void*,ordinary_launch::deadline)const;
    bool io_ending_known()const noexcept;
    bool input_ending_known()const noexcept;
    bool cleanup_known()const noexcept;
    void end_input(const ordinary_target_staging_connection&,const ordinary_group_target_receiving_owner&,
        const ::lattice::database&,const void*,ordinary_launch::deadline);
    bool release_input_after_cleanup()noexcept;
public:
    ~ordinary_target_history_page_reader();
    ordinary_target_history_page_reader(const ordinary_target_history_page_reader&)=delete;
    ordinary_target_history_page_reader& operator=(const ordinary_target_history_page_reader&)=delete;
};

// Private immutable durable IO original. Numeric coordinates alone are DATA;
// they cannot issue SQL work or advance the acknowledged/reusable raw cursor.
class ordinary_target_history_page_append final {
    friend class ordinary_target_history_spool;
    friend class ordinary_target_staging_connection;
    friend class ::lattice::database;
    struct state;
    const std::shared_ptr<state> state_;
    explicit ordinary_target_history_page_append(std::shared_ptr<state>);
    std::pair<std::uint64_t,std::uint64_t> require_durable_span(
        const ordinary_target_staging_connection&,const ordinary_group_target_receiving_owner&,
        const ordinary_history_decoded_page&,ordinary_launch::deadline)const;
    void require_locator_joined(const ordinary_target_staging_connection&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline)const;
    std::pair<std::uint64_t,std::uint64_t> require_committed_span(const ordinary_target_staging_connection&,
        const ordinary_group_target_receiving_owner&,std::uint64_t,const std::string&,const std::string&,ordinary_launch::deadline)const;
    bool cleanup_known()const noexcept;
    bool release_input_after_cleanup()noexcept;
public:
    ~ordinary_target_history_page_append();
    ordinary_target_history_page_append(const ordinary_target_history_page_append&)=delete;
    ordinary_target_history_page_append& operator=(const ordinary_target_history_page_append&)=delete;
};

// Separate passive raw extent. Only the actual Conn after its SAME receiver's
// Native header ending can issue it. Writes are limited to the SAME wire header
// at offset zero and one exact pending page. No public path/caller offset/quota
// or import API. A private matching SQL/VM/leave+input ending permits bounded
// pending-slot reuse; unknown/failed endings permanently retain the consumed tail.
class ordinary_target_history_spool final {
    friend class ordinary_target_staging_connection;
    friend class ordinary_target_history_page_append;
    friend class ordinary_target_history_page_reader;
    struct state;
    std::shared_ptr<state> state_;
    explicit ordinary_target_history_spool(std::shared_ptr<state>);
    static void prepare(const std::shared_ptr<ordinary_target_staging_connection>&,
        const std::shared_ptr<ordinary_group_target_receiving_owner>&,
        std::shared_ptr<ordinary_target_history_spool>&,ordinary_launch::deadline);
    void require_current(ordinary_launch::deadline)const;
    void persist_header(ordinary_launch::deadline);
    void require_header_ending(ordinary_launch::deadline)const;
    void require_header_observation(ordinary_launch::deadline)const;
    void require_header_ended_data()const;
    std::shared_ptr<const ordinary_target_history_page_append> append_page(ordinary_launch::deadline);
    void require_no_page_append(ordinary_launch::deadline)const;
    void join_page_locator(const ordinary_target_staging_connection&,const ordinary_group_target_receiving_owner&,
        const std::shared_ptr<const ordinary_target_history_page_append>&,ordinary_launch::deadline);
    void end_page_input(const ordinary_target_staging_connection&,const ordinary_group_target_receiving_owner&,ordinary_launch::deadline);
    void read_page(const ::lattice::database&,const void*,const void*,
        std::shared_ptr<ordinary_target_history_page_reader>&,ordinary_launch::deadline);
    void require_read_extent(const ordinary_target_history_read_span&,ordinary_launch::deadline)const;
    bool owns_leaf(const char*)const noexcept;
    void verify_member_census(int,bool,ordinary_launch::deadline)const;
    class abandoned_ending_scope final {
        friend class ordinary_target_staging_connection;
        friend class ordinary_target_history_spool;
        const ordinary_target_staging_connection* const connection_;
        const ordinary_target_history_spool* const spool_;
        const void* const cleanup_scope_;
        const std::uint64_t generation_;
        const void* const namespace_original_;
        abandoned_ending_scope(const ordinary_target_staging_connection& connection,
            const ordinary_target_history_spool& spool,const void* cleanup,std::uint64_t generation,const void* original)noexcept
            :connection_(&connection),spool_(&spool),cleanup_scope_(cleanup),generation_(generation),namespace_original_(original){}
        abandoned_ending_scope(const abandoned_ending_scope&)=delete;
        abandoned_ending_scope& operator=(const abandoned_ending_scope&)=delete;
    };
    bool abandonment_inputs_known()const noexcept;
    bool retire_abandoned(const abandoned_ending_scope&)noexcept;
    bool retire_unused(const ordinary_target_staging_connection&)noexcept;
public:
    ~ordinary_target_history_spool();
    ordinary_target_history_spool(const ordinary_target_history_spool&)=delete;
    ordinary_target_history_spool& operator=(const ordinary_target_history_spool&)=delete;
};
}
