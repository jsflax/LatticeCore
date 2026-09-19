#include "recovery_writer_access.hpp"

namespace lattice::detail {

struct recovery_writer_access::frame {
    lattice_db& owner;
    database& writer;
    database::sync_apply_chunk_state& settlement;
    frame* previous;
    frame(lattice_db& o, database& w, database::sync_apply_chunk_state& s)
        : owner(o), writer(w), settlement(s), previous(current_) { current_ = this; }
    ~frame() { current_ = previous; }
};
thread_local recovery_writer_access::frame* recovery_writer_access::current_ = nullptr;

database* recovery_writer_access::active_writer(lattice_db& owner) {
    for (auto* f = current_; f; f = f->previous) {
        if (&f->owner != &owner) continue;
        // A consumed scope must not fall through to public ownership of a
        // successor. Logical close does not revoke this admitted physical turn.
        auto* h = f->writer.internal_handle();
        if (f->settlement.state != database::sync_apply_chunk_state::phase::active ||
            sqlite3_get_autocommit(h) != 0 || sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE)
            return nullptr;
        return &f->writer;
    }
    // Refuse another thread before any SQLite call: the admitted maintenance
    // frame may hold that connection while waiting for this caller to finish.
    if (owner.is_closed() || owner.txn_owner_thread_.load(std::memory_order_acquire) != std::this_thread::get_id() ||
        !owner.owns_write_transaction()) return nullptr;
    auto& writer = owner.db();
    auto* h = writer.internal_handle();
    if (writer.is_closed() || sqlite3_get_autocommit(h) != 0 ||
        sqlite3_txn_state(h, "main") != SQLITE_TXN_WRITE) return nullptr;
    return &writer;
}

recovery_install_result recovery_writer_access::install(std::shared_ptr<lattice_db> owner,
    const std::function<void(database&)>& body) {
    return install_impl(std::move(owner), body, {});
}

recovery_install_result recovery_writer_access::install_impl(std::shared_ptr<lattice_db> owner,
    const std::function<void(database&)>& body, const std::function<void()>& after_unlock) {
    recovery_install_result result;
    std::shared_ptr<database> writer;
    lattice_db::recovery_commit_batch batch;
    using phase = database::sync_apply_chunk_state::phase;
    database::sync_apply_chunk_state settlement;
    try {
        if (!owner || !body) throw db_error("recovery install requires an owning store and body");
        uint64_t revision;
        {
            std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
            if (owner->closed_.load()) throw db_error("recovery install: owner is closed");
            writer = owner->db_;
            revision = owner->connection_revision_;
        }
        if (!writer) throw db_error("recovery install: no published writer");
        database::maintenance_scope::probe_before_store_gate(*writer);
        {
            lattice_db::store_write_gate_hold gate(*owner);
            std::unique_lock<std::recursive_timed_mutex> memory_gate;
#ifdef __EMSCRIPTEN__
            constexpr bool memory_maintenance = true;
#else
            const bool memory_maintenance = owner->config_.is_in_memory();
#endif
            if (memory_maintenance && !owner->store_write_gate_)
                memory_gate = std::unique_lock<std::recursive_timed_mutex>(owner->vec0_memory_maintenance_gate_);
            database::maintenance_scope maintenance(*writer);
            auto* context = writer->lattice_update_hook_context_.get();
            {
                std::lock_guard<std::mutex> lock(owner->connection_ownership_mutex_);
                if (owner->closed_.load() || owner->connection_revision_ != revision || owner->db_ != writer ||
                    !context || context->owner != owner.get() || context->connection != writer->internal_handle() ||
                    context->sync_chunk || context->entry_cursor_active || context->recovery_delivery_deferred)
                    throw db_error("recovery install: admission invalidated or hook ownership unavailable");
                // Admission linearizes here. A later close may fence new work,
                // but the retained writer can settle under maintenance_scope.
            }
            {
                std::lock_guard<std::mutex> lock(owner->change_buffer_mutex_);
                if (owner->is_flushing_ || owner->recovery_change_buffer_reserved_ || !owner->change_buffer_.empty())
                    throw db_error("recovery install: existing notification delivery is unsettled");
                owner->recovery_change_buffer_reserved_ = true;
                owner->active_recovery_install_operations_.fetch_add(1, std::memory_order_acq_rel);
            }
            struct release_reservation {
                lattice_db& owner;
                database& writer;
                database::lattice_update_hook_context& context;
                database::sync_apply_chunk_state& settlement;
                ~release_reservation() {
                    if (context.sync_chunk == &settlement) context.sync_chunk = nullptr;
                    context.entry_cursor_active = false;
                    context.entry_cursor_present = false;
                    context.recovery_delivery_deferred = false;
                    writer.txn_dirty_.store(false, std::memory_order_relaxed);
                    std::lock_guard<std::mutex> lock(owner.change_buffer_mutex_);
                    owner.recovery_change_buffer_reserved_ = false;
                    owner.active_recovery_install_operations_.fetch_sub(1, std::memory_order_release);
                }
            } release{*owner, *writer, *context, settlement};
            context->sync_chunk = &settlement;
            context->entry_cursor_active = true;
            context->entry_cursor_present = false;
            context->recovery_delivery_deferred = true;
            frame authority(*owner, *writer, settlement);
            try {
                writer->begin_transaction();
                settlement.state = phase::active;
                body(*writer);
                if (settlement.state != phase::active || sqlite3_get_autocommit(writer->internal_handle()) != 0)
                    throw db_error("recovery install body settled its owned transaction");
                for (auto* stmt = sqlite3_next_stmt(writer->internal_handle(), nullptr); stmt;
                     stmt = sqlite3_next_stmt(writer->internal_handle(), stmt)) {
                    if (sqlite3_stmt_busy(stmt)) throw db_error("recovery install body left an active statement");
                }
                // Derive exact audit/model events while this write transaction
                // still pins its view. After COMMIT another handle may write.
                owner->flush_changes_once_impl(writer.get(), &batch);
                writer->commit();
                // Memory / DELETE-journal paths have no WAL callback. Their
                // successful COMMIT return is still before any deferred observer.
                if (settlement.state == phase::active) context->note_settled(true);
                if (settlement.state != phase::committed)
                    throw db_error("recovery install commit did not settle the owned transaction");
            } catch (...) {
                result.primary_error = std::current_exception();
                if (settlement.state == phase::active) {
                    try {
                        writer->rollback();
                        if (settlement.state != phase::rolled_back)
                            throw db_error("recovery install rollback did not settle the owned transaction");
                    } catch (...) {
                        result.cleanup_error = std::current_exception();
                        // Do not publish this still-active transaction to new
                        // ordinary operations. Raw-handle repair is not promised.
                        writer->closed_.store(true, std::memory_order_release);
                    }
                }
            }
            if (settlement.state == phase::committed) {
                result.state = recovery_install_state::committed;
                if (batch.audit_frontier) {
                    const auto frontier = *batch.audit_frontier;
                    auto advance = [frontier](lattice_db* target) {
                        auto previous = target->last_seen_audit_id_.load(std::memory_order_acquire);
                        while (previous < frontier && !target->last_seen_audit_id_.compare_exchange_weak(
                            previous, frontier, std::memory_order_acq_rel)) {}
                    };
                    if (owner->storage_shared_across_instances())
                        instance_registry::instance().for_each_alive(owner->config_.path, advance);
                    else advance(owner.get());
                }
            } else if (settlement.state == phase::rolled_back) result.state = recovery_install_state::rolled_back;
            else if (settlement.state == phase::active) result.state = recovery_install_state::unsettled;
        }
    } catch (...) {
        if (!result.primary_error) result.primary_error = std::current_exception();
        if (settlement.state == phase::committed) result.state = recovery_install_state::committed;
    }
    // All owned SQLite/store scopes and private helper authority ended above.
    // Payloads and callback copies likewise die outside those scopes.
    if (result.state == recovery_install_state::committed) {
        try {
            if (after_unlock) after_unlock(); // private deterministic test rendezvous only
            deliver(*owner, batch);
        } catch (...) { result.postcommit_error = std::current_exception(); }
    }
    return result;
}

void recovery_writer_access::deliver(lattice_db& owner, const lattice_db::recovery_commit_batch& batch) {
    auto alive = [&](auto&& fn) {
        if (owner.storage_shared_across_instances()) {
            instance_registry::instance().for_each_alive(owner.config_.path, [&](lattice_db* target) {
                if (!target->is_closed()) fn(target);
            });
        } else if (!owner.is_closed()) fn(&owner);
    };
    // Even a zero-event commit invalidates every remaining alive recipient.
    // Closing the source must not hide its durable commit from a sibling.
    alive([&](lattice_db* target) {
        target->fire_invalidation_hooks_local(batch.invalidations, lattice_db::invalidation_reason::commit);
    });
    if (!batch.events.empty()) alive([&](lattice_db* target) { target->notify_changes_batched(batch.events); });

    // Protect existing source-side hint resources against concurrent close.
    // This is not raw synchronizer ownership and does not enable recovery sync.
    auto guard = owner.guard_;
    {
        std::lock_guard<std::mutex> lock(owner.connection_ownership_mutex_);
        if (owner.closed_.load() || !guard->alive.load()) return;
        ++instance_guard::tls_depths()[guard.get()]; // allocation before refcount publication
        guard->notify_refcount.fetch_add(1, std::memory_order_seq_cst);
    }
    struct release_hold {
        std::shared_ptr<instance_guard> guard;
        ~release_hold() {
            auto& depths = instance_guard::tls_depths();
            if (--depths[guard.get()] == 0) depths.erase(guard.get());
            guard->notify_refcount.fetch_sub(1, std::memory_order_seq_cst);
        }
    } release{guard};
    if (!owner.is_closed() && batch.needs_upload_hint) owner.trigger_sync_upload();
    if (!owner.is_closed() && owner.shared_xproc_notifier_ && !owner.config_.read_only)
        owner.shared_xproc_notifier_->post_notification();
}
} // namespace lattice::detail
