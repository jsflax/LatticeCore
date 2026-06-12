#include <stdio.h>
#include <cmath>
#include <list.hpp>
#include <lattice.hpp>
#include <managed_object.hpp>
#include <geo_bounds.hpp>

// MARK: - Link List Ref retain/release for SWIFT_SHARED_REFERENCE
#if LATTICE_HAS_FRT
void retainLinkListRef(lattice::link_list_ref* p) {
    if (p) {
        p->retain();
    }
}

void releaseLinkListRef(lattice::link_list_ref* p) {
    if (p) {
        if (p->release()) {
            delete p;
        }
    }
}
#endif

namespace lattice {

link_list::link_list(const managed<std::vector<swift_dynamic_object*>>& o) : lattice(nullptr) {
    new (&managed_) managed(o);
    lattice = static_cast<swift_lattice*>(managed_.lattice);
}

// Mint a Swift-facing db handle on demand (FRT: fresh unretained heap ref the
// Swift side owns and frees; value path: by-value copy, empty when absent).
#if LATTICE_HAS_FRT
swift_lattice_ref* link_list_ref::getLattice() const {
   return swift_lattice_ref::_make(swift_lattice_ref::shared_for_lattice(impl_->lattice));
}
#else
swift_lattice_ref link_list_ref::getLattice() const {
   return swift_lattice_ref::_make(swift_lattice_ref::shared_for_lattice(impl_->lattice));
}
#endif

link_list::element_proxy& link_list::element_proxy::operator=(const dynamic_object_ref& o) {
    if (list->lattice) {
        if (o.is_managed()) {
            list->managed_[idx] = &o.impl_->managed_;
        } else {
            list->lattice->add(*o.impl_.get());
            list->managed_[idx] = &o.impl_->managed_;
        }
    } else {
        list->unmanaged_[idx] = o.shared();
    }
    return *this;
}

link_list::element_proxy link_list::operator[](size_t idx) const {
    element_proxy proxy;
    proxy.idx = idx;
    proxy.list = const_cast<link_list*>(this);
    if (lattice) {
        auto managed_obj = managed_[idx];
        const auto& schema = *lattice->get_properties_for_table(managed_obj.table_name());
        for (auto& [name, column_type] : schema) {
            managed_obj.properties_[name] = column_type;
            managed_obj.property_types_[name] = column_type.type;
            managed_obj.property_names_.push_back(name);
        }

        proxy.object = std::make_shared<dynamic_object>(managed_obj);
    } else {
        proxy.object = unmanaged_[idx];
    }
    return proxy;
}

// Capacity
size_t link_list::size() const {
    if (lattice) {
        return managed_.size();
    } else {
        return unmanaged_.size();
    }
}

bool link_list::empty() const {
    return size() == 0;
}

// Modifiers
void link_list::push_back(const dynamic_object_ref& obj) {
    if (lattice) {
        if (obj.is_managed()) {
            // Already managed - just add the link
            managed_.push_back(&obj.impl_->managed_);
        } else {
            // Not managed - add to database first, then link.
            // Wrap in a transaction so both AuditLog entries (child INSERT +
            // link INSERT) are committed atomically. Without this, the sync
            // system can upload the child before the link entry exists.
            auto& db = lattice->db();
            bool own_txn = !db.is_in_transaction();
            if (own_txn) db.begin_transaction();
            try {
                lattice->add(*obj.impl_.get());
                managed_.push_back(&obj.impl_->managed_);
                if (own_txn) db.commit();
            } catch (...) {
                if (own_txn && db.is_in_transaction()) {
                    try { db.rollback(); } catch (...) {}
                }
                throw;
            }
        }
    } else {
        // Store shared_ptr to keep the dynamic_object alive
        unmanaged_.push_back(obj.shared());
    }
}

void link_list::push_back(const swift_dynamic_object& obj) {
    if (lattice) {
        // Add to database and then link
        managed_.push_back(obj);
    } else {
        // For unmanaged, we need to store a pointer - this is tricky
        // For now, we don't support adding raw swift_dynamic_object to unmanaged list
    }
}

void link_list::erase(size_t idx) {
    if (lattice) {
        // Get the object at index and erase the link
        if (idx < managed_.size()) {
            auto obj = managed_[idx];
            managed_.erase(&obj.operator lattice::managed<swift_dynamic_object> &());
        }
    } else {
        if (idx < unmanaged_.size()) {
            unmanaged_.erase(unmanaged_.begin() + idx);
        }
    }
}

void link_list::clear() {
    if (lattice) {
        managed_.clear();
    } else {
        unmanaged_.clear();
    }
}

// Iterator
dynamic_object& link_list::iterator::operator*() const {
    return *list_->operator[](idx_).object;
}

link_list::iterator link_list::begin() {
    return iterator(this, 0);
}

link_list::iterator link_list::end() {
    return iterator(this, size());
}

std::optional<size_t> link_list::find_index(const dynamic_object_ref& obj) const {
    size_t count = size();

    if (lattice) {
        // Managed list - compare by global_id
        if (!obj.is_managed()) {
            return std::nullopt;  // Can't find unmanaged object in managed list
        }
        global_id_t target_id = obj.impl_->managed_.global_id();
        for (size_t i = 0; i < count; i++) {
            auto& element = managed_[i];
            if (element.global_id() == target_id) {
                return i;
            }
        }
    } else {
        // Unmanaged list - compare by dynamic_object pointer
        const dynamic_object* target_ptr = obj.impl_.get();
        for (size_t i = 0; i < count; i++) {
            if (unmanaged_[i].get() == target_ptr) {
                return i;
            }
        }
    }

    return std::nullopt;
}

std::vector<size_t> link_list::find_where(const std::string& sql_predicate) const {
    return find_indices(sql_predicate, "", true);
}

std::vector<size_t> link_list::find_indices(const std::string& sql_predicate,
                                            const std::string& order_column,
                                            bool ascending) const {
    // Only supported for managed lists
    if (!lattice) {
        return {};
    }

    // Virtual lists don't support find_indices (no single target table)
    if (managed_.is_virtual_) {
        return {};
    }

    // Access managed list's internals
    const auto& link_table = managed_.link_table_;
    const auto& target_table = managed_.target_table_;
    const auto& parent_id = managed_.parent_global_id_;
    auto* db = managed_.db;

    if (link_table.empty() || !db || !db->table_exists(link_table)) {
        return {};
    }

    // List positions of matching elements, ordered by a target-table column
    // when requested (list order otherwise; `__pos` tie-break keeps sorts
    // stable). Returns positions only — no row data is loaded, so callers
    // can hydrate elements lazily.
    //
    // Positions MUST index the same membership load_if_needed() builds:
    // the cache skips dangling links (junction rows whose target row was
    // deleted), so number rows AFTER the join — numbering all link rows
    // would shift every position past a dangling link, hydrating the wrong
    // element or walking off the end of the cache.
    std::string sql = R"(
        WITH ordered_links AS (
            SELECT t.*, (ROW_NUMBER() OVER (ORDER BY l.rowid)) - 1 AS __pos
            FROM )" + link_table + R"( l
            INNER JOIN )" + target_table + R"( t ON t.globalId = l.rhs
            WHERE l.lhs = ? COLLATE NOCASE
        )
        SELECT __pos FROM ordered_links
    )";
    if (!sql_predicate.empty()) {
        sql += " WHERE " + sql_predicate;
    }
    if (!order_column.empty()) {
        sql += " ORDER BY " + order_column + (ascending ? " ASC" : " DESC") + ", __pos";
    } else {
        sql += " ORDER BY __pos";
    }

    auto rows = db->query(sql, {parent_id});

    // Positions reflect the database NOW; drop the element cache so the
    // next access reloads against the same snapshot. Without this, a list
    // loaded earlier (and never invalidated by other connections' writes)
    // can be shorter than — or ordered differently from — these positions.
    managed_.loaded_ = false;
    managed_.cached_objects_.clear();

    std::vector<size_t> indices;
    indices.reserve(rows.size());

    for (const auto& row : rows) {
        auto it = row.find("__pos");
        if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
            indices.push_back(static_cast<size_t>(std::get<int64_t>(it->second)));
        }
    }

    return indices;
}

// MARK: - GeoBounds List implementations

geo_bounds_list::geo_bounds_list(const managed<std::vector<geo_bounds*>>& o) : lattice(nullptr) {
    new (&managed_) managed<std::vector<geo_bounds*>>(o);
    lattice = static_cast<swift_lattice*>(managed_.lattice);
}

geo_bounds_list::element_proxy geo_bounds_list::operator[](size_t idx) const {
    element_proxy proxy;
    proxy.idx = idx;
    proxy.list = const_cast<geo_bounds_list*>(this);
    if (lattice) {
        // managed_[idx] returns element_proxy -> managed<geo_bounds*>& -> detach() -> geo_bounds
        auto& self = const_cast<geo_bounds_list&>(*this);
        managed<geo_bounds*>& m = self.managed_[idx];
        proxy.value = m.detach();
    } else {
        proxy.value = unmanaged_[idx];
    }
    return proxy;
}

geo_bounds_list::element_proxy& geo_bounds_list::element_proxy::operator=(const geo_bounds& bounds) {
    value = bounds;
    list->set(idx, bounds);
    return *this;
}

#if LATTICE_HAS_FRT
void geo_bounds_list::element_proxy::assign(geo_bounds_ref* ref) {
    if (ref) {
        *this = *ref->get();
    }
}

geo_bounds_ref* geo_bounds_list::element_proxy::getObjectRef() const {
    return new geo_bounds_ref(value);
}
#else
void geo_bounds_list::element_proxy::assign(const geo_bounds_ref& ref) {
    if (ref.get()) {
        *this = *ref.get();
    }
}

geo_bounds_ref geo_bounds_list::element_proxy::getObjectRef() const {
    return geo_bounds_ref(value);
}
#endif

size_t geo_bounds_list::size() const {
    if (lattice) {
        return managed_.size();
    } else {
        return unmanaged_.size();
    }
}

bool geo_bounds_list::empty() const {
    return size() == 0;
}

void geo_bounds_list::push_back(const geo_bounds& bounds) {
    if (lattice) {
        managed_.push_back(bounds);
    } else {
        unmanaged_.push_back(bounds);
    }
}

void geo_bounds_list::push_back(geo_bounds_ref* ref) {
    if (ref) {
        push_back(*ref->get());
    }
}

void geo_bounds_list::erase(size_t idx) {
    if (lattice) {
        managed_.erase(idx);
    } else {
        if (idx < unmanaged_.size()) {
            unmanaged_.erase(unmanaged_.begin() + static_cast<std::ptrdiff_t>(idx));
        }
    }
}

void geo_bounds_list::clear() {
    if (lattice) {
        managed_.clear();
    } else {
        unmanaged_.clear();
    }
}

void geo_bounds_list::set(size_t idx, const geo_bounds& bounds) {
    if (lattice) {
        managed_[idx] = bounds;
    } else {
        if (idx < unmanaged_.size()) {
            unmanaged_[idx] = bounds;
        }
    }
}

std::optional<size_t> geo_bounds_list::find_index(const geo_bounds& target) const {
    if (lattice) {
        const auto& list_table = managed_.list_table_;
        const auto& parent_id = managed_.parent_global_id_;
        auto* db = managed_.db;

        if (list_table.empty() || !db || !db->table_exists(list_table)) {
            return std::nullopt;
        }

        // Query to find the index of a matching geo_bounds by coordinates
        std::string sql = R"(
            WITH ordered_bounds AS (
                SELECT id, minLat, maxLat, minLon, maxLon,
                       (ROW_NUMBER() OVER (ORDER BY id)) - 1 as idx
                FROM )" + list_table + R"(
                WHERE parentId = ?
            )
            SELECT idx FROM ordered_bounds
            WHERE minLat = ? AND maxLat = ? AND minLon = ? AND maxLon = ?
            LIMIT 1
        )";

        auto rows = db->query(sql, {parent_id, target.min_lat, target.max_lat, target.min_lon, target.max_lon});

        if (!rows.empty()) {
            auto it = rows[0].find("idx");
            if (it != rows[0].end() && std::holds_alternative<int64_t>(it->second)) {
                return static_cast<size_t>(std::get<int64_t>(it->second));
            }
        }
        return std::nullopt;
    } else {
        // Unmanaged: compare by coordinates with epsilon
        const double eps = 1e-9;
        auto close_enough = [eps](double a, double b) {
            return std::abs(a - b) < eps;
        };

        for (size_t i = 0; i < unmanaged_.size(); i++) {
            const geo_bounds& element = unmanaged_[i];
            if (close_enough(element.min_lat, target.min_lat) &&
                close_enough(element.max_lat, target.max_lat) &&
                close_enough(element.min_lon, target.min_lon) &&
                close_enough(element.max_lon, target.max_lon)) {
                return i;
            }
        }
        return std::nullopt;
    }
}

std::vector<size_t> geo_bounds_list::find_where(const std::string& sql_predicate) const {
    // Only supported for managed lists
    if (!lattice) {
        return {};
    }

    const auto& list_table = managed_.list_table_;
    const auto& parent_id = managed_.parent_global_id_;
    auto* db = managed_.db;

    if (list_table.empty() || !db || !db->table_exists(list_table)) {
        return {};
    }

    // Query to find all indices of matching elements
    std::string sql = R"(
        WITH ordered_bounds AS (
            SELECT id, (ROW_NUMBER() OVER (ORDER BY id)) - 1 as idx
            FROM )" + list_table + R"(
            WHERE parentId = ?
        )
        SELECT ob.idx
        FROM ordered_bounds ob
        INNER JOIN )" + list_table + R"( t ON t.id = ob.id
        WHERE )" + sql_predicate + R"(
        ORDER BY ob.idx
    )";

    auto rows = db->query(sql, {parent_id});

    std::vector<size_t> indices;
    indices.reserve(rows.size());

    for (const auto& row : rows) {
        auto it = row.find("idx");
        if (it != row.end() && std::holds_alternative<int64_t>(it->second)) {
            indices.push_back(static_cast<size_t>(std::get<int64_t>(it->second)));
        }
    }

    return indices;
}

// See link_list_ref::getLattice above — same on-demand mint.
#if LATTICE_HAS_FRT
swift_lattice_ref* geo_bounds_list_ref::getLattice() const {
    return swift_lattice_ref::_make(swift_lattice_ref::shared_for_lattice(impl_->lattice));
}
#else
swift_lattice_ref geo_bounds_list_ref::getLattice() const {
    return swift_lattice_ref::_make(swift_lattice_ref::shared_for_lattice(impl_->lattice));
}
#endif

}

// MARK: - GeoBounds Ref retain/release for SWIFT_SHARED_REFERENCE
#if LATTICE_HAS_FRT
void retainGeoBoundsRef(lattice::geo_bounds_ref* p) {
    if (p) {
        p->retain();
    }
}

void releaseGeoBoundsRef(lattice::geo_bounds_ref* p) {
    if (p) {
        if (p->release()) {
            delete p;
        }
    }
}

// MARK: - GeoBounds List Ref retain/release for SWIFT_SHARED_REFERENCE
void retainGeoBoundsListRef(lattice::geo_bounds_list_ref* p) {
    if (p) {
        p->retain();
    }
}

void releaseGeoBoundsListRef(lattice::geo_bounds_list_ref* p) {
    if (p) {
        if (p->release()) {
            delete p;
        }
    }
}
#endif
