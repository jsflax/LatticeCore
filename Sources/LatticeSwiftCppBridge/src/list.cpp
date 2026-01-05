#include <stdio.h>
#include <cmath>
#include <list.hpp>
#include <lattice.hpp>
#include <managed_object.hpp>
#include <geo_bounds.hpp>

// MARK: - Link List Ref retain/release for SWIFT_SHARED_REFERENCE
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

namespace lattice {

link_list::link_list(const managed<std::vector<swift_dynamic_object*>>& o) : lattice(nullptr) {
    new (&managed_) managed(o);
    lattice = static_cast<swift_lattice*>(managed_.lattice);
}

swift_lattice_ref* link_list_ref::getLattice() const SWIFT_COMPUTED_PROPERTY {
   return swift_lattice_ref::get_ref_for_lattice(impl_->lattice);
}

link_list::element_proxy& link_list::element_proxy::operator=(dynamic_object_ref* o) {
    if (list->lattice) {
        if (o->is_managed()) {
            list->managed_[idx] = &o->impl_->managed_;
        } else {
            list->lattice->add(*o->impl_.get());
            list->managed_[idx] = &o->impl_->managed_;
        }
    } else {
        list->unmanaged_[idx] = o;
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
        proxy.object = unmanaged_[idx]->impl_;
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
void link_list::push_back(dynamic_object_ref* obj) {
    if (lattice) {
        if (obj->is_managed()) {
            // Already managed - just add the link
            managed_.push_back(&obj->impl_->managed_);
        } else {
            // Not managed - add to database first
            lattice->add(*obj->impl_.get());
            managed_.push_back(&obj->impl_->managed_);
        }
    } else {
        unmanaged_.push_back(obj);
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
        // Unmanaged list - compare by pointer address
        const dynamic_object_ref* target_ptr = &obj;
        for (size_t i = 0; i < count; i++) {
            if (unmanaged_[i] == target_ptr) {
                return i;
            }
        }
    }

    return std::nullopt;
}

std::vector<size_t> link_list::find_where(const std::string& sql_predicate) const {
    // Only supported for managed lists
    if (!lattice) {
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

    // Query to find all indices of matching elements
    // Uses ROW_NUMBER to get position in the ordered list
    std::string sql = R"(
        WITH ordered_links AS (
            SELECT rhs, (ROW_NUMBER() OVER (ORDER BY rowid)) - 1 as idx
            FROM )" + link_table + R"(
            WHERE lhs = ?
        )
        SELECT ol.idx
        FROM ordered_links ol
        INNER JOIN )" + target_table + R"( t ON t.globalId = ol.rhs
        WHERE )" + sql_predicate + R"(
        ORDER BY ol.idx
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

void geo_bounds_list::element_proxy::assign(geo_bounds_ref* ref) {
    if (ref) {
        *this = *ref->get();
    }
}

geo_bounds_ref* geo_bounds_list::element_proxy::getObjectRef() const {
    return new geo_bounds_ref(value);
}

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

swift_lattice_ref* geo_bounds_list_ref::getLattice() const SWIFT_COMPUTED_PROPERTY {
    return swift_lattice_ref::get_ref_for_lattice(impl_->lattice);
}

}

// MARK: - GeoBounds Ref retain/release for SWIFT_SHARED_REFERENCE
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
