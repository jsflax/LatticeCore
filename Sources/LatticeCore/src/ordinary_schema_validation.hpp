#pragma once
#include "ordinary_schema_projection.hpp"
#include <lattice/db.hpp>
#include <functional>
#include <optional>
#include <cstdint>
#include <memory>
#include "ordinary_initialization_storage.hpp"
#include <variant>

namespace lattice::detail {
// Trusted synchronous adapter over the caller's retained main snapshot. It
// must charge TEXT/BLOB bytes BEFORE copying and reject the (row_limit+1)th
// row. The validator additionally checks aggregate result bounds. No callback
// may run arbitrary application code or release/change the retained view.
using ordinary_metadata_query = std::function<std::vector<database::row_t>(
    const std::string&, const std::vector<column_value_t>&, size_t row_limit)>;
struct ordinary_metadata_limits {
    size_t objects=65536, rows=262144, bytes=32*1024*1024, sql_bytes=256*1024;
};
// Source-private owning inventory DATA. A genuine Request TEXT allocation
// is adopted by move, preserving that original storage. Any NEW copy or BLOB
// spelling owns allocator-accounted characters. No view/account pointer is
// retained; copying never makes a second uncharged standard-string payload.
class ordinary_inventory_text final {
    std::variant<ordinary_initialization_text,std::string> value_;
public:
    ordinary_inventory_text()=default;
    ordinary_inventory_text(const char* value):value_(std::in_place_index<0>,value){}
    ordinary_inventory_text(std::string_view value):value_(std::in_place_index<0>,value.empty()?"":value.data(),value.size()){}
    ordinary_inventory_text(std::string&& original)noexcept:value_(std::in_place_index<1>,std::move(original)){}
    ordinary_inventory_text(const std::string& value):ordinary_inventory_text(std::string_view(value)){}
    ordinary_inventory_text(ordinary_initialization_text&& value)noexcept:value_(std::in_place_index<0>,std::move(value)){}
    ordinary_inventory_text(const ordinary_initialization_text& value):ordinary_inventory_text(std::string_view(value)){}
    ordinary_inventory_text(const ordinary_inventory_text& value):ordinary_inventory_text(value.view()){}
    ordinary_inventory_text& operator=(const ordinary_inventory_text& value){if(this!=&value){ordinary_inventory_text copy(value);*this=std::move(copy);}return *this;}
    ordinary_inventory_text(ordinary_inventory_text&&)noexcept=default;
    ordinary_inventory_text& operator=(ordinary_inventory_text&&)noexcept=default;
    std::string_view view()const noexcept{return std::visit([](const auto& v){return std::string_view(v.data(),v.size());},value_);}
    operator std::string_view()const noexcept{return view();}
    const char* data()const noexcept{return view().data();}
    size_t size()const noexcept{return view().size();}
    bool empty()const noexcept{return view().empty();}
    char operator[](size_t n)const noexcept{return view()[n];}
    size_t find(std::string_view value,size_t at=0)const noexcept{return view().find(value,at);}
    bool starts_with(std::string_view value)const noexcept{return view().starts_with(value);}
    template<class Value>requires std::is_convertible_v<const Value&,std::string_view>
    friend bool operator==(const ordinary_inventory_text& a,const Value& b)noexcept{return a.view()==std::string_view(b);}
};
struct ordinary_column_metadata {
    ordinary_initialization_text name,type;
    int64_t cid=0,not_null=0,primary_key=0,hidden=0;
    std::optional<ordinary_initialization_text> default_sql;
};
struct ordinary_object_metadata { ordinary_inventory_text type,name,owner,sql; };
struct ordinary_unique_index_metadata {
    ordinary_initialization_text name,table,create_sql;
    ordinary_initialization_vector<ordinary_initialization_text> columns;
};
struct ordinary_table_metadata {
    ordinary_initialization_text name;
    ordinary_initialization_vector<ordinary_column_metadata> columns;
};
struct ordinary_derived_metadata {
    ordinary_derived_kind kind;
    ordinary_initialization_text model,property,table,create_sql;
    bool present=false;
    // nprobe==0 means the generated declaration omitted that option; the
    // pinned module supplies its default. It never means zero actual probes.
    size_t dimensions=0,ivf_nlist=0,ivf_nprobe=0;
};
struct ordinary_schema_metadata {
    int64_t schema_cookie=0,temp_schema_cookie=0;
    size_t inspected_rows=0,copied_bytes=0;
    // Includes verified module shadow tables; application row scope is still
    // the caller's projection.tables, never this metadata map's key set.
    ordinary_initialization_text_map<ordinary_table_metadata> tables;
    ordinary_initialization_vector<ordinary_object_metadata> objects;
    // Only explicit generated uniqueness constraints. Never PK/globalId
    // autoindexes. The caller owns atomic suspension and exact restoration.
    ordinary_initialization_vector<ordinary_unique_index_metadata> unique_indexes;
    ordinary_initialization_vector<ordinary_derived_metadata> derived;
};
// DATA only: an inventory-first preimage for the actual retained main/TEMP
// schemas. The four pragma_* metadata producer names are reserved in both
// schemas before table-valued PRAGMA connection. A verified origin index
// identifies one exact trigger object below;
// the name/index/fact is never an independent SQL, open, or migration grant.
// Existing ordinary/shadow tables receive the unchanged engine metadata
// cross-checks. Modules receive only the closed pre-connect declaration checks;
// module xConnect/format/config and all application rows remain untouched here.
struct ordinary_preimage_table_shape {
    ordinary_initialization_text name;
    std::uint64_t columns=0;
    bool without_rowid=false;
    ordinary_initialization_vector<ordinary_column_metadata> physical_columns; // validated same-view DATA only
};
struct ordinary_schema_preimage {
    int64_t schema_cookie=0,temp_schema_cookie=0;
    size_t inspected_rows=0,copied_bytes=0;
    ordinary_initialization_vector<ordinary_object_metadata> main_objects,temp_objects;
    ordinary_initialization_vector<ordinary_preimage_table_shape> plain_shapes;
    ordinary_initialization_vector<ordinary_initialization_text> absent_tables;
    ordinary_initialization_vector<size_t> verified_trigger_origins; // indices into main_objects
};
class ordinary_preimage_statement;
using ordinary_preimage_query=std::function<std::vector<database::row_t>(
    const std::shared_ptr<const ordinary_preimage_statement>&)>;
// Actual Core startup borrows its synchronous stack callable. The old generic
// std::function alias above keeps its original public/source behavior.
using ordinary_initialization_preimage_query=ordinary_initialization_function_ref<
    std::vector<database::row_t>(const std::shared_ptr<const ordinary_preimage_statement>&)>;
ordinary_schema_preimage validate_ordinary_preexisting_schema(
    const ordinary_schema_projection&,const ordinary_preimage_query&,
    const ordinary_metadata_limits& = {});
// Source-private typed construction/final-proof channel. Only the immutable
// startup Plan may select either operation; no public bool/purpose or caller
// SQL can turn missing members into a completed family. Both return DATA only.
class observer_delivery_initialization_plan;
struct ordinary_preimage_recipe_test_access;
class ordinary_initialization_schema_validation final {
    friend class observer_delivery_initialization_plan;
    friend struct ordinary_preimage_recipe_test_access;
    friend struct ordinary_final_recipe_test_access;
    friend struct ordinary_vector_connect_recipe_test_access;
    friend ordinary_schema_preimage validate_ordinary_preexisting_schema(
        const ordinary_schema_projection&,const ordinary_preimage_query&,const ordinary_metadata_limits&);
    friend ordinary_schema_preimage inspect_ordinary_preexisting_read_tables(
        const ordinary_schema_projection&,const ordinary_preimage_query&,const ordinary_metadata_limits&);
    enum class final_read {meter_namespace,sync_control_row,meter_row,metadata_rows,encoding,recursive_triggers,foreign_keys,databases};
    static std::shared_ptr<const ordinary_preimage_statement> final_statement_(final_read);
    static ordinary_schema_metadata complete_(const ordinary_schema_projection&,const ordinary_initialization_preimage_query&,const ordinary_metadata_limits&);
    static ordinary_schema_metadata complete_body_(const ordinary_schema_projection&,const ordinary_metadata_query&,const ordinary_metadata_limits&,const ordinary_initialization_preimage_query*);
    friend ordinary_schema_metadata validate_ordinary_schema(const ordinary_schema_projection&,const ordinary_metadata_query&,const ordinary_metadata_limits&);
    enum class view {preexisting,construction_prefix,construction_initial,required_present};
    static ordinary_schema_preimage preexisting_(const ordinary_schema_projection& p,
        const ordinary_initialization_preimage_query& q,const ordinary_metadata_limits& l){return validate_(p,q,l,view::preexisting);}
    static ordinary_schema_preimage read_tables_(const ordinary_schema_projection&,
        const ordinary_initialization_preimage_query&,const ordinary_metadata_limits&);
    // Pure DATA parsing of already generated immutable library programs.
    // No caller SQL or execution/admission result is accepted or returned.
    static ordinary_initialization_text compiled_object_(std::string_view,std::string_view kind);
    static ordinary_initialization_vector<ordinary_initialization_text> compiled_implicit_indexes_(std::string_view,std::string_view table);
    static ordinary_schema_preimage validate_(const ordinary_schema_projection&,
        const ordinary_initialization_preimage_query&,const ordinary_metadata_limits&,view,ordinary_schema_projection* selected=nullptr);
    static ordinary_schema_preimage construction_initial_(ordinary_schema_projection& p,
        const ordinary_initialization_preimage_query& q,const ordinary_metadata_limits& l){return validate_(p,q,l,view::construction_initial,&p);}
    static ordinary_schema_preimage construction_prefix_(const ordinary_schema_projection& p,
        const ordinary_initialization_preimage_query& q,const ordinary_metadata_limits& l){return validate_(p,q,l,view::construction_prefix);}
    static ordinary_schema_preimage required_present_(const ordinary_schema_projection& p,
        const ordinary_initialization_preimage_query& q,const ordinary_metadata_limits& l){return validate_(p,q,l,view::required_present);}
};
// Distinct bootstrap SELECT-only inspection. It proves present plain tables
// and their exact engine metadata, while retaining unvalidated trigger DATA.
// It returns NO verified trigger origins or DML/publication authority. Existing
// complete enrolled-program validation remains mandatory before publication.
ordinary_schema_preimage inspect_ordinary_preexisting_read_tables(
    const ordinary_schema_projection&,const ordinary_preimage_query&,
    const ordinary_metadata_limits& = {});
// Only the actual library preimage producer can issue this immutable request.
// It cannot be constructed from caller SQL, a recognized shape, or a digest.
// The synchronous callback must charge actual TEXT/BLOB bytes before copying,
// reject row_limit()+1, columns beyond column_limit(), and result bytes beyond
// result_byte_limit() (including field names); field names are at most 256 B.
class ordinary_preimage_statement final {
public:
    enum class kind : std::uint8_t { unissued,main_cookie,temp_cookie,
        main_inventory,temp_inventory,table_columns,table_foreign_keys,
        temp_table_foreign_keys,index_list,index_columns,final_meter_row,final_metadata_rows,final_encoding,final_recursive_triggers,final_foreign_keys,final_databases,final_sync_control_row,module_fts_config,module_vector_info,module_vector_columns };
private:
    struct state;
    const std::uint64_t creator_;
    state* const state_;
    ordinary_preimage_statement(std::uint64_t creator,state* actual)noexcept
        :creator_(creator),state_(actual){}
    static void destroy_(const ordinary_preimage_statement*)noexcept;
    void require_current_()const;
    void enter_native_()const;
    // DATA predicate for the exact preproved selected Vec xConnect read only.
    // The actual Native descriptor additionally proves its Open/episode/revision.
    bool vector_connect_read_(const char*,const char*,const char*)const noexcept;
    friend struct ordinary_vector_connect_recipe_test_access;
    friend class observer_delivery_initialization_sql;
    friend class ordinary_initialization_schema_validation;
    friend ordinary_schema_preimage validate_ordinary_preexisting_schema(
        const ordinary_schema_projection&,const ordinary_preimage_query&,
        const ordinary_metadata_limits&);
    friend ordinary_schema_preimage inspect_ordinary_preexisting_read_tables(
        const ordinary_schema_projection&,const ordinary_preimage_query&,
        const ordinary_metadata_limits&);
public:
    ~ordinary_preimage_statement()noexcept;
    ordinary_preimage_statement(const ordinary_preimage_statement&)=delete;
    ordinary_preimage_statement(ordinary_preimage_statement&&)=delete;
    ordinary_preimage_statement& operator=(const ordinary_preimage_statement&)=delete;
    ordinary_preimage_statement& operator=(ordinary_preimage_statement&&)=delete;
    bool creator_current()const noexcept;
    kind kind_id()const;
    const std::string& sql()const;
    const std::vector<column_value_t>& parameters()const;
    // Immutable borrowed DATA, retained by this statement. Native initialization
    // uses these views without creating legacy std-container capacity. Legacy
    // materialization is generic-only and cannot later enter the Native channel.
    std::string_view sql_view()const;
    size_t parameter_count()const;
    std::string_view parameter_text(size_t actual_index)const;
    size_t row_limit()const;
    size_t result_byte_limit()const;
    size_t parameter_bytes()const;
    static constexpr size_t column_limit()noexcept{return 16;}
    static constexpr size_t parameter_limit()noexcept{return 1;}
    static constexpr size_t parameter_byte_limit()noexcept{return 256;}
};
// Caller must precharge the whole bounded procedure on its same setup budget
// BEFORE entry (including typed statement/result storage), retain one view,
// and refuse callback reentry/view changes. No application code may run here.
// Missing selected families remain absent; an existing incompatible or partial
// family refuses instead of being relabeled an approved automatic migration.
class ordinary_schema_validation_error : public std::runtime_error {
public: using std::runtime_error::runtime_error;
};
// Describes observed metadata only. No source/receipt/install authority, row
// scans, SQL mutation, repair, or callback installation. Physical ALTER-column
// defaults/nullability/order may differ between logically equal declarations;
// actual physical constraints remain visible to the caller's value preflight.
// Only selected modules are connected. Module shadow ownership derives from
// exact pinned module programs and actual closed DDL/index/column metadata;
// this API does not separately observe SQLite's internal TF_Shadow bit.
ordinary_schema_metadata validate_ordinary_schema(const ordinary_schema_projection&,
    const ordinary_metadata_query&, const ordinary_metadata_limits& = {});
} // namespace lattice::detail
