#pragma once
#include "ordinary_owned_launch.hpp"
#include "ordinary_target_programme_tail.hpp"
#include "ordinary_target_schema_root_tail.hpp"
#include "ordinary_schema_validation.hpp"
#include "ordinary_initialization_storage.hpp"
#include <string_view>
#include <lattice/ordinary_history.hpp>
#include <memory>
#include <algorithm>
#include <cstdint>
#include <string>
#include <vector>
#include <variant>
#include <exception>
#include <stdexcept>

namespace lattice {class database;}
namespace lattice::detail {
class ordinary_export_access;
class ordinary_target_staging_connection;
struct ordinary_export_frame;
struct ordinary_internal_schema_metadata;

// Actual GROUP request-owned schema input original. This type has no public
// schema/query/SQL accessor and never supplies source cut or row authority.
// The next target-only generator/channel must consume this SAME original,
// rather than a copied digest, model list or successful-validation Boolean.
class ordinary_group_target_schema_original final {
    friend struct ordinary_target_schema_roots_ending;
    ordinary_target_schema_root_tail terminal_schema_root_; // First: after actual const state member.
    friend class ordinary_export_access;
    friend class ordinary_target_staging_connection;
    void require_connection(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;
    friend class ::lattice::database;
    struct catalog_program {
        ordinary_target_programme_tail terminal_programme_; // First: post-member programme ending.
        friend class ordinary_export_access;
        friend class ordinary_target_staging_connection;
        friend class ::lattice::database;
        enum class purpose {generated,require_absent_object,require_empty_vector_model,schema_cookie,fingerprint_cookie};
        using argument=std::variant<std::string,std::int64_t>;
        struct operation {purpose kind;std::string sql;std::vector<argument> bindings;};
        static constexpr std::size_t maximum_operations=4096,maximum_bytes=4194304,maximum_bindings=8192;
        const std::weak_ptr<ordinary_group_target_schema_original> original;
        std::vector<operation> operations;
        std::size_t bytes=0,bindings=0;
        std::exception_ptr first_failure;
        bool generation_returned=false;
        explicit catalog_program(const std::shared_ptr<ordinary_group_target_schema_original>& value)
            :terminal_programme_(this,value.get(),0),original(value) {
            operations.reserve(maximum_operations);
            if(operations.capacity()>maximum_operations||operations.capacity()>maximum_bytes/sizeof(operation))
                throw std::runtime_error("GROUP catalog operation capacity bound");
            bytes=operations.capacity()*sizeof(operation);
            terminal_programme_.construction_returned_=true;
        }
        void append(purpose kind,std::string sql,std::vector<argument> values={}) {
            if(generation_returned||operations.size()==maximum_operations||values.size()>64||
               values.capacity()>maximum_bindings-bindings||sql.empty()||sql.find('\0')!=std::string::npos||
               sql.capacity()>maximum_bytes||values.capacity()>(maximum_bytes-sql.capacity())/sizeof(argument))
                throw std::runtime_error("GROUP catalog generated program bound");
            std::size_t charge=sql.capacity()+values.capacity()*sizeof(argument);
            for(const auto& value:values)if(const auto* text=std::get_if<std::string>(&value)) {
                if(text->find('\0')!=std::string::npos||text->capacity()>maximum_bytes-charge)
                    throw std::runtime_error("GROUP catalog generated parameter bound");
                charge+=text->capacity();
            }
            if(charge>maximum_bytes-bytes)throw std::runtime_error("GROUP catalog retained program bound");
            bytes+=charge;bindings+=values.capacity();operations.push_back({kind,std::move(sql),std::move(values)});
        }

    };
    const std::shared_ptr<catalog_program>& require_catalog(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;

    struct projection_program {
        ordinary_target_programme_tail terminal_programme_; // First: post-member programme ending.
        const std::weak_ptr<const ordinary_group_target_schema_original> original;
        std::shared_ptr<ordinary_schema_projection> plan;
        std::shared_ptr<ordinary_export_frame> logical;
        std::shared_ptr<ordinary_schema_metadata> metadata;
        std::shared_ptr<ordinary_internal_schema_metadata> internal;
        static constexpr std::size_t maximum_bytes=ordinary_history_limits::metadata_bytes,maximum_cells=2097152;
        std::size_t bytes=sizeof(projection_program),cells=0;
        bool generation_entered=false,generation_returned=false,validation_entered=false,validation_returned=false;
        std::exception_ptr first_failure;
        explicit projection_program(const std::shared_ptr<const ordinary_group_target_schema_original>& actual)
            :terminal_programme_(this,actual.get(),1),original(actual){terminal_programme_.construction_returned_=true;}
        void add(std::size_t n){if(n>maximum_bytes-bytes)throw std::runtime_error("GROUP actual projection metadata bound");bytes+=n;}
        void cell(){if(cells==maximum_cells)throw std::runtime_error("GROUP actual projection metadata cells bound");++cells;}
    };
    const std::shared_ptr<projection_program>& prepare_projection(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;
    const std::shared_ptr<projection_program>& require_projection(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;
    void validate_projection_read(const ordinary_target_staging_connection&,const ordinary_metadata_query&,ordinary_launch::deadline)const;

    struct import_suspension_program {
        ordinary_target_programme_tail terminal_programme_; // First: post-member programme ending.
        enum class purpose {require_empty,drop_trigger,drop_index,require_absent};
        struct operation {purpose kind;std::string sql,name;};
        static constexpr std::size_t maximum_operations=16384,maximum_bytes=4194304;
        const std::weak_ptr<const ordinary_group_target_schema_original> original;
        const std::shared_ptr<projection_program> projection; // SAME actual Native validated identities/restore SQL.
        std::vector<operation> operations;
        std::size_t bytes=sizeof(import_suspension_program);
        bool generation_entered=false,generation_returned=false;
        std::exception_ptr first_failure;
        import_suspension_program(const std::shared_ptr<const ordinary_group_target_schema_original>& actual,
            const std::shared_ptr<projection_program>& input)
            :terminal_programme_(this,actual.get(),2),original(actual),projection(input){terminal_programme_.construction_returned_=true;}
        void add(std::size_t n){if(n>maximum_bytes-bytes)throw std::runtime_error("GROUP actual import suspension programme bound");bytes+=n;}
        void append(purpose kind,std::string sql,std::string_view name) {
            // The actual ordinary operation owns its identity; input is only borrowed.
            auto retained_name=ordinary_initialization_legacy_text(name);
            if(generation_returned||operations.size()==maximum_operations||sql.empty()||sql.find('\0')!=std::string::npos)
                throw std::runtime_error("GROUP import suspension operation shape bound");
            if(operations.size()==operations.capacity()) {
                const auto wanted=std::min(maximum_operations,std::max<std::size_t>(1,operations.capacity()*2));
                add((wanted-operations.capacity())*sizeof(operation));operations.reserve(wanted);
                if(operations.capacity()>wanted)add((operations.capacity()-wanted)*sizeof(operation));
            }
            add(sql.capacity()+retained_name.capacity());operations.push_back({kind,std::move(sql),std::move(retained_name)});
        }
    };
    const std::shared_ptr<import_suspension_program>& prepare_import_suspension(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;
    const std::shared_ptr<import_suspension_program>& require_import_suspension(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;

    struct row_import_program {
        ordinary_target_programme_tail terminal_programme_; // First: post-member programme ending.
        struct table_program {
            std::string name,insert_sql,read_sql;
            std::vector<std::string> columns;
            std::vector<column_type> types;
            std::vector<bool> required;
            bool local_id=false;
        };
        const std::weak_ptr<const ordinary_group_target_schema_original> original;
        const std::shared_ptr<projection_program> projection;
        std::vector<table_program> tables;
        static constexpr std::size_t maximum_bytes=4194304;
        std::size_t bytes=sizeof(row_import_program);
        bool generation_entered=false,generation_returned=false;
        std::exception_ptr first_failure;
        row_import_program(const std::shared_ptr<const ordinary_group_target_schema_original>& actual,
            const std::shared_ptr<projection_program>& input)
            :terminal_programme_(this,actual.get(),3),original(actual),projection(input){terminal_programme_.construction_returned_=true;}
        void add(std::size_t n){if(n>maximum_bytes-bytes)throw std::runtime_error("GROUP row programme bound refused");bytes+=n;}
    };
    const std::shared_ptr<row_import_program>& prepare_row_import(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;
    const std::shared_ptr<row_import_program>& require_row_import(const ordinary_target_staging_connection&,ordinary_launch::deadline)const;

    struct state;
    const std::shared_ptr<state> state_;
    explicit ordinary_group_target_schema_original(std::shared_ptr<state>);
public:
    ~ordinary_group_target_schema_original();
    ordinary_group_target_schema_original(const ordinary_group_target_schema_original&)=delete;
    ordinary_group_target_schema_original& operator=(const ordinary_group_target_schema_original&)=delete;
};
} // namespace lattice::detail
