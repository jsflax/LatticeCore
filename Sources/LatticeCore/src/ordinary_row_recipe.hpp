#pragma once
#include "ordinary_export.hpp"
#include "ordinary_initialization_storage.hpp"
#include <string_view>
#include <cmath>
// Exact normal physical row/INSERT producer only. Returns DATA; no target/native
// SQL role, model selection, source cut, currentness or import grant is issued.
namespace lattice::detail {
struct ordinary_row_recipe final {
private:
    [[noreturn]] static void refuse(const char* reason){throw db_error(std::string("ordinary owned export: ")+reason);}
    static void require_ordinary_storage_() {
        if(ordinary_initialization_storage_scope::debit_scope_active())
            throw ordinary_initialization_storage_error("startup cannot enter generic row recipe");
    }
public:
    static std::string quote(std::string_view name) {
    require_ordinary_storage_();
    if(name.empty()||name.size()>256||name.find('\0')!=std::string::npos)refuse("invalid identifier");
    std::string out="\"";for(char c:name){out+=c;if(c=='\"')out+='\"';}return out+'\"';
}
    static void validate_cells(const std::vector<column_value_t>& row,const std::vector<column_type>& types,
        const std::vector<bool>& required) {
        for(size_t c=0;c<row.size();++c) {
                const auto& value=row[c];
                if(std::holds_alternative<std::nullptr_t>(value)) {if(c==0||required[c])refuse("incoming value violates actual nullability");continue;}
                const auto type=types[c];
                if((type==column_type::text&&!std::holds_alternative<std::string>(value))||
                   (type==column_type::integer&&!std::holds_alternative<int64_t>(value))||
                   (type==column_type::blob&&!std::holds_alternative<std::vector<uint8_t>>(value))||
                   (type==column_type::real&&!std::holds_alternative<double>(value)&&!std::holds_alternative<int64_t>(value)))
                    refuse("logical row storage disagrees with actual declaration");
                if(const auto* n=std::get_if<double>(&value);n&&!std::isfinite(*n))refuse("nonfinite logical row number");
            }
    }

    struct fields_program {std::string columns,arguments,assignments;};
    static fields_program fields(const std::vector<std::string>& names) {
        require_ordinary_storage_();
        fields_program out;auto& columns=out.columns;auto& arguments=out.arguments;auto& assignments=out.assignments;
        for(const auto& name:names) {
            if(!columns.empty()){columns+=',';arguments+=',';assignments+=',';}
            columns+=quote(name);arguments+='?';assignments+=quote(name)+"=?";
        }
        return out;
    }
    static std::string insert(std::string_view name,const fields_program& fields) {
        require_ordinary_storage_();
        const auto& columns=fields.columns;const auto& arguments=fields.arguments;
        return "INSERT INTO main."+quote(name)+'('+columns+") VALUES("+arguments+')';
    }
};
}
