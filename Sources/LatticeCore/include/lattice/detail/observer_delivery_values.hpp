#pragma once
#include "../types.hpp"
#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <stdexcept>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <variant>

namespace lattice {template<typename T,typename>struct managed;}
namespace lattice::detail {
// Issued by the actual LATTICE_SCHEMA expansion, separately from borrowed
// member metadata. Manual managed specializations remain legacy-only unless
// a reviewed closed generator is added; same-spelled tuples do not classify
// an arbitrary default constructor as a bounded output constructor.
template<typename Original>struct observer_delivery_generated_schema:std::false_type{};
// Closed, borrowed synchronous input DATA. No function supplied by an
// application can collect/copy/encode values while capacity is unavailable.
struct observer_delivery_values {
    using value_view=std::variant<std::nullptr_t,int64_t,double,std::string_view,std::span<const uint8_t>>;
    struct field_view {std::string_view name;value_view contents;};
    struct row_view {std::string_view table;std::span<const field_view> fields;};
    struct write_limits {uint64_t events=0,event_bytes=0,workspace_bytes=0;};
    static constexpr size_t column_ceiling=128;
    // Reused for one row at a time in BOTH plan passes. This fixed prospective
    // DATA workspace does not grow with offered row count or value length.
    class field_buffer {
        std::array<field_view,column_ceiling> fields_{};
        std::array<std::array<char,36>,column_ceiling> uuid_text_{};
        size_t count_=0,uuid_count_=0;
        template<typename T>struct optional_type:std::false_type{};
        template<typename T>struct optional_type<std::optional<T>>:std::true_type{};
    public:
        field_buffer()=default;
        field_buffer(const field_buffer&)=delete;
        field_buffer& operator=(const field_buffer&)=delete;
        void reset()noexcept{count_=0;uuid_count_=0;}
        std::span<const field_view> fields()const noexcept{return {fields_.data(),count_};}
        void append(std::string_view name,value_view value) {
            if(count_==column_ceiling)throw std::length_error("delivery producer row column bound");
            fields_[count_++]={name,value};
        }
        void append_column(std::string_view name,const column_value_t& value) {
            std::visit([&](const auto& actual){append_scalar(name,actual);},value);
        }
        template<typename T>void append_scalar(std::string_view name,const T& value) {
            using U=std::remove_cv_t<T>;
            if constexpr(std::is_same_v<U,std::nullptr_t>)append(name,nullptr);
            else if constexpr(std::is_same_v<U,int64_t>||std::is_same_v<U,int>||std::is_same_v<U,bool>)
                append(name,static_cast<int64_t>(value));
            else if constexpr(std::is_same_v<U,double>||std::is_same_v<U,float>)append(name,static_cast<double>(value));
            else if constexpr(std::is_same_v<U,std::string>)append(name,std::string_view(value.data(),value.size()));
            else if constexpr(std::is_same_v<U,std::vector<uint8_t>>)append(name,std::span<const uint8_t>(value.data(),value.size()));
            else if constexpr(std::is_same_v<U,timestamp_t>) {
                // Exact legacy to_column_value precision, without an owning DTO.
                const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(value.time_since_epoch()).count();
                append(name,static_cast<double>(ms)/1000.0);
            } else if constexpr(std::is_same_v<U,uuid_t>) {
                if(uuid_count_==column_ceiling)throw std::length_error("delivery UUID scratch bound");
                auto& text=uuid_text_[uuid_count_++];size_t out=0;constexpr char hex[]="0123456789abcdef";
                for(size_t index=0;index<value.bytes.size();++index) {
                    if(index==4||index==6||index==8||index==10)text[out++]='-';
                    text[out++]=hex[value.bytes[index]>>4];text[out++]=hex[value.bytes[index]&15];
                }
                append(name,std::string_view(text.data(),text.size()));
            } else if constexpr(optional_type<U>::value) {
                if(value)append_scalar(name,*value);else append(name,nullptr);
            } else {
                // No implicit conversion, virtual getter, collect_values(),
                // JSON callback or opaque copy is invoked for unsupported T.
                throw std::invalid_argument("delivery producer requires a closed value representation");
            }
        }
    };
    class cursor;
};

template<typename Owner,typename Field>struct observer_delivery_member {
    std::string_view name;
    Field Owner::*pointer;
    // Compile-time expanded geo names, avoiding pre-admission name allocation.
    std::array<std::string_view,4> geo_names;
};
template<typename Output,typename Field>struct observer_delivery_output_member {
    std::string_view name;
    managed<Field,void> Output::*pointer;
    std::array<std::string_view,4> geo_names;
};
namespace observer_delivery_value_detail {
template<typename Original>void emplace_generated_output(std::vector<managed<Original,void>>& output) {
    if constexpr(observer_delivery_generated_schema<Original>::value)output.emplace_back();
    else throw std::invalid_argument("delivery output requires its actual generated wrapper");
}
template<typename T>struct optional:std::false_type{};
template<typename T>struct optional<std::optional<T>>:std::true_type{using value_type=T;};
inline const observer_delivery_values::value_view& field(
    observer_delivery_values::row_view row,std::string_view name) {
    for(const auto& column:row.fields)if(column.name==name)return column.contents;
    throw std::invalid_argument("admitted delivery output field missing");
}
// Only closed builtin representations. Materialization reads the immutable
// admitted row; it never rereads an application source after admission.
template<typename T>T materialized_scalar(const observer_delivery_values::value_view& value) {
    if constexpr(optional<T>::value) {
        if(std::holds_alternative<std::nullptr_t>(value))return std::nullopt;
        return materialized_scalar<typename optional<T>::value_type>(value);
    } else if constexpr(std::is_same_v<T,int64_t>||std::is_same_v<T,int>||std::is_same_v<T,bool>)
        return static_cast<T>(std::get<int64_t>(value));
    else if constexpr(std::is_same_v<T,double>||std::is_same_v<T,float>)
        return static_cast<T>(std::get<double>(value));
    else if constexpr(std::is_same_v<T,std::string>)return std::string(std::get<std::string_view>(value));
    else if constexpr(std::is_same_v<T,std::vector<uint8_t>>) {
        const auto bytes=std::get<std::span<const uint8_t>>(value);return {bytes.begin(),bytes.end()};
    } else if constexpr(std::is_same_v<T,timestamp_t>) {
        return timestamp_t(std::chrono::duration_cast<timestamp_t::duration>(
            std::chrono::duration<double>(std::get<double>(value))));
    } else if constexpr(std::is_same_v<T,uuid_t>) {
        const auto text=std::get<std::string_view>(value);
        if(text.size()!=36)throw std::invalid_argument("admitted delivery UUID length");
        uuid_t result;size_t at=0;
        const auto nibble=[](char c)->uint8_t{
            if(c>='0'&&c<='9')return static_cast<uint8_t>(c-'0');
            if(c>='a'&&c<='f')return static_cast<uint8_t>(c-'a'+10);
            if(c>='A'&&c<='F')return static_cast<uint8_t>(c-'A'+10);
            throw std::invalid_argument("admitted delivery UUID digit");
        };
        for(size_t index=0;index<result.bytes.size();++index){
            if(index==4||index==6||index==8||index==10){
                if(text[at++]!='-')throw std::invalid_argument("admitted delivery UUID separator");
            }
            result.bytes[index]=static_cast<uint8_t>((nibble(text[at])<<4)|nibble(text[at+1]));at+=2;
        }
        return result;
    } else throw std::invalid_argument("delivery output requires closed builtin members");
}
template<typename Original,typename Managed>void materialize(Managed& destination,
    observer_delivery_values::row_view row,std::string_view name,
    const std::array<std::string_view,4>& geo) {
    if constexpr(std::is_same_v<Original,geo_bounds>||std::is_same_v<Original,std::optional<geo_bounds>>) {
        if constexpr(std::is_same_v<Original,std::optional<geo_bounds>>) {
            if(std::holds_alternative<std::nullptr_t>(field(row,geo[0]))){
                for(const auto column:geo)if(!std::holds_alternative<std::nullptr_t>(field(row,column)))
                    throw std::invalid_argument("admitted delivery mixed nullable bounds");
                destination.unmanaged_value=std::nullopt;return;
            }
        }
        destination.unmanaged_value=geo_bounds{std::get<double>(field(row,geo[0])),std::get<double>(field(row,geo[1])),
            std::get<double>(field(row,geo[2])),std::get<double>(field(row,geo[3]))};
    } else if constexpr(requires{destination.unmanaged_value=materialized_scalar<Original>(field(row,name));}) {
        destination.unmanaged_value=materialized_scalar<Original>(field(row,name));
    } else throw std::invalid_argument("delivery relationship output is not yet closed");
}
inline void add_structured_bytes(uint64_t& total,uint64_t count) {
    if(count>UINT64_MAX-total)throw std::length_error("delivery output workspace overflow");total+=count;
}
inline void add_structured_product(uint64_t& total,uint64_t count,uint64_t width) {
    if(width&&count>UINT64_MAX/width)throw std::length_error("delivery output workspace overflow");
    add_structured_bytes(total,count*width);
}
template<typename T>struct is_member:std::false_type{};
template<typename Owner,typename Field>struct is_member<observer_delivery_member<Owner,Field>>:std::true_type{};
template<typename T>struct is_members:std::false_type{};
template<typename... Fields>struct is_members<std::tuple<Fields...>>:std::bool_constant<(is_member<Fields>::value&&...)>{};
template<typename T>struct is_output_member:std::false_type{};
template<typename Output,typename Field>
struct is_output_member<observer_delivery_output_member<Output,Field>>:std::true_type{};
template<typename T>struct is_outputs:std::false_type{};
template<typename... Fields>struct is_outputs<std::tuple<Fields...>>:
    std::bool_constant<(is_output_member<Fields>::value&&...)>{};
template<typename Output,typename Field>void materialize_output(Output& output,
    observer_delivery_values::row_view row,const observer_delivery_output_member<Output,Field>& member) {
    if(member.pointer==nullptr)throw std::invalid_argument("delivery output member pointer missing");
    materialize<Field>(output.*(member.pointer),row,member.name,member.geo_names);
}
template<typename Output,typename Field>void bind_output(Output& output,
    const observer_delivery_output_member<Output,Field>& member) {
    // Only the exact builtin managed<Field> receives this call. No model's
    // custom bind_to_db/materializer is selected by its method spelling.
    const std::string name(member.name);
    (output.*(member.pointer)).bind_to_parent(&output,name.c_str());
}
template<typename Output,typename Field>void adopt_output(Output& output,int64_t rowid,
    const observer_delivery_output_member<Output,Field>& member)noexcept {
    (output.*(member.pointer)).row_id=rowid;
}
template<typename Owner,typename Field>void append(observer_delivery_values::field_buffer& out,
    const Owner& source,const observer_delivery_member<Owner,Field>& member) {
    if(member.pointer==nullptr)throw std::invalid_argument("delivery input member pointer missing");
    const auto& value=source.*(member.pointer); // builtin member access, no collector
    if constexpr(std::is_same_v<Field,geo_bounds>) {
        out.append_scalar(member.geo_names[0],value.min_lat);out.append_scalar(member.geo_names[1],value.max_lat);
        out.append_scalar(member.geo_names[2],value.min_lon);out.append_scalar(member.geo_names[3],value.max_lon);
    } else if constexpr(std::is_same_v<Field,std::optional<geo_bounds>>) {
        if(value) {
            out.append_scalar(member.geo_names[0],value->min_lat);out.append_scalar(member.geo_names[1],value->max_lat);
            out.append_scalar(member.geo_names[2],value->min_lon);out.append_scalar(member.geo_names[3],value->max_lon);
        } else for(const auto name:member.geo_names)out.append(name,nullptr);
    } else out.append_scalar(member.name,value);
}
}
class observer_delivery_swift_input;
class observer_delivery_values::cursor {
    using reader=row_view(*)(const void*,size_t,field_buffer&);
    const void* data_=nullptr;size_t rows_=0;reader read_=nullptr;
    cursor(const void* data,size_t rows,reader read)noexcept:data_(data),rows_(rows),read_(read){}
    friend class observer_delivery_swift_input; // closed Bridge implementation
    template<typename T>static row_view read_typed(const void* raw,size_t index,field_buffer& out) {
        const auto& value=static_cast<const T*>(raw)[index];
        const auto& members=managed<T,void>::observer_delivery_members;
        std::apply([&](const auto&... member){(observer_delivery_value_detail::append(out,value,member),...);},members);
        return {managed<T,void>::observer_delivery_table,out.fields()};
    }
public:
    cursor()=default;
    template<typename T>static cursor typed(std::span<const T> input) {
        if constexpr(requires{managed<T,void>::observer_delivery_members;managed<T,void>::observer_delivery_table;}) {
            using Members=std::remove_cvref_t<decltype(managed<T,void>::observer_delivery_members)>;
            using Table=decltype(managed<T,void>::observer_delivery_table);
            if constexpr(observer_delivery_value_detail::is_members<Members>::value&&
                         std::is_same_v<Table,const std::string_view>) {
                static_assert(std::is_const_v<std::remove_reference_t<decltype((managed<T,void>::observer_delivery_members))>>,
                    "delivery member metadata must be immutable DATA");
                return {input.data(),input.size(),read_typed<T>};
            } else throw std::invalid_argument("delivery producer member/table metadata is not closed DATA");
        } else throw std::invalid_argument("delivery producer has no closed member metadata");
    }
    size_t size()const noexcept{return rows_;}
    row_view read(size_t index,field_buffer& out)const {
        if(!read_||index>=rows_)throw std::out_of_range("delivery producer row index");
        out.reset();return read_(data_,index,out);
    }
};
}
