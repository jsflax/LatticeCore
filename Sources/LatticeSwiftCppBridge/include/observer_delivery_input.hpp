#pragma once
#include "dynamic_object.hpp"
#include <lattice/detail/observer_delivery_values.hpp>

namespace lattice::detail {
class observer_delivery_swift_input final {
    using values=observer_delivery_values;
    static values::row_view raw(const swift_dynamic_object& object,values::field_buffer& fields) {
        // Initial closed INSERT support is flat only. These rejections happen
        // during allocation-free plan validation, before graph clone/JSON/SQL.
        // Relationship operators remain required before production activation.
        if(object.properties.size()>values::column_ceiling||
           object.values.size()>values::column_ceiling)
            throw std::length_error("delivery Swift input field bound");
        if(!object.link_values.empty()||!object.list_values.empty()||
           !object.union_values.empty()||!object.geo_bounds_lists.empty())
            throw std::invalid_argument("delivery relationship producer is not yet closed");
        for(const auto& [name,descriptor]:object.properties) {
            if(descriptor.kind!=property_kind::primitive||descriptor.is_union||descriptor.is_vector)
                throw std::invalid_argument("delivery property producer is not yet closed");
            if(!descriptor.is_geo_bounds) {
                const auto found=object.values.find(name);
                if(found!=object.values.end())fields.append_column(found->first,found->second);
            } else {
                // Reuse existing actual map keys, never allocate name+suffix.
                constexpr std::array<std::string_view,4> suffixes={"_minLat","_maxLat","_minLon","_maxLon"};
                for(const auto suffix:suffixes)for(const auto& [column,value]:object.values) {
                    const std::string_view key=column;
                    if(key.size()==name.size()+suffix.size()&&key.starts_with(name)&&key.ends_with(suffix)) {
                        fields.append_column(key,value);break;
                    }
                }
            }
        }
        return {object.table_name,fields.fields()};
    }
public:
    // Only library-minted closed layouts. This prospective input is fixed
    // pointer/count/mode DATA; it does not build a roots/holds vector or copy
    // an object/collector before plan admission.
    class input final {
        friend class observer_delivery_swift_input;
        enum class layout { pointers,objects,refs,unmanaged };
        const void* data_=nullptr;size_t count_=0;layout layout_=layout::pointers;
        input(const void* data,size_t count,layout kind)noexcept:data_(data),count_(count),layout_(kind){}
    public:
        size_t size()const noexcept{return count_;}
        bool has_originals()const noexcept{return layout_!=layout::unmanaged;}
        dynamic_object* original(size_t index)const {
            if(index>=count_)throw std::out_of_range("delivery Swift input index");
            switch(layout_){
                case layout::pointers:return static_cast<dynamic_object* const*>(data_)[index];
                case layout::objects:return const_cast<dynamic_object*>(&static_cast<const dynamic_object*>(data_)[index]);
                case layout::refs:
#if LATTICE_HAS_FRT
                    {auto* ref=static_cast<dynamic_object_ref* const*>(data_)[index];return ref?ref->impl_.get():nullptr;}
#else
                    return static_cast<const dynamic_object_ref*>(data_)[index].impl_.get();
#endif
                case layout::unmanaged:return nullptr;
            }
            return nullptr;
        }
        const swift_dynamic_object& source(size_t index)const {
            if(index>=count_)throw std::out_of_range("delivery Swift input index");
            if(layout_==layout::unmanaged)return static_cast<const swift_dynamic_object*>(data_)[index];
            const auto* object=original(index);
            if(!object||object->deleted_||object->lattice)
                throw std::invalid_argument("delivery add requires actual unmanaged root");
            return object->unmanaged_;
        }
        // Copy only AFTER admission; actual Swift withExtendedLifetime still
        // retains all wrapper originals throughout the synchronous call.
        std::shared_ptr<dynamic_object> retained(size_t index)const {
            if(index>=count_)throw std::out_of_range("delivery Swift input index");
            if(layout_!=layout::refs)return {};
#if LATTICE_HAS_FRT
            auto* ref=static_cast<dynamic_object_ref* const*>(data_)[index];return ref?ref->impl_:nullptr;
#else
            return static_cast<const dynamic_object_ref*>(data_)[index].impl_;
#endif
        }
    };
private:
    static values::row_view input_row(const void* data,size_t index,values::field_buffer& fields) {
        return raw(static_cast<const input*>(data)->source(index),fields);
    }
public:
    static input objects(std::span<dynamic_object* const> objects)noexcept {
        return {objects.data(),objects.size(),input::layout::pointers};
    }
    static input objects(std::span<dynamic_object> objects)noexcept {
        return {objects.data(),objects.size(),input::layout::objects};
    }
#if LATTICE_HAS_FRT
    static input refs(std::span<dynamic_object_ref* const> refs)noexcept {
#else
    static input refs(std::span<const dynamic_object_ref> refs)noexcept {
#endif
        return {refs.data(),refs.size(),input::layout::refs};
    }
    static input unmanaged(std::span<const swift_dynamic_object> objects)noexcept {
        return {objects.data(),objects.size(),input::layout::unmanaged};
    }
    static values::cursor cursor(const input& source)noexcept {
        return {&source,source.size(),input_row};
    }
};
}
