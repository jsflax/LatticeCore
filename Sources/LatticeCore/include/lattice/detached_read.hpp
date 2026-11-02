#pragma once
#include "lattice.hpp"
#include <cmath>
#include <algorithm>
#include <map>
#include <set>

namespace lattice {
// The existing maintenance scope serializes the complete read transaction and suppresses
// settled hook delivery until after the scope unlocks. No raw handle or new connection is used.
struct detached_read_access {
    database& db;
    database::maintenance_scope ownership;
    bool active = false;
    explicit detached_read_access(database& value) : db(value), ownership(value) {
        db.execute("BEGIN DEFERRED"); active = true;
    }
    void finish() { if (active) { db.rollback(); active = false; } }
    ~detached_read_access() noexcept { if (active) { try { db.rollback(); } catch (...) {} } }
    detached_read_access(const detached_read_access&) = delete;
    detached_read_access& operator=(const detached_read_access&) = delete;
};

namespace detached_read {
constexpr size_t max_ids = 4, max_fields = 32, max_metadata_rows = 256;
constexpr size_t max_blob_cell = 262144, max_blob_bytes = 1048576;
constexpr size_t max_text_cell = 4096, max_metadata_bytes = 262144, max_binary_metadata_bytes = 65536;
struct selection { std::string table; std::vector<std::string> properties; };
struct exact_request : selection { std::vector<std::string> global_ids; };
struct indexed_request : selection { std::string property, equal_to; int64_t after_id = 0; size_t limit = 256; };
struct row { int64_t id = 0; std::string table, global_id; std::map<std::string, column_value_t> values; };
struct result { std::vector<row> rows; audit_read_fence fence; bool has_more = false; int64_t next_id = 0; };
inline void require(bool value, const char* message) { if (!value) throw db_error(message); }
inline bool identifier(const std::string& value) {
    if (value.empty() || value.size()>128) return false;
    for (size_t i=0;i<value.size();++i) { const auto c=value[i];
        if (!(c=='_' || (c>='A'&&c<='Z') || (c>='a'&&c<='z') || (i && c>='0'&&c<='9'))) return false;
    } return true;
}
inline std::string folded(std::string value) { for (auto& c:value) if(c>='A'&&c<='Z') c+=32; return value; }
inline bool uuid(const std::string& value) {
    if (value.size()!=36) return false;
    for(size_t i=0;i<36;++i) { const char c=value[i]; const bool dash=i==8||i==13||i==18||i==23;
        if(dash ? c!='-' : !((c>='0'&&c<='9')||(c>='a'&&c<='f'))) return false;
    } return true;
}
inline std::string quote(const std::string& value) { require(identifier(value),"Invalid scoped read identifier"); return "\""+value+"\""; }
inline int64_t integer(const database::row_t& row, const std::string& key) {
    const auto at=row.find(key); require(at!=row.end() && std::holds_alternative<int64_t>(at->second),"Invalid scoped read integer"); return std::get<int64_t>(at->second);
}
inline std::string text(const database::row_t& row, const std::string& key) {
    const auto at=row.find(key); require(at!=row.end() && std::holds_alternative<std::string>(at->second),"Invalid scoped read text"); return std::get<std::string>(at->second);
}
inline bool same_fence(const audit_read_fence& a,const audit_read_fence& b) {
    return a.cursor.head==b.cursor.head && a.cursor.floor==b.cursor.floor && a.cursor.head_global_id==b.cursor.head_global_id &&
        a.has_history_generation==b.has_history_generation && a.history_generation==b.history_generation;
}
inline bool utf8(const std::string& value) {
    for(size_t i=0;i<value.size();) {
        const auto c=static_cast<unsigned char>(value[i++]); if(c<0x80) continue;
        size_t n; uint32_t code,minimum;
        if(c>=0xc2&&c<=0xdf) {n=1;code=c&31;minimum=0x80;}
        else if(c>=0xe0&&c<=0xef) {n=2;code=c&15;minimum=0x800;}
        else if(c>=0xf0&&c<=0xf4) {n=3;code=c&7;minimum=0x10000;} else return false;
        if(value.size()-i<n) return false;
        while(n--) {const auto next=static_cast<unsigned char>(value[i++]);if((next&0xc0)!=0x80)return false;code=(code<<6)|(next&63);}
        if(code<minimum||code>0x10ffff||(code>=0xd800&&code<=0xdfff))return false;
    } return true;
}
struct field { std::string name,column; property_descriptor descriptor; };
template<class Owner> std::vector<field> fields(const Owner& owner,const selection& request,bool metadata) {
    require(identifier(request.table),"Invalid scoped read table");
    require(!request.properties.empty()&&request.properties.size()<=max_fields,"Scoped read property bound exceeded");
    const auto* schema=owner.get_properties_for_table(request.table);
    require(schema!=nullptr,"Scoped read requires a registered table");
    std::set<std::string> names,columns; std::vector<field> output;
    for(const auto& name:request.properties) {
        require(identifier(name)&&folded(name)!="id"&&folded(name)!="globalid"&&names.insert(folded(name)).second,"Invalid scoped read property");
        const auto at=schema->find(name); require(at!=schema->end(),"Unknown scoped read property");
        const auto& property=at->second;
        require(property.kind==property_kind::primitive && !property.is_vector && !property.is_geo_bounds,"Scoped read requires primitive fields");
        require(!metadata||property.type!=column_type::blob,"Metadata reads cannot select blobs");
        const auto column=property.column_name.empty()?name:property.column_name;
        require(identifier(column)&&folded(column)!="id"&&folded(column)!="globalid"&&columns.insert(folded(column)).second,"Invalid scoped read physical column");
        output.push_back({name,column,property});
    } return output;
}
inline void table(database& db,const std::string& name) {
    const auto attached=db.query("SELECT COUNT(*) AS n FROM pragma_database_list WHERE name NOT IN ('main','temp')");
    require(attached.size()==1&&integer(attached.front(),"n")==0,"Scoped read refuses attached databases");
    const auto tables=db.query("SELECT type='table' AND upper(substr(sql,1,20)) NOT LIKE 'CREATE VIRTUAL TABLE%' AS valid FROM main.sqlite_schema WHERE name=? LIMIT 2",{name});
    require(tables.size()==1&&integer(tables.front(),"valid")==1,"Scoped read requires an ordinary main table");
    // Registered properties do not prove the actual system identity shape after corruption.
    // Every later WHERE id=? must retain an indexed, unique integer primary-key lookup.
    const auto primary=db.query("SELECT COUNT(*) AS n, SUM(name='id' AND pk=1 AND type='INTEGER' COLLATE NOCASE) AS valid FROM pragma_table_info(?,'main') WHERE pk>0",{name});
    require(primary.size()==1&&integer(primary.front(),"n")==1&&integer(primary.front(),"valid")==1,"Scoped read requires one INTEGER id primary key");
}
// Actual SQLite leading columns, not a caller-declared index promise. Partial/expression indexes
// are excluded. Bound catalog output before copying identifiers; no CREATE/repair takes place.
inline std::string index(database& db,const std::string& table,const std::string& column,bool unique) {
    const auto indexes=db.query("SELECT CASE WHEN typeof(name)='text' AND length(CAST(name AS BLOB))<=128 THEN name END AS name, \"unique\" AS is_unique, partial FROM pragma_index_list(?,'main') LIMIT 65",{table});
    require(indexes.size()<=64,"Scoped read index catalog bound exceeded");
    for(const auto& item:indexes) {
        if(integer(item,"partial")!=0 || (unique&&integer(item,"is_unique")!=1)) continue;
        const auto name=text(item,"name"); require(identifier(name),"Invalid scoped read index name");
        const auto parts=db.query("SELECT CASE WHEN typeof(name)='text' AND length(CAST(name AS BLOB))<=128 THEN name END AS name, cid, \"key\" AS is_key FROM pragma_index_xinfo(?,'main') WHERE \"key\"=1 ORDER BY seqno LIMIT 2",{name});
        if(!parts.empty() && (!unique || parts.size()==1) && integer(parts.front(),"cid")>=0 && integer(parts.front(),"is_key")==1 && text(parts.front(),"name")==column) return name;
    }
    throw db_error("Scoped read requires an actual leading-column index");
}
struct budget {
    size_t blobs=0,metadata=0; bool metadata_only;
    void charge(int64_t amount,bool blob) {
        require(amount>=0 && amount<=static_cast<int64_t>(blob?max_blob_cell:max_text_cell),"Scoped read cell byte bound exceeded"); const auto n=static_cast<size_t>(amount);
        if(blob) { require(!metadata_only&&n<=max_blob_cell&&n<=max_blob_bytes-blobs,"Scoped read blob byte bound exceeded");blobs+=n; }
        else {const auto max=metadata_only?max_metadata_bytes:max_binary_metadata_bytes;
            require(n<=max_text_cell&&n<=max-metadata,"Scoped read metadata byte bound exceeded");metadata+=n;}
    }
};
inline std::string type_predicate(const field& field) {
    const auto q=quote(field.column),type="typeof("+q+")";
    std::string accepted;
    switch(field.descriptor.type) {
        case column_type::integer: accepted=type+"='integer'";break;
        case column_type::real: accepted=type+" IN ('integer','real')";break;
        case column_type::text: accepted=type+"='text'";break;
        case column_type::blob: accepted=type+"='blob'";break;
    }
    if(field.descriptor.nullable) accepted="("+accepted+" OR "+type+"='null')";
    return accepted;
}
inline std::vector<row> hydrate(database& db,const selection& request,const std::vector<field>& selected,
                                const std::vector<int64_t>& ids,bool metadata) {
    budget total{0,0,metadata}; std::vector<row> output;
    // Preflight ALL selected rows before hydrating ANY selected variable field.
    for(const auto id:ids) {
        std::string sql="SELECT CASE WHEN typeof(globalId)='text' AND length(CAST(globalId AS BLOB))=36 THEN globalId END AS global_id";
        for(size_t i=0;i<selected.size();++i) {
            sql+=",("+type_predicate(selected[i])+") AS t"+std::to_string(i);
            sql+=",CASE WHEN typeof("+quote(selected[i].column)+") IN ('integer','real') THEN 8 ELSE COALESCE(length(CAST("+quote(selected[i].column)+" AS BLOB)),0) END AS n"+std::to_string(i);
        }
        sql+=" FROM main."+quote(request.table)+" WHERE id=? LIMIT 2";
        const auto rows=db.query(sql,{id}); require(rows.size()==1,"Scoped read row identity changed");
        require(uuid(text(rows.front(),"global_id")),"Invalid scoped read row UUID");total.charge(36,false);
        for(size_t i=0;i<selected.size();++i) {
            require(integer(rows.front(),"t"+std::to_string(i))==1,"Scoped read stored type mismatch");
            total.charge(integer(rows.front(),"n"+std::to_string(i)),selected[i].descriptor.type==column_type::blob);
        }
    }
    for(const auto id:ids) {
        std::string sql="SELECT globalId"; for(const auto& field:selected) sql+=","+quote(field.column);
        sql+=" FROM main."+quote(request.table)+" WHERE id=? LIMIT 2";
        const auto rows=db.query(sql,{id});require(rows.size()==1,"Scoped read row disappeared");
        row copy;copy.id=id;copy.table=request.table;copy.global_id=text(rows.front(),"globalId");require(uuid(copy.global_id),"Invalid scoped read UUID");
        for(const auto& field:selected) {
            const auto at=rows.front().find(field.column);require(at!=rows.front().end(),"Missing scoped read column");
            if(const auto value=std::get_if<double>(&at->second)) require(std::isfinite(*value),"Nonfinite scoped read number");
            if(const auto value=std::get_if<std::string>(&at->second)) require(utf8(*value),"Scoped read requires valid UTF8");
            copy.values.emplace(field.name,at->second);
        } output.push_back(std::move(copy));
    } return output;
}
template<class Owner> result capture(const std::shared_ptr<Owner>& owner,const selection& request,
                                     const std::vector<std::string>* exact,const indexed_request* page) {
    require(owner&&!owner->is_closed(),"Scoped read requires an open owner");
    const auto selected=fields(*owner,request,page!=nullptr);
    if(exact) {require(!exact->empty()&&exact->size()<=max_ids,"Scoped read ID bound exceeded");std::set<std::string> seen;
        for(const auto& id:*exact)require(uuid(id)&&seen.insert(id).second,"Invalid or duplicate scoped read ID");}
    else require(page && uuid(page->equal_to)&&page->after_id>=0&&page->limit>0&&page->limit<=max_metadata_rows,"Invalid scoped metadata request");
    auto connection=owner->borrow_read_connection(); require(!owner->is_closed(),"Scoped read owner retired");
    result output;
    {
        detached_read_access scope(*connection);table(*connection,request.table);
        output.fence=lattice_db::read_audit_read_fence_on(*connection);
        std::string sql="SELECT CASE WHEN typeof(id)='integer' AND id>0 THEN id END AS id FROM main."+quote(request.table);
        std::vector<column_value_t> parameters;
        if(exact) {
            const auto key=index(*connection,request.table,"globalId",true);sql+=" INDEXED BY "+quote(key)+" WHERE globalId IN (";
            for(size_t i=0;i<exact->size();++i) {if(i)sql+=",";sql+="?";parameters.emplace_back((*exact)[i]);}
            sql+=") ORDER BY id LIMIT ?";parameters.emplace_back(static_cast<int64_t>(max_ids+1));
        } else {
            const auto* schema=owner->get_properties_for_table(request.table); const auto at=schema->find(page->property);
            require(identifier(page->property)&&at!=schema->end()&&at->second.kind==property_kind::primitive&&at->second.type==column_type::text,"Scoped equality requires a registered text field");
            const auto column=at->second.column_name.empty()?page->property:at->second.column_name;require(identifier(column),"Invalid scoped equality column");
            const auto key=index(*connection,request.table,column,false);
            sql+=" INDEXED BY "+quote(key)+" WHERE "+quote(column)+"=? AND id>? ORDER BY id LIMIT ?";
            parameters={page->equal_to,page->after_id,static_cast<int64_t>(page->limit+1)};
        }
        const auto probes=connection->query(sql,parameters);std::vector<int64_t> ids;
        const auto maximum=exact?exact->size():page->limit;
        require(probes.size()<=maximum+(page?1:0),"Scoped read identity multiplicity exceeded");
        int64_t previous=page?page->after_id:0;
        for(size_t i=0;i<probes.size();++i) {const auto id=integer(probes[i],"id");require(id>previous,"Invalid scoped read keyset");previous=id;if(i<maximum)ids.push_back(id);}
        output.has_more=page&&probes.size()>maximum;
        output.rows=hydrate(*connection,request,selected,ids,page!=nullptr);
        if(exact) for(const auto& row:output.rows) require(std::find(exact->begin(),exact->end(),row.global_id)!=exact->end(),"Foreign scoped read row");
        if(!ids.empty())output.next_id=ids.back();
        require(same_fence(output.fence,lattice_db::read_audit_read_fence_on(*connection)),"Scoped read audit interval changed");
        require(!owner->is_closed()&&!connection->is_closed(),"Scoped read owner retired");scope.finish();
    }
    require(!owner->is_closed()&&!connection->is_closed(),"Scoped read owner retired");return output;
}
template<class Owner> result by_ids(const std::shared_ptr<Owner>& owner,const exact_request& request) {return capture(owner,request,&request.global_ids,nullptr);}
template<class Owner> result by_indexed_uuid(const std::shared_ptr<Owner>& owner,const indexed_request& request) {return capture(owner,request,nullptr,&request);}
} // namespace detached_read
} // namespace lattice
