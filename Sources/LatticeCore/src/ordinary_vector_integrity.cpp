#include "ordinary_vector_integrity.hpp"
#include <algorithm>
#include <cmath>
#include <cstring>
#include <map>
#include <set>
#include <string_view>

namespace lattice::detail {
namespace {
using row=database::row_t;
using rows=std::vector<row>;
using blob=std::vector<uint8_t>;
constexpr size_t chunk_slots=1024, cell_slots=64;
void need(bool ok,const char* why){if(!ok)throw ordinary_vector_integrity_error(why);}
const column_value_t& value(const row& r,const char* key){const auto i=r.find(key);need(i!=r.end(),"vector integrity result field missing");return i->second;}
int64_t number(const row& r,const char* key){const auto* p=std::get_if<int64_t>(&value(r,key));need(p,"vector integrity integer required");return *p;}
const std::string& text(const row& r,const char* key){const auto* p=std::get_if<std::string>(&value(r,key));need(p,"vector integrity text required");return *p;}
const blob& bytes(const row& r,const char* key){const auto* p=std::get_if<blob>(&value(r,key));need(p,"vector integrity blob required");return *p;}
std::string ordinary_vector_identifier_sql_(std::string_view s){need(!s.empty()&&s.size()<=256&&s.find('\0')==std::string::npos,"vector integrity identifier bound");std::string q="\"";for(char c:s){q+=c;if(c=='\"')q+='\"';}return q+'\"';}
void finite(const blob& b){need(b.size()%sizeof(float)==0,"vector integrity float width");for(size_t i=0;i<b.size();i+=sizeof(float)){float f;std::memcpy(&f,b.data()+i,sizeof(f));need(std::isfinite(f),"vector integrity nonfinite float");}}
bool valid(const blob& b,size_t at){return (b[at/8]&(uint8_t{1}<<(at%8)))!=0;}
int64_t slot_id(const blob& b,size_t at){int64_t n;std::memcpy(&n,b.data()+at*sizeof(n),sizeof(n));return n;}
struct reader {
    const ordinary_metadata_query& query;const ordinary_vector_integrity_limits& limits;ordinary_vector_integrity& result;
    void charge(size_t n){need(n<=limits.bytes-result.copied_bytes,"vector integrity aggregate byte bound");result.copied_bytes+=n;}
    rows get(const std::string& sql,const std::vector<column_value_t>& args,size_t cap,bool append_limit=true){
        need(cap<=limits.rows,"vector integrity query row bound");
        // Existing metadata adapters require a positive admission. For an
        // expected empty set allow one bounded observation, then refuse it.
        auto out=query(append_limit?sql+" LIMIT "+std::to_string(cap+1):sql,args,std::max<size_t>(cap,1));
        need(out.size()<=cap,"vector integrity callback overrun");
        need(out.size()<=limits.rows*8+32-result.inspected_rows,"vector integrity aggregate row bound");result.inspected_rows+=out.size();
        charge(out.size()*sizeof(row));
        for(const auto& r:out){need(r.size()<=8,"vector integrity result field bound");for(const auto& [k,v]:r){charge(k.size()+sizeof(column_value_t));
            if(const auto* s=std::get_if<std::string>(&v))charge(s->size());else if(const auto* b=std::get_if<blob>(&v))charge(b->size());}}
        return out;
    }
    int64_t cookie(const char* name){auto r=get(std::string("PRAGMA ")+name+".schema_version",{},1,false);need(r.size()==1,"vector integrity schema cookie missing");return number(r[0],"schema_version");}
};
struct indexed_row {
    const blob* expected=nullptr;
    int64_t chunk=0,offset=0;
    bool chunk_seen=false,ivf_seen=false;
};
} // namespace
ordinary_vector_integrity validate_ordinary_vector_integrity(const ordinary_derived_metadata& d,
    const ordinary_metadata_query& query,bool expect_trained,const ordinary_vector_integrity_limits& limits){
    need(query&&limits.rows&&limits.rows<=65536&&limits.bytes&&limits.bytes<=256*1024*1024,"vector integrity limits");
    need(d.kind==ordinary_derived_kind::vector&&d.present&&d.dimensions&&d.dimensions<=8192&&d.ivf_nlist<=65536&&
        (!d.ivf_nprobe||(d.ivf_nlist&&d.ivf_nprobe<=d.ivf_nlist))&&(!expect_trained||d.ivf_nlist),"vector integrity actual profile");
    static_assert(sizeof(float)==4&&sizeof(int64_t)==8);
    const auto model="main."+ordinary_vector_identifier_sql_(d.model),column=ordinary_vector_identifier_sql_(d.property);
    (void)ordinary_vector_identifier_sql_(d.table);
    const auto shadow=[&](const char* suffix){return "main."+ordinary_vector_identifier_sql_(d.table+suffix);};
    const size_t width=d.dimensions*sizeof(float);
    ordinary_vector_integrity result;reader read{query,limits,result};
    const auto main_cookie=read.cookie("main"),temp_cookie=read.cookie("temp");
    auto expected=read.get("SELECT globalId AS gid,"+column+" AS vector FROM "+model+" WHERE "+column+" IS NOT NULL AND length("+column+")>0 ORDER BY CAST(globalId AS BLOB)",{},limits.rows);
    std::map<std::string_view,const blob*,ordinary_identifier_less> by_gid;
    for(const auto& r:expected){const auto& gid=text(r,"gid");const auto& b=bytes(r,"vector");
        need(!gid.empty()&&gid.size()<=4096&&gid.find('\0')==std::string::npos&&b.size()==width,"vector integrity model row shape");finite(b);
        read.charge(sizeof(decltype(by_gid)::value_type));need(by_gid.emplace(gid,&b).second,"vector integrity model identity collision");}
    result.rows=expected.size();
    auto ids=read.get("SELECT rowid AS rid,id AS gid,chunk_id AS chunk,chunk_offset AS offset FROM "+shadow("_rowids")+" ORDER BY rowid",{},result.rows);
    need(ids.size()==result.rows,"vector integrity rowid count differs");
    std::map<int64_t,indexed_row> by_rowid;std::set<std::string_view,ordinary_identifier_less> seen_gids;
    for(const auto& r:ids){const auto rid=number(r,"rid"),chunk=number(r,"chunk"),offset=number(r,"offset");const auto& gid=text(r,"gid");
        const auto e=by_gid.find(gid);need(e!=by_gid.end()&&e->first==std::string_view(gid),"vector integrity installed identity bytes differ");
        need(rid>0&&chunk>0&&offset>=0&&offset<int64_t(chunk_slots),"vector integrity rowid position");
        read.charge(sizeof(decltype(by_rowid)::value_type)+sizeof(std::string_view));
        need(seen_gids.insert(gid).second&&by_rowid.emplace(rid,indexed_row{e->second,chunk,offset}).second,"vector integrity duplicate installed identity");}
    auto chunks=read.get("SELECT chunk_id AS cid,size,validity,rowids FROM "+shadow("_chunks")+" ORDER BY chunk_id",{},result.rows);
    result.chunks=chunks.size();std::set<int64_t> chunk_ids;
    for(const auto& r:chunks){const auto cid=number(r,"cid");const auto& bits=bytes(r,"validity");const auto& rowids=bytes(r,"rowids");
        need(cid>0&&number(r,"size")==int64_t(chunk_slots)&&bits.size()==chunk_slots/8&&rowids.size()==chunk_slots*sizeof(int64_t),"vector integrity chunk shape");
        read.charge(sizeof(int64_t));need(chunk_ids.insert(cid).second,"vector integrity duplicate chunk");size_t used=0,last=0;
        for(size_t at=0;at<chunk_slots;++at)if(valid(bits,at)){const auto rid=slot_id(rowids,at);auto i=by_rowid.find(rid);
            need(i!=by_rowid.end()&&!i->second.chunk_seen&&i->second.chunk==cid&&i->second.offset==int64_t(at),"vector integrity orphan/duplicate/wrong chunk slot");
            i->second.chunk_seen=true;++used;last=at+1;}
        need(used,"vector integrity empty rebuilt chunk");
        if(!d.ivf_nlist){
            // Pinned vec0 addresses this table by SQLite's physical _rowid_,
            // while its declared untyped rowid is a separate unique key.
            // Compare both; copy only through the highest live slot.
            auto vector=read.get("SELECT _rowid_ AS physical,rowid AS cid,length(vectors) AS size,substr(vectors,1,?) AS vector FROM "+shadow("_vector_chunks00")+" WHERE rowid=?",{int64_t(last*width),cid},1);
            need(vector.size()==1&&number(vector[0],"physical")==cid&&number(vector[0],"cid")==cid&&number(vector[0],"size")==int64_t(chunk_slots*width),"vector integrity flat chunk identity/size");
            const auto& b=bytes(vector[0],"vector");need(b.size()==last*width,"vector integrity truncated flat bytes");
            for(size_t at=0;at<last;++at)if(valid(bits,at)){const auto& expected_bytes=*by_rowid.at(slot_id(rowids,at)).expected;
                need(std::equal(expected_bytes.begin(),expected_bytes.end(),b.begin()+at*width),"vector integrity flat slot bytes differ");}
        }
    }
    for(const auto& [rid,r]:by_rowid)need(r.chunk_seen,"vector integrity missing chunk slot");
    if(!d.ivf_nlist){
        auto flat=read.get("SELECT rowid AS cid FROM "+shadow("_vector_chunks00")+" ORDER BY rowid",{},result.chunks);
        need(flat.size()==chunk_ids.size(),"vector integrity flat chunk count");for(const auto& r:flat)need(chunk_ids.erase(number(r,"cid"))==1,"vector integrity orphan flat chunk");
    }else{
        auto marker=read.get("SELECT value AS trained FROM "+shadow("_info")+" WHERE key='ivf_trained_0'",{},1);
        need(marker.size()==1&&number(marker[0],"trained")==int64_t(expect_trained),"vector integrity actual trained state differs");result.trained=expect_trained;
        const size_t expected_centroids=expect_trained?std::min(d.ivf_nlist,result.rows):0;
        need(!expect_trained||expected_centroids,"vector integrity trained empty index");
        auto centroids=read.get("SELECT centroid_id AS cid,centroid FROM "+shadow("_ivf_centroids00")+" ORDER BY centroid_id",{},expected_centroids);
        need(centroids.size()==expected_centroids,"vector integrity centroid count");result.centroids=centroids.size();
        for(size_t i=0;i<centroids.size();++i){need(number(centroids[i],"cid")==int64_t(i)&&bytes(centroids[i],"centroid").size()==width,"vector integrity centroid identity/width");finite(bytes(centroids[i],"centroid"));}
        auto maps=read.get("SELECT rowid AS rid,cell_id AS cell,slot FROM "+shadow("_ivf_rowid_map00")+" ORDER BY rowid",{},result.rows);
        need(maps.size()==result.rows,"vector integrity IVF map count");std::map<int64_t,std::pair<int64_t,int64_t>> positions;
        for(const auto& r:maps){const auto rid=number(r,"rid"),cell=number(r,"cell"),slot=number(r,"slot");
            need(by_rowid.count(rid)&&cell>0&&slot>=0&&slot<int64_t(cell_slots),"vector integrity IVF map position");
            read.charge(sizeof(decltype(positions)::value_type));need(positions.emplace(rid,std::make_pair(cell,slot)).second,"vector integrity duplicate IVF map");}
        auto cells=read.get("SELECT rowid AS cid,centroid_id AS centroid,n_vectors,validity,rowids,length(vectors) AS size FROM "+shadow("_ivf_cells00")+" ORDER BY rowid",{},result.rows);
        result.cells=cells.size();std::set<int64_t> seen_cells;
        for(const auto& r:cells){const auto cid=number(r,"cid"),centroid=number(r,"centroid"),count=number(r,"n_vectors");const auto& bits=bytes(r,"validity");const auto& rowids=bytes(r,"rowids");
            need(cid>0&&count>0&&count<=int64_t(cell_slots)&&bits.size()==cell_slots/8&&rowids.size()==cell_slots*sizeof(int64_t)&&number(r,"size")==int64_t(cell_slots*width),"vector integrity IVF cell shape");
            read.charge(sizeof(int64_t));need(seen_cells.insert(cid).second,"vector integrity duplicate IVF cell");
            need(expect_trained?(centroid>=0&&centroid<int64_t(expected_centroids)):centroid==-1,"vector integrity IVF cell centroid");
            size_t used=0,last=0;for(size_t at=0;at<cell_slots;++at)if(valid(bits,at)){const auto rid=slot_id(rowids,at);auto i=by_rowid.find(rid);auto p=positions.find(rid);
                need(i!=by_rowid.end()&&!i->second.ivf_seen&&p!=positions.end()&&p->second==std::make_pair(cid,int64_t(at)),"vector integrity orphan/duplicate/wrong IVF slot");
                i->second.ivf_seen=true;++used;last=at+1;}
            need(used==size_t(count),"vector integrity IVF live count differs");
            auto vector=read.get("SELECT substr(vectors,1,?) AS vector FROM "+shadow("_ivf_cells00")+" WHERE rowid=?",{int64_t(last*width),cid},1);
            need(vector.size()==1&&bytes(vector[0],"vector").size()==last*width,"vector integrity IVF payload missing/truncated");const auto& b=bytes(vector[0],"vector");
            for(size_t at=0;at<last;++at)if(valid(bits,at)){const auto& expected_bytes=*by_rowid.at(slot_id(rowids,at)).expected;
                need(std::equal(expected_bytes.begin(),expected_bytes.end(),b.begin()+at*width),"vector integrity IVF slot bytes differ");}
        }
        for(const auto& [rid,r]:by_rowid)need(r.ivf_seen,"vector integrity missing IVF slot");
    }
    need(main_cookie==read.cookie("main")&&temp_cookie==read.cookie("temp"),"vector integrity retained schema changed");
    return result;
}
} // namespace lattice::detail
