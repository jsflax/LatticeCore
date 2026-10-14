// Included in ordinary_receiver_session.cpp after the existing strict original
// decoders. This is bounded passive decoding, never authenticated admission.
namespace history_receive_codec {
json parse_frame(std::string_view raw){
    if(raw.empty()||raw.size()>ordinary_sync_limits::frame_bytes)reject("history frame byte bound");
    // Unknown textBytes can contain twice the raw TEXT byte limit. Other
    // fields are narrowed below. The same actual native pool is pre-reserved.
    {sax check(raw.size(),2*ordinary_sync_limits::scalar_bytes);if(!json::sax_parse(raw.begin(),raw.end(),&check))reject("history invalid or over-budget JSON");}
    auto root=json::parse(raw.begin(),raw.end());keys(root,{"latticeOrdinarySync"});auto value=std::move(root["latticeOrdinarySync"]);
    if(!value.is_object()||!value.contains("version")||signed_number(value["version"])!=2||!value.contains("profile")||value["profile"]!="paged-history-v1")reject("history profile/version differs");return value;
}
void suffix(const json& value,std::string_view raw){const auto& expected=text(value["digest"],64);digest_text(expected);const auto last=",\"digest\":\""+expected+"\"}}";
    if(raw.size()<last.size()||raw.substr(raw.size()-last.size())!=last||digest(raw.substr(0,raw.size()-last.size()))!=expected)reject("history exact digest framing differs");}
ordinary_history_descriptor descriptor(std::string_view raw,const ordinary_history_limits& limits){
    auto value=parse_frame(raw);keys(value,{"version","profile","kind","source","epoch","cut","stage","mode","base","scope","schema","catalog","captureDevice","captureInode","rows","knownOriginals","unknownOriginals","pages","contentDigest","models","tables","digest"});suffix(value,raw);
    if(value["kind"]!="historyCut"||value["mode"]!="full"||!value["base"].is_null())reject("history cut mode differs");
    decoded_budget budget;budget.maximum=ordinary_history_limits::metadata_bytes;
    budget.add(sizeof(ordinary_history_descriptor)+raw.size());ordinary_history_descriptor result;auto& frame=result.projection;frame.wire_profile=true;
    result.header=raw;result.header_digest=budget.copy(text(value["digest"],64));result.stage=budget.copy(text(value["stage"],36));uuid(result.stage);
    frame.source=budget.copy(text(value["source"],36));uuid(frame.source);frame.epoch=decimal(value["epoch"]);frame.cut=decimal(value["cut"]);if(frame.epoch!=1)reject("history unproved epoch transition");
    frame.scope=budget.copy(text(value["scope"],caps::models*260));frame.schema=budget.copy(text(value["catalog"],caps::schema_bytes));const auto& schema=text(value["schema"],64);digest_text(schema);if(digest(frame.schema)!=schema)reject("history catalog digest differs");
    unsigned_decimal(value["captureDevice"]);unsigned_decimal(value["captureInode"]);frame.device=budget.copy(text(value["captureDevice"],32));frame.inode=budget.copy(text(value["captureInode"],32));
    result.rows=uint64_t(decimal(value["rows"],int64_t(limits.records),true));result.known=uint64_t(decimal(value["knownOriginals"],int64_t(limits.records),true));result.unknown=uint64_t(decimal(value["unknownOriginals"],int64_t(limits.records),true));result.pages=uint64_t(decimal(value["pages"],int64_t(limits.pages),true));
    if(result.known>limits.records-result.rows||result.unknown>limits.records-result.rows-result.known)reject("history combined record bound");const auto total=result.rows+result.known+result.unknown;
    if((result.pages==0)!=(total==0)||result.pages>total||(total&&result.pages<(total+limits.records_per_page-1)/limits.records_per_page))reject("history impossible page count");
    result.content_digest=budget.copy(text(value["contentDigest"],64));digest_text(result.content_digest);
    const auto& models=value["models"];const auto& tables=value["tables"];
    if(!models.is_array()||models.empty()||models.size()>caps::models||!tables.is_array()||tables.empty()||tables.size()>caps::projection_tables)reject("history scope/table bound");
    budget.array(models.size(),sizeof(std::string));frame.models.reserve(models.size());std::string scope;
    for(const auto& item:models){const auto& name=text(item,256);if(!frame.models.empty()&&frame.models.back()>=name)reject("history model order differs");frame.models.push_back(budget.copy(name));budget.add(name.size()+24);scope+=std::to_string(name.size())+':'+name;}
    if(scope!=frame.scope)reject("history root scope differs");budget.array(tables.size(),sizeof(ordinary_export_table));frame.tables.reserve(tables.size());size_t columns=0;
    for(const auto& item:tables){keys(item,{"name","columns","types"});ordinary_export_table table;table.name=budget.copy(text(item["name"],256));if(!frame.tables.empty()&&frame.tables.back().name>=table.name)reject("history table order differs");
        const auto& names=item["columns"];const auto& types=item["types"];if(!names.is_array()||names.empty()||names.size()>caps::columns_per_table||!types.is_array()||types.size()!=names.size()||names.size()>caps::projection_columns-columns)reject("history projected column bound");columns+=names.size();
        budget.array(names.size(),sizeof(std::string)+sizeof(column_type));table.columns.reserve(names.size());table.types.reserve(types.size());std::set<std::string> unique;
        for(size_t n=0;n<names.size();++n){const auto& name=text(names[n],256);budget.add(sizeof(std::string)+4*sizeof(void*)+name.size());if(!unique.insert(name).second)reject("history duplicate column");const auto type=signed_number(types[n]);if(type<static_cast<int>(column_type::integer)||type>static_cast<int>(column_type::blob))reject("history column type");table.columns.push_back(budget.copy(name));table.types.push_back(static_cast<column_type>(type));}
        if(table.columns[0]!="globalId"||table.types[0]!=column_type::text)reject("history table lacks identity");frame.tables.push_back(std::move(table));}
    return result;
}
column_value_t unknown_cell(const json& value,decoded_budget& budget){
    if(!value.is_array()||value.empty()||value.size()>2)reject("unknown history typed cell shape");const auto& kind=text(value[0],16);
    if(kind=="textBytes"){
        if(value.size()!=2)reject("unknown history text shape");const auto& hex_bytes=text(value[1],2*caps::scalar_bytes,true);if(hex_bytes.size()%2||!std::all_of(hex_bytes.begin(),hex_bytes.end(),hex))reject("unknown history text spelling");
        budget.add(hex_bytes.size()/2);std::string bytes;bytes.reserve(hex_bytes.size()/2);const auto digit=[](char c){return c<='9'?c-'0':c-'a'+10;};for(size_t n=0;n<hex_bytes.size();n+=2)bytes.push_back(char(digit(hex_bytes[n])*16+digit(hex_bytes[n+1])));return bytes;
    }
    if(kind=="realBits"){
        if(value.size()!=2)reject("unknown history REAL shape");const auto& encoded=text(value[1],16);if(encoded.size()!=16||!std::all_of(encoded.begin(),encoded.end(),hex))reject("unknown history REAL bits");uint64_t bits=0;for(char c:encoded)bits=(bits<<4)|uint64_t(c<='9'?c-'0':c-'a'+10);const auto result=std::bit_cast<double>(bits);if(std::isfinite(result))reject("finite unknown REAL uses normal scalar");return result;
    }
    if(kind=="text")reject("unknown historical TEXT must preserve exact bytes");return scalar(value,budget);
}
ordinary_history_decoded_page page(std::string_view raw,const ordinary_history_descriptor& header,
    const ordinary_history_progress& prior,const ordinary_history_limits& limits){
    auto value=parse_frame(raw);keys(value,{"version","profile","kind","source","epoch","cut","stage","index","previousDigest","records","digest"});suffix(value,raw);
    if(value["kind"]!="historyPage"||text(value["source"],36)!=header.projection.source||decimal(value["epoch"])!=header.projection.epoch||decimal(value["cut"])!=header.projection.cut||text(value["stage"],36)!=header.stage||uint64_t(decimal(value["index"],int64_t(limits.pages),true))!=prior.next_page||prior.next_page>=header.pages||text(value["previousDigest"],64,true)!=prior.previous)reject("history page coordinate/order differs");
    const auto& records=value["records"];if(!records.is_array()||records.empty()||records.size()>limits.records_per_page)reject("history page records bound");
    decoded_budget budget;budget.maximum=ordinary_history_limits::row_scratch_bytes-caps::parser_workspace_bytes;budget.add(sizeof(ordinary_history_decoded_page)+raw.size());ordinary_history_decoded_page result;result.raw=raw;result.index=prior.next_page;result.previous=budget.copy(prior.previous);budget.add(64);result.full_digest=digest(raw);
    budget.array(records.size(),sizeof(ordinary_history_record));result.records.reserve(records.size());auto rows=prior.rows,known=prior.known,unknown=prior.unknown;
    for(const auto& item:records){
        if(item.is_object()&&item.contains("row")){keys(item,{"row"});const auto& row=item["row"];keys(row,{"table","values"});if(rows>=header.rows||known||unknown)reject("history row count/phase");
            const auto& name=text(row["table"],256);const auto found=std::lower_bound(header.projection.tables.begin(),header.projection.tables.end(),name,[](const auto& a,const auto& b){return a.name<b;});if(found==header.projection.tables.end()||found->name!=name)reject("history row outside actual descriptor");
            const auto& values=row["values"];if(!values.is_array()||values.size()!=found->columns.size())reject("history row columns differ");ordinary_history_row decoded;decoded.table=size_t(found-header.projection.tables.begin());budget.row(values.size());decoded.values.reserve(values.size());
            for(size_t n=0;n<values.size();++n){if(values[n].is_array()&&values[n].size()==2&&values[n][0]=="text"&&(!values[n][1].is_string()||values[n][1].get_ref<const std::string&>().size()>caps::scalar_bytes))reject("history model text scalar bound");auto v=scalar(values[n],budget);if(!storage(v,found->types[n]))reject("history row storage differs");decoded.values.push_back(std::move(v));}const auto* id=std::get_if<std::string>(&decoded.values[0]);if(!id||id->empty()||id->size()>4096||id->find('\0')!=std::string::npos)reject("history row identity bound");result.records.emplace_back(std::move(decoded));++rows;
        }else if(item.is_object()&&item.contains("original")){
            keys(item,{"original","originalDescriptor"});if(rows!=header.rows||known>=header.known)reject("history known count/phase");ordinary_history_known decoded;decoded.entry=original_entry(item["original"],header.projection.tables,budget);decoded.original=original_descriptor(item["originalDescriptor"],budget);result.records.emplace_back(std::move(decoded));++known;
        }else {
            keys(item,{"unknownOriginal"});const auto& observed=item["unknownOriginal"];keys(observed,{"provenance","audit"});if(observed["provenance"]!="legacy-observed"||rows!=header.rows||unknown>=header.unknown||!observed["audit"].is_array()||observed["audit"].size()!=12)reject("history unknown count/shape");
            ordinary_history_unknown decoded;budget.row(12);decoded.audit.reserve(12);for(const auto& cell:observed["audit"])decoded.audit.push_back(unknown_cell(cell,budget));result.records.emplace_back(std::move(decoded));++unknown;
        }
    }
    if(prior.next_page+1==header.pages&&(rows!=header.rows||known!=header.known||unknown!=header.unknown))reject("history final page leaves missing records");return result;
}
}

ordinary_history_descriptor decode_ordinary_history_descriptor(std::string_view raw,const ordinary_history_limits& limits){return history_receive_codec::descriptor(raw,limits);}
ordinary_history_decoded_page decode_ordinary_history_page(std::string_view raw,const ordinary_history_descriptor& header,const ordinary_history_progress& prior,const ordinary_history_limits& limits){return history_receive_codec::page(raw,header,prior,limits);}
