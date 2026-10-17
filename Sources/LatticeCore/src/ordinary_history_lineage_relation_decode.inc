// Included after the genuine history codec in ordinary_receiver_session.cpp.
// Exact, bounded passive parsing. Actual TLS/owner/request admission is outside
// this codec; neither decoded coordinates nor a frontier can grant installation.
namespace history_lineage_codec {
constexpr size_t wire_bytes=262144,retained_bytes=1024*1024;
void source_scope(const std::string& scope){
    // Root-scope spelling: bounded nonempty canonical length-prefixed UTF-8
    // names. The actual new header independently proves its local root closure.
    size_t at=0,count=0;std::string_view previous;
    while(at<scope.size()){
        const auto colon=scope.find(':',at);if(colon==std::string::npos||colon-at>3)reject("relation scope length spelling");
        size_t length=0;const auto parsed=std::from_chars(scope.data()+at,scope.data()+colon,length);
        if(parsed.ec!=std::errc{}||parsed.ptr!=scope.data()+colon||!length||length>256||std::to_string(length)!=scope.substr(at,colon-at)||length>scope.size()-colon-1)reject("relation scope name bound");
        const std::string_view name(scope.data()+colon+1,length);if(!previous.empty()&&previous>=name)reject("relation scope root order differs");
        previous=name;at=colon+1+length;if(++count>caps::models)reject("relation scope root count bound");
    }
    if(!count)reject("relation scope absent");
}
std::string route_uuid(const std::string& raw){
    auto value=raw;for(auto& c:value)if(c>='A'&&c<='F')c+=('a'-'A');uuid(value);return value;
}
ordinary_history_lineage_predecessor predecessor(const json& value,decoded_budget& budget){
    keys(value,{"scope","source","epoch","issuedFrontier","captureDevice","captureInode","schema"});
    ordinary_history_lineage_predecessor out;
    out.scope=budget.copy(text(value["scope"],caps::models*260));source_scope(out.scope);
    out.source=budget.copy(text(value["source"],36));uuid(out.source);
    out.epoch=decimal(value["epoch"]);if(out.epoch!=1)reject("relation predecessor epoch transition unproved");
    out.issued_frontier=decimal(value["issuedFrontier"],INT64_MAX,true);
    unsigned_decimal(value["captureDevice"]);unsigned_decimal(value["captureInode"]);
    out.device=budget.copy(text(value["captureDevice"],20));out.inode=budget.copy(text(value["captureInode"],20));
    out.schema_digest=budget.copy(text(value["schema"],64));digest_text(out.schema_digest);return out;
}
bool same(const ordinary_history_lineage_predecessor& a,const ordinary_history_lineage_predecessor& b){
    return a.scope==b.scope&&a.source==b.source&&a.schema_digest==b.schema_digest&&a.device==b.device&&a.inode==b.inode&&a.epoch==b.epoch&&a.issued_frontier==b.issued_frontier;
}
size_t retained(const ordinary_history_lineage_relation& value){
    static_assert(sizeof(ordinary_history_lineage_relation)<retained_bytes);
    size_t total=sizeof(value);const auto add=[&](const std::string& s){if(s.capacity()>retained_bytes-total)reject("relation actual retained capacity bound");total+=s.capacity();};
    add(value.request);add(value.profile_identifier);add(value.principal);add(value.server_channel);if(value.group)add(*value.group);
    const auto old=[&](const auto& prior){if(!prior)return;add(prior->scope);add(prior->source);add(prior->schema_digest);add(prior->device);add(prior->inode);};
    old(value.mounted_predecessor);old(value.selected_predecessor);
    const auto& now=value.current;add(now.scope);add(now.source);add(now.schema_digest);add(now.device);add(now.inode);add(now.stage);add(now.header_digest);add(now.content_digest);return total;
}
ordinary_history_lineage_relation decode(std::string_view raw,const ordinary_receiver_history_expectation& expected,const std::string& nonce,const std::string& channel,const std::string& profile){
    if(raw.empty()||raw.size()>wire_bytes)reject("history relation wire byte bound");
    auto value=history_receive_codec::parse_frame(raw);
    keys(value,{"version","profile","kind","relation","request","modelProfile","principal","serverChannel","group","facts"});
    if(value["kind"]!="historyRelation"||value["relation"]!="mounted-lineage-frontier-v1"||value["request"]!=nonce||value["modelProfile"]!=profile||
       expected.profile_identifier!=profile||expected.channel!=channel||expected.channel!="wss:"+expected.endpoint||expected.member.empty())reject("history relation actual request/route differs");
    // The retained native issuer supplied the expected endpoint/channel/member.
    // SDK SyncChannel is a distinct authenticated route convention, never the
    // native wss channel. Canonicalize UUID case only to compare its principal.
    if(expected.group){
        if(!expected.member.starts_with("sync/group-")||!expected.member.ends_with(".sqlite")||route_uuid(expected.member.substr(11,expected.member.size()-18))!=*expected.group)reject("history relation retained archive group differs");
    }else if(expected.member!="sync/memory-synced.sqlite")reject("history relation retained personal archive differs");
    decoded_budget budget;budget.maximum=retained_bytes;budget.add(sizeof(ordinary_history_lineage_relation));ordinary_history_lineage_relation out;
    out.request=budget.copy(text(value["request"],36));uuid(out.request);
    out.profile_identifier=budget.copy(text(value["modelProfile"],64));
    out.principal=budget.copy(text(value["principal"],36));uuid(out.principal);if(out.principal!=expected.principal)reject("history relation authenticated principal differs");
    out.server_channel=budget.copy(text(value["serverChannel"],1024));
    if(expected.group){
        out.group=budget.copy(text(value["group"],36));uuid(*out.group);
        if(out.group!=expected.group||!out.server_channel.starts_with("group-")||route_uuid(out.server_channel.substr(6))!=*out.group)reject("history relation actual authenticated group differs");
    }else if(!value["group"].is_null()||route_uuid(out.server_channel)!=out.principal)reject("history relation personal route differs");
    const auto& facts=value["facts"];keys(facts,{"version","mountedPredecessor","selectedPredecessor","current"});
    if(signed_number(facts["version"])!=1)reject("history relation native observation version differs");
    if(!facts["mountedPredecessor"].is_null())out.mounted_predecessor=predecessor(facts["mountedPredecessor"],budget);
    if(!facts["selectedPredecessor"].is_null())out.selected_predecessor=predecessor(facts["selectedPredecessor"],budget);
    const auto& now=facts["current"];keys(now,{"source","epoch","cut","stage","scope","schema","captureDevice","captureInode","headerDigest","contentDigest"});auto& current=out.current;
    current.source=budget.copy(text(now["source"],36));uuid(current.source);current.stage=budget.copy(text(now["stage"],36));uuid(current.stage);
    current.epoch=decimal(now["epoch"]);current.cut=decimal(now["cut"]);if(current.epoch!=1)reject("history relation new epoch transition unproved");
    current.scope=budget.copy(text(now["scope"],caps::models*260));source_scope(current.scope);
    current.schema_digest=budget.copy(text(now["schema"],64));digest_text(current.schema_digest);
    unsigned_decimal(now["captureDevice"]);unsigned_decimal(now["captureInode"]);current.device=budget.copy(text(now["captureDevice"],20));current.inode=budget.copy(text(now["captureInode"],20));
    current.header_digest=budget.copy(text(now["headerDigest"],64));digest_text(current.header_digest);current.content_digest=budget.copy(text(now["contentDigest"],64));digest_text(current.content_digest);
    const auto physical=[&](const auto& old){if(old&&(old->device!=current.device||old->inode!=current.inode))reject("history relation actual mounted file differs");};physical(out.mounted_predecessor);physical(out.selected_predecessor);
    if(out.selected_predecessor){const auto& old=*out.selected_predecessor;
        if(old.scope!=current.scope||old.source!=current.source||old.epoch!=current.epoch||old.schema_digest!=current.schema_digest||old.issued_frontier==INT64_MAX||old.issued_frontier+1!=current.cut)reject("history relation selected predecessor/advance differs");}
    else if(current.cut!=1)reject("history relation selected predecessor missing for advanced cut");
    if(out.mounted_predecessor&&out.mounted_predecessor->scope==current.scope){
        if(!out.selected_predecessor||!same(*out.mounted_predecessor,*out.selected_predecessor))reject("history relation same-scope mounted predecessor differs");}
    if(expected.group){
        // The actual fixed group mount's full catalog equals its selected three
        // roots. An advanced same-scope cut must observe BOTH prior views.
        if(bool(out.mounted_predecessor)!=bool(out.selected_predecessor)||
           (out.mounted_predecessor&&(!same(*out.mounted_predecessor,*out.selected_predecessor)||out.mounted_predecessor->scope!=current.scope)))reject("history relation actual group full/selected predecessor differs");
    }
    (void)retained(out);return out;
}
void match_header(const ordinary_history_lineage_relation& value,const ordinary_history_descriptor& descriptor){
    const auto& now=value.current;const auto& actual=descriptor.projection;
    if(now.source!=actual.source||now.epoch!=actual.epoch||now.cut!=actual.cut||now.scope!=actual.scope||now.schema_digest!=digest(actual.schema)||now.device!=actual.device||now.inode!=actual.inode||now.stage!=descriptor.stage||now.header_digest!=descriptor.header_digest||now.content_digest!=descriptor.content_digest)reject("history relation differs from actual issued header/content");
    // This match still proves no exact old receipt/digest/baseline, canonical
    // install, known-original application, ACK, pruning or RESULT/debt retirement.
}
}
