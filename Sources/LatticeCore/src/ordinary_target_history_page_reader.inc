// Included only after the actual Conn, scope and physical spool originals.
// No descriptor open, mmap, SQL operation, caller offset or whole-chain copy.
struct ordinary_target_history_page_reader::state {
    using spool_state=ordinary_target_history_spool::state;
    // Charge destruction follows actual buffer destruction on every path.
    std::shared_ptr<ordinary_resource_reservation<true>> decoder_charge;
    std::vector<std::uint8_t> buffer;
    const std::weak_ptr<spool_state> spool;
    const spool_state* const spool_identity;
    const ordinary_target_history_spool* const facade;
    const ordinary_target_staging_connection* const connection_identity;
    const ordinary_group_target_receiving_owner* const receiver_identity;
    const ::lattice::database* const shell;
    const void* const scope_identity;
    const void* const output_identity;
    std::shared_ptr<const void> scope_pin; // Actual scope state; ended only after real Native/input destruction.
    const std::uint64_t generation;
    const ordinary_target_history_read_span span;
    const std::uint64_t device,inode;
    const int fd;
    bool io_borrowed=false,operation_returned=false,io_joined=false;
    bool input_ending_attempted=false,input_ending_returned=false;
    std::exception_ptr first_failure;
#ifdef LATTICE_TARGET_MAIN_POSIX
    static constexpr std::size_t read_chunk=65536;
    static constexpr std::size_t read_limit=(ordinary_history_limits{}.page_bytes+read_chunk-1)/read_chunk;
    struct read_call:spool_state::call {
        std::size_t relative=0,requested=0;
        spool_state::snapshot after;
    };
    spool_state::snapshot before;
    std::array<read_call,read_limit> reads{};
    std::size_t read_count=0,read_bytes=0;
    bool calls_known()const noexcept {
        const auto known=[](const spool_state::call& call){return !call.entered||call.returned;};
        const auto snapshot_known=[&](const spool_state::snapshot& actual) {
            return known(actual.directory)&&known(actual.held)&&known(actual.named);
        };
        if(!snapshot_known(before))return false;
        for(std::size_t at=0;at<read_count;++at)
            if(!known(reads[at])||!snapshot_known(reads[at].after))return false;
        return true;
    }
#endif
    state(const std::shared_ptr<spool_state>& original,const ordinary_target_history_spool& spool_original,
        const ::lattice::database& actual,const void* scope,const void* output,ordinary_target_history_read_span coordinates,
        std::shared_ptr<ordinary_resource_reservation<true>> charge)
        :decoder_charge(std::move(charge)),spool(original),spool_identity(original.get()),facade(&spool_original),
         connection_identity(original->connection_identity),receiver_identity(original->receiver_identity),shell(&actual),
         scope_identity(scope),output_identity(output),
         scope_pin(static_cast<const ordinary_target_staging_connection::database_scope*>(scope)->held_),
         generation(static_cast<const ordinary_target_staging_connection::database_scope*>(scope)->held_->generation),
         span(coordinates),device(original->device),inode(original->inode),fd(original->fd){}
};
ordinary_target_history_page_reader::ordinary_target_history_page_reader(std::shared_ptr<state> actual):state_(std::move(actual)){}
ordinary_target_history_page_reader::~ordinary_target_history_page_reader()=default; // No IO, retry or implied refund.
std::size_t ordinary_target_history_page_reader::retained_bytes(std::uint64_t length) {
    // During first new capture the census retains one ended predecessor's
    // fixed IO receipts. It has no buffer, decoder reference or SQL scope pin.
    // Charge BOTH fixed reader banks, but only ONE bounded page allocation.
    constexpr auto fixed=2*(sizeof(state)+sizeof(ordinary_target_history_page_reader))+16*sizeof(void*);
    if(!length||length>ordinary_history_limits{}.page_bytes||length>std::numeric_limits<std::size_t>::max()-fixed)
        birth_fail("history raw reader retained capacity refused");
    return fixed+std::size_t(length); // Charged by the actual phase before issue/allocation.
}
void ordinary_target_history_spool::require_read_extent(const ordinary_target_history_read_span& span,ordinary_launch::deadline end)const {
    const auto data=state_;if(!data)birth_fail("history raw reader spool original missing");data->require_local(end);
    if(data->failed||data->ending_attempted||!data->ready||!data->born||data->fd<0||data->unlinked||
       !data->header_attempted||!data->header_operation_returned||data->header_borrowed||!data->header_joined||
       !data->header_identity||!data->header_length||data->header_checked_bytes!=data->header_length||
       !data->header_write.returned||data->header_write.result!=std::int64_t(data->header_length)||
       !data->header_sync.returned||data->header_sync.result)
        birth_fail("history raw reader lacks actual header/extent IO ending");
    const auto append=data->page_append;const auto last=append?append->state_:nullptr;
    // The immutable final stage byte value is joined to the actual last
    // appended page, not the preallocated st_size or a caller's watermark.
    if(!last||!last->locator_joined||!last->input_ending_returned||last->input_pin||last->decoder_charge||
       last->first_failure||!last->operation_returned||last->borrowed||!last->io_joined||
       last->fd!=data->fd||last->device!=data->device||last->inode!=data->inode||
       last->index+1!=data->header_identity->pages||last->offset>data->extent_charge||
       last->length>data->extent_charge-last->offset||span.total!=last->offset+last->length||
       span.page>=data->header_identity->pages||span.offset<data->header_length||span.offset>span.total||
       !span.length||span.length>ordinary_history_limits{}.page_bytes||span.length>span.total-span.offset||
       span.total>data->extent_charge)
        birth_fail("history raw reader span differs from actual complete durable prefix");
#ifdef LATTICE_TARGET_MAIN_POSIX
    if(!data->header_calls_known()||!last->calls_known()||span.offset>std::uint64_t(std::numeric_limits<off_t>::max())||
       span.length>std::uint64_t(std::numeric_limits<ssize_t>::max())||
       span.total>std::uint64_t(std::numeric_limits<off_t>::max()))
        birth_fail("history raw reader extent/offset or original calls unavailable");
    std::size_t header_at=0;
    for(std::size_t n=0;n<data->header_read_count;++n) {
        const auto& read=data->header_reads[n];
        if(header_at>data->header_length||!read.returned||!read.bytes_match||read.offset!=header_at||
           !read.requested||read.result!=std::int64_t(read.requested)||read.requested>data->header_length-header_at)
            birth_fail("history raw reader original header read ending differs");
        header_at+=read.requested;
    }
    if(header_at!=data->header_length)birth_fail("history raw reader original header ending incomplete");
#else
    birth_fail("history raw reader unsupported on this platform");
#endif
}
ordinary_target_history_read_span ordinary_target_staging_connection::history_spool_reader_span(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,
    const ::lattice::database& actual,const void* scope_original,const void* output,
    const ordinary_target_history_page_reader* reader,ordinary_launch::deadline end)const {
    require_original(end);const auto native=state_;
    const auto scope=native->active_scope.lock();
    if(!native->history_spool_attempted||native->history_spool.get()!=&spool||native->shell.get()!=&actual||
       native->cleanup_attempted||native->member_fd<0||!scope||scope->original.lock()!=native||
       native->last_scope!=scope||native->active_scope.lock()!=scope||native->scope_identity!=scope.get()||
       !scope->creator_binding(actual)||!scope->schema||scope->cleanup||scope->ending||scope->left||scope->failed||
       scope->native!=actual.db_||scope->history_chain==scope->row_import||scope->history_header||scope->history_page||
       scope->catalog||scope->projection||scope->import_suspension||!scope->schema_begin_observed||
       scope->schema_begin_rc!=SQLITE_DONE||scope->schema_commit_observed||scope->schema_rollback_observed||
       scope->schema_original.lock()!=owner.require_header_schema(*this))
        birth_fail("history raw reader lacks SAME active chain/import BEGIN scope");
    // Validate the opaque facade address against the actual phase field
    // before dereferencing it; coordinates/output cannot construct a scope.
    const auto selected=scope->history_chain?
        actual.target_history_chain_raw_span_(*this,owner,scope_original,output,reader):
        actual.target_row_import_raw_span_(*this,owner,scope_original,output,reader);
    const auto* issued=static_cast<const database_scope*>(scope_original);
    if(!issued||issued->held_!=scope)birth_fail("history raw reader SAME issued scope facade changed");
    issued->require_current(actual);issued->require_schema_work(actual);
    if(reader) {
        const auto phase=reader->state_;
        if(!phase||phase->connection_identity!=this||phase->receiver_identity!=&owner||phase->shell!=&actual||
           phase->scope_identity!=scope_original||phase->output_identity!=output||phase->scope_pin.get()!=scope.get()||
           phase->generation!=scope->generation||native->history_spool->state_->page_reader.get()!=reader)
            birth_fail("history raw reader actual retained lease/scope changed");
    }
    return selected;
}
const std::shared_ptr<ordinary_resource_reservation<true>>& ordinary_target_staging_connection::history_spool_reader_charge(
    const ordinary_target_history_spool& spool,const ordinary_group_target_receiving_owner& owner,const ::lattice::database& actual,
    const void* scope,const void* output,const ordinary_target_history_page_reader* reader,ordinary_launch::deadline end)const {
    (void)history_spool_reader_span(spool,owner,actual,scope,output,reader,end);
    return owner.require_decoder_charge(*this); // SAME receiver; no slot, friendship or writer permission is issued.
}
void ordinary_target_history_page_reader::require_current(const ::lattice::database& actual,const void* scope,
    ordinary_launch::deadline end)const {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    const auto original=data?data->connection.lock():nullptr;const auto owner=data?data->receiver.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||!original||original.get()!=phase->connection_identity||
       !owner||owner.get()!=phase->receiver_identity||phase->facade!=original->state_->history_spool.get()||
       phase->shell!=&actual||phase->scope_identity!=scope||phase->input_ending_attempted||phase->first_failure||
       phase->fd!=data->fd||phase->device!=data->device||phase->inode!=data->inode||!phase->decoder_charge||
       phase->decoder_charge!=original->history_spool_reader_charge(*phase->facade,*owner,actual,scope,phase->output_identity,this,end))
        birth_fail("history raw reader SAME live original unavailable");
    const auto selected=original->history_spool_reader_span(*phase->facade,*owner,actual,scope,phase->output_identity,this,end);
    if(selected.page!=phase->span.page||selected.offset!=phase->span.offset||selected.length!=phase->span.length||selected.total!=phase->span.total)
        birth_fail("history raw reader actual retained metadata changed");
    phase->facade->require_read_extent(selected,end);
}
const std::vector<std::uint8_t>& ordinary_target_history_page_reader::bytes(const ::lattice::database& actual,
    const void* scope,ordinary_launch::deadline end)const {
    require_current(actual,scope,end);if(!io_ending_known())birth_fail("history raw reader exact IO ending unavailable");
    return state_->buffer;
}
bool ordinary_target_history_page_reader::io_ending_known()const noexcept {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||!data->creator_current()||phase->first_failure||
       phase->io_borrowed||!phase->operation_returned||!phase->io_joined||phase->fd!=data->fd||
       phase->device!=data->device||phase->inode!=data->inode)return false;
#ifdef LATTICE_TARGET_MAIN_POSIX
    if(!phase->calls_known()||!phase->before.returned)return false;
    std::size_t at=0;
    for(std::size_t n=0;n<phase->read_count;++n) {
        const auto& read=phase->reads[n];
        if(std::uint64_t(at)>phase->span.length||read.relative!=at||!read.returned||!read.requested||
           read.result!=std::int64_t(read.requested)||std::uint64_t(read.requested)>phase->span.length-std::uint64_t(at)||
           !read.after.returned)return false;
        at+=read.requested;
    }
    return std::uint64_t(at)==phase->span.length&&std::uint64_t(phase->read_bytes)==phase->span.length;
#else
    return false;
#endif
}
bool ordinary_target_history_page_reader::input_ending_known()const noexcept {
    const auto phase=state_;
    return phase&&phase->input_ending_attempted&&phase->input_ending_returned&&phase->buffer.empty()&&
        !phase->buffer.capacity()&&!phase->decoder_charge&&!phase->scope_pin&&io_ending_known();
}
bool ordinary_target_history_page_reader::cleanup_known()const noexcept {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||!data->creator_current()||phase->io_borrowed||!phase->operation_returned||
       (phase->input_ending_attempted&&!phase->input_ending_returned))return false;
#ifdef LATTICE_TARGET_MAIN_POSIX
    return phase->calls_known(); // Known failure can be disposed only after real target cleanup.
#else
    return false;
#endif
}
void ordinary_target_history_spool::read_page(const ::lattice::database& actual,const void* scope,const void* output,
    std::shared_ptr<ordinary_target_history_page_reader>& retained,ordinary_launch::deadline end) {
    const auto data=state_;if(!data)birth_fail("history raw reader actual spool missing");data->require_local(end);
    const auto connection=data->connection.lock();const auto owner=data->receiver.lock();
    if(!connection||!owner||retained||output!=&retained||data->failed||data->ending_attempted)
        birth_fail("history raw reader issued output/original unavailable");
    const auto span=connection->history_spool_reader_span(*this,*owner,actual,scope,output,nullptr,end);
    require_read_extent(span,end);
    if(data->read_attempted) {
        if(!data->read_operation_returned||!data->page_reader||!data->page_reader->input_ending_known())
            birth_fail("history raw reader previous actual borrower remains held");
        data->read_observation=data->page_reader; // Keeps census snapshot storage alive during first new capture.
    }
    if(data->read_attempts>=2*ordinary_history_limits{}.pages)birth_fail("history raw reader fixed phase count refused");
    ++data->read_attempts;data->read_attempted=true;data->read_operation_returned=false;
    try {
        const auto charge=connection->history_spool_reader_charge(*this,*owner,actual,scope,output,nullptr,end);
        auto phase=std::make_shared<ordinary_target_history_page_reader::state>(data,*this,actual,scope,output,span,charge);
        retained=std::shared_ptr<ordinary_target_history_page_reader>(new ordinary_target_history_page_reader(phase));
        data->page_reader=retained; // Both actual phase and spool hold before allocation or first Native call.
        retained->require_current(actual,scope,end);
        phase->buffer.resize(std::size_t(span.length));
        if(phase->buffer.capacity()!=std::size_t(span.length))birth_fail("history raw reader exact retained buffer capacity differs");
#ifdef LATTICE_TARGET_MAIN_POSIX
        phase->io_borrowed=true;
        retained->require_current(actual,scope,end);data->capture(phase->before);
        data->require_snapshot(phase->before,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        data->read_observation.reset(); // New actual first snapshot now owns the census observation.
        for(std::size_t relative=0;std::uint64_t(relative)<span.length;) {
            const auto count=std::min<std::size_t>(ordinary_target_history_page_reader::state::read_chunk,std::size_t(span.length)-relative);
            if(phase->read_count==phase->reads.size()||std::uint64_t(relative)>span.length||
               std::uint64_t(count)>span.length-std::uint64_t(relative)||span.offset>span.total||
               std::uint64_t(relative)>span.total-span.offset||std::uint64_t(count)>span.total-span.offset-std::uint64_t(relative))
                birth_fail("history raw reader exact relative span refused");
            auto& read=phase->reads[phase->read_count++];read.relative=relative;read.requested=count;read.attempted=true;
            retained->require_current(actual,scope,end);errno=0;read.entered=true;
            const auto bytes=::pread(phase->fd,phase->buffer.data()+relative,count,off_t(span.offset+std::uint64_t(relative)));
            const int error=bytes<0?errno:0;state::returned(read,bytes,error);
            data->capture(read.after); // Actual first result is retained before any physical/current diagnostics.
            data->require_snapshot(read.after,std::int64_t(data->extent_charge),data->extent_charge/512,true);
            if(bytes<0||std::size_t(bytes)!=count)birth_fail("history raw reader first pread short or refused");
            retained->require_current(actual,scope,end);phase->read_bytes+=count;relative+=count;
        }
        retained->require_current(actual,scope,end);
        phase->operation_returned=true;phase->io_borrowed=false;phase->io_joined=true;
        if(!retained->io_ending_known())birth_fail("history raw reader actual one-page IO ending incomplete");
        data->read_operation_returned=true;
#else
        birth_fail("history raw reader unsupported on this platform");
#endif
    }catch(...) {
        data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();
        if(data->creator_current()) {
            if(!retained)data->read_operation_returned=true; // Known pre-Native allocation failure, never a retry grant.
#ifdef LATTICE_TARGET_MAIN_POSIX
            else {
                const auto phase=retained->state_;
                if(!phase->first_failure)phase->first_failure=std::current_exception();
                if(phase->calls_known()){phase->operation_returned=true;phase->io_borrowed=false;data->read_operation_returned=true;}
            }
#endif
        }
        throw;
    }
}
void ordinary_target_staging_connection::read_history_page(::lattice::database& actual,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,database_scope& scope,
    std::shared_ptr<ordinary_target_history_page_reader>& retained,ordinary_launch::deadline end) {
    if(!owner||!state_->history_spool)birth_fail("history raw read actual owner/spool missing");
    (void)history_spool_reader_span(*state_->history_spool,*owner,actual,&scope,&retained,nullptr,end);
    state_->history_spool->read_page(actual,&scope,&retained,retained,end);
}
void ordinary_target_staging_connection::require_history_spool_reader_ending(const ordinary_target_history_spool& spool,
    const ordinary_group_target_receiving_owner& owner,const ::lattice::database& actual,const void* scope_original,
    const ordinary_target_history_page_reader& reader,ordinary_launch::deadline end)const {
    require_original(end);const auto native=state_;const auto phase=reader.state_;
    const auto scope=phase?std::static_pointer_cast<const database_scope::scope_state>(phase->scope_pin):nullptr;
    if(native->history_spool.get()!=&spool||native->shell.get()!=&actual||native->cleanup_attempted||!scope||!phase||
       scope->original.lock()!=native||!native->last_scope||!native->last_scope->left||!native->last_scope->leave_joined||
       !native->last_scope->leave_returned||native->last_scope->failed||scope->generation>native->last_scope->generation||
       !native->active_scope.expired()||native->scope_identity||
       !scope->left||!scope->leave_returned||!scope->leave_joined||scope->failed||scope->ending||scope->cleanup||
       scope->history_chain==scope->row_import||scope->schema_original.lock()!=owner.require_header_schema(*this)||
       phase->scope_pin.get()!=scope.get()||phase->scope_identity!=scope_original||phase->generation!=scope->generation||
       phase->connection_identity!=this||phase->receiver_identity!=&owner||phase->shell!=&actual||
       spool.state_->page_reader.get()!=&reader||!reader.io_ending_known()||!phase->decoder_charge||
       phase->decoder_charge!=owner.require_decoder_charge(*this))
        birth_fail("history raw reader actual Native leave/borrower ending unavailable");
    if(scope->history_chain)actual.require_target_history_chain_raw_retired_(*this,owner,scope_original,reader);
    else actual.require_target_row_import_raw_retired_(*this,owner,scope_original,reader);
}
void ordinary_target_history_page_reader::end_input(const ordinary_target_staging_connection& connection,
    const ordinary_group_target_receiving_owner& owner,const ::lattice::database& actual,const void* scope,ordinary_launch::deadline end) {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||phase->input_ending_attempted||phase->first_failure||
       !phase->decoder_charge)
        birth_fail("history raw reader input ending already claimed or original absent");
    connection.require_history_spool_reader_ending(*phase->facade,owner,actual,scope,*this,end);
    phase->input_ending_attempted=true;
    try {
        {std::vector<std::uint8_t> ended;ended.swap(phase->buffer);} // Actual byte allocation destruction returns first.
        connection.require_history_spool_reader_ending(*phase->facade,owner,actual,scope,*this,end);
        phase->decoder_charge.reset(); // Only this lease's reference; receiver admission remains held.
        phase->scope_pin.reset(); // Actual phase still holds its scope/Native receipts; no old-bank overlap on reader reuse.
        phase->input_ending_returned=true;
    }catch(...) {
        if(!phase->first_failure)phase->first_failure=std::current_exception();
        data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();throw;
    }
}
bool ordinary_target_history_page_reader::release_input_after_cleanup()noexcept {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||!cleanup_known()||!data->unlinked||data->fd!=-1||!data->unlink.returned||data->unlink.result||
       !data->unlink_directory_sync.returned||data->unlink_directory_sync.result||!data->close.returned||data->close.result)return false;
    {std::vector<std::uint8_t> ended;ended.swap(phase->buffer);}
    phase->decoder_charge.reset();phase->scope_pin.reset();return true; // Actual failed-target cleanup ending; no new read or acceptance.
}
