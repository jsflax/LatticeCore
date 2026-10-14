// Included only in the actual Conn TU after its state and descriptor ending.
// Raw extent: the existing durable quota includes header + raw pages. No
// framing/index prefix consumes it; later metadata must be charged separately
// in MAIN. The exact SAME wire header is written/read here at offset zero.
// One private pending raw page may be ended before its SQL consumer exists.
// This slice does not route legacy SQL page bytes or join/reuse a raw cursor.
struct ordinary_target_history_spool::state {
    static constexpr const char* leaf="history.raw.v1";
    const std::weak_ptr<ordinary_target_staging_connection> connection;
    const ordinary_target_staging_connection* const connection_identity;
    const std::weak_ptr<ordinary_group_target_receiving_owner> receiver;
    const ordinary_group_target_receiving_owner* const receiver_identity;
    const std::shared_ptr<const ordinary_unpublished_group_target> target;
    const ordinary_launch::process_identity process=ordinary_launch::current_process();
    const std::thread::id thread=std::this_thread::get_id();
    const ordinary_launch::deadline end;
    const int member;
    const std::uint64_t member_device,member_inode;
    // Actual existing protocol quota, never enlarged by remote DATA. Held by
    // the already charged process-lifetime Conn; no replacement or refund.
    const std::uint64_t extent_charge=ordinary_history_limits{}.durable_stage_bytes;
    int fd=-1;
    bool born=false,ready=false,failed=false,ending_attempted=false,ending_joined=false,unlinked=false;
    bool abandoned_selected=false,abandoned_birth_known=false;
    const void* abandoned_scope_original=nullptr;std::uint64_t abandoned_generation=0;
    struct input_release_record {const void* original=nullptr;bool attempted=false,returned=false,joined=false;};
    std::array<input_release_record,4> abandoned_inputs{};
    bool header_attempted=false,header_operation_returned=false,header_borrowed=false,header_joined=false;
    const ordinary_history_descriptor* header_identity=nullptr;
    std::size_t header_length=0,header_checked_bytes=0;
    bool page_append_attempted=false,page_append_operation_returned=false;
    std::shared_ptr<ordinary_target_history_page_append> page_append;
    // The census points to an actual retained observation. Preserve the old
    // original until a new capture returns; never leave a dangling snapshot.
    std::shared_ptr<ordinary_target_history_page_append> page_observation;
    std::uint64_t page_attempts=0;
    // One actual reader and one physical-observation predecessor. Ended
    // buffers are destroyed before this fixed slot can admit another phase.
    std::shared_ptr<ordinary_target_history_page_reader> page_reader,read_observation;
    std::uint64_t read_attempts=0;
    bool read_attempted=false,read_operation_returned=false;
    std::uint64_t device=0,inode=0,physical_blocks_high=0;
    std::int64_t extent_high=0;
    std::exception_ptr first_failure;
    struct call {
        bool attempted=false,entered=false,returned=false;
        std::int64_t result=-1;int error=0;
    } open,allocate,resize,file_sync,directory_sync,unlink,unlink_directory_sync,close;
    call header_write,header_sync;
#ifdef LATTICE_TARGET_MAIN_POSIX
    struct stat_call:call {struct stat value{};};
    struct snapshot {
        bool attempted=false,returned=false;
        stat_call directory,held,named;
    } before,allocated,sized,durable,ending_before,ending_unlinked,abandoned_birth;
    snapshot header_written,header_durable;
    static constexpr std::size_t header_read_chunk=65536;
    static constexpr std::size_t header_read_limit=
        (ordinary_sync_limits::frame_bytes+header_read_chunk-1)/header_read_chunk;
    struct header_read_call:call {
        std::size_t offset=0,requested=0;
        bool bytes_match=false;
        snapshot after;
    };
    std::array<header_read_call,header_read_limit> header_reads{};
    std::array<std::uint8_t,header_read_chunk> header_read_buffer{};
    std::size_t header_read_count=0;
    const snapshot* observed=nullptr;
#if defined(__APPLE__)
    fstore_t allocation{}; // Actual kernel input/output retained unchanged.
#endif
#endif
    state(const std::shared_ptr<ordinary_target_staging_connection>& original,
        const std::shared_ptr<ordinary_group_target_receiving_owner>& owner)
        :connection(original),connection_identity(original.get()),receiver(owner),receiver_identity(owner.get()),
         target(original->state_->target),end(original->state_->end),member(original->state_->member_fd),
         member_device(original->state_->member_device),member_inode(original->state_->member_inode){}
    bool creator_current()const noexcept {
        return ordinary_launch::current_process()==process&&std::this_thread::get_id()==thread;
    }
    void require_local(ordinary_launch::deadline requested)const {
        if(!creator_current()||requested>end||std::chrono::steady_clock::now()>=requested)
            birth_fail("history spool creator/deadline unavailable");
    }
    static void returned(call& original,std::int64_t result,int error)noexcept {
        original.result=result;original.error=error;original.returned=true;
    }
#ifdef LATTICE_TARGET_MAIN_POSIX
    void capture(snapshot& out) {
        if(!creator_current()||out.attempted||fd<0)birth_fail("history spool observation original unavailable");
        out.attempted=true;
        out.directory.attempted=true;errno=0;out.directory.entered=true;
        const int directory_rc=::fstat(member,&out.directory.value);
        const int directory_error=directory_rc?errno:0;returned(out.directory,directory_rc,directory_error);
        if(!creator_current())birth_fail("history spool inherited observation ending");
        out.held.attempted=true;errno=0;out.held.entered=true;
        const int held_rc=::fstat(fd,&out.held.value);
        const int held_error=held_rc?errno:0;returned(out.held,held_rc,held_error);
        if(!held_rc) {
            observed=&out;
            if(out.held.value.st_size>=0)extent_high=std::max(extent_high,std::int64_t(out.held.value.st_size));
            if(out.held.value.st_blocks>=0)physical_blocks_high=std::max(physical_blocks_high,std::uint64_t(out.held.value.st_blocks));
        }
        if(!creator_current())birth_fail("history spool inherited observation ending");
        out.named.attempted=true;errno=0;out.named.entered=true;
        const int named_rc=::fstatat(member,leaf,&out.named.value,AT_SYMLINK_NOFOLLOW);
        const int named_error=named_rc?errno:0;returned(out.named,named_rc,named_error);
        out.returned=true; // All first returns precede checking or diagnostics.
    }
    bool directory_same(const struct stat& actual)const noexcept {
        return S_ISDIR(actual.st_mode)&&actual.st_uid==::geteuid()&&(actual.st_mode&07777)==0700&&
            std::uint64_t(actual.st_dev)==member_device&&std::uint64_t(actual.st_ino)==member_inode;
    }
    bool file_same(const struct stat& actual,std::uint64_t links)const noexcept {
        return S_ISREG(actual.st_mode)&&actual.st_uid==::geteuid()&&(actual.st_mode&07777)==0600&&
            actual.st_nlink==links&&actual.st_size>=0&&actual.st_blocks>=0&&
            std::uint64_t(actual.st_dev)==device&&std::uint64_t(actual.st_ino)==inode;
    }
    void require_snapshot(const snapshot& actual,std::int64_t size,std::uint64_t blocks,bool named)const {
        if(!actual.returned||actual.directory.result||actual.held.result||!directory_same(actual.directory.value)||
           !file_same(actual.held.value,named?1:0)||actual.held.value.st_size!=size||
           std::uint64_t(actual.held.value.st_blocks)!=blocks)
            birth_fail("history spool held physical postimage differs");
        if(named) {
            if(actual.named.result||!file_same(actual.named.value,1)||actual.named.value.st_size!=size||
               std::uint64_t(actual.named.value.st_blocks)!=blocks)
                birth_fail("history spool named physical postimage differs");
        }else if(actual.named.result!=-1||actual.named.error!=ENOENT)
            birth_fail("history spool unlink namespace ending differs");
    }
    bool header_calls_known()const noexcept {
        const auto known=[](const call& actual){return !actual.entered||actual.returned;};
        const auto snapshot_known=[&](const snapshot& actual) {
            return known(actual.directory)&&known(actual.held)&&known(actual.named);
        };
        if(!known(header_write)||!known(header_sync)||!snapshot_known(header_written)||!snapshot_known(header_durable))return false;
        for(std::size_t at=0;at<header_read_count;++at)
            if(!known(header_reads[at])||!snapshot_known(header_reads[at].after))return false;
        return true;
    }
#endif
};
struct ordinary_target_history_page_append::state {
    using spool_state=ordinary_target_history_spool::state;
    // Declared before the actual input alias so reservation destruction follows
    // real input destruction, including retained failed/unknown native debt.
    std::shared_ptr<ordinary_resource_reservation<true>> decoder_charge;
    std::shared_ptr<const ordinary_history_decoded_page> input_pin;
    const std::weak_ptr<spool_state> spool;
    const spool_state* const spool_identity;
    const ordinary_target_history_spool* const facade;
    const ordinary_target_staging_connection* const connection_identity;
    const ordinary_group_target_receiving_owner* const receiver_identity;
    const ordinary_history_decoded_page* const page_identity;
    const std::uint64_t index,offset,length,device,inode;
    const int fd;
    const std::string previous,digest;
    bool borrowed=false,operation_returned=false,io_joined=false,locator_joined=false;
    bool input_ending_attempted=false,input_ending_returned=false;
    std::exception_ptr first_failure;
    spool_state::call write,sync;
#ifdef LATTICE_TARGET_MAIN_POSIX
    spool_state::snapshot written,durable;
    static constexpr std::size_t read_chunk=65536;
    static constexpr std::size_t read_limit=(ordinary_history_limits{}.page_bytes+read_chunk-1)/read_chunk;
    struct read_call:spool_state::call {
        std::size_t relative=0,requested=0;
        bool bytes_match=false;
        spool_state::snapshot after;
    };
    std::array<read_call,read_limit> reads{};
    std::array<std::uint8_t,read_chunk> read_buffer{};
    std::size_t read_count=0,checked_bytes=0;
    bool calls_known()const noexcept {
        const auto known=[](const spool_state::call& call){return !call.entered||call.returned;};
        const auto snapshot_known=[&](const spool_state::snapshot& actual) {
            return known(actual.directory)&&known(actual.held)&&known(actual.named);
        };
        if(!known(write)||!known(sync)||!snapshot_known(written)||!snapshot_known(durable))return false;
        for(std::size_t at=0;at<read_count;++at)
            if(!known(reads[at])||!snapshot_known(reads[at].after))return false;
        return true;
    }
#endif
    state(const std::shared_ptr<spool_state>& actual,const ordinary_target_history_spool& original,
        const ordinary_history_decoded_page& input,std::uint64_t before,
        std::shared_ptr<ordinary_resource_reservation<true>> charge,std::shared_ptr<const ordinary_history_decoded_page> pin)
        :decoder_charge(std::move(charge)),input_pin(std::move(pin)),spool(actual),spool_identity(actual.get()),facade(&original),connection_identity(actual->connection_identity),
         receiver_identity(actual->receiver_identity),page_identity(&input),index(input.index),offset(before),length(input.raw.size()),
         device(actual->device),inode(actual->inode),fd(actual->fd),previous(input.previous),digest(input.full_digest){}
};
ordinary_target_history_page_append::ordinary_target_history_page_append(std::shared_ptr<state> value):state_(std::move(value)){}
ordinary_target_history_page_append::~ordinary_target_history_page_append()=default; // No implicit native cleanup/refund.
bool ordinary_target_history_page_append::release_input_after_cleanup()noexcept {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||!cleanup_known()||!data->unlinked||data->fd!=-1||
       !data->unlink.returned||data->unlink.result||!data->unlink_directory_sync.returned||data->unlink_directory_sync.result||
       !data->close.returned||data->close.result)return false;
    phase->input_pin.reset(); // Actual decoded/packet destruction precedes SAME reservation release.
    phase->decoder_charge.reset();return true;
}
bool ordinary_target_history_page_append::cleanup_known()const noexcept {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||!data->creator_current()||phase->borrowed||!phase->operation_returned||
       (phase->input_ending_attempted&&!phase->input_ending_returned))return false;
#ifdef LATTICE_TARGET_MAIN_POSIX
    return phase->calls_known(); // Known failed IO permits genuine unpublished-target disposal.
#else
    return false;
#endif
}
std::pair<std::uint64_t,std::uint64_t> ordinary_target_history_page_append::require_durable_span(
    const ordinary_target_staging_connection& connection,const ordinary_group_target_receiving_owner& owner,
    const ordinary_history_decoded_page& input,ordinary_launch::deadline end)const {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||&connection!=phase->connection_identity||&owner!=phase->receiver_identity)
        birth_fail("history raw page span SAME issuer/receiver missing");
    data->require_local(end);
    const auto spool=connection.state_->history_spool;
    if(!spool||spool.get()!=phase->facade||data->page_append.get()!=this)
        birth_fail("history raw page span actual retained slot changed");
    spool->require_header_ending(end);
    if(phase->input_ending_returned)
        birth_fail("history raw page input original has ended; coordinates grant no renewed borrow");
    if(&connection.history_spool_page_original(*spool,owner,end)!=&input||&input!=phase->page_identity||
       !phase->decoder_charge||connection.history_spool_decoder_charge(*spool,owner,end)!=phase->decoder_charge||phase->input_pin.get()!=&input||
       input.index!=phase->index||input.previous!=phase->previous||input.full_digest!=phase->digest||input.raw.size()!=phase->length||
       phase->fd!=data->fd||phase->device!=data->device||phase->inode!=data->inode||
       !phase->operation_returned||phase->borrowed||!phase->io_joined||phase->first_failure||
       !phase->write.returned||phase->write.result!=std::int64_t(phase->length)||!phase->sync.returned||phase->sync.result||
       phase->offset>data->extent_charge||!phase->length||phase->length>data->extent_charge-phase->offset)
        birth_fail("history raw page span lacks actual durable IO ending");
#ifdef LATTICE_TARGET_MAIN_POSIX
    std::size_t relative=0;
    for(std::size_t at=0;at<phase->read_count;++at) {
        const auto& read=phase->reads[at];
        if(std::uint64_t(relative)>phase->length||!read.returned||!read.bytes_match||read.relative!=relative||!read.requested||
           read.result!=std::int64_t(read.requested)||std::uint64_t(read.requested)>phase->length-std::uint64_t(relative))
            birth_fail("history raw page span exact read ending incomplete");
        relative+=read.requested;
    }
    if(std::uint64_t(relative)!=phase->length||std::uint64_t(phase->checked_bytes)!=phase->length)
        birth_fail("history raw page span read ending has missing bytes");
#else
    birth_fail("history raw page span unsupported");
#endif
    return {phase->offset,phase->length}; // Passive coordinates; no SQL/COMMIT/cursor join is claimed.
}
void ordinary_target_history_page_append::require_locator_joined(const ordinary_target_staging_connection& connection,
    const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end)const {
    const auto phase=state_;const auto data=phase?phase->spool.lock():nullptr;
    if(!data||data.get()!=phase->spool_identity||!data->creator_current()||&connection!=phase->connection_identity||
       &owner!=phase->receiver_identity||data->page_append.get()!=this||!phase->locator_joined||phase->first_failure||
       !phase->operation_returned||phase->borrowed||!phase->io_joined)
        birth_fail("history raw page lacks SAME joined locator original");
    data->require_local(end);
}
std::pair<std::uint64_t,std::uint64_t> ordinary_target_history_page_append::require_committed_span(
    const ordinary_target_staging_connection& connection,const ordinary_group_target_receiving_owner& owner,
    std::uint64_t index,const std::string& previous,const std::string& digest,ordinary_launch::deadline end)const {
    require_locator_joined(connection,owner,end);const auto phase=state_;const auto data=phase->spool.lock();
    const auto spool=connection.state_->history_spool;
    if(!spool||spool.get()!=phase->facade)birth_fail("history raw committed span original spool changed");
    spool->require_header_observation(end); // Historical DATA only, no writer/reader authority.
    if(index!=phase->index||previous!=phase->previous||digest!=phase->digest||phase->fd!=data->fd||
       phase->device!=data->device||phase->inode!=data->inode||!phase->write.returned||phase->write.result!=std::int64_t(phase->length)||
       !phase->sync.returned||phase->sync.result||phase->offset>data->extent_charge||!phase->length||
       phase->length>data->extent_charge-phase->offset)
        birth_fail("history raw committed span retained IO identity/result differs");
#ifdef LATTICE_TARGET_MAIN_POSIX
    if(!phase->calls_known()||phase->checked_bytes!=phase->length)birth_fail("history raw committed span Native observation debt");
    std::size_t relative=0;
    for(std::size_t at=0;at<phase->read_count;++at) {
        const auto& read=phase->reads[at];
        if(std::uint64_t(relative)>phase->length||!read.returned||!read.bytes_match||read.relative!=relative||!read.requested||
           read.result!=std::int64_t(read.requested)||std::uint64_t(read.requested)>phase->length-std::uint64_t(relative))
            birth_fail("history raw committed span exact retained read ending differs");
        relative+=read.requested;
    }
    if(relative!=phase->length)birth_fail("history raw committed span missing retained read bytes");
#else
    birth_fail("history raw committed span unsupported");
#endif
    return {phase->offset,phase->length}; // Historical DATA only; no input or reader lease is renewed.
}
ordinary_target_history_spool::ordinary_target_history_spool(std::shared_ptr<state> value):state_(std::move(value)){}
// No destructor syscall, implicit cleanup, fallback, retry or quota refund.
ordinary_target_history_spool::~ordinary_target_history_spool()=default;
void ordinary_target_history_spool::require_current(ordinary_launch::deadline end)const {
    const auto data=state_;if(!data)birth_fail("history spool original missing");data->require_local(end);
    const auto original=data->connection.lock();const auto owner=data->receiver.lock();
    if(!original||original.get()!=data->connection_identity||!owner||owner.get()!=data->receiver_identity||
       data->failed||data->ending_attempted)birth_fail("history spool actual issuer/receiver unavailable");
    original->require_history_spool_current(*this,*owner,end);
}
void ordinary_target_history_spool::prepare(const std::shared_ptr<ordinary_target_staging_connection>& original,
    const std::shared_ptr<ordinary_group_target_receiving_owner>& owner,
    std::shared_ptr<ordinary_target_history_spool>& retained,ordinary_launch::deadline end) {
    if(!original||!owner||retained||&retained!=&original->state_->history_spool||!original->state_->history_spool_attempted)
        birth_fail("history spool requires actual Conn construction slot");
    // The real caller checked its header ending before consuming this slot.
    // Publish before another current check, which must census this original.
    auto& allocation=original->state_->history_spool_allocation;
    if(allocation.attempted)birth_fail("history spool allocation-only producer already consumed");
    allocation.attempted=true;
    std::shared_ptr<state> data;
    try {
        // This bounded body contains ONLY actual state/facade allocation and
        // publication. The first RAW open/physical birth is below this body.
        data=std::make_shared<state>(original,owner);
        retained=std::shared_ptr<ordinary_target_history_spool>(new ordinary_target_history_spool(data));
        allocation.returned=true;
    }catch(...) {
        allocation.exception_original=std::current_exception();
        allocation.exception_returned=true;allocation.returned=true;
        throw; // Actual allocation-only exception ended; no Native birth issued.
    }
    try {
        retained->require_current(end);
#ifdef LATTICE_TARGET_MAIN_POSIX
        static_assert(sizeof(off_t)>=8,"history spool needs actual 64-bit file offsets");
        if(!data->extent_charge||data->extent_charge>std::uint64_t(std::numeric_limits<off_t>::max())||
           data->extent_charge%512)birth_fail("history spool protocol extent is not physically representable");
        data->open.attempted=true;retained->require_current(end);errno=0;data->open.entered=true;
        const int fd=::openat(data->member,state::leaf,O_RDWR|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC,0600);
        const int open_error=fd<0?errno:0;data->fd=fd;state::returned(data->open,fd,open_error);
        if(fd<0)birth_fail("exclusive history spool birth refused");
        // Birth corroboration precedes another current join or mutation.
        data->capture(data->before);
        if(data->before.held.result)birth_fail("history spool actual birth observation unavailable");
        data->device=std::uint64_t(data->before.held.value.st_dev);
        data->inode=std::uint64_t(data->before.held.value.st_ino);
        data->require_snapshot(data->before,0,0,true);data->born=true;
        retained->require_current(end);data->allocate.attempted=true;
#if defined(__APPLE__)
        data->allocation.fst_flags=F_ALLOCATEALL;data->allocation.fst_posmode=F_PEOFPOSMODE;
        data->allocation.fst_offset=0;data->allocation.fst_length=off_t(data->extent_charge);
        retained->require_current(end);errno=0;data->allocate.entered=true;
        const int allocated=::fcntl(data->fd,F_PREALLOCATE,&data->allocation);
        const int allocation_error=allocated?errno:0;state::returned(data->allocate,allocated,allocation_error);
        data->capture(data->allocated); // Retain partial/error physical postimages too.
        if(allocated||data->allocation.fst_bytesalloc<off_t(data->extent_charge))
            birth_fail("history spool first physical allocation refused");
        data->require_snapshot(data->allocated,0,data->extent_charge/512,true);
        retained->require_current(end);data->resize.attempted=true;
        retained->require_current(end);errno=0;data->resize.entered=true;
        const int resized=::ftruncate(data->fd,off_t(data->extent_charge));
        const int resize_error=resized?errno:0;state::returned(data->resize,resized,resize_error);
        data->capture(data->sized);
        if(resized)birth_fail("history spool first extent sizing refused");
        data->require_snapshot(data->sized,std::int64_t(data->extent_charge),data->extent_charge/512,true);
#elif defined(__linux__)
        retained->require_current(end);data->allocate.entered=true;
        const int allocated=::posix_fallocate(data->fd,0,off_t(data->extent_charge));
        state::returned(data->allocate,allocated,allocated); // Actual POSIX error code, not errno.
        data->capture(data->allocated);
        if(allocated)birth_fail("history spool first physical allocation refused");
        data->require_snapshot(data->allocated,std::int64_t(data->extent_charge),data->extent_charge/512,true);
#endif
        retained->require_current(end);data->file_sync.attempted=true;
        retained->require_current(end);errno=0;data->file_sync.entered=true;
        const int synced=::fsync(data->fd);const int sync_error=synced?errno:0;
        state::returned(data->file_sync,synced,sync_error);data->capture(data->durable);
        if(synced)birth_fail("history spool physical reservation sync refused");
        data->require_snapshot(data->durable,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        retained->require_current(end);data->directory_sync.attempted=true;
        retained->require_current(end);errno=0;data->directory_sync.entered=true;
        const int directory_synced=::fsync(data->member);const int directory_error=directory_synced?errno:0;
        state::returned(data->directory_sync,directory_synced,directory_error);
        if(directory_synced)birth_fail("history spool namespace sync refused");
        retained->require_current(end);data->ready=true;
#else
        birth_fail("history spool physical reservation unsupported on this platform");
#endif
    }catch(...) {data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();throw;}
}
void ordinary_target_history_spool::persist_header(ordinary_launch::deadline end) {
    const auto data=state_;if(!data)birth_fail("history spool header original missing");
    data->require_local(end);
    if(data->header_attempted)birth_fail("history spool header original already consumed");
    data->header_attempted=true; // First attempt is permanent, including any pre-Native failure.
    try {
        require_current(end);
        const auto original=data->connection.lock();const auto owner=data->receiver.lock();
        if(!original||!owner||!data->ready)birth_fail("history spool header actual issuer unavailable");
        const auto& header=original->history_spool_header_original(*this,*owner,end);
        data->header_identity=&header;data->header_length=header.header.size();
        if(!data->header_length||data->header_length>ordinary_sync_limits::frame_bytes||
           std::uint64_t(data->header_length)>data->extent_charge)
            birth_fail("history spool exact header exceeds existing extent/frame bound");
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(data->header_length>std::size_t(std::numeric_limits<ssize_t>::max()))
            birth_fail("history spool header native byte count is not representable");
        // The strong SAME receiver/Conn locals retain this actual input through
        // every call and its ending. No supplied byte buffer or offset exists.
        data->header_borrowed=true;
        require_current(end);data->header_write.attempted=true;
        require_current(end);errno=0;data->header_write.entered=true;
        const auto written=::pwrite(data->fd,header.header.data(),data->header_length,off_t{0});
        const int write_error=written<0?errno:0;state::returned(data->header_write,written,write_error);
        data->capture(data->header_written); // Keep the first partial/error physical postimage too.
        data->require_snapshot(data->header_written,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        if(written<0||std::size_t(written)!=data->header_length)
            birth_fail("history spool first header write was partial or refused");
        require_current(end);data->header_sync.attempted=true;
        require_current(end);errno=0;data->header_sync.entered=true;
        const int synced=::fsync(data->fd);const int sync_error=synced?errno:0;
        state::returned(data->header_sync,synced,sync_error);data->capture(data->header_durable);
        data->require_snapshot(data->header_durable,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        if(synced)birth_fail("history spool first header sync refused");
        for(std::size_t offset=0;offset<data->header_length;) {
            const auto count=std::min(state::header_read_chunk,data->header_length-offset);
            if(data->header_read_count>=data->header_reads.size()||
               std::uint64_t(offset)>data->extent_charge||std::uint64_t(count)>data->extent_charge-std::uint64_t(offset))
                birth_fail("history spool header read offset/length bound");
            auto& read=data->header_reads[data->header_read_count++];
            read.offset=offset;read.requested=count;read.attempted=true;
            require_current(end);errno=0;read.entered=true;
            const auto bytes=::pread(data->fd,data->header_read_buffer.data(),count,off_t(offset));
            const int read_error=bytes<0?errno:0;state::returned(read,bytes,read_error);
            data->capture(read.after);
            data->require_snapshot(read.after,std::int64_t(data->extent_charge),data->extent_charge/512,true);
            if(bytes<0||std::size_t(bytes)!=count)
                birth_fail("history spool first header read was short or refused");
            if(std::memcmp(data->header_read_buffer.data(),header.header.data()+offset,count))
                birth_fail("history spool header readback differs from actual wire original");
            read.bytes_match=true;data->header_checked_bytes+=count;offset+=count;
        }
        require_current(end);
        if(&original->history_spool_header_original(*this,*owner,end)!=data->header_identity||
           header.header.size()!=data->header_length||data->header_checked_bytes!=data->header_length)
            birth_fail("history spool header same-original read ending differs");
        data->header_operation_returned=true;data->header_borrowed=false;data->header_joined=true;
#else
        birth_fail("history spool header native ending unsupported on this platform");
#endif
    }catch(...) {
        data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();
#ifdef LATTICE_TARGET_MAIN_POSIX
        // A known partial/error return is terminal; it is never retried. Only
        // a SAME-creator fully returned operation releases its local borrow.
        if(data->creator_current()&&data->header_calls_known()) {
            data->header_operation_returned=true;data->header_borrowed=false;
        }
#endif
        throw;
    }
}
void ordinary_target_history_spool::require_header_ending(ordinary_launch::deadline end)const {
    require_current(end);require_header_ended_data(); // Normal writer gate remains no-active-SQL.
}
void ordinary_target_history_spool::require_header_observation(ordinary_launch::deadline end)const {
    const auto data=state_;if(!data)birth_fail("history spool observation original missing");data->require_local(end);
    const auto original=data->connection.lock();const auto owner=data->receiver.lock();
    if(!original||original.get()!=data->connection_identity||!owner||owner.get()!=data->receiver_identity||
       data->failed||data->ending_attempted)
        birth_fail("history spool observation actual issuer/receiver unavailable");
    const auto& header=original->history_spool_header_observation(*this,*owner,end);
    if(&header!=data->header_identity||header.header.size()!=data->header_length)
        birth_fail("history spool observation SAME retained header differs");
    require_header_ended_data(); // No capture/syscall/borrow/scope is issued by this observation.
}
void ordinary_target_history_spool::require_header_ended_data()const {
    const auto data=state_;
    if(!data->ready||!data->header_attempted||!data->header_operation_returned||data->header_borrowed||
       !data->header_joined||!data->header_identity||!data->header_length||data->header_checked_bytes!=data->header_length||
       !data->header_write.returned||data->header_write.result!=std::int64_t(data->header_length)||
       !data->header_sync.returned||data->header_sync.result)
        birth_fail("history spool exact header native ending is incomplete");
#ifdef LATTICE_TARGET_MAIN_POSIX
    std::size_t offset=0;
    for(std::size_t at=0;at<data->header_read_count;++at) {
        const auto& read=data->header_reads[at];
        if(offset>data->header_length||!read.returned||!read.bytes_match||read.offset!=offset||!read.requested||
           read.result!=std::int64_t(read.requested)||read.requested>data->header_length-offset)
            birth_fail("history spool exact header read ending is incomplete");
        offset+=read.requested;
    }
    if(offset!=data->header_length)birth_fail("history spool header read ending has missing bytes");
#else
    birth_fail("history spool header ending unsupported on this platform");
#endif
}
void ordinary_target_history_spool::require_no_page_append(ordinary_launch::deadline end)const {
    const auto data=state_;if(!data)birth_fail("history raw page slot original missing");data->require_local(end);
    if(data->page_append_attempted||data->page_append)
        birth_fail("history raw page append requires its matching locator consumer, not legacy BLOB staging");
}
std::shared_ptr<const ordinary_target_history_page_append> ordinary_target_history_spool::append_page(ordinary_launch::deadline end) {
    const auto data=state_;if(!data)birth_fail("history raw page original missing");data->require_local(end);
    if(data->page_append_attempted) {
        const auto prior=data->page_append;const auto phase=prior?prior->state_:nullptr;
        if(!prior||!phase||data->failed||!data->page_append_operation_returned||!phase->locator_joined||
           !phase->input_ending_attempted||!phase->input_ending_returned||phase->input_pin||phase->decoder_charge||
           phase->borrowed||phase->first_failure||!prior->cleanup_known())
            birth_fail("history raw page prior IO/SQL/leave/input ending cannot be replaced");
        data->page_observation=prior; // Actual old snapshot storage remains held through next first capture.
        data->page_append.reset();
    }
    if(data->page_attempts>=ordinary_history_limits{}.pages)
        birth_fail("history raw page fixed admitted attempts exhausted");
    ++data->page_attempts;data->page_append_attempted=true;data->page_append_operation_returned=false;
    // Distinct monotonic attempt before allocation/effects; failures cannot retry/refund this tail.
    try {
        require_header_ending(end);
        const auto original=data->connection.lock();const auto owner=data->receiver.lock();
        if(!original||!owner)birth_fail("history raw page actual issuer/receiver missing");
        const auto& header=original->history_spool_header_original(*this,*owner,end);
        const auto& input=original->history_spool_page_original(*this,*owner,end);
        ordinary_history_progress before;
        const auto offset=original->history_spool_page_before_bytes(*this,*owner,before,end);
        const ordinary_history_limits limits;
        // SQL prefix DATA comes only from the genuine prior ended programme.
        // The prior pending slot was genuinely ended above; no remote progress
        // or copied offset permits a replacement or unused-tail write.
        if(before.complete||input.index!=before.next_page||input.previous!=before.previous||
           data->page_attempts!=before.next_page+1||!header.pages||offset<data->header_length||
           (!before.next_page&&(before.rows||before.known||before.unknown||before.last_table!=-1||
             !before.previous.empty()||offset!=data->header_length))||
           input.raw.empty()||input.raw.size()>limits.page_bytes||input.records.empty()||input.records.size()>limits.records_per_page||
           offset>data->extent_charge||std::uint64_t(input.raw.size())>data->extent_charge-offset)
            birth_fail("history raw page actual prefix/order/extent bound differs");
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(offset>std::uint64_t(std::numeric_limits<off_t>::max())||
           input.raw.size()>std::size_t(std::numeric_limits<ssize_t>::max()))
            birth_fail("history raw page native offset/length not representable");
        using append_state=ordinary_target_history_page_append::state;
        const auto charge=original->history_spool_decoder_charge(*this,*owner,end);
        const auto pin=original->history_spool_page_input_pin(*this,*owner,end);
        if(!charge||pin.get()!=&input)birth_fail("history raw page SAME input/reservation pin missing");
        auto phase=std::make_shared<append_state>(data,*this,input,offset,charge,pin);
        data->page_append=std::shared_ptr<ordinary_target_history_page_append>(new ordinary_target_history_page_append(phase));
        phase->borrowed=true; // SAME strong receiver/Conn locals retain actual input through all IO and ending.
        require_current(end);phase->write.attempted=true;
        require_current(end);errno=0;phase->write.entered=true;
        const auto written=::pwrite(phase->fd,input.raw.data(),std::size_t(phase->length),off_t(phase->offset));
        const int write_error=written<0?errno:0;state::returned(phase->write,written,write_error);
        data->capture(phase->written);
        data->require_snapshot(phase->written,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        data->page_observation.reset(); // observed now points into SAME retained new phase.
        if(written<0||std::uint64_t(written)!=phase->length)
            birth_fail("history raw page first write was partial or refused");
        require_current(end);phase->sync.attempted=true;
        require_current(end);errno=0;phase->sync.entered=true;
        const int synced=::fsync(phase->fd);const int sync_error=synced?errno:0;
        state::returned(phase->sync,synced,sync_error);data->capture(phase->durable);
        data->require_snapshot(phase->durable,std::int64_t(data->extent_charge),data->extent_charge/512,true);
        if(synced)birth_fail("history raw page first sync refused");
        for(std::size_t relative=0;std::uint64_t(relative)<phase->length;) {
            const auto count=std::min<std::size_t>(append_state::read_chunk,std::size_t(phase->length)-relative);
            if(phase->read_count>=phase->reads.size()||std::uint64_t(relative)>phase->length||
               std::uint64_t(count)>phase->length-std::uint64_t(relative)||
               phase->offset>data->extent_charge||std::uint64_t(relative)>data->extent_charge-phase->offset||
               std::uint64_t(count)>data->extent_charge-phase->offset-std::uint64_t(relative))
                birth_fail("history raw page read offset/length bound");
            auto& read=phase->reads[phase->read_count++];read.relative=relative;read.requested=count;read.attempted=true;
            require_current(end);errno=0;read.entered=true;
            const auto bytes=::pread(phase->fd,phase->read_buffer.data(),count,off_t(phase->offset+std::uint64_t(relative)));
            const int read_error=bytes<0?errno:0;state::returned(read,bytes,read_error);data->capture(read.after);
            data->require_snapshot(read.after,std::int64_t(data->extent_charge),data->extent_charge/512,true);
            if(bytes<0||std::size_t(bytes)!=count)birth_fail("history raw page first read was short or refused");
            if(std::memcmp(phase->read_buffer.data(),input.raw.data()+relative,count))
                birth_fail("history raw page readback differs from SAME wire input");
            read.bytes_match=true;phase->checked_bytes+=count;relative+=count;
        }
        require_current(end);
        ordinary_history_progress final_before;
        if(&original->history_spool_page_original(*this,*owner,end)!=phase->page_identity||
           original->history_spool_page_before_bytes(*this,*owner,final_before,end)!=phase->offset||
           final_before.next_page!=before.next_page||final_before.previous!=before.previous||
           final_before.rows!=before.rows||final_before.known!=before.known||final_before.unknown!=before.unknown||
           final_before.last_table!=before.last_table||final_before.complete!=before.complete||
           std::uint64_t(phase->checked_bytes)!=phase->length)
            birth_fail("history raw page final SAME input/prior ending differs");
        phase->operation_returned=true;phase->borrowed=false;phase->io_joined=true;
        (void)data->page_append->require_durable_span(*original,*owner,input,end);
        data->page_append_operation_returned=true;
        return data->page_append; // IO joined only; no acknowledged/reusable cursor was advanced.
#else
        birth_fail("history raw page append unsupported on this platform");
#endif
    }catch(...) {
        data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();
        if(data->creator_current()) {
            if(!data->page_append)data->page_append_operation_returned=true; // Known pre-Native failure.
#ifdef LATTICE_TARGET_MAIN_POSIX
            else {
                const auto phase=data->page_append->state_;
                if(!phase->first_failure)phase->first_failure=std::current_exception();
                if(phase->calls_known()) {
                    phase->operation_returned=true;phase->borrowed=false;data->page_append_operation_returned=true;
                }
            }
#endif
        }
        throw;
    }
}
void ordinary_target_history_spool::join_page_locator(const ordinary_target_staging_connection& connection,
    const ordinary_group_target_receiving_owner& owner,const std::shared_ptr<const ordinary_target_history_page_append>& append,
    ordinary_launch::deadline end) {
    require_header_ending(end);const auto data=state_;const auto phase=append?append->state_:nullptr;
    if(!phase||data->page_append!=append||!data->page_append_operation_returned||phase->input_ending_attempted||phase->first_failure)
        birth_fail("history raw locator join lost actual retained append");
    connection.require_history_raw_locator_ending(owner,*append,end); // Actual SQL/VM/COMMIT/leave/postimage proof.
    (void)append->require_durable_span(connection,owner,connection.history_spool_page_original(*this,owner,end),end);
    phase->locator_joined=true; // No Native effect or cursor mutation; this SAME completed join only.
}
void ordinary_target_history_spool::end_page_input(const ordinary_target_staging_connection& connection,
    const ordinary_group_target_receiving_owner& owner,ordinary_launch::deadline end) {
    require_header_ending(end);const auto data=state_;const auto append=data->page_append;const auto phase=append?append->state_:nullptr;
    if(!phase||phase->input_ending_attempted||!phase->locator_joined||phase->first_failure||!append->cleanup_known())
        birth_fail("history raw input ending lost actual joined append");
    connection.require_history_raw_locator_ending(owner,*append,end);
    append->require_locator_joined(connection,owner,end);
    phase->input_ending_attempted=true;
    try {
        // The SAME live alias remains valid while the receiver rechecks its
        // SQL progress and first transport return. No new lease is issued.
        connection.end_history_raw_input(owner,phase->input_pin,end);
        if(phase->input_pin)birth_fail("history raw actual input lease destruction did not return");
        phase->decoder_charge.reset(); // Receiver destroyed actual decoded/wire captures first.
        phase->input_ending_returned=true;
    }catch(...) {
        if(!phase->first_failure)phase->first_failure=std::current_exception();
        data->failed=true;if(!data->first_failure)data->first_failure=std::current_exception();
        throw; // Partial/unknown input ending cannot renew or replace this slot.
    }
}
bool ordinary_target_history_spool::owns_leaf(const char* name)const noexcept {
    const auto data=state_;
    return data&&data->creator_current()&&data->born&&!data->unlinked&&name&&std::strcmp(name,state::leaf)==0;
}
void ordinary_target_history_spool::verify_member_census(int member,bool seen,ordinary_launch::deadline end)const {
    const auto data=state_;if(!data)birth_fail("history spool census original missing");data->require_local(end);
    const auto original=data->connection.lock();
    if(!original||original.get()!=data->connection_identity||original->state_->history_spool.get()!=this||
       original->state_->target!=data->target||member!=data->member||data->failed)
        birth_fail("history spool census actual issuer changed or failed");
    // NO root/IO/target/receiver current recursion inside the root's census.
    if(!data->born) {
        if(seen||data->open.entered||data->open.returned||data->fd>=0)
            birth_fail("history spool census birth is unresolved");
        return;
    }
#ifdef LATTICE_TARGET_MAIN_POSIX
    if(data->unlinked) {
        if(seen||!data->ending_joined||!data->unlink.returned||data->unlink.result||
           !data->unlink_directory_sync.returned||data->unlink_directory_sync.result||
           !data->close.returned||data->close.result||data->fd!=-1)
            birth_fail("history spool census unlink/close ending unresolved");
        return;
    }
    if(!seen||data->fd<0||!data->observed||!data->observed->returned||data->observed->held.result||data->ending_attempted)
        birth_fail("history spool census lacks actual retained physical observation");
    struct stat directory{},held{},named{};
    if(::fstat(member,&directory)||!data->directory_same(directory)||::fstat(data->fd,&held)||
       !data->file_same(held,1)||::fstatat(member,state::leaf,&named,AT_SYMLINK_NOFOLLOW)||!data->file_same(named,1)||
       held.st_size!=data->observed->held.value.st_size||held.st_blocks!=data->observed->held.value.st_blocks||
       named.st_size!=held.st_size||named.st_blocks!=held.st_blocks||held.st_size>std::int64_t(data->extent_charge)||
       std::uint64_t(held.st_blocks)>data->extent_charge/512)
        birth_fail("history spool census physical identity/charge differs");
#else
    birth_fail("history spool census unsupported");
#endif
}
bool ordinary_target_history_spool::retire_unused(const ordinary_target_staging_connection& original)noexcept {
    const auto data=state_;
    if(!data||!data->creator_current()||&original!=data->connection_identity||
       !original.history_spool_retirement_admissible(this))return false;
    if(data->ending_attempted)return data->ending_joined; // Observe; never reissue an ending.
    data->ending_attempted=true;
    try {
#ifdef LATTICE_TARGET_MAIN_POSIX
        if(!data->born||data->fd<0||!data->open.returned||data->open.result!=data->fd||data->unlinked)return false;
        const auto known=[](const state::call& call){return !call.entered||call.returned;};
        if(!known(data->allocate)||!known(data->resize)||!known(data->file_sync)||!known(data->directory_sync))return false;
        if(data->header_borrowed||(data->header_attempted&&!data->header_operation_returned)||!data->header_calls_known())return false;
        if((data->page_append_attempted&&!data->page_append_operation_returned)||
           (data->page_append&&!data->page_append->cleanup_known())||
           (data->page_observation&&!data->page_observation->cleanup_known()))return false;
        if((data->read_attempted&&!data->read_operation_returned)||
           (data->page_reader&&!data->page_reader->cleanup_known())||
           (data->read_observation&&!data->read_observation->cleanup_known()))return false;
        // No active header write/read borrow or unknown first native return may
        // be erased by cleanup. The private raw-page pending original must also
        // have known ended IO; no SQL locator/import handoff exists in this slice.
        // These reads corroborate a failed allocation's owned extent, without
        // granting future work or refunding its permanent charge.
        data->capture(data->ending_before);
        if(data->ending_before.held.result)return false;
        data->require_snapshot(data->ending_before,std::int64_t(data->ending_before.held.value.st_size),
            std::uint64_t(data->ending_before.held.value.st_blocks),true);
        if(!data->creator_current()||!original.history_spool_retirement_admissible(this))return false;
        data->unlink.attempted=true;errno=0;data->unlink.entered=true;
        const int removed=::unlinkat(data->member,state::leaf,0);const int unlink_error=removed?errno:0;
        state::returned(data->unlink,removed,unlink_error);
        if(removed)return false;data->unlinked=true;
        data->capture(data->ending_unlinked);
        data->require_snapshot(data->ending_unlinked,std::int64_t(data->ending_before.held.value.st_size),
            std::uint64_t(data->ending_before.held.value.st_blocks),false);
        if(!data->creator_current()||!original.history_spool_retirement_admissible(this))return false;
        data->unlink_directory_sync.attempted=true;errno=0;data->unlink_directory_sync.entered=true;
        const int directory_synced=::fsync(data->member);const int directory_error=directory_synced?errno:0;
        state::returned(data->unlink_directory_sync,directory_synced,directory_error);
        if(directory_synced)return false;
        if(!data->creator_current()||!original.history_spool_retirement_admissible(this))return false;
        data->close.attempted=true;errno=0;data->close.entered=true;
        const int closed=::close(data->fd);const int close_error=closed?errno:0;
        state::returned(data->close,closed,close_error);
        if(closed)return false; // Unknown/EINTR close is retained; no retry.
        data->fd=-1;
        if(data->page_append&&!data->page_append->release_input_after_cleanup())return false;
        if(data->page_observation&&!data->page_observation->release_input_after_cleanup())return false;
        if(data->page_reader&&!data->page_reader->release_input_after_cleanup())return false;
        if(data->read_observation&&!data->read_observation->release_input_after_cleanup())return false;
        data->ending_joined=true;return true;
#else
        return false;
#endif
    }catch(...) {return false;}
}

#include "ordinary_target_history_page_reader.inc"
