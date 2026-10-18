// Included in the issuer's private namespace before the actual creators.
// These real descriptions originate ONLY at a completed create/recreate or
// exact active-member acquisition. Stored READY records cannot issue freshness.
std::atomic<bool> resource_continuous_ending_debt{false};
// One atomic pair retains the actual first rc/errno together, even when
// different attempts encounter ending debt concurrently. DATA, never a grant.
std::atomic<std::uint64_t> resource_continuous_first_close_failure{UINT64_MAX};
bool resource_continuous_close(int& original,std::uint64_t creator)noexcept {
    if(creator!=pid())return false;
    if(original<0)return true;
    const int fd=std::exchange(original,-1);errno=0;const int rc=::close(fd);const int error=errno;
    if(creator!=pid())return false;
    if(rc==0)return true;
    auto unset=std::uint64_t{UINT64_MAX};
    const auto actual=(std::uint64_t(static_cast<std::uint32_t>(rc))<<32)|static_cast<std::uint32_t>(error);
    resource_continuous_first_close_failure.compare_exchange_strong(unset,actual,std::memory_order_release,std::memory_order_relaxed);
    resource_continuous_ending_debt.store(true,std::memory_order_release);return false;
}
struct resource_creation_original final {
    enum class source : std::uint8_t {active,initial,recreated};
    const std::uint64_t creator=pid();resource_root* root=nullptr;source provenance=source::active;
    std::array<char,256> name{};std::uint16_t name_size=0,record_size=0;
    std::array<std::uint8_t,8192> record{};
    file_identity directory_id,main_id,stub_id,entry_id,physical_id;
    int directory=-1,entry=-1,physical=-1;
    void capture_(const std::string& leaf_name,const std::vector<std::uint8_t>& exact_record) {
        if(creator!=pid())resource_fail();directory_id=id(metadata(directory));
        if(creator!=pid())resource_fail();entry_id=id(metadata(entry));
        if(creator!=pid())resource_fail();physical_id=id(metadata(physical));
        if(creator!=pid())resource_fail();leaf(leaf_name);if(leaf_name.size()>255||exact_record.empty()||exact_record.size()>record.size()||creator!=pid())resource_fail();
        name_size=static_cast<std::uint16_t>(leaf_name.size());record_size=static_cast<std::uint16_t>(exact_record.size());
        std::copy(leaf_name.begin(),leaf_name.end(),name.begin());std::copy(exact_record.begin(),exact_record.end(),record.begin());
    }
    resource_creation_original(resource_root& actual,const std::string& leaf_name,source issued,
        descriptor&& member,descriptor&& entry_description,descriptor&& physical_description,
        const std::vector<std::uint8_t>& exact_record,file_identity main,file_identity stub)
        :root(&actual),provenance(issued),main_id(main),stub_id(stub) {
        // Adopt before observations can throw, including a constructor failure.
        directory=member.release();entry=entry_description.release();physical=physical_description.release();
        try {capture_(leaf_name,exact_record);}
        catch(...) {(void)settle();throw;} // exact primary and actual ending debt
    }
    resource_creation_original(resource_root& actual,const std::string& leaf_name,source issued,
        descriptor&& member,descriptor&& entry_description,descriptor&& physical_description,
        const json& exact_record,file_identity main,file_identity stub)
        :root(&actual),provenance(issued),main_id(main),stub_id(stub) {
        directory=member.release();entry=entry_description.release();physical=physical_description.release();
        // Canonical encoding is also INSIDE checked custody, never an argument
        // allocation between successful recreation and adoption of its guards.
        try {const auto raw=bytes(exact_record);capture_(leaf_name,raw);}
        catch(...) {(void)settle();throw;}
    }
    resource_creation_original(resource_creation_original&& prior)noexcept
        :creator(prior.creator),root(std::exchange(prior.root,nullptr)),provenance(prior.provenance),name(prior.name),
         name_size(prior.name_size),record_size(prior.record_size),record(prior.record),directory_id(prior.directory_id),
         main_id(prior.main_id),stub_id(prior.stub_id),entry_id(prior.entry_id),physical_id(prior.physical_id),
         directory(std::exchange(prior.directory,-1)),entry(std::exchange(prior.entry,-1)),physical(std::exchange(prior.physical,-1)) {}
    resource_creation_original(const resource_creation_original&)=delete;
    resource_creation_original& operator=(const resource_creation_original&)=delete;
    bool settle()noexcept {
        if(creator!=pid())return false;
        // A failed close consumes no guessed ending. Remaining actual guards
        // stay open until process exit; no destructor retries an uncertain fd.
        if(!resource_continuous_close(physical,creator)) {entry=-1;directory=-1;return false;}
        if(!resource_continuous_close(entry,creator)) {directory=-1;return false;}
        return resource_continuous_close(directory,creator);
    }
    ~resource_creation_original()noexcept{(void)settle();}
};
