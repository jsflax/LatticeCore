#pragma once
// TEST-only passive stage copies. A false original predicate is final before
// any SQL/printing below. No stage probe performs SQL, allocation or clock work.
#include <array>
#include <atomic>
#include <cstdio>
#include <cstring>
#include <exception>
namespace lattice::test_observation {
struct StartupPredicateObservation {
    inline static constexpr std::array<const char*,9> labels{{"barrier-committed","control-response-ready",
        "range-response-ready","response-consumed","install-committed","resume-committed",
        "reconciliation-pending","admission-deferred","outgoing-handed-off-before-turn-settlement"}};
    std::array<std::atomic<uint64_t>,9> counts{};std::atomic<uint64_t> other{0};
    void observe(const char* stage)noexcept {
        for(size_t n=0;n<labels.size();++n)if(stage&&std::strcmp(stage,labels[n])==0){counts[n].fetch_add(1);return;}
        other.fetch_add(1);
    }
    bool result(bool original,const char* predicate,lattice_db& owner)const noexcept {
        if(original)return original;
        std::fprintf(stderr,"original-startup predicate=%s result=0 stageCountsAreSeparateAtomicCopies=1",predicate);
        for(size_t n=0;n<labels.size();++n)std::fprintf(stderr," %s=%llu",labels[n],static_cast<unsigned long long>(counts[n].load()));
        std::fprintf(stderr," other=%llu\n",static_cast<unsigned long long>(other.load()));
        try {
            // Fixed, bounded aggregate rows only. These are FRESH POST-failure
            // owner reads, not the failed predicate's earlier/cached state.
            const auto scalar=[&](const char* sql){const auto rows=owner.db().query(sql);
                if(rows.size()!=1)throw db_error("startup diagnostic scalar shape");return std::get<int64_t>(rows.front().at("n"));};
            const auto phase=scalar("SELECT phase AS n FROM _lattice_producer_continuity");
            const auto installed=scalar("SELECT COUNT(*) AS n FROM _lattice_install_channel WHERE revision=1");
            const auto unclaimed=scalar("SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NULL");
            const auto claimed=scalar("SELECT COUNT(*) AS n FROM _lattice_obligation_entry WHERE stage=0 AND first_export IS NOT NULL");
            const auto unsynchronized=scalar("SELECT COUNT(*) AS n FROM AuditLog WHERE isSynchronized=0");
            const auto requests=scalar("SELECT COUNT(*) AS n FROM _lattice_recovery_request");
            std::fprintf(stderr,"original-startup FRESH_POST_FAILURE phase=%lld installedRevision1=%lld openUnclaimed=%lld openClaimed=%lld unsynchronizedAudit=%lld retainedRequests=%lld\n",
                static_cast<long long>(phase),static_cast<long long>(installed),static_cast<long long>(unclaimed),
                static_cast<long long>(claimed),static_cast<long long>(unsynchronized),static_cast<long long>(requests));
        }catch(const std::exception& error){
            const char* text=error.what();std::fputs("original-startup post-read-errorHex=",stderr);
            if(!text)std::fputs("[null-what]",stderr);else for(size_t n=0;n<512&&text[n];++n)std::fprintf(stderr,"%02x",static_cast<unsigned>(static_cast<unsigned char>(text[n])));
            std::fputc('\n',stderr);
        }catch(...){std::fputs("original-startup post-read-error=[nonstd]\n",stderr);}
        return original;
    }
};
} // namespace lattice::test_observation
