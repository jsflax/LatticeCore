#include <gtest/gtest.h>
#include "../../Sources/LatticeCore/src/ordinary_export_fingerprint.hpp"
#include <limits>

namespace {
using namespace lattice;
using namespace lattice::detail;
std::string fingerprint_pattern(size_t count) {
    std::string out(count,'\0');
    for(size_t i=0;i<count;++i)out[i]=static_cast<char>((i*131+17)&255);
    return out;
}
}

// Fixed SHA-256 vectors were generated from the original big-endian framing
// with Python hashlib, independently of both C++ hash implementations.
TEST(OrdinaryFingerprint, BinaryByteRangesKeepLegacyFramingAtBlockAndLengthBoundaries) {
    const std::pair<size_t,const char*> cases[] = {
        {0,"07d8c77a6ca4ab613325e2a035872a51c1c14c3728565a13fe254f1fab114fb9"},
        {1,"80a486f66e69e7651ccd6200e0dab8eac3565c9ebb3e08dfbdf7d43d43e72ee7"},
        {47,"6e8fb9aa7b9cdd034ca9fbbfe047fe40b6eee91f7410b08c389ed5fdcf4849ae"},
        {48,"826b542e0223a158e63c66deecb1036e468d2f17580e2909925a89f90e4f4199"},
        {55,"7cbd3094ef8de2ae06e38d53c9304327ec344952e9b3ae8fbfd70c97315ede45"},
        {56,"c94e6124af5908d68b46641c61e8ff9d48ced7bf829613036125f506125c9f38"},
        {63,"a4e629febbafb74e50c7439fa6f424ea64ef6d56b3e2b8dffef9ce9b0a9e4fcb"},
        {64,"d7f04ebf761d1331a79661e11a127c9dbacee38aa1a584228b50bd05c733fafe"},
        {65,"5f8a236b9c01b4625612260a40d59d04bbc22d30b1e55ff1b5ca0bb7197795ab"},
        {119,"bac4114044bdef43ce1c52d41cc42af0d1b9f046926b024bf858f4300bf46c19"},
        {120,"b9483e1507bc790752da49cf652e99f1830df18b5a370eac3139775bdb958876"},
        {127,"2c2e79d402e2e5ab7c6ff9eb85da041245adfeaed2bd693224cf84c7c0969a31"},
        {128,"721f8cca477da847f4b1c31b8ac4ed352943cbb19fc94b568b8051da29f3d34d"},
        {129,"89027ec71ca2b675a0450c4848ac4c52b3b5186115b2bd29d42d2d765df03be9"},
        {2048,"f93272426366df1862a167b3dedf1054d853bd298a6fc87b3df8949c788957d2"},
        {65535,"bdb6bd439858d3130d8b08934b0ca420ac7fba93ac7e70e6756d94b52f785d28"},
        {65536,"4851694f5cf1da2e45509142cc5f2e060c0d7babc9f0e158c380974d02a561da"},
        {65537,"9bc53e6d123395976236832766b55fbda5d51105b8a11a361ebaa7166fd6d8f4"},
    };
    for(const auto& [size,expected]:cases) {
        SCOPED_TRACE(size);
        auto raw=fingerprint_pattern(size);
        const std::vector<uint8_t> blob(raw.begin(),raw.end());
        ordinary_export_fingerprint text,bytes;
        text.bytes(std::string_view("ordinary-fingerprint-bytes-v1"));text.bytes(raw);
        bytes.bytes(std::string_view("ordinary-fingerprint-bytes-v1"));bytes.bytes(blob);
        EXPECT_EQ(text.finish(),expected);EXPECT_EQ(bytes.finish(),expected);
    }
}

TEST(OrdinaryFingerprint, AllScalarTagsAndExactFloatingBitsKeepLegacyStorageSeal) {
    const auto raw=fingerprint_pattern(257);
    const std::vector<column_value_t> row{
        nullptr,std::numeric_limits<int64_t>::min(),std::numeric_limits<int64_t>::max(),
        std::bit_cast<double>(uint64_t{0x8000000000000000}),
        std::bit_cast<double>(uint64_t{0x7ff8000000000123}),std::string{},
        std::string({char(0),char(255),char(128),'A',char(0)}),std::vector<uint8_t>{},
        std::vector<uint8_t>(raw.begin(),raw.end())};
    EXPECT_EQ(ordinary_history_storage_seal("ordinary-fingerprint-scalars-v1",row,row.size()),
        "5537f50ef0d97f393c14fef39b720de19e8238e63413451ea677f9b1028ce5ba");
}

TEST(OrdinaryFingerprint, CopiesOwnPartialInputAfterCallerStorageChanges) {
    ordinary_export_fingerprint left;
    auto borrowed=fingerprint_pattern(63);left.bytes(borrowed);
    auto right=left;
    borrowed.assign(borrowed.size(),'x');borrowed.clear();borrowed.shrink_to_fit();
    left.number(std::numeric_limits<uint64_t>::max());
    right.bytes(std::string_view("right\0branch",12));
    EXPECT_EQ(left.finish(),"69c39d324cb5eb24d6288d2042d9ce2bad4d12eeb2dadb8b640b47913f9068b5");
    EXPECT_EQ(right.finish(),"e8195899c41e848d4ab59f97f278e8f1a45a2e8d8a3d9437fedc60afd847c940");
}
