#pragma once
#include "vendor/picosha2/picosha2.h"
#include <algorithm>
#include <array>
#include <cstdint>
#include <string>
#include <string_view>

namespace lattice::detail {
// Passive work performed by one synchronous byte-range call. Padding is not
// input. No pointer, observer, SQL object or authority escapes with these facts.
struct ready_sha256_work {
    uint64_t input_bytes=0,staged_input_bytes=0,direct_blocks=0;
};

// READY's private contiguous-byte input path. The vendor generic/iterator API
// is unchanged. Every copy owns its partial block, length and digest state.
class ready_sha256_state {
    using word=picosha2::word_t;
    using byte=picosha2::byte_t;
    std::array<word,8> digest_{};
    std::array<word,4> length_{}; // The vendor's four base-65536 byte digits.
    std::array<byte,64> tail_{};
    size_t used_=0;

    void add_length(word n) noexcept {
        word carry=0;
        length_[0]+=n;
        for(size_t i=0;i<length_.size();++i) {
            length_[i]+=carry;
            if(length_[i]>=65536u) {
                carry=length_[i]>>16;
                length_[i]&=65535u;
            } else break;
        }
    }
    void write_bit_length(byte* out) const noexcept {
        auto bits=length_;
        word carry=0;
        for(size_t i=0;i<bits.size();++i) {
            const word before=bits[i];
            bits[i]<<=3;bits[i]|=carry;bits[i]&=65535u;
            carry=(before>>(16-3))&65535u;
        }
        for(int i=3;i>=0;--i) {
            *out++=static_cast<byte>(bits[static_cast<size_t>(i)]>>8);
            *out++=static_cast<byte>(bits[static_cast<size_t>(i)]);
        }
    }
    void finish() noexcept {
        std::array<byte,64> block{};
        std::copy_n(tail_.begin(),used_,block.begin());
        block[used_]=0x80;
        if(used_>55) {
            picosha2::detail::hash256_block(digest_.begin(),block.begin(),block.end());
            block.fill(0);
        }
        write_bit_length(block.data()+56);
        picosha2::detail::hash256_block(digest_.begin(),block.begin(),block.end());
    }
public:
    ready_sha256_state() noexcept {
        std::copy_n(picosha2::detail::initial_message_digest,digest_.size(),digest_.begin());
    }
    ready_sha256_work process(std::string_view input) noexcept {
        ready_sha256_work work{static_cast<uint64_t>(input.size()),0,0};
        // Identical length conversion/carry to the original bounded READY
        // string calls. No new input or aggregate acceptance cap is imposed.
        add_length(static_cast<word>(input.size()));
        if(input.empty())return work; // Empty string_view may have null data.
        const auto* next=reinterpret_cast<const byte*>(input.data());
        size_t remaining=input.size();
        if(used_) {
            const size_t take=std::min(tail_.size()-used_,remaining);
            std::copy_n(next,take,tail_.begin()+used_);
            used_+=take;next+=take;remaining-=take;work.staged_input_bytes+=take;
            if(used_!=tail_.size())return work;
            picosha2::detail::hash256_block(digest_.begin(),tail_.begin(),tail_.end());
            used_=0;
        }
        while(remaining>=tail_.size()) {
            picosha2::detail::hash256_block(digest_.begin(),next,next+tail_.size());
            next+=tail_.size();remaining-=tail_.size();++work.direct_blocks;
        }
        if(remaining) {
            std::copy_n(next,remaining,tail_.begin());
            used_=remaining;work.staged_input_bytes+=remaining;
        }
        return work;
    }
    std::array<byte,picosha2::k_digest_size> digest_bytes() const noexcept {
        auto completed=*this;completed.finish();
        std::array<byte,picosha2::k_digest_size> out{};
        for(size_t i=0;i<completed.digest_.size();++i)
            for(size_t j=0;j<4;++j)
                out[i*4+j]=picosha2::detail::mask_8bit(static_cast<byte>(completed.digest_[i]>>(24-8*j)));
        return out;
    }
};

inline std::string ready_sha256_hex(std::string_view input,ready_sha256_work* work=nullptr) {
    ready_sha256_state state;
    const auto performed=state.process(input);
    if(work)*work=performed;
    return picosha2::bytes_to_hex_string(state.digest_bytes());
}
} // namespace lattice::detail
