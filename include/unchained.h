#pragma once

#include <vector>
#include <cstdint>
#include <functional>
#include <utility>
#include <type_traits>
#include <limits>
#include <algorithm>
#include <iostream>

namespace Contest {

template <typename Key, typename Payload>
class UnchainedHash {
private:
    struct Tuple {
        Key      key;
        Payload  payload;
        uint16_t hash_suffix;// hash's low 16 bits (gia bloom+filtering) 
    };

    //64 bit word gia dir entry
    using DirWord = uint64_t;

    // mask gia 16 low bits
    static constexpr uint64_t BLOOM_MASK = 0xFFFFull;

    //num of bit to be moved
    static constexpr uint64_t PTR_SHIFT  = 16ull;

    //dhmiourgia packed dir word
    static inline uint64_t make_dir_word(uint32_t index, uint16_t bloom) noexcept {
        return (static_cast<uint64_t>(index) << PTR_SHIFT) | static_cast<uint64_t>(bloom);
    }

    //extract index from directory word
    static inline uint32_t dir_index(DirWord w) noexcept {
        return static_cast<uint32_t>(w >> PTR_SHIFT);
    }

    //extract bloom filter from dir word
    static inline uint16_t dir_bloom(DirWord w) noexcept {
        return static_cast<uint16_t>(w & BLOOM_MASK);
    }

    //storage
    std::vector<Tuple>  tuples_;//tuples se synexomeni mnhmh
    std::vector<uint64_t> hashes_;
    std::vector<DirWord> directory_;//ena entry ana prefix

    uint32_t dir_size_    = 0;  //number of buckets (prefixes)
    uint8_t  prefix_bits_ = 0;  //log2(dir_size_)

    //fibonacci hashing
    static inline uint64_t fibonacci_mix(uint64_t x) noexcept {
        static constexpr uint64_t C = 11400714819323198485ull; //pollaplasiasmos me const relative me xrysh tomh
        return x * C;
    }

    //ypologismos 64bit hash gia ena key
    //std::hash+fib mix
    inline uint64_t hash_key(const Key& key) const noexcept {
        uint64_t base = static_cast<uint64_t>(std::hash<Key>{}(key));
        return fibonacci_mix(base);
    }

    //exagwgh prefix apo pshla bits tou hash
    inline uint32_t prefix_from_hash(uint64_t h) const noexcept {
        if (prefix_bits_ == 0) return 0;
        return static_cast<uint32_t>(h >> (64 - prefix_bits_));
    }

    //suffix apo 16 low bits tou hash
    static inline uint16_t suffix_from_hash(uint64_t h) noexcept {
        return static_cast<uint16_t>(h & 0xFFFFu);
    }

    //dynamh tou 2 gia megethos tou dir
    static uint32_t next_pow2(uint32_t x) noexcept {
        if (x == 0) return 1;
        --x;
        x |= x >> 1;
        x |= x >> 2;
        x |= x >> 4;
        x |= x >> 8;
        x |= x >> 16;
        return x + 1;
    }

    //epilogh megethous tou dir
    void choose_directory_size(uint32_t build_size) noexcept {
        if (build_size == 0) {
            dir_size_    = 0;
            prefix_bits_ = 0;
            return;
        }
        dir_size_ = next_pow2(build_size);
        uint32_t tmp = dir_size_;
        prefix_bits_ = 0;
        while (tmp > 1) {
            tmp >>= 1;
            ++prefix_bits_;
        }
    }

    //16-bit Bloom filter helperss

    //bloom mask from suffix
    static inline uint16_t bloom_mask(uint16_t suffix) noexcept {
        uint16_t mask = 0;
        mask |= static_cast<uint16_t>(1u << ( suffix        & 0xF));
        mask |= static_cast<uint16_t>(1u << ((suffix >> 4)  & 0xF));
        mask |= static_cast<uint16_t>(1u << ((suffix >> 8)  & 0xF));
        mask |= static_cast<uint16_t>(1u << ((suffix >> 12) & 0xF));
        return mask;
    }

    //add tuple in bloom filter
    static inline void bloom_add(uint16_t& bloom, uint16_t suffix) noexcept {
        bloom = static_cast<uint16_t>(bloom | bloom_mask(suffix));
    }

    //chack bloom filter kata to
    static inline bool bloom_maybe_contains(uint16_t bloom, uint16_t suffix) noexcept {
        uint16_t mask = bloom_mask(suffix);
        return (bloom & mask) == mask;
    }

public:
    UnchainedHash() = default;
    //{
    //    static bool printed_once = false;
    //    if (!printed_once) {
    //        std::cout << "[DEBUG] Using UnchainedHash (Fibonacci only) ";
    //       printed_once = true;
    //    }
    //}

    //katharismos domwn
    void clear() {
        tuples_.clear();
        hashes_.clear();
        directory_.clear();
        dir_size_    = 0;
        prefix_bits_ = 0;
    }
    //desmeysh mnhmhs gia build phase
    void reserve(uint32_t expected_build_size) {
        tuples_.reserve(expected_build_size);
        hashes_.reserve(expected_build_size);
    }

    //build phase
    void build_insert(const Key& key, const Payload& payload) {
        uint64_t h      = hash_key(key); //plhres hash
        uint16_t suffix = suffix_from_hash(h); //low 16 bits
        tuples_.push_back(Tuple{key, payload, suffix});
        hashes_.push_back(h);
    }

    //oloklhrwsh build
    void finalize_build() {
        uint32_t n = static_cast<uint32_t>(tuples_.size());
        if (n == 0) {
            clear();
            return;
        }

        choose_directory_size(n);
        directory_.assign(static_cast<size_t>(dir_size_) + 1u, 0ull);

        //count tuple by prefix
        std::vector<uint32_t> counts(dir_size_, 0);
        std::vector<uint32_t> prefix_cache(n);

        for (uint32_t i = 0; i < n; ++i) {
            uint32_t p = prefix_from_hash(hashes_[i]);
            prefix_cache[i] = p;
            ++counts[p];
        }

        //prefix sums start offsets
        std::vector<uint32_t> starts(static_cast<size_t>(dir_size_) + 1u, 0);
        uint32_t offset = 0;
        for (uint32_t p = 0; p < dir_size_; ++p) {
            starts[p] = offset;
            offset   += counts[p];
        }
        starts[dir_size_] = offset; 

        //create adjacency array
        std::vector<Tuple>   new_tuples(n);
        std::vector<uint32_t> write_pos(dir_size_);
        std::vector<uint16_t> blooms(dir_size_, 0);

        for (uint32_t p = 0; p < dir_size_; ++p) {
            write_pos[p] = starts[p];
        }

        for (uint32_t i = 0; i < n; ++i) {
            uint32_t p   = prefix_cache[i];
            uint32_t pos = write_pos[p]++;
            new_tuples[pos] = tuples_[i];
            bloom_add(blooms[p], tuples_[i].hash_suffix);
        }

        tuples_.swap(new_tuples);
        hashes_.clear();
        hashes_.shrink_to_fit();

        //filling dir
        for (uint32_t p = 0; p < dir_size_; ++p) {
            directory_[p] = make_dir_word(starts[p], blooms[p]);
        }
        directory_[dir_size_] = make_dir_word(starts[dir_size_], 0);
    }

    //probe phase
    template <typename Callback>
    void probe(const Key& key, Callback&& cb) const {
        if (dir_size_ == 0) return;

        uint64_t h      = hash_key(key);
        uint32_t p      = prefix_from_hash(h);
        uint16_t suffix = suffix_from_hash(h);

        if (p >= dir_size_) return;

        DirWord entry      = directory_[p];
        DirWord next_entry = directory_[p + 1];

        //bllom filter early rejection
        uint16_t bloom = dir_bloom(entry);
        if (!bloom_maybe_contains(bloom, suffix)) return;

        uint32_t start = dir_index(entry);
        uint32_t end   = dir_index(next_entry);

        //seiriakh sarwsh mikrou range
        for (uint32_t i = start; i < end; ++i) {
            const Tuple& t = tuples_[i];
            if (t.hash_suffix != suffix) continue;
            if (t.key == key) cb(t.payload);
        }
    }
};

} 
