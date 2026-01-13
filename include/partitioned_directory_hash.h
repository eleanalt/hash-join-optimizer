#pragma once
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <thread>
#include <vector>
#include <algorithm>
#include <stdexcept>

namespace Contest {

// 64-bit hash (stable & good spread)
static inline uint64_t splitmix64(uint64_t x) {
    x += 0x9e3779b97f4a7c15ull;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ull;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebull;
    return x ^ (x >> 31);
}

static inline size_t next_pow2(size_t x) {
    size_t p = 1;
    while (p < x) p <<= 1;
    return p;
}

static inline size_t default_num_threads_env() {
    if (const char* s = std::getenv("SIGMOD_THREADS")) {
        long v = std::strtol(s, nullptr, 10);
        if (v > 0) return (size_t)v;
    }
    unsigned t = std::thread::hardware_concurrency();
    return std::max<unsigned>(1u, t);
}

struct Range { size_t begin, end; };

static inline Range split_range(size_t n, size_t tid, size_t nt) {
    size_t chunk = (n + nt - 1) / nt;
    size_t b = tid * chunk;
    size_t e = std::min(n, b + chunk);
    return {b, e};
}

// Build tuple stored in final tupleStorage (contiguous)
struct DirTuple {
    uint64_t hash;   // full hash
    uint32_t key;    // join key
    size_t   row;    // build row id
};

struct ThreadLocalPartitions {
    std::vector<std::vector<DirTuple>> part_vec; // [numParts] vectors
    std::vector<size_t>                counts;   // [numParts]
};

struct PartitionedDirectoryHash {
    std::vector<uint64_t> directory_store;
    uint64_t*             directory = nullptr; // shifted by +1 to allow directory[-1]
    std::vector<DirTuple> tupleStorage;

    size_t numParts = 0;
    size_t dir_bits = 0;
    size_t dir_size = 0;
    size_t shift    = 0; // slot = hash >> shift

    std::vector<size_t> partCounts;
    std::vector<size_t> partOffsets;

    void init(size_t num_parts, size_t directory_bits) {
        numParts = next_pow2(num_parts);
        dir_bits = directory_bits;
        dir_size = 1ull << dir_bits;
        shift    = 64 - dir_bits;

        directory_store.assign(dir_size + 1, 0);
        directory = directory_store.data() + 1;
        // directory[-1] will be set later to point at base of tupleStorage
    }

    static inline uint16_t computeTag(uint64_t h) {
        // optional: low 16 bits as tag
        return static_cast<uint16_t>(h);
    }

    // Build with: (1) partitioning, (2) count+prefix+copy per partition
    template <class GetKeyFn>
    void build(size_t total_rows, size_t num_threads, GetKeyFn&& get_key) {
        // -------- Phase 1: Threaded partitioning into temporary per(thread,partition) vectors --------
        std::vector<ThreadLocalPartitions> tls(num_threads);
        for (size_t t = 0; t < num_threads; ++t) {
            tls[t].part_vec.resize(numParts);
            tls[t].counts.assign(numParts, 0);
        }

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto r = split_range(total_rows, tid, num_threads);
                auto& local = tls[tid];

                for (size_t row = r.begin; row < r.end; ++row) {
                    // get_key(row) -> optional key; caller will throw/skip as needed
                    auto ok = get_key(row);
                    if (!ok.has_value()) continue;

                    uint32_t key = ok.value();
                    uint64_t h   = splitmix64(static_cast<uint64_t>(key));
                    size_t part  = h >> (64 - static_cast<size_t>(std::log2(numParts))); // top bits

                }
            });
        }

        for (auto& th : threads) th.join();
        threads.clear();

        // Correct partitioning with integer shift
        const size_t partShift = 64 - static_cast<size_t>(__builtin_ctzll(numParts)); // log2(numParts)
        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto r = split_range(total_rows, tid, num_threads);
                auto& local = tls[tid];

                for (size_t row = r.begin; row < r.end; ++row) {
                    auto ok = get_key(row);
                    if (!ok.has_value()) continue;

                    uint32_t key = ok.value();
                    uint64_t h   = splitmix64(static_cast<uint64_t>(key));
                    size_t part  = h >> partShift; // top bits

                    local.part_vec[part].push_back(DirTuple{h, key, row});
                    local.counts[part] += 1;
                }
            });
        }
        for (auto& th : threads) th.join();
        threads.clear();

        // -------- Phase 2: One thread aggregates partition counts, computes partition offsets --------
        partCounts.assign(numParts, 0);
        for (size_t p = 0; p < numParts; ++p) {
            size_t sum = 0;
            for (size_t t = 0; t < num_threads; ++t) sum += tls[t].counts[p];
            partCounts[p] = sum;
        }

        partOffsets.assign(numParts + 1, 0);
        for (size_t p = 0; p < numParts; ++p) partOffsets[p + 1] = partOffsets[p] + partCounts[p];

        const size_t totalTuples = partOffsets[numParts];
        tupleStorage.resize(totalTuples);

        // directory[-1] points to base (index 0)
        directory[-1] = (0ull << 16);

        // -------- Phase 3: postProcessBuild per partition (parallel) --------

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                auto pr = split_range(numParts, tid, num_threads);

                for (size_t part = pr.begin; part < pr.end; ++part) {
                    if (partCounts[part] == 0) continue;

                    // 3a) Counting: directory[slot] += (count<<16) and OR tag
                    // First compute slot range for this partition:
                    // k = 64 - shift = dir_bits
                    const uint64_t k = 64 - shift; // == dir_bits
                    const uint64_t start = (static_cast<uint64_t>(part) << k) / numParts;
                    const uint64_t end   = ((static_cast<uint64_t>(part) + 1) << k) / numParts;

                    for (size_t t = 0; t < num_threads; ++t) {
                        auto& vec = tls[t].part_vec[part];
                        for (auto& tup : vec) {
                            uint64_t slot = tup.hash >> shift; // [0, dir_size)
                            // slot should lie in [start, end)
                            (void)start; (void)end;
                            directory[slot] += (1ull << 16);
                            directory[slot] |= computeTag(tup.hash);
                        }
                    }

                    // 3b) Prefix sum -> convert counts to pointers (indices)
                    uint64_t cur = static_cast<uint64_t>(partOffsets[part]); // prevCount in pseudo
                    for (uint64_t i = start; i < end; ++i) {
                        uint64_t val = directory[i] >> 16; // count
                        directory[i] = (cur << 16) | (static_cast<uint16_t>(directory[i]));
                        cur += val;
                    }

                    // 3c) Copy tuples into final contiguous storage and advance pointers
                    for (size_t t = 0; t < num_threads; ++t) {
                        auto& vec = tls[t].part_vec[part];
                        for (auto& tup : vec) {
                            uint64_t slot = tup.hash >> shift;
                            uint64_t target = directory[slot] >> 16; // index
                            tupleStorage[static_cast<size_t>(target)] = tup;
                            directory[slot] += (1ull << 16); // advance pointer
                        }
                    }
                }
            });
        }
        for (auto& th : threads) th.join();
        threads.clear();
    }

    template <class F>
    inline void probe(uint32_t key, F&& f) const {
        uint64_t h = splitmix64(static_cast<uint64_t>(key));
        uint64_t slot = h >> shift;

        uint64_t end = directory[slot] >> 16;

        uint64_t begin = (slot == 0) ? (directory[-1] >> 16) : (directory[slot - 1] >> 16);

        for (uint64_t i = begin; i < end; ++i) {
            const auto& tup = tupleStorage[static_cast<size_t>(i)];
            if (tup.key == key) {
                f(tup.row);
            }
        }
    }
};

} // namespace Contest
