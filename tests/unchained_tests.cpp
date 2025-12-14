#include <cstdint>
#include <string>
#include <vector>
#include <algorithm>
#include <unchained.h>
#include <catch2/catch_test_macros.hpp>
using Contest::UnchainedHash;


TEST_CASE("UnchainedHash: probe on empty does nothing") {
    UnchainedHash<uint64_t, int> h;

    bool called = false;
    h.probe(42, [&](int) { called = true; });

    REQUIRE_FALSE(called);
}

TEST_CASE("UnchainedHash: finalize_build on empty is safe") {
    UnchainedHash<uint64_t, int> h;
    h.finalize_build();

    bool called = false;
    h.probe(1, [&](int){ called = true; });
    REQUIRE_FALSE(called);
}

TEST_CASE("UnchainedHash: probe before finalize_build returns nothing") {
    UnchainedHash<uint64_t, int> h;
    h.build_insert(1, 111);

    bool called = false;
    h.probe(1, [&](int){ called = true; });

    REQUIRE_FALSE(called);
}

TEST_CASE("UnchainedHash: single insert + finalize_build + hit") {
    UnchainedHash<uint64_t, int> h;
    h.reserve(1);
    h.build_insert(7, 123);
    h.finalize_build();

    int out = 0;
    bool called = false;
    h.probe(7, [&](int v) { called = true; out = v; });

    REQUIRE(called);
    REQUIRE(out == 123);
}

TEST_CASE("UnchainedHash: miss does not call callback") {
    UnchainedHash<uint64_t, int> h;
    h.build_insert(7, 123);
    h.finalize_build();

    bool called = false;
    h.probe(8, [&](int) { called = true; });

    REQUIRE_FALSE(called);
}

TEST_CASE("UnchainedHash: unique key calls callback exactly once") {
    UnchainedHash<uint64_t, int> h;
    h.build_insert(10, 5);
    h.finalize_build();

    int cnt = 0;
    h.probe(10, [&](int){ ++cnt; });

    REQUIRE(cnt == 1);
}

TEST_CASE("UnchainedHash: multiple inserts and all retrievable") {
    UnchainedHash<uint64_t, uint64_t> h;

    constexpr uint64_t N = 10'000;
    for (uint64_t i = 0; i < N; ++i) {
        h.build_insert(i, i * 10);
    }
    h.finalize_build();

    for (uint64_t k : {0ull, 1ull, 2ull, 17ull, 999ull, 4096ull, 9999ull}) {
        uint64_t got = 0;
        bool called = false;
        h.probe(k, [&](uint64_t v) { called = true; got = v; });

        REQUIRE(called);
        REQUIRE(got == k * 10);
    }
}

TEST_CASE("UnchainedHash: duplicates return multiple callbacks") {
    UnchainedHash<uint64_t, int> h;

    h.build_insert(5, 10);
    h.build_insert(5, 20);
    h.build_insert(5, 30);
    h.finalize_build();

    std::vector<int> vals;
    h.probe(5, [&](int v) { vals.push_back(v); });

    REQUIRE(vals.size() == 3);
    std::sort(vals.begin(), vals.end());
    REQUIRE(vals[0] == 10);
    REQUIRE(vals[1] == 20);
    REQUIRE(vals[2] == 30);
}

TEST_CASE("UnchainedHash: clear resets and allows rebuild") {
    UnchainedHash<std::string, int> h;

    h.build_insert("a", 1);
    h.build_insert("b", 2);
    h.finalize_build();

    bool called_a = false;
    h.probe("a", [&](int) { called_a = true; });
    REQUIRE(called_a);

    h.clear();

    bool called_after_clear = false;
    h.probe("a", [&](int) { called_after_clear = true; });
    REQUIRE_FALSE(called_after_clear);

    // rebuild
    h.build_insert("x", 9);
    h.finalize_build();

    int out = 0;
    bool called_x = false;
    h.probe("x", [&](int v) { called_x = true; out = v; });
    REQUIRE(called_x);
    REQUIRE(out == 9);
}

TEST_CASE("UnchainedHash: many string keys (basic stress)") {
    UnchainedHash<std::string, int> h;

    std::vector<std::string> keys;
    keys.reserve(5000);
    for (int i = 0; i < 5000; ++i) {
        keys.push_back("key_" + std::to_string(i));
        h.build_insert(keys.back(), i);
    }
    h.finalize_build();

    for (int i : {0, 1, 42, 999, 2048, 4999}) {
        bool called = false;
        int got = -1;
        h.probe("key_" + std::to_string(i), [&](int v) { called = true; got = v; });
        REQUIRE(called);
        REQUIRE(got == i);
    }
}

TEST_CASE("UnchainedHash: boundary sizes around powers of two") {
    for (uint32_t n : {1u, 2u, 3u, 4u, 5u, 7u, 8u, 9u, 15u, 16u, 17u}) {
        UnchainedHash<uint64_t, uint64_t> h;

        for (uint64_t i = 0; i < static_cast<uint64_t>(n); ++i) {
            h.build_insert(i, i + 100);
        }
        h.finalize_build();

        std::vector<uint64_t> keys = {
            static_cast<uint64_t>(0),
            static_cast<uint64_t>(n / 2),
            static_cast<uint64_t>(n - 1)
        };

        for (uint64_t k : keys) {
            bool called = false;
            uint64_t got = 0;

            h.probe(k, [&](uint64_t v) { called = true; got = v; });

            REQUIRE(called);
            REQUIRE(got == k + 100);
        }
    }
}

struct K {
    uint64_t h;   
    uint64_t id;  
    bool operator==(const K& o) const noexcept { return id == o.id; }
};

namespace std {
template<> struct hash<K> {
    size_t operator()(const K& k) const noexcept {
        return static_cast<size_t>(k.h);
    }
};
}

TEST_CASE("UnchainedHash: custom-key clustering (many keys share similar hashes) still correct") {
    UnchainedHash<K, int> h;
    constexpr int N = 2000;
    for (int i = 0; i < N; ++i) {
        K key{ /*h=*/ 0x12340000ull + static_cast<uint64_t>(i), /*id=*/ static_cast<uint64_t>(i) };
        h.build_insert(key, i * 3);
    }
    h.finalize_build();

    for (int i : {0, 1, 2, 123, 999, 1999}) {
        K key{ /*h=*/ 0x12340000ull + static_cast<uint64_t>(i), /*id=*/ static_cast<uint64_t>(i) };

        bool called = false;
        int got = -1;
        h.probe(key, [&](int v){ called = true; got = v; });

        REQUIRE(called);
        REQUIRE(got == i * 3);
    }

    {
        K missing{ /*h=*/ 0x12340000ull + 777ull, /*id=*/ 777777ull };
        bool called = false;
        h.probe(missing, [&](int){ called = true; });
        REQUIRE_FALSE(called);
    }
}
