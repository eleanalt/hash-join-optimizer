#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <cstdint>
#include <limits>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "cuckoo.h"

struct BadKey {
    int x;
    bool operator==(const BadKey& o) const noexcept { return x == o.x; }
};

namespace std {
template <>
struct hash<BadKey> {
    size_t operator()(const BadKey&) const noexcept { return 0u; }
};
} 

TEMPLATE_TEST_CASE("Insert and find basic keys", "[cuckoo][basic]", int32_t, int64_t, double) {
    Contest::CuckooHash<TestType, TestType> ht;

    ht.emplace(TestType(1), TestType(10));
    ht.emplace(TestType(2), TestType(20));
    ht.emplace(TestType(3), TestType(30));

    REQUIRE(ht.size() == 3);
    REQUIRE(ht.contains(TestType(1)));
    REQUIRE(ht.contains(TestType(2)));
    REQUIRE(ht.contains(TestType(3)));

    REQUIRE(*ht.find(TestType(1)) == TestType(10));
    REQUIRE(*ht.find(TestType(2)) == TestType(20));
    REQUIRE(*ht.find(TestType(3)) == TestType(30));
    REQUIRE(ht.find(TestType(999)) == nullptr);
}

TEST_CASE("Insert duplicate keys overwrites value (size stable)", "[cuckoo][overwrite]") {
    Contest::CuckooHash<int, int> ht;
    ht.emplace(42, 100);
    REQUIRE(ht.size() == 1);
    REQUIRE(*ht.find(42) == 100);
    ht.emplace(42, 200);
    REQUIRE(ht.size() == 1);
    REQUIRE(*ht.find(42) == 200);
}
TEST_CASE("operator[] inserts default for missing key and allows modification", "[cuckoo][brackets]") {
    Contest::CuckooHash<int, int> ht;
    REQUIRE(ht.empty());
    REQUIRE(ht[123] == 0);
    REQUIRE(ht.contains(123));
    REQUIRE(ht.size() == 1);
    ht[123] = 999;
    REQUIRE(ht[123] == 999);
    REQUIRE(ht.size() == 1);
    ht[5] = 50;
    REQUIRE(ht.contains(5));
    REQUIRE(ht.size() == 2);
    REQUIRE(ht[5] == 50);
}

TEST_CASE("Rehash and grow preserves all entries under heavy insertion", "[cuckoo][rehash]") {
    Contest::CuckooHash<int, int> ht(16);

    for (int i = 0; i < 5000; ++i) {
        ht.emplace(i, i * 10);
    }

    REQUIRE(ht.size() == 5000);
    REQUIRE(ht.bucket_count() >= 16);

    // load_factor should be <= 0.5 after growing (by design)
    REQUIRE(ht.load_factor() <= 0.5);

    for (int i = 0; i < 5000; ++i) {
        REQUIRE(ht.contains(i));
        auto* p = ht.find(i);
        REQUIRE(p != nullptr);
        REQUIRE(*p == i * 10);
    }
}


TEST_CASE("Edge numeric keys: negatives and min/max", "[cuckoo][edge]") {
    Contest::CuckooHash<int, int> ht;

    ht.emplace(0, 1);
    ht.emplace(-1, 2);
    ht.emplace(std::numeric_limits<int>::min(), 3);
    ht.emplace(std::numeric_limits<int>::max(), 4);

    REQUIRE(ht.size() == 4);
    REQUIRE(*ht.find(0) == 1);
    REQUIRE(*ht.find(-1) == 2);
    REQUIRE(*ht.find(std::numeric_limits<int>::min()) == 3);
    REQUIRE(*ht.find(std::numeric_limits<int>::max()) == 4);
}

TEST_CASE("String keys: empty and long keys, overwrite", "[cuckoo][string]") {
    Contest::CuckooHash<std::string, int> ht;

    std::string empty = "";
    std::string longk(10'000, 'x');

    ht.emplace(empty, 1);
    ht.emplace(longk, 2);
    REQUIRE(ht.size() == 2);

    REQUIRE(*ht.find(empty) == 1);
    REQUIRE(*ht.find(longk) == 2);

    ht.emplace(longk, 99);
    REQUIRE(ht.size() == 2);
    REQUIRE(*ht.find(longk) == 99);
}


TEST_CASE("Init capacity 0 is handled and remains functional", "[cuckoo][capacity]") {
    Contest::CuckooHash<int, int> ht(0);

    REQUIRE(ht.bucket_count() >= 1);
    REQUIRE(ht.empty());

    ht.emplace(1, 10);
    REQUIRE(ht.size() == 1);
    REQUIRE(*ht.find(1) == 10);
}
