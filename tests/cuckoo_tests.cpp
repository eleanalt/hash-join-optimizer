#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include "cuckoo.h"

TEMPLATE_TEST_CASE("Insert and find basic keys", "[insert]", int32_t, int64_t, double) {
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

TEST_CASE("Trigger rehash when load factor exceeded", "[rehash]") {
    Contest::CuckooHash<int, int> ht(16);
    for (int i = 0; i < 1000; ++i)
        ht.emplace(i, i * 10);
    REQUIRE(ht.size() == 1000);
    for (int i = 0; i < 1000; ++i)
        REQUIRE(ht.contains(i));
}

TEST_CASE("Insert duplicate keys (overwrite behavior)", "[insert]") {
    Contest::CuckooHash<int, int> ht;
    ht.emplace(42, 100);
    ht.emplace(42, 200);
    REQUIRE(ht.size() == 1);
    REQUIRE(*ht.find(42) == 200); 
}

TEMPLATE_TEST_CASE("Modify entry using operator[]", "[modify]", int32_t, int64_t, double) {
    Contest::CuckooHash<TestType, TestType> ht;
    ht.emplace(TestType(1), TestType(10));
    REQUIRE(ht[TestType(1)] == TestType(10));
    ht[TestType(1)] = TestType(99);
    REQUIRE(ht[TestType(1)] == TestType(99));
    ht[TestType(2)] = TestType(123);
    REQUIRE(ht.contains(TestType(2)));
    REQUIRE(ht[TestType(2)] == TestType(123));
}

TEMPLATE_TEST_CASE("Accessing non-existent keys via operator[]", "[access]", int32_t, int64_t, double) {
    Contest::CuckooHash<int, TestType> ht;

    REQUIRE(ht.empty());
    ht[5] = TestType(50);

    REQUIRE(ht.contains(5));
    REQUIRE(ht[5] == TestType(50));
    REQUIRE(ht.size() == 1);
    REQUIRE(ht[123] == TestType{});
    REQUIRE(ht.size() == 2);
}

TEST_CASE("Check load factor and capacity after inserts", "[stats]") {
    Contest::CuckooHash<int, int> ht(8);

    for (int i = 0; i < 8; ++i)
        ht.emplace(i, i);

    REQUIRE(ht.load_factor() <= 0.5);
    REQUIRE(ht.bucket_count() >= 8);
}

TEST_CASE("Find returns correct pointer and nullptr when not found", "[find]") {
    Contest::CuckooHash<int, int> ht;

    ht.emplace(10, 100);
    ht.emplace(20, 200);

    int* val1 = ht.find(10);
    int* val2 = ht.find(20);
    int* val3 = ht.find(999);

    REQUIRE(val1 != nullptr);
    REQUIRE(*val1 == 100);
    REQUIRE(val2 != nullptr);
    REQUIRE(*val2 == 200);
    REQUIRE(val3 == nullptr);
}
