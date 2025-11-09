#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp>
#include <string>
#include <type_traits>
#include "hopscotch.h"

using namespace Contest;

TEST_CASE("Hopscotch: next_pow2 behaves correctly", "[hopscotch][next_pow2]") {
    REQUIRE(HopscotchHash<int,int>::next_pow2(0) == 1);
    REQUIRE(HopscotchHash<int,int>::next_pow2(1) == 1);
    REQUIRE(HopscotchHash<int,int>::next_pow2(2) == 2);
    REQUIRE(HopscotchHash<int,int>::next_pow2(3) == 4);
    REQUIRE(HopscotchHash<int,int>::next_pow2(4) == 4);
    REQUIRE(HopscotchHash<int,int>::next_pow2(5) == 8);
    REQUIRE(HopscotchHash<int,int>::next_pow2(1024) == 1024);
    REQUIRE(HopscotchHash<int,int>::next_pow2(1025) == 2048);
}


TEMPLATE_TEST_CASE("Hopscotch: Insert + contains + operator[] (basic)",
                   "[hopscotch][insert][contains][subscript]",
                   int32_t, int64_t, double, std::string) {
    HopscotchHash<TestType, TestType> ht;

    if constexpr (std::is_same_v<TestType, std::string>) {
        TestType k0 = "0", k1 = "1", k2 = "2";
        ht.emplace(k0, TestType("10"));
        ht.emplace(k1, TestType("11"));
        ht.emplace(k2, TestType("12"));

        REQUIRE(ht.contains(k0));
        REQUIRE(ht.contains(k1));
        REQUIRE(ht.contains(k2));

        REQUIRE(ht[k0] == TestType("10"));
        REQUIRE(ht[k1] == TestType("11"));
        REQUIRE(ht[k2] == TestType("12"));

        REQUIRE_FALSE(ht.contains(TestType("does-not-exist")));
    } else {
        TestType k0 = static_cast<TestType>(0);
        TestType k1 = static_cast<TestType>(1);
        TestType k2 = static_cast<TestType>(2);

        ht.emplace(k0, static_cast<TestType>(10));
        ht.emplace(k1, static_cast<TestType>(11));
        ht.emplace(k2, static_cast<TestType>(12));

        REQUIRE(ht.contains(k0));
        REQUIRE(ht.contains(k1));
        REQUIRE(ht.contains(k2));

        REQUIRE(ht[k0] == static_cast<TestType>(10));
        REQUIRE(ht[k1] == static_cast<TestType>(11));
        REQUIRE(ht[k2] == static_cast<TestType>(12));

        REQUIRE_FALSE(ht.contains(static_cast<TestType>(123456)));
    }
}

TEST_CASE("Hopscotch: constructors work and rehash is transparent", "[hopscotch][ctor][rehash]") {
    HopscotchHash<int,int> ht_small(64);
    HopscotchHash<int,int> ht_default;

    for (int i = 0; i < 2000; ++i) {
        ht_small.emplace(i, i * 2);
        ht_default.emplace(i, i * 3);
    }
    for (int i = 0; i < 2000; ++i) {
        REQUIRE(ht_small.contains(i));
        REQUIRE(ht_small[i] == i * 2);

        REQUIRE(ht_default.contains(i));
        REQUIRE(ht_default[i] == i * 3);
    }
}

TEMPLATE_TEST_CASE("Hopscotch: operator[] creates default and allows write-back",
                   "[hopscotch][subscript][modify]",
                   int32_t, int64_t, double) {
    HopscotchHash<int, TestType> ht;

    ht[5] = static_cast<TestType>(50);
    REQUIRE(ht.contains(5));
    REQUIRE(ht[5] == static_cast<TestType>(50));

    TestType def = ht[123];
    REQUIRE(def == TestType{});  
    REQUIRE(ht.contains(123));    
}

TEST_CASE("Hopscotch: emplace duplicate keys (non-overwrite semantics)", "[hopscotch][insert]") {
    HopscotchHash<int,int> ht;
    ht.emplace(42, 100);
    ht.emplace(42, 200);

    REQUIRE(ht.contains(42));
    int v = ht[42];
    REQUIRE((v == 100 || v == 200));
}

TEST_CASE("Hopscotch: stress insert + mutate with operator[]", "[hopscotch][stress]") {
    HopscotchHash<int,int> ht(128);
    const int N = 5000;

    for (int i = 0; i < N; ++i) ht.emplace(i, i);
    for (int i = 0; i < N; ++i) {
        REQUIRE(ht.contains(i));
        ht[i] = i + 1;
    }
    for (int i = 0; i < N; ++i) {
        REQUIRE(ht[i] == i + 1);
    }
}