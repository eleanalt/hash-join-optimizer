#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include "value_t.h"

using namespace Contest;

TEST_CASE("StrRef encoding/decoding", "[StrRef]") {

    SECTION("Basic encode + getters") {
        StrRef s(false, 5, 7, 9, 11);

        REQUIRE(s.get_table()  == 5);
        REQUIRE(s.get_column() == 7);
        REQUIRE(s.get_page()   == 9);
        REQUIRE(s.get_offset() == 11);
        REQUIRE(s.is_long()    == false);
    }

    SECTION("Long string flag works") {
        StrRef s(true, 1, 2, 3, 4);

        REQUIRE(s.is_long());
        REQUIRE(s.get_table()  == 1);
        REQUIRE(s.get_column() == 2);
        REQUIRE(s.get_page()   == 3);
        REQUIRE(s.get_offset() == 4);
    }

    SECTION("encode() matches constructor") {
        StrRef a(true, 3, 4, 5, 6);

        StrRef b;
        b.encode(true, 3, 4, 5, 6);

        REQUIRE(a.ref == b.ref);
    }

    SECTION("clear() resets everything") {
        StrRef s(true, 9, 9, 9, 9);
        s.clear();

        REQUIRE(s.ref == 0);
        REQUIRE(s.is_long() == false);
        REQUIRE(s.get_table() == 0);
        REQUIRE(s.get_column() == 0);
        REQUIRE(s.get_page() == 0);
        REQUIRE(s.get_offset() == 0);
    }
}

TEST_CASE("value_t int32 behavior", "[value_t]") {

    SECTION("parse_int32 stores type + value") {
        value_t v;
        v.parse_int32(12345);

        REQUIRE(v.is_int32());
        REQUIRE_FALSE(v.is_strref());
        REQUIRE_FALSE(v.is_null());
        REQUIRE(v.get_int32() == 12345);
    }

    SECTION("negative int works") {
        value_t v;
        v.parse_int32(-999);

        REQUIRE(v.is_int32());
        REQUIRE(v.get_int32() == -999);
    }
}

TEST_CASE("value_t StrRef behavior", "[value_t]") {

    SECTION("parse_strref stores the StrRef and type marker") {
        StrRef s(true, 2, 4, 6, 8);

        value_t v;
        v.parse_strref(s);

        REQUIRE(v.is_strref());
        REQUIRE_FALSE(v.is_int32());
        REQUIRE_FALSE(v.is_null());

        StrRef decoded = v.get_strref();
        REQUIRE(decoded.ref == (s.ref & ~(3ull << 62)));  // top 2 bits masked
        REQUIRE(decoded.get_table()  == s.get_table());
        REQUIRE(decoded.get_column() == s.get_column());
        REQUIRE(decoded.get_page()   == s.get_page());
        REQUIRE(decoded.get_offset() == s.get_offset());
    }
}

TEST_CASE("value_t null behavior", "[value_t]") {

    SECTION("default is null") {
        value_t v;

        REQUIRE(v.is_null());
        REQUIRE_FALSE(v.is_int32());
        REQUIRE_FALSE(v.is_strref());
    }
}
