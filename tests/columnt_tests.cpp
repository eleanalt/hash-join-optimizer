#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>
#include "column_t.h"

TEST_CASE("column_t basic construction", "[column_t]") {
    SECTION("Initialize with INT32") {
        column_t col(DataType::INT32);
        REQUIRE(col.type == DataType::INT32);
        REQUIRE(col.rows_num == 0);
        REQUIRE(col.pages.empty());
    }
    
    SECTION("Initialize with VARCHAR") {
        column_t col(DataType::VARCHAR);
        REQUIRE(col.type == DataType::VARCHAR);
        REQUIRE(col.rows_num == 0);
    }
}

TEST_CASE("column_t append single value", "[column_t]") {
    column_t col(DataType::INT32);
    
    value_t val;
    val.parse_int32(42);
    
    col.append_row(val);
    
    REQUIRE(col.rows_num == 1);
    REQUIRE(col.pages.size() == 1);
    
    value_t retrieved = col.get_row_value(0);
    REQUIRE(retrieved.is_int32());
    REQUIRE(retrieved.get_int32() == 42);
}

TEST_CASE("column_t append multiple values", "[column_t]") {
    column_t col(DataType::INT32);
    
    SECTION("Append 10 values") {
        for (int i = 0; i < 10; i++) {
            value_t val;
            val.parse_int32(i * 10);
            col.append_row(val);
        }
        
        REQUIRE(col.rows_num == 10);
        REQUIRE(col.pages.size() == 1);
        
        for (int i = 0; i < 10; i++) {
            value_t retrieved = col.get_row_value(i);
            REQUIRE(retrieved.is_int32());
            REQUIRE(retrieved.get_int32() == i * 10);
        }
    }
    
    SECTION("Append 100 values") {
        for (int i = 0; i < 100; i++) {
            value_t val;
            val.parse_int32(i);
            col.append_row(val);
        }
        
        REQUIRE(col.rows_num == 100);
        REQUIRE(col.pages.size() == 1);
        
        value_t first = col.get_row_value(0);
        value_t last = col.get_row_value(99);
        REQUIRE(first.get_int32() == 0);
        REQUIRE(last.get_int32() == 99);
    }
}

TEST_CASE("column_t page allocation", "[column_t]") {
    column_t col(DataType::INT32);
    
    SECTION("Exactly one page (1024 values)") {
        for (int i = 0; i < 1024; i++) {
            value_t val;
            val.parse_int32(i);
            col.append_row(val);
        }
        
        REQUIRE(col.rows_num == 1024);
        REQUIRE(col.pages.size() == 1);
        
        value_t last = col.get_row_value(1023);
        REQUIRE(last.get_int32() == 1023);
    }
    
    SECTION("Trigger second page (1025 values)") {
        for (int i = 0; i < 1025; i++) {
            value_t val;
            val.parse_int32(i);
            col.append_row(val);
        }
        
        REQUIRE(col.rows_num == 1025);
        REQUIRE(col.pages.size() == 2);
        
        value_t last_in_first_page = col.get_row_value(1023);
        value_t first_in_second_page = col.get_row_value(1024);
        REQUIRE(last_in_first_page.get_int32() == 1023);
        REQUIRE(first_in_second_page.get_int32() == 1024);
    }
    
    SECTION("Multiple pages (3000 values)") {
        for (int i = 0; i < 3000; i++) {
            value_t val;
            val.parse_int32(i);
            col.append_row(val);
        }
        
        REQUIRE(col.rows_num == 3000);
        REQUIRE(col.pages.size() == 3);
        
        value_t mid = col.get_row_value(1500);
        value_t last = col.get_row_value(2999);
        REQUIRE(mid.get_int32() == 1500);
        REQUIRE(last.get_int32() == 2999);
    }
}

TEST_CASE("column_t with NULL values", "[column_t]") {
    column_t col(DataType::INT32);
    
    value_t null_val;  // Default is NULL
    value_t int_val;
    int_val.parse_int32(99);
    
    col.append_row(int_val);
    col.append_row(null_val);
    col.append_row(int_val);
    
    REQUIRE(col.rows_num == 3);
    
    value_t v0 = col.get_row_value(0);
    value_t v1 = col.get_row_value(1);
    value_t v2 = col.get_row_value(2);
    
    REQUIRE(v0.is_int32());
    REQUIRE(v0.get_int32() == 99);
    REQUIRE(v1.is_null());
    REQUIRE(v2.is_int32());
    REQUIRE(v2.get_int32() == 99);
}

TEST_CASE("column_t with StrRef values", "[column_t]") {
    column_t col(DataType::VARCHAR);
    
    StrRef ref1(false, 1, 2, 3, 4);
    StrRef ref2(true, 5, 6, 7, 8);
    
    value_t val1, val2;
    val1.parse_strref(ref1);
    val2.parse_strref(ref2);
    
    col.append_row(val1);
    col.append_row(val2);
    
    REQUIRE(col.rows_num == 2);
    
    value_t retrieved1 = col.get_row_value(0);
    value_t retrieved2 = col.get_row_value(1);
    
    REQUIRE(retrieved1.is_strref());
    REQUIRE(retrieved2.is_strref());
    
    StrRef r1 = retrieved1.get_strref();
    StrRef r2 = retrieved2.get_strref();
    
    REQUIRE(r1.get_table() == 1);
    REQUIRE(r1.get_column() == 2);
    REQUIRE(r2.is_long());
    REQUIRE(r2.get_table() == 5);
}

TEST_CASE("column_t edge cases", "[column_t]") {
    SECTION("Empty column") {
        column_t col(DataType::INT32);
        REQUIRE(col.rows_num == 0);
        REQUIRE(col.pages.empty());
    }
    
    SECTION("Single value at page boundary") {
        column_t col(DataType::INT32);
        
        for (int i = 0; i < 1024; i++) {
            value_t val;
            val.parse_int32(0);
            col.append_row(val);
        }
        
        REQUIRE(col.pages.size() == 1);
        
        value_t boundary_val;
        boundary_val.parse_int32(999);
        col.append_row(boundary_val);
        
        REQUIRE(col.pages.size() == 2);
        REQUIRE(col.rows_num == 1025);
        
        value_t retrieved = col.get_row_value(1024);
        REQUIRE(retrieved.get_int32() == 999);
    }
}