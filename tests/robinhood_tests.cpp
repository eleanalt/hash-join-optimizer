#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_template_test_macros.hpp> 
#include "include/robinhood.h"

TEMPLATE_TEST_CASE("Insert and find","[insert]",int32_t,int64_t,double,std::string) {
    Contest::RobinhoodHash<TestType,TestType> ht;

    
    ht.emplace(TestType(0),TestType(10));
    ht.emplace(TestType(1),TestType(11));
    ht.emplace(TestType(2),TestType(12));

    REQUIRE(ht.contains(TestType(0)));
    REQUIRE(ht.contains(TestType(1)));
    REQUIRE(ht.contains(TestType(2)));
    REQUIRE(ht.get_size()==3);

    REQUIRE(!ht.contains(3));

}

TEST_CASE("Trigger rehash","[insert]") {
    Contest::RobinhoodHash<int,int> ht(64);

    for(int i = 0; i < 65; i++) {
        ht.emplace(i,i);
    }

    REQUIRE(ht.get_size()==65);

    for(int i = 0; i < 65; i++) {
        REQUIRE(ht[i]==i);
    }
}

TEST_CASE("Insert duplicate keys","[insert]") {
    Contest::RobinhoodHash<int,int> ht;

    ht.emplace(1,1);
    ht.emplace(1,2);

    REQUIRE(ht[1]==1);
    REQUIRE(ht.get_size()==1);


}

TEMPLATE_TEST_CASE("Modify entry","[modify]",int32_t,int64_t,double,std::string) {
    Contest::RobinhoodHash<TestType,TestType> ht;

    
    ht.emplace(TestType(0),TestType(10));
    ht.emplace(TestType(1),TestType(11));
    ht.emplace(TestType(2),TestType(12));

    REQUIRE(ht[TestType(0)] == TestType(10));
    ht[TestType(0)] = TestType(20);
    REQUIRE(ht[TestType(0)] == TestType(20));


    REQUIRE(ht[TestType(1)] == TestType(11));
    ht[TestType(1)] = TestType(21);
    REQUIRE(ht[TestType(1)] == TestType(21));
    

    REQUIRE(ht[TestType(2)] == TestType(12));
    ht[TestType(2)] = TestType(22);
    REQUIRE(ht[TestType(2)] == TestType(22));

    REQUIRE(ht.get_size()==3);

}

TEMPLATE_TEST_CASE("Access empty table","[modify]",int32_t,int64_t,double,std::string) {
    Contest::RobinhoodHash<int,TestType> ht;

    ht[0] = TestType(100);
    REQUIRE(ht.get_size()==1);

    REQUIRE(ht.contains(0));
    REQUIRE(ht[0] == TestType(100));
    REQUIRE(ht[123] == TestType{});
    REQUIRE(ht.get_size()==2);

}








