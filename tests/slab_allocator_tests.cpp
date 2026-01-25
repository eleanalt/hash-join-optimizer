#define CATCH_CONFIG_MAIN
#include <catch2/catch_test_macros.hpp>

#include "slab_allocator.h"

using namespace Contest;

struct TestTuple {
    int a;
    int b;
};

TEST_CASE("SlabAllocator basic consume behavior", "[slab]") {
    GlobalAllocator global;
    SlabAllocator slab(&global, 4); // 4 partitions

    TestTuple t1{1, 2};
    TestTuple t2{3, 4};
    TestTuple t3{5, 6};

    SECTION("First allocation creates level2 and level3 chunks") {
        REQUIRE(slab.counts[0] == 0);

        slab.consume(t1, 0);

        REQUIRE(slab.counts[0] == 1);

        auto* chunk3 = slab.level3[0].getChunks();
        REQUIRE(chunk3 != nullptr);

        auto* chunk2 = slab.level2.getChunks();
        REQUIRE(chunk2 != nullptr);

        TestTuple* stored = reinterpret_cast<TestTuple*>(
            reinterpret_cast<char*>(chunk3 + 1)
        );
        REQUIRE(stored->a == 1);
        REQUIRE(stored->b == 2);
    }

    SECTION("Multiple allocations bump inside same level3 chunk") {
        slab.consume(t1, 1);
        slab.consume(t2, 1);

        REQUIRE(slab.counts[1] == 2);

        auto* chunk = slab.level3[1].getChunks();
        REQUIRE(chunk != nullptr);

        TestTuple* first = reinterpret_cast<TestTuple*>(reinterpret_cast<char*>(chunk + 1));
        REQUIRE(first->a == 1);
        REQUIRE(first->b == 2);

        TestTuple* second = reinterpret_cast<TestTuple*>(
            reinterpret_cast<char*>(chunk + 1) + sizeof(TestTuple)
        );
        REQUIRE(second->a == 3);
        REQUIRE(second->b == 4);
    }

    SECTION("Allocator creates new level3 chunk when full") {

        slab.level3[2].addSpace(global.allocate(sizeof(Chunk) + sizeof(TestTuple)), 
                                sizeof(Chunk) + sizeof(TestTuple));

        slab.consume(t1, 2); // fills the only slot

        // Next consume should trigger new chunk creation
        slab.consume(t2, 2);

        REQUIRE(slab.counts[2] == 2);

        // Head should now be the new chunk
        Chunk* head = slab.level3[2].getChunks();
        REQUIRE(head != nullptr);
        REQUIRE(head->next != nullptr); // old chunk still linked
    }

    SECTION("Allocator handles multiple partitions independently") {
        slab.consume(t1, 0);
        slab.consume(t2, 1);
        slab.consume(t3, 2);

        REQUIRE(slab.counts[0] == 1);
        REQUIRE(slab.counts[1] == 1);
        REQUIRE(slab.counts[2] == 1);

        REQUIRE(slab.level3[0].getChunks() != nullptr);
        REQUIRE(slab.level3[1].getChunks() != nullptr);
        REQUIRE(slab.level3[2].getChunks() != nullptr);

        // Ensure they are not the same chunk
        REQUIRE(slab.level3[0].getChunks() != slab.level3[1].getChunks());
        REQUIRE(slab.level3[1].getChunks() != slab.level3[2].getChunks());
    }
}
