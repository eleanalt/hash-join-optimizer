#pragma once
#include <cstddef>
#include <cstdlib>
#include <vector>

namespace Contest {

// Chunk linked list header
struct Chunk {
    Chunk* next = nullptr;
    size_t bytes_used = 0;
};

// level 1 malloc wrapper
struct GlobalAllocator {
    void* allocate(size_t size) {
        void *chunk = malloc(size);
        if(!chunk) throw std::runtime_error("Out of memory");
        return chunk;
    }

    void free(void* chunk) {
        std::free(chunk);
    }
};

// level 2 and 3 bump allocators
struct BumpAlloc {
private:
    Chunk* head = nullptr; // Head of chunk linked list ie the current chunk
    char* start = nullptr; // Start of free memory in current chunk
    char* end = nullptr; // End of free memory in current chunk
    size_t chunk_size = 0; // Size of current chunk

public:

    void addSpace(void* src_chunk, size_t size) {
        //Set header for new chunk
        Chunk* chunk = (Chunk*) src_chunk;
        chunk->next = head;
        chunk->bytes_used = 0;

        head = chunk; // Assign new chunk as the new linked list head

        chunk_size = size; 

        start = (char*)(chunk + 1); // set start pointer right after the chunk header
        end = (char*)chunk + size; // set end pointer to end of chunk
    }

    void* allocate(size_t size) {
        if(start + size > end) return nullptr; // Not enough space

        void* block = start;
        start += size; // bump start pointer
        head->bytes_used += size;

        return block;
    }

    size_t freeSpace() const {
        if(!start) return 0;
        return end - start;
    }

    Chunk* getChunks() const {
        return head;
    }

};

struct SlabAllocator {

    static constexpr size_t LARGE_CHUNK = 64 * 1024 * 1024; //64MB
    static constexpr size_t SMALL_CHUNK = 128 * 1024; //64KB

    GlobalAllocator* level1;
    BumpAlloc level2;
    std::vector<BumpAlloc> level3;
    std::vector<size_t> counts;
    
    public:
    SlabAllocator(GlobalAllocator* global, size_t num_parts) {
        level1 = global;
        level3.resize(num_parts);
        counts.assign(num_parts,0);
    }

    template<typename T>
    void consume(const T& tuple, size_t part_num) {

        if(level3[part_num].freeSpace() < sizeof(tuple)) { // level3 out of space
            if(level2.freeSpace() < SMALL_CHUNK) { // level2 out of space
                    //allocate new chunk from level 1, global allocator
                    level2.addSpace(level1->allocate(LARGE_CHUNK),LARGE_CHUNK);
            }
            // allocate new chunk from level2 bump allocator
            level3[part_num].addSpace(level2.allocate(SMALL_CHUNK),SMALL_CHUNK);
        }

        // Allocate and assign new tuple from level3 bump allocato of the current partition
        T* new_chunk = (T*)level3[part_num].allocate(sizeof(tuple));
        *new_chunk = tuple;
        counts[part_num]+=1;
        
    }
};

} // namespace Contest
