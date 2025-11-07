#pragma once

#include <unordered_map>
#include <vector>
#include <bitset>

namespace Contest {

// Hopscotch hashing algorithm implementation
template <typename Key, typename Value>
class HopscotchHash {

private:
    static constexpr std::size_t H = 32;

    struct Bucket {
        Key key;
        Value value;
        std::bitset<H> bitmap{0};
        bool occupied = false;

    };

    static constexpr size_t DEFAULT_CAPACITY = 16384;
    static constexpr double MAX_LOAD_FACTOR = 0.9;

    size_t capacity = DEFAULT_CAPACITY;
    size_t size = 0;
    std::vector<Bucket> hash_table;

public:

    size_t get_index(const Key& key) const {
        size_t hash = std::hash<Key>{}(key);

        //murmur3 finalizer
        hash ^= hash >> 33;
        hash *= 0xff51afd7ed558ccdULL;
        hash ^= hash >> 33;
        hash *= 0xc4ceb9fe1a85ec53ULL;
        hash ^= hash >> 33;

        return hash & (capacity - 1); 
    }

    void rehash() {
        capacity = capacity*2;

        std::vector<Bucket> old_ht = std::move(hash_table);
        hash_table = std::vector<Bucket>(capacity);

        size = 0;
        for (const Bucket& b : old_ht) {
            if(b.occupied) {
                emplace(b.key,b.value);
            }
        }
    }


    // Probe for empty position in hash_table after a given index
    bool probe(const size_t& ref_index,size_t& out_index) const  {

        if(size == capacity) return false;

        size_t index = ref_index;
        while (hash_table[index].occupied) {
            index = (index + 1) % capacity;
        }

        return true;
    }

    // Computes distance even if to < from
    inline size_t dist(size_t from, size_t to) const {
        return (to + capacity - from) & (capacity - 1);
    }

public:

//Inserts key value pair in hash table
void emplace(const Key& key, const Value& value) {
    size_t index = get_index(key);
    size_t empty_index = 0;

    // If load factor exceeded,home index neighbourhood is full or no empty slot found rehash and retry
    if ((double)size/capacity > MAX_LOAD_FACTOR || hash_table[index].bitmap.all() || !probe(index, empty_index)) {
        rehash();
        index = get_index(key);
        probe(index, empty_index);
    }

    // Begin bringing empty slot in home's neighbourhood
    while (dist(index, empty_index) >= H) {
        bool moved = false;

        // For each candidate preceding empty slot by H -1
        for (size_t off = 1; off < H; ++off) {
            size_t candidate_bucket = (empty_index + capacity - off) & (capacity - 1);
            std::bitset<H>& bitmap = hash_table[candidate_bucket].bitmap;

            // For each slot in candidates's neighbourhood
            for (size_t bit = 0; bit < H; ++bit) {
                if (!bitmap[bit]) continue;

                size_t from = (candidate_bucket + bit) & (capacity - 1); // Slot being swapped

                // Check element is before empty slot
                if (dist(candidate_bucket, from) >= dist(candidate_bucket, empty_index))
                    continue;

                // Check element stays in its home neighbourhood after moving
                if (dist(candidate_bucket, empty_index) >= H)
                    continue;

                // Perform the move
                hash_table[empty_index].key = std::move(hash_table[from].key);
                hash_table[empty_index].value = std::move(hash_table[from].value);
                hash_table[empty_index].occupied = true;
                hash_table[from].occupied = false;

                // Fix bitmap
                bitmap.reset(bit);
                bitmap.set(dist(candidate_bucket, empty_index));

                empty_index = from;
                moved = true;
                break;
            }
            if (moved) break;
        }

        // Table is full so rehash and retry
        if (!moved) {
            rehash();
            emplace(key, value);
            return;
        }
    }

    // Finally perform the insertion in the keys neighbourhood
    hash_table[empty_index].key = key;
    hash_table[empty_index].value = value;
    hash_table[empty_index].occupied = true;
    hash_table[index].bitmap.set(dist(index, empty_index));
    size++;
}



    //Checks whether key is contained in hash table
    bool contains(const Key& key) const  {

    }

    //Retrieve value at index key
    Value& operator[](const Key& key) {

    }
};

}

