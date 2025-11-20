#pragma once

#include <vector>
#include <iostream>

namespace Contest {



// Robinhood hashing algorithm implementation
template <typename Key, typename Value>
class RobinhoodHash {

private:
    struct Bucket {
        Key key;
        Value value;
        unsigned int psl = 0;
        bool occupied = false;
    };
    static constexpr size_t DEFAULT_CAPACITY = 32768;
    static constexpr double MAX_LOAD_FACTOR = 0.9;

    size_t capacity = DEFAULT_CAPACITY;
    size_t size = 0;
    std::vector<Bucket> hash_table;

    size_t get_index(const Key& key) const {
        size_t hash = std::hash<Key>{}(key);
        // Murmur3 finalizer, necessary for good index distributions and shorter PSL chains
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

    bool probe(const Key& key,size_t& out_index) const  {
        size_t index = get_index(key);
        size_t cur_psl = 0;

        // Search until key found or probed bucket psl violates robinhood invariant
        while (true) {
            const Bucket &bucket = hash_table[index];
            if (!bucket.occupied || cur_psl > bucket.psl) return false;
            if (bucket.key == key) { 
                out_index = index; 
                return true; 
            }
            cur_psl++;
            index = (index + 1) & (capacity - 1);
        }


        return false;
    }
    

    

public:

    RobinhoodHash() {
        hash_table.resize(DEFAULT_CAPACITY);
    }

    RobinhoodHash(size_t init_capacity) {
        hash_table.resize(init_capacity);
        capacity = init_capacity;
    }

    size_t get_size() {
        return size;
    }

    //Inserts key value pair in hash table
    void emplace(const Key& key,const Value& value) {

        if ((double)size/capacity > MAX_LOAD_FACTOR) {
            rehash();
        }

        size_t index = get_index(key);

        Bucket temp_bucket = Bucket{key,std::move(value),0,true};

        // Probe occupied buckets
        while (hash_table[index].occupied) {
            if (hash_table[index].key == key) return; // Ignore emplace for existing key
            if (temp_bucket.psl > hash_table[index].psl) { // Bucket at current probing position has higher psl
                std::swap(temp_bucket,hash_table[index]); // Steal bucket
            } 

            temp_bucket.psl++;

            index = (index + 1) & (capacity - 1);
        }

        hash_table[index] = temp_bucket;
        size++;
    }

    //Checks whether key is contained in hash table
    bool contains(const Key& key) const  {
        size_t index;
        return probe(key,index);
    }

    //Retrieve value at index key
    Value& operator[](const Key& key) {
        size_t index;
        if(probe(key,index)) {
            return hash_table[index].value;
        }

        emplace(key,Value{});
        probe(key,index);

        return hash_table[index].value;
    }
};

}

