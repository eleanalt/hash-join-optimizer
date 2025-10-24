#pragma once

#include <vector>

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
    
    static constexpr size_t DEFAULT_CAPACITY = 128;
    static constexpr double MAX_LOAD_FACTOR = 0.7;

    size_t capacity = DEFAULT_CAPACITY;
    size_t size = 0;
    std::vector<Bucket> hash_table;

    size_t get_index(const Key& key) {
        size_t hash = std::hash<Key>{}(key);
        return hash % capacity;
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

public:

    RobinhoodHash() {
        hash_table.resize(DEFAULT_CAPACITY);
    }

    //Inserts key value pair in hash table
    void emplace(const Key& key,const Value& value) {

        if ((double)size/capacity > MAX_LOAD_FACTOR) {
            rehash();
        }

        size_t index = get_index(key);

        Bucket temp_bucket = Bucket{key,value,0,true};
        Bucket *cur_bucket = &hash_table[index];

        while (cur_bucket->occupied) {
            if (temp_bucket.psl > cur_bucket->psl) {
                std::swap(temp_bucket,*cur_bucket);
            } else {
                temp_bucket.psl++;
            }

            index = (index + 1) % capacity;
            cur_bucket = &hash_table[index];
        }

        *cur_bucket = temp_bucket;
        size++;
    }

    //Checks whether key is contained in hash table
    bool contains(const Key& key) const  {
        auto itr = hash_table.find(key); 
        return itr != hash_table.end();
    }

    //Retrieve value at index key
    Value& operator[](const Key& key) {
        return hash_table[key];
    }
};

}

