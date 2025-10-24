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
    size_t capacity = DEFAULT_CAPACITY;
    size_t size = 0;
    double load_factor = 0;
    std::vector<Bucket> hash_table;

    size_t get_index(const Key& key) {
        size_t hash = std::hash<Key>{}(key);
        return hash % capacity;
    }

public:

    RobinhoodHash() {
        hash_table.resize(DEFAULT_CAPACITY);
    }

    //Inserts key value pair in hash table
    void emplace(const Key& key,const Value& value) {
        
        unsigned int cur_psl = 0;
        size_t index = get_index(key);

        Bucket temp_bucket = Bucket{key,value,0,true};
        Bucket *cur_bucket = &hash_table[index];
        // rehash around here
        while (cur_bucket->occupied) {
            if (cur_psl > cur_bucket->psl) {
                cur_psl = cur_bucket->psl;
                std::swap(temp_bucket,*cur_bucket);
            } else {
                cur_psl++;
            }

            cur_bucket = &hash_table[(index + 1)%capacity];
        }

        *cur_bucket = temp_bucket;
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

