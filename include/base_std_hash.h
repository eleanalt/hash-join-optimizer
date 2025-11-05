#pragma once

#include <unordered_map>
#include <vector>

namespace Contest {

// Default std::unordered_map implementation
// Different Hashing Algorithms must implement and expose the same public methods.
template <typename Key, typename Value>
class StdHash {
    std::unordered_map<Key,Value> hash_table;
public:

    
    //Inserts key value pair in hash table
    void emplace(const Key& key,const Value& value) {
        hash_table.emplace(key,value);
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

