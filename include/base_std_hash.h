#pragma once

#include <unordered_map>
#include <vector>

namespace Contest {
template <typename Key, typename Value>
class StdHash {
    std::unordered_map<Key,Value> hash_table;
public:

    void emplace(const Key& key,const Value& value) {
        hash_table.emplace(key,value);
    }

    bool contains(const Key& key) const  {
        auto itr = hash_table.find(key); 
        return itr != hash_table.end();
    }

    Value& operator[](const Key& key) {
        return hash_table[key];
    }
};

}

