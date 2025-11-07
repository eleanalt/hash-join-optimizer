#pragma once

#include <vector>
#include <functional>
#include <utility>
#include <limits>

namespace Contest {

template <typename Key, typename Value> 
//CuckooHash class implementation   
class CuckooHash{
private:
    struct Entry{
        Key key{};
        Value value{};
        bool occupied = false;
    };

    static constexpr double MAX_LOAD_FACTOR = 0.5;
    static constexpr std::size_t DEFAULT_CAPACITY = 1024;

    std::size_t capacity;
    std::size_t sz = 0;
    std::vector<Entry> table1;
    std::vector<Entry> table2;

    //Calculation of next power of two
    static std::size_t next_pow_of2(std::size_t x){
        if(x==0) return 1; 
        if((x & (x-1))==0) return x;
        //Checking for overflow
        if (x > (std::numeric_limits<std::size_t>::max() >> 1)){
            std::size_t p = 1ULL << (sizeof(std::size_t)*8 - 1);
            return p;
        }
        std::size_t p = 1;
        while (p<x) p <<= 1;
        return p;
    }
    //mask for faster mod(if capacity is power of 2)
    inline std::size_t mask() const noexcept { return capacity - 1; }

//First hash function
inline std::size_t h1(const Key& key) const noexcept{
    std::size_t hash = std::hash<Key>{}(key);
    hash ^=hash >> 33;
    hash *= 0xff51afd7ed558ccdULL;  
    hash ^= hash >> 33;                               
    return hash & mask();                       
}

inline std::size_t h2(const Key& key) const noexcept{
    std::size_t hash = std::hash<Key>{}(key);
    hash ^=(hash << 13);
    hash ^=(hash >> 7);
    hash ^=(hash << 17);                             
    return hash & mask();                       
}

//Insertion again of all the elemenents in double capacity

void rehash_and_grow() { //Called when load factor exceeds 0.5 or cycle pattern
    capacity <<= 1; //Doubling capacity
    //Moving old tables
    std::vector<Entry> old1 = std::move(table1);
    std::vector<Entry> old2 = std::move(table2);
    //Creating 2 new tables with new capacity
    table1.assign(capacity, {});
    table2.assign(capacity, {});
    std::size_t old_size = sz; //Keeping olfd size for checks
    sz=0; //Counting again
    for (auto& e : old1) if (e.occupied) emplace_internal(std::move(e.key), std::move(e.value));
    for (auto& e : old2) if (e.occupied) emplace_internal(std::move(e.key), std::move(e.value));

    (void)old_size;
}
//Checking if load factor > 0.5
inline bool lf_check(std::size_t extra = 1) const noexcept{
    double proj_load = static_cast<double>(sz + extra) / (2.0 * static_cast<double>(capacity));
    return proj_load>MAX_LOAD_FACTOR; //If lf>0.5 need for rehash
}

template <typename K, typename V>
void emplace_internal(K&& key, V&& value){
    Key cur_key = std::forward<K>(key); //current key to insert
    Value cur_val = std::forward<V>(value); //curent key's value
    //Allowing up to n displacements

    std::size_t displacements = 0;
    bool in_table1 = true;

    while(true){
        std::size_t idx = in_table1 ? h1(cur_key) : h2(cur_key); //Index calculation according to table we are at

        Entry& bucket = in_table1 ? table1[idx] : table2[idx];  
        
        if(!bucket.occupied){
            bucket.key = std::move(cur_key); 
            bucket.value = std::move(cur_val);
            bucket.occupied = true;
            ++sz;
            return;
        }

        if (bucket.key == cur_key){
            bucket.value = std::move(cur_val);
            return;
        }

        std::swap(cur_key,bucket.key);
        std::swap(cur_val,bucket.value);

        in_table1 = !in_table1;
        if (++displacements >= sz){
            rehash_and_grow();
            in_table1 = true;
        }
    }
}

public:

CuckooHash():
capacity(DEFAULT_CAPACITY),table1(DEFAULT_CAPACITY),table2(DEFAULT_CAPACITY){}

    explicit CuckooHash(std::size_t init_capacity){
        capacity = next_pow_of2(init_capacity == 0 ? 1 : init_capacity);
        table1.assign(capacity, {});
        table2.assign(capacity, {});
    }

    std::size_t size() const noexcept {return sz;}
    bool empty() const noexcept {return sz==0;}
    std::size_t bucket_count() const noexcept {return capacity;}
     double load_factor() const noexcept {  
        return (capacity == 0) ? 0.0     
             : static_cast<double>(sz) / (2.0 * static_cast<double>(capacity)); 
    }

    //Checking load factor before insertion
    template <typename K, typename V>
    void emplace(K&& key, V&& value){
        if(lf_check(contains(key) ? 0: 1))
        rehash_and_grow();
    emplace_internal(std::forward<K>(key), std::forward<V>(value));
    }

    void insert_or_assign(const Key& key, const Value& value) { emplace(key, value);}
    void insert_or_assign(Key&& key, const Value&& value) { emplace(std::move(key), std::move(value)); }

    // Checking if key exists
    bool contains(const Key& key) const noexcept{
        std::size_t i1=h1(key);
        if (table1[i1].occupied && table1[i1].key == key) return true;
        std::size_t i2=h2(key);
        if (table2[i2].occupied && table2[i2].key == key) return true;
        return false;
    }

    //Returns key to value or null if it does not exist
    Value* find(const Key& key) noexcept{
        std::size_t i1=h1(key);
        if(table1[i1].occupied && table1[i1].key == key) return &table1[i1].value;
        std::size_t i2=h2(key);
        if(table2[i2].occupied && table2[i2].key == key) return &table2[i2].value;
        return nullptr;
    }

    Value& operator[](const Key& key){
        std::size_t i1 = h1(key);
        if (table1[i1].occupied && table1[i1].key == key) return table1[i1].value;
        std::size_t i2 = h2(key);
        if (table2[i2].occupied && table2[i2].key == key) return table2[i2].value;

        emplace (key, Value{});

        i1=h1(key);
        if (table1[i1].occupied && table1[i1].key == key) return table1[i1].value;
        i2=h2(key);
        return table2[i2].value;
    }
};
}