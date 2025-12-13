#pragma once
#include <cstdint>
#include <stdexcept>
#include <string>


struct StrRef {

    uint64_t ref = 0;

    // 2 MSBs are unused for StrRef
    static constexpr uint64_t LONGSTR_BIT = 1ull << 61;

    static constexpr int TABLE_LEN  = 10;
    static constexpr int COLUMN_LEN = 10;
    static constexpr int PAGE_LEN   = 18;
    static constexpr int OFFSET_LEN   = 16;
    static constexpr int PADDING = 7;   // 7 least significant padding bits 


    static constexpr int TABLE_SHIFT  = PADDING + OFFSET_LEN + PAGE_LEN + COLUMN_LEN;
    static constexpr int COLUMN_SHIFT = PADDING + OFFSET_LEN + PAGE_LEN;
    static constexpr int PAGE_SHIFT   = PADDING + OFFSET_LEN;
    static constexpr int OFFSET_SHIFT   = PADDING;

    static constexpr uint64_t TABLE_MASK  = (1ull << TABLE_LEN) - 1;
    static constexpr uint64_t COLUMN_MASK = (1ull << COLUMN_LEN) - 1;
    static constexpr uint64_t PAGE_MASK   = (1ull << PAGE_LEN) - 1;
    static constexpr uint64_t OFFSET_MASK   = (1ull << OFFSET_LEN) - 1;


    StrRef() = default;

    StrRef(bool is_long,uint64_t table,uint64_t col, uint64_t page, uint64_t off_idx) {

        if(table > TABLE_MASK)
            throw std::out_of_range("StrRef table overflow: " + std::to_string(table) + 
                                    " > " + std::to_string(TABLE_MASK));
        if(col > COLUMN_MASK)
            throw std::out_of_range("StrRef column overflow: " + std::to_string(col) + 
                                    " > " + std::to_string(COLUMN_MASK));
        if(page > PAGE_MASK)
            throw std::out_of_range("StrRef page overflow: " + std::to_string(page) + 
                                    " > " + std::to_string(PAGE_MASK));
        if(off_idx > OFFSET_MASK)
            throw std::out_of_range("StrRef offset overflow: " + std::to_string(off_idx) + 
                                    " > " + std::to_string(OFFSET_MASK));
        
        if(is_long) ref |= LONGSTR_BIT;

        ref |= (table & TABLE_MASK) << TABLE_SHIFT;
        ref |= (col & COLUMN_MASK) << COLUMN_SHIFT;
        ref |= (page & PAGE_MASK) << PAGE_SHIFT;
        ref |= (off_idx & OFFSET_MASK) << OFFSET_SHIFT;

    }

    void clear() {
        ref = 0;
    }

    void encode(bool is_long=false,uint64_t table=0,uint64_t col=0, uint64_t page=0, uint64_t off_idx=0) {

        if(table > TABLE_MASK)
            throw std::out_of_range("StrRef table overflow: " + std::to_string(table) + 
                                    " > " + std::to_string(TABLE_MASK));
        if(col > COLUMN_MASK)
            throw std::out_of_range("StrRef column overflow: " + std::to_string(col) + 
                                    " > " + std::to_string(COLUMN_MASK));
        if(page > PAGE_MASK)
            throw std::out_of_range("StrRef page overflow: " + std::to_string(page) + 
                                    " > " + std::to_string(PAGE_MASK));
        if(off_idx > OFFSET_MASK)
            throw std::out_of_range("StrRef offset overflow: " + std::to_string(off_idx) + 
                                    " > " + std::to_string(OFFSET_MASK));
        
        if(is_long) ref |= LONGSTR_BIT;

        ref |= (table & TABLE_MASK) << TABLE_SHIFT;
        ref |= (col & COLUMN_MASK) << COLUMN_SHIFT;
        ref |= (page & PAGE_MASK) << PAGE_SHIFT;
        ref |= (off_idx & OFFSET_MASK) << OFFSET_SHIFT;
     
    }
    size_t get_table() const { return (ref >> TABLE_SHIFT) & TABLE_MASK; }
    size_t get_column() const { return (ref >> COLUMN_SHIFT) & COLUMN_MASK; }
    size_t get_page() const { return (ref >> PAGE_SHIFT) & PAGE_MASK; }
    size_t get_offset() const { return (ref >> OFFSET_SHIFT) & OFFSET_MASK; }
    
    bool is_long() const { return ref & LONGSTR_BIT; }


};

struct value_t {
    // MSB 01 -> int32_t
    // MSB 10 -> StrRef
    // MSB 00 -> Null

    uint64_t value = 0; 

    void parse_int32(int32_t num) {
        value = static_cast<uint32_t>(num);
        value |= (1ull << 62); // mark value as int32
    }

    void parse_strref(StrRef str) {
        value = str.ref & ~(3ull << 62);
        value |= (1ull << 63);
    }

    bool is_int32() const { return (value >> 62) == 1;}
    bool is_strref() const { return (value >> 62) == 2;}
    bool is_null() const { return (value >> 62) == 0;}

    int32_t get_int32() const {
        return static_cast<int32_t>(value); 
    }

    StrRef get_strref() const { 
        StrRef ref; 
        ref.ref = value & ~(3ull << 62); 
        return ref; 
    }


};

