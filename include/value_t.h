#include <cstdint>

struct StrRef {

    uint64_t ref = 0;

    static constexpr uint64_t NULL_BIT = 1ull << 62;
    static constexpr uint64_t LONG_BIT = 1ull << 61;

    static constexpr int TABLE_LEN  = 10;
    static constexpr int COLUMN_LEN = 10;
    static constexpr int PAGE_LEN   = 16;
    static constexpr int OFFSET_LEN   = 16;
    static constexpr int PADDING = 9;   // 9 least significant padding bits 


    static constexpr int TABLE_SHIFT  = PADDING + OFFSET_LEN + PAGE_LEN + COLUMN_LEN;
    static constexpr int COLUMN_SHIFT = PADDING + OFFSET_LEN + PAGE_LEN;
    static constexpr int PAGE_SHIFT   = PADDING + OFFSET_LEN;
    static constexpr int OFFSET_SHIFT   = PADDING;

    static constexpr uint64_t TABLE_MASK  = (1ull << TABLE_LEN) - 1;
    static constexpr uint64_t COLUMN_MASK = (1ull << COLUMN_LEN) - 1;
    static constexpr uint64_t PAGE_MASK   = (1ull << PAGE_LEN) - 1;
    static constexpr uint64_t OFFSET_MASK   = (1ull << OFFSET_LEN) - 1;


    StrRef() = default;

    StrRef(bool is_null,bool is_long,uint64_t table,uint64_t col, uint64_t page, uint64_t off_idx) {
        
        if(is_null){ref |= NULL_BIT; return;} // string is null
        if(is_long) ref |= LONG_BIT;

        ref |= (table & TABLE_MASK) << TABLE_SHIFT;
        ref |= (col & COLUMN_MASK) << COLUMN_SHIFT;
        ref |= (page & PAGE_MASK) << PAGE_SHIFT;
        ref |= (off_idx & OFFSET_MASK) << OFFSET_SHIFT;

    }

    void clear() {
        ref = 0;
    }

    void encode(bool is_null =false ,bool is_long=false,uint64_t table=0,uint64_t col=0, uint64_t page=0, uint64_t off_idx=0) {
  
        if(is_null){ref |= NULL_BIT; return;} // string is null so refernce bits dont matter
        if(is_long) ref |= LONG_BIT;

        ref |= (table & TABLE_MASK) << TABLE_SHIFT;
        ref |= (col & COLUMN_MASK) << COLUMN_SHIFT;
        ref |= (page & PAGE_MASK) << PAGE_SHIFT;
        ref |= (off_idx & OFFSET_MASK) << OFFSET_SHIFT;
     
    }
    uint64_t get_table() const { return (ref >> TABLE_SHIFT) & TABLE_MASK; }
    uint64_t get_column() const { return (ref >> COLUMN_SHIFT) & COLUMN_MASK; }
    uint64_t get_page() const { return (ref >> PAGE_SHIFT) & PAGE_MASK; }
    uint64_t get_offset() const { return (ref >> OFFSET_SHIFT) & OFFSET_MASK; }
    
    bool is_null() const { return ref & NULL_BIT; }
    bool is_long() const { return ref & LONG_BIT; }


};