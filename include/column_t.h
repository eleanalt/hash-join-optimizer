#include <cstdint>
#include <stdexcept>
#include <string.h>
#include <vector>
#include "attribute.h"
#include "value_t.h"

static constexpr size_t COLUMN_PAGE_LEN = 1024;

struct ColumnPage {
    value_t data[COLUMN_PAGE_LEN] = {};
};

struct column_t {
    size_t rows_num;
    DataType type;
    std::vector<ColumnPage*>  pages;

    column_t(DataType data_type) : rows_num(0), type(data_type) {}

    ColumnPage* new_page() {
        ColumnPage* page = new ColumnPage();
        pages.push_back(page);
        return page;
    }

    void append_row(value_t value) {
        size_t page_rows = rows_num % COLUMN_PAGE_LEN;
        ColumnPage* page = (page_rows == 0)? new_page() : pages.back();

        page->data[page_rows] = value;
        rows_num++;
    
    }

    value_t get_row_value (size_t row_idx) const {
        size_t page_idx = row_idx / COLUMN_PAGE_LEN;
        size_t page_offset = row_idx % COLUMN_PAGE_LEN;

        return pages[page_idx]->data[page_offset];
    }

    value_t& operator[](size_t row_idx) {
        size_t page_idx = row_idx / COLUMN_PAGE_LEN;
        size_t page_offset = row_idx % COLUMN_PAGE_LEN;

        return pages[page_idx]->data[page_offset];    
    }

    ~column_t() {
        for(auto* page : pages) {
            delete page;
        }
    }
};

