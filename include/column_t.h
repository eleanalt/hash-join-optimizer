#pragma once
#include <cstdint>
#include <stdexcept>
#include <string.h>
#include <vector>
#include <memory>
#include "attribute.h"
#include "value_t.h"

static constexpr size_t COLUMN_PAGE_LEN = 1024;
static constexpr size_t PAGE_DIVISION_SHIFT = 10;


struct ColumnPage {
    value_t data[COLUMN_PAGE_LEN] = {};
};

struct column_t {
    size_t rows_num;
    DataType type;
    std::vector<std::unique_ptr<ColumnPage>>  pages;

    column_t(DataType data_type) : rows_num(0), type(data_type) {}
    column_t(column_t&&) noexcept = default;
    column_t& operator=(column_t&&) noexcept = default;

    ColumnPage* new_page() {
        pages.push_back(std::make_unique<ColumnPage>());
        return pages.back().get();   
    }

    void append_row(value_t value) {
        size_t page_rows = rows_num & (COLUMN_PAGE_LEN-1);
        ColumnPage* page = (page_rows == 0)? new_page() : pages.back().get();

        page->data[page_rows] = value;
        rows_num++;
    
    }

    value_t get_row_value (size_t row_idx) const {
        size_t page_idx = row_idx >> PAGE_DIVISION_SHIFT;
        size_t page_offset = row_idx & (COLUMN_PAGE_LEN -1);

        return pages[page_idx]->data[page_offset];
    }

    value_t& operator[](size_t row_idx) {
        size_t page_idx = row_idx >> PAGE_DIVISION_SHIFT;
        size_t page_offset = row_idx & (COLUMN_PAGE_LEN -1);

        return pages[page_idx]->data[page_offset];  
    }

};

