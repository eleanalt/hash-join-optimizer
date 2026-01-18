#pragma once
#include <cstdint>
#include <stdexcept>
#include <string.h>
#include <vector>
#include <memory>
#include "plan.h"
#include "attribute.h"
#include "value_t.h"

static constexpr size_t COLUMN_PAGE_LEN = 1024;
static constexpr size_t PAGE_DIVISION_SHIFT = 10;

constexpr size_t DENSE_ROWS_PER_PAGE = 1984; // rows in page when page is fully dense

struct ColumnPage {
    value_t data[COLUMN_PAGE_LEN] = {};
};

struct column_t {
    size_t rows_num;
    DataType type;
    std::vector<std::unique_ptr<ColumnPage>>  pages;

    bool direct_flag;
    const Column* input_col;

    column_t(DataType data_type) : rows_num(0), type(data_type), direct_flag(false), input_col(nullptr) {}
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

        if(direct_flag) {
            value_t value;

            size_t page_idx = row_idx/DENSE_ROWS_PER_PAGE;
            size_t page_offset = row_idx%DENSE_ROWS_PER_PAGE;

            auto* page = input_col->pages[page_idx]->data;

            int32_t* data_begin = reinterpret_cast<int32_t*>(page + 4);

            value.parse_int32(data_begin[page_offset]);
            return value;
        } 

        size_t page_idx = row_idx >> PAGE_DIVISION_SHIFT;
        size_t page_offset = row_idx & (COLUMN_PAGE_LEN -1);

        return pages[page_idx]->data[page_offset];
    }

    value_t operator[](size_t row_idx) {
        return get_row_value(row_idx);
    }

    value_t operator[](size_t row_idx) const {
        return get_row_value(row_idx);
    }

    void use_input_col(const Column* in_col,size_t row_count) {
        direct_flag = true;
        input_col = in_col;
        rows_num = row_count;
    }

};

