#pragma once
#include <stdexcept>
#include "column_t.h"

namespace Contest {

static inline void merge_column_pages(column_t& dst, column_t& src) {

    if (src.direct_flag) {

        for (size_t i = 0; i < src.rows_num; ++i) {
            dst.append_row(src[i]);
        }
        return;
    }

    if (dst.rows_num == 0 && dst.pages.empty() && !dst.direct_flag) {
        dst.pages.reserve(dst.pages.size() + src.pages.size());
        for (auto& p : src.pages) {
            dst.pages.push_back(std::move(p));
        }
        dst.rows_num += src.rows_num;

        src.pages.clear();
        src.rows_num = 0;
        return;
    }
    for (size_t i = 0; i < src.rows_num; ++i) {
        dst.append_row(src[i]);
    }
}

} // namespace Contest
