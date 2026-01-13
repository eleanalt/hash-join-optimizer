#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "hash_config.h"
#include "base_std_hash.h"
#include "value_t.h"
#include "column_t.h"
#include "column_merge.h"
#include <atomic>
#include "partitioned_directory_hash.h"
#include "column_merge.h"
#include <thread>

#ifdef USE_UNCHAINED_HASH
#include "unchained.h"
#endif

namespace Contest {

using ExecuteResult = std::vector<column_t>;

ExecuteResult execute_impl(const Plan& plan, size_t node_idx);

struct JoinAlgorithm {
    bool                                             build_left;
    ExecuteResult&                                   left;
    ExecuteResult&                                   right;
    ExecuteResult&                                   results;
    size_t                                           left_col, right_col;
    const std::vector<std::tuple<size_t, DataType>>& output_attrs;

    template <class T>
    auto run() {
        using namespace Contest;

        // Determine build and probe sides
        ExecuteResult& build_side = build_left ? left : right;
        ExecuteResult& probe_side = build_left ? right : left;

        size_t build_col = build_left ? left_col : right_col;
        size_t probe_col = build_left ? right_col : left_col;

        // Prepare final output columns
        results.reserve(output_attrs.size());
        for (auto [idx, dtype] : output_attrs) {
            results.emplace_back(column_t(dtype));
        }

        // We implement "by the book" directory+tupleStorage build for INT32 join keys
        const size_t num_threads = default_num_threads_env();

        static bool printed = false;
        if (!printed) {
            printed = true;
            std::cerr << "[DIRJOIN] threads=" << num_threads << "\n";
        }

        column_t& build_join_col = build_side[build_col];
        column_t& probe_join_col = probe_side[probe_col];

        // ---------- BUILD: partitions + directory(count/prefix/copy) ----------
        PartitionedDirectoryHash index;

        // directory_bits controls directory size (2^bits entries).
        // 16 => 65536 entries, good baseline.
        index.init(/*num_parts=*/num_threads, /*directory_bits=*/16);

        // Minimal optional type (no <optional> to keep it simple)
        struct OptU32 {
            bool     has;
            uint32_t v;
            bool has_value() const { return has; }
            uint32_t value() const { return v; }
        };

        index.build(build_join_col.rows_num, num_threads, [&](size_t row) -> OptU32 {
            auto v = build_join_col.get_row_value(row);
            if (v.is_null()) return {false, 0};
            if (!v.is_int32()) throw std::runtime_error("Join key isn't of type int32_t");
            return {true, static_cast<uint32_t>(v.get_int32())};
        });

        // ---------- PROBE: threaded probing with work stealing ----------
        // Work stealing over chunks of probe rows
        const size_t chunk_rows  = 4096;
        const size_t nprobe      = probe_join_col.rows_num;
        const size_t num_chunks  = (nprobe + chunk_rows - 1) / chunk_rows;

        std::atomic<size_t> next_chunk{0};

        // Each thread stores (chunk_id + columns).
        // Later we merge chunks in chunk order to remain deterministic.
        struct ChunkOut {
            size_t       chunk_id;
            ExecuteResult cols;
        };

        std::vector<std::vector<ChunkOut>> thread_chunks(num_threads);

        auto make_cols = [&]() {
            ExecuteResult cols;
            cols.reserve(output_attrs.size());
            for (auto [idx, dtype] : output_attrs) {
                cols.emplace_back(column_t(dtype));
            }
            return cols;
        };

        std::vector<std::thread> threads;
        threads.reserve(num_threads);

        for (size_t tid = 0; tid < num_threads; ++tid) {
            threads.emplace_back([&, tid]() {
                while (true) {
                    size_t cid = next_chunk.fetch_add(1, std::memory_order_relaxed);
                    if (cid >= num_chunks) break;

                    size_t begin = cid * chunk_rows;
                    size_t end   = std::min(nprobe, begin + chunk_rows);

                    ChunkOut out;
                    out.chunk_id = cid;
                    out.cols     = make_cols();

                    for (size_t row_idx = begin; row_idx < end; ++row_idx) {
                        auto v = probe_join_col.get_row_value(row_idx);
                        if (v.is_null()) continue;
                        if (!v.is_int32()) throw std::runtime_error("Join key isn't of type int32_t");

                        uint32_t key = static_cast<uint32_t>(v.get_int32());

                        index.probe(key, [&](size_t build_idx) {
                            size_t out_idx = 0;
                            for (auto [col_idx, _] : output_attrs) {
                                value_t val;

                                // Get output column value from left+right in correct order
                                if (build_left) {
                                    if (col_idx < left.size()) {
                                        val = build_side[col_idx][build_idx];
                                    } else {
                                        val = probe_side[col_idx - left.size()][row_idx];
                                    }
                                } else {
                                    if (col_idx < left.size()) {
                                        val = probe_side[col_idx][row_idx];
                                    } else {
                                        val = build_side[col_idx - left.size()][build_idx];
                                    }
                                }

                                out.cols[out_idx++].append_row(val);
                            }
                        });
                    }

                    thread_chunks[tid].push_back(std::move(out));
                }
            });
        }

        for (auto& th : threads) th.join();
        threads.clear();

        // ---------- AGGREGATION: single-threaded, deterministic ----------
        std::vector<ChunkOut*> all;
        all.reserve(num_chunks);

        for (auto& vec : thread_chunks) {
            for (auto& co : vec) {
                all.push_back(&co);
            }
        }

        std::sort(all.begin(), all.end(),
                  [](const ChunkOut* a, const ChunkOut* b) { return a->chunk_id < b->chunk_id; });

        for (auto* co : all) {
            for (size_t c = 0; c < results.size(); ++c) {
                merge_column_pages(results[c], co->cols[c]);
            }
        }
    }
};

ExecuteResult execute_hash_join(const Plan&          plan,
    const JoinNode&                                  join,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    auto                           left_idx    = join.left;
    auto                           right_idx   = join.right;
    auto&                          left_node   = plan.nodes[left_idx];
    auto&                          right_node  = plan.nodes[right_idx];
    auto&                          left_types  = left_node.output_attrs;
    auto&                          right_types = right_node.output_attrs;
    auto                           left        = execute_impl(plan, left_idx);
    auto                           right       = execute_impl(plan, right_idx);
    std::vector<column_t> results;

    JoinAlgorithm join_algorithm{.build_left = join.build_left,
        .left                                = left,
        .right                               = right,
        .results                             = results,
        .left_col                            = join.left_attr,
        .right_col                           = join.right_attr,
        .output_attrs                        = output_attrs};
    if (join.build_left) {
        switch (std::get<1>(left_types[join.left_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    } else {
        switch (std::get<1>(right_types[join.right_attr])) {
        case DataType::INT32:   join_algorithm.run<int32_t>(); break;
        case DataType::INT64:   join_algorithm.run<int64_t>(); break;
        case DataType::FP64:    join_algorithm.run<double>(); break;
        case DataType::VARCHAR: join_algorithm.run<std::string>(); break;
        }
    }

    return results;
}

bool get_bitmap(const uint8_t* bitmap, uint16_t idx) {
    auto byte_idx = idx / 8;
    auto bit      = idx % 8;
    return bitmap[byte_idx] & (1u << bit);
}

void set_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx     = idx / 8;
    auto bit          = idx % 8;
    bitmap[byte_idx] |= (1u << bit);
}

void unset_bitmap(std::vector<uint8_t>& bitmap, uint16_t idx) {
    while (bitmap.size() < idx / 8 + 1) {
        bitmap.emplace_back(0);
    }
    auto byte_idx     = idx / 8;
    auto bit          = idx % 8;
    bitmap[byte_idx] &= ~(1u << bit);
}

bool col_is_dense(const Column& column) {

    for (size_t page_idx = 0; page_idx < column.pages.size(); ++page_idx) {
        auto* page = column.pages[page_idx]->data;
        auto  num_rows   = *reinterpret_cast<uint16_t*>(page);
        auto non_null_rows = *reinterpret_cast<int16_t*>(page + 2);

        if (num_rows != non_null_rows) return false;
    }

    return true;
}

// Returns a columnar table ExecuteResult of column_t's with value_t entries
ExecuteResult copy_scan(const ColumnarTable& table, size_t table_id,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    namespace views = ranges::views;

    //Initialize output columns
    ExecuteResult results;
    results.reserve(output_attrs.size());
    for (auto [idx, dtype] : output_attrs) {
        results.emplace_back(column_t(dtype));
    }

    auto task = [&](size_t begin, size_t end) {

        // Iterate over output columns to materialize
        for (size_t column_idx = begin; column_idx < end; ++column_idx) {

            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            auto& column = table.columns[in_col_idx];

            if(column.type == DataType::INT32 && col_is_dense(column)) {
                results[column_idx].use_input_col(&column,table.num_rows);
                continue;
            }

            // Iterate over pages in column
            for (size_t page_idx = 0; page_idx < column.pages.size(); ++page_idx) {
                auto* page = column.pages[page_idx]->data;

                switch (column.type) {
                case DataType::INT32: {
                    auto  num_rows   = *reinterpret_cast<uint16_t*>(page);
                    auto* data_begin = reinterpret_cast<int32_t*>(page + 4);

                    auto* bitmap =
                        reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);

                    uint16_t data_idx = 0;

                    // Materialize INT32 values
                    for (uint16_t i = 0; i < num_rows; ++i) {
                        if (get_bitmap(bitmap, i)) {
                            auto value = data_begin[data_idx++];

                            value_t encoded_value;
                            encoded_value.parse_int32(value);
                            results[column_idx].append_row(encoded_value);
                        } else {
                            results[column_idx].append_row(value_t{});

                        }
                    }
                    break;
                }

                case DataType::VARCHAR: {
                    auto num_rows = *reinterpret_cast<uint16_t*>(page);

                    if (num_rows == 0xffff) {
                        // Handle long string
                        auto        num_chars  = *reinterpret_cast<uint16_t*>(page + 2);
                        auto*       data_begin = reinterpret_cast<char*>(page + 4);
                        std::string value{data_begin, data_begin + num_chars};

                        StrRef ref(true,table_id,in_col_idx,page_idx,0);
                        value_t encoded_value;
                        encoded_value.parse_strref(ref);
                        results[column_idx].append_row(encoded_value);

                    } else if (num_rows == 0xfffe) {
                        continue;

                    } else {
                        // Handle normal VARCHAR
                        auto  num_non_null = *reinterpret_cast<uint16_t*>(page + 2);
                        auto* offset_begin = reinterpret_cast<uint16_t*>(page + 4);
                        auto* data_begin   = reinterpret_cast<char*>(page + 4 + num_non_null * 2);
                        auto* string_begin = data_begin;
                        auto* bitmap =
                            reinterpret_cast<uint8_t*>(page + PAGE_SIZE - (num_rows + 7) / 8);

                        uint16_t data_idx = 0;
                        // Create StrRef for each VARCHAR
                        for (uint16_t i = 0; i < num_rows; ++i) {
                            if (get_bitmap(bitmap, i)) {

                                StrRef ref(false,table_id,in_col_idx,page_idx,data_idx++);
                                value_t encoded_value;
                                encoded_value.parse_strref(ref);
                                results[column_idx].append_row(encoded_value);
                            } else {
                                results[column_idx].append_row(value_t{});
                            }
                        }
                    }
                    break;
                }
                }
            }
        }
    };
    filter_tp.run(task, output_attrs.size());

    return results;
}

std::string deref_str_ref(StrRef ref,const std::vector<ColumnarTable>& inputs) {
    const ColumnarTable& table = inputs[ref.get_table()];
    const Column& col = table.columns[ref.get_column()];
    size_t page_idx = ref.get_page();
    auto* raw = col.pages[page_idx]->data;
    auto num_rows = *reinterpret_cast<uint16_t*>(raw);

    std::string deref_str;

    if(ref.is_long()) { // long string
        // initial special page
        if(num_rows != 0xffff) throw std::runtime_error("Page doesn't refer to long string");     

        auto        num_chars  = *reinterpret_cast<uint16_t*>(raw + 2);
        auto*       data_begin = reinterpret_cast<char*>(raw + 4);
        deref_str.assign(data_begin, data_begin + num_chars);
        
        page_idx++;
        if(page_idx >= col.pages.size()) { // return if last page in pages vector of column
            return deref_str;
        }

        raw = col.pages[page_idx]->data;
        num_rows = *reinterpret_cast<uint16_t*>(raw);

        // keep iterating over following special pages
        while (num_rows == 0xfffe) {
            num_chars  = *reinterpret_cast<uint16_t*>(raw + 2);
            data_begin = reinterpret_cast<char*>(raw + 4);

            deref_str.insert(deref_str.end(), data_begin, data_begin + num_chars);
            
            page_idx++;
            if(page_idx >= col.pages.size()) {
                break;
            }

            raw = col.pages[page_idx]->data;
            num_rows = *reinterpret_cast<uint16_t*>(raw);
        
        }


    } else { // normal string
        auto offset_idx = ref.get_offset();
        auto  num_non_null = *reinterpret_cast<uint16_t*>(raw + 2);
        auto* offset_begin = reinterpret_cast<uint16_t*>(raw + 4);
        auto* data_begin   = reinterpret_cast<char*>(raw + 4 + num_non_null * 2);
                
        auto* string_begin = (offset_idx == 0) ? data_begin : (data_begin + offset_begin[offset_idx - 1]);
 
        auto offset = offset_begin[offset_idx];
        deref_str.assign(string_begin, data_begin + offset);
    }

    return deref_str;

}

// Converts value_t column-store table to ColumnarTable, fully materialized
ColumnarTable table_to_columnar(ExecuteResult& table, 
    std::vector<DataType>&                     data_types,
    const std::vector<ColumnarTable>&          inputs) {
    
    namespace views  = ranges::views;
    ColumnarTable ret;
    ret.num_rows = table.empty() ? 0 : table[0].rows_num;

    // Iterate over output columns
    for (auto [col_idx, data_type]: data_types | views::enumerate) {
    
        ret.columns.emplace_back(data_type);
        auto& column = ret.columns.back();
        column_t& in_column = table[col_idx];

        switch (data_type) {
        case DataType::INT32: {
            uint16_t             num_rows = 0;
            std::vector<int32_t> data;
            std::vector<uint8_t> bitmap;
            data.reserve(2048);
            bitmap.reserve(256);
            
            // Creates page in current columnm, then saves and clears intermediate data
            auto save_page = [&column, &num_rows, &data, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(data.size());
                memcpy(page + 4, data.data(), data.size() * 4);
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                bitmap.clear();
            };

            // Iterate over rows of current column
            for (size_t row_idx = 0; row_idx < in_column.rows_num; row_idx++) {
                auto value = in_column[row_idx];

                    if (value.is_int32()) {
                        // New data doesn't fit in page
                        if (4 + (data.size() + 1) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        set_bitmap(bitmap, num_rows);
                        data.emplace_back(value.get_int32());
                    } else if (value.is_null()) {
                        // New data doesn't fit in page
                        if (4 + (data.size()) * 4 + (num_rows / 8 + 1) > PAGE_SIZE) {
                            save_page();
                        }
                        unset_bitmap(bitmap, num_rows);
                    }
                    ++num_rows;
            }

            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        case DataType::VARCHAR: {
            uint16_t              num_rows = 0;
            std::vector<char>     data;
            std::vector<uint16_t> offsets;
            std::vector<uint8_t>  bitmap;

            data.reserve(8192);
            offsets.reserve(4096);
            bitmap.reserve(512);

            // Handles saving long string over many special pages as needed
            auto save_long_string = [&column](std::string_view data) {
                size_t offset     = 0;
                auto   first_page = true;
                while (offset < data.size()) {
                    auto* page = column.new_page()->data;
                    if (first_page) {
                        *reinterpret_cast<uint16_t*>(page) = 0xffff;
                        first_page                         = false;
                    } else {
                        *reinterpret_cast<uint16_t*>(page) = 0xfffe;
                    }
                    auto page_data_len = std::min(data.size() - offset, PAGE_SIZE - 4);
                    *reinterpret_cast<uint16_t*>(page + 2) = page_data_len;
                    memcpy(page + 4, data.data() + offset, page_data_len);
                    offset += page_data_len;
                }
            };
            // Handles saving normal strings in page
            auto save_page = [&column, &num_rows, &data, &offsets, &bitmap]() {
                auto* page                             = column.new_page()->data;
                *reinterpret_cast<uint16_t*>(page)     = num_rows;
                *reinterpret_cast<uint16_t*>(page + 2) = static_cast<uint16_t>(offsets.size());
                memcpy(page + 4, offsets.data(), offsets.size() * 2);
                memcpy(page + 4 + offsets.size() * 2, data.data(), data.size());
                memcpy(page + PAGE_SIZE - bitmap.size(), bitmap.data(), bitmap.size());
                num_rows = 0;
                data.clear();
                offsets.clear();
                bitmap.clear();
            };

            for (size_t row_idx = 0; row_idx < in_column.rows_num; row_idx++) {
                auto value = in_column[row_idx];

                if (value.is_strref()) { 
                    std::string str_value = deref_str_ref(value.get_strref(),inputs);
                    if (str_value.size() > PAGE_SIZE - 7) { // string doesn't fit in page
                        if (num_rows > 0) {
                            save_page();
                        }
                        save_long_string(str_value);
                    } else {
                        size_t page_size_after = 4 + (offsets.size() + 1) * 2 + (data.size() + str_value.size()) + (num_rows / 8 + 1);
                        if ( page_size_after > PAGE_SIZE) { // string doesn't fit in page
                            save_page();
                        }
                        set_bitmap(bitmap, num_rows);
                        data.insert(data.end(), str_value.begin(), str_value.end());
                        offsets.emplace_back(data.size());
                        ++num_rows;
                    }
                } else if (value.is_null()) {
                    size_t page_size_after = 4 + offsets.size() * 2 + data.size() + (num_rows / 8 + 1);
                    if ( page_size_after > PAGE_SIZE) { // string doesn't fit in page
                        save_page();
                    }
                    unset_bitmap(bitmap, num_rows);
                    ++num_rows;
                } else {
                    throw std::runtime_error("not string or null");
                }
            }
            if (num_rows != 0) {
                save_page();
            }
            break;
        }
        }
    }
    return ret;
}

ExecuteResult execute_scan(const Plan&               plan,
    const ScanNode&                                  scan,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {

    auto                           table_id = scan.base_table_id;
    auto&                          input    = plan.inputs[table_id];
    return copy_scan(input,table_id,output_attrs);
}

ExecuteResult execute_impl(const Plan& plan, size_t node_idx) {
    auto& node = plan.nodes[node_idx];
    return std::visit(
        [&](const auto& value) {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, JoinNode>) {
                return execute_hash_join(plan, value, node.output_attrs);
            } else {
                return execute_scan(plan, value, node.output_attrs);
            }
        },
        node.data);
}

ColumnarTable execute(const Plan& plan, [[maybe_unused]] void* context) {
    namespace views = ranges::views;
    auto ret        = execute_impl(plan, plan.root);
    
    auto ret_types  = plan.nodes[plan.root].output_attrs
                   | views::transform([](const auto& v) { return std::get<1>(v); })
                   | ranges::to<std::vector<DataType>>();
    
    return table_to_columnar(ret,ret_types,plan.inputs);

}

void* build_context() {
    return nullptr;
}
 
void destroy_context([[maybe_unused]] void* context) {}

}// namespace Contest