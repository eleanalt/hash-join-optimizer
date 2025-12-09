#include <hardware.h>
#include <plan.h>
#include <table.h>
#include <inner_column.h>
#include "hash_config.h"
#include "base_std_hash.h"
#include "value_t.h"

namespace Contest {

using ExecuteResult = std::vector<std::vector<value_t>>;

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
        namespace views = ranges::views;


        //Using generic hash table aliased in hash_config.h
        GenericHash<uint32_t, std::vector<size_t>> hash_table;

        // Determine build and probe sides
        ExecuteResult& build_side = build_left ? left : right;
        ExecuteResult& probe_side = build_left ? right : left;

        size_t build_col = build_left ? left_col : right_col;
        size_t probe_col = build_left ? right_col : left_col;

            // Insert build side join keys in hash table
            for (auto&& [idx, record]: build_side | views::enumerate) {

                if(record[build_col].is_null()) continue;
                if(!record[build_col].is_int32()) throw std::runtime_error("Join key isn't of type int32_t");

                uint32_t key = record[build_col].get_int32();
                          
                if (!hash_table.contains(key)) {
                    hash_table.emplace(key, std::vector<size_t>(1, idx));
                } else {
                    hash_table[key].push_back(idx);
                }

            }

            // Scan probe side for keys in hash table
            for (auto& probe_record: probe_side) {

                if(probe_record[probe_col].is_null()) continue;
                if(!probe_record[probe_col].is_int32()) throw std::runtime_error("Join key isn't of type int32_t");

                uint32_t key = probe_record[probe_col].get_int32();

                    if (hash_table.contains(key)) {
                        auto& indices = hash_table[key];
                        
                        // Create output records for each build side row
                        for (auto build_idx: indices) { 
                            auto&             build_record = build_side[build_idx];
                            std::vector<value_t> new_record;
                            new_record.reserve(output_attrs.size());

                            // Iterate over output columns 
                            for (auto [col_idx, _]: output_attrs) {
                                value_t val;
                                
                                // Get value for current output column (left row + right row)
                                if (build_left) {
                                    if (col_idx < left[0].size()) {
                                        val = build_record[col_idx];
                                    } else {
                                        val = probe_record[col_idx - left[0].size()];
                                    }
                                } else {
                                    if (col_idx < left[0].size()) {
                                        val = probe_record[col_idx];
                                    } else {
                                        val = build_record[col_idx - left[0].size()];
                                    }
                                }
                                
                                new_record.emplace_back(val);
                            }
                            results.emplace_back(std::move(new_record));
                        }
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
    std::vector<std::vector<value_t>> results;

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

// Returns fully materialized value_t row table
ExecuteResult copy_scan(const ColumnarTable& table, size_t table_id,
    const std::vector<std::tuple<size_t, DataType>>& output_attrs) {
    
    namespace views = ranges::views;

    ExecuteResult results(table.num_rows, std::vector<value_t>(output_attrs.size(),value_t{}) );
    std::vector<DataType>          types(table.columns.size());

    auto task = [&](size_t begin, size_t end) {
        size_t col_pap = 0;

        // Iterate over output columns to materialize
        for (size_t column_idx = begin; column_idx < end; ++column_idx) {

            size_t in_col_idx = std::get<0>(output_attrs[column_idx]);
            auto& column = table.columns[in_col_idx];
            types[in_col_idx] = column.type;
            size_t row_idx = 0;

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
                            if (row_idx >= table.num_rows) {
                                throw std::runtime_error("row_idx");
                            }
                            results[row_idx++][column_idx].parse_int32(value);
                        } else {
                            ++row_idx;
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

                        if (row_idx >= table.num_rows) {
                            throw std::runtime_error("row_idx");
                        }
                        
                        StrRef ref(true,table_id,in_col_idx,page_idx,0);
                        results[row_idx++][column_idx].parse_strref(ref);

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

                                if (row_idx >= table.num_rows) {
                                    throw std::runtime_error("row_idx");
                                }

                                StrRef ref(false,table_id,in_col_idx,page_idx,data_idx++);
                                results[row_idx++][column_idx].parse_strref(ref);
                            } else {
                                ++row_idx;
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
        if(num_rows != 0xffff) throw std::runtime_error("Page doesn't refer to long string");     

        auto        num_chars  = *reinterpret_cast<uint16_t*>(raw + 2);
        auto*       data_begin = reinterpret_cast<char*>(raw + 4);
        deref_str.assign(data_begin, data_begin + num_chars);
        
        page_idx++;
        if(page_idx >= col.pages.size()) {
            return deref_str;
        }

        raw = col.pages[page_idx]->data;
        num_rows = *reinterpret_cast<uint16_t*>(raw);

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

std::vector<std::vector<Data>> value_to_variant(const ExecuteResult& rows,const std::vector<ColumnarTable>& inputs) {

    std::vector<std::vector<Data>> result;
    result.reserve(rows.size());

    for(auto& row : rows) {
        std::vector<Data> variant_row;
        variant_row.reserve(row.size());
        
        for (auto& item : row) {
            if (item.is_int32()) {
                variant_row.emplace_back(item.get_int32());
            } else if (item.is_strref()) {
                variant_row.emplace_back(deref_str_ref(item.get_strref(),inputs) );
            } else {
                variant_row.emplace_back(std::monostate{});
            }
        }
        result.emplace_back(std::move(variant_row));
    }

    return result;
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
    
    auto variant_ret = value_to_variant(ret,plan.inputs);
    Table table{std::move(variant_ret), std::move(ret_types)};
    return table.to_columnar();

    

}

void* build_context() {
    return nullptr;
}

void destroy_context([[maybe_unused]] void* context) {}

} // namespace Contest