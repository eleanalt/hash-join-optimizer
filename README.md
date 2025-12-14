[![Review Assignment Due Date](https://classroom.github.com/assets/deadline-readme-button-22041afd0340ce965d47ae6ef1cefeee28c7c493a6346c4f15d667ab976d596c.svg)](https://classroom.github.com/a/gjaw_qSU)
# SIGMOD Contest 2025

## Task

Given the joining pipeline and the pre-filtered input data, your task is to implement an efficient joining algorithm to accelerate the execution time of the joining pipeline. Specifically, you need to implement the following function in `src/execute.cpp`:

```C++
ColumnarTable execute(const Plan& plan, void* context);
```

Optionally, you can implement these two functions as well to prepare any global context (e.g., thread pool) to accelerate the execution.

```C++
void* build_context();
void destroy_context(void*);
```

### Input format

The input plan in the above function is defined as the following struct.

```C++
struct ScanNode {
    size_t base_table_id;
};

struct JoinNode {
    bool   build_left;
    size_t left;
    size_t right;
    size_t left_attr;
    size_t right_attr;
};

struct PlanNode {
    std::variant<ScanNode, JoinNode>          data;
    std::vector<std::tuple<size_t, DataType>> output_attrs;
};

struct Plan {
    std::vector<PlanNode>      nodes;
    std::vector<ColumnarTable> inputs;
    size_t root;
}
```

**Scan**:
- The `base_table_id` member refers to which input table in the `inputs` member of a plan is used by the Scan node.
- Each item in the `output_attrs` indicates which column in the base table should be output and what type it is.

**Join**:
- The `build_left` member refers to which side the hash table should be built on, where `true` indicates building the hash table on the left child, and `false` indicates the opposite.
- The `left` and `right` members are the indexes of the left and right child of the Join node in the `nodes` member of a plan, respectively.
- The `left_attr` and `right_attr` members are the join condition of Join node. Supposing that there are two records, `left_record` and `right_record`, from the intermediate results of the left and right child, respectively. The members indicate that the two records should be joined when `left_record[left_attr] == right_record[right_attr]`.
- Each item in the `output_attrs` indicates which column in the result of children should be output and what type it is. Supposing that the left child has $n_l$ columns and the right child has $n_r$ columns, the value of the index $i \in \{0, \dots, n_l + n_r - 1\}$, where the ranges $\{0, \dots, n_l - 1\}$ and $\{n_l, \dots, n_l + n_r - 1\}$ indicate the output column is from left and right child respectively.

**Root**: The `root` member of a plan indicates which node is the root node of the execution plan tree.

### Data format

The input and output data both follow a simple columnar data format.

```C++
enum class DataType {
    INT32,       // 4-byte integer
    INT64,       // 8-byte integer
    FP64,        // 8-byte floating point
    VARCHAR,     // string of arbitary length
};

constexpr size_t PAGE_SIZE = 8192;

struct alignas(8) Page {
    std::byte data[PAGE_SIZE];
};

struct Column {
    DataType           type;
    std::vector<Page*> pages;
};

struct ColumnarTable {
    size_t              num_rows;
    std::vector<Column> columns;
};
```

A `ColumnarTable` first stores how many rows the table has in the `num_rows` member, then stores each column seperately as a `Column`. Each `Column` has a type and stores the items of the column into several pages. Each page is of 8192 bytes. In each page:

- The first 2 bytes are a `uint16_t` which is the number of rows $n_r$ in the page.
- The following 2 bytes are a `uint16_t` which is the number of non-`NULL` values $n_v$ in the page.
- The first $n_r$ bits in the last $\left\lfloor\frac{(n_r + 7)}{8}\right\rfloor$ bytes is a bitmap indicating whether the corresponding row has value or is `NULL`.

**Fixed-length attribute**: There are $n_v$ contiguous values begins at the first aligned position. For example, in a `Page` of `INT32`, the first value is at `data + 4`. While in a `Page` of `INT64` and `FP64`, the first value is at `data + 8`.

**Variable-length attribute**: There are $n_v$ contigous offsets (`uint16_t`) begins at `data + 4` in a `Page`, followed by the content of the varchars which begins at `char_begin = data + 4 + n_r * 2`. Each offset indicates the ending offset of the corresponding `VARCHAR` with respect to the `char_begin`.

**Long string**: When the length of a string is longer than `PAGE_SIZE - 7`, it can not fit in a normal page. Special pages will be used to store such string. If $n_r$ `== 0xffff` or $n_r$ `== 0xfffe`, the `Page` is a special page for long string. `0xffff` means the page is the first page of a long string and `0xfffe` means the page is the following page of a long string. The following 2 bytes is a `uint16_t` indicating the number of chars in the page, beginning at `data + 4`.

## Requirement

- You can only modify the file `src/execute.cpp` in the project.
- You must not use any third-party libraries. If you are using libraries for development (e.g., for logging), ensure to remove them before the final submission.
- The joining pipeline (including order and build side) is optimized by PostgreSQL for `Hash Join` only. However, in the `execute` function, you are free to use other algorithms and change the pipeline, as long as the result is equivalent.
- For any struct listed above, all of there members are public. You can manipulate them in free functions as desired as long as the original files are not changed and the manipulated objects can be destructed properly.
- Your program will be evaluated on an unpublished benchmark sampled from the original JOB benchmark. You will not be able to access the test benchmark.

## Quick start

> [!TIP]
> Run all the following commands in the root directory of this project.

First, download the imdb dataset.

```bash
./download_imdb.sh
```

Second, build the project.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc)
```

Third, prepare the DuckDB database for correctness checking.

```bash
./build/build_database imdb.db
```

Now, you can run the tests:
```bash
./build/run plans.json
```
> [!TIP]
> If you want to use `Ninja Multi-Config` as the generator. The commands will look like:
> 
>```bash
> cmake -S . -B build -Wno-dev -G "Ninja Multi-Config"
> cmake --build build --config Release -- -j $(nproc)
> ./build/Release/build_database imdb.db
> ./build/Release/run plans.json
> ```

# Cache
## This section is only for UNIX users
There are 2 new executables with this repository. They cache the join tables and
result of each query and mmap them for faster loading times and getting rid of duckdb.

To build the cache you need to run:
```bash
./build/build_cache plans.json
```

> [!TIP] 
> If you are using `Linux x86_64` you can download our prebuilt cache with:
> ```
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_x86.tar.gz
> ```
> If you are using `macOS arm64` you can download our prebuilt cache with:
> ```
> wget http://share.uoa.gr/protected/all-download/sigmod25/sigmod25_cache_arm.tar.gz
> ```
> For all other systems you will need to build the cache on your own.

After the cache is built you can run the queries using:
```bash
./build/fast plans.json
```

Also after you have built the cache you no longer need to build the `run` executable
every time (which depends on duckdb and can be slow to compile). Just compile 
the executable that uses the cache:
```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev
cmake --build build -- -j $(nproc) fast
```

Code is compiled with Clang 18.

# Phase 1 — Hashing Algorithms

## Contributors
Our implementation of custom hash tables for Phase 1 was developed collaboratively:

- **1115202100089 — Ελεάνα Λύτη**  
  Implemented **Cuckoo Hashing**, as well as the **contains()** and **operator[]** methods for the Hopscotch Hashing variant.

- **1115202100174 — Αλέξιος Σούλι**  
  Implemented **Robinhood Hashing** and the **emplace()** method for Hopscotch Hashing.

## Selecting a Hashing Algorithm
To compile with a specific hashing algorithm other than `std::unordered_map` run:

``` 
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -Wno-dev -DHASH_ALGO={algo} && 
cmake --build build --target fast
```

Replace `{algo}` with one of the following:

- `robinhood` — Robinhood Hashing  
- `cuckoo` — Cuckoo Hashing  
- `hopscotch` — Hopscotch Hashing
- `unchained` — Unchained Hashing  


## Unit Tests

Each hashing implementation includes its own test suite. You can build them with:

- `cmake --build build -- -j $(nproc) robinhood_tests`  
- `cmake --build build -- -j $(nproc) cuckoo_tests`  
- `cmake --build build -- -j $(nproc) hopscotch_tests`
- `cmake --build build -- -j $(nproc) unchained_tests`

## Algorithm Notes

### Robinhood Hashing
Our Robinhood Hashing implementation follows the standard probe-distance balancing strategy.
We use `std::hash` as the base hashing function and apply a **Murmur3 finalizer** to improve distribution quality. This significantly reduced maximum probe sequence lengths—by up to 95% in our tests (PSL directly impacts lookup and insertion performance).
Given Robinhood Hashing's ability to maintain efficient performance at high load factors (0.90+), we selected **0.9** as a practical and well-balanced target load factor.

### Cuckoo Hashing
Our Cuckoo Hashing implementation uses **two independent hash functions** to assign each key to one of two possible positions in separate tables. Both hash functions start with `std::hash` as a base and then apply **custom bit-mixing routines** (shift and XOR operations) to improve randomness and reduce collisions. This ensures that each key has two independent candidate positions, enabling **constant-time lookups** by checking at most two locations.

To maintain efficiency and prevent long displacement cycles, we selected a **maximum load factor of 0.5**. When this threshold is exceeded, the table is **rehashed and doubled in size**, guaranteeing that insertions remain fast and lookups maintain O(1) performance.

### Hopscotch Hashing
Our Hopscotch Hashing implementation uses **`std::hash` with a Murmur3-inspired finalizer** to distribute keys uniformly. Each bucket maintains a **bitmap representing a fixed-size neighborhood (`H = 32`)**, allowing keys to be moved within this neighborhood to resolve collisions efficiently. The neighborhood size makes the most of **cache locality** while remaining within the **32-bit bitmap limit**, which allows fast bitwise operations for checking and accessing occupancy. We selected a **practical load factor of 0.9**, which keeps most insertions local and ensures O(1) lookups while maintaining high cache efficiency.

### Unchained Hashing
Both CRC32 and Fibonacci hashing were experimentally implemented.
In the software-based implementation, CRC32 incurred substantially higher computation overhead, leading to worse overall hash join execution time.
Consequently, Fibonacci hashing was chosen as the final hashing strategy.

## Performance 

Below are the execution times measured during benchmarking:

| Hash Implementation | Time (ms) |
|--------------------|-----------|
| **UnorderedMap (Base)** | 340k ms   |
| **Robinhood**      | 333k ms   |
| **Hopscotch**      | 343k ms   |
| **Cuckoo**         | 330k ms   |
| **Cuckoo**         | 240k ms   |

Below are the execution times measured after the implementation of late materilization and joins in column store:

| Hash Implementation | Time (ms) |
|--------------------|-----------|
| **UnorderedMap (Base)** | 60k ms   |
| **Robinhood**      | 54k ms   |
| **Hopscotch**      | 50k ms   |
| **Cuckoo**         | 58k ms   |
| **Unchained**      | 37k ms   |

