/**
 * BGP Simulator - High Performance Production Edition
 * - Fixed multi-run state reset bug (pool_running flag lifecycle)
 * - Sharded concurrent prefix hash table with a contiguous string arena
 * - Sparse ASN hash map supporting arbitrary 32-bit ASNs
 * - Resilient cycle-breaking rank calculation for real-world BGP topologies
 * - Multi-hop AS-path tracking up to 16 hops
 * - Pre-sized allocation buffers to eliminate dynamic memory growth latency
 */

#include <iostream>
#include <vector>
#include <algorithm>
#include <queue>
#include <cstdint>
#include <cstring>
#include <cstdio>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <thread>
#include <atomic>
#include <memory>
#include <immintrin.h>
#include <pthread.h>
#include <random>
#include <unordered_map>
#include <mutex>

#ifdef BUILD_PYTHON_MODULE
#include <pybind11/pybind11.h>
namespace py = pybind11;
#endif

// -----------------------------------------------------------------------------
// Constants & Configuration
// -----------------------------------------------------------------------------
constexpr int MAX_PATH_LEN = 16;
constexpr size_t THREAD_OUT_CAP = 1024 * 1024 * 256;
constexpr size_t HASH_SHARDS = 256;
constexpr size_t HASH_TABLE_SHARD_SIZE = 32768;
constexpr size_t HASH_SHARD_MASK = HASH_TABLE_SHARD_SIZE - 1;
constexpr int WORK_CHUNK_SIZE = 256;
constexpr int MICRO_SORT_THRESHOLD = 32;
constexpr int SEND_BATCH_SIZE = 4096;

int g_num_threads = 4;

using Relationship = uint8_t;
constexpr Relationship ORIGIN = 0;
constexpr Relationship CUSTOMER = 1;
constexpr Relationship PEER = 2;
constexpr Relationship PROVIDER = 3;

static inline size_t round_up_64(size_t sz) {
    return (sz + 63) & ~size_t(63);
}

// -----------------------------------------------------------------------------
// Data Structures
// -----------------------------------------------------------------------------
template<typename T>
struct FastVector {
    T* data_ = nullptr;
    size_t size_ = 0;
    size_t cap_ = 0;

    FastVector() = default;
    ~FastVector() { if (data_) free(data_); }

    FastVector(const FastVector&) = delete;
    FastVector& operator=(const FastVector&) = delete;

    FastVector(FastVector&& other) noexcept {
        data_ = other.data_; size_ = other.size_; cap_ = other.cap_;
        other.data_ = nullptr; other.size_ = 0; other.cap_ = 0;
    }
    FastVector& operator=(FastVector&& other) noexcept {
        if (this != &other) {
            if (data_) free(data_);
            data_ = other.data_; size_ = other.size_; cap_ = other.cap_;
            other.data_ = nullptr; other.size_ = 0; other.cap_ = 0;
        }
        return *this;
    }

    __attribute__((always_inline)) void push_back(const T& val) {
        if (size_ == cap_) grow();
        data_[size_++] = val;
    }

    __attribute__((always_inline)) void emplace_back(const T& val) {
        if (size_ == cap_) grow();
        data_[size_++] = val;
    }

    __attribute__((always_inline)) T& back() { return data_[size_ - 1]; }
    __attribute__((always_inline)) void pop_back() { size_--; }
    __attribute__((always_inline)) bool empty() const { return size_ == 0; }
    __attribute__((always_inline)) void clear() { size_ = 0; }
    __attribute__((always_inline)) size_t size() const { return size_; }
    __attribute__((always_inline)) size_t capacity() const { return cap_; }
    __attribute__((always_inline)) T* begin() { return data_; }
    __attribute__((always_inline)) T* end() { return data_ + size_; }
    __attribute__((always_inline)) const T* begin() const { return data_; }
    __attribute__((always_inline)) const T* end() const { return data_ + size_; }
    __attribute__((always_inline)) T& operator[](size_t i) { return data_[i]; }
    __attribute__((always_inline)) const T& operator[](size_t i) const { return data_[i]; }

    void reserve(size_t n) {
        if (n > cap_) {
            cap_ = n;
            size_t alloc_bytes = round_up_64(cap_ * sizeof(T));
            T* new_data = static_cast<T*>(aligned_alloc(64, alloc_bytes));
            if (data_) {
                memcpy(new_data, data_, size_ * sizeof(T));
                free(data_);
            }
            data_ = new_data;
        }
    }

    void swap(FastVector<T>& other) {
        std::swap(data_, other.data_);
        std::swap(size_, other.size_);
        std::swap(cap_, other.cap_);
    }

    void insert_range(const T* start, const T* end_ptr) {
        size_t count = end_ptr - start;
        if (size_ + count > cap_) reserve(std::max<size_t>(cap_ * 2, size_ + count + 128));
        memcpy(data_ + size_, start, count * sizeof(T));
        size_ += count;
    }

private:
    __attribute__((noinline)) void grow() {
        reserve(cap_ ? cap_ * 2 : 16);
    }
};

struct alignas(64) Announcement {
    uint32_t prefix_id;
    uint32_t next_hop;
    Relationship recv_relationship;
    bool rov_invalid;
    uint8_t path_len;
    uint8_t _pad[5];
    uint32_t path[MAX_PATH_LEN];

    __attribute__((always_inline)) uint64_t get_score() const {
        return (static_cast<uint64_t>(recv_relationship) << 56) |
               (static_cast<uint64_t>(path_len) << 40) |
               next_hop;
    }
};

struct alignas(64) AS {
    uint32_t asn;
    int rank = -1;
    bool rov_enabled = false;
    uint32_t prov_start = 0, prov_count = 0;
    uint32_t cust_start = 0, cust_count = 0;
    uint32_t peer_start = 0, peer_count = 0;

    FastVector<Announcement> rib;
    FastVector<Announcement> next_rib;
    std::vector<FastVector<Announcement>> received_queues;

    AS(uint32_t id) : asn(id) { }
    AS() : asn(0) { }

    void init_queues(int num_threads) {
        received_queues.resize(num_threads);
    }
};

struct EdgeNode { int neighbor_idx; int next_edge_idx; };
struct ASNStr { char str[16]; uint8_t len; };

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
std::vector<AS> as_graph;
std::unordered_map<uint32_t, int> asn_to_idx;
std::vector<ASNStr> asn_str_cache;

struct PrefixEntry { uint32_t offset; uint16_t len; };
std::vector<char> prefix_string_arena;
std::vector<PrefixEntry> prefix_map;

struct HashEntry { uint64_t key; uint32_t id; };
struct alignas(64) HashShard {
    std::vector<HashEntry> table;
    std::mutex lock;
    HashShard() { table.resize(HASH_TABLE_SHARD_SIZE, {0, 0xFFFFFFFF}); }
};
std::vector<HashShard> hash_shards(HASH_SHARDS);
std::mutex global_prefix_lock;

std::vector<int> global_providers;
std::vector<int> global_customers;
std::vector<int> global_peers;

std::vector<EdgeNode> edge_pool;
std::vector<int> head_p, head_c, head_r;

thread_local std::vector<uint32_t> sort_idxs;
thread_local std::vector<uint32_t> count_buf;
thread_local FastVector<Announcement> send_buffer;
thread_local FastVector<Announcement> local_merged_queue;

struct PrivateOutArena {
    char* buf;
    size_t pos;
    size_t cap;
    PrivateOutArena() {
        cap = THREAD_OUT_CAP;
        buf = static_cast<char*>(aligned_alloc(64, round_up_64(cap)));
        pos = 0;
    }
    ~PrivateOutArena() { free(buf); }

    inline void ensure(size_t n) {
        if (pos + n >= cap) {
            cap *= 2;
            char* new_buf = static_cast<char*>(aligned_alloc(64, round_up_64(cap)));
            memcpy(new_buf, buf, pos);
            free(buf);
            buf = new_buf;
        }
    }
    inline void write_char(char c) { buf[pos++] = c; }
    inline void write_str(const char* s, size_t l) {
        if (l == 1) { buf[pos++] = *s; return; }
        memcpy(buf + pos, s, l); pos += l;
    }
    inline void write_asn(uint32_t node_idx) {
        const ASNStr& s = asn_str_cache[node_idx];
        memcpy(buf + pos, s.str, s.len);
        pos += s.len;
    }
    inline void write_raw_asn(uint32_t raw_asn) {
        char tmp[16];
        int len = sprintf(tmp, "%u", raw_asn);
        memcpy(buf + pos, tmp, len);
        pos += len;
    }
};

std::vector<std::unique_ptr<PrivateOutArena>> thread_out_arenas;
char* global_out_map = nullptr;

pthread_barrier_t sync_barrier;
std::atomic<bool> pool_running{true};
std::atomic<size_t> work_counter{0};

enum StageType { STAGE_PROPAGATE, STAGE_WRITE, STAGE_MMAP_COPY };
StageType current_stage_type;

struct PropagateConfig {
    const std::vector<int>* nodes;
    const std::vector<int>* global_arr;
    Relationship rel_type;
    bool do_process;
    bool do_send;
} prop_config;

struct WriteConfig {
    size_t total_nodes;
    size_t chunk_size;
} write_config;

struct CopyConfig {
    std::vector<size_t>* offsets;
} copy_config;

// -----------------------------------------------------------------------------
// Helpers & Fast Hash
// -----------------------------------------------------------------------------
inline uint32_t fast_atoi(char*& p) {
    uint32_t x = 0;
    while (*p >= '0' && *p <= '9') {
        x = (x * 10) + (*p - '0');
        p++;
    }
    return x;
}

inline void skip_until_num(char*& p) {
    while (*p && (*p < '0' || *p > '9')) p++;
}

inline void skip_line(char*& p) {
    while (*p && *p != '\n') p++;
    if (*p) p++;
}

inline uint64_t hash_string(const char* str, size_t len) {
    uint64_t hash = 0xCBF29CE484222325ULL;
    for (size_t i = 0; i < len; ++i) {
        hash ^= static_cast<uint8_t>(str[i]);
        hash *= 0x100000001B3ULL;
    }
    return hash;
}

uint32_t get_prefix_id_threadsafe(const char* prefix, size_t len) {
    uint64_t h = hash_string(prefix, len);
    size_t shard_idx = (h >> 32) % HASH_SHARDS;
    HashShard& shard = hash_shards[shard_idx];

    std::lock_guard<std::mutex> guard(shard.lock);
    size_t idx = h & HASH_SHARD_MASK;

    while (shard.table[idx].id != 0xFFFFFFFF) {
        if (shard.table[idx].key == h) {
            return shard.table[idx].id;
        }
        idx = (idx + 1) & HASH_SHARD_MASK;
    }

    std::lock_guard<std::mutex> global_guard(global_prefix_lock);
    uint32_t id = static_cast<uint32_t>(prefix_map.size());
    uint32_t offset = static_cast<uint32_t>(prefix_string_arena.size());
    prefix_string_arena.insert(prefix_string_arena.end(), prefix, prefix + len);
    prefix_map.push_back({offset, static_cast<uint16_t>(len)});
    shard.table[idx] = {h, id};
    return id;
}

inline int get_as_index(uint32_t asn) {
    auto it = asn_to_idx.find(asn);
    if (it != asn_to_idx.end()) return it->second;

    int idx = static_cast<int>(as_graph.size());
    as_graph.emplace_back(asn);
    as_graph.back().init_queues(g_num_threads);
    asn_to_idx[asn] = idx;
    return idx;
}

struct MappedFile {
    char* buf = nullptr;
    size_t size = 0;
};

MappedFile map_file_read(const std::string& filename) {
    MappedFile mf;
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) return mf;

    struct stat sb;
    if (fstat(fd, &sb) == -1 || sb.st_size == 0) {
        close(fd);
        return mf;
    }

    mf.size = sb.st_size;
    mf.buf = static_cast<char*>(mmap(nullptr, mf.size, PROT_READ, MAP_PRIVATE, fd, 0));
    close(fd);

    if (mf.buf == MAP_FAILED) {
        mf.buf = nullptr;
        mf.size = 0;
        return mf;
    }
    madvise(mf.buf, mf.size, MADV_SEQUENTIAL);
    return mf;
}

void unmap_file(MappedFile& mf) {
    if (mf.buf && mf.buf != MAP_FAILED && mf.size > 0) {
        munmap(mf.buf, mf.size);
        mf.buf = nullptr;
        mf.size = 0;
    }
}

void precompute_asn_strings() {
    asn_str_cache.resize(as_graph.size());
    char temp[32];
    for (size_t i = 0; i < as_graph.size(); ++i) {
        int len = sprintf(temp, "%u", as_graph[i].asn);
        std::memcpy(asn_str_cache[i].str, temp, len);
        asn_str_cache[i].len = static_cast<uint8_t>(len);
    }
}

// -----------------------------------------------------------------------------
// Graph & RIB Processing Logic
// -----------------------------------------------------------------------------
void counting_sort_indices(const FastVector<Announcement>& queue, size_t N) {
    size_t max_pfx = prefix_map.size();
    if (count_buf.size() <= max_pfx) count_buf.resize(max_pfx + 10000);
    if (sort_idxs.size() < N) sort_idxs.resize(N + 64);

    std::vector<uint32_t> local_out(N);
    std::memset(count_buf.data(), 0, (max_pfx + 1) * sizeof(uint32_t));

    for (size_t i = 0; i < N; ++i) count_buf[queue[i].prefix_id]++;

    uint32_t total = 0;
    for (size_t i = 0; i < max_pfx; ++i) {
        uint32_t old_count = count_buf[i];
        count_buf[i] = total;
        total += old_count;
    }
    for (size_t i = 0; i < N; ++i) local_out[count_buf[queue[i].prefix_id]++] = i;
    std::memcpy(sort_idxs.data(), local_out.data(), N * sizeof(uint32_t));
}

void process_queue(int idx) {
    AS& node = as_graph[idx];

    local_merged_queue.clear();
    for (int t = 0; t < g_num_threads; ++t) {
        if (!node.received_queues[t].empty()) {
            local_merged_queue.insert_range(node.received_queues[t].begin(), node.received_queues[t].end());
            node.received_queues[t].clear();
        }
    }

    if (local_merged_queue.empty()) return;

    node.next_rib.clear();
    node.next_rib.reserve(local_merged_queue.size() + node.rib.size() + 16);

    const auto& queue = local_merged_queue;
    size_t N = queue.size();

    if (sort_idxs.size() < N) sort_idxs.resize(N + 64);

    if (N < MICRO_SORT_THRESHOLD) {
        for (size_t i = 0; i < N; ++i) sort_idxs[i] = i;
        for (size_t i = 1; i < N; ++i) {
            uint32_t key = sort_idxs[i];
            int j = static_cast<int>(i) - 1;
            while (j >= 0 && queue[sort_idxs[j]].prefix_id > queue[key].prefix_id) {
                sort_idxs[j + 1] = sort_idxs[j];
                j--;
            }
            sort_idxs[j + 1] = key;
        }
    } else {
        counting_sort_indices(queue, N);
    }

    size_t rib_idx = 0;
    size_t sorted_idx = 0;
    size_t rib_size = node.rib.size();

    while (sorted_idx < N) {
        uint32_t actual_idx = sort_idxs[sorted_idx];
        uint32_t curr_pid = queue[actual_idx].prefix_id;
        const Announcement* best_new = nullptr;

        while (sorted_idx < N) {
            uint32_t curr_actual = sort_idxs[sorted_idx];
            if (queue[curr_actual].prefix_id != curr_pid) break;
            const auto& ann = queue[curr_actual];
            if (!node.rov_enabled || !ann.rov_invalid) {
                if (best_new == nullptr || ann.get_score() < best_new->get_score()) {
                    best_new = &ann;
                }
            }
            sorted_idx++;
        }

        while (rib_idx < rib_size && node.rib[rib_idx].prefix_id < curr_pid) {
            node.next_rib.push_back(node.rib[rib_idx]);
            rib_idx++;
        }

        bool has_existing = (rib_idx < rib_size && node.rib[rib_idx].prefix_id == curr_pid);

        if (best_new) {
            if (has_existing && best_new->get_score() >= node.rib[rib_idx].get_score()) {
                node.next_rib.push_back(node.rib[rib_idx]);
            } else {
                node.next_rib.emplace_back(*best_new);
                Announcement& stored = node.next_rib.back();
                if (stored.path_len < MAX_PATH_LEN) {
                    for (int z = stored.path_len; z > 0; --z) stored.path[z] = stored.path[z - 1];
                    stored.path[0] = node.asn;
                    stored.path_len++;
                } else {
                    node.next_rib.pop_back();
                    if (has_existing) node.next_rib.push_back(node.rib[rib_idx]);
                }
            }
            if (has_existing) rib_idx++;
        } else if (has_existing) {
            node.next_rib.push_back(node.rib[rib_idx]);
            rib_idx++;
        }
    }
    while (rib_idx < rib_size) {
        node.next_rib.push_back(node.rib[rib_idx]);
        rib_idx++;
    }
    node.rib.swap(node.next_rib);
}

void send_announcements(int sender_idx, const std::vector<int>& global_arr, uint32_t start, uint32_t count, Relationship rel_type, int t_id) {
    AS& sender = as_graph[sender_idx];
    if (sender.rib.empty()) return;

    uint32_t sender_asn = sender.asn;
    uint32_t end = start + count;

    if (send_buffer.capacity() < sender.rib.size()) send_buffer.reserve(sender.rib.size() + 1024);

    for (uint32_t k = start; k < end; ++k) {
        int recv_idx = global_arr[k];
        AS& receiver = as_graph[recv_idx];
        uint32_t recv_asn = receiver.asn;

        send_buffer.clear();

        for (const auto& stored_ann : sender.rib) {
            bool loop = false;
            for (int z = 0; z < stored_ann.path_len; ++z) {
                if (stored_ann.path[z] == recv_asn) {
                    loop = true;
                    break;
                }
            }

            if (!loop) {
                send_buffer.emplace_back(stored_ann);
                Announcement& dest = send_buffer.back();
                dest.next_hop = sender_asn;
                dest.recv_relationship = rel_type;
            }
        }

        if (!send_buffer.empty()) {
            receiver.received_queues[t_id].insert_range(send_buffer.begin(), send_buffer.end());
        }
    }
}

// -----------------------------------------------------------------------------
// Output Generation & Worker Threads
// -----------------------------------------------------------------------------
void write_chunk(PrivateOutArena* arena, size_t start, size_t end) {
    for (size_t i = start; i < end; ++i) {
        const auto& node = as_graph[i];
        if (node.rib.empty()) continue;
        for (const auto& ann : node.rib) {
            arena->ensure(MAX_PATH_LEN * 16 + 256);
            arena->write_asn(i);
            arena->write_char(',');
            const auto& pfx_view = prefix_map[ann.prefix_id];
            arena->write_str(&prefix_string_arena[pfx_view.offset], pfx_view.len);
            arena->write_str(",\"(", 3);
            if (ann.path_len > 0) {
                arena->write_raw_asn(ann.path[0]);
                if (ann.path_len == 1) {
                    arena->write_char(',');
                } else {
                    for (int k = 1; k < ann.path_len; ++k) {
                        arena->write_str(", ", 2);
                        arena->write_raw_asn(ann.path[k]);
                    }
                }
            }
            arena->write_str(")\"\n", 3);
        }
    }
}

void pool_worker(int t_id) {
    sort_idxs.reserve(1000000);
    count_buf.reserve(1000000);
    send_buffer.reserve(524288);
    local_merged_queue.reserve(524288);
    thread_out_arenas[t_id] = std::make_unique<PrivateOutArena>();

    while (true) {
        pthread_barrier_wait(&sync_barrier);
        if (!pool_running.load(std::memory_order_acquire)) break;

        if (current_stage_type == STAGE_PROPAGATE) {
            const std::vector<int>& nodes = *prop_config.nodes;
            size_t N = nodes.size();
            while (true) {
                size_t start = work_counter.fetch_add(WORK_CHUNK_SIZE, std::memory_order_relaxed);
                if (start >= N) break;
                size_t end = std::min(start + WORK_CHUNK_SIZE, N);

                if (prop_config.do_process) {
                    for (size_t i = start; i < end; ++i) process_queue(nodes[i]);
                }
                if (prop_config.do_send && prop_config.global_arr) {
                    for (size_t i = start; i < end; ++i) {
                        int idx = nodes[i];
                        send_announcements(idx, *prop_config.global_arr,
                            prop_config.rel_type == CUSTOMER ? as_graph[idx].prov_start :
                            prop_config.rel_type == PEER ? as_graph[idx].peer_start : as_graph[idx].cust_start,
                            prop_config.rel_type == CUSTOMER ? as_graph[idx].prov_count :
                            prop_config.rel_type == PEER ? as_graph[idx].peer_count : as_graph[idx].cust_count,
                            prop_config.rel_type, t_id);
                    }
                }
            }
        } else if (current_stage_type == STAGE_WRITE) {
            size_t start = t_id * write_config.chunk_size;
            size_t end = std::min(start + write_config.chunk_size, write_config.total_nodes);
            if (start < end) write_chunk(thread_out_arenas[t_id].get(), start, end);
        } else if (current_stage_type == STAGE_MMAP_COPY) {
            size_t offset = (*copy_config.offsets)[t_id];
            size_t amt = thread_out_arenas[t_id]->pos;
            if (amt > 0 && global_out_map) {
                memcpy(global_out_map + offset, thread_out_arenas[t_id]->buf, amt);
            }
        }
        pthread_barrier_wait(&sync_barrier);
    }
}

void trigger_workers() {
    work_counter.store(0, std::memory_order_relaxed);
    pthread_barrier_wait(&sync_barrier);
    pthread_barrier_wait(&sync_barrier);
}

void run_prop_stage(const std::vector<int>& nodes, const std::vector<int>* global_arr, Relationship rel_type, bool do_process, bool do_send) {
    if (nodes.empty()) return;
    current_stage_type = STAGE_PROPAGATE;
    prop_config.nodes = &nodes;
    prop_config.global_arr = global_arr;
    prop_config.rel_type = rel_type;

    if (do_process) {
        prop_config.do_process = true;
        prop_config.do_send = false;
        trigger_workers();
    }
    if (do_send) {
        prop_config.do_process = false;
        prop_config.do_send = true;
        trigger_workers();
    }
}

void run_simulation_and_write() {
    int max_rank = 0;
    for (const auto& node : as_graph) if (node.rank > max_rank) max_rank = node.rank;

    std::vector<std::vector<int>> rank_nodes(max_rank + 1);
    for (size_t i = 0; i < as_graph.size(); ++i) {
        int r = std::max(0, as_graph[i].rank);
        rank_nodes[r].push_back(static_cast<int>(i));
    }

    std::mt19937 g(12345);
    for (auto& rank_vec : rank_nodes) std::shuffle(rank_vec.begin(), rank_vec.end(), g);

    std::vector<int> all_nodes(as_graph.size());
    for (size_t i = 0; i < as_graph.size(); ++i) all_nodes[i] = static_cast<int>(i);
    std::shuffle(all_nodes.begin(), all_nodes.end(), g);

    thread_out_arenas.resize(g_num_threads);
    pthread_barrier_init(&sync_barrier, nullptr, g_num_threads + 1);

    // Reset pool_running flag for subsequent simulation runs in the same process
    pool_running.store(true, std::memory_order_release);

    std::vector<std::thread> pool;
    for (int i = 0; i < g_num_threads; ++i) pool.emplace_back(pool_worker, i);

    for (int r = 0; r <= max_rank; ++r) run_prop_stage(rank_nodes[r], &global_providers, CUSTOMER, true, true);
    run_prop_stage(all_nodes, nullptr, PEER, true, false);
    run_prop_stage(all_nodes, &global_peers, PEER, false, true);
    run_prop_stage(all_nodes, nullptr, PEER, true, false);
    for (int r = max_rank; r >= 0; --r) run_prop_stage(rank_nodes[r], &global_customers, PROVIDER, true, true);

    precompute_asn_strings();

    current_stage_type = STAGE_WRITE;
    write_config.total_nodes = as_graph.size();
    write_config.chunk_size = (write_config.total_nodes + g_num_threads - 1) / g_num_threads;
    trigger_workers();

    int out_fd = open("ribs.csv", O_RDWR | O_CREAT | O_TRUNC, 0644);
    if (out_fd != -1) {
        size_t total_out_size = 19;
        std::vector<size_t> mmap_offsets(g_num_threads);
        for (int i = 0; i < g_num_threads; ++i) {
            mmap_offsets[i] = total_out_size;
            total_out_size += thread_out_arenas[i]->pos;
        }
        if (ftruncate(out_fd, total_out_size) != -1) {
            global_out_map = static_cast<char*>(mmap(nullptr, total_out_size, PROT_READ | PROT_WRITE, MAP_SHARED, out_fd, 0));
            if (global_out_map != MAP_FAILED) {
                memcpy(global_out_map, "asn,prefix,as_path\n", 19);

                current_stage_type = STAGE_MMAP_COPY;
                copy_config.offsets = &mmap_offsets;
                trigger_workers();

                munmap(global_out_map, total_out_size);
            }
        }
        close(out_fd);
    }

    pool_running.store(false, std::memory_order_release);
    pthread_barrier_wait(&sync_barrier);
    for (auto& t : pool) t.join();
    pthread_barrier_destroy(&sync_barrier);
}

// -----------------------------------------------------------------------------
// Loaders
// -----------------------------------------------------------------------------
void load_topology(const std::string& filename) {
    MappedFile mf = map_file_read(filename);
    if (!mf.buf) return;

    char* p = mf.buf;
    as_graph.reserve(80000);
    edge_pool.reserve(1000000);
    head_p.resize(80000, -1);
    head_c.resize(80000, -1);
    head_r.resize(80000, -1);

    while (p < mf.buf + mf.size && *p) {
        if (*p == '#') { skip_line(p); continue; }
        if (*p < '0' || *p > '9') { p++; continue; }
        uint32_t asn1 = fast_atoi(p); skip_until_num(p); uint32_t asn2 = fast_atoi(p);
        while (*p && *p != '|' && *p != '\n') p++;
        if (*p == '|') p++;
        int rel = 0;
        if (*p == '-') { rel = -1; p += 2; }
        else { rel = *p - '0'; p++; }
        skip_line(p);

        int idx1 = get_as_index(asn1);
        int idx2 = get_as_index(asn2);
        if (idx1 >= static_cast<int>(head_p.size()) || idx2 >= static_cast<int>(head_p.size())) {
            size_t req = std::max(idx1, idx2) + 10000;
            head_p.resize(req, -1); head_c.resize(req, -1); head_r.resize(req, -1);
        }
        if (rel == -1) {
            edge_pool.push_back({idx2, head_c[idx1]}); head_c[idx1] = static_cast<int>(edge_pool.size() - 1);
            edge_pool.push_back({idx1, head_p[idx2]}); head_p[idx2] = static_cast<int>(edge_pool.size() - 1);
        } else if (rel == 0) {
            edge_pool.push_back({idx2, head_r[idx1]}); head_r[idx1] = static_cast<int>(edge_pool.size() - 1);
            edge_pool.push_back({idx1, head_r[idx2]}); head_r[idx2] = static_cast<int>(edge_pool.size() - 1);
        }
    }
    unmap_file(mf);

    size_t total_p = 0, total_c = 0, total_r = 0;
    for (size_t i = 0; i < as_graph.size(); ++i) {
        int curr;
        curr = head_p[i]; while (curr != -1) { as_graph[i].prov_count++; curr = edge_pool[curr].next_edge_idx; }
        curr = head_c[i]; while (curr != -1) { as_graph[i].cust_count++; curr = edge_pool[curr].next_edge_idx; }
        curr = head_r[i]; while (curr != -1) { as_graph[i].peer_count++; curr = edge_pool[curr].next_edge_idx; }
        total_p += as_graph[i].prov_count; total_c += as_graph[i].cust_count; total_r += as_graph[i].peer_count;

        size_t degree = as_graph[i].prov_count + as_graph[i].peer_count + as_graph[i].cust_count;
        if (degree > 0) {
            for (int t = 0; t < g_num_threads; ++t) {
                as_graph[i].received_queues[t].reserve(degree * 16);
            }
            as_graph[i].rib.reserve(4);
        }
    }

    global_providers.resize(total_p); global_customers.resize(total_c); global_peers.resize(total_r);
    size_t p_ptr = 0, c_ptr = 0, r_ptr = 0;
    for (size_t i = 0; i < as_graph.size(); ++i) {
        AS& node = as_graph[i];
        node.prov_start = p_ptr; int curr = head_p[i];
        while (curr != -1) { global_providers[p_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
        node.cust_start = c_ptr; curr = head_c[i];
        while (curr != -1) { global_customers[c_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
        node.peer_start = r_ptr; curr = head_r[i];
        while (curr != -1) { global_peers[r_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
    }
}

void parse_announcements_chunk(char* start, char* end, int t_id) {
    char* p = start;
    while (p < end && *p) {
        if (*p < '0' || *p > '9') { skip_line(p); continue; }
        uint32_t asn = fast_atoi(p); skip_until_num(p);
        char* prefix_start = p;
        while (*p && *p != ',' && *p != '\n') { p++; }
        size_t prefix_len = p - prefix_start;
        bool rov = false;
        if (*p == ',') {
            p++;
            if (*p == 'T' || *p == 't' || *p == '1') rov = true;
        }
        skip_line(p);

        auto it = asn_to_idx.find(asn);
        if (it == asn_to_idx.end()) continue;
        int idx = it->second;
        uint32_t pid = get_prefix_id_threadsafe(prefix_start, prefix_len);

        Announcement ann{};
        ann.prefix_id = pid;
        ann.path_len = 0;
        ann.next_hop = asn;
        ann.recv_relationship = ORIGIN;
        ann.rov_invalid = rov;

        as_graph[idx].received_queues[t_id].push_back(ann);
    }
}

void load_announcements(const std::string& filename) {
    MappedFile mf = map_file_read(filename);
    if (!mf.buf) return;

    if (prefix_map.empty()) {
        prefix_map.reserve(1000000);
        prefix_string_arena.reserve(16 * 1024 * 1024);
    }

    std::vector<std::thread> parser_threads;
    size_t chunk_size = mf.size / g_num_threads;

    for (int i = 0; i < g_num_threads; ++i) {
        char* chunk_start = mf.buf + (i * chunk_size);
        char* chunk_end = (i == g_num_threads - 1) ? (mf.buf + mf.size) : (chunk_start + chunk_size);

        if (i > 0) {
            while (chunk_start < mf.buf + mf.size && *chunk_start != '\n') chunk_start++;
            if (*chunk_start == '\n') chunk_start++;
        }
        if (i < g_num_threads - 1) {
            while (chunk_end < mf.buf + mf.size && *chunk_end != '\n') chunk_end++;
            if (*chunk_end == '\n') chunk_end++;
        }

        parser_threads.emplace_back(parse_announcements_chunk, chunk_start, chunk_end, i);
    }

    for (auto& t : parser_threads) t.join();
    unmap_file(mf);

    for (size_t i = 0; i < as_graph.size(); ++i) {
        process_queue(static_cast<int>(i));
    }
}

void load_rov(const std::string& filename) {
    if (filename.empty()) return;
    MappedFile mf = map_file_read(filename);
    if (!mf.buf) return;

    char* p = mf.buf;
    while (p < mf.buf + mf.size && *p) {
        if (*p < '0' || *p > '9') { p++; continue; }
        uint32_t asn = fast_atoi(p); skip_line(p);
        auto it = asn_to_idx.find(asn);
        if (it != asn_to_idx.end()) {
            as_graph[it->second].rov_enabled = true;
        }
    }
    unmap_file(mf);
}

void compute_ranks() {
    std::queue<int> q;
    std::vector<int> remaining_customers(as_graph.size(), 0);
    size_t processed_nodes = 0;

    for (size_t i = 0; i < as_graph.size(); ++i) {
        remaining_customers[i] = as_graph[i].cust_count;
        if (remaining_customers[i] == 0) {
            as_graph[i].rank = 0;
            q.push(static_cast<int>(i));
        }
    }

    while (!q.empty()) {
        int u = q.front(); q.pop();
        processed_nodes++;
        uint32_t start = as_graph[u].prov_start;
        uint32_t end = start + as_graph[u].prov_count;
        for (uint32_t k = start; k < end; ++k) {
            int v = global_providers[k];
            if (as_graph[v].rank < as_graph[u].rank + 1) {
                as_graph[v].rank = as_graph[u].rank + 1;
            }
            if (--remaining_customers[v] == 0) q.push(v);
        }
    }

    if (processed_nodes < as_graph.size()) {
        for (size_t i = 0; i < as_graph.size(); ++i) {
            if (remaining_customers[i] > 0) {
                if (as_graph[i].rank == -1) as_graph[i].rank = 0;
                q.push(static_cast<int>(i));
            }
        }
        while (!q.empty()) {
            int u = q.front(); q.pop();
            uint32_t start = as_graph[u].prov_start;
            uint32_t end = start + as_graph[u].prov_count;
            for (uint32_t k = start; k < end; ++k) {
                int v = global_providers[k];
                if (as_graph[v].rank < as_graph[u].rank + 1) {
                    as_graph[v].rank = as_graph[u].rank + 1;
                }
            }
        }
    }
}

void reset_simulation() {
    as_graph.clear();
    asn_to_idx.clear();
    prefix_map.clear();
    prefix_string_arena.clear();
    global_providers.clear();
    global_customers.clear();
    global_peers.clear();
    edge_pool.clear();
    head_p.clear();
    head_c.clear();
    head_r.clear();
    asn_str_cache.clear();
    for (auto& shard : hash_shards) {
        std::lock_guard<std::mutex> guard(shard.lock);
        std::fill(shard.table.begin(), shard.table.end(), HashEntry{0, 0xFFFFFFFF});
    }
}

struct Args { std::string rel_file, ann_file, rov_file; };
Args parse_args(int argc, char* argv[]) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--relationships" && i + 1 < argc) args.rel_file = argv[++i];
        else if (arg == "--announcements" && i + 1 < argc) args.ann_file = argv[++i];
        else if (arg == "--rov-asns" && i + 1 < argc) args.rov_file = argv[++i];
    }
    return args;
}

#ifndef BUILD_PYTHON_MODULE
int main(int argc, char* argv[]) {
    Args args = parse_args(argc, argv);
    if (args.rel_file.empty()) return 1;

    unsigned int hw = std::thread::hardware_concurrency();
    g_num_threads = hw > 0 ? static_cast<int>(hw) : 4;

    load_topology(args.rel_file);
    compute_ranks();
    load_rov(args.rov_file);
    load_announcements(args.ann_file);
    run_simulation_and_write();

    return 0;
}
#else
void run_bgp_simulation(std::string rel_file, std::string ann_file, std::string rov_file) {
    reset_simulation();

    unsigned int hw = std::thread::hardware_concurrency();
    g_num_threads = hw > 0 ? static_cast<int>(hw) : 4;

    load_topology(rel_file);
    compute_ranks();
    load_rov(rov_file);
    load_announcements(ann_file);
    run_simulation_and_write();
}
PYBIND11_MODULE(bgp_simulator, m) {
    m.doc() = "High Performance BGP Simulator";
    m.def("run", &run_bgp_simulation, py::arg("relationships"), py::arg("announcements"), py::arg("rov_asns") = "");
}
#endif
