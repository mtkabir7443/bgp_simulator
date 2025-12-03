/**
 * BGP Simulator - LUDICROUS EDITION
 * 1. MEMORY: malloc-based Arena (avoid mmap thrash).
 * 2. ALLOC: Massive pre-allocation (degree * 64) to kill System Time.
 * 3. IO: ftruncate pre-allocation for output file.
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

#ifdef BUILD_PYTHON_MODULE
#include <pybind11/pybind11.h>
namespace py = pybind11;
#endif

// -----------------------------------------------------------------------------
// Constants & Tuning
// -----------------------------------------------------------------------------
constexpr int MAX_PATH_LEN = 16;
// Increased to 128MB per thread to virtually ensure NO reallocs/remaps
constexpr size_t THREAD_OUT_CAP = 1024 * 1024 * 128; 
constexpr size_t HASH_TABLE_SIZE = 262144;
constexpr size_t HASH_MASK = HASH_TABLE_SIZE - 1;
constexpr int WORK_CHUNK_SIZE = 256; 
constexpr int MICRO_SORT_THRESHOLD = 32; 
// Larger batches = fewer lock acquisitions
constexpr int SEND_BATCH_SIZE = 2048; 

// -----------------------------------------------------------------------------
// Data Structures
// -----------------------------------------------------------------------------
using Relationship = uint8_t;
constexpr Relationship ORIGIN = 0;
constexpr Relationship CUSTOMER = 1;
constexpr Relationship PEER = 2;
constexpr Relationship PROVIDER = 3;

template<typename T>
struct FastVector {
    T* data_ = nullptr;
    size_t size_ = 0;
    size_t cap_ = 0;

    FastVector() = default;
    ~FastVector() { if(data_) free(data_); }

    FastVector(const FastVector&) = delete;
    FastVector& operator=(const FastVector&) = delete;

    FastVector(FastVector&& other) noexcept {
        data_ = other.data_; size_ = other.size_; cap_ = other.cap_;
        other.data_ = nullptr; other.size_ = 0; other.cap_ = 0;
    }
    FastVector& operator=(FastVector&& other) noexcept {
        if (this != &other) {
            if(data_) free(data_);
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
            data_ = (T*)realloc(data_, cap_ * sizeof(T));
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
        cap_ = cap_ ? cap_ * 2 : 16;
        data_ = (T*)realloc(data_, cap_ * sizeof(T));
    }
};

struct alignas(64) AlignedSpinLock {
    std::atomic_bool lock_ = {false};
    __attribute__((always_inline)) void lock() {
        if (!lock_.exchange(true, std::memory_order_acquire)) return;
        while (true) {
             while (lock_.load(std::memory_order_relaxed)) _mm_pause();
             if (!lock_.exchange(true, std::memory_order_acquire)) return;
        }
    }
    __attribute__((always_inline)) void unlock() {
        lock_.store(false, std::memory_order_release);
    }
};

struct Announcement {
    uint64_t score;
    uint16_t prefix_id;
    uint16_t path_len;
    uint32_t next_hop;
    uint32_t path[MAX_PATH_LEN]; 
    Relationship recv_relationship;
    bool rov_invalid;

    __attribute__((always_inline)) void update_score() {
        score = (static_cast<uint64_t>(recv_relationship) << 56) |
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
    FastVector<Announcement> received_queue;

    AS(uint32_t id) : asn(id) {}
    AS() = default; 
};

struct EdgeNode { int neighbor_idx; int next_edge_idx; };
struct ASNStr { char str[12]; uint8_t len; };

// -----------------------------------------------------------------------------
// Globals
// -----------------------------------------------------------------------------
std::vector<AS> as_graph;
std::unique_ptr<AlignedSpinLock[]> node_locks; 
std::vector<int> asn_map;
std::vector<ASNStr> asn_str_cache; 

struct PrefixView { const char* str; uint16_t len; };
std::vector<PrefixView> prefix_map;
struct HashEntry { uint64_t key; uint16_t id; };
std::vector<HashEntry> fast_hash_table;

std::vector<int> global_providers;
std::vector<int> global_customers;
std::vector<int> global_peers;

std::vector<EdgeNode> edge_pool;
std::vector<int> head_p, head_c, head_r;

thread_local std::vector<uint32_t> sort_idxs;
thread_local std::vector<uint32_t> count_buf;
thread_local FastVector<Announcement> send_buffer;

struct PrivateOutArena {
    char* buf;
    size_t pos;
    size_t cap;
    // SWITCHED TO MALLOC: Often faster than raw mmap for repeated allocs/access patterns
    PrivateOutArena() { 
        cap = THREAD_OUT_CAP; 
        buf = (char*)malloc(cap);
        pos = 0; 
    }
    ~PrivateOutArena() { free(buf); }
    
    inline void ensure(size_t n) {
        if (pos + n >= cap) {
            cap *= 2;
            buf = (char*)realloc(buf, cap);
        }
    }
    inline void write_char(char c) { buf[pos++] = c; }
    inline void write_str(const char* s, size_t l) { 
        if(l == 1) { buf[pos++] = *s; return; }
        memcpy(buf + pos, s, l); pos += l; 
    }
    inline void write_asn(uint32_t asn) {
        const ASNStr& s = asn_str_cache[asn]; memcpy(buf + pos, s.str, s.len); pos += s.len;
    }
};
std::vector<std::unique_ptr<PrivateOutArena>> thread_out_arenas;

pthread_barrier_t sync_barrier;
std::atomic<bool> pool_running{true};
std::atomic<size_t> work_counter{0};

enum StageType { STAGE_PROPAGATE, STAGE_WRITE };
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

// -----------------------------------------------------------------------------
// Helpers
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
    uint64_t hash = 0;
    size_t i = 0;
    for (; i + 8 <= len; i += 8) {
        hash = _mm_crc32_u64(hash, *(const uint64_t*)(str + i));
    }
    for (; i < len; ++i) {
        hash = _mm_crc32_u8(hash, str[i]);
    }
    return hash;
}

uint16_t get_prefix_id(const char* prefix, size_t len) {
    uint64_t h = hash_string(prefix, len);
    size_t idx = h & HASH_MASK;
    while (fast_hash_table[idx].id != 0xFFFF) {
        if (fast_hash_table[idx].key == h) return fast_hash_table[idx].id;
        idx = (idx + 1) & HASH_MASK;
    }
    uint16_t id = (uint16_t)prefix_map.size();
    prefix_map.push_back({prefix, (uint16_t)len});
    fast_hash_table[idx] = {h, id};
    return id;
}

inline int get_as_index(uint32_t asn) {
    if (asn >= asn_map.size()) asn_map.resize(asn + 10000, -1);
    if (asn_map[asn] == -1) {
        int idx = as_graph.size();
        as_graph.emplace_back(asn);
        asn_map[asn] = idx;
        return idx;
    }
    return asn_map[asn];
}

char* read_file_to_buffer(const std::string& filename, size_t& out_size) {
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd == -1) { out_size = 0; return nullptr; }
    struct stat sb;
    if (fstat(fd, &sb) == -1) { close(fd); out_size = 0; return nullptr; }
    out_size = sb.st_size;
    if (out_size == 0) { close(fd); return nullptr; }
    char* buf = (char*)mmap(NULL, sb.st_size, PROT_READ, MAP_PRIVATE | MAP_POPULATE, fd, 0);
    if (buf == MAP_FAILED) { close(fd); _exit(1); }
    madvise(buf, sb.st_size, MADV_SEQUENTIAL | MADV_HUGEPAGE);
    return buf;
}

void precompute_asn_strings() {
    uint32_t max_asn = asn_map.size();
    asn_str_cache.resize(max_asn);
    char temp[16];
    for (size_t i = 0; i < max_asn; ++i) {
        if (asn_map[i] == -1) continue; 
        int len = sprintf(temp, "%zu", i);
        std::memcpy(asn_str_cache[i].str, temp, len);
        asn_str_cache[i].len = len;
    }
}

// -----------------------------------------------------------------------------
// Logic
// -----------------------------------------------------------------------------
void counting_sort_indices(const FastVector<Announcement>& queue, size_t N) {
    if (count_buf.size() < 65536) count_buf.resize(65536);
    std::vector<uint32_t> local_out(N); 
    std::memset(count_buf.data(), 0, prefix_map.size() * sizeof(uint32_t));
    
    for (size_t i = 0; i < N; ++i) count_buf[queue[i].prefix_id]++;
    
    uint32_t total = 0;
    size_t limit = prefix_map.size(); 
    for (size_t i = 0; i < limit; ++i) {
        uint32_t old_count = count_buf[i];
        count_buf[i] = total;
        total += old_count;
    }
    for (size_t i = 0; i < N; ++i) local_out[count_buf[queue[i].prefix_id]++] = i;
    std::memcpy(sort_idxs.data(), local_out.data(), N * sizeof(uint32_t));
}

void process_queue(int idx) {
    AS& node = as_graph[idx];
    if (node.received_queue.empty()) return;
    
    node.next_rib.clear();
    if (node.next_rib.capacity() < node.rib.size() + 16) 
        node.next_rib.reserve(node.rib.size() + 16);

    const auto& queue = node.received_queue;
    size_t N = queue.size();

    if (sort_idxs.size() < N) sort_idxs.resize(N + 64);
    
    if (N < MICRO_SORT_THRESHOLD) {
        for(size_t i=0; i<N; ++i) sort_idxs[i] = i;
        for (size_t i = 1; i < N; ++i) {
            uint32_t key = sort_idxs[i];
            int j = i - 1;
            while (j >= 0 && queue[sort_idxs[j]].prefix_id > queue[key].prefix_id) {
                sort_idxs[j + 1] = sort_idxs[j];
                j = j - 1;
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
        uint16_t curr_pid = queue[actual_idx].prefix_id;
        const Announcement* best_new = nullptr;

        while(sorted_idx < N) {
            uint32_t curr_actual = sort_idxs[sorted_idx];
            if (queue[curr_actual].prefix_id != curr_pid) break;
            const auto& ann = queue[curr_actual];
            if (!node.rov_enabled || !ann.rov_invalid) {
                if (best_new == nullptr || ann.score < best_new->score) {
                    best_new = &ann;
                }
            }
            sorted_idx++;
        }

        while(rib_idx < rib_size && node.rib[rib_idx].prefix_id < curr_pid) {
            node.next_rib.push_back(node.rib[rib_idx]); 
            rib_idx++;
        }

        bool has_existing = (rib_idx < rib_size && node.rib[rib_idx].prefix_id == curr_pid);
        
        if (best_new) {
            if (has_existing && best_new->score >= node.rib[rib_idx].score) {
                 node.next_rib.push_back(node.rib[rib_idx]);
            } else {
                node.next_rib.emplace_back(*best_new);
                Announcement& stored = node.next_rib.back();
                if (stored.path_len < MAX_PATH_LEN) {
                    for(int z=stored.path_len; z>0; --z) stored.path[z] = stored.path[z-1];
                    stored.path[0] = node.asn; 
                    stored.path_len++; 
                    stored.update_score();
                } else {
                    node.next_rib.pop_back(); 
                    if (has_existing) node.next_rib.push_back(node.rib[rib_idx]);
                }
            }
            if(has_existing) rib_idx++;
        } else if (has_existing) { 
            node.next_rib.push_back(node.rib[rib_idx]); 
            rib_idx++; 
        }
    }
    while(rib_idx < rib_size) { 
        node.next_rib.push_back(node.rib[rib_idx]); 
        rib_idx++; 
    }
    node.rib.swap(node.next_rib);
    node.received_queue.clear();
}

void send_announcements(int sender_idx, const std::vector<int>& global_arr, uint32_t start, uint32_t count, Relationship rel_type) {
    AS& sender = as_graph[sender_idx];
    if (sender.rib.empty()) return;
    
    uint32_t sender_asn = sender.asn;
    uint32_t end = start + count;

    send_buffer.clear();
    if (send_buffer.capacity() < SEND_BATCH_SIZE) send_buffer.reserve(SEND_BATCH_SIZE);

    for (uint32_t k = start; k < end; ++k) {
        int recv_idx = global_arr[k];
        AS& receiver = as_graph[recv_idx];
        uint32_t recv_asn = receiver.asn;

        send_buffer.clear();
        __m256i v_recv = _mm256_set1_epi32(recv_asn);

        for (const auto& stored_ann : sender.rib) {
            bool loop = false;
            if (stored_ann.path_len > 0) {
                 __m256i v_path1 = _mm256_loadu_si256((const __m256i*)&stored_ann.path[0]);
                 __m256i v_cmp1 = _mm256_cmpeq_epi32(v_path1, v_recv);
                 if (_mm256_movemask_epi8(v_cmp1) != 0) loop = true;
                 else if (stored_ann.path_len > 8) {
                     __m256i v_path2 = _mm256_loadu_si256((const __m256i*)&stored_ann.path[8]);
                     __m256i v_cmp2 = _mm256_cmpeq_epi32(v_path2, v_recv);
                     if (_mm256_movemask_epi8(v_cmp2) != 0) loop = true;
                 }
            }
            
            if (!loop) {
                send_buffer.emplace_back(stored_ann);
                Announcement& dest = send_buffer.back();
                dest.next_hop = sender_asn;
                dest.recv_relationship = rel_type;
                dest.update_score();
            }
        }

        if (!send_buffer.empty()) {
            node_locks[recv_idx].lock();
            receiver.received_queue.insert_range(send_buffer.begin(), send_buffer.end());
            node_locks[recv_idx].unlock();
        }
    }
}

// -----------------------------------------------------------------------------
// Output
// -----------------------------------------------------------------------------
void write_chunk(PrivateOutArena* arena, size_t start, size_t end) {
    for (size_t i = start; i < end; ++i) {
        const auto& node = as_graph[i];
        if (node.rib.empty()) continue;
        for (const auto& ann : node.rib) {
            arena->write_asn(node.asn);
            arena->write_char(',');
            const auto& pfx_view = prefix_map[ann.prefix_id];
            arena->write_str(pfx_view.str, pfx_view.len);
            arena->write_str(",\"(", 3);
            if (ann.path_len > 0) {
                 arena->write_asn(ann.path[0]);
                 if (ann.path_len == 1) {
                     arena->write_char(','); 
                 } else {
                     for (int k = 1; k < ann.path_len; ++k) { 
                         arena->write_str(", ", 2);
                         arena->write_asn(ann.path[k]);
                     }
                 }
            }
            arena->write_str(")\"\n", 3);
        }
    }
}

// -----------------------------------------------------------------------------
// Thread Pool
// -----------------------------------------------------------------------------
void pool_worker(int t_id) {
    sort_idxs.reserve(65536);
    count_buf.reserve(65536);
    send_buffer.reserve(4096);
    thread_out_arenas[t_id] = std::make_unique<PrivateOutArena>();

    while (true) {
        pthread_barrier_wait(&sync_barrier);
        if (!pool_running) break;

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
                            prop_config.rel_type);
                    }
                }
            }
        } else if (current_stage_type == STAGE_WRITE) {
            size_t start = t_id * write_config.chunk_size;
            size_t end = std::min(start + write_config.chunk_size, write_config.total_nodes);
            if (start < end) write_chunk(thread_out_arenas[t_id].get(), start, end);
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
    node_locks = std::make_unique<AlignedSpinLock[]>(as_graph.size()); 

    std::vector<std::vector<int>> rank_nodes(max_rank + 1);
    for(size_t i=0; i<as_graph.size(); ++i) rank_nodes[as_graph[i].rank].push_back(i);
    
    std::mt19937 g(12345);
    for(auto& rank_vec : rank_nodes) {
        std::shuffle(rank_vec.begin(), rank_vec.end(), g);
    }

    std::vector<int> all_nodes(as_graph.size());
    for(size_t i=0; i<as_graph.size(); ++i) all_nodes[i] = i;
    std::shuffle(all_nodes.begin(), all_nodes.end(), g);

    int num_threads = std::thread::hardware_concurrency();
    if(num_threads > 2) num_threads = 2;
    if(num_threads == 0) num_threads = 2;
    
    thread_out_arenas.resize(num_threads);
    pthread_barrier_init(&sync_barrier, NULL, num_threads + 1);
    
    std::vector<std::thread> pool;
    for(int i=0; i<num_threads; ++i) pool.emplace_back(pool_worker, i);

    for (int r = 0; r <= max_rank; ++r) run_prop_stage(rank_nodes[r], &global_providers, CUSTOMER, true, true);
    run_prop_stage(all_nodes, nullptr, PEER, true, false); 
    run_prop_stage(all_nodes, &global_peers, PEER, false, true); 
    run_prop_stage(all_nodes, nullptr, PEER, true, false); 
    for (int r = max_rank; r >= 0; --r) run_prop_stage(rank_nodes[r], &global_customers, PROVIDER, true, true);

    precompute_asn_strings();
    current_stage_type = STAGE_WRITE;
    write_config.total_nodes = as_graph.size();
    write_config.chunk_size = (write_config.total_nodes + num_threads - 1) / num_threads;
    trigger_workers();

    pool_running = false;
    pthread_barrier_wait(&sync_barrier);
    for(auto& t : pool) t.join();
    pthread_barrier_destroy(&sync_barrier);

    int out_fd = open("ribs.csv", O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out_fd == -1) { exit(1); }
    
    // Estimate size and ftruncate to avoid metadata updates per write
    size_t total_out_size = 0;
    for(int i=0; i<num_threads; ++i) total_out_size += thread_out_arenas[i]->pos;
    if (ftruncate(out_fd, total_out_size + 19) == -1) {} // Optimization, ignore error

    if(write(out_fd, "asn,prefix,as_path\n", 19)) {};
    
    for(int i=0; i<num_threads; ++i) {
        if (thread_out_arenas[i]->pos > 0) {
             size_t left = thread_out_arenas[i]->pos;
             char* ptr = thread_out_arenas[i]->buf;
             while(left > 0) {
                 ssize_t ret = write(out_fd, ptr, left);
                 if (ret > 0) { left -= ret; ptr += ret; }
                 else if (ret < 0 && errno != EINTR) break;
             }
        }
    }
    close(out_fd);
}

// -----------------------------------------------------------------------------
// Loaders
// -----------------------------------------------------------------------------
void load_topology(const std::string& filename) {
    size_t sz;
    char* buf = read_file_to_buffer(filename, sz);
    if (!buf) { exit(1); }
    char* p = buf;
    as_graph.reserve(80000); asn_map.resize(500000, -1);
    edge_pool.reserve(1000000); head_p.resize(80000, -1); head_c.resize(80000, -1); head_r.resize(80000, -1);
    while (*p) {
        if (*p == '#') { skip_line(p); continue; }
        if (*p < '0' || *p > '9') { p++; continue; }
        uint32_t asn1 = fast_atoi(p); skip_until_num(p); uint32_t asn2 = fast_atoi(p);
        while (*p && *p != '|') p++; 
        if (*p == '|') p++; 
        int rel = 0; if (*p == '-') { rel = -1; p += 2; } else { rel = *p - '0'; p++; }
        skip_line(p);
        int idx1 = get_as_index(asn1); int idx2 = get_as_index(asn2);
        if (idx1 >= (int)head_p.size() || idx2 >= (int)head_p.size()) {
            size_t req = std::max(idx1, idx2) + 10000;
            head_p.resize(req, -1); head_c.resize(req, -1); head_r.resize(req, -1);
        }
        if (rel == -1) {
            edge_pool.push_back({idx2, head_c[idx1]}); head_c[idx1] = edge_pool.size() - 1;
            edge_pool.push_back({idx1, head_p[idx2]}); head_p[idx2] = edge_pool.size() - 1;
        } else if (rel == 0) {
            edge_pool.push_back({idx2, head_r[idx1]}); head_r[idx1] = edge_pool.size() - 1;
            edge_pool.push_back({idx1, head_r[idx2]}); head_r[idx2] = edge_pool.size() - 1;
        }
    }
    size_t total_p = 0, total_c = 0, total_r = 0;
    for (size_t i = 0; i < as_graph.size(); ++i) {
        int curr;
        curr = head_p[i]; while(curr != -1) { as_graph[i].prov_count++; curr = edge_pool[curr].next_edge_idx; }
        curr = head_c[i]; while(curr != -1) { as_graph[i].cust_count++; curr = edge_pool[curr].next_edge_idx; }
        curr = head_r[i]; while(curr != -1) { as_graph[i].peer_count++; curr = edge_pool[curr].next_edge_idx; }
        total_p += as_graph[i].prov_count; total_c += as_graph[i].cust_count; total_r += as_graph[i].peer_count;
        
        // --- LUDICROUS PRE-ALLOCATION ---
        size_t degree = as_graph[i].prov_count + as_graph[i].peer_count + as_graph[i].cust_count;
        if (degree > 0) {
             // 64x Multiplier = Virtually guaranteed no reallocs
             as_graph[i].received_queue.reserve(degree * 64); 
             as_graph[i].rib.reserve(4);
        }
    }
    madvise(as_graph.data(), as_graph.capacity() * sizeof(AS), MADV_HUGEPAGE);

    global_providers.resize(total_p); global_customers.resize(total_c); global_peers.resize(total_r);
    size_t p_ptr = 0, c_ptr = 0, r_ptr = 0;
    for (size_t i = 0; i < as_graph.size(); ++i) {
        AS& node = as_graph[i];
        node.prov_start = p_ptr; int curr = head_p[i];
        while(curr != -1) { global_providers[p_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
        node.cust_start = c_ptr; curr = head_c[i];
        while(curr != -1) { global_customers[c_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
        node.peer_start = r_ptr; curr = head_r[i];
        while(curr != -1) { global_peers[r_ptr++] = edge_pool[curr].neighbor_idx; curr = edge_pool[curr].next_edge_idx; }
    }
}

void load_announcements(const std::string& filename) {
    size_t sz;
    char* buf = read_file_to_buffer(filename, sz);
    if (!buf) return;
    if (fast_hash_table.empty()) fast_hash_table.resize(HASH_TABLE_SIZE, {0, 0xFFFF});
    char* p = buf;
    skip_line(p); 
    while (*p) {
        uint32_t asn = fast_atoi(p); skip_until_num(p);
        char* prefix_start = p; while (*p && *p != ',') { p++; } size_t prefix_len = p - prefix_start;
        bool rov = false;
        if (*p == ',') {
            p++; 
            if (*p == 'T' || *p == 't' || *p == '1') rov = true;
        }
        skip_line(p);
        if (asn >= asn_map.size() || asn_map[asn] == -1) continue;
        int idx = asn_map[asn];
        uint16_t pid = get_prefix_id(prefix_start, prefix_len);
        Announcement ann{}; 
        ann.prefix_id = pid; ann.path[0] = asn; ann.path_len = 1; ann.next_hop = asn;
        ann.recv_relationship = ORIGIN; ann.rov_invalid = rov;
        ann.update_score();
        as_graph[idx].rib.push_back(ann);
    }
}

void load_rov(const std::string& filename) {
    if (filename.empty()) return;
    size_t sz;
    char* buf = read_file_to_buffer(filename, sz);
    if (!buf) return; 
    char* p = buf;
    while (*p) {
        if (*p < '0' || *p > '9') { p++; continue; }
        uint32_t asn = fast_atoi(p); skip_line(p);
        if (asn < asn_map.size() && asn_map[asn] != -1) as_graph[asn_map[asn]].rov_enabled = true;
    }
}

void compute_ranks() {
    std::queue<int> q;
    std::vector<int> remaining_customers(as_graph.size(), 0);
    size_t processed_nodes = 0; 
    for (size_t i = 0; i < as_graph.size(); ++i) {
        remaining_customers[i] = as_graph[i].cust_count;
        if (remaining_customers[i] == 0) { as_graph[i].rank = 0; q.push(i); }
    }
    while(!q.empty()) {
        int u = q.front(); q.pop(); processed_nodes++; 
        uint32_t start = as_graph[u].prov_start; uint32_t end = start + as_graph[u].prov_count;
        for (uint32_t k = start; k < end; ++k) {
            int v = global_providers[k];
            if (as_graph[v].rank < as_graph[u].rank + 1) as_graph[v].rank = as_graph[u].rank + 1;
            if (--remaining_customers[v] == 0) q.push(v);
        }
    }
    if (processed_nodes < as_graph.size()) { std::cerr << "Cycle detected" << std::endl; exit(1); }
}

void reset_simulation() {
    as_graph.clear(); asn_map.clear(); prefix_map.clear();
    if (fast_hash_table.empty()) fast_hash_table.resize(HASH_TABLE_SIZE, {0, 0xFFFF});
    else std::fill(fast_hash_table.begin(), fast_hash_table.end(), HashEntry{0, 0xFFFF});
    global_providers.clear(); global_customers.clear(); global_peers.clear();
    edge_pool.clear(); head_p.clear(); head_c.clear(); head_r.clear();
    asn_str_cache.clear();
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
    load_topology(rel_file);
    compute_ranks();
    load_rov(rov_file);
    load_announcements(ann_file);
    run_simulation_and_write();
}
PYBIND11_MODULE(bgp_simulator, m) {
    m.doc() = "BGP Simulator (Singularity)";
    m.def("run", &run_bgp_simulation, py::arg("relationships"), py::arg("announcements"), py::arg("rov_asns")="");
}
#endif