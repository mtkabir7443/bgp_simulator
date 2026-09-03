/**
 * BGP Simulator GPU Engine
 *
 * Host-side parsing and RIB serialization matching the CPU engine in main.cpp:
 *   - Relationship-typed CSR adjacency (providers / customers / peers)
 *   - Rank-ordered Gao-Rexford stage dispatch
 *   - Device-side ROV invalid dropping
 *   - Race-free best-path selection via atomicMin on a packed score
 *   - Writes ribs.csv in the same asn,prefix,as_path format as the CPU engine
 *
 * NOTE: the RIB is dense (num_nodes * num_prefixes). This is fine for parity
 * testing and small/medium topologies but will exhaust VRAM on full CAIDA-scale
 * input. A sparse per-node RIB is the next step.
 */

#include <cuda_runtime.h>

#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <queue>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#define CUDA_CHECK(call)                                                        \
    do {                                                                        \
        cudaError_t err = (call);                                               \
        if (err != cudaSuccess) {                                               \
            fprintf(stderr, "CUDA Error at %s:%d - %s\n", __FILE__, __LINE__,   \
                    cudaGetErrorString(err));                                   \
            exit(EXIT_FAILURE);                                                 \
        }                                                                       \
    } while (0)

// Matches MAX_PATH_LEN in main.cpp
constexpr int MAX_PATH_LEN = 16;
constexpr int THREADS_PER_BLOCK = 256;

using Relationship = uint8_t;
constexpr Relationship REL_ORIGIN   = 0;
constexpr Relationship REL_CUSTOMER = 1;
constexpr Relationship REL_PEER     = 2;
constexpr Relationship REL_PROVIDER = 3;

constexpr uint64_t SCORE_NONE = 0xFFFFFFFFFFFFFFFFULL;

// Lower score wins. Relationship dominates (customer < peer < provider),
// then path length, then sender index as a deterministic tiebreak.
__host__ __device__ inline uint64_t pack_score(Relationship rel, uint32_t path_len, uint32_t sender) {
    return (static_cast<uint64_t>(rel) << 56) |
           (static_cast<uint64_t>(path_len) << 40) |
           static_cast<uint64_t>(sender);
}

// ---------------------------------------------------------------------------
// Host-side topology
// ---------------------------------------------------------------------------

struct Csr {
    std::vector<int> start;
    std::vector<int> count;
    std::vector<int> adj;
};

struct Topology {
    std::unordered_map<uint32_t, int> asn_to_idx;
    std::vector<uint32_t> idx_to_asn;

    std::vector<std::vector<int>> providers;
    std::vector<std::vector<int>> customers;
    std::vector<std::vector<int>> peers;

    std::vector<uint8_t> rov_enabled;

    int index_of(uint32_t asn) {
        auto it = asn_to_idx.find(asn);
        if (it != asn_to_idx.end()) return it->second;
        int idx = static_cast<int>(idx_to_asn.size());
        asn_to_idx.emplace(asn, idx);
        idx_to_asn.push_back(asn);
        providers.emplace_back();
        customers.emplace_back();
        peers.emplace_back();
        rov_enabled.push_back(0);
        return idx;
    }

    int size() const { return static_cast<int>(idx_to_asn.size()); }
};

static Csr flatten(const std::vector<std::vector<int>>& lists) {
    Csr csr;
    csr.start.resize(lists.size());
    csr.count.resize(lists.size());
    size_t total = 0;
    for (const auto& l : lists) total += l.size();
    csr.adj.reserve(total);
    for (size_t i = 0; i < lists.size(); ++i) {
        csr.start[i] = static_cast<int>(csr.adj.size());
        csr.count[i] = static_cast<int>(lists[i].size());
        csr.adj.insert(csr.adj.end(), lists[i].begin(), lists[i].end());
    }
    if (csr.adj.empty()) csr.adj.push_back(0);  // never cudaMalloc zero bytes
    return csr;
}

// rel.txt: "asn1|asn2|rel" where 0 = peer-to-peer, -1 = provider-to-customer.
// Matches load_topology() in main.cpp.
static bool load_topology(const std::string& path, Topology& topo) {
    std::ifstream in(path);
    if (!in) {
        fprintf(stderr, "Could not open relationships file: %s\n", path.c_str());
        return false;
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || line[0] == '#') continue;

        uint32_t asn1 = 0, asn2 = 0;
        int rel = 0;
        char sep1 = 0, sep2 = 0;
        std::istringstream ss(line);
        if (!(ss >> asn1)) continue;
        ss >> sep1 >> asn2 >> sep2 >> rel;

        int i1 = topo.index_of(asn1);
        int i2 = topo.index_of(asn2);

        if (rel == -1) {
            topo.customers[i1].push_back(i2);
            topo.providers[i2].push_back(i1);
        } else if (rel == 0) {
            topo.peers[i1].push_back(i2);
            topo.peers[i2].push_back(i1);
        }
    }
    return true;
}

struct HostAnnouncement {
    int origin_idx;
    uint32_t prefix_id;
    uint8_t rov_invalid;
};

// ann.txt: "asn,prefix[,rov_flag]". Matches parse_announcements_chunk().
static bool load_announcements(const std::string& path,
                               Topology& topo,
                               std::vector<HostAnnouncement>& out,
                               std::unordered_map<std::string, uint32_t>& prefix_ids,
                               std::vector<std::string>& prefix_strings) {
    std::ifstream in(path);
    if (!in) {
        fprintf(stderr, "Could not open announcements file: %s\n", path.c_str());
        return false;
    }

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || !std::isdigit(static_cast<unsigned char>(line[0]))) continue;

        size_t c1 = line.find(',');
        if (c1 == std::string::npos) continue;
        size_t c2 = line.find(',', c1 + 1);

        uint32_t asn = static_cast<uint32_t>(std::stoul(line.substr(0, c1)));
        std::string prefix = line.substr(c1 + 1, (c2 == std::string::npos ? line.size() : c2) - c1 - 1);

        uint8_t rov = 0;
        if (c2 != std::string::npos && c2 + 1 < line.size()) {
            char f = line[c2 + 1];
            if (f == 'T' || f == 't' || f == '1') rov = 1;
        }

        auto it = topo.asn_to_idx.find(asn);
        if (it == topo.asn_to_idx.end()) continue;

        auto pit = prefix_ids.find(prefix);
        uint32_t pid;
        if (pit == prefix_ids.end()) {
            pid = static_cast<uint32_t>(prefix_strings.size());
            prefix_ids.emplace(prefix, pid);
            prefix_strings.push_back(prefix);
        } else {
            pid = pit->second;
        }

        out.push_back({it->second, pid, rov});
    }
    return true;
}

static void load_rov(const std::string& path, Topology& topo) {
    if (path.empty()) return;
    std::ifstream in(path);
    if (!in) return;

    std::string line;
    while (std::getline(in, line)) {
        if (line.empty() || !std::isdigit(static_cast<unsigned char>(line[0]))) continue;
        uint32_t asn = static_cast<uint32_t>(std::stoul(line));
        auto it = topo.asn_to_idx.find(asn);
        if (it != topo.asn_to_idx.end()) topo.rov_enabled[it->second] = 1;
    }
}

// Kahn's algorithm over customer counts. Matches compute_ranks() in main.cpp.
static std::vector<int> compute_ranks(const Topology& topo) {
    int n = topo.size();
    std::vector<int> rank(n, 0);
    std::vector<int> remaining(n, 0);
    std::queue<int> q;

    for (int i = 0; i < n; ++i) {
        remaining[i] = static_cast<int>(topo.customers[i].size());
        if (remaining[i] == 0) q.push(i);
    }

    while (!q.empty()) {
        int u = q.front();
        q.pop();
        for (int p : topo.providers[u]) {
            rank[p] = std::max(rank[p], rank[u] + 1);
            if (--remaining[p] == 0) q.push(p);
        }
    }
    return rank;
}

// ---------------------------------------------------------------------------
// Device state
// ---------------------------------------------------------------------------

struct DeviceRib {
    uint64_t* score;
    uint32_t* path;      // MAX_PATH_LEN entries per RIB slot
    uint8_t*  path_len;
    uint8_t*  rov_invalid;
    uint32_t* next_hop;
};

// Phase 1: every candidate races atomicMin on the destination score.
__global__ void propagate_score_kernel(
    const int* __restrict__ stage_nodes, int num_stage_nodes,
    int num_prefixes,
    const int* __restrict__ adj_start,
    const int* __restrict__ adj_count,
    const int* __restrict__ adj,
    const uint8_t* __restrict__ rov_enabled,
    Relationship rel_type,
    DeviceRib rib)
{
    int tid = blockIdx.x * blockDim.x + threadIdx.x;
    int total = num_stage_nodes * num_prefixes;
    if (tid >= total) return;

    int u = stage_nodes[tid / num_prefixes];
    int p = tid % num_prefixes;

    size_t src = static_cast<size_t>(u) * num_prefixes + p;
    uint8_t len = rib.path_len[src];
    if (len == 0 || len >= MAX_PATH_LEN) return;

    uint8_t invalid = rib.rov_invalid[src];
    uint64_t cand = pack_score(rel_type, len + 1, static_cast<uint32_t>(u));

    int start = adj_start[u];
    int count = adj_count[u];

    for (int e = 0; e < count; ++e) {
        int v = adj[start + e];
        if (rov_enabled[v] && invalid) continue;

        bool loop = false;
        for (int h = 0; h < len; ++h) {
            if (rib.path[src * MAX_PATH_LEN + h] == static_cast<uint32_t>(v)) { loop = true; break; }
        }
        if (loop) continue;

        size_t dst = static_cast<size_t>(v) * num_prefixes + p;
        atomicMin(reinterpret_cast<unsigned long long*>(&rib.score[dst]),
                  static_cast<unsigned long long>(cand));
    }
}

// Phase 2: the single winning candidate materializes its path. Because the
// sender index is packed into the low bits, exactly one candidate matches.
__global__ void propagate_commit_kernel(
    const int* __restrict__ stage_nodes, int num_stage_nodes,
    int num_prefixes,
    const int* __restrict__ adj_start,
    const int* __restrict__ adj_count,
    const int* __restrict__ adj,
    const uint8_t* __restrict__ rov_enabled,
    Relationship rel_type,
    DeviceRib rib)
{
    int tid = blockIdx.x * blockDim.x + threadIdx.x;
    int total = num_stage_nodes * num_prefixes;
    if (tid >= total) return;

    int u = stage_nodes[tid / num_prefixes];
    int p = tid % num_prefixes;

    size_t src = static_cast<size_t>(u) * num_prefixes + p;
    uint8_t len = rib.path_len[src];
    if (len == 0 || len >= MAX_PATH_LEN) return;

    uint8_t invalid = rib.rov_invalid[src];
    uint64_t cand = pack_score(rel_type, len + 1, static_cast<uint32_t>(u));

    int start = adj_start[u];
    int count = adj_count[u];

    for (int e = 0; e < count; ++e) {
        int v = adj[start + e];
        if (rov_enabled[v] && invalid) continue;

        bool loop = false;
        for (int h = 0; h < len; ++h) {
            if (rib.path[src * MAX_PATH_LEN + h] == static_cast<uint32_t>(v)) { loop = true; break; }
        }
        if (loop) continue;

        size_t dst = static_cast<size_t>(v) * num_prefixes + p;
        if (rib.score[dst] != cand) continue;

        rib.path[dst * MAX_PATH_LEN] = static_cast<uint32_t>(v);
        for (int h = 0; h < len; ++h) {
            rib.path[dst * MAX_PATH_LEN + h + 1] = rib.path[src * MAX_PATH_LEN + h];
        }
        rib.path_len[dst] = len + 1;
        rib.rov_invalid[dst] = invalid;
        rib.next_hop[dst] = static_cast<uint32_t>(u);
    }
}

// ---------------------------------------------------------------------------

struct Args {
    std::string rel_file, ann_file, rov_file;
};

static Args parse_args(int argc, char* argv[]) {
    Args args;
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--relationships" && i + 1 < argc) args.rel_file = argv[++i];
        else if (arg == "--announcements" && i + 1 < argc) args.ann_file = argv[++i];
        else if (arg == "--rov-asns" && i + 1 < argc) args.rov_file = argv[++i];
    }
    return args;
}

int main(int argc, char* argv[]) {
    Args args = parse_args(argc, argv);
    if (args.rel_file.empty() || args.ann_file.empty()) {
        fprintf(stderr,
                "Usage: %s --relationships <file> --announcements <file> [--rov-asns <file>]\n",
                argv[0]);
        return 1;
    }

    std::cout << "[CUDA Engine] Loading topology...\n";

    Topology topo;
    if (!load_topology(args.rel_file, topo)) return 1;

    std::vector<HostAnnouncement> anns;
    std::unordered_map<std::string, uint32_t> prefix_ids;
    std::vector<std::string> prefix_strings;
    if (!load_announcements(args.ann_file, topo, anns, prefix_ids, prefix_strings)) return 1;

    load_rov(args.rov_file, topo);

    int N = topo.size();
    int P = static_cast<int>(prefix_strings.size());
    if (N == 0 || P == 0) {
        std::cout << "[CUDA Engine] Nothing to simulate (0 ASes or 0 prefixes).\n";
        std::ofstream out("ribs.csv");
        out << "asn,prefix,as_path\n";
        return 0;
    }

    size_t entries = static_cast<size_t>(N) * P;
    size_t bytes = entries * (sizeof(uint64_t) + sizeof(uint32_t) * MAX_PATH_LEN +
                              2 * sizeof(uint8_t) + sizeof(uint32_t));

    size_t free_mem = 0, total_mem = 0;
    CUDA_CHECK(cudaMemGetInfo(&free_mem, &total_mem));
    if (bytes > free_mem) {
        fprintf(stderr,
                "Dense RIB needs %.2f GB for %d ASes x %d prefixes but only %.2f GB is free.\n"
                "The GPU engine currently uses a dense RIB; use the CPU engine for this input.\n",
                bytes / 1e9, N, P, free_mem / 1e9);
        return 1;
    }

    std::cout << "[CUDA Engine] " << N << " ASes, " << P << " prefixes, "
              << anns.size() << " announcements (" << bytes / (1024.0 * 1024.0) << " MB RIB)\n";

    Csr prov = flatten(topo.providers);
    Csr cust = flatten(topo.customers);
    Csr peer = flatten(topo.peers);

    std::vector<int> rank = compute_ranks(topo);
    int max_rank = 0;
    for (int r : rank) max_rank = std::max(max_rank, r);

    std::vector<std::vector<int>> rank_nodes(max_rank + 1);
    for (int i = 0; i < N; ++i) rank_nodes[rank[i]].push_back(i);
    std::vector<int> all_nodes(N);
    for (int i = 0; i < N; ++i) all_nodes[i] = i;

    // ---- device allocation ----
    DeviceRib rib{};
    CUDA_CHECK(cudaMalloc(&rib.score, entries * sizeof(uint64_t)));
    CUDA_CHECK(cudaMalloc(&rib.path, entries * MAX_PATH_LEN * sizeof(uint32_t)));
    CUDA_CHECK(cudaMalloc(&rib.path_len, entries * sizeof(uint8_t)));
    CUDA_CHECK(cudaMalloc(&rib.rov_invalid, entries * sizeof(uint8_t)));
    CUDA_CHECK(cudaMalloc(&rib.next_hop, entries * sizeof(uint32_t)));

    CUDA_CHECK(cudaMemset(rib.score, 0xFF, entries * sizeof(uint64_t)));  // SCORE_NONE
    CUDA_CHECK(cudaMemset(rib.path, 0, entries * MAX_PATH_LEN * sizeof(uint32_t)));
    CUDA_CHECK(cudaMemset(rib.path_len, 0, entries * sizeof(uint8_t)));
    CUDA_CHECK(cudaMemset(rib.rov_invalid, 0, entries * sizeof(uint8_t)));
    CUDA_CHECK(cudaMemset(rib.next_hop, 0, entries * sizeof(uint32_t)));

    // Seed origin announcements host-side, then upload.
    {
        std::vector<uint64_t> h_score(entries, SCORE_NONE);
        std::vector<uint32_t> h_path(entries * MAX_PATH_LEN, 0);
        std::vector<uint8_t>  h_len(entries, 0);
        std::vector<uint8_t>  h_inv(entries, 0);
        std::vector<uint32_t> h_nh(entries, 0);

        for (const auto& a : anns) {
            size_t e = static_cast<size_t>(a.origin_idx) * P + a.prefix_id;
            h_score[e] = pack_score(REL_ORIGIN, 1, static_cast<uint32_t>(a.origin_idx));
            h_path[e * MAX_PATH_LEN] = static_cast<uint32_t>(a.origin_idx);
            h_len[e] = 1;
            h_inv[e] = a.rov_invalid;
            h_nh[e] = static_cast<uint32_t>(a.origin_idx);
        }

        CUDA_CHECK(cudaMemcpy(rib.score, h_score.data(), entries * sizeof(uint64_t), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(rib.path, h_path.data(), entries * MAX_PATH_LEN * sizeof(uint32_t), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(rib.path_len, h_len.data(), entries * sizeof(uint8_t), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(rib.rov_invalid, h_inv.data(), entries * sizeof(uint8_t), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(rib.next_hop, h_nh.data(), entries * sizeof(uint32_t), cudaMemcpyHostToDevice));
    }

    uint8_t* d_rov = nullptr;
    CUDA_CHECK(cudaMalloc(&d_rov, N * sizeof(uint8_t)));
    CUDA_CHECK(cudaMemcpy(d_rov, topo.rov_enabled.data(), N * sizeof(uint8_t), cudaMemcpyHostToDevice));

    auto upload_csr = [](const Csr& c, int** d_start, int** d_count, int** d_adj, int n) {
        CUDA_CHECK(cudaMalloc(d_start, n * sizeof(int)));
        CUDA_CHECK(cudaMalloc(d_count, n * sizeof(int)));
        CUDA_CHECK(cudaMalloc(d_adj, c.adj.size() * sizeof(int)));
        CUDA_CHECK(cudaMemcpy(*d_start, c.start.data(), n * sizeof(int), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(*d_count, c.count.data(), n * sizeof(int), cudaMemcpyHostToDevice));
        CUDA_CHECK(cudaMemcpy(*d_adj, c.adj.data(), c.adj.size() * sizeof(int), cudaMemcpyHostToDevice));
    };

    int *dp_start, *dp_count, *dp_adj;
    int *dc_start, *dc_count, *dc_adj;
    int *dr_start, *dr_count, *dr_adj;
    upload_csr(prov, &dp_start, &dp_count, &dp_adj, N);
    upload_csr(cust, &dc_start, &dc_count, &dc_adj, N);
    upload_csr(peer, &dr_start, &dr_count, &dr_adj, N);

    int* d_stage = nullptr;
    CUDA_CHECK(cudaMalloc(&d_stage, N * sizeof(int)));

    cudaEvent_t start_ev, stop_ev;
    CUDA_CHECK(cudaEventCreate(&start_ev));
    CUDA_CHECK(cudaEventCreate(&stop_ev));
    CUDA_CHECK(cudaEventRecord(start_ev));

    auto run_stage = [&](const std::vector<int>& nodes,
                         int* adj_start, int* adj_count, int* adj,
                         Relationship rel_type) {
        if (nodes.empty()) return;
        CUDA_CHECK(cudaMemcpy(d_stage, nodes.data(), nodes.size() * sizeof(int), cudaMemcpyHostToDevice));

        int total = static_cast<int>(nodes.size()) * P;
        int blocks = (total + THREADS_PER_BLOCK - 1) / THREADS_PER_BLOCK;

        propagate_score_kernel<<<blocks, THREADS_PER_BLOCK>>>(
            d_stage, static_cast<int>(nodes.size()), P,
            adj_start, adj_count, adj, d_rov, rel_type, rib);
        CUDA_CHECK(cudaGetLastError());

        propagate_commit_kernel<<<blocks, THREADS_PER_BLOCK>>>(
            d_stage, static_cast<int>(nodes.size()), P,
            adj_start, adj_count, adj, d_rov, rel_type, rib);
        CUDA_CHECK(cudaGetLastError());
    };

    // Gao-Rexford stage order, mirroring run_simulation_and_write() in main.cpp:
    // routes climb to providers by rank, cross peer links, then descend to customers.
    for (int r = 0; r <= max_rank; ++r)
        run_stage(rank_nodes[r], dp_start, dp_count, dp_adj, REL_CUSTOMER);

    run_stage(all_nodes, dr_start, dr_count, dr_adj, REL_PEER);

    for (int r = max_rank; r >= 0; --r)
        run_stage(rank_nodes[r], dc_start, dc_count, dc_adj, REL_PROVIDER);

    CUDA_CHECK(cudaEventRecord(stop_ev));
    CUDA_CHECK(cudaEventSynchronize(stop_ev));

    float ms = 0;
    CUDA_CHECK(cudaEventElapsedTime(&ms, start_ev, stop_ev));

    // ---- writeback ----
    std::vector<uint32_t> h_path(entries * MAX_PATH_LEN);
    std::vector<uint8_t>  h_len(entries);
    CUDA_CHECK(cudaMemcpy(h_path.data(), rib.path, entries * MAX_PATH_LEN * sizeof(uint32_t), cudaMemcpyDeviceToHost));
    CUDA_CHECK(cudaMemcpy(h_len.data(), rib.path_len, entries * sizeof(uint8_t), cudaMemcpyDeviceToHost));

    std::ofstream out("ribs.csv");
    if (!out) {
        fprintf(stderr, "Could not open ribs.csv for writing.\n");
        return 1;
    }
    out << "asn,prefix,as_path\n";

    size_t rows = 0;
    for (int u = 0; u < N; ++u) {
        for (int p = 0; p < P; ++p) {
            size_t e = static_cast<size_t>(u) * P + p;
            uint8_t len = h_len[e];
            if (len == 0) continue;

            out << topo.idx_to_asn[u] << ',' << prefix_strings[p] << ",\"(";
            for (int h = 0; h < len; ++h) {
                if (h) out << ", ";
                out << topo.idx_to_asn[h_path[e * MAX_PATH_LEN + h]];
            }
            if (len == 1) out << ',';  // Python single-element tuple repr
            out << ")\"\n";
            ++rows;
        }
    }
    out.close();

    std::cout << "[CUDA Engine] Wrote " << rows << " RIB entries to ribs.csv\n";
    std::cout << "GPU Engine completed in: " << ms << " ms\n";

    cudaFree(rib.score); cudaFree(rib.path); cudaFree(rib.path_len);
    cudaFree(rib.rov_invalid); cudaFree(rib.next_hop);
    cudaFree(d_rov); cudaFree(d_stage);
    cudaFree(dp_start); cudaFree(dp_count); cudaFree(dp_adj);
    cudaFree(dc_start); cudaFree(dc_count); cudaFree(dc_adj);
    cudaFree(dr_start); cudaFree(dr_count); cudaFree(dr_adj);
    return 0;
}
