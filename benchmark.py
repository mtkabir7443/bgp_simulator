import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bgp_simulator

def run_speed_check(route_counts=[100_000, 500_000, 1_000_000]):
    rel_file = "rel_bench.txt"
    ann_file = "ann_bench.txt"

    print("================================================================")
    print("🚀 BGP SIMULATOR SPEED & THROUGHPUT BENCHMARK")
    print("================================================================")

    with open(rel_file, "w") as f:
        f.write("1|2|0\n2|3|-1\n3|4|-1\n4|5|0\n5|6|-1\n")

    for count in route_counts:
        print(f"\n[+] Benchmarking {count:,} routes...")
        
        # 1. Batch buffered dataset generation
        t0 = time.perf_counter()
        with open(ann_file, "w", buffering=16 * 1024 * 1024) as f:
            chunk_size = 50_000
            for chunk_start in range(0, count, chunk_size):
                chunk_end = min(chunk_start + chunk_size, count)
                lines = [
                    f"1,10.{(i // 65536) % 256}.{(i // 256) % 256}.{i % 256}/32,0\n"
                    for i in range(chunk_start, chunk_end)
                ]
                f.write("".join(lines))

        gen_time = time.perf_counter() - t0
        file_size_mb = os.path.getsize(ann_file) / (1024 * 1024)

        # 2. C++ Engine execution
        t1 = time.perf_counter()
        bgp_simulator.run(relationships=rel_file, announcements=ann_file, rov_asns="")
        sim_time = time.perf_counter() - t1

        # 3. Output results
        throughput = count / sim_time if sim_time > 0 else 0
        total_rib_routes = count * 6
        print(f"    Dataset Size:      {file_size_mb:.2f} MB (generated in {gen_time:.2f}s)")
        print(f"    Engine Sim Time:   {sim_time * 1000:.2f} ms ({sim_time:.4f} s)")
        print(f"    Ingestion Rate:    {throughput:,.0f} input routes/sec")
        print(f"    RIB Throughput:    {(total_rib_routes / sim_time):,.0f} processed rib paths/sec")

    for f in [rel_file, ann_file, "ribs.csv"]:
        if os.path.exists(f):
            os.remove(f)

    print("\n================================================================")
    print("✅ Benchmark complete.")
    print("================================================================")

if __name__ == "__main__":
    run_speed_check()
