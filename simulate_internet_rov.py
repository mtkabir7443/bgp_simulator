import os
import sys
import time
import random

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import bgp_simulator

def generate_hierarchical_topology(
    num_tier1=15, 
    num_tier2=150, 
    num_tier3=5_000, 
    rel_filename="rel_internet.txt"
):
    print(f"[1/4] Generating hierarchical Internet topology ({num_tier1 + num_tier2 + num_tier3:,} ASNs)...")
    edges = []

    t1_start, t1_end = 1, num_tier1
    t2_start, t2_end = t1_end + 1, t1_end + num_tier2
    t3_start, t3_end = t2_end + 1, t2_end + num_tier3

    # 1. Tier-1 Full Mesh Peering (rel = 0)
    for i in range(t1_start, t1_end + 1):
        for j in range(i + 1, t1_end + 1):
            edges.append(f"{i}|{j}|0\n")

    # 2. Tier-2 Transit Providers (Multi-homed to Tier-1s + Peering)
    for t2 in range(t2_start, t2_end + 1):
        providers = random.sample(range(t1_start, t1_end + 1), k=random.randint(2, min(4, num_tier1)))
        for p in providers:
            edges.append(f"{p}|{t2}|-1\n")

        potential_peers = [x for x in range(t2_start, t2_end + 1) if x != t2]
        peers = random.sample(potential_peers, k=min(3, len(potential_peers)))
        for peer in peers:
            if t2 < peer:
                edges.append(f"{t2}|{peer}|0\n")

    # 3. Tier-3 Customer Stubs (Multi-homed to Tier-2 providers)
    for t3 in range(t3_start, t3_end + 1):
        providers = random.sample(range(t2_start, t2_end + 1), k=random.randint(1, 2))
        for p in providers:
            edges.append(f"{p}|{t3}|-1\n")

    with open(rel_filename, "w", buffering=16 * 1024 * 1024) as f:
        f.writelines(edges)

    print(f"      Topology generated: {len(edges):,} relationship links.")
    return (t1_start, t1_end), (t2_start, t2_end), (t3_start, t3_end)

def generate_workload_and_hijacks(
    t3_range, 
    num_valid_prefixes=2_000, 
    num_hijacks=100, 
    rov_adoption_ratio=0.40,
    ann_filename="ann_internet.txt",
    rov_filename="rov_internet.txt"
):
    print(f"[2/4] Generating {num_valid_prefixes:,} legitimate prefixes & {num_hijacks:,} hijacks...")
    t3_start, t3_end = t3_range
    t3_nodes = list(range(t3_start, t3_end + 1))
    
    announcements = []

    # 1. Legitimate origin announcements
    for i in range(num_valid_prefixes):
        origin_as = random.choice(t3_nodes)
        pfx = f"100.{(i // 65536) % 256}.{(i // 256) % 256}.{i % 256}/24"
        announcements.append(f"{origin_as},{pfx},0\n")

    # 2. Forged Subprefix / Hijack announcements (rov_invalid = 1)
    attacker_as = random.choice(t3_nodes)
    for i in range(num_hijacks):
        hijacked_pfx = f"100.0.{(i // 256) % 256}.{i % 256}/24"
        announcements.append(f"{attacker_as},{hijacked_pfx},1\n")

    with open(ann_filename, "w", buffering=16 * 1024 * 1024) as f:
        f.writelines(announcements)

    # 3. Configure ROV filtering nodes (focusing on Tier-1 core + Tier-2 transit)
    all_asns = list(range(1, t3_end + 1))
    num_rov = int(len(all_asns) * rov_adoption_ratio)
    rov_asns = random.sample(all_asns, k=num_rov)

    with open(rov_filename, "w") as f:
        for asn in rov_asns:
            f.write(f"{asn}\n")

    print(f"      Announcements: {len(announcements):,} routes written.")
    print(f"      ROV Adoption:  {num_rov:,}/{len(all_asns):,} ASNs ({rov_adoption_ratio*100:.1f}%) enforcing ROV.")
    return attacker_as

def evaluate_simulation_results(attacker_as, rib_filename="ribs.csv"):
    print("[4/4] Parsing convergence results from ribs.csv...")
    if not os.path.exists(rib_filename):
        print("Error: ribs.csv not found.")
        return

    total_rib_entries = 0
    poisoned_routes = 0

    with open(rib_filename, "r") as f:
        header = f.readline()
        for line in f:
            total_rib_entries += 1
            if f"{attacker_as}\"" in line or f"{attacker_as}," in line or f" {attacker_as})" in line:
                poisoned_routes += 1

    print(f"      Total Converged RIB Entries: {total_rib_entries:,}")
    print(f"      Attacker Contamination:     {poisoned_routes:,} routes selected attacker AS {attacker_as}")
    print(f"      Defense Cleanliness:         {((total_rib_entries - poisoned_routes) / total_rib_entries * 100):.2f}% clean")

def run_experiment():
    rel_file = "rel_internet.txt"
    ann_file = "ann_internet.txt"
    rov_file = "rov_internet.txt"
    rib_file = "ribs.csv"

    print("================================================================")
    print("🌐 GLOBAL INTERNET SIMULATION & ROV DEFENSE BENCHMARK")
    print("================================================================")

    t1_r, t2_r, t3_r = generate_hierarchical_topology(num_tier1=15, num_tier2=150, num_tier3=5_000, rel_filename=rel_file)

    attacker_as = generate_workload_and_hijacks(
        t3_range=t3_r, 
        num_valid_prefixes=2_000, 
        num_hijacks=100, 
        rov_adoption_ratio=0.40,
        ann_filename=ann_file,
        rov_filename=rov_file
    )

    print("[3/4] Running C++ Engine on full Internet topology...")
    t0 = time.perf_counter()
    bgp_simulator.run(relationships=rel_file, announcements=ann_file, rov_asns=rov_file)
    elapsed = time.perf_counter() - t0
    print(f"      Simulation completed in {elapsed * 1000:.2f} ms ({elapsed:.4f} seconds)")

    evaluate_simulation_results(attacker_as, rib_filename=rib_file)

    for f in [rel_file, ann_file, rov_file, rib_file]:
        if os.path.exists(f):
            os.remove(f)

    print("\n================================================================")
    print("✅ Experiment completed successfully.")
    print("================================================================")

if __name__ == "__main__":
    run_experiment()
