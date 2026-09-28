"""Synthetic hierarchical topology benchmark; does not load real CAIDA data."""

import argparse

from benchmark import benchmark_workspace, measure_engine

REL_FILE = "caida_rel.txt"
ANN_FILE = "caida_ann.txt"
ROV_FILE = "caida_rov.txt"


def prepare_caida_dataset(num_tier1=20, num_tier2=3_000, num_stubs=72_000):
    if min(num_tier1, num_tier2, num_stubs) < 1:
        raise ValueError("Each topology tier must contain at least one AS")
    total_nodes = num_tier1 + num_tier2 + num_stubs
    t2_start = num_tier1 + 1
    t2_end = num_tier1 + num_tier2
    print(f"Generating synthetic hierarchy ({total_nodes:,} ASes)")
    with open(REL_FILE, "w", buffering=32 * 1024 * 1024) as output:
        for i in range(1, num_tier1 + 1):
            for j in range(i + 1, num_tier1 + 1):
                output.write(f"{i}|{j}|0\n")
        for t2 in range(t2_start, t2_end + 1):
            providers = {(t2 % num_tier1) + 1, ((t2 + 3) % num_tier1) + 1}
            for provider in sorted(providers):
                output.write(f"{provider}|{t2}|-1\n")
            if t2 % 4 == 0 and t2 + 1 <= t2_end:
                output.write(f"{t2}|{t2 + 1}|0\n")
        for stub in range(t2_end + 1, total_nodes + 1):
            provider = t2_start + (stub % num_tier2)
            output.write(f"{provider}|{stub}|-1\n")
    return t2_end + 1


def generate_announcements(num_prefixes=500, num_stubs=72_000, stub_start=3021):
    if not 1 <= num_prefixes <= 65_536:
        raise ValueError("Prefix count must be between 1 and 65,536")
    if num_stubs < 1 or stub_start < 1:
        raise ValueError("Stub count and first stub ASN must be positive")
    with open(ANN_FILE, "w", buffering=16 * 1024 * 1024) as output:
        for i in range(num_prefixes):
            origin_asn = stub_start + (i * 137 % num_stubs)
            # One distinct, canonical /24 per input announcement within 100.0.0.0/8.
            output.write(f"{origin_asn},100.{i // 256}.{i % 256}.0/24,0\n")
    with open(ROV_FILE, "w") as output:
        for asn in range(1, min(stub_start, 1000)):
            output.write(f"{asn}\n")


def run_caida_benchmark(num_tier1=20, num_tier2=3_000, num_stubs=72_000, num_prefixes=500):
    if min(num_tier1, num_tier2, num_stubs) < 1:
        raise ValueError("Each topology tier must contain at least one AS")
    if not 1 <= num_prefixes <= 65_536:
        raise ValueError("Prefix count must be between 1 and 65,536")

    import bgp_simulator

    print("Synthetic topology benchmark (no CAIDA dataset is loaded)")
    with benchmark_workspace():
        stub_start = prepare_caida_dataset(num_tier1, num_tier2, num_stubs)
        generate_announcements(num_prefixes, num_stubs, stub_start)
        result = measure_engine(bgp_simulator, REL_FILE, ANN_FILE, ROV_FILE)
    elapsed = result["elapsed_seconds"]
    print(f"Topology: {num_tier1 + num_tier2 + num_stubs:,} ASes")
    print(f"Input: {result['announcements']:,} announcements, "
          f"{result['unique_prefixes']:,} unique prefixes")
    print(f"End-to-end engine time (including CSV output): {elapsed:.3f} s")
    print(f"Observed RIB entries: {result['rib_entries']:,}")
    print(f"RIB entries / engine second: {result['rib_entries'] / elapsed:,.0f}")
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tier1", type=int, default=20, help="number of core ASes")
    parser.add_argument("--tier2", type=int, default=3000, help="number of transit ASes")
    parser.add_argument("--stubs", type=int, default=72000, help="number of customer ASes")
    parser.add_argument("--prefixes", type=int, default=500, help="number of distinct /24 prefixes")
    args = parser.parse_args()
    try:
        run_caida_benchmark(args.tier1, args.tier2, args.stubs, args.prefixes)
    except ValueError as error:
        parser.error(str(error))
