"""Synthetic throughput benchmark; timings include parsing, routing, and CSV output."""

import argparse
import csv
import os
import sys
import tempfile
import time
from contextlib import contextmanager
from pathlib import Path


# The extension is built separately from the Python benchmark scripts.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "build"))


@contextmanager
def benchmark_workspace():
    """The engine writes ribs.csv in cwd; keep every generated file disposable."""
    original_cwd = Path.cwd()
    with tempfile.TemporaryDirectory(prefix="bgp_benchmark_") as directory:
        os.chdir(directory)
        try:
            yield
        finally:
            os.chdir(original_cwd)


def measure_engine(engine, relationships, announcements, rov_asns=""):
    with open(announcements, newline="") as source:
        prefixes = set()
        announcement_count = 0
        for row in csv.reader(source):
            announcement_count += 1
            prefixes.add(row[1])
    unique_prefixes = len(prefixes)
    del prefixes

    started = time.perf_counter()
    engine.run(
        relationships=relationships, announcements=announcements, rov_asns=rov_asns
    )
    elapsed = time.perf_counter() - started

    with open("ribs.csv", newline="") as output:
        reader = csv.reader(output)
        if next(reader, None) != ["asn", "prefix", "as_path"]:
            raise ValueError("Engine output is missing the expected RIB header")
        rib_entries = sum(1 for row in reader if row)
    return {
        "announcements": announcement_count,
        "unique_prefixes": unique_prefixes,
        "rib_entries": rib_entries,
        "elapsed_seconds": elapsed,
    }


def run_speed_check(route_counts=(100_000, 500_000, 1_000_000)):
    route_counts = tuple(route_counts)
    if not route_counts or any(count < 1 or count > 2**24 for count in route_counts):
        raise ValueError("Route counts must be between 1 and 16,777,216")

    # Load the extension before entering the disposable working directory.
    import bgp_simulator

    print("Synthetic six-AS throughput benchmark")
    results = []
    with benchmark_workspace():
        Path("rel_bench.txt").write_text("1|2|0\n2|3|-1\n3|4|-1\n4|5|0\n5|6|-1\n")
        for count in route_counts:
            started = time.perf_counter()
            with open("ann_bench.txt", "w", buffering=16 * 1024 * 1024) as output:
                for chunk_start in range(0, count, 50_000):
                    output.writelines(
                        f"1,10.{i // 65536}.{(i // 256) % 256}.{i % 256}/32,0\n"
                        for i in range(chunk_start, min(chunk_start + 50_000, count))
                    )
            generation_seconds = time.perf_counter() - started
            size_mb = Path("ann_bench.txt").stat().st_size / (1024 * 1024)
            result = measure_engine(bgp_simulator, "rel_bench.txt", "ann_bench.txt")
            results.append(result)
            elapsed = result["elapsed_seconds"]
            print(f"\nInput: {result['announcements']:,} announcements, "
                  f"{result['unique_prefixes']:,} unique prefixes")
            print(f"Dataset: {size_mb:.2f} MiB, generated in {generation_seconds:.3f} s")
            print(f"End-to-end engine time (including CSV output): {elapsed:.3f} s")
            print(f"Observed RIB entries: {result['rib_entries']:,}")
            print(f"Input announcements / engine second: {count / elapsed:,.0f}")
            print(f"RIB entries / engine second: {result['rib_entries'] / elapsed:,.0f}")
    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--routes", type=int, nargs="+", default=[100_000, 500_000, 1_000_000],
                        help="announcement counts to run (use --routes 10 for a smoke test)")
    args = parser.parse_args()
    try:
        run_speed_check(args.routes)
    except ValueError as error:
        parser.error(str(error))
