"""Run the CPU engine on a disposable synthetic workload."""

import argparse
import subprocess
import tempfile
from pathlib import Path

STRESS_ROUTES = 500_000
CPU_BINARY = Path(__file__).resolve().parents[1] / "build" / "bgp_simulator"


def run_stress_test(route_count=STRESS_ROUTES):
    if not 1 <= route_count <= 2**24:
        raise ValueError("Route count must be between 1 and 16,777,216")
    if not CPU_BINARY.is_file():
        raise FileNotFoundError("CPU engine not found. Run `make cpu` in the repository first.")

    with tempfile.TemporaryDirectory(prefix="bgp_stress_") as directory:
        workspace = Path(directory)
        print(f"Generating stress dataset with {route_count:,} routes...")
        (workspace / "rel_stress.txt").write_text("1|2|0\n2|3|-1\n3|4|0\n")
        with (workspace / "ann_stress.txt").open("w") as output:
            for i in range(route_count):
                output.write(f"1,10.{(i // 65536) % 256}.{(i // 256) % 256}.{i % 256}/32,0\n")

        print("Launching CPU simulation under heavy load...")
        result = subprocess.run(
            [str(CPU_BINARY), "--relationships", "rel_stress.txt",
             "--announcements", "ann_stress.txt"],
            capture_output=True,
            text=True,
            cwd=workspace,
        )
    if result.returncode == 0:
        print("SUCCESS: Engine completed execution successfully.")
    else:
        print(f"FAILURE: Engine failed with return code {result.returncode}.\nStderr: {result.stderr}")
    return result.returncode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--routes", type=int, default=STRESS_ROUTES,
                        help="number of announcements (default: 500000)")
    args = parser.parse_args()
    try:
        raise SystemExit(run_stress_test(args.routes))
    except (ValueError, OSError) as error:
        parser.error(str(error))
