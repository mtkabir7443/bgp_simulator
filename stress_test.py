import os
import subprocess
import sys

STRESS_ROUTES = 500_000
CPU_BINARY = "./bgp_simulator"

print(f"Generating stress dataset with {STRESS_ROUTES} routes...")
with open("rel_stress.txt", "w") as f:
    f.write("1|2|0\n2|3|-1\n3|4|0\n")

with open("ann_stress.txt", "w") as f:
    for i in range(STRESS_ROUTES):
        f.write(f"1,10.{ (i // 65536) % 256 }.{ (i // 256) % 256 }.{ i % 256 }/32,0\n")

print("Launching CPU simulation under heavy load...")
result = subprocess.run(
    [CPU_BINARY, "--relationships", "rel_stress.txt", "--announcements", "ann_stress.txt"],
    capture_output=True,
    text=True
)

for f in ["rel_stress.txt", "ann_stress.txt"]:
    if os.path.exists(f):
        os.remove(f)

if result.returncode == 0:
    print("SUCCESS: Engine completed execution successfully without memory errors.")
    sys.exit(0)
else:
    print(f"FAILURE: Engine failed with returncode {result.returncode}.\nStderr: {result.stderr}")
    sys.exit(1)
