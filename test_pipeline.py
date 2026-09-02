import subprocess
import os
import pytest

CPU_BINARY = "./bgp_simulator"
GPU_BINARY = "./bgp_sim_gpu"

@pytest.fixture(autouse=True)
def cleanup_files():
    yield
    for f in ["rel_test.txt", "ann_test.txt", "rel.txt", "ann.txt", "ribs.csv"]:
        if os.path.exists(f):
            os.remove(f)

def test_binaries_exist():
    assert os.path.exists(CPU_BINARY) or os.path.exists(GPU_BINARY), "No simulator binaries found."

def test_cpu_empty_and_minimal_simulation():
    if not os.path.exists(CPU_BINARY):
        pytest.skip("CPU binary not built.")

    with open("rel_test.txt", "w") as f:
        f.write("1|2|0\n")
    with open("ann_test.txt", "w") as f:
        f.write("1,10.0.0.0/24,0\n")

    cmd = [CPU_BINARY, "--relationships", "rel_test.txt", "--announcements", "ann_test.txt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    assert os.path.exists("ribs.csv")

def test_gpu_simulation_execution():
    if not os.path.exists(GPU_BINARY):
        pytest.skip("GPU binary not built.")

    with open("rel.txt", "w") as f:
        f.write("1 2\n")
    with open("ann.txt", "w") as f:
        f.write("asn,prefix\n1 100\n")

    cmd = [GPU_BINARY, "--relationships", "rel.txt", "--announcements", "ann.txt"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    assert result.returncode == 0
    assert "GPU KERNEL EXECUTION" in result.stdout
