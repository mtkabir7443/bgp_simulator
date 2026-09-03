import os
import subprocess

import pytest

CPU_BINARY = os.path.abspath("./bgp_simulator")
GPU_BINARY = os.path.abspath("./bgp_sim_gpu")

# Minimal three-AS chain: 1 peers with 2, 2 is a provider to 3.
# Exercises the provider stage, loop detection, and path construction.
REL_FIXTURE = "1|2|0\n2|3|-1\n"
ANN_FIXTURE = "1,192.168.1.0/24,0\n"


def _run(binary, workdir):
    """Run an engine on the standard fixture inside workdir."""
    rel = workdir / "rel.txt"
    ann = workdir / "ann.txt"
    rel.write_text(REL_FIXTURE)
    ann.write_text(ANN_FIXTURE)

    return subprocess.run(
        [binary, "--relationships", str(rel), "--announcements", str(ann)],
        capture_output=True,
        text=True,
        cwd=workdir,
    )


def test_binaries_exist():
    assert os.path.exists(CPU_BINARY) or os.path.exists(GPU_BINARY), \
        "No simulator binaries found. Run `make` first."


def test_cpu_empty_and_minimal_simulation(tmp_path):
    if not os.path.exists(CPU_BINARY):
        pytest.skip("CPU binary not built.")

    rel = tmp_path / "rel_test.txt"
    ann = tmp_path / "ann_test.txt"
    rel.write_text("1|2|0\n")
    ann.write_text("1,10.0.0.0/24,0\n")

    result = subprocess.run(
        [CPU_BINARY, "--relationships", str(rel), "--announcements", str(ann)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )

    assert result.returncode == 0, f"CPU engine exited {result.returncode}: {result.stderr}"
    assert (tmp_path / "ribs.csv").exists(), "ribs.csv was not generated."


def test_gpu_simulation_execution(tmp_path):
    if not os.path.exists(GPU_BINARY):
        pytest.skip("GPU binary not built.")

    result = _run(GPU_BINARY, tmp_path)

    assert result.returncode == 0, f"GPU engine exited {result.returncode}: {result.stderr}"
    assert (tmp_path / "ribs.csv").exists(), "GPU engine did not emit ribs.csv"
    assert "GPU Engine completed" in result.stdout


def test_gpu_parses_input(tmp_path):
    """The GPU engine must actually read the topology, not ignore its arguments."""
    if not os.path.exists(GPU_BINARY):
        pytest.skip("GPU binary not built.")

    result = _run(GPU_BINARY, tmp_path)

    assert result.returncode == 0, f"GPU engine exited {result.returncode}: {result.stderr}"
    assert "Nothing to simulate" not in result.stdout, \
        "GPU engine parsed 0 ASes or 0 prefixes from valid input."

    rows = [l for l in (tmp_path / "ribs.csv").read_text().splitlines()[1:] if l.strip()]
    assert len(rows) == 3, f"Expected 3 RIB entries, got {len(rows)}"


def test_cpu_gpu_output_parity(tmp_path):
    """Both engines must produce identical RIBs on the same input."""
    if not (os.path.exists(CPU_BINARY) and os.path.exists(GPU_BINARY)):
        pytest.skip("Both binaries required for parity check.")

    cpu_dir = tmp_path / "cpu"
    gpu_dir = tmp_path / "gpu"
    cpu_dir.mkdir()
    gpu_dir.mkdir()

    for binary, workdir in ((CPU_BINARY, cpu_dir), (GPU_BINARY, gpu_dir)):
        result = _run(binary, workdir)
        assert result.returncode == 0, f"{binary} exited {result.returncode}: {result.stderr}"

    cpu_ribs = cpu_dir / "ribs.csv"
    gpu_ribs = gpu_dir / "ribs.csv"

    assert cpu_ribs.exists(), "CPU engine did not emit ribs.csv"
    assert gpu_ribs.exists(), "GPU engine did not emit ribs.csv"

    cpu_rows = sorted(l.strip() for l in cpu_ribs.read_text().splitlines() if l.strip())
    gpu_rows = sorted(l.strip() for l in gpu_ribs.read_text().splitlines() if l.strip())

    assert cpu_rows == gpu_rows, (
        "CPU and GPU engines disagree on RIB contents.\n"
        f"CPU only: {[r for r in cpu_rows if r not in gpu_rows]}\n"
        f"GPU only: {[r for r in gpu_rows if r not in cpu_rows]}"
    )
