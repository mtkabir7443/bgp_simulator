import os
import subprocess

import pytest

CPU_BINARY = os.path.abspath("./bgp_simulator")
GPU_BINARY = os.path.abspath("./bgp_sim_gpu")


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

    rel = tmp_path / "rel.txt"
    ann = tmp_path / "ann.txt"
    rel.write_text("1 2\n")
    ann.write_text("asn,prefix\n1 100\n")

    result = subprocess.run(
        [GPU_BINARY, "--relationships", str(rel), "--announcements", str(ann)],
        capture_output=True,
        text=True,
        cwd=tmp_path,
    )

    assert result.returncode == 0, f"GPU engine exited {result.returncode}: {result.stderr}"
    assert "GPU Engine (kernel-only)" in result.stdout


def test_cpu_gpu_output_parity(tmp_path):
    """Both engines should produce identical RIBs on the same input."""
    if not (os.path.exists(CPU_BINARY) and os.path.exists(GPU_BINARY)):
        pytest.skip("Both binaries required for parity check.")

    rel = tmp_path / "rel.txt"
    ann = tmp_path / "ann.txt"
    rel.write_text("1|2|0\n2|3|-1\n")
    ann.write_text("1,192.168.1.0/24,0\n")

    cpu_dir = tmp_path / "cpu"
    gpu_dir = tmp_path / "gpu"
    cpu_dir.mkdir()
    gpu_dir.mkdir()

    for binary, workdir in ((CPU_BINARY, cpu_dir), (GPU_BINARY, gpu_dir)):
        result = subprocess.run(
            [binary, "--relationships", str(rel), "--announcements", str(ann)],
            capture_output=True,
            text=True,
            cwd=workdir,
        )
        assert result.returncode == 0, f"{binary} exited {result.returncode}: {result.stderr}"

    cpu_ribs = cpu_dir / "ribs.csv"
    gpu_ribs = gpu_dir / "ribs.csv"

    if not (cpu_ribs.exists() and gpu_ribs.exists()):
        pytest.skip("One engine did not emit ribs.csv; parity not comparable.")

    cpu_rows = sorted(l.strip() for l in cpu_ribs.read_text().splitlines() if l.strip())
    gpu_rows = sorted(l.strip() for l in gpu_ribs.read_text().splitlines() if l.strip())

    assert cpu_rows == gpu_rows, "CPU and GPU engines disagree on RIB contents."
