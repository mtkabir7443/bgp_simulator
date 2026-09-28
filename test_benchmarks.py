"""Regression checks for workload identity, measured counts, and file isolation."""

import csv
import ipaddress
from pathlib import Path

import pytest

from benchmark import benchmark_workspace, run_speed_check
from caida_benchmark import generate_announcements, prepare_caida_dataset, run_caida_benchmark


def test_synthetic_prefixes_are_unique_and_canonical(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    stub_start = prepare_caida_dataset(2, 3, 5)
    generate_announcements(500, 5, stub_start)
    with Path("caida_ann.txt").open() as stream:
        rows = list(csv.reader(stream))
    networks = {ipaddress.ip_network(row[1], strict=True) for row in rows}
    assert len(rows) == len(networks) == 500
    assert all(stub_start <= int(row[0]) < stub_start + 5 for row in rows)


@pytest.mark.parametrize("scenario", ["chain", "hierarchy"])
def test_benchmark_counts_and_preserves_files(tmp_path, monkeypatch, scenario):
    pytest.importorskip("bgp_simulator")
    monkeypatch.chdir(tmp_path)
    names = ["rel_bench.txt", "ann_bench.txt", "caida_rel.txt",
             "caida_ann.txt", "caida_rov.txt", "ribs.csv"]
    for name in names:
        Path(name).write_text(f"preserve {name}\n")
    if scenario == "chain":
        result = run_speed_check([10])[0]
        assert result["announcements"] == result["unique_prefixes"] == 10
        # The second peer crossing is forbidden: only four ASes receive routes.
        assert result["rib_entries"] == 40
    else:
        result = run_caida_benchmark(2, 3, 5, 4)
        assert result["announcements"] == result["unique_prefixes"] == 4
        assert result["rib_entries"] == 40
    assert result["elapsed_seconds"] > 0
    assert Path.cwd() == tmp_path
    assert {path.name for path in tmp_path.iterdir()} == set(names)
    for name in names:
        assert Path(name).read_text() == f"preserve {name}\n"


def test_benchmark_restores_directory_on_failure(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(RuntimeError, match="simulation failed"):
        with benchmark_workspace():
            temporary = Path.cwd()
            Path("ribs.csv").write_text("partial output")
            raise RuntimeError("simulation failed")
    assert Path.cwd() == tmp_path
    assert not temporary.exists()
    assert not list(tmp_path.iterdir())
