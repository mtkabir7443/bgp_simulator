"""Expected routes for the supported static policy model, independent of parity."""

import ast
import csv
from pathlib import Path
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]
PREFIX = "192.0.2.0/24"


@pytest.fixture(params=["bgp_simulator", "bgp_sim_gpu"], ids=["cpu", "gpu"])
def engine(request):
    binary = ROOT / "build" / request.param
    if not binary.exists():
        pytest.skip(f"Build {request.param} to exercise this backend.")
    return binary


def run_engine(engine, workdir, relationships, announcements, rov=None):
    rel, ann = workdir / "rel.txt", workdir / "ann.txt"
    rel.write_text(relationships)
    ann.write_text(announcements)
    args = [str(engine), "--relationships", str(rel), "--announcements", str(ann)]
    if rov is not None:
        rov_path = workdir / "rov.txt"
        rov_path.write_text(rov)
        args += ["--rov-asns", str(rov_path)]
    return subprocess.run(args, cwd=workdir, capture_output=True, text=True, timeout=30)


def read_routes(path):
    routes = {}
    with path.open(newline="") as stream:
        reader = csv.DictReader(stream)
        assert reader.fieldnames == ["asn", "prefix", "as_path"]
        for row in reader:
            key = (int(row["asn"]), row["prefix"])
            assert key not in routes, f"Duplicate RIB entry: {key}"
            routes[key] = ast.literal_eval(row["as_path"])
    return routes


@pytest.mark.parametrize(
    "relationships,origins,rov,expected",
    [
        ("", [], None, {}),
        ("1|2|0\n2|3|-1\n", [(1, 0)], None,
         {1: (1,), 2: (2, 1), 3: (3, 2, 1)}),
        # A peer-learned route must not cross a second peer link.
        ("1|2|0\n2|3|0\n", [(1, 0)], None,
         {1: (1,), 2: (2, 1)}),
        # Equal policy and length: raw ASN, not input-assigned node index, wins.
        ("100|9|0\n100|1|0\n", [(9, 0), (1, 0)], None,
         {100: (100, 1), 9: (9,), 1: (1,)}),
        ("100|1|0\n100|9|0\n", [(1, 0), (9, 0)], None,
         {100: (100, 1), 9: (9,), 1: (1,)}),
        # A longer customer route beats a shorter peer route.
        ("100|2|-1\n2|1|-1\n100|9|0\n", [(1, 0), (9, 0)], None,
         {100: (100, 2, 1), 2: (2, 1), 1: (1,), 9: (9,)}),
        ("100|9|-1\n100|2|-1\n2|1|-1\n", [(1, 0), (9, 0)], None,
         {100: (100, 9), 2: (2, 1), 1: (1,), 9: (9,)}),
        ("1|2|0\n3|4|0\n", [(1, 0)], None,
         {1: (1,), 2: (2, 1)}),
        # ROV applies at the origin as well as at receiving ASes in this model.
        ("1|2|0\n", [(1, 1)], "1\n", {}),
        ("1|2|0\n2|3|-1\n", [(1, 1)], "2\n", {1: (1,)}),
        ("1|2|0\n", [(1, 0)], "1\n2\n", {1: (1,), 2: (2, 1)}),
        ("4000000000|1|0\n", [(4000000000, 0)], None,
         {4000000000: (4000000000,), 1: (1, 4000000000)}),
    ],
    ids=["empty", "peer-to-customer", "no-peer-transit", "asn-tie",
         "asn-tie-reordered", "prefer-customer", "prefer-shorter",
         "disconnected", "rov-origin", "rov-receiver", "rov-valid", "32-bit-asn"],
)
def test_expected_routes(engine, tmp_path, relationships, origins, rov, expected):
    announcements = "".join(f"{asn},{PREFIX},{invalid}\n" for asn, invalid in origins)
    result = run_engine(engine, tmp_path, relationships, announcements, rov)
    assert result.returncode == 0, result.stderr
    assert read_routes(tmp_path / "ribs.csv") == {
        (asn, PREFIX): path for asn, path in expected.items()
    }


def test_path_capacity(engine, tmp_path):
    relationships = "".join(f"{i}|{i + 1}|-1\n" for i in range(1, 17))
    result = run_engine(engine, tmp_path, relationships, f"1,{PREFIX},0\n")
    assert result.returncode == 0, result.stderr
    assert read_routes(tmp_path / "ribs.csv") == {
        (asn, PREFIX): tuple(range(asn, 0, -1)) for asn in range(1, 17)
    }


@pytest.mark.parametrize("announcements", ["", f"2,{PREFIX},0\n"])
def test_rejects_provider_customer_cycle(engine, tmp_path, announcements):
    result = run_engine(engine, tmp_path, "1|2|-1\n2|3|-1\n3|1|-1\n", announcements)
    assert result.returncode != 0
    assert "cycl" in result.stderr.lower()
    assert not (tmp_path / "ribs.csv").exists()


@pytest.mark.parametrize("missing", ["relationships", "announcements", "rov-asns"])
def test_missing_input_is_an_error(engine, tmp_path, missing):
    rel, ann = tmp_path / "rel.txt", tmp_path / "ann.txt"
    rel.write_text("1|2|0\n")
    ann.write_text(f"1,{PREFIX},0\n")
    paths = {"relationships": rel, "announcements": ann}
    paths[missing] = tmp_path / "does-not-exist.txt"
    args = [str(engine)]
    for option, path in paths.items():
        args += [f"--{option}", str(path)]
    result = subprocess.run(args, cwd=tmp_path, capture_output=True, text=True, timeout=30)
    assert result.returncode != 0
    assert "does-not-exist.txt" in result.stderr
    assert not (tmp_path / "ribs.csv").exists()


@pytest.mark.parametrize("boundary_file", ["relationships", "announcements", "rov"])
def test_page_boundary_without_final_newline(engine, tmp_path, boundary_file):
    # End exactly at a mapped-page boundary, without relying on zero padding.
    values = {"relationships": "1|2|0", "announcements": f"1,{PREFIX},0", "rov": "2"}
    values[boundary_file] = "\n" * (4096 - len(values[boundary_file])) + values[boundary_file]
    result = run_engine(engine, tmp_path, values["relationships"], values["announcements"], values["rov"])
    assert result.returncode == 0, result.stderr
    assert read_routes(tmp_path / "ribs.csv") == {(1, PREFIX): (1,), (2, PREFIX): (2, 1)}
