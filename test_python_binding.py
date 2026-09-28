"""PyBind11 extension smoke test.

Runs the C++ engine in-process via the compiled `bgp_simulator` module.
All fixtures and outputs are confined to a temporary directory so this
never clobbers generated datasets in the repository root.
"""

import os
import tempfile

import bgp_simulator


def test_pybind11_execution():
    print("Testing PyBind11 C++ Extension...")

    original_cwd = os.getcwd()

    with tempfile.TemporaryDirectory(prefix="bgp_pybind_") as workdir:
        os.chdir(workdir)
        try:
            rel_file = "rel_py_test.txt"
            ann_file = "ann_py_test.txt"

            with open(rel_file, "w") as f:
                f.write("1|2|0\n2|3|-1\n")

            with open(ann_file, "w") as f:
                f.write("1,192.168.1.0/24,0\n")

            # Invoke C++ engine directly from Python
            bgp_simulator.run(
                relationships=rel_file,
                announcements=ann_file,
                rov_asns="",
            )

            assert os.path.exists("ribs.csv"), "ribs.csv was not generated."

            with open("ribs.csv") as f:
                lines = [line.strip() for line in f if line.strip()]

            assert lines, "ribs.csv is empty."

            print(f"Successfully generated {len(lines)} lines:")
            for line in lines:
                print(f"  {line}")
        finally:
            os.chdir(original_cwd)

    print("\nPyBind11 execution passed successfully.")


def test_recovery_after_invalid_input():
    """A rejected call must not poison the next simulation in the same process."""
    original_cwd = os.getcwd()
    with tempfile.TemporaryDirectory(prefix="bgp_recovery_") as workdir:
        os.chdir(workdir)
        try:
            with open("rel.txt", "w") as stream:
                stream.write("1|2|-1\n2|1|-1\n")
            with open("ann.txt", "w") as stream:
                stream.write("1,192.0.2.0/24,0\n")
            try:
                bgp_simulator.run("rel.txt", "ann.txt")
            except RuntimeError as error:
                assert "cycl" in str(error).lower()
            else:
                raise AssertionError("Provider/customer cycle was accepted")
            assert not os.path.exists("ribs.csv")

            with open("rel.txt", "w") as stream:
                stream.write("1|2|0\n")
            for _ in range(2):
                bgp_simulator.run("rel.txt", "ann.txt")
                with open("ribs.csv") as stream:
                    assert set(stream.read().splitlines()) == {
                        "asn,prefix,as_path",
                        '1,192.0.2.0/24,"(1,)"',
                        '2,192.0.2.0/24,"(2, 1)"',
                    }
        finally:
            os.chdir(original_cwd)


if __name__ == "__main__":
    test_pybind11_execution()
    test_recovery_after_invalid_input()
