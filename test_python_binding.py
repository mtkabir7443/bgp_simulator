import os
import sys
import bgp_simulator

def test_pybind11_execution():
    print("Testing PyBind11 C++ Extension...")

    # Define minimal topology and announcement data
    rel_file = "rel_py_test.txt"
    ann_file = "ann_py_test.txt"

    with open(rel_file, "w") as f:
        f.write("1|2|0\n2|3|-1\n")

    with open(ann_file, "w") as f:
        f.write("1,192.168.1.0/24,0\n")

    # Invoke C++ engine directly from Python
    bgp_simulator.run(relationships=rel_file, announcements=ann_file, rov_asns="")

    assert os.path.exists("ribs.csv"), "ribs.csv was not generated."

    with open("ribs.csv", "r") as f:
        lines = [line.strip() for line in f.readlines() if line.strip()]

    print(f"Successfully generated {len(lines)} lines:")
    for line in lines:
        print(f"  {line}")

    # Cleanup temporary test files
    for f in [rel_file, ann_file, "ribs.csv"]:
        if os.path.exists(f):
            os.remove(f)

    print("\n✅ PyBind11 execution passed successfully.")

if __name__ == "__main__":
    test_pybind11_execution()
