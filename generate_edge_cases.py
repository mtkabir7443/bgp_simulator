def generate_infinite_loop():
    print("Generating Infinite Loop Topology...")
    with open("rel_loop.txt", "w") as f:
        # AS 1 -> AS 2 -> AS 3 -> AS 1 (Circular Dependency)
        f.write("1|2|-1\n2|3|-1\n3|1|-1\n")
    
    with open("ann_loop.txt", "w") as f:
        f.write("asn,prefix,rov\n")
        f.write("1,192.168.1.0/24,0\n")

def generate_island_clusters():
    print("Generating Disconnected Island Topology...")
    with open("rel_island.txt", "w") as f:
        f.write("1|2|-1\n") # Cluster A
        f.write("3|4|-1\n") # Cluster B
        
    with open("ann_island.txt", "w") as f:
        f.write("asn,prefix,rov\n")
        f.write("1,10.0.0.0/8,0\n")

if __name__ == "__main__":
    generate_infinite_loop()
    generate_island_clusters()
    print("Edge case topologies generated.")
