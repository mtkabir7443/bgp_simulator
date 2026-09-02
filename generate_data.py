import random

# Tuned to consume ~14GB of physical RAM during simulation
NUM_ASES = 15000       
NUM_LINKS = 45000     
NUM_ANNOUNCEMENTS = 30000 

print("Generating rel.txt...")
with open("rel.txt", "w") as f:
    for _ in range(NUM_LINKS):
        u = random.randint(1, NUM_ASES)
        v = random.randint(1, NUM_ASES)
        if u == v: continue
        
        # Enforce strict hierarchy: smaller AS is always the provider
        if u > v:
            u, v = v, u 
            
        # -1 (u is Provider to v) or 0 (Peer-to-Peer)
        rel = random.choice([-1, 0])
        f.write(f"{u}|{v}|{rel}\n")

print("Generating ann.txt...")
with open("ann.txt", "w") as f:
    f.write("asn,prefix,rov\n")
    for _ in range(NUM_ANNOUNCEMENTS):
        asn = random.randint(1, NUM_ASES)
        prefix = f"{random.randint(1,255)}.{random.randint(0,255)}.{random.randint(0,255)}.0/24"
        f.write(f"{asn},{prefix},0\n")

print("Done! Generated acyclic massive synthetic BGP dataset tuned for 14GB route state.")
