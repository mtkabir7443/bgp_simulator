import networkx as nx
import matplotlib.pyplot as plt

def generate_and_visualize_hijack():
    print("[+] Generating layered attack topology graph...")
    G = nx.DiGraph()

    # Define edges (Provider -> Customer / Peering)
    edges = [
        (1, 10), (1, 11), (1, 12),
        (2, 10), (2, 11), (2, 12),
        (10, 100),
        (11, 101),
        (12, 102),
        (12, 666)
    ]
    G.add_edges_from(edges)

    # Stratified hierarchical coordinates (x, y)
    pos = {
        # Tier-1 Core (Top)
        1: (-1.5, 3.0),
        2: (1.5, 3.0),
        
        # Tier-2 Transit Providers (Middle)
        10: (-3.0, 1.5),
        11: (0.0, 1.5),
        12: (3.0, 1.5),
        
        # Tier-3 Customer Stubs & Attacker (Bottom)
        100: (-3.0, 0.0),   # Legitimate Origin
        101: (0.0, 0.0),    # Contaminated Stub
        102: (2.0, 0.0),    # Contaminated Stub
        666: (4.0, 0.0)     # Malicious Hijacker
    }

    # Color definitions
    node_colors = {
        100: "#2ecc71",  # Origin (Green)
        666: "#e74c3c",  # Attacker (Red)
        1:   "#3498db",  # ROV Protected Core (Blue)
        10:  "#3498db",  # ROV Protected Transit (Blue)
        2:   "#e67e22",  # Contaminated Core (Orange)
        11:  "#e67e22",  # Contaminated Transit (Orange)
        12:  "#e67e22",  # Contaminated Transit (Orange)
        101: "#e67e22",  # Poisoned Stub (Orange)
        102: "#e67e22",  # Poisoned Stub (Orange)
    }
    colors = [node_colors[node] for node in G.nodes()]

    plt.figure(figsize=(11, 7), facecolor="#ffffff")
    
    # Draw graph elements
    nx.draw_networkx_nodes(
        G, 
        pos, 
        node_color=colors, 
        node_size=1300, 
        edgecolors="#2c3e50", 
        linewidths=1.5
    )
    nx.draw_networkx_labels(
        G, 
        pos, 
        font_color="white", 
        font_size=11, 
        font_weight="bold"
    )
    nx.draw_networkx_edges(
        G, 
        pos, 
        edge_color="#7f8c8d", 
        arrows=True, 
        arrowsize=18, 
        width=1.5, 
        min_source_margin=20, 
        min_target_margin=20
    )

    # Layer labels shifted left to prevent overlap with AS nodes
    plt.text(-4.8, 3.0, "Tier-1 Core", fontsize=11, fontweight="bold", color="#34495e", verticalalignment="center")
    plt.text(-4.8, 1.5, "Tier-2 Transit", fontsize=11, fontweight="bold", color="#34495e", verticalalignment="center")
    plt.text(-4.8, 0.0, "Tier-3 Stubs", fontsize=11, fontweight="bold", color="#34495e", verticalalignment="center")

    plt.title("BGP Subprefix Hijack: ROV Defense (Blue) vs Propagation (Orange)", fontsize=13, fontweight="bold", pad=20)
    plt.xlim(-5.2, 4.8)
    plt.ylim(-0.6, 3.6)
    plt.axis("off")
    
    output_img = "hijack_attack_graph.png"
    plt.savefig(output_img, dpi=300, bbox_inches="tight")
    print(f"✅ Clean layered graph saved to: {output_img}")

if __name__ == "__main__":
    generate_and_visualize_hijack()
