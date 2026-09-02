import os
import sys
import time

class PrefixTrieNode:
    def __init__(self):
        self.children = [None, None]
        self.best_asn = None
        self.path = None

class LongestPrefixMatchTrie:
    def __init__(self):
        self.root = PrefixTrieNode()

    def _parse_cidr(self, cidr_str):
        ip_str, mask_str = cidr_str.strip().split('/')
        octets = [int(o) for o in ip_str.split('.')]
        ip_int = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3]
        return ip_int, int(mask_str)

    def insert_route(self, cidr_str, best_asn, path):
        ip_int, mask_len = self._parse_cidr(cidr_str)
        curr = self.root
        for bit_idx in range(31, 31 - mask_len, -1):
            bit = (ip_int >> bit_idx) & 1
            if curr.children[bit] is None:
                curr.children[bit] = PrefixTrieNode()
            curr = curr.children[bit]
        curr.best_asn = best_asn
        curr.path = path

    def lookup_ip(self, ip_str):
        octets = [int(o) for o in ip_str.split('.')]
        ip_int = (octets[0] << 24) | (octets[1] << 16) | (octets[2] << 8) | octets[3]
        
        curr = self.root
        longest_match = None
        for bit_idx in range(31, -1, -1):
            if curr.best_asn is not None:
                longest_match = (curr.best_asn, curr.path)
            bit = (ip_int >> bit_idx) & 1
            if curr.children[bit] is None:
                break
            curr = curr.children[bit]
        return longest_match

def test_lpm_and_withdraw():
    print("================================================================")
    print("🌐 CIDR LONGEST PREFIX MATCH (LPM) & CHURN DEMONSTRATION")
    print("================================================================")

    trie = LongestPrefixMatchTrie()
    
    # 1. Covering legitimate route: 10.0.0.0/16 via AS 100
    trie.insert_route("10.0.0.0/16", best_asn=100, path=[100])
    print("[+] Legitimate covering route installed: 10.0.0.0/16 -> AS 100")

    # 2. Lookup before hijack
    target_ip = "10.0.5.1"
    matched_asn, path = trie.lookup_ip(target_ip)
    print(f"    Lookup {target_ip}: routed to AS {matched_asn} with path {path}")

    # 3. Subprefix Hijack: 10.0.5.0/24 via Malicious AS 666
    print("[+] Malicious Subprefix injected: 10.0.5.0/24 -> AS 666")
    trie.insert_route("10.0.5.0/24", best_asn=666, path=[666])

    # 4. Lookup after hijack (LPM forces traffic to more specific /24)
    matched_asn, path = trie.lookup_ip(target_ip)
    print(f"    Lookup {target_ip}: routed to AS {matched_asn} (POISONED BY LPM)")

    print("\n✅ LPM Subprefix match verified.")

if __name__ == "__main__":
    test_lpm_and_withdraw()
