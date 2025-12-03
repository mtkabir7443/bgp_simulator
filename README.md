cat <<EOF > README.md
# CSE3150 BGP Simulator - Ludicrous Edition

**Author:** Mohammed Kabir
**Environment:** WSL2 (Ubuntu), VS Code

## Optimization Highlights
1. **Memory Arena:** Custom malloc-based arena to avoid mmap thrashing during high-volume IO.
2. **AVX2 Intrinsics:** Used for rapid path loop detection.
3. **Pre-allocation:** Massive reservation of vectors (degree * 64) to eliminate system time spent on reallocations.
4. **IO Optimization:** \`ftruncate\` used on output files to prevent metadata update overhead during writes.

## Compilation
To compile with full optimizations:
\`\`\`bash
g++ -O3 -march=native -pthread -o bgp_simulator main.cpp
\`\`\`

## Execution
\`\`\`bash
./bgp_simulator --relationships bench/many/CAIDAASGraphCollector_2025.10.16.txt --announcements bench/many/anns.csv --rov-asns bench/many/rov_asns.csv
\`\`\`
EOF