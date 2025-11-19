#!/bin/bash

echo "Building CrowdCompute Worker Images..."

# Build the CPU-optimized Hashcat image
echo "[1/1] Building crowd-hashcat-cpu..."
docker build -t crowd-hashcat-cpu -f core/images/hashcat-cpu/Dockerfile .

echo "-----------------------------------"
echo "Build complete! Images are ready for the Worker."