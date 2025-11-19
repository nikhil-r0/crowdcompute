# Use a small base image (Ubuntu is ~70MB)
FROM ubuntu:22.04

# Avoid prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# 1. Update and install dependencies
# - hashcat: The cracker
# - pocl-opencl-icd: The CPU OpenCL driver
# - clinfo: Utility to verify OpenCL availability
# - Removed: libpoclu-dev (not needed for runtime execution)
RUN apt-get update && apt-get install -y --no-install-recommends \
    hashcat \
    pocl-opencl-icd \
    clinfo \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Verify OpenCL installation during build
# If this fails during 'docker build', we know the image is broken.
RUN clinfo

# 3. Set entrypoint to allow command injection
ENTRYPOINT ["/bin/bash", "-c"]