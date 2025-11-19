# Use a small base image (Ubuntu is ~70MB)
FROM ubuntu:22.04

# Avoid prompts during install
ENV DEBIAN_FRONTEND=noninteractive

# 1. Update and install dependencies
# - hashcat: The cracker
# - pocl-opencl-icd: The CPU OpenCL driver
# - --no-install-recommends: Avoids installing heavy extras like docs/themes
RUN apt-get update && apt-get install -y --no-install-recommends \
    hashcat \
    pocl-opencl-icd \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# 2. Verify installation (useful debug step during build)
RUN hashcat --version

# 3. Set entrypoint to allow command injection
ENTRYPOINT ["/bin/bash", "-c"]