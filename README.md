# **CrowdCompute: Distributed Task Execution Framework**

CrowdCompute is a modular, plugin-based distributed computing framework that allows you to offload heavy processing tasks—like password cracking or data sorting—to a network of lightweight worker nodes.

---

## 🚀 **Key Features**

* **Plugin Architecture:** Extend with new job types (ML training, rendering, etc.).
* **Docker-outside-of-Docker (DooD):** Workers spawn sibling containers for heavy tasks.
* **Generic Coordinator:** Manages job queues, storage, and scheduling.
* **Smart Distribution:** Supports sharding, parallel execution, and Map→Reduce flows.

---

## 📋 **Prerequisites**

Install the following on **all machines** (Coordinator + Workers):

* Docker
* Docker Compose
* Python 3.11+ (optional, for submitting test jobs)

---

# 🛠️ Setup Guide

---

## **1. Network Configuration (Required for Multi-Device Deployments)**

Workers must connect to the Coordinator through its IP.

### **Find the Coordinator IP**

On the machine running the Coordinator:

* macOS/Linux: `ifconfig`
* Windows: `ipconfig`

Use this IP wherever `<COORDINATOR_IP>` is shown.

---

## **2. Update `.env` Files (Coordinator + Workers)**

CrowdCompute relies on environment variables stored in `.env` or `.env.local`.

You **must** update these values on both Coordinator and Worker machines.

---

### **core/coordinator/.env or .env.local**

```
COORDINATOR_BASE_URL=http://<COORDINATOR_IP>:8000
NOT_TEST_URL=http://<COORDINATOR_IP>:8000
```

---

### **core/worker/.env or .env.local**

```
COORDINATOR_URL=http://<COORDINATOR_IP>:8000
NOT_TEST_URL=http://<COORDINATOR_IP>:8000
```

❗ **Do not leave `NOT_TEST_URL="your_coordinator_url"` unchanged — workers will not connect.**
This must be set to the actual Coordinator URL.

---

## **3. Prepare Plugin Images (Hashcat)**

Required only if you will run password-cracking jobs.

Run this once on every Worker machine:

```bash
./build_images.sh
```

This builds the optimized plugin image:

```
crowd-hashcat-cpu:latest
```

---

# 🏃‍♂️ Running the System

---

## **Option A — Docker Compose (One Machine / Dev Mode)**

Easiest way to test everything:

```bash
docker-compose up --build
```

This starts:

* 1 Coordinator
* 1 Worker
* Shared internal Docker network (`crowd-net`)

---

## **Option B — Distributed Mode (Multiple Machines)**

Deploy Coordinator and Workers on different machines in the same network.

---

# **Step 1 — Start the Coordinator (Machine A)**

### Build the image:

```bash
docker build -t coordinator -f core/coordinator/Dockerfile .
```

### Run the Coordinator:

```bash
docker run --rm \
  --name coordinator \
  -p 8000:8000 \
  -v "$(pwd)/file_storage:/app/file_storage" \
  coordinator
```

The `.env` file provides the Coordinator URL — no `-e` flags needed.

---

# **Step 2 — Start a Worker (Machine B, C, D...)**

### Build the Worker:

```bash
docker build -t worker -f core/worker/Dockerfile .
```

### (Optional) Build plugin dependency images:

```bash
./build_images.sh
```

### Run the Worker:

```bash
docker run --rm \
  --name worker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v crowdcompute_hashcat_cache:/root/.hashcat \
  worker
```

**Important Flags:**

* `-v /var/run/docker.sock:/var/run/docker.sock` → Required for plugin containers
* `-v crowdcompute_hashcat_cache:/root/.hashcat` → Optional cache for faster Hashcat runs

---

# 🧪 Submitting Jobs

---

## **1. Password Cracking (Hashcat)**

### Create a sample wordlist:

```bash
echo "password123" > demo_wordlist.txt
echo "secret" >> demo_wordlist.txt
echo "hashcat" >> demo_wordlist.txt
echo "admin" >> demo_wordlist.txt
```

### Submit a job:

In `submit_hashcat.py`, update:

```
COORDINATOR_URL="http://<COORDINATOR_IP>:8000"
```

Then run:

```bash
python submit_hashcat.py
```

---

## **2. Distributed Sorting (MapReduce)**

```bash
python test.py sort_map demo_wordlist.txt --chunks 4
```

---

# 📂 Project Structure

```
core/coordinator/       → FastAPI coordinator service
core/worker/            → Generic worker agent
core/plugins/
    hashcat.py          → Hashcat plugin
    sort_map.py         → Map step for sorting
    sort_reduce.py      → Reduce step for sorting
core/images/            → Dockerfiles for plugin containers
file_storage/           → Uploaded files + task results
```

---

# ⚠️ Troubleshooting

### Worker cannot connect

* Ensure the Worker can reach the Coordinator:
  `ping <COORDINATOR_IP>`
* Check firewall permissions for port 8000.

### “Image not found”

You forgot:

```bash
./build_images.sh
```

### Docker errors (FileNotFoundError, connection errors)

You likely forgot this mount:

```bash
-v /var/run/docker.sock:/var/run/docker.sock
```

---


