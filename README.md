
**CrowdCompute: Distributed Task Execution Framework**

CrowdCompute is a modular, plugin-based distributed computing framework that allows you to offload heavy processing tasks—like password cracking or data sorting—to a network of lightweight worker nodes.

🚀 **Key Features**
**Plugin Architecture:** Easily extendable to support new job types (e.g., ML training, rendering) by adding a Python class.
**Docker-outside-of-Docker (DooD):** Workers are lightweight Python containers that spawn specialized, ephemeral sibling containers for heavy tasks.
**Generic Coordinator:** A central server that manages job queues, file storage, and task assignment without knowing the specifics of the job logic.
**Smart Distribution:** Supports sharding input files and chaining tasks (Map -> Reduce).

📋 **Prerequisites**
Before running the project, ensure you have the following installed on all machines (Coordinator and Workers):

* Docker: Install Docker
* Docker Compose: (Usually included with Docker Desktop/Engine)
* Python 3.11+ (Optional, for running test scripts locally)

🛠️ **Setup Guide**

---

### **1. Network Configuration (Crucial for Multi-Device)**

For workers to connect to the Coordinator from different machines, they need to know the Coordinator's IP address.

**Find the Coordinator's Local IP:**
On the machine running the Coordinator, run `ifconfig` (Linux/Mac) or `ipconfig` (Windows).
Note the IP address (e.g., `192.168.1.50`).

**Configure Environment Variables:**
Create a `.env` file in `core/coordinator/` and `core/worker/` (or pass these variables at runtime).

* **Coordinator:** Set `COORDINATOR_BASE_URL` to its own public IP/URL.
* **Worker:** Set `COORDINATOR_URL` to point to the Coordinator.

---

### **2. Prepare Specialized Images (Hashcat)**

If you plan to run Password Cracking (Hashcat) jobs, you must build the optimized CPU-ready image on every worker machine once. This prevents the worker from downloading or building it during a task.

Run this script from the root of the project:

```
./build_images.sh
```

This builds a local image named `crowd-hashcat-cpu:latest` (~200MB).

---

## 🏃‍♂️ Running the System

### **Option A: Using Docker Compose (Single Machine / Dev)**

This is the easiest way to test everything on one computer.

```
docker-compose up --build
```

This starts one Coordinator and one Worker on a shared internal network (`crowd-net`).

---

### **Option B: Standalone / Distributed Mode (Multiple Machines)**

Follow these steps to run the Coordinator on one machine and Workers on others.

---

### **Step 1: Start the Coordinator (Machine A)**

**Build the image:**

```
docker build -t coordinator -f core/coordinator/Dockerfile .
```

**Run the container:**
Replace `YOUR_PUBLIC_IP` with the actual IP (e.g., `192.168.1.50`).

```
docker run --rm \
  --name coordinator \
  -p 8000:8000 \
  -v "$(pwd)/file_storage:/app/file_storage" \
  -e COORDINATOR_BASE_URL="http://YOUR_PUBLIC_IP:8000" \
  coordinator
```

---

### **Step 2: Start a Worker (Machine B, C, D...)**

**Build the images (First time only):**

```
# 1. Build the base worker image
docker build -t worker -f core/worker/Dockerfile .

# 2. Build the plugin dependency image (Hashcat)
./build_images.sh
```

**Run the Worker:**
Replace `http://192.168.1.50:8000` with your Coordinator's URL.

Critical Flags:
`-v /var/run/docker.sock:/var/run/docker.sock`: Gives the worker control over the host's Docker daemon (required for plugins).
`-v crowdcompute_hashcat_cache:/root/.hashcat`: Optional but recommended for performance caching.

```
docker run --rm \
  --name worker \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v crowdcompute_hashcat_cache:/root/.hashcat \
  -e COORDINATOR_URL="http://192.168.1.50:8000" \
  worker
```

---

## 🧪 Submitting Jobs

You can submit jobs using the provided Python scripts in the root directory or via the API directly.

---

### **1. Password Cracking (Hashcat)**

Crack a password using a distributed wordlist attack.

**Create a demo wordlist:**

```
echo "password123" > demo_wordlist.txt
echo "secret" >> demo_wordlist.txt
echo "hashcat" >> demo_wordlist.txt  # The target!
echo "admin" >> demo_wordlist.txt
```

**Run the submission script:**
Ensure you have the `requests` library installed (`pip install requests`).

```
# Edit submit_hashcat.py to set COORDINATOR_URL="http://192.168.1.50:8000"
python submit_hashcat.py
```

---

### **2. Distributed Sorting (MapReduce)**

Sort a large text file by splitting it among workers.

```
python test.py sort_map demo_wordlist.txt --chunks 4
```

---

📂 **Project Structure**

```
core/coordinator/: The FastAPI server managing tasks.
core/worker/: The generic worker agent.
core/plugins/:
    hashcat.py: Logic for password cracking tasks.
    sort_map.py: Logic for sorting chunks of data.
    sort_reduce.py: Logic for merging sorted chunks.
core/images/: Custom Dockerfiles for plugins (e.g., hashcat-cpu).
file_storage/: Local storage for uploaded files and results.
```

---

⚠️ **Troubleshooting**

* **Worker can't connect?**
  Check if the Coordinator's IP is reachable from the Worker machine (`ping 192.168.1.50`).
  Ensure port 8000 is open in the firewall.

* **"Image not found" error?**
  You forgot to run `./build_images.sh` on the worker machine.

* **"Connection aborted / FileNotFoundError"?**
  You forgot to mount the Docker socket (`-v /var/run/docker.sock:/var/run/docker.sock`) when running the worker.

