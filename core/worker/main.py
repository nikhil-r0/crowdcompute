import asyncio
from dotenv import load_dotenv
load_dotenv()
import sys
from schema import RegisterForm, Capabilities, BasePlugin # type: ignore
import requests
import json
import time
import os
import shutil
from typing import Dict, Type

# Import the new, split plugins
from plugins.sort_map import SortMapPlugin
from plugins.sort_reduce import SortReducePlugin

# --- Worker Configuration ---
CAPABILITIES = Capabilities(cpus=4, ram_gb=16, gpus=1)
WORKER_NAME = "Worker-1"
REGISTER_FORM = RegisterForm(name=WORKER_NAME, capabilities=CAPABILITIES)
# Get Coordinator URL from environment variable, default to localhost for local testing
COORDINATOR_URL = os.environ.get("COORDINATOR_URL",os.environ.get("NOT_TEST_URL", "http://localhost:8000"))
Worker_id = ""
POLL_INTERVAL_NO_TASK = 10
POLL_INTERVAL_AFTER_TASK = 2
WORKER_DATA_DIR = "worker_data"

# --- NEW: Plugin Registry ---
PLUGIN_REGISTRY: Dict[str, Type[BasePlugin]] = {}

def register_plugins():
    """
    Finds and registers all available plugins.
    The worker uses this to map a job_type to the correct execution code.
    The worker uses this to map a 'job_type' to the correct execution code.
    """
    print("Registering plugins...")
    # Add all your plugin classes here
    plugins_to_register = [SortMapPlugin, SortReducePlugin] 
    
    for plugin_class in plugins_to_register:
        job_type = plugin_class.get_job_type() # Simplified
        if job_type in PLUGIN_REGISTRY:
            print(f"Warning: Duplicate job_type '{job_type}' found. Overwriting.")
        PLUGIN_REGISTRY[job_type] = plugin_class
        print(f"  Registered '{job_type}' -> {plugin_class.__name__}")

# --- Worker Functions (register, get_task, release_task, download, upload) ---
# (These functions are unchanged from the previous version)

async def register_worker():
    """ Registers the worker with the coordinator. """
    print(f"Registering with Coordinator: {REGISTER_FORM.model_dump_json()}")
    try:
        request = requests.post(f"{COORDINATOR_URL}/register", json=REGISTER_FORM.model_dump())
        request.raise_for_status()
        global Worker_id
        Worker_id = request.json()["worker_id"]
        print(f"Successfully registered. Worker ID: {Worker_id}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"Failed to register worker: {e}")
        return False

async def get_task():
    """ Gets a task from the coordinator. """
    print("Polling for task...")
    try:
        request = requests.post(f"{COORDINATOR_URL}/get-task", params={"worker_id": Worker_id})
        request.raise_for_status()
        response_data = request.json()
        
        if "task" in response_data:
            return response_data
        else:
            return None
    except requests.exceptions.RequestException as e:
        print(f"Failed to get task: {e}")
        return None

async def release_task(task_id: str):
    """ Releases a completed task. """
    print(f"Releasing task {task_id}...")
    try:
        request = requests.post(
            f"{COORDINATOR_URL}/release-task",
            params={"worker_id": Worker_id, "task_id": task_id}
        )
        request.raise_for_status()
        print(f"Successfully released task {task_id}: {request.json()}")
    except requests.exceptions.RequestException as e:
        print(f"Failed to release task {task_id}: {e}")

async def download_file(url: str, save_path: str):
    """ Downloads a file from a URL. """
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(save_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        print(f"  Downloaded {url} to {save_path}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  Failed to download {url}: {e}")
        return False

async def upload_file(url: str, file_path: str):
    """ Uploads a file to a URL. """
    try:
        with open(file_path, 'rb') as f:
            files = {'file': (os.path.basename(file_path), f)}
            r = requests.post(url, files=files)
            r.raise_for_status()
        print(f"  Uploaded {file_path} to {url}")
        return True
    except requests.exceptions.RequestException as e:
        print(f"  Failed to upload {file_path}: {e}")
        return False

# --- Task Processing Logic (REFACTORED) ---

async def process_task(task: dict):
    """
    REFACTORED: This function now routes tasks to plugins
    based on the PLUGIN_REGISTRY.
    """
    task_id = task['task_id']
    payload = task['payload']
    job_type = payload.get('job_type', 'unknown')
    
    # 1. Create a temporary working directory for this task
    task_dir = os.path.join(WORKER_DATA_DIR, task_id)
    if os.path.exists(task_dir):
        shutil.rmtree(task_dir)
    os.makedirs(task_dir)
    
    print(f"\n--- [TASK {task_id} RECEIVED] ---")
    print(f"  Job Type: {job_type}")
    print(f"  Created temp dir: {task_dir}")

    # 2. Download all input files
    local_input_files = {}
    download_success = True
    for name, url in payload.get('input_files', {}).items():
        # Use a predictable name based on the key (e.g., 'data', 'hashes')
        local_path = os.path.join(task_dir, name)
        if await download_file(url, local_path):
            local_input_files[name] = local_path
        else:
            print(f"  Failed to get input file {name}. Aborting task.")
            download_success = False
            break
    
    if not download_success:
        shutil.rmtree(task_dir) # Clean up
        return # Skip this task (we should implement a failure report)

    # 3. Execute work based on job_type
    local_result_path = "" # Path to the file we will upload
    
    if job_type in PLUGIN_REGISTRY:
        # --- Plugin-based execution ---
        plugin = PLUGIN_REGISTRY[job_type]
        print(f"  Routing task to plugin: {plugin.__name__}")
        try:
            # Pass params to execute_task (simplified)
            success, result_path = plugin.execute_task(
                local_input_files=local_input_files,
                local_output_dir=task_dir
            )
            if success:
                local_result_path = result_path
            else:
                print(f"  Plugin {plugin.__name__} failed task.")
        except Exception as e:
            print(f"  Plugin {plugin.__name__} raised an exception: {e}")

    elif job_type == 'hashcat_demo':
        # --- Legacy Hashcat Demo Logic ---
        # (This can be moved to its own plugin later)
        print("  Simulating hashcat command...")
        cmd = (
            f"  hashcat -m {payload.get('params', {}).get('hashcat_mode', '?')} "
            f"-o {os.path.join(task_dir, 'result.pot')} "
            f"{local_input_files.get('hashes', '?')} "
            f"{local_input_files.get('wordlist', '?')}"
        )
        print(cmd)
        await asyncio.sleep(2) # Shorter simulation
        
        # Create dummy result
        local_result_path = os.path.join(task_dir, "demo_result.pot")
        with open(local_result_path, "w") as f:
            f.write("8743b52063cd84097a65d1633f5c74f5:hashcat\n")
        print(f"  Created dummy result at {local_result_path}")
        
    else:
        print(f"  Unknown job_type: {job_type}. No plugin registered.")

    # 4. Upload the result (if one was created)
    if local_result_path and os.path.exists(local_result_path):
        upload_url = payload.get('output_path')
        if upload_url:
            await upload_file(upload_url, local_result_path)
        else:
            print("  No output_path defined. Skipping upload.")
    else:
        print("  No result file generated. Skipping upload.")

    # 5. Clean up the task directory
    try:
        shutil.rmtree(task_dir)
        print(f"  Cleaned up temp dir: {task_dir}")
    except Exception as e:
        print(f"  Failed to clean up temp dir {task_dir}: {e}")

    print(f"--- [TASK {task_id} COMPLETE] ---\n")

async def main_loop():
    """ Main worker loop. """
    os.makedirs(WORKER_DATA_DIR, exist_ok=True)
    if not await register_worker():
        print("Registration failed. Exiting.")
        return

    while True:
        task_data = await get_task()
        if task_data:
            task_object = task_data['task']
            await process_task(task_object)
            await release_task(task_object['task_id'])
            await asyncio.sleep(POLL_INTERVAL_AFTER_TASK)
        else:
            print(f"No tasks available. Sleeping for {POLL_INTERVAL_NO_TASK} seconds...")
            await asyncio.sleep(POLL_INTERVAL_NO_TASK)

if __name__ == "__main__":
    try:
        # Register plugins on worker startup
        register_plugins()
        print("Starting worker...")
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("\nWorker shutting down. Cleaning up...")
        if os.path.exists(WORKER_DATA_DIR):
            shutil.rmtree(WORKER_DATA_DIR)
            print(f"Removed {WORKER_DATA_DIR}")