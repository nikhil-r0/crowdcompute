import asyncio
from dotenv import load_dotenv
load_dotenv()
import sys
from schema import RegisterForm, Capabilities, BasePlugin #type:ignore
import requests
import json
import time
import os
import shutil
from typing import Dict, Type, Optional

# Plugins
from plugins.sort_map import SortMapPlugin
from plugins.sort_reduce import SortReducePlugin
from plugins.hashcat import HashcatPlugin

# Config
CAPABILITIES = Capabilities(cpus=4, ram_gb=16, gpus=1)
WORKER_NAME = "Worker-1"
REGISTER_FORM = RegisterForm(name=WORKER_NAME, capabilities=CAPABILITIES)
COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://localhost:8000")
Worker_id = ""
POLL_INTERVAL_NO_TASK = 5
POLL_INTERVAL_AFTER_TASK = 1
WORKER_DATA_DIR = "worker_data"

PLUGIN_REGISTRY: Dict[str, Type[BasePlugin]] = {}

def register_plugins():
    print("Registering plugins...")
    plugins = [SortMapPlugin, SortReducePlugin, HashcatPlugin] 
    for p in plugins:
        PLUGIN_REGISTRY[p.get_job_type()] = p
        print(f"  Registered '{p.get_job_type()}' -> {p.__name__}")

async def register_worker():
    try:
        request = requests.post(f"{COORDINATOR_URL}/register", json=REGISTER_FORM.model_dump())
        request.raise_for_status()
        global Worker_id
        Worker_id = request.json()["worker_id"]
        print(f"Registered with ID: {Worker_id}")
        return True
    except Exception as e: 
        print(f"Registration failed: {e}")
        return False

async def get_task() -> Optional[dict]:
    """
    Fetches a task. Returns the task dictionary if one exists, 
    or None if the queue is empty or an error occurred.
    """
    try:
        r = requests.post(f"{COORDINATOR_URL}/get-task", params={"worker_id": Worker_id})
        
        if r.status_code == 200:
            data = r.json()
            # Encapsulation: Only return if it's actually a task
            if "task" in data:
                return data
            
            # If we get here, it's a 200 OK but "No tasks available"
            return None
            
        return None
    except Exception as e: 
        print(f"Error polling for task: {e}")
        return None

async def release_task(task_id):
    try: requests.post(f"{COORDINATOR_URL}/release-task", params={"worker_id": Worker_id, "task_id": task_id})
    except: pass

async def download_file(url, path):
    try:
        r = requests.get(url, stream=True)
        r.raise_for_status()
        with open(path, 'wb') as f:
             for chunk in r.iter_content(chunk_size=8192): f.write(chunk)
        return True
    except Exception as e:
        print(f"Download error: {e}")
        return False

async def upload_file(url, path):
    try:
        with open(path, 'rb') as f:
            requests.post(url, files={'file': (os.path.basename(path), f)})
        return True
    except Exception as e:
        print(f"Upload error: {e}")
        return False

async def process_task(task: dict):
    task_id = task['task_id']
    payload = task['payload']
    job_type = payload.get('job_type', 'unknown')
    params = payload.get('params', {}) 
    
    task_dir = os.path.join(WORKER_DATA_DIR, task_id)
    if os.path.exists(task_dir): shutil.rmtree(task_dir)
    os.makedirs(task_dir)
    
    print(f"\n--- [TASK {task_id}] Job: {job_type} ---")

    local_input_files = {}
    download_success = True
    for name, url in payload.get('input_files', {}).items():
        local_path = os.path.join(task_dir, name)
        if await download_file(url, local_path):
            local_input_files[name] = local_path
        else:
            download_success = False
            break
    
    if not download_success:
        shutil.rmtree(task_dir)
        return 

    local_result_path = "" 
    
    if job_type in PLUGIN_REGISTRY:
        plugin = PLUGIN_REGISTRY[job_type]
        try:
            success, result_path = plugin.execute_task(
                local_input_files=local_input_files,
                local_output_dir=task_dir,
                params=params 
            )
            if success:
                local_result_path = result_path
        except Exception as e:
            print(f"  Plugin execution error: {e}")
    else:
        print(f"  Unknown job_type: {job_type}")

    if local_result_path and os.path.exists(local_result_path):
        upload_url = payload.get('output_path')
        if upload_url:
            await upload_file(upload_url, local_result_path)

    try: shutil.rmtree(task_dir)
    except: pass
    print(f"--- [TASK COMPLETE] ---\n")

async def main_loop():
    os.makedirs(WORKER_DATA_DIR, exist_ok=True)
    if not await register_worker(): return

    while True:
        # CLEAN LOOP: The logic is now much simpler to read
        task_data = await get_task()
        
        if task_data:
            task_object = task_data['task']
            await process_task(task_object)
            await release_task(task_object['task_id'])
            await asyncio.sleep(POLL_INTERVAL_AFTER_TASK)
        else:
            await asyncio.sleep(POLL_INTERVAL_NO_TASK)

if __name__ == "__main__":
    register_plugins()
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        print("Worker stopped.")