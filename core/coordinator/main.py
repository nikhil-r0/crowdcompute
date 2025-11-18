import os
from dotenv import load_dotenv
load_dotenv()
import sys
import uuid
import shutil
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from fastapi.responses import FileResponse
from starlette.staticfiles import StaticFiles
from schema import RegisterForm, TaskPayload, Task, BasePlugin # type: ignore
from typing import Dict, Type

# Import the new, split plugins
from plugins.sort_map import SortMapPlugin
from plugins.sort_reduce import SortReducePlugin

# --- FastAPI App Initialization ---
app = FastAPI()

# --- File Storage Setup ---
STORAGE_DIR = "file_storage"
JOBS_DIR = os.path.join(STORAGE_DIR, "jobs")
RESULTS_DIR = os.path.join(STORAGE_DIR, "results")
os.makedirs(JOBS_DIR, exist_ok=True)
os.makedirs(RESULTS_DIR, exist_ok=True)
app.mount("/data", StaticFiles(directory=STORAGE_DIR), name="data")

# --- In-memory storage ---
registered_workers = {}
tasks_queue = {}
assigned_tasks = {}
job_status = {}

# --- NEW: Coordinator Plugin Registry ---
# Registers plugins that can CREATE jobs
JOB_CREATOR_PLUGINS: Dict[str, Type[BasePlugin]] = {}

def register_job_creator_plugins():
    """
    Registers plugins that can be used to initiate jobs.
    """
    print("Registering job creator plugins...")
    # Add plugins that can start a job (e.g., SortMap, but not SortReduce)
    plugins_to_register = [SortMapPlugin] 
    
    for plugin_class in plugins_to_register:
        job_type = plugin_class.get_job_type()
        if job_type in JOB_CREATOR_PLUGINS:
            print(f"Warning: Duplicate job_type '{job_type}' found. Overwriting.")
        JOB_CREATOR_PLUGINS[job_type] = plugin_class
        print(f"  Registered job creator: '{job_type}' -> {plugin_class.__name__}")

register_job_creator_plugins()
# --- End Plugin Registry ---


# Get base URL from environment variable
# This is the PUBLIC address workers will use to reach the coordinator
COORDINATOR_BASE_URL = os.environ.get("COORDINATOR_BASE_URL", os.environ.get("NOT_TEST_URL", "http://localhost:8000"))
print(f"Coordinator public URL set to: {COORDINATOR_BASE_URL}")


# --- DEMO HASHCAT TASK (Unchanged) ---
demo_job_id = "demo_job_123"
demo_task_id = str(uuid.uuid4())
demo_job_dir = os.path.join(JOBS_DIR, demo_job_id)
os.makedirs(demo_job_dir, exist_ok=True)
hash_file_path = os.path.join(demo_job_dir, "hashes.txt")
with open(hash_file_path, "w") as f:
    f.write("8743b52063cd84097a65d1633f5c74f5\n") # "hashcat"
wordlist_file_path = os.path.join(demo_job_dir, "wordlist_chunk_01.txt")
with open(wordlist_file_path, "w") as f:
    f.write("hello\nworld\nhashcat\n")

demo_task_payload = TaskPayload(
    job_type="hashcat_demo",
    input_files={
        "hashes": f"{COORDINATOR_BASE_URL}/data/jobs/{demo_job_id}/hashes.txt",
        "wordlist": f"{COORDINATOR_BASE_URL}/data/jobs/{demo_job_id}/wordlist_chunk_01.txt"
    },
    output_path=f"{COORDINATOR_BASE_URL}/upload/{demo_job_id}/{demo_task_id}",
    params={"hashcat_mode": "0", "options": "-a 0 -O"}
)
demo_task = Task(
    task_id=demo_task_id,
    job_id=demo_job_id,
    payload=demo_task_payload
)
tasks_queue[demo_task.task_id] = demo_task
# --- END DEMO HASHCAT TASK ---


# The lease duration for a task
lease_time = timedelta(seconds=15)

# --- Task Endpoints ---
@app.get("/")
def root():
    return {"message": "Task and File Server Running..."}

@app.post("/register")
def register_worker(data: RegisterForm):
    worker_id = str(uuid.uuid4())
    registered_workers[worker_id] = data
    print(f"Registered new worker: {worker_id} with name {data.name}")
    return {"worker_id": worker_id, "status": "registered"}

@app.post("/get-task")
def assign_task(worker_id: str):
    if worker_id not in registered_workers:
        raise HTTPException(status_code=404, detail="Worker not found")
    if not tasks_queue:
        return {"message": "No tasks available."}

    task_id, task = next(iter(tasks_queue.items()))
    del tasks_queue[task_id]
    lease_expires_at = datetime.now(timezone.utc) + lease_time
    assigned_tasks[task_id] = {
        "task": task, # Store the full task object
        "worker_id": worker_id,
        "lease_expires": lease_expires_at
    }
    print(f"Assigned task {task_id} to worker {worker_id}")
    return {"task": task, "lease_expires": lease_expires_at}

@app.post("/release-task")
def release_task(worker_id: str, task_id: str):
    """
    MODIFIED: Now checks for job completion to trigger reduce tasks.
    """
    if task_id not in assigned_tasks:
        raise HTTPException(status_code=404, detail="Task not found or already released")
    
    assigned_task_info = assigned_tasks[task_id]
    if assigned_task_info['worker_id'] != worker_id:
        raise HTTPException(status_code=403, detail="Worker not authorized to release this task")

    # Get job info *before* deleting from assigned_tasks
    task_object = assigned_task_info['task']
    job_id = task_object.job_id
    task_job_type = task_object.payload.job_type

    # Remove from assigned
    del assigned_tasks[task_id]
    print(f"Worker {worker_id} released task {task_id}")

    # --- Job Chaining Logic ---
    if job_id in job_status:
        current_job = job_status[job_id]
        
        if task_job_type == "sort_map":
            current_job["completed_tasks"] += 1
            print(f"Sort job {job_id} progress: {current_job['completed_tasks']}/{current_job['total_tasks']} map tasks complete.")
            
            # Check if all map tasks for this job are done
            if current_job["completed_tasks"] == current_job["total_tasks"]:
                print(f"All map tasks for {job_id} complete. Creating reduce task...")
                
                # Create the single reduce task
                reduce_task_id = str(uuid.uuid4())
                reduce_payload = TaskPayload(
                    job_type="sort_reduce", # This job_type will be picked up by the worker
                    # Pass all the sorted chunk URLs as input
                    input_files={
                        f"chunk_{i}": url for i, url in enumerate(current_job["map_results"])
                    },
                    # Final output will be uploaded here
                    output_path=f"{COORDINATOR_BASE_URL}/upload/{job_id}/{reduce_task_id}",
                    params={}
                )
                reduce_task = Task(
                    task_id=reduce_task_id,
                    job_id=job_id,
                    payload=reduce_payload
                )
                
                tasks_queue[reduce_task.task_id] = reduce_task
                print(f"Queued reduce task {reduce_task.task_id} for job {job_id}")

        elif task_job_type == "sort_reduce":
            # The final step is done
            print(f"--- SORT JOB {job_id} COMPLETE ---")
            del job_status[job_id] # Clean up job tracker
    
    return {"message": f"Task {task_id} released successfully."}

@app.get("/tasks")
def get_all_tasks():
    return {
        "queued_tasks_count": len(tasks_queue),
        "assigned_tasks_count": len(assigned_tasks),
        "queued_tasks": tasks_queue,
        "assigned_tasks": assigned_tasks,
        "job_status": job_status
    }

# --- NEW: /submit-job ENDPOINT ---
@app.post("/submit-job/{job_type}")
async def submit_job(
    job_type: str,
    file: UploadFile = File(...),
    num_chunks: int = Form(10) # Example of an extra parameter
):
    """
    Endpoint to submit a new job with a file.
    """
    if job_type not in JOB_CREATOR_PLUGINS:
        raise HTTPException(status_code=404, detail=f"Job type '{job_type}' not found or cannot create jobs.")
    
    plugin = JOB_CREATOR_PLUGINS[job_type]
    job_id = f"{job_type}_{str(uuid.uuid4())[:8]}"
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    print(f"Received new job {job_id} for plugin {plugin.__name__}")

    try:
        # 1. Call the plugin's create_job_tasks method
        task_payloads, initial_status = plugin.create_job_tasks(
            job_id=job_id,
            job_dir=job_dir,
            coordinator_base_url=COORDINATOR_BASE_URL,
            uploaded_file=file,
            params={"num_chunks": num_chunks}
        )
        
        # 2. Add tasks to queue and set job status
        for payload in task_payloads:
            task_id = str(uuid.uuid4())
            # Update the placeholder output_path with the real task_id
            payload.output_path = f"{COORDINATOR_BASE_URL}/upload/{job_id}/{task_id}"
            
            task = Task(task_id=task_id, job_id=job_id, payload=payload)
            tasks_queue[task.task_id] = task
        
        job_status[job_id] = initial_status
        print(f"New job {job_id} created with {len(task_payloads)} map tasks.")
        
        return {
            "message": "Job submitted successfully",
            "job_id": job_id,
            "tasks_created": len(task_payloads)
        }

    except Exception as e:
        print(f"Failed to create job {job_id}: {e}")
        # Clean up failed job dir
        shutil.rmtree(job_dir)
        raise HTTPException(status_code=500, detail=f"Failed to create job: {e}")


# --- FILE UPLOAD ENDPOINT ---
@app.post("/upload/{job_id}/{task_id}")
async def upload_task_result(job_id: str, task_id: str, file: UploadFile = File(...)):
    """
    MODIFIED: Now updates job_status with the result file URL.
    """
    # Create the results directory for this job
    job_results_dir = os.path.join(RESULTS_DIR, job_id)
    os.makedirs(job_results_dir, exist_ok=True)
    
    output_filename = f"{task_id}_{file.filename or 'result.dat'}"
    save_path = os.path.join(job_results_dir, output_filename)
    
    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        print(f"Successfully received result for task {task_id} at {save_path}")

        # --- Job Status Update Logic ---
        if job_id in job_status:
            # We need to store the URL, not the local path
            file_url = f"{COORDINATOR_BASE_URL}/data/results/{job_id}/{output_filename}"
            job_status[job_id]["map_results"].append(file_url)
            print(f"Logged result for job {job_id}. Total results: {len(job_status[job_id]['map_results'])}")

        return {
            "message": "File uploaded successfully",
            "saved_path": save_path
        }
    except Exception as e:
        print(f"Error saving file for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
    finally:
        file.file.close()


# --- NEW: DEMO SORT JOB CREATION ---
def _create_demo_sort_job():
    """
    Creates a demo sort job on startup, replacing the /submit-sort-job endpoint.
    This simulates a user submitting a small file to be sorted.
    """
    print("Creating demo sort job...")
    job_id = "demo_sort_job_789"
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    # 1. Create a demo file to be sorted
    demo_file_path = os.path.join(job_dir, "demo_unsorted_data.txt")
    with open(demo_file_path, "w") as f:
        f.write("zebra\napple\norange\nbanana\nkiwi\ngrape\n")
    
    # 2. Use the plugin to create the tasks
    # We need to simulate the objects the plugin expects
    
    # Simulate UploadFile
    class MockUploadFile:
        def __init__(self, file_path, filename):
            self.file = open(file_path, "rb")
            self.filename = filename
        def close(self):
            self.file.close()
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()

    mock_file = MockUploadFile(demo_file_path, "demo_unsorted_data.txt")
    
    try:
        # 3. Call the SortMapPlugin's create_job_tasks method
        task_payloads, initial_status = SortMapPlugin.create_job_tasks(
            job_id=job_id,
            job_dir=job_dir,
            coordinator_base_url=COORDINATOR_BASE_URL,
            uploaded_file=mock_file,
            params={"num_chunks": 2} # Split into 2 chunks
        )
        
        # 4. Add tasks to queue and set job status
        for payload in task_payloads:
            # We assign the final ID and update the output path
            task_id = str(uuid.uuid4())
            payload.output_path = f"{COORDINATOR_BASE_URL}/upload/{job_id}/{task_id}"
            
            task = Task(task_id=task_id, job_id=job_id, payload=payload)
            tasks_queue[task.task_id] = task
        
        job_status[job_id] = initial_status
        print(f"Demo sort job created with {len(task_payloads)} map tasks.")

    except Exception as e:
        print(f"Failed to create demo sort job: {e}")
# --- REMOVED: _create_demo_sort_job ---
# This is no longer needed as we can submit jobs via the API


# --- OpenAPI Generation & Startup ---
openapi_schema = app.openapi()
import json
with open("openapi.json", "w") as f:
    json.dump(openapi_schema, f, indent=2)
