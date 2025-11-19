import os
from dotenv import load_dotenv
load_dotenv()
import sys
import uuid
import shutil
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException, UploadFile, File, Form, Request
from fastapi.responses import FileResponse
from starlette.staticfiles import StaticFiles
from schema import RegisterForm, TaskPayload, Task, BasePlugin # type: ignore
from typing import Dict, Type, Optional, Any

# Import plugins
from plugins.sort_map import SortMapPlugin
from plugins.sort_reduce import SortReducePlugin
from plugins.hashcat import HashcatPlugin 

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

# --- Coordinator Plugin Registry ---
PLUGIN_REGISTRY: Dict[str, Type[BasePlugin]] = {}

def register_plugins():
    print("Registering plugins in Coordinator...")
    plugins_to_register = [SortMapPlugin, SortReducePlugin, HashcatPlugin] 
    
    for plugin_class in plugins_to_register:
        job_type = plugin_class.get_job_type()
        PLUGIN_REGISTRY[job_type] = plugin_class
        print(f"  Registered: '{job_type}' -> {plugin_class.__name__}")

register_plugins()

# Get base URL
COORDINATOR_BASE_URL = os.environ.get("COORDINATOR_BASE_URL", os.environ.get("NOT_TEST_URL", "http://localhost:8000"))
print(f"Coordinator public URL set to: {COORDINATOR_BASE_URL}")

# The lease duration for a task
lease_time = timedelta(seconds=15)

# --- Task Endpoints ---
@app.get("/")
def root():
    return {"message": "CrowdCompute Coordinator Running..."}

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
    Releases a task. Uses the Plugin Registry to handle completion logic generically.
    """
    if task_id not in assigned_tasks:
        raise HTTPException(status_code=404, detail="Task not found or already released")
    
    assigned_task_info = assigned_tasks[task_id]
    if assigned_task_info['worker_id'] != worker_id:
        raise HTTPException(status_code=403, detail="Worker not authorized to release this task")

    # Get task object before deleting
    task_object: Task = assigned_task_info['task']
    task_job_type = task_object.payload.job_type

    # Remove from assigned
    del assigned_tasks[task_id]
    print(f"Worker {worker_id} released task {task_id}")

    # --- Generic Plugin Completion Logic ---
    if task_job_type in PLUGIN_REGISTRY:
        plugin = PLUGIN_REGISTRY[task_job_type]
        try:
            plugin.on_task_complete(
                task=task_object,
                job_status=job_status,
                tasks_queue=tasks_queue,
                coordinator_base_url=COORDINATOR_BASE_URL
            )
        except Exception as e:
            print(f"Error in plugin {plugin.__name__}.on_task_complete: {e}")
    else:
        print(f"Warning: No plugin found for job_type '{task_job_type}'")
    
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

@app.post("/submit-job/{job_type}")
async def submit_job(
    job_type: str, 
    request: Request,
    file: UploadFile = File(...) # <--- Let FastAPI handle the file extraction
):
    """
    GENERIC JOB SUBMISSION ENDPOINT.
    Accepts any form data and passes it as 'params' to the plugin.
    """
    if job_type not in PLUGIN_REGISTRY:
        raise HTTPException(status_code=404, detail=f"Job type '{job_type}' not found.")
    
    plugin = PLUGIN_REGISTRY[job_type]
    
    # --- Dynamic Form Parsing ---
    try:
        form_data = await request.form()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid form data: {e}")

    # Extract All Other Fields as Params
    params: Dict[str, Any] = {}
    for key, value in form_data.items():
        if key != "file": # Skip the file, we already have it in 'file' argument
            # Convert string values if needed, or keep as strings
            params[key] = value

    # --- Job Creation Logic ---
    job_id = f"{job_type}_{str(uuid.uuid4())[:8]}"
    job_dir = os.path.join(JOBS_DIR, job_id)
    os.makedirs(job_dir, exist_ok=True)
    
    print(f"Received new job {job_id} for plugin {plugin.__name__}")
    print(f"  Params: {params}")

    try:
        # Call the plugin
        task_payloads, initial_status = plugin.create_job_tasks(
            job_id=job_id,
            job_dir=job_dir,
            coordinator_base_url=COORDINATOR_BASE_URL,
            uploaded_file=file,
            params=params
        )
        
        # Queue tasks
        for payload in task_payloads:
            task_id = str(uuid.uuid4())
            payload.output_path = f"{COORDINATOR_BASE_URL}/upload/{job_id}/{task_id}"
            task = Task(task_id=task_id, job_id=job_id, payload=payload)
            tasks_queue[task.task_id] = task
        
        job_status[job_id] = initial_status
        print(f"New job {job_id} created with {len(task_payloads)} initial tasks.")
        
        return {
            "message": "Job submitted successfully",
            "job_id": job_id,
            "tasks_created": len(task_payloads)
        }

    except NotImplementedError:
         shutil.rmtree(job_dir)
         raise HTTPException(status_code=400, detail=f"Plugin '{job_type}' does not support job creation via file upload.")
    except ValueError as e:
        shutil.rmtree(job_dir)
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(f"Failed to create job {job_id}: {e}")
        shutil.rmtree(job_dir)
        raise HTTPException(status_code=500, detail=f"Failed to create job: {e}")

@app.post("/upload/{job_id}/{task_id}")
async def upload_task_result(job_id: str, task_id: str, file: UploadFile = File(...)):
    """
    Saves uploaded results and updates the job status map if applicable.
    """
    job_results_dir = os.path.join(RESULTS_DIR, job_id)
    os.makedirs(job_results_dir, exist_ok=True)
    
    output_filename = f"{task_id}_{file.filename or 'result.dat'}"
    save_path = os.path.join(job_results_dir, output_filename)
    
    try:
        with open(save_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        print(f"Received result for task {task_id}")

        if job_id in job_status:
            file_url = f"{COORDINATOR_BASE_URL}/data/results/{job_id}/{output_filename}"
            if "map_results" in job_status[job_id]:
                job_status[job_id]["map_results"].append(file_url)
            elif "results" in job_status[job_id]:
                job_status[job_id]["results"].append(file_url)

        return {
            "message": "File uploaded successfully",
            "saved_path": save_path
        }
    except Exception as e:
        print(f"Error saving file for task {task_id}: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to save file: {e}")
    finally:
        file.file.close()