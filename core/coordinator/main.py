import os
import sys
import uuid
from datetime import datetime, timedelta, timezone
from fastapi import FastAPI, HTTPException
sys.path.append('..')
from schema import  RegisterForm, TaskPayload, Task

# --- FastAPI App Initialization ---
app = FastAPI()


# In-memory storage for workers and tasks.
# Using dictionaries provides easy access and modification by ID.
registered_workers = {}
tasks_queue = {}
assigned_tasks = {}

# A single demo task to get started
demo_task = Task(task_id=str(uuid.uuid4()), payload=TaskPayload(message="print 'Hello world!'"))
tasks_queue[demo_task.task_id] = demo_task

# The lease duration for a task
lease_time = timedelta(seconds=15)

# --- Endpoints ---
@app.get("/")
def root():
    """
    Simple root endpoint to confirm the server is running.
    """
    return {"message": "Task Server Running..."}

@app.post("/register")
def register_worker(data: RegisterForm):
    """
    Registers a new worker and assigns a unique ID.
    """
    worker_id = str(uuid.uuid4())
    registered_workers[worker_id] = data
    print(f"Registered new worker: {worker_id} with name {data.name}")
    return {
        "worker_id": worker_id,
        "status": "registered"
    }

@app.post("/get-task")
def assign_task(worker_id: str):
    """
    Assigns a task to a worker if one is available.
    """
    if worker_id not in registered_workers:
        raise HTTPException(status_code=404, detail="Worker not found")

    if not tasks_queue:
        return {"message": "No tasks available."}

    # Get the first task from the queue
    task_id, task = next(iter(tasks_queue.items()))
    del tasks_queue[task_id] # Remove from the queue

    # Assign the task and set the lease expiration time
    lease_expires_at = datetime.now(timezone.utc) + lease_time
    assigned_tasks[task_id] = {
        "task": task,
        "worker_id": worker_id,
        "lease_expires": lease_expires_at
    }

    print(f"Assigned task {task_id} to worker {worker_id}")

    return {
        "task": task,
        "lease_expires": lease_expires_at
    }

@app.post("/release-task")
def release_task(worker_id: str, task_id: str):
    """
    Allows a worker to release a task after it's completed.
    """
    if task_id not in assigned_tasks:
        raise HTTPException(status_code=404, detail="Task not found or already released")
    
    assigned_task = assigned_tasks[task_id]
    if assigned_task['worker_id'] != worker_id:
        raise HTTPException(status_code=403, detail="Worker not authorized to release this task")

    # The task is considered complete and is removed from memory
    del assigned_tasks[task_id]
    print(f"Worker {worker_id} released task {task_id}")
    
    return {"message": f"Task {task_id} released successfully."}

@app.get("/tasks")
def get_all_tasks():
    """
    Returns the current state of tasks (queued and assigned).
    """
    return {
        "queued_tasks_count": len(tasks_queue),
        "assigned_tasks_count": len(assigned_tasks),
        "queued_tasks": tasks_queue,
        "assigned_tasks": assigned_tasks
    }



openapi_schema = app.openapi()
# You can then save this dictionary to a JSON file
import json
with open("openapi.json", "w") as f:
    json.dump(openapi_schema, f, indent=2)
