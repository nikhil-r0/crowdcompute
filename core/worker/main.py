import asyncio
import sys
sys.path.append('../common')
from schema import RegisterForm, Capabilities # type: ignore
import requests
import json

CAPABILITIES = Capabilities(cpus=4, ram_gb=16, gpus=1)
WORKER_NAME = "Worker-1"
REGISTER_FORM = RegisterForm(name=WORKER_NAME, capabilities=CAPABILITIES)
COORDINATOR_URL = "http://localhost:8000"
Worker_id = ""

async def register_worker():
    """
    Registers the worker with the coordinator.
    """
    print(f"Registering with Coordinator with Form {REGISTER_FORM}")
    print(REGISTER_FORM.model_dump_json())
    request = requests.post(f"{COORDINATOR_URL}/register",json=REGISTER_FORM.model_dump())
    print(request.content)
    global Worker_id
    Worker_id = json.loads(request.content.decode("utf-8"))["worker_id"]


async def get_task():
    """
    Gets the task from the coordinator.
    """
    print(f"Requesting task with Worker_id : {Worker_id}")
    request = requests.post(f"{COORDINATOR_URL}/get-task",params={"worker_id":Worker_id}) 
    print(request.content)   

asyncio.run(register_worker()) 
print(Worker_id)
asyncio.run(get_task())
