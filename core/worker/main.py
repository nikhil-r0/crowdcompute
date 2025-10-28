import asyncio
import sys
sys.path.append("..")
from schema import RegisterForm, Capabilities
import requests

CAPABILITIES = Capabilities(cpus=4, ram_gb=16, gpus=1)
WORKER_NAME = "Worker-1"
REGISTER_FORM = RegisterForm(name=WORKER_NAME, capabilities=CAPABILITIES)
COORDINATOR_URL = "http://localhost:8000"

async def register_worker():
    """
    Registers the worker with the coordinator.
    """
    print(f"Registering with Coordinator with Form {REGISTER_FORM}")
    print(REGISTER_FORM.model_dump_json())
    request = requests.post(f"{COORDINATOR_URL}/register",json=REGISTER_FORM.model_dump())
    print(request.content)

asyncio.run(register_worker()) 