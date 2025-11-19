from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from fastapi import UploadFile 

class Capabilities(BaseModel):
    cpus: int
    ram_gb: int = Field(alias='ram_gb')
    gpus: int

class RegisterForm(BaseModel):
    name: str
    capabilities: Capabilities

class TaskPayload(BaseModel):
    job_type: str  
    input_files: Dict[str, str] 
    output_path: str 
    params: Dict[str, Any] 

class Task(BaseModel):
    task_id: str
    job_id: str
    payload: TaskPayload

class BasePlugin(ABC):

    @staticmethod
    @abstractmethod
    def get_job_type() -> str:
        pass

    @staticmethod
    @abstractmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str, 
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile], 
        params: Dict[str, Any] 
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        pass

    @staticmethod
    @abstractmethod
    def execute_task(
        local_input_files: Dict[str, str], 
        local_output_dir: str,
        params: Dict[str, Any] # <--- MANDATORY NOW
    ) -> (tuple[bool, str]):
        """
        Executes a task.
        :param params: Dictionary of parameters (e.g., hash_mode, learning_rate). 
                       Plugins that don't use params can simply ignore this argument.
        """
        pass

    @staticmethod
    @abstractmethod
    def on_task_complete(
        task: Task,
        job_status: Dict[str, Any],
        tasks_queue: Dict[str, Task],
        coordinator_base_url: str
    ) -> None:
        pass