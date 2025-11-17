from pydantic import BaseModel, Field
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from fastapi import UploadFile # Used in BasePlugin signature

# --- Pydantic Models ---
class Capabilities(BaseModel):
    """
    Defines the compute capabilities of a worker.
    The ram_gb field is aliased for proper serialization.
    """
    cpus: int
    ram_gb: int = Field(alias='ram_gb')
    gpus: int

class RegisterForm(BaseModel):
    """
    Model for the data submitted during worker registration.
    """
    name: str
    capabilities: Capabilities

class TaskPayload(BaseModel):
    """
    Model for a data processing task.
    Replaces the old simple 'message' payload.
    """
    job_type: str  # e.g., 'sort', 'hashcat_demo'
    input_files: Dict[str, str] # e.g., {"hashes": "/path/to/hashes.txt"}
    output_path: str # e.g., "/path/to/results.pot"
    params: Dict[str, Any] # e.g., {"hashcat_mode": "0"}

class Task(BaseModel):
    """
    Model representing a single task.
    Now includes a job_id to group related tasks.
    """
    task_id: str
    job_id: str
    payload: TaskPayload

# --- NEW: Plugin Base Class (SIMPLIFIED) ---

class BasePlugin(ABC):
    """
    Abstract Base Class for all CrowdCompute plugins.
    Defines the standard interface for:
    1. Coordinator: Creating tasks from a new job submission.
    2. Worker: Executing a specific task.
    """

    @staticmethod
    @abstractmethod
    def get_job_type() -> str:
        """
        Returns the single, unique job_type string this plugin is responsible for.
        e.g., "sort_map"
        
        This is used by the Coordinator and Worker to register the plugin.
        """
        pass

    @staticmethod
    @abstractmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str, # Local path on coordinator to save/shard files
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile], # For jobs submitted via file upload
        params: Dict[str, Any] # Extra params from the submission
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        """
        Handles the "job submission" logic on the coordinator.
        - Processes/shards the uploaded file into 'job_dir'.
        - Creates the initial list of TaskPayloads.
        - Returns any initial state for the job_status tracker.
        
        Returns a tuple: 
        1. A list of TaskPayload objects to be added to the queue.
        2. A dictionary for the initial 'job_status' entry (e.g., {"total_tasks": 10, ...}).
        """
        pass

    @staticmethod
    @abstractmethod
    def execute_task(
        local_input_files: Dict[str, str], # Dict of {name: local_path}
        local_output_dir: str # A temp dir to write results to
    ) -> (tuple[bool, str]):
        """
        Executes a single task on the worker.
        (The 'job_type' parameter is removed, as the plugin itself is the type).
        
        Returns a tuple: (success_status, path_to_local_result_file)
        - success_status: True if the task succeeded, False otherwise.
        - local_result_path: Path to the single result file (e.g., "sorted_chunk.txt").
                             Return "" or None if no file output is expected.
        """
        pass