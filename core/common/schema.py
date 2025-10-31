from pydantic import BaseModel, Field

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
    capabilities: Capabilities # Corrected the typo from 'capabilites'

class TaskPayload(BaseModel):
    """
    A more specific model for the task payload.
    Using a dictionary is more flexible than a generic 'object'.
    """
    message: str

class Task(BaseModel):
    """
    Model representing a single task.
    """
    task_id: str
    payload: TaskPayload