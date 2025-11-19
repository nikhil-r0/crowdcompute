import os
import shutil
import uuid
from typing import List, Dict, Any, Optional
from fastapi import UploadFile

# Import the base class and schema
from schema import BasePlugin, TaskPayload, Task

class SortMapPlugin(BasePlugin):
    """
    Plugin for the "map" step of a distributed sort.
    """

    @staticmethod
    def get_job_type() -> str:
        return "sort_map"

    @staticmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str, 
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile],
        params: Dict[str, Any]
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        
        if not uploaded_file:
            raise ValueError("SortPlugin requires a file upload.")

        num_chunks = int(params.get("num_chunks", 10))
        
        # 1. Save the main unsorted file
        unsorted_file_path = os.path.join(job_dir, uploaded_file.filename or "UNSORTED.txt")
        try:
            with open(unsorted_file_path, "wb") as buffer:
                shutil.copyfileobj(uploaded_file.file, buffer)
        except Exception as e:
            print(f"Failed to save uploaded file: {e}")
            raise
        finally:
            uploaded_file.file.close()
            
        # 2. Shard the file
        chunk_file_paths = SortMapPlugin._shard(unsorted_file_path, job_dir, num_chunks)
        if not chunk_file_paths:
            raise Exception("Failed to shard file")

        # 3. Create 'sort_map' task payloads
        task_payloads = []
        for chunk_path in chunk_file_paths:
            task_id = str(uuid.uuid4()) 
            chunk_filename = os.path.basename(chunk_path)
            chunk_url = f"{coordinator_base_url}/data/jobs/{job_id}/{chunk_filename}"
            
            payload = TaskPayload(
                job_type="sort_map",
                input_files={"data": chunk_url}, 
                output_path=f"{coordinator_base_url}/upload/{job_id}/{task_id}", 
                params={}
            )
            task_payloads.append(payload)

        # 4. Define initial job status
        initial_job_status = {
            "job_type": "sort",
            "total_tasks": len(chunk_file_paths),
            "completed_tasks": 0,
            "map_results": [] 
        }
    
        print(f"SortMapPlugin created {len(task_payloads)} map tasks for job {job_id}.")
        return task_payloads, initial_job_status

    @staticmethod
    def execute_task(
        local_input_files: Dict[str, str],
        local_output_dir: str ,
        params: Dict[str, Any] # <--- Added params (ignored)
    ) -> (tuple[bool, str]):
        local_result_path = ""
        success = False

        input_file = local_input_files.get("data")
        local_result_path = os.path.join(local_output_dir, "sorted_chunk.txt")
        if input_file:
            print(f"  Executing sort_map on {input_file}")
            success = SortMapPlugin._execute_map(input_file, local_result_path)
        
        if not success:
            local_result_path = "" 

        return success, local_result_path

    @staticmethod
    def on_task_complete(
        task: Task,
        job_status: Dict[str, Any],
        tasks_queue: Dict[str, Task],
        coordinator_base_url: str
    ) -> None:
        """
        Handles logic when a 'sort_map' task finishes.
        Checks if all maps are done, then triggers 'sort_reduce'.
        """
        job_id = task.job_id
        
        if job_id not in job_status:
            return

        current_job = job_status[job_id]
        current_job["completed_tasks"] += 1
        print(f"Sort job {job_id} progress: {current_job['completed_tasks']}/{current_job['total_tasks']} map tasks complete.")
        
        # Check if all map tasks for this job are done
        if current_job["completed_tasks"] == current_job["total_tasks"]:
            print(f"All map tasks for {job_id} complete. Creating reduce task...")
            
            # Create the single reduce task
            reduce_task_id = str(uuid.uuid4())
            reduce_payload = TaskPayload(
                job_type="sort_reduce", 
                # Pass all the sorted chunk URLs as input
                input_files={
                    f"chunk_{i}": url for i, url in enumerate(current_job["map_results"])
                },
                output_path=f"{coordinator_base_url}/upload/{job_id}/{reduce_task_id}",
                params={}
            )
            reduce_task = Task(
                task_id=reduce_task_id,
                job_id=job_id,
                payload=reduce_payload
            )
            
            tasks_queue[reduce_task.task_id] = reduce_task
            print(f"Queued reduce task {reduce_task.task_id} for job {job_id}")


    # --- Internal Helper Methods ---

    @staticmethod
    def _shard(input_file_path: str, output_dir: str, num_chunks: int) -> list[str]:
        chunk_files = []
        try:
            with open(input_file_path, 'r') as f:
                total_lines = sum(1 for line in f)
            
            if total_lines == 0: return []

            lines_per_chunk = (total_lines // num_chunks) + 1
            
            with open(input_file_path, 'r') as f_in:
                chunk_index = 0
                lines_written = 0
                
                chunk_path = os.path.join(output_dir, f"chunk_{chunk_index}.txt")
                chunk_files.append(chunk_path)
                f_out = open(chunk_path, 'w')
                
                for line in f_in:
                    f_out.write(line)
                    lines_written += 1
                    
                    if lines_written >= lines_per_chunk and chunk_index < num_chunks - 1:
                        f_out.close()
                        chunk_index += 1
                        lines_written = 0
                        chunk_path = os.path.join(output_dir, f"chunk_{chunk_index}.txt")
                        chunk_files.append(chunk_path)
                        f_out = open(chunk_path, 'w')
                        
                f_out.close()
            return chunk_files

        except Exception as e:
            print(f"Failed to shard file: {e}")
            return []

    @staticmethod
    def _execute_map(local_input_path: str, local_output_path: str) -> bool:
        try:
            with open(local_input_path, 'r') as f_in:
                lines = f_in.readlines()
            lines.sort()
            with open(local_output_path, 'w') as f_out:
                f_out.writelines(lines)
            return True
        except Exception:
            return False