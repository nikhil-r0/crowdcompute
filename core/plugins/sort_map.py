import os
import shutil
import uuid
from typing import List, Dict, Any, Optional
from fastapi import UploadFile

# Import the base class and schema
from schema import BasePlugin, TaskPayload

class SortMapPlugin(BasePlugin):
    """
    Plugin for the "map" step of a distributed sort.
    - Implements 'create_job_tasks' to shard the file.
    - Implements 'execute_task' to sort a single chunk.
    """

    @staticmethod
    def get_job_type() -> str:
        """
        Returns the single job type this plugin handles.
        """
        return "sort_map"

    @staticmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str, # Local path on coordinator (e.g., "file_storage/jobs/job_123")
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile],
        params: Dict[str, Any]
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        """
        Handles the "submit_sort_job" logic on the coordinator.
        - Saves the uploaded file.
        - Shards the file.
        - Creates all 'sort_map' tasks.
        """
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

        # 3. Create 'sort_map' task payloads for each chunk
        task_payloads = []
        for chunk_path in chunk_file_paths:
            task_id = str(uuid.uuid4()) # This is a placeholder
            chunk_filename = os.path.basename(chunk_path)
            chunk_url = f"{coordinator_base_url}/data/jobs/{job_id}/{chunk_filename}"
            
            payload = TaskPayload(
                job_type="sort_map",
                input_files={"data": chunk_url}, # Worker will download this
                output_path=f"{coordinator_base_url}/upload/{job_id}/{task_id}", # This will be replaced
                params={}
            )
            task_payloads.append(payload)

        # 4. Define initial job status
        initial_job_status = {
            "job_type": "sort",
            "total_tasks": len(chunk_file_paths),
            "completed_tasks": 0,
            "map_results": [] # This will be filled by the upload endpoint
        }
    
        print(f"SortMapPlugin created {len(task_payloads)} map tasks for job {job_id}.")
        
        return task_payloads, initial_job_status

    @staticmethod
    def execute_task(
        local_input_files: Dict[str, str], # Dict of {name: local_path}
        local_output_dir: str # A temp dir to write results to
    ) -> (tuple[bool, str]):
        """
        Executes the 'sort_map' task: sorts a single chunk.
        """
        local_result_path = ""
        success = False

        input_file = local_input_files.get("data")
        local_result_path = os.path.join(local_output_dir, "sorted_chunk.txt")
        if input_file:
            print(f"  Executing sort_map on {input_file}")
            success = SortMapPlugin._execute_map(input_file, local_result_path)
            if success:
                print(f"  sort_map complete. Output: {local_result_path}")
            else:
                print("  sort_map failed.")
        
        if not success:
            local_result_path = "" # Don't upload if it failed

        return success, local_result_path

    # --- Internal Helper Methods ---

    @staticmethod
    def _shard(input_file_path: str, output_dir: str, num_chunks: int) -> list[str]:
        """ Internal helper: Shards a large file into smaller chunks. """
        print(f"Sharding file {input_file_path} into {num_chunks} chunks...")
        chunk_files = []
        try:
            with open(input_file_path, 'r') as f:
                total_lines = sum(1 for line in f)
            
            if total_lines == 0:
                print("Warning: Input file is empty.")
                return []

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
            
            print(f"Sharding complete. Created {len(chunk_files)} chunk files.")
            return chunk_files

        except Exception as e:
            print(f"Failed to shard file: {e}")
            for f in chunk_files:
                if os.path.exists(f):
                    os.remove(f)
            return []

    @staticmethod
    def _execute_map(local_input_path: str, local_output_path: str) -> bool:
        """ Internal helper: Sorts a single chunk of data. """
        try:
            with open(local_input_path, 'r') as f_in:
                lines = f_in.readlines()
            
            lines.sort()
            
            with open(local_output_path, 'w') as f_out:
                f_out.writelines(lines)
                
            return True
        except Exception as e:
            print(f"Sort map execution failed: {e}")
            return False