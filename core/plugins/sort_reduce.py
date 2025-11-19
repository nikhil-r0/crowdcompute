import os
import heapq
from typing import List, Dict, Any, Optional
from fastapi import UploadFile
from schema import BasePlugin, TaskPayload, Task

class SortReducePlugin(BasePlugin):
    """
    Plugin for the "reduce" step of a distributed sort.
    """

    @staticmethod
    def get_job_type() -> str:
        return "sort_reduce"

    @staticmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str,
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile],
        params: Dict[str, Any]
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        raise NotImplementedError("SortReducePlugin does not create jobs.")

    @staticmethod
    def execute_task(
        local_input_files: Dict[str, str], 
        local_output_dir: str,
        params: Dict[str, Any] # <--- Added params (ignored)
    ) -> (tuple[bool, str]):
        
        input_chunks = list(local_input_files.values())
        local_result_path = os.path.join(local_output_dir, "FINAL_SORTED.txt")
        
        if not input_chunks:
            return False, ""

        print(f"  Executing sort_reduce on {len(input_chunks)} chunks")
        success = SortReducePlugin._execute_reduce(input_chunks, local_result_path)
        
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
        job_id = task.job_id
        print(f"--- SORT JOB {job_id} COMPLETE ---")
        if job_id in job_status:
            del job_status[job_id]

    @staticmethod
    def _execute_reduce(local_input_files: list[str], local_output_path: str) -> bool:
        open_files = []
        try:
            for f_path in local_input_files:
                open_files.append(open(f_path, 'r'))
            with open(local_output_path, 'w') as f_out:
                for line in heapq.merge(*open_files):
                    f_out.write(line)
            return True
        except Exception as e:
            print(f"Sort reduce execution failed: {e}")
            return False
        finally:
            for f in open_files:
                f.close()