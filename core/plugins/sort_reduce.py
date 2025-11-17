import os
import heapq
from typing import List, Dict, Any, Optional
from fastapi import UploadFile

# Import the base class and schema
from schema import BasePlugin, TaskPayload

class SortReducePlugin(BasePlugin):
    """
    Plugin for the "reduce" step of a distributed sort.
    - Implements 'execute_task' to merge sorted chunks.
    """

    @staticmethod
    def get_job_type() -> str:
        """
        Returns the single job type this plugin handles.
        """
        return "sort_reduce"

    @staticmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str,
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile],
        params: Dict[str, Any]
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        """
        This plugin does not create jobs from user submissions.
        The 'sort_reduce' task is created by the coordinator
        during job chaining.
        """
        raise NotImplementedError("SortReducePlugin does not create jobs.")

    @staticmethod
    def execute_task(
        local_input_files: Dict[str, str], # Dict of {name: local_path}
        local_output_dir: str # A temp dir to write results to
    ) -> (tuple[bool, str]):
        """
        Executes the 'sort_reduce' task: merges sorted chunks.
        """
        local_result_path = ""
        success = False

        input_chunks = list(local_input_files.values())
        local_result_path = os.path.join(local_output_dir, "FINAL_SORTED.txt")
        
        if not input_chunks:
            print("  sort_reduce error: No input files found.")
            return False, ""

        print(f"  Executing sort_reduce on {len(input_chunks)} chunks")
        success = SortReducePlugin._execute_reduce(input_chunks, local_result_path)
        if success:
            print(f"  sort_reduce complete. Output: {local_result_path}")
        else:
            print("  sort_reduce failed.")
        
        if not success:
            local_result_path = "" # Don't upload if it failed

        return success, local_result_path

    # --- Internal Helper Method ---

    @staticmethod
    def _execute_reduce(local_input_files: list[str], local_output_path: str) -> bool:
        """
        Internal helper: Merges multiple sorted chunks.
        """
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