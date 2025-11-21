import os
import shutil
import uuid
import tarfile
import io
from typing import List, Dict, Any, Optional, TYPE_CHECKING
from fastapi import UploadFile
from schema import BasePlugin, TaskPayload, Task

if TYPE_CHECKING:
    import docker
    from docker.errors import ImageNotFound as DockerImageNotFound
else:
    try:
        import docker
        from docker.errors import ImageNotFound as DockerImageNotFound
    except ImportError:
        docker = None
        class DockerImageNotFound(Exception): pass # type: ignore

class HashcatPlugin(BasePlugin):
    """
    Plugin for distributed password cracking using Hashcat.
    """

    @staticmethod
    def get_job_type() -> str:
        return "hashcat_crack"

    @staticmethod
    def create_job_tasks(
        job_id: str,
        job_dir: str,
        coordinator_base_url: str,
        uploaded_file: Optional[UploadFile],
        params: Dict[str, Any]
    ) -> (tuple[List[TaskPayload], Dict[str, Any]]):
        
        if not uploaded_file:
            raise ValueError("Hashcat job requires a wordlist file upload.")

        target_hash = params.get("target_hash")
        hash_mode = params.get("hash_mode", "0")
        num_chunks = int(params.get("num_chunks", 5))

        if not target_hash:
            raise ValueError("Missing parameter: target_hash")

        wordlist_path = os.path.join(job_dir, "wordlist.txt")
        with open(wordlist_path, "wb") as buffer:
            shutil.copyfileobj(uploaded_file.file, buffer)
        uploaded_file.file.close()

        chunk_paths = HashcatPlugin._shard_wordlist(wordlist_path, job_dir, num_chunks)

        task_payloads = []
        for chunk_path in chunk_paths:
            task_id = str(uuid.uuid4())
            chunk_filename = os.path.basename(chunk_path)
            chunk_url = f"{coordinator_base_url}/data/jobs/{job_id}/{chunk_filename}"
            
            payload = TaskPayload(
                job_type="hashcat_crack",
                input_files={"wordlist_chunk": chunk_url},
                output_path=f"{coordinator_base_url}/upload/{job_id}/{task_id}",
                params={
                    "target_hash": target_hash,
                    "hash_mode": hash_mode
                }
            )
            task_payloads.append(payload)

        initial_status = {
            "job_type": "hashcat",
            "status": "running",
            "cracked_password": None,
            "total_tasks": len(task_payloads),
            "completed_tasks": 0,
            "map_results": [] 
        }
        
        return task_payloads, initial_status

    @staticmethod
    def execute_task(
        local_input_files: Dict[str, str],
        local_output_dir: str,
        params: Dict[str, Any]
    ) -> (tuple[bool, str]):
        
        if not docker:
            print("Error: Docker SDK not installed on this worker.")
            return False, ""

        wordlist_path = local_input_files.get("wordlist_chunk")
        if not wordlist_path: return False, ""
            
        target_hash = params.get("target_hash")
        hash_mode = params.get("hash_mode", "0")

        try:
            client = docker.from_env()
            image_name = "crowd-hashcat-cpu:latest"
            
            try:
                client.images.get(image_name)
            except DockerImageNotFound: 
                print(f"  [DooD] ERROR: Image '{image_name}' not found! Please run ./build_images.sh")
                return False, ""
            
            session_id = f"sess_{uuid.uuid4().hex[:8]}"
            
            container_name = f"hashcat_worker_{uuid.uuid4().hex[:8]}"
            
            print(f"  [DooD] Spawning hashcat container for hash {target_hash}...")
        
            # Clean command (Removed clinfo fallback)
            cmd = (
                f"hashcat -m {hash_mode} -a 0 -D 1,2 --force -O --self-test-disable "
                f"--session {session_id} " 
                f"--outfile /root/result.txt --potfile-disable {target_hash} /root/wordlist.txt"
            )            
            container = client.containers.create(
                image=image_name,
                command=cmd,
                name=container_name,
                entrypoint="",
                detach=True,
                privileged=True,
                volumes={
                    'crowdcompute_hashcat_cache': {
                        'bind': '/root/.hashcat',
                        'mode': 'rw'
                    }
                }
            )
            
            with open(wordlist_path, 'rb') as f:
                data = f.read()
                
            tar_stream = io.BytesIO()
            with tarfile.open(fileobj=tar_stream, mode='w') as tar:
                tar_info = tarfile.TarInfo(name='wordlist.txt')
                tar_info.size = len(data)
                tar_stream.seek(0)
                tar.addfile(tar_info, io.BytesIO(data))
            tar_stream.seek(0)
            
            container.put_archive('/root/', tar_stream)
            
            container.start()
            result = container.wait() 
            
            # Logs are commented out as requested, uncomment to debug
            logs = container.logs().decode('utf-8')
            print(f"  [Hashcat Logs]:\n{logs}")

            cracked_content = None
            try:
                bits, stat = container.get_archive('/root/result.txt')
                file_obj = io.BytesIO()
                for chunk in bits:
                    file_obj.write(chunk)
                file_obj.seek(0)
                
                with tarfile.open(fileobj=file_obj) as tar:
                    member = tar.getmember('result.txt')
                    f = tar.extractfile(member)
                    if f:
                        cracked_content = f.read().decode('utf-8').strip()
            except Exception:
                pass

            container.remove()
            
            local_result_path = os.path.join(local_output_dir, "cracked.txt")
            
            if cracked_content:
                print(f"  [Hashcat] PASSWORD CRACKED: {cracked_content}")
                with open(local_result_path, "w") as f:
                     f.write(cracked_content)
                return True, local_result_path
            else:
                print("  [Hashcat] Exhausted chunk. Password not found.")
                return True, "" 

        except Exception as e:
            print(f"  [DooD] Error: {e}")
            if 'container' in locals():
                try: container.remove(force=True)
                except: pass
            return False, ""

    @staticmethod
    def on_task_complete(
        task: Task,
        job_status: Dict[str, Any],
        tasks_queue: Dict[str, Task],
        coordinator_base_url: str
    ) -> None:
        
        job_id = task.job_id
        if job_id in job_status:
            job_status[job_id]["completed_tasks"] += 1
            
            results = job_status[job_id].get("map_results", [])
            if results:
                print(f"!!! PASSWORD FOUND FOR JOB {job_id} !!!")
                print("initiating KILL SWITCH for other tasks...")
                
                job_status[job_id]["status"] = "cracked"
                job_status[job_id]["cracked_password"] = "See results"
                
                tasks_to_remove = [tid for tid, t in tasks_queue.items() if t.job_id == job_id]
                for tid in tasks_to_remove:
                    del tasks_queue[tid]
                
                print(f"Cancelled {len(tasks_to_remove)} pending tasks.")

    @staticmethod
    def _shard_wordlist(path, job_dir, num_chunks):
        chunk_files = []
        try:
            with open(path, 'r', encoding='latin-1') as f:
                lines = f.readlines()
            
            total_lines = len(lines)
            chunk_size = (total_lines // num_chunks) + 1
            
            for i in range(0, total_lines, chunk_size):
                chunk = lines[i:i + chunk_size]
                chunk_name = f"chunk_{len(chunk_files)}.txt"
                chunk_path = os.path.join(job_dir, chunk_name)
                with open(chunk_path, 'w') as out:
                    out.writelines(chunk)
                chunk_files.append(chunk_path)
            return chunk_files
        except Exception:
            return []