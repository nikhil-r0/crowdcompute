import requests
import argparse
import os

def submit_job(coordinator_url: str, job_type: str, file_path: str, num_chunks: int):
    """
    Uploads a file to the coordinator to start a new job.
    
    :param coordinator_url: The base URL of the coordinator (e.g., http://localhost:8000)
    :param job_type: The type of job to run (e.g., "sort_map")
    :param file_path: The local path to the file to upload
    :param num_chunks: The number of chunks to split the file into
    """
    
    if not os.path.exists(file_path):
        print(f"Error: File not found at {file_path}")
        return

    # The endpoint URL
    url = f"{coordinator_url}/submit-job/{job_type}"
    
    # Prepare the form data and the file
    # 'data' holds the non-file fields
    # 'files' holds the file part
    data = {
        "num_chunks": num_chunks
    }
    
    try:
        with open(file_path, 'rb') as f:
            files = {
                "file": (os.path.basename(file_path), f)
            }
            
            print(f"Submitting '{job_type}' job to {url} with file {file_path}...")
            
            # Make the POST request
            response = requests.post(url, data=data, files=files)
            
            # Check for success
            response.raise_for_status()
            
            print("\n--- Success ---")
            print(f"Response: {response.json()}")

    except requests.exceptions.ConnectionError:
        print(f"\nError: Could not connect to coordinator at {url}")
        print("Please ensure the coordinator is running and accessible.")
    except requests.exceptions.HTTPError as e:
        print(f"\nError: Job submission failed (HTTP {e.response.status_code})")
        try:
            print(f"Details: {e.response.json().get('detail', e.response.text)}")
        except requests.exceptions.JSONDecodeError:
            print(f"Details: {e.response.text}")
    except Exception as e:
        print(f"\nAn unexpected error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Submit a job to the CrowdCompute Coordinator")
    parser.add_argument("job_type", 
                        type=str, 
                        help="The type of job to run (e.g., 'sort_map')")
    parser.add_argument("file_path", 
                        type=str, 
                        help="The path to the input file")
    parser.add_argument("--url", 
                        type=str, 
                        default="http://localhost:8000", 
                        help="The base URL of the coordinator")
    parser.add_argument("--chunks", 
                        type=int, 
                        default=10, 
                        help="Number of chunks to split the file into (if applicable)")

    args = parser.parse_args()
    
    submit_job(args.url, args.job_type, args.file_path, args.chunks)