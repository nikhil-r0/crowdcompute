import requests
import os
import sys

def submit_hashcat_job():
    # Configuration
    COORDINATOR_URL = "http://localhost:8000"
    JOB_TYPE = "hashcat_crack"
    FILE_PATH = "demo_wordlist.txt"
    
    # The MD5 hash of the word "hashcat"
    TARGET_HASH = "8743b52063cd84097a65d1633f5c74f5" 
    HASH_MODE = "0" # 0 = MD5
    NUM_CHUNKS = 4 # Split the small file into 4 parts to force distribution

    # Check file
    if not os.path.exists(FILE_PATH):
        print(f"Error: {FILE_PATH} not found. Please create it first.")
        return

    url = f"{COORDINATOR_URL}/submit-job/{JOB_TYPE}"
    
    print(f"Submitting Job to {url}...")
    print(f"  Target Hash: {TARGET_HASH}")
    print(f"  Wordlist: {FILE_PATH}")

    try:
        with open(FILE_PATH, 'rb') as f:
            files = {
                "file": (os.path.basename(FILE_PATH), f)
            }
            # Note: In the plugin implementation, we look for 'target_hash' in params
            # FASTAPI handles non-file fields as form data
            data = {
                "target_hash": TARGET_HASH,
                "hash_mode": HASH_MODE,
                "num_chunks": NUM_CHUNKS
            }
            
            response = requests.post(url, data=data, files=files)
            response.raise_for_status()
            
            print("\n[SUCCESS] Job Submitted!")
            print(response.json())
            print(f"\nCheck status at: {COORDINATOR_URL}/tasks")

    except requests.exceptions.ConnectionError:
        print(f"\n[ERROR] Could not connect to {COORDINATOR_URL}. Is the server running?")
    except Exception as e:
        print(f"\n[ERROR] {e}")
        if 'response' in locals():
             print(f"Server response: {response.text}")

if __name__ == "__main__":
    submit_hashcat_job()