import subprocess
import time

def fetch_logs(app_id):
    print(f"Fetching logs for {app_id}...")
    res = subprocess.run(["python", "-m", "modal", "app", "logs", app_id], capture_output=True, text=True)
    with open(f"{app_id}_logs.txt", "w", encoding="utf-8") as f:
        f.write(res.stdout)
        f.write("\n\nSTDERR:\n")
        f.write(res.stderr)
    print(f"Done writing to {app_id}_logs.txt")

if __name__ == "__main__":
    fetch_logs("ap-E6yZ71okkLeS4EgmEA5kEl")
