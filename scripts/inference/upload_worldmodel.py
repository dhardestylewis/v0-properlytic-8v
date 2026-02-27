"""Upload fixed worldmodel.py to GCS."""
import modal, os, json

app = modal.App("upload-wm-v2")
image = modal.Image.debian_slim(python_version="3.11").pip_install("google-cloud-storage")
gcs_secret = modal.Secret.from_name("gcs-creds", required_keys=["GOOGLE_APPLICATION_CREDENTIALS_JSON"])

@app.function(image=image, secrets=[gcs_secret], timeout=60)
def upload_to_gcs(content: str) -> str:
    from google.cloud import storage
    creds = json.loads(os.environ["GOOGLE_APPLICATION_CREDENTIALS_JSON"])
    client = storage.Client.from_service_account_info(creds)
    bucket = client.bucket("properlytic-raw-data")
    blob = bucket.blob("code/worldmodel.py")
    blob.upload_from_string(content.encode("utf-8"), content_type="text/x-python")
    downloaded = blob.download_as_text()
    assert ".expand(B, -1, -1).clone()" in downloaded, "Fix not in uploaded file!"
    return f"OK: {blob.size} bytes, clone fix verified"

@app.local_entrypoint()
def main():
    # Read locally (this runs on YOUR machine, not Modal)
    wm_path = os.path.join(os.path.dirname(__file__), "inference", "worldmodel.py")
    with open(wm_path, "r", encoding="utf-8") as f:
        content = f.read()
    assert ".expand(B, -1, -1).clone()" in content, "Fix not in local file!"
    print(f"[LOCAL] Read {len(content)} chars, clone fix confirmed")
    result = upload_to_gcs.remote(content)
    print(f"[UPLOAD] {result}")
