import json
import time
import requests
import os
from azure.identity import DefaultAzureCredential


# ----------------------------
# Helper: get Fabric access token
# ----------------------------
def get_token(scope="https://api.fabric.microsoft.com/.default"):
    cred = DefaultAzureCredential()
    token = cred.get_token(scope)
    return token.token

# ----------------------------
# Create Livy session
# ----------------------------
def create_livy_session(livy_url):
    access_token = get_token()
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    payload = {
     "kind": "spark",
      "conf": {
        "spark.dynamicAllocation.enabled": "false"
     }
    }

    print("📡 Creating Livy session...")
    resp = requests.post(livy_url, headers=headers, json=payload)
    resp.raise_for_status()
    session = resp.json()
    print(json.dumps(session, indent=2))
    return session["id"]

# ----------------------------
# Wait for session to become idle
# ----------------------------
def wait_for_session_ready(livy_url, session_id, timeout=300):
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{livy_url}/{session_id}"

    print("⏳ Waiting for session to become idle...")
    time.sleep(5)
    start = time.time()
    while True:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state")
        print(f"   Session state: {state}")
        if state == "idle":
            print("✅ Session is ready.")
            return
        if state in ("dead", "error", "killed"):
            raise RuntimeError(f"Session failed to start: {json.dumps(data, indent=2)}")
        if time.time() - start > timeout:
            raise TimeoutError("Session did not become idle within timeout.")
        time.sleep(5)

# ----------------------------
# Submit a Spark statement
# ----------------------------
def run_statement(livy_url, session_id, code):
    token = get_token()
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    url = f"{livy_url}/{session_id}/statements"
    payload = {
        "code": code,
        "kind": "pyspark"   # 👈 changed from "spark"
    }

    print("🚀 Submitting Spark code...")
    resp = requests.post(url, headers=headers, json=payload)
    resp.raise_for_status()
    stmt = resp.json()
    print(json.dumps(stmt, indent=2))
    return stmt["id"]
# ----------------------------
# Poll statement until available
# ----------------------------
def poll_statement(livy_url, session_id, statement_id, timeout=300):
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{livy_url}/{session_id}/statements/{statement_id}"

    print("📊 Polling statement for completion...")
    start = time.time()
    while True:
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        data = resp.json()
        state = data.get("state")
        print(f"   Statement state: {state}")
        if state in ("available", "error", "cancelled"):
            print("✅ Final statement result:")
            print(json.dumps(data, indent=2))
            return data
        if time.time() - start > timeout:
            raise TimeoutError("Statement did not finish within timeout.")
        time.sleep(5)

# ----------------------------
# Delete the session
# ----------------------------
def close_session(livy_url, session_id):
    token = get_token()
    headers = {"Authorization": f"Bearer {token}"}
    url = f"{livy_url}/{session_id}"
    print("🧹 Closing session...")
    resp = requests.delete(url, headers=headers)
    if resp.status_code == 200:
        print(f"✅ Session {session_id} closed successfully.")
    else:
        print(f"⚠️ Failed to close session (status {resp.status_code}).")

# ----------------------------
# Main entry
# ----------------------------
if __name__ == "__main__":
    # Get configuration from environment variables
    WORKSPACE_ID = os.getenv("FABRIC_WORKSPACE_ID", "YOUR_WORKSPACE_ID")
    LAKEHOUSE_ID = os.getenv("FABRIC_LAKEHOUSE_ID", "YOUR_LAKEHOUSE_ID")
    API_DOMAIN = os.getenv("FABRIC_API_DOMAIN", "api.fabric.microsoft.com")
    
    # Construct Livy API endpoint
    livy_url = (
        f"https://{API_DOMAIN}/v1/workspaces/{WORKSPACE_ID}/lakehouses/{LAKEHOUSE_ID}/livyapi/versions/2023-12-01/sessions"
    )

    # 1️⃣ Create session
    session_id = create_livy_session(livy_url)

    try:
        # 2️⃣ Wait until it's ready
        wait_for_session_ready(livy_url, session_id)

        # 3️⃣ Submit Spark code
        spark_code = "print('Hello from Fabric!'); spark.range(5).show()"
        statement_id = run_statement(livy_url, session_id, spark_code)
        # in seconds
        timeout = int(os.getenv("FABRIC_LIVY_TIMEOUT", "3600"))
        # 4️⃣ Wait for results
        poll_statement(livy_url, session_id, statement_id, timeout)

    finally:
        # 5️⃣ Always close the session
        close_session(livy_url, session_id)
