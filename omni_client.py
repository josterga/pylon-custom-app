import requests
import base64
import json
import time
import pyarrow as pa
from io import BytesIO
import logging

class OmniClient:
    def __init__(self, api_key, base_url):
        self.api_key = api_key
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        self.log = logging.getLogger(__name__)

    def _poll_job(self, job_ids, timeout=30, interval=3):
        url = f"{self.base_url}/query/wait"
        end_time = time.time() + timeout
        while time.time() < end_time:
            self.log.info("Polling Omni job(s)")
            resp = requests.get(url, headers=self.headers, params={"job_ids": json.dumps(job_ids)})
            resp.raise_for_status()
            for obj in resp.json():
                if obj.get("result", "").startswith("/////"):
                    return obj["result"]
            time.sleep(interval)
        raise TimeoutError(f"Jobs {job_ids} did not complete within {timeout} seconds")

    def run_query(self, query_json):
        self.log.info("Sending Omni API request")
        resp = requests.post(f"{self.base_url}/query/run", headers=self.headers, json=query_json)
        self.log.info(f"Omni response status: {resp.status_code}")
        resp.raise_for_status()

        json_objects = []
        depth = 0
        start = 0
        for i, ch in enumerate(resp.text):
            if ch == '{':
                if depth == 0:
                    start = i
                depth += 1
            elif ch == '}':
                depth -= 1
                if depth == 0:
                    json_objects.append(resp.text[start:i+1])

        arrow_data, job_ids = None, []
        for obj in map(json.loads, json_objects):
            if obj.get("result", "").startswith("/////"):
                arrow_data = base64.b64decode(obj["result"])
                break
            if "remaining_job_ids" in obj:
                job_ids.extend(obj["remaining_job_ids"])

        if not arrow_data and job_ids:
            arrow_data = base64.b64decode(self._poll_job(job_ids))

        if not arrow_data:
            raise Exception("Arrow table not found")

        with BytesIO(arrow_data) as buffer:
            table = pa.ipc.RecordBatchStreamReader(buffer).read_all()
            df = table.to_pandas()

        self.log.info(f"Retrieved DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
        return df
