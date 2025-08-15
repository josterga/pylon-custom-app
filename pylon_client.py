import requests
import logging

class PylonClient:
    def __init__(self, api_key, base_url="https://api.usepylon.com/"):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        self.log = logging.getLogger(__name__)

    def get_issue_body_html(self, issue_id: str):
        url = f"{self.base_url}/issues/{issue_id}"
        self.log.info(f"Fetching Pylon issue: {issue_id}")
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            resp.raise_for_status()
            body_html = resp.json().get("data", {}).get("body_html")
            if body_html:
                self.log.info(f"Issue {issue_id} body_html found")
            else:
                self.log.warning(f"Issue {issue_id} has no body_html")
            return body_html
        except requests.HTTPError as e:
            self.log.error(f"Pylon API error: {e.response.status_code} {e.response.text}")
        except Exception:
            self.log.exception(f"Failed to fetch issue {issue_id}")
        return None
