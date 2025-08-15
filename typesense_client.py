import requests
import json
import logging

class TypesenseClient:
    def __init__(self, base_url, api_key):
        self.url = f"https://{base_url}-1.a1.typesense.net/multi_search?x-typesense-api-key={api_key}"
        self.headers = {
            "accept": "application/json, text/plain, */*",
            "content-type": "text/plain"
        }
        self.log = logging.getLogger(__name__)

    def search_docs(self, keyword):
        body = {
            "searches": [{
                "collection": "omni-docs",
                "q": keyword,
                "query_by": "hierarchy.lvl0,hierarchy.lvl1,hierarchy.lvl2,hierarchy.lvl3,hierarchy.lvl4,hierarchy.lvl5,hierarchy.lvl6,content",
                "include_fields": "hierarchy.lvl0,hierarchy.lvl1,content,url",
                "group_by": "url",
                "group_limit": 3
            }]
        }
        try:
            resp = requests.post(self.url, headers=self.headers, data=json.dumps(body), timeout=8)
            resp.raise_for_status()
            payload = resp.json()
            results = payload.get("results", [])
            if not results:
                return None

            for group in results[0].get("grouped_hits", []):
                hits = group.get("hits", [])
                if not hits:
                    continue
                doc = hits[0]["document"]
                title_parts = [doc.get(f"hierarchy.lvl{i}") for i in range(7) if doc.get(f"hierarchy.lvl{i}")]
                title = " > ".join(title_parts) if title_parts else doc.get("url", "")
                url = doc["url"]
                if not url.startswith("http"):
                    url = "https://docs.omni.co" + url
                return title, url

            return None
        except Exception as e:
            self.log.warning(f"[TYPESENSE] Search failed for '{keyword}': {e}")
            return None
