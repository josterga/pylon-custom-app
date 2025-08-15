import os
import json
import logging
import pandas as pd
from flask import Flask, request, jsonify
from dotenv import load_dotenv
from bs4 import BeautifulSoup

from pylon_client import PylonClient
from omni_client import OmniClient
from typesense_client import TypesenseClient
from domain_utils import load_domain_sets, extract_weighted_domain_ngrams

# --- Setup ---
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- Config ---
OMNI_API_KEY = os.getenv("OMNI_API_KEY")
OMNI_BASE_URL = os.getenv("OMNI_BASE_URL")
PYLON_API_KEY = os.getenv("PYLON_API_KEY")
TYPESENSE_API_KEY = os.getenv("TYPESENSE_API_KEY")
TYPESENSE_BASE_URL = os.getenv("TYPESENSE_BASE_URL")

if not OMNI_API_KEY:
    raise ValueError("OMNI_API_KEY is missing")
if not PYLON_API_KEY:
    raise ValueError("PYLON_API_KEY is missing")

# --- Load resources ---
domain_keywords, domain_phrases = load_domain_sets("domain_keywords.json")

with open("query.json", "r") as f:
    QUERY_TEMPLATE = json.load(f)

# --- Clients ---
pylon = PylonClient(PYLON_API_KEY)
omni = OmniClient(OMNI_API_KEY, OMNI_BASE_URL)
typesense = TypesenseClient(TYPESENSE_BASE_URL, TYPESENSE_API_KEY)

app = Flask(__name__)

# --- Helper: Build link components for Pylon ---
def build_link_component(title: str, url: str) -> dict:
    return {
        "type": "link",
        "label": f"Related Documentation: {title}",
        "url": url
    }

# --- Helper: Build text components for Pylon ---
def build_text_component(label: str, value: str) -> dict:
    return {
        "type": "text",
        "label": label,
        "value": value
    }

@app.route('/', methods=['GET'])
def root():
    req_type = request.args.get('request_type')

    # Verification request
    if req_type == 'verify':
        return jsonify({"code": request.args.get('code')})

    elif req_type == 'fetch_data':
        try:
            account_id = request.args.get('requester_email')
            issue_id = request.args.get('issue_id')
            components = []

            # --- Step 1: Search related docs from Pylon issue ---
            doc_links = []
            if issue_id:
                body_html = pylon.get_issue_body_html(issue_id)
                if body_html:
                    query_text = BeautifulSoup(body_html, "html.parser").get_text()[:200]
                    weighted_ngrams = extract_weighted_domain_ngrams(query_text, domain_keywords, domain_phrases)

                    for phrase, _ in sorted(weighted_ngrams.items(), key=lambda x: -x[1]):
                        result = typesense.search_docs(phrase)  # should return (title, url) or None
                        if result:
                            title, url = result
                            doc_links.append({
                                "type": "link",
                                "label": f"Related Documentation: {title}",
                                "url": url
                            })
                        if len(doc_links) >= 4:
                            break

            # --- Step 2: Run Omni query ---
            sf_query = json.loads(json.dumps(QUERY_TEMPLATE))  # deep copy
            sf_query["query"]["filters"]["dbt_czima__users.email"]["values"] = [account_id]

            df = omni.run_query(sf_query)

            # --- Step 3: Append account info ---
            if not df.empty:
                row = df.iloc[0]
                components = doc_links[:]  # start with doc links

                for col in df.columns:
                    val = row[col]
                    val_str = str(val) if pd.notna(val) else "(not available)"

                    if (
                        isinstance(val_str, str)
                        and (
                            val_str.lower().startswith("http")
                            or "omniapp.co" in val_str.lower()
                        )
                    ):
                        # Ensure valid URL format for Pylon
                        url_val = val_str
                        if not url_val.lower().startswith("http"):
                            url_val = "https://" + url_val.lstrip("/")  # add scheme

                        components.append({
                            "type": "link",
                            "label": col.replace("_", " ").title(),
                            "url": url_val
                        })
                    else:
                        components.append({
                            "type": "text",
                            "label": col.replace("_", " ").title(),
                            "value": val_str
                        })

                return jsonify({
                    "version": "1.0.0",
                    "header": {"title": "Account Info"},
                    "components": components
                }), 200

            else:
                return jsonify({
                    "version": "1.0.0",
                    "header": {"title": "No Data Found"},
                    "components": [],
                    "message": f"No records found for account_id={account_id}"
                }), 404

        except Exception as e:
            logging.exception("Error in fetch_data")
            return jsonify({
                "version": "1.0.0",
                "header": {"title": "Error"},
                "components": [],
                "message": str(e)
            }), 500

    return jsonify({"error": "Invalid request_type"}), 400



if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
