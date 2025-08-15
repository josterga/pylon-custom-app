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
from pylon_client import PylonClient, PylonComponents

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

components_helper = PylonComponents()

@app.route('/', methods=['GET'])
def root():
    req_type = request.args.get('request_type')

    if req_type == 'verify':
        return jsonify({"code": request.args.get('code')})

    elif req_type == 'fetch_data':
        try:
            account_id = request.args.get('requester_email')
            issue_id = request.args.get('issue_id')

            # --- Step 1: Omni query (unchanged) ---
            sf_query = json.loads(json.dumps(QUERY_TEMPLATE))
            sf_query["query"]["filters"]["dbt_czima__users.email"]["values"] = [account_id]
            df = omni.run_query(sf_query)

            if df.empty:
                return jsonify({
                    "version": "1.0.0",
                    "header": {"title": "No Data Found"},
                    "components": [],
                    "message": f"No records found for account_id={account_id}"
                }), 404

            row = df.iloc[0]

            # --- Step 2: Build components via Pylon module helper ---
            components = components_helper.assemble_issue_plus_row_components(
                issue_id=issue_id,
                pylon_client=pylon,
                typesense_client=typesense,
                extract_weighted_domain_ngrams=extract_weighted_domain_ngrams,
                domain_keywords=domain_keywords,
                domain_phrases=domain_phrases,
                omni_row=row,
                max_links=4,
            )

            return jsonify({
                "version": "1.0.0",
                "header": {"title": "Account Info"},
                "components": components
            }), 200

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
