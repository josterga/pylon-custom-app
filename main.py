import logging
import requests
import base64
import pandas as pd
import pyarrow as pa
import json
from io import BytesIO
import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import time
from bs4 import BeautifulSoup
import re


# --- Setup Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

load_dotenv()
app = Flask(__name__)

# --- Config Validation ---
OMNI_API_KEY = os.getenv('OMNI_API_KEY')
OMNI_BASE_URL = os.getenv('OMNI_BASE_URL')
OMNI_MODEL_ID = os.getenv('OMNI_MODEL_ID')
PYLON_API_KEY = os.getenv('PYLON_API_KEY')
TYPESENSE_API_KEY = os.getenv('TYPESENSE_API_KEY')

if not OMNI_API_KEY:
    logger.error("OMNI_API_KEY not set")
    raise ValueError("OMNI_API_KEY is missing")
if not PYLON_API_KEY:
    logger.error("PYLON_API_KEY not set")
    raise ValueError("PYLON_API_KEY is missing")

PYLON_ENDPOINT_URL = "https://api.usepylon.com/"
PYLON_HEADERS = {
    "Content-Type": "application/json",
    "Authorization": f"Bearer {PYLON_API_KEY}"
}

STOPWORDS = set("""
a about above after again against all am an and any are as at be because been before being below between both but by can did do does doing down during each few for from further had has have having he her here hers herself him himself his how i if in into is it its itself just me more most my myself no nor not of off on once only or other our ours ourselves out over own same she should so some such than that the their theirs them themselves then there these they this those through to too under until up very was we were what when where which while who whom why will with you your yours yourself yourselves
please thanks hi hello regards note see ask wanted share request should could would know let make get new set use work issue report show think look found question want need help appreciate attached sent send sending replied reply replying regards sincerely best
""".split())

def get_pylon_issue_body(issue_id: str):
    """
    Fetches an issue from Pylon by ID and logs its body_html.
    """
    url = f"{PYLON_ENDPOINT_URL}issues/{issue_id}"
    logger.info(f"Fetching Pylon issue: {issue_id}")

    try:
        resp = requests.get(url, headers=PYLON_HEADERS, timeout=10)
        resp.raise_for_status()
        json_resp = resp.json()
        body_html = json_resp.get("data", {}).get("body_html")

        if body_html:
            logger.info(f"Issue {issue_id} body_html:\n{body_html}")
        else:
            logger.warning(f"Issue {issue_id} has no body_html")

        return body_html

    except requests.HTTPError as e:
        logger.error(f"Pylon API returned error for issue {issue_id}: {e.response.status_code} {e.response.text}")
    except Exception as e:
        logger.exception(f"Failed to fetch issue {issue_id}")

TYPESENSE_URL = f"https://3hb7fy1kz94rdwuqp-1.a1.typesense.net/multi_search?x-typesense-api-key={TYPESENSE_API_KEY}"
TYPESENSE_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "content-type": "text/plain"
}
TYPESENSE_SEARCH_BODY_TEMPLATE = {
    "searches": [
        {
            "collection": "omni-docs",
            "q": "",  # <-- keyword filled in below
            "query_by": "hierarchy.lvl0,hierarchy.lvl1,hierarchy.lvl2,hierarchy.lvl3,hierarchy.lvl4,hierarchy.lvl5,hierarchy.lvl6,content",
            "include_fields": "hierarchy.lvl0,hierarchy.lvl1,hierarchy.lvl2,hierarchy.lvl3,hierarchy.lvl4,hierarchy.lvl5,hierarchy.lvl6,content,anchor,url,type,id",
            "highlight_full_fields": "hierarchy.lvl0,hierarchy.lvl1,hierarchy.lvl2,hierarchy.lvl3,hierarchy.lvl4,hierarchy.lvl5,hierarchy.lvl6,content",
            "group_by": "url",
            "group_limit": 1,
            "sort_by": "item_priority:desc",
            "snippet_threshold": 8,
            "highlight_affix_num_tokens": 4,
            "filter_by": ""
        }
    ]
}

def extract_ngrams(text, n=3, stopwords=STOPWORDS, max_stop_ratio=0.5):
    """
    Extracts n-grams from text, filtering out those where more than max_stop_ratio of words are stopwords.
    """
    tokens = re.findall(r'\b\w+\b', text.lower())
    ngrams = []
    for i in range(len(tokens)-n+1):
        ng = tokens[i:i+n]
        stop_count = sum(w in stopwords for w in ng)
        # Allow n-grams with <=50% stopwords (configurable)
        if n == 1:
            if ng[0] not in stopwords:
                ngrams.append(ng[0])
        else:
            if stop_count <= int(max_stop_ratio * n):
                ngrams.append(' '.join(ng))
    return ngrams

def extract_keyword_ngrams(text, ngram_sizes=(3,2,1), min_word_length=3, stopwords=STOPWORDS):
    """
    Returns all n-grams (trigram, bigram, unigram) built ONLY from non-stopwords.
    Deduplicates across all ngram sizes, longest first.
    """
    keywords = extract_keywords(text, min_word_length=min_word_length, stopwords=stopwords)
    ngrams = set()
    for n in ngram_sizes:
        if len(keywords) >= n:
            for i in range(len(keywords) - n + 1):
                ngram = ' '.join(keywords[i:i+n])
                ngrams.add(ngram)
    # Return in order: trigrams, then bigrams, then unigrams
    ngram_list = []
    for n in ngram_sizes:
        ngram_list += [ng for ng in ngrams if len(ng.split()) == n]
    return ngram_list

def omni_docs_top_keyword_ngrams_from_text(text, max_results=3, ngram_sizes=(3,2,1)):
    seen_urls = set()
    doc_links = []
    keyword_ngrams = extract_keyword_ngrams(text, ngram_sizes=ngram_sizes)
    logger.info(f"Keyword n-grams for Typesense: {keyword_ngrams}")
    for phrase in keyword_ngrams:
        title, url = search_omni_docs_typesense(phrase)
        if url and url not in seen_urls:
            doc_links.append((title, url))
            seen_urls.add(url)
            if len(doc_links) >= max_results:
                return doc_links
    return doc_links

def search_omni_docs_typesense(keyword, logger=logging):
    body = TYPESENSE_SEARCH_BODY_TEMPLATE.copy()
    body["searches"][0] = body["searches"][0].copy()
    body["searches"][0]["q"] = keyword

    logger.info(f"[TYPESENSE] Searching docs for: '{keyword}'")
    try:
        resp = requests.post(
            TYPESENSE_URL,
            headers=TYPESENSE_HEADERS,
            data=json.dumps(body),
            timeout=8
        )
        logger.info(f"[TYPESENSE] Status: {resp.status_code}")
        resp.raise_for_status()
        resp_json = resp.json()

        results = resp_json.get("results", [])
        if not results:
            logger.info("[TYPESENSE] No results in response")
            return (None, None)

        grouped_hits = results[0].get("grouped_hits", [])
        logger.info(f"[TYPESENSE] Number of grouped_hits: {len(grouped_hits)}")
        if not grouped_hits:
            logger.info("[TYPESENSE] No grouped_hits for this keyword")
            return (None, None)

        # Get the first group with at least one hit
        for group in grouped_hits:
            hits = group.get("hits", [])
            if not hits:
                continue
            doc = hits[0]["document"]
            # Build title from hierarchy fields
            title_parts = []
            for i in range(7):
                lvl = doc.get(f"hierarchy.lvl{i}")
                if lvl:
                    title_parts.append(lvl)
            title = " > ".join(title_parts)
            url = doc["url"]
            if not url.startswith("http"):
                url = "https://docs.omni.co" + url
            logger.info(f"[TYPESENSE] Top hit: {title} -> {url}")
            return (title, url)

        logger.info("[TYPESENSE] No hits in any grouped_hit for this keyword")
        return (None, None)

    except Exception as e:
        logger.warning(f"[TYPESENSE] Search failed for '{keyword}': {e}")
        return (None, None)


def extract_keywords(text, min_word_length=3, stopwords=STOPWORDS):
    tokens = re.findall(r'\b\w+\b', text.lower())
    keywords = [t for t in tokens if t not in stopwords and len(t) >= min_word_length]
    return keywords



def omni_docs_best_match_from_text(text):
    keywords = extract_keywords(text)
    logger.info(f"[TYPESENSE] Extracted keywords: {keywords}")
    for kw in keywords:
        title, url = search_omni_docs_typesense(kw)
        if url:
            return title, url
    return None, None

def search_omni_docs_keywords(text, base_url="https://docs.omni.co"):
    """
    Extract keywords, run a docs search for each, return the first hit found.
    Returns (title, href) tuple, or (None, None) on failure.
    Includes extensive debugging output.
    """
    keywords = extract_keywords(text)
    logger.info(f"[DEBUG] Docs search keywords: {keywords}")

    for kw in keywords:
        search_url = f"{base_url}/search?q={requests.utils.quote(kw)}"
        logger.info(f"[DEBUG] Searching Omni Docs for keyword: '{kw}' at URL: {search_url}")
        try:
            resp = requests.get(search_url, timeout=8)
            logger.info(f"[DEBUG] HTTP status code: {resp.status_code} for keyword '{kw}'")
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")

            hits = soup.find_all("li", class_="DocSearch-Hit")
            logger.info(f"[DEBUG] Found {len(hits)} DocSearch-Hit elements for '{kw}'")

            if not hits:
                # Optionally, dump a part of the HTML for diagnostics (limit to first 800 chars)
                logger.info(f"[DEBUG] No hits found. First 800 chars of HTML: {resp.text[:800]}")
                continue

            # Show the HTML snippet for the first hit
            first_hit_html = str(hits[0])
            logger.info(f"[DEBUG] First DocSearch-Hit HTML snippet:\n{first_hit_html}")

            a_tag = hits[0].find("a", href=True)
            if not a_tag:
                logger.info(f"[DEBUG] No <a> tag found in first hit for '{kw}'")
                continue

            href = a_tag['href']
            # Ensure absolute URL
            if not href.startswith("http"):
                href = base_url + href
            title_elem = a_tag.find(class_="DocSearch-Hit-title")
            title = title_elem.get_text(" ", strip=True) if title_elem else href

            logger.info(f"[DEBUG] Docs search hit: title='{title}', href='{href}'")
            return (title, href)
        except Exception as e:
            logger.warning(f"[DEBUG] Docs search failed for keyword '{kw}': {e}")

    logger.info("[DEBUG] No documentation hits found for any keywords")
    return (None, None)

def omni_docs_top_ngrams_from_text(text, max_results=3, ngram_sizes=(3,2)):
    """
    Extracts n-grams (e.g. trigrams and bigrams), queries Typesense for each,
    and returns up to max_results unique doc links.
    """
    seen_urls = set()
    doc_links = []
    # Try largest n-grams first for specificity
    for n in ngram_sizes:
        ngram_phrases = extract_ngrams(text, n)
        for phrase in ngram_phrases:
            title, url = search_omni_docs_typesense(phrase)
            if url and url not in seen_urls:
                doc_links.append((title, url))
                seen_urls.add(url)
                if len(doc_links) >= max_results:
                    return doc_links
    return doc_links

def search_omni_docs(query, base_url="https://docs.omni.co"):
    """
    Search docs.omni.co with a query and return the top result's URL.
    Returns (title, href) tuple, or (None, None) on failure.
    """
    search_url = f"{base_url}/search?q={requests.utils.quote(query)}"
    try:
        resp = requests.get(search_url, timeout=8)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find first hit in search results (DocSearch-Hit class)
        hit = soup.find("li", class_="DocSearch-Hit")
        if not hit:
            return (None, None)

        a_tag = hit.find("a", href=True)
        if not a_tag:
            return (None, None)
        
        href = a_tag['href']
        # Ensure absolute URL
        if not href.startswith("http"):
            href = base_url + href
        title_elem = a_tag.find(class_="DocSearch-Hit-title")
        title = title_elem.get_text(" ", strip=True) if title_elem else href
        return (title, href)
    except Exception as e:
        logger.warning(f"Docs search failed for query '{query}': {e}")
        return (None, None)

def omni_docs_top_matches_from_text(text, max_results=3):
    """
    Returns up to max_results (title, url) tuples from Typesense using extracted keywords.
    """
    keywords = extract_keywords(text)
    logger.info(f"[TYPESENSE] Extracted keywords: {keywords}")
    seen_urls = set()
    matches = []
    for kw in keywords:
        body = TYPESENSE_SEARCH_BODY_TEMPLATE.copy()
        body["searches"][0] = body["searches"][0].copy()
        body["searches"][0]["q"] = kw

        try:
            resp = requests.post(
                TYPESENSE_URL,
                headers=TYPESENSE_HEADERS,
                data=json.dumps(body),
                timeout=8
            )
            resp.raise_for_status()
            resp_json = resp.json()
            results = resp_json.get("results", [])
            if not results:
                continue
            grouped_hits = results[0].get("grouped_hits", [])
            for group in grouped_hits:
                hits = group.get("hits", [])
                if not hits:
                    continue
                doc = hits[0]["document"]
                url = doc["url"]
                if not url.startswith("http"):
                    url = "https://docs.omni.co" + url
                # Deduplicate by URL
                if url in seen_urls:
                    continue
                seen_urls.add(url)
                # Build a readable title
                title_parts = [doc.get(f"hierarchy.lvl{i}") for i in range(7) if doc.get(f"hierarchy.lvl{i}")]
                title = " > ".join(title_parts)
                matches.append((title, url))
                if len(matches) >= max_results:
                    return matches
        except Exception as e:
            logger.warning(f"[TYPESENSE] Search failed for '{kw}': {e}")
    return matches


def poll_omni_job(remaining_job_ids, timeout=30, interval=3):
    """
    Polls Omni /query/wait until job completes or timeout.
    Returns base64 result string if successful.
    """
    headers = {
        "Authorization": f"Bearer {OMNI_API_KEY}"
    }

    url = f"{OMNI_BASE_URL}/query/wait"
    end_time = time.time() + timeout

    while time.time() < end_time:
        logger.info(f"Polling Omni job(s)")
        params = {
            "job_ids": json.dumps(remaining_job_ids)
        }
        resp = requests.get(url, headers=headers, params=params)
        resp.raise_for_status()

        data = resp.json()
        logger.debug(f"Poll response:")

        for obj in data:
            if obj.get("result", "").startswith("/////"):
                return obj["result"]

        if all(obj.get("timeout", False) for obj in data):
            time.sleep(interval)
        else:
            break

    raise TimeoutError(f"Jobs {remaining_job_ids} did not complete within {timeout} seconds")

# --- Core Functions ---
def get_omni_query_results(account_id: str):
    query = {
        "query": {
  "limit": 50000,
  "sorts": [
    {
      "null_sort": "OMNI_DEFAULT",
      "column_name": "salesforce__account.name",
      "is_column_sort": "false",
      "sort_descending": "false"
    }
  ],
  "table": "salesforce__opportunity",
  "fields": [
    "salesforce__account.name",
    "salesforce__account.lifecycle_stage_c",
    "dbt_czima__organizations.org_url",
    "dbt_czima__organizations.tenant",
    "salesforce__account_owner.name",
    "salesforce__opportunity.lead_solutions_engineer",
    "salesforce__opportunity.has_ps",
    "ps_owner.name",
    "dbt_czima__organizations.raw_link"
  ],
  "pivots": [],
  "dbtMode": "false",
  "filters": {
    "dbt_czima__users.email": {
      "is_negative": "false",
      "kind": "EQUALS",
      "type": "string",
      "values": [
        account_id
      ],
      "appliedLabels": {}
    }
  },
  "modelId": "fc9fb50e-3fc9-4571-9379-b6c8d8388ca1",
  "version": 7,
  "controls": [],
  "rewriteSql": "true",
  "row_totals": {},
  "fill_fields": [],
  "calculations": [],
  "column_limit": 50,
  "join_via_map": {},
  "column_totals": {},
  "userEditedSQL": "",
  "default_group_by": "true",
  "custom_summary_types": {},
  "join_paths_from_topic_name": "salesforce__opportunity",
}
     }  # same query JSON as before

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {OMNI_API_KEY}"
    }

    logger.info("Sending Omni API request")
    response = requests.post(f"{OMNI_BASE_URL}/query/run", headers=headers, json=query)
    # logger.debug(f"Omni raw response text: {response.text}")
    
    logger.info(f"Omni response status: {response.status_code}")
    if response.status_code != 200:
        logger.error(f"Omni API error: {response.text}")
        raise Exception(f"Omni API failed: {response.status_code}")

    response_text = response.text
    json_objects = []
    depth = 0
    start = 0

    for i, char in enumerate(response_text):
        if char == '{':
            if depth == 0:
                start = i
            depth += 1
        elif char == '}':
            depth -= 1
            if depth == 0:
                json_objects.append(response_text[start:i+1])

    logger.info(f"Parsed {len(json_objects)} JSON objects from Omni response")
    parsed_objects = [json.loads(obj) for obj in json_objects]
    # logger.info(parsed_objects)
    arrow_data = None
    remaining_job_ids = []

    for obj in parsed_objects:
        if "result" in obj and obj["result"].startswith("/////"):
            arrow_data = base64.b64decode(obj["result"])
            break
        if "remaining_job_ids" in obj:
            remaining_job_ids.extend(obj["remaining_job_ids"])

    if not arrow_data and remaining_job_ids:
        logger.info(f"Job pending:")
        result_b64 = poll_omni_job(remaining_job_ids)
        arrow_data = base64.b64decode(result_b64)

    if not arrow_data:
        raise Exception("Arrow table not found")

    with BytesIO(arrow_data) as buffer:
        reader = pa.ipc.RecordBatchStreamReader(buffer)
        table = reader.read_all()
        df = table.to_pandas()

    logger.info(f"Retrieved DataFrame with {df.shape[0]} rows and {df.shape[1]} columns")
    return df

# --- Routes ---
@app.route('/', methods=['GET'])
def root():
    request_type = request.args.get('request_type')
    logger.info(f"Received GET / request_type={request_type}")

    if request_type == 'verify':
        code = request.args.get('code')
        logger.info(f"Verification request with code={code}")
        return jsonify({"code": code})

    elif request_type == 'fetch_data':
        try:
            account_id = request.args.get('requester_email')
            logger.info(f"Fetching data for requester_email={account_id}")

            issue_id = request.args.get('issue_id')
            doc_link_components = []  # always initialize

            if issue_id:
                body_html = get_pylon_issue_body(issue_id)
                if body_html:
                    query = BeautifulSoup(body_html, "html.parser").get_text()[:200]
                    doc_links = omni_docs_top_keyword_ngrams_from_text(query, max_results=3)
                    for title, url in doc_links:
                        doc_link_components.append({
                            "type": "link",
                            "label": f"Related Documentation: {title}",
                            "url": url
                        })
                    logger.info(f"Docs links found for issue {issue_id}: {[x['url'] for x in doc_link_components]}")
                else:
                    logger.info(f"Issue {issue_id} has no body_html; skipping docs search.")
            else:
                logger.warning("No issue_id provided in webhook payload")

            df = get_omni_query_results(account_id)
            
            if not df.empty:
                row = df.iloc[0]
                header = {"title": "Account Info"}
                components = []
                # Insert all docs link components at the top, if found
                components.extend(doc_link_components)
                for col in df.columns:
                    val = row[col]
                    components.append({
                        "type": "text",
                        "label": col.replace("_", " ").title(),
                        "value": str(val) if pd.notna(val) else "(not available)"
                    })
                pylon_response = {
                    "version": "1.0.0",
                    "header": header,
                    "components": components
                }
                return jsonify(pylon_response), 200
            else:
                logger.warning(f"No data found for account_id={account_id}")
                no_data_resp = {
                    "version": "1.0.0",
                    "header": {
                        "title": "No Data Found",
                        "icon_url": "http://example.com/no-data-icon.png"
                    },
                    "components": [],
                    "message": f"No records found for account_id={account_id}"
                }
                return jsonify(no_data_resp), 404

        except Exception as e:
            logger.exception("Error processing fetch_data request")
            error_resp = {
                "version": "1.0.0",
                "header": {
                    "title": "Error",
                    "icon_url": "http://example.com/error-icon.png"
                },
                "components": [],
                "message": str(e)
            }
            return jsonify(error_resp), 500

@app.route('/webhook', methods=['GET', 'POST'])
def webhook():
    if request.method == 'GET':
        code = request.args.get('code')
        logger.info(f"Webhook GET verification with code={code}")
        return jsonify({"code": code})

    logger.info("Webhook POST received")
    try:
        df = get_omni_query_results(account_id=123)
    except Exception as e:
        logger.warning("Omni API failed in webhook, returning sample data")
        df = pd.DataFrame({"sample_col": [1, 2, 3]})

    logger.info("Webhook processing complete")
    return jsonify({"status": "success"})

if __name__ == "__main__":
    logger.info("Starting Flask app")
    app.run(debug=True, host='0.0.0.0', port=8000)
