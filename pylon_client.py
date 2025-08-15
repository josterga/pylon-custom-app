# pylon_client.py
import logging
import requests
from typing import List, Dict, Optional, Tuple
from bs4 import BeautifulSoup

class PylonClient:
    def __init__(self, api_key: str, base_url: str = "https://api.usepylon.com/"):
        self.base_url = base_url.rstrip("/")
        self.headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        self.log = logging.getLogger(__name__)

    def get_issue_body_html(self, issue_id: str) -> Optional[str]:
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


class PylonComponents:
    """
    Helper for producing Pylon UI components in a consistent format.
    Lives in the pylon module so callers can do:
        from pylon_client import PylonClient, PylonComponents
    """

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.log = logger or logging.getLogger(__name__)

    # ---- Base component builders ----
    @staticmethod
    def link(label: str, url: str) -> Dict:
        url = PylonComponents._normalize_url(url)
        return {"type": "link", "label": label, "url": url}

    @staticmethod
    def text(label: str, value: str) -> Dict:
        return {"type": "text", "label": label, "value": value}

    @staticmethod
    def _normalize_url(url: str) -> str:
        # Keep scheme if present; otherwise default to https.
        if not url.lower().startswith(("http://", "https://")):
            return "https://" + url.lstrip("/")
        return url

    # ---- Issue-driven related-docs ----
    def issue_related_docs(
        self,
        issue_id: str,
        pylon_client: PylonClient,
        typesense_client,
        extract_weighted_domain_ngrams,  # function injection
        domain_keywords: Dict,
        domain_phrases: Dict,
        max_links: int = 4,
        preview_chars: int = 200,
        min_signal_weight: float = 0.0,  # set >0 to filter weak signals
    ) -> List[Dict]:
        """
        Builds 'Related Documentation' link components derived from a Pylon issue's HTML body.

        Returns a list of link components.
        """
        components: List[Dict] = []

        body_html = pylon_client.get_issue_body_html(issue_id)
        if not body_html:
            return components

        text = BeautifulSoup(body_html, "html.parser").get_text()[:preview_chars]
        weighted = extract_weighted_domain_ngrams(text, domain_keywords, domain_phrases)

        # Sort phrases by weight descending; filter out weak signals if configured
        ranked: List[Tuple[str, float]] = sorted(
            ((p, w) for p, w in weighted.items() if w >= min_signal_weight),
            key=lambda x: -x[1],
        )

        for phrase, _w in ranked:
            try:
                # Expecting (title, url) or None per your Typesense client
                result = typesense_client.search_docs(phrase)
                if result:
                    title, url = result
                    components.append(
                        self.link(label=f"Related Documentation: {title}", url=url)
                    )
                if len(components) >= max_links:
                    break
            except Exception:
                self.log.exception("Typesense search failed for phrase '%s'", phrase)
                # continue to next phrase

        return components

    # ---- Omni row -> components ----
    def row_to_components(self, row) -> List[Dict]:
        """
        Convert a pandas Series row to a list of components.
        - URLs become 'link' components
        - everything else becomes 'text'
        """
        out: List[Dict] = []
        for col, val in row.items():
            val_str = "" if val is None else str(val)
            label = col.replace("_", " ").title()
            if val_str and (
                val_str.lower().startswith(("http://", "https://"))
                or "omniapp.co" in val_str.lower()
            ):
                out.append(self.link(label=label, url=val_str))
            else:
                out.append(self.text(label=label, value=val_str if val_str else "(not available)"))
        return out

    # ---- Orchestration convenience (optional) ----
    def assemble_issue_plus_row_components(
        self,
        issue_id: Optional[str],
        pylon_client: PylonClient,
        typesense_client,
        extract_weighted_domain_ngrams,
        domain_keywords: Dict,
        domain_phrases: Dict,
        omni_row,  # pandas Series or None
        max_links: int = 4,
    ) -> List[Dict]:
        """
        If you want a single call that returns [related docs ... account fields].
        """
        components: List[Dict] = []
        if issue_id:
            components.extend(
                self.issue_related_docs(
                    issue_id=issue_id,
                    pylon_client=pylon_client,
                    typesense_client=typesense_client,
                    extract_weighted_domain_ngrams=extract_weighted_domain_ngrams,
                    domain_keywords=domain_keywords,
                    domain_phrases=domain_phrases,
                    max_links=max_links,
                )
            )
        if omni_row is not None:
            components.extend(self.row_to_components(omni_row))
        return components
