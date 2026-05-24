"""Website scraper: URL → AI auto-config (15 dk kurulum).

Usage:
    POST /api/scrape-site  {"url": "https://example.com"}
    → auto-generates tenant config with services, prices, delivery, etc.
"""
from __future__ import annotations
import json, logging, re, os
from typing import Any
from urllib.parse import urljoin, urlparse
import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("scraper")

# ── Fetch website content ───────────────────────────────────────────

def fetch_website_text(url: str, max_pages: int = 5) -> str:
    """Fetch main pages, extract text content. Returns concatenated markdown-like text."""
    visited: set[str] = set()
    texts: list[str] = []
    to_visit = [url]

    for target in to_visit:
        if len(visited) >= max_pages:
            break
        if target in visited:
            continue
        visited.add(target)
        try:
            resp = requests.get(target, timeout=15, headers={
                "User-Agent": "Mozilla/5.0 (compatible; DoelAI/1.0)"
            })
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            # Remove scripts/style
            for tag in soup(["script", "style", "nav", "footer", "header"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            # Filter too-short or duplicate pages
            if len(text) > 100:
                texts.append(f"--- Page: {target} ---\n{text[:8000]}")

            # Find internal links
            base = urlparse(url)
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                full = urljoin(url, href)
                parsed = urlparse(full)
                if parsed.netloc == base.netloc and full not in visited and len(to_visit) < max_pages:
                    # Skip anchors, mailto, tel
                    if parsed.fragment or parsed.scheme not in ("http", "https"):
                        continue
                    to_visit.append(full)
        except Exception as exc:
            logger.debug("scrape_skip %s %s", target, exc)

    return "\n\n".join(texts)

# ── LLM-based extraction ────────────────────────────────────────────

def extract_business_config(site_text: str, llm_call: callable) -> dict[str, Any]:
    """Use LLM to extract business info from scraped text.
    
    llm_call(prompt: str) -> str  (returns JSON string)
    """
    prompt = f"""You are a business analyzer. Given website content, extract business information.

Return ONLY valid JSON with this exact structure:
{{
    "business_name": "Company name",
    "business_type": "Industry/category",
    "business_tagline": "Short tagline if found",
    "services": [
        {{
            "slug": "service-slug-in-english",
            "display": "Service Name in Turkish",
            "keywords": ["keyword1", "keyword2"],
            "price": "price text like 5.000 TL or Özel teklif",
            "price_note": "any price notes",
            "summary": "1-2 sentence description",
            "delivery_time": "delivery time if mentioned"
        }}
    ],
    "tone_notes": "How the business communicates with customers",
    "booking_type": "What kind of appointment/booking they need"
}}

Website content:
{site_text[:15000]}

IMPORTANT: Return ONLY valid JSON. No markdown, no explanations. If no services found, return {{"services": []}}."""

    result = llm_call(prompt)
    # Try to parse JSON from response
    result = result.strip()
    # Remove markdown code fences if any
    result = re.sub(r"^```(?:json)?\s*", "", result)
    result = re.sub(r"\s*```$", "", result)
    try:
        data = json.loads(result)
    except json.JSONDecodeError:
        logger.warning("scrape_llm_parse_failed raw=%s", result[:200])
        data = {"business_name": "", "services": []}
    # Normalize to our config format
    return _normalize_to_config(data)

def _normalize_to_config(llm_data: dict[str, Any]) -> dict[str, Any]:
    """Convert LLM extraction to tenant config format."""
    services_raw = llm_data.get("services", [])
    service_catalog = []
    for svc in services_raw:
        if not svc.get("display") and not svc.get("slug"):
            continue
        entry = {
            "slug": svc.get("slug", svc.get("display", "unknown").lower().replace(" ", "-")),
            "display": svc.get("display") or svc.get("slug", ""),
            "keywords": svc.get("keywords", [svc.get("display", "").lower()]),
            "price": svc.get("price", "Özel teklif"),
            "price_note": svc.get("price_note", ""),
            "summary": svc.get("summary", ""),
        }
        if svc.get("delivery_time"):
            entry["delivery_time"] = svc["delivery_time"]
        service_catalog.append(entry)

    config = {
        "business_name": llm_data.get("business_name") or "İşletme Adı",
        "business_type": llm_data.get("business_type") or "",
        "business_tagline": llm_data.get("business_tagline") or "",
        "booking_mode": "ön görüşme",
        "service_catalog": service_catalog,
        "tone": llm_data.get("tone_notes") or "professionally helpful, direct, reassuring",
    }
    return config

# ── Main entry ──────────────────────────────────────────────────────

def scrape_url_to_config(url: str, llm_call: callable) -> dict[str, Any]:
    """Full pipeline: fetch → extract → normalize.
    
    Returns config dict ready to save as tenant config.
    """
    site_text = fetch_website_text(url)
    if not site_text or len(site_text.strip()) < 50:
        return {"business_name": "İşletme (içerik taranamadı)", "service_catalog": []}
    config = extract_business_config(site_text, llm_call)
    config["_scraped_from"] = url
    return config
