"""Website scraper: URL → AI tenant config."""
from __future__ import annotations

import json
import logging
import re
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger("scraper")


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip(" -–—•|\t")


def _slugify(value: str) -> str:
    tr = str.maketrans("çğıöşüÇĞİÖŞÜ", "cgiosuCGIOSU")
    raw = value.translate(tr).lower()
    return re.sub(r"[^a-z0-9]+", "-", raw).strip("-")[:70] or "hizmet"


def fetch_website_pages(url: str, max_pages: int = 8) -> list[dict[str, str]]:
    visited: set[str] = set()
    pages: list[dict[str, str]] = []
    to_visit = [url]
    base = urlparse(url)
    priority = ("hizmet", "service", "fiyat", "price", "paket", "pricing", "tedavi", "ürün", "urun")

    for target in to_visit:
        if len(visited) >= max_pages:
            break
        if target in visited:
            continue
        visited.add(target)
        try:
            resp = requests.get(target, timeout=15, headers={"User-Agent": "Mozilla/5.0 (compatible; DoelAI/1.0)"})
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            title = soup.title.get_text(" ", strip=True) if soup.title else ""
            for tag in soup(["script", "style", "noscript", "svg", "nav", "footer"]):
                tag.decompose()
            text = soup.get_text(separator="\n", strip=True)
            if len(text) > 80:
                pages.append({"url": target, "title": title, "html": str(soup)[:40000], "text": text[:20000]})
            links: list[tuple[int, str]] = []
            for a in soup.find_all("a", href=True):
                full = urljoin(target, a.get("href") or "")
                parsed = urlparse(full)
                if parsed.netloc != base.netloc or parsed.scheme not in ("http", "https") or parsed.fragment:
                    continue
                label = f"{a.get_text(' ', strip=True)} {parsed.path}".lower()
                if full not in visited and full not in to_visit:
                    links.append((0 if any(w in label for w in priority) else 1, full))
            for _, full in sorted(links)[:20]:
                if len(to_visit) < max_pages:
                    to_visit.append(full)
        except Exception as exc:
            logger.debug("scrape_skip %s %s", target, exc)
    return pages


def fetch_website_text(url: str, max_pages: int = 5) -> str:
    pages = fetch_website_pages(url, max_pages=max_pages)
    return "\n\n".join(f"--- Page: {p['url']} ---\n{p['text']}" for p in pages)


def _looks_like_service(name: str) -> bool:
    name = _clean(name)
    if not (3 <= len(name) <= 90):
        return False
    low = name.lower()
    bad = {"anasayfa", "iletişim", "iletisim", "hakkımızda", "hakkimizda", "gizlilik", "kvkk", "blog", "login", "giriş", "kayıt", "doel", "doel digital"}
    if low in bad or low.startswith(("http", "copyright")) or any(x in low for x in ("haksız rekabet", "haksiz rekabet", "sesi aç", "sesi ac", "temel değer", "temel deger", "haftanın trend", "haftanin trend", "ihtiyacın olan", "ihtiyacin olan", "ağzıma düş", "agzima dus", "kusursuz dengesi", "cmo,", "onayınızla", "onayinizla", "çalışıyor musunuz", "calisiyor musunuz", "genellikle", "adına", "adina", "bütçe", "butce")) :
        return False
    if "?" in name or len(name.split()) > 7:
        return False
    return bool(re.search(r"hizmet|paket|tasarım|tasarim|seo|reklam|sosyal|medya|web|yazılım|yazilim|otomasyon|crm|danışman|danisman|randevu|klinik|estetik|güzellik|guzellik|terapi|tedavi|bakım|bakim|e-ticaret|prodüksiyon|produksiyon|marka stratejisi|stratejik planlama|kreatif|temizlik|nakliyat|montaj|transfer|masaj", low, re.I))


def deterministic_service_extract(pages: list[dict[str, str]]) -> dict[str, Any]:
    price_re = re.compile(r"(?:₺|TL|TRY|USD|EUR|€|\$)\s?\d[\d\.,]*|\d[\d\.,]*\s?(?:TL|TRY|₺|USD|EUR|€|\$)", re.I)
    seen: set[str] = set()
    items: list[dict[str, str]] = []
    for page in pages:
        soup = BeautifulSoup(page.get("html", ""), "html.parser")
        blocks = soup.find_all(["section", "article", "li", "div"], limit=3000)
        for block in blocks:
            text = _clean(block.get_text(" ", strip=True))
            if not (8 <= len(text) <= 900):
                continue
            names = [_clean(h.get_text(" ", strip=True)) for h in block.find_all(["h1", "h2", "h3", "h4", "strong", "b"], limit=4)]
            if not names:
                names = [_clean(x) for x in block.get_text("\n", strip=True).splitlines()[:3]]
            price = (price_re.search(text).group(0) if price_re.search(text) else "Özel teklif")
            for name in names:
                key = name.lower()
                if key in seen or not _looks_like_service(name):
                    continue
                seen.add(key)
                items.append({"display": name, "price": price, "summary": text[:260]})
                break
            if len(items) >= 12:
                break
        if len(items) < 5:
            lines = [_clean(x) for x in page.get("text", "").splitlines() if _clean(x)]
            for i, line in enumerate(lines):
                key = line.lower()
                if key in seen or not _looks_like_service(line):
                    continue
                ctx = " ".join(lines[i:i+4])[:260]
                m = price_re.search(ctx)
                seen.add(key)
                items.append({"display": line, "price": m.group(0) if m else "Özel teklif", "summary": ctx})
                if len(items) >= 12:
                    break
    services = []
    for idx, item in enumerate(items[:10], 1):
        name = item["display"]
        services.append({"slug": _slugify(name) or f"service-{idx}", "display": name, "keywords": [w.lower() for w in re.split(r"\W+", name) if len(w) > 2][:8], "price": item["price"], "price_note": "", "summary": item["summary"]})
    title = next((_clean(p.get("title")) for p in pages if _clean(p.get("title"))), "İşletme")
    return {"business_name": title, "business_type": "", "business_tagline": "", "booking_mode": "ön görüşme", "service_catalog": services, "tone": "professionally helpful, direct, reassuring"}


def extract_business_config(site_text: str, llm_call: callable) -> dict[str, Any]:
    prompt = f"""Extract services/products with prices from website text. Return ONLY JSON: {{"business_name":"","services":[{{"slug":"","display":"","keywords":[],"price":"","price_note":"","summary":""}}]}}\n\n{site_text[:15000]}"""
    try:
        result = llm_call(prompt).strip()
        result = re.sub(r"^```(?:json)?\s*", "", result)
        result = re.sub(r"\s*```$", "", result)
        data = json.loads(result)
    except Exception as exc:
        logger.warning("scrape_llm_failed %s", exc)
        data = {"services": []}
    return _normalize_to_config(data)


def _normalize_to_config(llm_data: dict[str, Any]) -> dict[str, Any]:
    service_catalog = []
    for svc in llm_data.get("services", []) or []:
        display = svc.get("display") or svc.get("name") or svc.get("slug") or ""
        if not display:
            continue
        service_catalog.append({"slug": svc.get("slug") or _slugify(display), "display": display, "keywords": svc.get("keywords") or [display.lower()], "price": svc.get("price") or "Özel teklif", "price_note": svc.get("price_note") or "", "summary": svc.get("summary") or ""})
    return {"business_name": llm_data.get("business_name") or "İşletme", "business_type": llm_data.get("business_type") or "", "business_tagline": llm_data.get("business_tagline") or "", "booking_mode": "ön görüşme", "service_catalog": service_catalog, "tone": llm_data.get("tone_notes") or "professionally helpful, direct, reassuring"}


def scrape_url_to_config(url: str, llm_call: callable) -> dict[str, Any]:
    pages = fetch_website_pages(url)
    site_text = "\n\n".join(f"--- Page: {p['url']} ---\n{p['text']}" for p in pages)
    if not site_text or len(site_text.strip()) < 50:
        return {"business_name": "İşletme (içerik taranamadı)", "service_catalog": [], "_scraped_from": url}
    deterministic = deterministic_service_extract(pages)
    llm_config = extract_business_config(site_text, llm_call)
    config = llm_config if llm_config.get("service_catalog") else deterministic
    if llm_config.get("service_catalog") and len(llm_config["service_catalog"]) < len(deterministic.get("service_catalog", [])):
        config["service_catalog"] = deterministic["service_catalog"]
    if not config.get("business_name") or config.get("business_name") == "İşletme":
        config["business_name"] = deterministic.get("business_name") or "İşletme"
    config["_scraped_from"] = url
    return config
