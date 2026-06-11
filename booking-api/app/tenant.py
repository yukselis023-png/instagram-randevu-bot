"""Multi-tenant: each agency/client gets isolated config + brand."""
from __future__ import annotations
import json, os, re, logging, time, hashlib, hmac
from typing import Any
from datetime import datetime, timezone
from urllib.parse import urlparse

logger = logging.getLogger("tenant")

# ── In-memory tenant cache ──────────────────────────────────────────
_tenant_cache: dict[str, dict[str, Any]] = {}
_tenant_cache_ts: float = 0
TENANT_CACHE_TTL: float = 60.0  # seconds

# ── Default tenant (backward compat) ────────────────────────────────
DEFAULT_TENANT_SLUG = "doel"

def _load_default_config() -> dict[str, Any]:
    """Load doel.json as default tenant config."""
    cfg_path = os.path.join(os.path.dirname(__file__), "config", "doel.json")
    try:
        with open(cfg_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"business_name": "DOEL Digital", "service_catalog": []}

DEFAULT_TENANT: dict[str, Any] = {
    "id": 1,
    "slug": DEFAULT_TENANT_SLUG,
    "brand_name": "DOEL Digital",
    "logo_url": "",
    "colors": {"primary": "#1a1a2e", "secondary": "#e94560"},
    "config": _load_default_config(),
    "channels": ["instagram_dm", "whatsapp", "webchat"],
    "actions": ["appointment", "call", "visit", "form"],
    "created_at": "2026-01-01T00:00:00Z",
}

# ── Tenant resolvers ────────────────────────────────────────────────

def resolve_tenant(conn: Any, slug: str | None) -> dict[str, Any]:
    """Resolve tenant by slug, falling back to default."""
    if not slug or slug == DEFAULT_TENANT_SLUG:
        return DEFAULT_TENANT

    # Check cache
    now = time.time()
    if slug in _tenant_cache and (now - _tenant_cache_ts) < TENANT_CACHE_TTL:
        return _tenant_cache[slug]

    try:
        row = None
        if conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, slug, brand_name, logo_url, colors, config, channels, actions, created_at FROM tenants WHERE slug = %s", (slug,))
                row = cur.fetchone()
        if row:
            tenant = {
                "id": row["id"],
                "slug": row["slug"],
                "brand_name": row["brand_name"],
                "logo_url": row["logo_url"] or "",
                "colors": row["colors"] if isinstance(row.get("colors"), dict) else json.loads(row.get("colors") or "{}"),
                "config": row["config"] if isinstance(row.get("config"), dict) else json.loads(row.get("config") or "{}"),
                "channels": row["channels"] if isinstance(row.get("channels"), list) else json.loads(row.get("channels") or "[]"),
                "actions": row["actions"] if isinstance(row.get("actions"), list) else json.loads(row.get("actions") or "[]"),
                "created_at": str(row["created_at"]),
            }
            _tenant_cache[slug] = tenant
            return tenant
    except Exception as exc:
        logger.warning("tenant_resolve_failed slug=%s error=%s", slug, exc)

    # DB'de bulunamadıysa config JSON dosyasını dene
    cfg_path = os.path.join(os.path.dirname(__file__), "config", f"{slug}.json")
    try:
        if os.path.exists(cfg_path):
            with open(cfg_path, encoding="utf-8") as f:
                config_data = json.load(f)
            tenant = {
                "id": 0,
                "slug": slug,
                "brand_name": config_data.get("business_name", slug.capitalize()),
                "logo_url": "",
                "colors": {"primary": "#1a1a2e", "secondary": "#e94560"},
                "config": config_data,
                "channels": ["instagram_dm", "whatsapp", "webchat"],
                "actions": ["appointment", "call", "visit", "form"],
                "created_at": "2026-01-01T00:00:00Z",
            }
            _tenant_cache[slug] = tenant
            return tenant
    except Exception as exc:
        logger.warning("tenant_config_file_failed slug=%s error=%s", slug, exc)

    return dict(DEFAULT_TENANT)  # fallback

def create_tenant(conn: Any, slug: str, brand_name: str, config: dict | None = None) -> dict[str, Any]:
    """Create a new tenant."""
    import json
    config = config or {"business_name": brand_name, "service_catalog": []}
    default_channels = ["instagram_dm", "whatsapp", "webchat"]
    default_actions = ["appointment", "call", "visit", "form"]
    default_colors = {"primary": "#1a1a2e", "secondary": "#e94560"}
    with conn.cursor() as cur:
        cur.execute(
            """INSERT INTO tenants (slug, brand_name, logo_url, colors, config, channels, actions)
               VALUES (%s, %s, %s, %s, %s, %s, %s)
               ON CONFLICT (slug) DO UPDATE SET
               brand_name = EXCLUDED.brand_name, config = EXCLUDED.config
               RETURNING id, slug, brand_name, created_at""",
            (slug, brand_name, "", json.dumps(default_colors), json.dumps(config), json.dumps(default_channels), json.dumps(default_actions)),
        )
        row = cur.fetchone()
    conn.commit()
    _tenant_cache.pop(slug, None)
    return {"id": row["id"], "slug": row["slug"], "brand_name": row["brand_name"]}

def get_tenant_from_header(headers: dict[str, str]) -> str:
    """Extract tenant slug from X-Tenant header or path prefix."""
    slug = headers.get("x-tenant", "") or headers.get("X-Tenant", "") or DEFAULT_TENANT_SLUG
    return slug.strip().lower()

def get_tenant_from_host(host: str) -> str | None:
    """Extract tenant from subdomain: {tenant}.doelai.com"""
    host = (host or "").split(":")[0].lower()
    parts = host.split(".")
    if len(parts) >= 3 and parts[-2] == "doelai":
        return parts[0]
    return None

def tenant_config_for_service(tenant: dict[str, Any], service_slug: str) -> dict | None:
    """Get service config from tenant's service catalog."""
    catalog = tenant.get("config", {}).get("service_catalog", [])
    for svc in catalog:
        if svc.get("slug") == service_slug:
            return svc
        if service_slug in [k.lower().replace(" ", "-") for k in svc.get("keywords", [])]:
            return svc
    return None

def tenant_configured_actions(tenant: dict[str, Any]) -> list[str]:
    """Get enabled action types for this tenant."""
    return tenant.get("actions", ["appointment", "call", "visit", "form"])

def tenant_channel_enabled(tenant: dict[str, Any], channel: str) -> bool:
    """Check if channel is enabled for this tenant."""
    return channel in tenant.get("channels", [])
