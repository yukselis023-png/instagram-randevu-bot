"""WhatsApp Cloud API integration.

Architecture:
  /api/channel/whatsapp  ← Meta webhook (GET=verify, POST=inbound)
  → normalize to internal message format
  → process_instagram_message_generic (reused from IG)
  → send reply via WhatsApp API
"""
from __future__ import annotations
import json, logging, os, hmac, hashlib
from typing import Any, Callable
import requests

logger = logging.getLogger("whatsapp")

WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")       # Meta app token
WHATSAPP_VERIFY = os.getenv("WHATSAPP_VERIFY", "doelai_verify")  # webhook verify token
WHATSAPP_PHONE_ID = os.getenv("WHATSAPP_PHONE_ID", "")  # Business phone number ID
WHATSAPP_APP_SECRET = os.getenv("META_APP_SECRET", os.getenv("FACEBOOK_APP_SECRET", ""))  # For signature verification
WHATSAPP_API_VERSION = "v22.0"
WHATSAPP_BASE = "https://graph.facebook.com"

# ── Webhook verification (Meta handshake) ───────────────────────────

def verify_webhook(mode: str | None, token: str | None, challenge: str | None) -> tuple[int, str | None]:
    """GET /api/channel/whatsapp?hub.mode=subscribe&hub.verify_token=...&hub.challenge=..."""
    if mode == "subscribe" and token == WHATSAPP_VERIFY:
        return 200, challenge
    return 403, None

def verify_signature(raw_body: bytes, signature_header: str | None) -> bool:
    """Verify X-Hub-Signature-256 against raw request body using Meta App Secret."""
    if not signature_header or not WHATSAPP_APP_SECRET:
        return True  # skip if not configured
    expected = hmac.new(WHATSAPP_APP_SECRET.encode(), raw_body, hashlib.sha256).hexdigest()
    prefix = "sha256="
    if signature_header.startswith(prefix):
        received = signature_header[len(prefix):]
        return hmac.compare_digest(expected, received)
    return False

# ── Inbound message parser ─────────────────────────────────────────

def parse_whatsapp_inbound(body: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse Meta webhook payload into normalized internal messages."""
    messages: list[dict[str, Any]] = []
    entries = body.get("entry", [])
    for entry in entries:
        changes = entry.get("changes", [])
        for change in changes:
            value = change.get("value", {})
            if value.get("messaging_product") != "whatsapp":
                continue
            # Phone number ID that received this message — used for tenant resolution
            metadata = value.get("metadata", {})
            recipient_phone_id = metadata.get("phone_number_id", "")
            for msg in value.get("messages", []):
                normalized = {
                    "sender_id": f"wa:{msg.get('from', 'unknown')}",
                    "instagram_username": msg.get("from", ""),
                    "message_text": _extract_text(msg),
                    "message_id": msg.get("id", ""),
                    "platform": "whatsapp",
                    "raw_event": {
                        "id": msg.get("id", ""),
                        "from": msg.get("from", ""),
                        "type": msg.get("type", ""),
                        "timestamp": msg.get("timestamp", ""),
                        "source": "whatsapp_webhook",
                        "platform": "whatsapp",
                        "recipient_phone_id": recipient_phone_id,
                    },
                    "wa_profile": value.get("contacts", [{}])[0].get("profile", {}),
                    "recipient_phone_id": recipient_phone_id,
                }
                messages.append(normalized)
    return messages

def _extract_text(msg: dict[str, Any]) -> str:
    msg_type = msg.get("type", "text")
    if msg_type == "text":
        return msg.get("text", {}).get("body", "")
    if msg_type == "interactive":
        interactive = msg.get("interactive", {})
        if interactive.get("type") == "button_reply":
            return interactive.get("button_reply", {}).get("title", "")
        if interactive.get("type") == "list_reply":
            return interactive.get("list_reply", {}).get("title", "")
    return ""

# ── Send WhatsApp message ──────────────────────────────────────────

def send_whatsapp_message(to: str, text: str, token: str | None = None, phone_id: str | None = None) -> bool:
    """Send text message via WhatsApp Cloud API.
    Supports per-tenant credentials; falls back to env vars."""
    _token = token or WHATSAPP_TOKEN
    _phone_id = phone_id or WHATSAPP_PHONE_ID
    if not _token or not _phone_id:
        logger.warning("whatsapp_not_configured")
        return False
    url = f"{WHATSAPP_BASE}/{WHATSAPP_API_VERSION}/{_phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {_token}",
        "Content-Type": "application/json",
    }
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": to.replace("wa:", ""),
        "type": "text",
        "text": {"preview_url": False, "body": text},
    }
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=15)
        if resp.status_code not in (200, 201):
            logger.error("whatsapp_send_failed status=%s body=%s", resp.status_code, resp.text[:300])
            return False
        return True
    except Exception as exc:
        logger.error("whatsapp_send_error %s", exc)
        return False

# ── Full inbound handler ────────────────────────────────────────────

def handle_whatsapp_inbound(
    body: dict[str, Any],
    process_fn: Callable,
    background_tasks: Any,
) -> list[dict[str, Any]]:
    """Process incoming WhatsApp webhook with per-tenant credential resolution."""
    messages = parse_whatsapp_inbound(body)
    results = []
    
    # Resolve tenant credentials from first message's recipient_phone_id
    _tenant_wa_token = None
    _tenant_wa_phone_id = None
    if messages:
        phone_id = messages[0].get("recipient_phone_id", "")
        if phone_id:
            try:
                from app.tenant import resolve_tenant
                from app.main import get_conn
                with get_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(
                            "SELECT slug, channels FROM tenants WHERE channels->>'phone_number_id' = %s LIMIT 1",
                            (phone_id,),
                        )
                        row = cur.fetchone()
                        if row:
                            channels = row.get("channels", {})
                            if isinstance(channels, str):
                                channels = json.loads(channels)
                            _tenant_wa_token = channels.get("access_token") or channels.get("token")
                            _tenant_wa_phone_id = channels.get("phone_number_id") or phone_id
            except Exception:
                logger.warning("whatsapp_tenant_resolve_failed phone_id=%s", phone_id)
    
    for msg in messages:
        class FakePayload:
            def __init__(self, d):
                self.sender_id = d["sender_id"]
                self.instagram_username = d["instagram_username"]
                self.message_text = d["message_text"]
                self.message_id = d["message_id"]
                self.trace_id = d["message_id"]
                self.recipient_id = None
                self.raw_event = d["raw_event"]
        payload = FakePayload(msg)
        try:
            result = process_fn(payload, background_tasks)
            if result and hasattr(result, "reply_text") and result.should_reply:
                wa_number = msg["sender_id"].replace("wa:", "")
                send_whatsapp_message(wa_number, result.reply_text, token=_tenant_wa_token, phone_id=_tenant_wa_phone_id)
            results.append({
                "sender": msg["sender_id"],
                "processed": bool(result),
                "reply_sent": bool(result and hasattr(result, "reply_text") and result.should_reply) if result else False,
            })
        except Exception as exc:
            logger.error("whatsapp_process_error sender=%s error=%s", msg["sender_id"], exc)
            results.append({"sender": msg["sender_id"], "error": str(exc)})
    return results
