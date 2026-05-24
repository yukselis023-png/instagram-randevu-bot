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
WHATSAPP_API_VERSION = "v22.0"
WHATSAPP_BASE = "https://graph.facebook.com"

# ── Webhook verification (Meta handshake) ───────────────────────────

def verify_webhook(mode: str | None, token: str | None, challenge: str | None) -> tuple[int, str | None]:
    """GET /api/channel/whatsapp?hub.mode=subscribe&hub.verify_token=...&hub.challenge=..."""
    if mode == "subscribe" and token == WHATSAPP_VERIFY:
        return 200, challenge
    return 403, None

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
                    },
                    "wa_profile": value.get("contacts", [{}])[0].get("profile", {}),
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
    """Process incoming WhatsApp webhook. Returns list of ProcessResults."""
    messages = parse_whatsapp_inbound(body)
    results = []
    for msg in messages:
        # Wrap as IncomingMessage-like dict for process_fn
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
                # Extract WA number from sender_id (strip "wa:" prefix)
                wa_number = msg["sender_id"].replace("wa:", "")
                send_whatsapp_message(wa_number, result.reply_text)
            results.append({
                "sender": msg["sender_id"],
                "processed": bool(result),
                "reply_sent": bool(result and hasattr(result, "reply_text") and result.should_reply) if result else False,
            })
        except Exception as exc:
            logger.error("whatsapp_process_error sender=%s error=%s", msg["sender_id"], exc)
            results.append({"sender": msg["sender_id"], "error": str(exc)})
    return results
