"""Human handoff: detect handoff request → Telegram notification.

Triggers:
  - Customer explicitly asks for human ("insanla konuşmak istiyorum")
  - LLM confidence below threshold (future)
  - Escalation keywords

Telegram bot sends notification with conversation summary.
"""
from __future__ import annotations
import re, logging, os
from typing import Any
import requests

logger = logging.getLogger("handoff")

# ── Detection ───────────────────────────────────────────────────────

HANDOFF_PATTERNS = [
    r"\b(?:insanla|yetkili|müdür|midur|yönetici|yonetici|patron|sorumlu)\b.{0,50}\b(?:konuş|konus|bağla|bagla|görüş|goruş|gorus|ulaş|ulas|bağlan|baglan|devral|aktar)\b",
    r"\b(?:beni (?:bir )?insana bağla|beni (?:bir )?yetkiliye bağla)\b",
    r"\b(?:müdürle|midurle|yetkiliyle|yöneticiyle|yoneticiyle)\b.{0,30}\b(?:görüş|goruş|gorus|konuş|konus)\b",
    r"\b(?:robot değil|bot değil|yapay zeka değil|ai değil|otomatik değil)\b",
    r"\b(?:gerçek|gercek|reel|canlı)\b.{0,20}\b(?:insan|kişi|kisi|yetkili)\b",
    r"\binsan(?:la)?\b.{0,30}(?:goruş|goruşmek|gorusmek|konuşmak|konusmak)\b",
    r"\b(?:operator|operatör|musteri hizmet|müşteri hizmet|destek)\b.{0,30}(?:bağla|bagla|ulaş|ulas|ara)\b",
]

HANDOFF_RE = re.compile("|".join(HANDOFF_PATTERNS), re.IGNORECASE)

def is_handoff_request(message_text: str) -> bool:
    """Check if customer is requesting human handoff."""
    return bool(HANDOFF_RE.search(message_text or ""))

# ── Telegram bot ────────────────────────────────────────────────────

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")  # Who gets notified

def send_telegram_handoff(
    tenant_name: str,
    customer_name: str,
    customer_phone: str | None,
    channel: str,
    summary: str,
    conversation_url: str | None = None,
) -> bool:
    """Send handoff notification to Telegram."""
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("telegram_not_configured")
        return False

    text = (
        f"🔔 *Handoff Talebi*\n"
        f"• Müşteri: {tenant_name}\n"
        f"• Kişi: {customer_name}\n"
        f"• Telefon: {customer_phone or 'Yok'}\n"
        f"• Kanal: {channel}\n"
        f"• Özet: {summary[:200]}\n"
    )
    if conversation_url:
        text += f"• [Konuşma]({conversation_url})\n"

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = requests.post(url, json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }, timeout=10)
        if resp.status_code != 200:
            logger.error("telegram_send_failed %s", resp.text[:200])
            return False
        return True
    except Exception as exc:
        logger.error("telegram_send_error %s", exc)
        return False

def build_handoff_reply(tenant: dict[str, Any]) -> str:
    """Build reply telling customer they'll be connected to human."""
    brand = tenant.get("brand_name") or tenant.get("config", {}).get("business_name", "Ekibimiz")
    return f"Size yardımcı olması için sizi {brand} ekibinden bir yetkiliye yönlendiriyorum. En kısa sürede size dönüş yapacaklar."
