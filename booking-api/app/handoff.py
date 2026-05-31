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
    r"\b(?:insan(?:la|a)?|yetkili(?:yle|ye)?|müdür(?:le|e)?|midur(?:le|e)?|yönetici(?:yle|ye)?|yonetici(?:yle|ye)?|patron|sorumlu)\b.{0,60}\b(?:konuş(?:mak|abilir|uruz|ürüz)?|konus(?:mak|abilir)?|bağla(?:r|n|m|abilir)?|bagla(?:r|m|abilir)?|görüş(?:mek|ebilir|ürüz|tür)?|goruş(?:mek|ebilir|tur|turuz)?|gorus(?:mek|ebilir|tur|turuz)?|ulaş|ulas|devral|aktar)\b",
    r"\b(?:beni (?:bir )?(?:insana|yetkiliye) bağla(?:r)?|beni (?:bir )?insanla görüştür|beni (?:bir )?insanla gorustur)\b",
    r"\b(?:müdürle|midurle|yetkiliyle|yöneticiyle|yoneticiyle|müşteri temsilcisiyle|musteri temsilcisiyle)\b.{0,40}\b(?:görüş(?:mek|ebilir|ürüz)?|goruş(?:mek|ebilir|tur)?|gorus(?:mek|ebilir|tur)?|konuş(?:mak|ebilir|uruz|ürüz)?|konus(?:mak|ebilir)?|bağla(?:r)?|bagla(?:r)?)\b",
    r"\b(?:robot değil|bot değil|yapay zeka değil|ai değil|otomatik değil)\b",
    r"\b(?:gerçek|gercek|reel|canlı)\b.{0,25}\b(?:insan|kişi|kisi|yetkili)\b",
    r"\binsan(?:la|a)?\b.{0,40}\b(?:görüş(?:mek|tür)?|goruş(?:mek|tur)?|gorus(?:mek|tur)?|konuş(?:mak)?|konus(?:mak)?|destek)\b",
    r"\b(?:operatör|operator|müşteri hizmet|destek)\w{0,12}.{0,40}\b(?:bağla(?:r|n|m|abilir)?|bagla(?:r|m|abilir)?|ulaş|ulas|ara|görüş|gorus|konuş|konus)\w{0,6}\b",
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
