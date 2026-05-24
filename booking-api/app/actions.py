"""Multi-action routing: detect customer intent → route to action.

Actions:
  appointment → existing booking pipeline
  call       → collect phone → queue callback notification
  visit      → collect address → notify team
  form       → send form link → webhook on submit

Each tenant can enable/disable actions in config.
"""
from __future__ import annotations
import re, logging
from typing import Any, Callable

logger = logging.getLogger("actions")

# ── Intent detection ────────────────────────────────────────────────

CALL_INTENT = re.compile(
    r"\b(?:beni ara|bana ulaş|bana ulas|telefon et|telefonla ara|arayın|arayin|aramamı ister|aramani istiyorum|goruntulu|görüntülü)\b",
    re.IGNORECASE,
)
VISIT_INTENT = re.compile(
    r"\b(?:ziyaret|gelmek istiyorum|yüz yüze|yuz yuze|ofisiniz|adresiniz|neredesiniz|gelip|bizzat)\b",
    re.IGNORECASE,
)
FORM_INTENT = re.compile(
    r"\b(?:form|başvuru|basvuru|teklif formu|bilgi formu|kayıt formu|kayit formu|başvuru formu|basvuru formu)\b",
    re.IGNORECASE,
)
APPOINTMENT_INTENT = re.compile(
    r"\b(?:randevu|görüşme|gorusme|ön görüşme|on gorusme|toplantı|toplanti|planlayalım|planlayalim)\b",
    re.IGNORECASE,
)

def detect_action_intent(message_text: str) -> str | None:
    """Detect which action the customer wants. Returns action type or None."""
    text = message_text or ""
    intent_order = ["call", "visit", "form", "appointment"]

    # Check in priority order (call is most urgent)
    if CALL_INTENT.search(text):
        return "call"
    if VISIT_INTENT.search(text):
        return "visit"
    if FORM_INTENT.search(text):
        return "form"
    return None  # Default to appointment (handled by booking pipeline)

# ── Action handlers ─────────────────────────────────────────────────

def build_call_action_reply(tenant: dict[str, Any], conversation: dict[str, Any]) -> str:
    """Customer wants a phone call. Collect/normalize phone + notify team."""
    name = conversation.get("full_name") or conversation.get("instagram_username") or "Müşteri"
    phone = conversation.get("phone") or ""
    if not phone:
        return "Sizi arayabilmemiz için telefon numaranızı yazar mısınız?"
    brand = tenant.get("brand_name", "Ekibimiz")
    return f"{name} Bey, {phone} numaranızdan sizi en kısa sürede arayacağız. {brand} olarak size dönüş sağlayacağız."

def build_visit_action_reply(tenant: dict[str, Any], conversation: dict[str, Any]) -> str:
    """Customer wants to visit. Collect address info."""
    address = conversation.get("address", "")
    if not address:
        return "Ziyaret için adresinizi veya bulunduğunuz bölgeyi yazar mısınız?"
    brand = tenant.get("business_name", "Ekibimiz")
    return f"Adresiniz not alındı. {brand} ekibimiz size dönüş yaparak ziyaret detaylarını planlayacak."

def build_form_action_reply(form_url: str = None) -> str:
    """Send form link."""
    url = form_url or "https://forms.gle/example"
    return f"Detaylı bilgi için formumuzu doldurabilir misiniz? {url}"

def route_to_action(
    action_type: str,
    tenant: dict[str, Any],
    conversation: dict[str, Any],
    default_booking_fn: Callable,
) -> tuple[str, str | None]:
    """Route to appropriate action. Returns (reply_text, action_used)."""
    if action_type == "call":
        reply = build_call_action_reply(tenant, conversation)
        return reply, "call"
    elif action_type == "visit":
        reply = build_visit_action_reply(tenant, conversation)
        return reply, "visit"
    elif action_type == "form":
        reply = build_form_action_reply()
        return reply, "form"
    return default_booking_fn(), "appointment"

def is_action_enabled(tenant: dict[str, Any], action: str) -> bool:
    """Check if action is enabled for tenant."""
    actions = tenant.get("actions", ["appointment"])
    return action in actions
