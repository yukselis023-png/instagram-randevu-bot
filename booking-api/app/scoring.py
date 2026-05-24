"""Lead scoring & qualification.

Score signals (0-100):
  +10  Specific service mention
  +15  Price/budget mention
  +20  Time urgency ("acil", "hemen", "bugün")
  +10  Multiple messages in short time
  +5   Self-identification (name, phone)
  +10  Booking intent ("randevu", "görüşme")
  -10  Vague/generic query
  -5   Price complaint
  -20  Competitor mention
"""
from __future__ import annotations
import re, logging
from typing import Any

logger = logging.getLogger("scoring")

URGENCY_WORDS = re.compile(r"\b(?:acil|hemen|bugün|bugun|yarın|yarin|en kısa|en kisa|süreli|limiti|kaçır|kacir|firsat|fırsat)\b", re.IGNORECASE)
SPECIFIC_SERVICE = re.compile(r"\b(?:web tasarım|web tasarim|sosyal medya|reklam|otomasyon|danışmanlık|danismanlik|seo|e-ticaret|eticaret|mobil uygulama|yapay zeka)\b", re.IGNORECASE)
BUDGET = re.compile(r"\b(?:fiyat|ucret|ücret|bütçe|butce|tl|usd|eur|dolar|para|maliyet|ne kadar|kaç tl|kac tl|ödem|odem|taksit)\b", re.IGNORECASE)
SELF_ID = re.compile(r"\b(?:adım|adim|ben|ismim|telefon|tel|benim adım)\b", re.IGNORECASE)
BOOK_INTENT = re.compile(r"\b(?:randevu|görüşme|gorusme|ön görüşme|on gorusme|toplantı|toplanti|planlayalım|planlayalim|kayıt|kayit|oluştur|olustur)\b", re.IGNORECASE)
VAGUE = re.compile(r"\b(?:merhaba|selam|iyi günler|iyi gunler|nasılsın|nasilsin|yardım|yardim|bilgi|soru)\b", re.IGNORECASE)
PRICE_COMPLAINT = re.compile(r"\b(?:pahalı|pahali|çok pahalı|cok pahali|bütçem yok|bütçem yetmez|butcem yok|ucuz|indirim|fazla)\b", re.IGNORECASE)
COMPETITOR = re.compile(r"\b(?:rakib|başka|baska|şirket|sirket|başka firma)\b", re.IGNORECASE)

def score_conversation_start(message_text: str) -> int:
    """Initial score based on first message."""
    score = 50  # baseline neutral
    text = message_text or ""
    lowered = text.lower()

    if SPECIFIC_SERVICE.search(text):
        score += 15
    if BUDGET.search(text):
        score += 10
    if URGENCY_WORDS.search(text):
        score += 20
    if SELF_ID.search(text):
        score += 5
    if BOOK_INTENT.search(text):
        score += 15
    if VAGUE.search(text) and len(text.split()) < 4:
        score -= 10
    if PRICE_COMPLAINT.search(text):
        score -= 10
    if COMPETITOR.search(text):
        score -= 15

    return max(0, min(100, score))

def score_conversation(conversation: dict[str, Any], message_text: str | None, message_count: int) -> int:
    """Full conversation score. Call after each message."""
    base = score_conversation_start(message_text or "")
    # Engagement bonus
    if message_count >= 3:
        base += 10
    if message_count >= 6:
        base += 5
    # Booking signal
    if conversation.get("appointment_id"):
        base += 20
    if conversation.get("state") in ("collect_name", "collect_phone", "collect_datetime", "completed"):
        base += 10
    return max(0, min(100, base))

def score_label(score: int) -> str:
    """Convert numeric score to label."""
    if score >= 80:
        return "hot"
    elif score >= 50:
        return "warm"
    else:
        return "cold"

def score_to_priority(score: int) -> int:
    """1=highest priority, 3=lowest."""
    if score >= 80:
        return 1
    elif score >= 50:
        return 2
    return 3

def format_score_for_crm(score: int, label: str) -> dict[str, Any]:
    """Payload for CRM score sync."""
    return {
        "lead_score": score,
        "lead_label": label,
        "priority": score_to_priority(score),
    }
