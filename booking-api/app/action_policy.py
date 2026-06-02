"""Deterministic action policy for the Instagram bot.

LLM never decides database mutations. This module classifies user text into
explicit actions and decides what the bot may do.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Any


def sanitize_text(value: str | None) -> str:
    text = str(value or "")
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("İ", "i").replace("I", "ı")
    text = re.sub(r"\s+", " ", text).strip()
    return text


GREETING_NAME_PATTERNS = (
    re.compile(r"^ben\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)(?:\s+(?:bey|han[ıi]m))?$", re.IGNORECASE),
    re.compile(r"^ad[ıi]m\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)(?:\s+(?:bey|han[ıi]m))?$", re.IGNORECASE),
    re.compile(r"^benim\s+ad[ıi]m\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)(?:\s+(?:bey|han[ıi]m))?$", re.IGNORECASE),
    re.compile(r"^merhaba,?\s+ben\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)(?:\s+(?:bey|han[ıi]m))?$", re.IGNORECASE),
)

EXPLICIT_NAME_CHANGE_PATTERNS = (
    re.compile(r"ben\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,}\s+de[ğg]il(?:im)?[,\s]+(?:ad[ıi]m|ismim)\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)", re.IGNORECASE),
    re.compile(r"(?:ad[ıi]m[ıi]|ismimi)\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)\s+olarak\s+(?:de[ğg]i[şs]tir|g[üu]ncelle)", re.IGNORECASE),
    re.compile(r"(?:ad[ıi]n[ıi]z[ıi]|ismimi)\s+([A-ZÇĞİÖŞÜa-zçğıöşü]{2,}(?:\s+[A-ZÇĞİÖŞÜa-zçğıöşü]{2,})?)\s+olarak", re.IGNORECASE),
    re.compile(r"isim\s+de[ğg]i[şs]ik", re.IGNORECASE),
    re.compile(r"ismimi\s+de[ğg]i[şs]tir", re.IGNORECASE),
    re.compile(r"ismimi\s+g[üu]ncelle", re.IGNORECASE),
    re.compile(r"ad[ıi]n[ıi]\s+de[ğg]i[şs]tir", re.IGNORECASE),
    re.compile(r"kayd[ıi]n[ıi]\s+g[üu]ncelle", re.IGNORECASE),
)

CANCEL_PATTERNS = (
    re.compile(r"\brandev(u|um|u)?(y[ıi])?\s*(iptal|iptal\s*et|sil|vazge[çc])", re.IGNORECASE),
    re.compile(r"\biptal\s*et", re.IGNORECASE),
    re.compile(r"\bvazge[çc]tim", re.IGNORECASE),
    re.compile(r"\brandevuyu\s*iptal", re.IGNORECASE),
    re.compile(r"\brandevu\s*iptal", re.IGNORECASE),
)

RESCHEDULE_PATTERNS = (
    re.compile(r"\b(randevu(m)?(y[ıi]|yu)?)\s*(de[ğg]i[şs]tir|tarih\s*de[ğg]i[şs]tir|saat\s*de[ğg]i[şs]tir|ta[şs][ıi]|ertele)", re.IGNORECASE),
    re.compile(r"\b(randevu(m)?(y[ıi]|yu)?)\s*([\d]{1,2}[.:][\d]{2}|saat\s*[\d]{1,2}|yar[ıi]n|bug[üu]n)"),
    re.compile(r"\bsaati\s*([\d]{1,2}[.:][\d]{2})"),
    re.compile(r"\b([\d]{1,2}\s*haziran)\s*saat\s*([\d]{1,2})", re.IGNORECASE),
)

NEW_BOOKING_PATTERNS = (
    re.compile(r"\byeni\s+randevu", re.IGNORECASE),
    re.compile(r"\byeniden\s+randevu", re.IGNORECASE),
    re.compile(r"\btekrar\s+randevu", re.IGNORECASE),
    re.compile(r"\bbir\s+randevu\s+daha", re.IGNORECASE),
    re.compile(r"\bba[şs]ka\s+bir\s+randevu", re.IGNORECASE),
    re.compile(r"\bba[şs]tan\s+randevu", re.IGNORECASE),
    re.compile(r"\brandevu\s+olu[şs]tur", re.IGNORECASE),
    re.compile(r"\brandevu\s+olustur", re.IGNORECASE),
)

RECALL_PATTERNS = (
    re.compile(r"\brandevu(m)?(um)?(u)?\s+vard[ıi]", re.IGNORECASE),
    re.compile(r"\bhat[ıi]rl[ıi]yor\s+musun", re.IGNORECASE),
    re.compile(r"\bhat[ıi]rla(t?)?(y?[ıi]r)?\s*m[ıi]s[ıi]n", re.IGNORECASE),
    re.compile(r"\baktif\s+randevu", re.IGNORECASE),
    re.compile(r"\bmevcut\s+randevu", re.IGNORECASE),
    re.compile(r"\brandevu(m)?\s+ne\s+zaman", re.IGNORECASE),
    re.compile(r"\bs[ıi]radaki\s+randevu", re.IGNORECASE),
    re.compile(r"\byakla[şs]an\s+randevu", re.IGNORECASE),
    re.compile(r"\b[öo]nceki\s+randevu", re.IGNORECASE),
)


def _extract_pattern_name(message_text: str, patterns) -> str | None:
    lowered = sanitize_text(message_text or "")
    for pat in patterns:
        match = pat.search(lowered)
        if match:
            candidate = match.group(1).strip().rstrip(".")
            parts = [p for p in candidate.split() if p]
            if 1 <= len(parts) <= 3:
                return " ".join(p.capitalize() for p in parts)
    return None


def classify_user_action(message_text: str) -> dict[str, Any]:
    """Classify user text into an explicit action category.

    Returns a dict with:
      - action: one of
          "greeting_with_name"
          "explicit_name_change"
          "cancel"
          "reschedule"
          "new_booking"
          "recall"
          "other"
      - proposed_name: name string if action implies name change
    """
    text = sanitize_text(message_text or "").strip()
    if not text:
        return {"action": "other", "proposed_name": None}

    if any(p.search(text) for p in CANCEL_PATTERNS):
        return {"action": "cancel", "proposed_name": None}

    if any(p.search(text) for p in NEW_BOOKING_PATTERNS):
        return {"action": "new_booking", "proposed_name": None}

    if any(p.search(text) for p in RECALL_PATTERNS):
        return {"action": "recall", "proposed_name": None}

    if any(p.search(text) for p in RESCHEDULE_PATTERNS):
        return {"action": "reschedule", "proposed_name": None}

    name_for_change = _extract_pattern_name(text, EXPLICIT_NAME_CHANGE_PATTERNS)
    if name_for_change:
        return {"action": "explicit_name_change", "proposed_name": name_for_change}

    name_for_greeting = _extract_pattern_name(text, GREETING_NAME_PATTERNS)
    if name_for_greeting:
        return {"action": "greeting_with_name", "proposed_name": name_for_greeting}

    return {"action": "other", "proposed_name": None}


def should_allow_appointment_name_update(action: dict[str, Any], message_text: str) -> bool:
    """Allow appointment/customer name update ONLY on explicit name change request.

    Greeters like 'Ben Mehmet bey' must NOT mutate the appointment/customer.
    """
    return action.get("action") == "explicit_name_change"
