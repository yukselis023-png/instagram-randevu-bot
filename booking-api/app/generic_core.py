import os
import json
import re
import datetime
import requests
import os
import logging
import time as time_module
from typing import Any, Tuple, Optional

from fastapi import BackgroundTasks
from app.main import (
    ProcessResult, IncomingMessage, get_conn, get_or_create_conversation, 
    sanitize_conversation_state, ensure_conversation_memory, 
    sync_conversation_memory_summary, has_processed_inbound_message,
    try_acquire_inbound_processing_lock, save_message_log, get_recent_message_history,
    should_reset_stale_conversation, reset_conversation_for_restart, build_normalized,
    update_conversation_memory_after_bot_reply, upsert_conversation, upsert_customer_from_conversation,
    schedule_customer_automation_events, sanitize_text, extract_inbound_message_id, extract_inbound_platform,
    build_inbound_dedupe_key, elapsed_ms, queue_crm_sync, get_config, call_llm_content,
    is_company_capability_question, build_company_capability_reply, is_user_business_identity_message, is_simple_greeting,
    is_business_fit_question, recommendation_engine, extract_name, extract_phone,
    is_invalid_phone_attempt, extract_date, extract_time_for_state, extract_time, create_appointment,
    build_confirmation_message, try_reschedule_confirmed_appointment, find_active_appointment_for_user,
    detect_customer_subsector, customer_sector_for_subsector, normalize_date_string, normalize_time_string,
    validate_slot, format_human_date, get_booking_label, TZ,
    collect_next_booking_slot_options, format_booking_slot_option, remember_booking_slot_options
)

logger = logging.getLogger(__name__)

BOOKING_OPT_IN_PHRASES = (
    "olur görüşelim",
    "olur goruselim",
    "görüşelim",
    "goruselim",
    "randevu alalım",
    "randevu alalim",
    "ön görüşme",
    "on gorusme",
)

SERVICE_REPEAT_QUESTION_FRAGMENTS = (
    "hangi hizmet",
    "hizmeti araştırıyorsunuz",
    "hizmeti arastiriyorsunuz",
    "hangi konuda bilgi",
)


def is_preconsultation_explanation_question(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    return (
        any(token in lowered for token in ("on gorusmede", "ön görüşmede", "on gorusme", "ön görüşme"))
        and any(token in lowered for token in ("ne konus", "ne konuş", "neler konus", "neler konuş", "ne olacak"))
    )


def is_active_booking_direct_clarification_question(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    if not lowered:
        return False
    if extract_phone(message_text) or extract_time(message_text) or extract_generic_datetime_time(message_text):
        return False
    if is_preconsultation_explanation_question(message_text) or is_price_question(message_text):
        return True
    direct_markers = (
        "kiminle", "kimle", "kim arayacak", "kim arar", "kim gorusecek", "kim görüşecek",
        "berkay", "anlamadim", "anlamadım", "hicbir sey", "hiçbir şey", "ne demek",
        "detay", "detaylari", "detayları", "anlatir misiniz", "anlatır mısınız",
        "nereden", "online", "video", "nasil gorusecegiz", "nasıl görüşeceğiz", "nasil gorusuruz", "nasıl görüşürüz", "odeme", "ödeme", "nasil yapiliyor", "nasıl yapılıyor",
        "ne kadar sure", "ne kadar süre", "kac dakika", "kaç dakika", "surecek", "sürecek",
        "sonradan", "sonra yazar", "daha sonra",
        "arayacaklar", "arayacak misiniz", "beni arayacak"
    )
    has_direct_marker = any(marker in lowered for marker in direct_markers)
    bare_active_clarifiers = (
        "kiminle", "kimle", "kim arayacak", "kim arar", "kim gorusecek", "kim görüşecek",
        "anlamadim", "anlamadım", "ne demek", "bu ne", "bu nasil", "bu nasıl",
        "nasil gorusecegiz", "nasıl görüşeceğiz", "nasil gorusuruz", "nasıl görüşürüz",
        "sonradan", "sonra yazar", "daha sonra",
        "beni arayacaklar", "arayacaklar"
    )
    if any(marker in lowered for marker in bare_active_clarifiers):
        return True
    if "berkay" in lowered and any(marker in lowered for marker in ("kim", " mi", " mı", " mu", " mü", "arayacak", "arar", "bey")):
        return True
    has_meeting_context = any(token in lowered for token in ("on gorus", "ön görüş", "gorus", "görüş", "randevu", "arama", "arayacak"))
    if has_direct_marker and (has_meeting_context or "odeme" in lowered or "ödeme" in lowered or "nereden" in lowered):
        return True
    return False


def is_booking_field_collection_reply(reply_text: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    if not lowered:
        return False
    asks_name = any(token in lowered for token in ("ad soyad", "adinizi", "adınızı", "isminizi", "ismininizi", "isminizi öğren", "isminizi ogren", "ismininizi öğren", "ismininizi ogren", "isim soyisim"))
    asks_phone = "telefon" in lowered and any(token in lowered for token in ("alabilir", "paylas", "paylaş", "yazar", "rica"))
    asks_datetime = any(token in lowered for token in ("uygun gun", "uygun gün", "uygun saat", "hangi saat", "gun ve saat", "gün ve saat"))
    return asks_name or asks_phone or asks_datetime


def reply_asks_for_collection_state(reply_text: str | None, state: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    if not lowered:
        return False
    if state == "collect_name":
        return any(token in lowered for token in ("ad soyad", "adinizi", "adınızı", "isminizi", "ismininizi", "isminizi öğren", "isminizi ogren", "ismininizi öğren", "ismininizi ogren", "isim soyisim"))
    if state == "collect_phone":
        return "telefon" in lowered and any(token in lowered for token in ("alabilir", "paylas", "paylaş", "yazar", "rica"))
    if state == "collect_datetime":
        return any(token in lowered for token in ("uygun gun", "uygun gün", "uygun saat", "hangi saat", "gun ve saat", "gün ve saat", "yarin", "yarın", "oglen", "öğlen"))
    return False


def can_preserve_valid_llm_reply_from_overwrite(reply_text: str | None, *, appointment_created: bool = False, appointment_id: Any = None) -> bool:
    clean = sanitize_text(reply_text or "").strip()
    if not clean or is_llm_error_reply(clean):
        return False
    if generic_llm_reply_rejection_reason(clean):
        return False
    if is_appointment_confirmation_like_reply(clean) and (not appointment_created or not appointment_id):
        return False
    return True


def build_active_direct_clarification_reply(message_text: str, cfg: dict[str, Any], conversation: dict[str, Any], memory: dict[str, Any]) -> str | None:
    return None


def is_booking_acknowledgement_message(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?")
    return lowered in {
        "olur",
        "olur goruselim",
        "olur görüşelim",
        "tamam goruselim",
        "tamam görüşelim",
        "goruselim",
        "görüşelim",
    }


def is_collect_name_continue_signal(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?…")
    if not lowered:
        return False
    return lowered in {
        "tamam",
        "olur",
        "evet",
        "goruselim",
        "görüşelim",
        "planlayalim",
        "planlayalım",
        "iyi olur",
        "tamam olur",
        "tamam goruselim",
        "tamam görüşelim",
        "olur goruselim",
        "olur görüşelim",
    }


def is_price_question(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    return any(token in lowered for token in ("ne kadar", "fiyat", "ucret", "ücret", "kac tl", "kaç tl", "bedel"))


def is_booking_opt_in(message_text: str, intent: str | None) -> bool:
    if is_preconsultation_explanation_question(message_text):
        return False
    lowered = sanitize_text(message_text or "").lower()
    return intent == "booking_request" or any(phrase in lowered for phrase in BOOKING_OPT_IN_PHRASES)


def clear_stale_active_booking_state(conversation: dict[str, Any], memory: dict[str, Any], conn: Any | None = None) -> None:
    has_confirmed_appointment = is_confirmed_generic_appointment(conversation) or bool(existing_generic_appointment_id(conversation, conn))
    conversation["state"] = "completed" if has_confirmed_appointment else "new"
    conversation["appointment_status"] = "confirmed" if has_confirmed_appointment else "collecting"
    conversation["assigned_human"] = False
    memory["open_loop"] = "completed" if has_confirmed_appointment else None
    memory["last_bot_question_type"] = None
    conversation["memory_state"] = memory
    sync_conversation_memory_summary(conversation)


NON_NAME_ACTION_PHRASES = {
    "kolay gelsin",
    "merhaba",
    "selam",
    "iyi gunler",
    "iyi günler",
    "tesekkurler",
    "teşekkürler",
    "tesekkur ederim",
    "teşekkür ederim",
    "tamam",
    "olur",
    "goruselim",
    "görüşelim",
    "evet",
    "hayir",
    "hayır",
}


def is_non_name_action_phrase(message_text: str | None) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?…")
    if not lowered:
        return False
    if lowered in NON_NAME_ACTION_PHRASES:
        return True
    return any(lowered.startswith(f"{phrase} ") for phrase in NON_NAME_ACTION_PHRASES)


def is_active_salutation_message(message_text: str | None) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?…")
    if not lowered:
        return False
    salutations = {
        "kolay gelsin",
        "merhaba",
        "selam",
        "iyi gunler",
        "iyi günler",
        "tesekkurler",
        "teşekkürler",
        "tesekkur ederim",
        "teşekkür ederim",
    }
    return lowered in salutations or any(lowered.startswith(f"{phrase} ") for phrase in salutations)


def is_username_save_request(message_text: str | None) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?…")
    if not lowered:
        return False
    wants_save = any(token in lowered for token in ["kaydet", "kayded", "kayd"])
    mentions_username = any(
        token in lowered
        for token in [
            "kullanici ad",
            "instagram ad",
            "instagram kullanici",
            "username",
            "ig ad",
        ]
    )
    return wants_save and mentions_username


def is_literal_username_placeholder_name(name_text: str | None) -> bool:
    lowered = sanitize_text(name_text or "").lower().strip(" @.!?…")
    return lowered in {
        "kullanici adi",
        "kullanici adim",
        "instagram adi",
        "instagram adim",
        "username",
        "ig adi",
        "ig adim",
    }


def instagram_username_name_label(username: str | None) -> str | None:
    clean = sanitize_text(username or "").strip().lstrip("@")
    if not clean:
        return None
    safe = re.sub(r"[^A-Za-z0-9._]", "", clean)
    if not safe:
        return None
    return f"@{safe}"


def should_apply_instagram_username_name(conversation: dict[str, Any], username_label: str | None) -> bool:
    if not username_label:
        return False
    current_name = sanitize_text(str(conversation.get("full_name") or conversation.get("lead_name") or "")).strip()
    if not current_name:
        return True
    return is_non_name_action_phrase(current_name) or is_literal_username_placeholder_name(current_name)


def is_valid_name_candidate(name_text: str | None, *, require_full_name: bool = False) -> bool:
    clean = sanitize_text(name_text or "").strip()
    if not clean or is_non_name_action_phrase(clean) or is_literal_username_placeholder_name(clean):
        return False
    parts = [part for part in re.split(r"\s+", clean) if part]
    if require_full_name and len(parts) < 2:
        return False
    if len(parts) > 4:
        return False
    if any(re.search(r"\d", part) for part in parts):
        return False
    return True


def clean_name_text(raw_name: str | None) -> str | None:
    """Strip conversational fillers from extracted names."""
    clean = sanitize_text(raw_name or "").strip()
    if not clean:
        return None
    fillers = ["aslında", "aslinda", "ben", "adım", "adim", "diye", "yani", "iste", "işte", "bey", "hanım", "hanim"]
    pattern = r"\b(?:" + "|".join(fillers) + r")\b"
    clean = re.sub(pattern, "", clean, flags=re.IGNORECASE).strip()
    clean = re.sub(r"\s+", " ", clean).strip()
    # Also remove trailing/leading commas, dots
    clean = clean.strip(".,;:!? ")
    return clean or None


def is_phone_like_attempt(message_text: str | None) -> bool:
    return bool(re.search(r"\d", sanitize_text(message_text or "")))


def build_active_state_recovery_reply(state: str | None) -> str | None:
    return None


def active_state_relevance(message_text: str, state: str | None, cfg: dict[str, Any]) -> tuple[bool, str | None]:
    if state == "collect_name":
        if is_username_save_request(message_text):
            return True, "username_save"
        if is_non_name_action_phrase(message_text) or is_simple_greeting(message_text):
            return True, "llm_flow"
        name = extract_name(message_text, "collect_name")
        is_valid_name = is_valid_name_candidate(name, require_full_name=True)
        return is_valid_name, "name" if is_valid_name else None
    if state == "collect_phone":
        if is_username_save_request(message_text):
            return True, "username_save"
        if extract_phone(message_text):
            return True, "phone"
        if is_phone_like_attempt(message_text) and is_invalid_phone_attempt(message_text, state):
            return True, "invalid_phone"
        return True, "llm_flow"
    if state == "collect_datetime":
        has_datetime = bool(
            extract_date(message_text)
            or extract_time_for_state(message_text, "collect_datetime")
            or extract_time(message_text)
            or extract_generic_datetime_time(message_text)
        )
        if has_datetime:
            return True, "datetime"
        return True, "llm_flow"
    if state == "collect_service":
        has_service = bool(detect_requested_service_from_text(message_text, cfg))
        return has_service, "service" if has_service else None
    return True, None


def known_requested_service(conversation: dict[str, Any], memory: dict[str, Any]) -> str | None:
    for key in ("requested_service", "selected_service", "service_interest"):
        value = sanitize_text(memory.get(key) or "")
        if value:
            return value
    value = sanitize_text(conversation.get("service") or "")
    return value or None


def detect_requested_service_from_text(message_text: str, cfg: dict[str, Any]) -> str | None:
    lowered = sanitize_text(message_text or "").lower()
    if not lowered:
        return None
    for service in cfg.get("service_catalog", []) or []:
        if not isinstance(service, dict):
            continue
        display = sanitize_text(service.get("display") or service.get("name") or "")
        candidates = [display, service.get("name"), service.get("slug")]
        candidates.extend(service.get("keywords") or [])
        for candidate in candidates:
            clean = sanitize_text(str(candidate or "")).lower()
            if clean and clean in lowered:
                if clean == "otomasyon" or "otomasyon" in clean:
                    return "Otomasyon"
                return display or sanitize_text(str(candidate or ""))
    return None


def remember_requested_service(conversation: dict[str, Any], memory: dict[str, Any], service_label: str | None) -> str | None:
    clean = sanitize_text(service_label or "")
    if not clean:
        return None
    memory["requested_service"] = clean
    memory["selected_service"] = clean
    memory["service_interest"] = clean
    conversation["service"] = clean
    return clean


def persist_user_business_identity_context(
    message_text: str,
    history: list[dict[str, Any]] | None,
    conversation: dict[str, Any],
    memory: dict[str, Any],
) -> bool:
    subsector = detect_customer_subsector(message_text, history)
    if not subsector:
        return False
    sector = customer_sector_for_subsector(subsector)
    if sector:
        memory["customer_sector"] = sector
    memory["customer_subsector"] = subsector
    conversation["memory_state"] = memory
    return True


def persist_customer_identity_to_crm(conn: Any, customer: dict[str, Any] | None, memory: dict[str, Any]) -> None:
    if not customer:
        return
    subsector = sanitize_text(str(memory.get("customer_subsector") or ""))
    sector = sanitize_text(str(memory.get("customer_sector") or customer.get("sector") or ""))
    if not subsector:
        return
    note = f"customer_subsector={subsector}"
    preferences_patch = {
        "customer_subsector": subsector,
        "customer_sector": sector or None,
    }
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customers
                SET sector = COALESCE(NULLIF(%s, ''), sector),
                    preferences = COALESCE(preferences, '{}'::jsonb) || %s::jsonb,
                    notes = CASE
                        WHEN COALESCE(notes, '') ILIKE %s THEN notes
                        WHEN COALESCE(notes, '') = '' THEN %s
                        ELSE notes || E'\n' || %s
                    END,
                    updated_at = NOW()
                WHERE id = %s
                """,
                (
                    sector,
                    json.dumps(preferences_patch, ensure_ascii=False),
                    f"%{note}%",
                    note,
                    note,
                    customer.get("id"),
                ),
            )
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("generic_customer_identity_persist_failed customer_id=%s", customer.get("id"))


def service_reply_phrase(service_label: str | None) -> str:
    lowered = sanitize_text(service_label or "").lower()
    if "otomasyon" in lowered:
        return "otomasyon"
    return lowered or "bu hizmet"


def find_service_config(cfg: dict[str, Any], service_label: str | None, memory: dict[str, Any] | None = None) -> dict[str, Any] | None:
    service_hint = sanitize_text(service_label or "").lower()
    goal_hint = sanitize_text((memory or {}).get("customer_goal") or "").lower()
    if not service_hint and any(token in goal_hint for token in ("dm", "randevu", "mesaj", "otomasyon")):
        service_hint = "otomasyon"
    for service in cfg.get("service_catalog", []) or []:
        if not isinstance(service, dict):
            continue
        candidates = [service.get("display"), service.get("name"), service.get("slug")]
        candidates.extend(service.get("keywords") or [])
        clean_candidates = [sanitize_text(str(candidate or "")).lower() for candidate in candidates]
        if service_hint and any(candidate and (candidate in service_hint or service_hint in candidate) for candidate in clean_candidates):
            return service
    return None


def build_service_price_reply(cfg: dict[str, Any], service_label: str | None, memory: dict[str, Any]) -> str | None:
    return None


def build_preconsultation_explanation_reply(service_label: str | None) -> str:
    return None


def build_completed_followup_reply(message_text: str, cfg: dict[str, Any]) -> tuple[str | None, str | None]:
    return None, None


def reply_repeats_service_question(reply_text: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    return any(fragment in lowered for fragment in SERVICE_REPEAT_QUESTION_FRAGMENTS)


def reply_question_count(reply_text: str | None) -> int:
    return sanitize_text(reply_text or "").count("?")


def reply_sentence_count(reply_text: str | None) -> int:
    text = sanitize_text(reply_text or "").strip()
    if not text:
        return 0
    endings = {".", "?", "!"}
    count = sum(1 for char in text if char in endings)
    return count or 1


def build_generic_business_context(message_text: str, cfg: dict[str, Any]) -> str:
    context = dict(cfg or {})
    if not is_company_capability_question(message_text):
        context.pop("unavailable_services", None)
        
    if is_user_business_identity_message(message_text):
        context["instruction_override"] = (
            "Kullanıcı kendi işletme sektörünü söylüyor. Bu, bizim o sektörel hizmeti verip vermediğimiz sorusu değildir; "
            "sunulmayan hizmet listesi veya capability reddi üretme. Business config içinde bulunan service_catalog, "
            "service_descriptions ve service_fit bilgilerine göre bu müşteriye nasıl yardımcı olunabileceğini doğal ve kısa açıkla. "
            "Hedef belirsizse en fazla 1 net soru sor."
        )

    return json.dumps(context, ensure_ascii=False)


def _collect_config_service_phrases(cfg: dict[str, Any], limit: int = 3) -> list[str]:
    phrases: list[str] = []
    for service in cfg.get("service_catalog", []) or []:
        if not isinstance(service, dict):
            continue
        label = sanitize_text(service.get("display") or service.get("name") or service.get("slug") or "")
        detail = sanitize_text(service.get("service_fit") or service.get("fit") or service.get("summary") or service.get("description") or "")
        if label and detail:
            phrases.append(f"{label}: {detail}")
        elif label:
            phrases.append(label)
        if len(phrases) >= limit:
            break

    descriptions = cfg.get("service_descriptions") or {}
    if isinstance(descriptions, dict):
        for label, detail in descriptions.items():
            label_text = sanitize_text(str(label or ""))
            detail_text = sanitize_text(str(detail or ""))
            if label_text and detail_text and not any(label_text in phrase for phrase in phrases):
                phrases.append(f"{label_text}: {detail_text}")
            if len(phrases) >= limit:
                break
    elif isinstance(descriptions, list):
        for item in descriptions:
            text = sanitize_text(str(item or ""))
            if text:
                phrases.append(text)
            if len(phrases) >= limit:
                break

    fit = cfg.get("service_fit") or cfg.get("business_fit")
    if isinstance(fit, str) and sanitize_text(fit) and len(phrases) < limit:
        phrases.append(sanitize_text(fit))
    elif isinstance(fit, dict):
        for label, detail in fit.items():
            label_text = sanitize_text(str(label or ""))
            detail_text = sanitize_text(str(detail or ""))
            if label_text and detail_text:
                phrases.append(f"{label_text}: {detail_text}")
            elif detail_text:
                phrases.append(detail_text)
            if len(phrases) >= limit:
                break
    return phrases[:limit]


def _humanize_turkish_label(value: str | None) -> str:
    text = str(value or "").strip()
    replacements = {
        "Tasarim": "Tasarım",
        "tasarim": "tasarım",
        "Cozumleri": "Çözümleri",
        "cozumleri": "çözümleri",
        "Cozumler": "Çözümler",
        "cozumler": "çözümler",
        "Cozum": "Çözüm",
        "cozum": "çözüm",
        "Musteri": "Müşteri",
        "musteri": "müşteri",
        "Yonetimi": "Yönetimi",
        "yonetimi": "yönetimi",
        "Pazarlama": "Pazarlama",
        "pazarlama": "pazarlama",
        "Sosyal Medya": "Sosyal Medya",
        "sosyal medya": "sosyal medya",
        "Yapay Zeka": "Yapay Zeka",
        "yapay zeka": "yapay zeka",
    }
    for old, new in replacements.items():
        text = text.replace(old, new)
    return text


def _natural_service_label(service: dict[str, Any]) -> str | None:
    raw = _humanize_turkish_label(str(service.get("display") or service.get("name") or service.get("slug") or ""))
    lowered = sanitize_text(raw).lower()
    if not lowered:
        return None
    if "web" in lowered or "site" in lowered:
        return "web sitesi"
    if "reklam" in lowered or "pazarlama" in lowered or "performans" in lowered or "ads" in lowered:
        return "reklam yönetimi"
    if "sosyal" in lowered or "instagram" in lowered or "social" in lowered:
        return "sosyal medya"
    if "otomasyon" in lowered or "randevu" in lowered or "mesaj" in lowered or "yapay zeka" in lowered or "ai" in lowered:
        return "mesaj/randevu otomasyonu"
    return raw[:1].lower() + raw[1:]


def _collect_natural_service_labels(cfg: dict[str, Any], limit: int = 4) -> list[str]:
    labels: list[str] = []
    for service in cfg.get("service_catalog", []) or []:
        if not isinstance(service, dict):
            continue
        label = _natural_service_label(service)
        if label and label not in labels:
            labels.append(label)
        if len(labels) >= limit:
            break
    return labels


def _join_natural_list(items: list[str]) -> str:
    clean = [str(item or "").strip() for item in items if str(item or "").strip()]
    if not clean:
        return ""
    if len(clean) == 1:
        return clean[0]
    if len(clean) == 2:
        return f"{clean[0]} ve {clean[1]}"
    return f"{', '.join(clean[:-1])} ve {clean[-1]}"


def is_service_overview_question(message_text: str, intent: str | None = None) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    return any(
        token in lowered
        for token in (
            "tam olarak ne yapiyorsunuz",
            "tam olarak ne yapıyorsunuz",
            "ne yapiyorsunuz",
            "ne yapıyorsunuz",
            "hizmetleriniz neler",
            "hizmetleriniz nedir",
            "hizmetleriniz",
            "neler yapiyorsunuz",
            "neler yapıyorsunuz",
        )
    )


def build_natural_service_overview_reply(cfg: dict[str, Any]) -> str | None:
    return None


def build_user_business_identity_reply(cfg: dict[str, Any]) -> str:
    return None


def generic_llm_reply_rejection_reason(reply_text: str | None) -> str | None:
    """Return a generic reason when an LLM reply should be repaired by config fallback.

    This intentionally avoids sector-specific deterministic rules: it only rejects empty,
    error-like, unrelated/fallback, capability-denial, overly long, or obvious catalog-dump
    replies. Valid concise LLM replies should remain the final outbound text.
    """
    reply = sanitize_text(reply_text or "")
    lowered = reply.lower()
    if not reply:
        return "empty"
    if is_llm_error_reply(reply):
        return "llm_error_reply"

    fallback_markers = (
        "anlayamadım",
        "anlayamadim",
        "tekrar yazar mısınız",
        "tekrar yazar misiniz",
        "şu anda yardımcı olamıyorum",
        "su anda yardimci olamiyorum",
        "şu an yanıtı netleştiremedim",
        "su an yaniti netlestiremedim",
        "mesajınızı aldım, birazdan devam edelim",
        "mesajinizi aldim, birazdan devam edelim",
        "bir hata oluştu",
        "bir hata olustu",
        "fallback",
    )
    if any(marker in lowered for marker in fallback_markers):
        return "fallback_reply"

    wrong_capability_markers = (
        "hizmet vermiyoruz",
        "hizmeti vermiyoruz",
        "hizmet sunmuyoruz",
        "hizmeti sunmuyoruz",
        "uzmanlik alanimiz disinda",
        "uzmanlık alanımız dışında",
        "alanimiz disinda",
        "alanımız dışında",
        "yapmiyoruz",
        "yapmıyoruz",
    )
    if any(marker in lowered for marker in wrong_capability_markers):
        return "misread_as_capability"

    if len(reply) > 500 or reply_question_count(reply) > 1 or reply_sentence_count(reply) > 4:
        return "too_long"

    if ";" in reply and ":" in reply:
        return "catalog_dump"

    return None


def identity_llm_reply_rejection_reason(reply_text: str | None) -> str | None:
    reason = generic_llm_reply_rejection_reason(reply_text)
    if reason == "misread_as_capability":
        return "identity_misread_as_capability"
    return reason


def build_enriched_inbound_raw_event(
    payload: IncomingMessage,
    inbound_platform: str,
    inbound_message_id: str | None,
    inbound_dedupe_key: str | None,
    trace_id: str,
) -> dict[str, Any]:
    raw_event_for_log = dict(payload.raw_event or {}) if isinstance(payload.raw_event, dict) else {}
    raw_event_for_log["platform"] = inbound_platform
    raw_event_for_log["message_id"] = sanitize_text(str(inbound_message_id or raw_event_for_log.get("message_id") or ""))
    raw_event_for_log["sender_id"] = sanitize_text(str(payload.sender_id or raw_event_for_log.get("sender_id") or ""))
    if inbound_dedupe_key:
        raw_event_for_log["dedupe_key"] = inbound_dedupe_key
    raw_event_for_log["trace_id"] = sanitize_text(trace_id or inbound_dedupe_key or inbound_message_id or payload.sender_id)
    return raw_event_for_log


def build_outbound_raw_event(
    decision_path: list[str],
    trace_id: str,
    inbound_dedupe_key: str | None,
    inbound_platform: str,
    inbound_message_id: str | None,
) -> dict[str, Any]:
    raw_event = {
        "type": "reply",
        "decision_path": decision_path,
        "trace_id": sanitize_text(trace_id or inbound_dedupe_key or inbound_message_id or ""),
        "platform": inbound_platform,
    }
    if inbound_message_id:
        raw_event["message_id"] = inbound_message_id
    if inbound_dedupe_key:
        raw_event["dedupe_key"] = inbound_dedupe_key
    return raw_event


def has_outbound_reply_for_trace(conn: Any, sender_id: str, trace_id: str | None) -> bool:
    clean_trace_id = sanitize_text(trace_id or "")
    if not clean_trace_id or not hasattr(conn, "cursor"):
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM message_logs
            WHERE instagram_user_id = %s
              AND direction = 'out'
              AND raw_payload->>'trace_id' = %s
            LIMIT 1
            """,
            (sender_id, clean_trace_id),
        )
        return cur.fetchone() is not None


def duplicate_process_result(
    payload: IncomingMessage,
    conversation: dict[str, Any],
    metrics: dict[str, Any],
    decision_label: str,
    request_started_at: float,
) -> ProcessResult:
    metrics["duplicate"] = True
    metrics["total_ms"] = elapsed_ms(request_started_at)
    return ProcessResult(
        sender_id=payload.sender_id,
        should_reply=False,
        reply_text=None,
        handoff=False,
        conversation_state=conversation.get("state", "new"),
        appointment_created=False,
        appointment_id=None,
        duplicate=True,
        normalized=build_normalized(conversation),
        metrics=metrics,
        decision_path=[decision_label],
    )


def strip_leading_greeting_for_non_greeting(message_text: str, reply_text: str | None) -> str:
    return reply_text or ""


def build_service_carryover_booking_reply(service_label: str | None, state: str | None) -> str | None:
    return None


def is_llm_error_reply(reply_text: str | None) -> bool:
    lowered = sanitize_text(reply_text or "").lower()
    return lowered.startswith("error:") or "llm json error" in lowered or "too many requests" in lowered


def extract_generic_datetime_time(message_text: str) -> str | None:
    lowered = sanitize_text(message_text or "").lower()
    if re.search(r"\b(oglen|ogle arasi|ogle|ogleye|ogleye dogru|ogle vakti)\b", lowered):
        return "12:00"
    match = re.search(r"\b(aksam|sabah|oglen)?\s*(\d{1,2})(?::(\d{2}))?\b", lowered)
    if not match:
        return None
    period, hour_text, minute_text = match.groups()
    hour = int(hour_text)
    minute = int(minute_text or "0")
    if period == "aksam" and 1 <= hour <= 11:
        hour += 12
    if period == "oglen" and hour == 12:
        hour = 12
    # 12-saat formatı düzeltmesi: "saat 3" → 15:00, "saat 5" → 17:00
    if 1 <= hour <= 6 and period not in ("sabah",):
        if "saat" in lowered or "boşluk" in lowered or "müsait" in lowered:
            hour += 12
    if 0 <= hour <= 23 and 0 <= minute <= 59:
        return f"{hour:02d}:{minute:02d}"
    return None


def is_confirmed_generic_appointment(conversation: dict[str, Any]) -> bool:
    return bool(
        conversation.get("appointment_id")
        or conversation.get("appointment_status") == "confirmed"
        or conversation.get("state") == "completed"
    )


def existing_generic_appointment_id(conversation: dict[str, Any], conn: Any | None = None) -> int | None:
    try:
        if conversation.get("appointment_id"):
            return int(conversation.get("appointment_id"))
    except Exception:
        pass
    lookup_user_id = conversation.get("instagram_user_id") or conversation.get("sender_id")
    if conn is not None and lookup_user_id:
        try:
            appointment = find_active_appointment_for_user(
                conn,
                lookup_user_id,
                preferred_date=conversation.get("requested_date"),
                preferred_time=conversation.get("requested_time"),
            )
            if appointment and appointment.get("id"):
                return int(appointment["id"])
        except Exception:
            return None
    return None


def is_explicit_reschedule_request(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower()
    subject = any(token in lowered for token in ["randevu", "gorusme", "görüşme", "on gorusme", "ön görüşme", "saat"])
    action = any(token in lowered for token in ["degistir", "değiştir", "guncelle", "güncelle", "yap", "kaydir", "kaydır", "cek", "çek"])
    return subject and action


def is_reschedule_confirmation_acceptance(message_text: str) -> bool:
    lowered = sanitize_text(message_text or "").lower().strip(" .!?")
    if lowered in {"evet", "onayliyorum", "onaylıyorum", "tamam", "olur", "aynen"}:
        return True
    has_acceptance = any(token in lowered for token in ("evet", "onay", "tamam", "olur", "aynen"))
    has_change_context = any(token in lowered for token in ("degistir", "değiştir", "guncelle", "güncelle", "olarak", "saat")) or bool(extract_time(message_text) or extract_generic_datetime_time(message_text))
    return has_acceptance and has_change_context


def detect_reschedule_candidate(message_text: str, extracted: dict[str, Any]) -> tuple[str | None, str | None]:
    detected_date = extract_date(message_text)
    detected_time = (
        extract_time_for_state(message_text, "collect_datetime")
        or extract_time(message_text)
        or extract_generic_datetime_time(message_text)
    )
    return detected_date, detected_time


def append_reschedule_handoff_note(conversation: dict[str, Any], requested_date: str | None, requested_time: str | None) -> None:
    note = "müşteri saat değişikliği istedi"
    if requested_date or requested_time:
        note = f"{note}: {requested_date or conversation.get('requested_date') or '-'} {requested_time or conversation.get('requested_time') or '-'}"
    current = sanitize_text(conversation.get("notes") or "")
    conversation["notes"] = f"{current}\n{note}".strip() if current else note
    conversation["assigned_human"] = True
    memory = ensure_conversation_memory(conversation)
    memory["pending_reschedule_request"] = {"requested_date": requested_date, "requested_time": requested_time, "note": note}
    memory["open_loop"] = "handoff"
    conversation["memory_state"] = memory


def build_reschedule_confirmation_question(conversation: dict[str, Any], requested_date: str | None, requested_time: str | None) -> str:
    target_date = requested_date or conversation.get("requested_date")
    target_time = requested_time or conversation.get("requested_time")
    if requested_date and requested_time:
        return f"Mevcut ön görüşmenizi {target_date} saat {target_time} olarak değiştirmek istediğinizi onaylıyor musunuz?"
    if requested_time:
        return f"Mevcut ön görüşme saatinizi {target_time} olarak değiştirmek istediğinizi onaylıyor musunuz?"
    return "Mevcut ön görüşme tarihinizi değiştirmek istediğinizi onaylıyor musunuz?"


def update_existing_appointment_from_pending_reschedule(
    conn: Any,
    conversation: dict[str, Any],
    appointment_id: int | None,
    requested_date: str | None,
    requested_time: str | None,
    username: str | None,
) -> tuple[bool, str, str | None]:
    if not appointment_id:
        return False, "", None

    detected_date = normalize_date_string(requested_date or conversation.get("requested_date"))
    detected_time = normalize_time_string(requested_time or conversation.get("requested_time"))
    validation_error = validate_slot(detected_date, detected_time)
    if validation_error:
        return False, validation_error, None

    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE appointments
                SET appointment_date = %s::date,
                    appointment_time = %s::time,
                    instagram_username = COALESCE(%s, instagram_username),
                    full_name = COALESCE(%s, full_name),
                    phone = COALESCE(%s, phone),
                    service = COALESCE(%s, service),
                    updated_at = NOW()
                WHERE id = %s
                  AND instagram_user_id = %s
                  AND status IN ('confirmed', 'preconsultation', 'scheduled')
                RETURNING id, status
                """,
                (
                    detected_date,
                    detected_time,
                    username or conversation.get("instagram_username"),
                    conversation.get("full_name"),
                    conversation.get("phone"),
                    conversation.get("service"),
                    appointment_id,
                    conversation.get("instagram_user_id") or conversation.get("sender_id"),
                ),
            )
            updated = cur.fetchone()
        if not updated:
            conn.rollback()
            return False, "", None
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        logger.exception("generic_pending_reschedule_update_failed appointment_id=%s", appointment_id)
        return False, "", None

    conversation["requested_date"] = detected_date
    conversation["requested_time"] = detected_time
    conversation["appointment_status"] = "confirmed"
    conversation["state"] = "completed"
    conversation["appointment_id"] = appointment_id
    conversation["assigned_human"] = False
    memory = ensure_conversation_memory(conversation)
    memory["reschedule_requested_date"] = None
    memory["reschedule_requested_time"] = None
    memory["pending_reschedule_request"] = None
    memory["open_loop"] = "completed"
    conversation["memory_state"] = memory
    try:
        conn.commit()
    except Exception:
        pass

    return True, f"Tamamdır, görüşme kaydınızı {format_human_date(detected_date)} saat {detected_time} olarak güncelledim.", "appointment_rescheduled"


def handle_confirmed_generic_reschedule(
    conn: Any,
    conversation: dict[str, Any],
    memory: dict[str, Any],
    message_text: str,
    extracted: dict[str, Any],
    username: str | None,
) -> dict[str, Any] | None:
    if not is_confirmed_generic_appointment(conversation):
        return None

    existing_id = existing_generic_appointment_id(conversation, conn)
    pending_reschedule = bool(memory.get("reschedule_requested_date") or memory.get("reschedule_requested_time"))
    pending_confirm = memory.get("open_loop") == "generic_reschedule_confirmation_pending" or pending_reschedule
    if pending_confirm and is_reschedule_confirmation_acceptance(message_text):
        requested_date = memory.get("reschedule_requested_date") or conversation.get("requested_date")
        requested_time = memory.get("reschedule_requested_time") or conversation.get("requested_time")
        updated, reply, label = update_existing_appointment_from_pending_reschedule(
            conn,
            conversation,
            existing_id,
            requested_date,
            requested_time,
            username,
        )
        if updated:
            return {"handled": True, "reply_text": reply, "handoff": False, "appointment_id": existing_id, "decision_label": label or "appointment_rescheduled"}
        update_text = f"randevuyu {requested_date or ''} {requested_time or ''} yap".strip()
        try:
            updated, reply, label = try_reschedule_confirmed_appointment(conn, conversation, update_text, username)
        except Exception:
            updated, reply, label = False, "", None
        if updated:
            return {"handled": True, "reply_text": reply, "handoff": False, "appointment_id": existing_id, "decision_label": label or "appointment_rescheduled"}
        append_reschedule_handoff_note(conversation, requested_date, requested_time)
        return {
            "handled": True,
            "reply_text": "Saat değişikliği talebinizi ekibe iletmek üzere not aldım. Mevcut randevu kaydınız korunuyor.",
            "handoff": True,
            "appointment_id": existing_id,
            "decision_label": "appointment_reschedule_handoff",
        }

    requested_date, requested_time = detect_reschedule_candidate(message_text, extracted)
    if not requested_date and not requested_time:
        return None

    if is_explicit_reschedule_request(message_text):
        try:
            updated, reply, label = try_reschedule_confirmed_appointment(conn, conversation, message_text, username)
        except Exception:
            updated, reply, label = False, "", None
        if updated:
            return {"handled": True, "reply_text": reply, "handoff": False, "appointment_id": existing_id, "decision_label": label or "appointment_rescheduled"}
        append_reschedule_handoff_note(conversation, requested_date, requested_time)
        return {
            "handled": True,
            "reply_text": "Mevcut randevu kaydınızı koruyorum; saat değişikliği talebinizi ekibe iletmek üzere not aldım.",
            "handoff": True,
            "appointment_id": existing_id,
            "decision_label": "appointment_reschedule_handoff",
        }

    memory["reschedule_requested_date"] = requested_date or conversation.get("requested_date")
    memory["reschedule_requested_time"] = requested_time or conversation.get("requested_time")
    memory["open_loop"] = "generic_reschedule_confirmation_pending"
    conversation["memory_state"] = memory
    return {
        "handled": True,
        "reply_text": build_reschedule_confirmation_question(conversation, requested_date, requested_time),
        "handoff": False,
        "appointment_id": existing_id,
        "decision_label": "appointment_reschedule_confirm_required",
        "reschedule_requested_date": memory.get("reschedule_requested_date"),
        "reschedule_requested_time": memory.get("reschedule_requested_time"),
    }


def build_active_booking_prompt_reply(conversation: dict[str, Any], memory: dict[str, Any]) -> str | None:
    return None


def is_appointment_confirmation_like_reply(reply: str) -> bool:
    lowered = sanitize_text(reply or "").lower()
    if not lowered:
        return False
    explicit_patterns = [
        r"\brandevu(?:nuz|nuzu|niz|nizi)?\b.{0,80}\b(?:olusturdum|olusturuldu|hazir|tamamlandi|planlandi|ayarladim|ayarlandi|ayarlanmistir|onaylandi)\b",
        r"\b(?:on gorusme|gorusme)\b.{0,80}\b(?:kaydiniz|kaydini|randevunuz|randevunuzu)\b.{0,80}\b(?:olusturdum|olusturuldu|tamamlandi|hazir|planlandi|onaylandi|ayarlandi|ayarlanmistir)\b",
        r"\b(?:on gorusmeniz|gorusmeniz)\b.{0,80}\b(?:olusturuldu|tamamlandi|hazir|planlandi|onaylandi|ayarlandi|ayarlanmistir)\b",
        r"\b(?:kaydiniz|kaydinizi|kayit)\b.{0,80}\b(?:olusturdum|olusturuldu|tamamlandi|hazir|onaylandi)\b",
        r"\bsizi\s+arayac(?:agiz|aktir|ak)\b",
        r"\bislem\b.{0,40}\btamamlandi\b",
        r"\bsaat(?:iniz|inizi|i)?\b.{0,40}\b(?:guncelledim|guncellendi|degistirdim|degisti)\b",
        r"\b(?:confirmed|scheduled)\b",
    ]
    if any(re.search(pattern, lowered) for pattern in explicit_patterns):
        return True
    has_definite_datetime = bool(re.search(r"\b(?:bugun|yarin|\d{1,2}[:.]\d{2}|saat\s*\d{1,2})\b", lowered))
    has_final_call_phrase = any(phrase in lowered for phrase in ["sizi arayacaktir", "sizi arayacak", "gorusmek uzere", "gorusuruz"])
    has_booking_context = any(token in lowered for token in ["randevu", "on gorusme", "gorusme", "kayit"])
    return has_booking_context and has_definite_datetime and has_final_call_phrase


def is_appointment_confirmation_like_reply_strict(reply: str) -> bool:
    """Stricter version used when enforcing direct clarification answers.
    Requires BOTH a definite datetime AND an explicit booking-created phrase.
    Avoids false positives like 'sizi arayacak' in direct-question answers.
    """
    lowered = sanitize_text(reply or "").lower()
    if not lowered:
        return False
    # Only block if reply explicitly claims a booking was created/scheduled
    explicit_created = [
        r"\brandevu(?:nuz|nuzu|niz|nizi)?\b.{0,80}\b(?:olusturdum|olusturuldu|tamamlandi|planlandi|ayarladim|ayarlandi|onaylandi)\b",
        r"\b(?:on gorusme|gorusme)\b.{0,80}\b(?:kaydiniz|kaydini|randevunuz|randevunuzu)\b.{0,80}\b(?:olusturdum|olusturuldu|tamamlandi|hazir|planlandi|onaylandi|ayarlandi)\b",
        r"\b(?:on gorusmeniz|gorusmeniz)\b.{0,80}\b(?:olusturuldu|tamamlandi|planlandi|onaylandi|ayarlandi)\b",
        r"\b(?:kaydiniz|kaydinizi|kayit)\b.{0,80}\b(?:olusturdum|olusturuldu|tamamlandi|onaylandi)\b",
        r"\bislem\b.{0,40}\btamamlandi\b",
        r"\b(?:confirmed|scheduled)\b",
    ]
    if any(re.search(pattern, lowered) for pattern in explicit_created):
        return True
    # Also block if there's a definite datetime AND a call-to-action phrase (real confirmation)
    has_definite_datetime = bool(re.search(r"\b(?:bugun|yarin|\d{1,2}[:.]\d{2}|saat\s*\d{1,2})\b", lowered))
    has_booking_context = any(token in lowered for token in ["randevu", "on gorusme", "gorusme", "kayit"])
    has_final_call_phrase = any(phrase in lowered for phrase in ["gorusmek uzere", "gorusuruz"])
    return has_definite_datetime and has_booking_context and has_final_call_phrase


def build_false_confirmation_guard_reply(conversation: dict[str, Any], memory: dict[str, Any]) -> str:
    state = conversation.get("state") or "collect_datetime"
    if state == "collect_datetime":
        if conversation.get("requested_date") and not conversation.get("requested_time"):
            return "Yarın için net saati yazabilir misiniz? Örneğin 12:00 veya 13:00."
        if conversation.get("requested_time") and not conversation.get("requested_date"):
            return "Hangi gün için planlayalım? Örneğin yarın 13:00 gibi yazabilirsiniz."
        return "Uygun gün ve saati net yazar mısınız? Örneğin yarın 13:00 gibi."
    active_prompt = build_active_booking_prompt_reply(conversation, memory)
    return active_prompt or "Randevu kaydı için eksik bilgileri tamamlayalım; uygun gün ve saati yazar mısınız?"


def process_instagram_message_generic(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    request_started_at = time_module.perf_counter()
    metrics = {
        "reply_engine": "generic_core",
        "total_ms": 0,
        "message_type": "reply"
    }

    from app.main import logger
    raw_ts = None
    if payload.raw_event and isinstance(payload.raw_event, dict):
        raw_ts = payload.raw_event.get("timestamp") or payload.raw_event.get("created_time")
    if raw_ts:
        try:
            if isinstance(raw_ts, (int, float)):
                message_time = datetime.datetime.fromtimestamp(raw_ts, tz=datetime.timezone.utc)
            else:
                raw_ts_str = str(raw_ts).strip()
                if raw_ts_str.isdigit():
                    ts_number = int(raw_ts_str)
                    if ts_number > 10_000_000_000:
                        ts_number = ts_number / 1000
                    message_time = datetime.datetime.fromtimestamp(ts_number, tz=datetime.timezone.utc)
                else:
                    message_time = datetime.datetime.fromisoformat(raw_ts_str.replace("Z", "+00:00"))
                    if message_time.tzinfo is None:
                        message_time = message_time.replace(tzinfo=datetime.timezone.utc)
            now_time = datetime.datetime.now(datetime.timezone.utc)
            if (now_time - message_time) > datetime.timedelta(minutes=10):
                logger.warning("Anti-spam: Ignoring old inbound message. Msg time: %s, Now: %s", message_time, now_time)
                metrics["ignored"] = "old_inbound_anti_spam"
                metrics["total_ms"] = elapsed_ms(request_started_at)
                return ProcessResult(
                    sender_id=payload.sender_id,
                    should_reply=False,
                    reply_text=None,
                    handoff=False,
                    conversation_state="ignored_old_inbound",
                    normalized={},
                    metrics=metrics,
                    decision_path=["ignored:anti_spam_old_ts"]
                )
        except Exception as e:
            logger.error("Failed to parse timestamp for anti-spam check: %s", e)

    message_text = sanitize_text(payload.message_text or "")
    logger.info("INBOUND_RECEIVED sender_id=%s text=%s platform=%s",
        payload.sender_id,
        repr(message_text[:200]) if message_text else "EMPTY",
        "generic")
    if not message_text:
        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=False,
            reply_text=None,
            handoff=False,
            conversation_state="ignored",
            normalized={},
            metrics=metrics,
            decision_path=["ignored:empty"]
        )

    inbound_message_id = extract_inbound_message_id(payload.raw_event)
    inbound_platform = extract_inbound_platform(payload.raw_event)
    inbound_dedupe_key = build_inbound_dedupe_key(inbound_platform, payload.sender_id, inbound_message_id)
    trace_id = sanitize_text(
        payload.trace_id
        or ((payload.raw_event or {}).get("trace_id") if isinstance(payload.raw_event, dict) else "")
        or inbound_dedupe_key
        or inbound_message_id
        or payload.sender_id
    )
    metrics["inbound_platform"] = inbound_platform
    metrics["inbound_dedupe_key"] = inbound_dedupe_key
    metrics["trace_id"] = trace_id
    raw_event_for_log = build_enriched_inbound_raw_event(payload, inbound_platform, inbound_message_id, inbound_dedupe_key, trace_id)
    
    with get_conn() as conn:
        sender_lock_wait_started_at = time_module.perf_counter()
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(hashtextextended(%s, 0))", (f"ig_sender:{payload.sender_id}",))
        metrics["sender_serial_lock"] = True
        metrics["sender_serial_lock_wait_ms"] = elapsed_ms(sender_lock_wait_started_at)

        conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
        if conversation.get("lead_name") and not conversation.get("full_name"):
            conversation["full_name"] = conversation.get("lead_name")
        sanitize_conversation_state(conversation)
        memory = ensure_conversation_memory(conversation)
        if inbound_platform in {"instagram_dm", "instagram_private_api", "igdm"}:
            memory["contact_channel"] = "instagram_dm"
            conversation["memory_state"] = memory
        # LLM'e cevap öncesi sadece context hazırla; PY müşteri metnini yazmaz.
        pre_llm_state = str(conversation.get("state") or "")
        pre_llm_name = None
        if pre_llm_state == "collect_name" and not is_username_save_request(message_text):
            pre_llm_name = extract_name(message_text, "collect_name")
            if pre_llm_name and is_valid_name_candidate(pre_llm_name, require_full_name=True):
                pre_llm_name = clean_name_text(pre_llm_name) or pre_llm_name
                conversation["lead_name"] = pre_llm_name
                conversation["full_name"] = pre_llm_name
        if (
            pre_llm_state in {"collect_name", "collect_phone", "collect_datetime", "collect_date", "collect_time", "collect_period"}
            and known_requested_service(conversation, memory)
            and (conversation.get("full_name") or conversation.get("lead_name"))
            and not normalize_date_string(conversation.get("requested_date"))
            and not normalize_time_string(conversation.get("requested_time"))
        ):
            try:
                slot_options = collect_next_booking_slot_options(conn, conversation, limit=3)
            except Exception as exc:  # noqa: BLE001
                logger.warning("generic_pre_llm_slot_context_failed sender_id=%s error=%s", payload.sender_id, exc)
                slot_options = []
            if slot_options:
                conversation["available_slots"] = [format_booking_slot_option(slot) for slot in slot_options[:3]]
                remember_booking_slot_options(conversation, slot_options[:3])
                memory = ensure_conversation_memory(conversation)
            else:
                conversation.pop("available_slots", None)
        sync_conversation_memory_summary(conversation)

        processing_lock_acquired = try_acquire_inbound_processing_lock(conn, inbound_platform, payload.sender_id, inbound_message_id)
        if not processing_lock_acquired:
            return duplicate_process_result(payload, conversation, metrics, "duplicate_inflight_ignored", request_started_at)
            
        duplicate_inbound = has_processed_inbound_message(conn, inbound_platform, payload.sender_id, inbound_message_id)
        if duplicate_inbound:
            return duplicate_process_result(payload, conversation, metrics, "duplicate_ignored", request_started_at)

        if has_outbound_reply_for_trace(conn, payload.sender_id, trace_id):
            return duplicate_process_result(payload, conversation, metrics, "duplicate_outbound_trace_ignored", request_started_at)

        cfg = get_config()
        stale_recovery_applies = should_reset_stale_conversation(conversation, message_text) and active_state_relevance(message_text, conversation.get("state"), cfg)[1] != "llm_flow"
        stale_active_booking_state = stale_recovery_applies and str(conversation.get("state") or "").startswith("collect_")
        if stale_active_booking_state:
            clear_stale_active_booking_state(conversation, memory, conn)
            memory = ensure_conversation_memory(conversation)
        elif stale_recovery_applies:
            reset_conversation_for_restart(conversation, clear_identity=True)
            memory = ensure_conversation_memory(conversation)
            
        inbound_saved = save_message_log(conn, payload.sender_id, "in", message_text, raw_event_for_log)
        if inbound_saved is False:
            return duplicate_process_result(payload, conversation, metrics, "duplicate_inbound_insert_ignored", request_started_at)
        recent_history = get_recent_message_history(conn, payload.sender_id)

        # 1. CORE LLM & INTENT RECOGNITION
        result_dict = invoke_generic_llm(message_text, conversation, memory, recent_history)
        
        intent = result_dict.get("intent", "fallback")
        reply_text = result_dict.get("reply_text", "Anlaşıldı.")
        llm_raw_reply_text = str(reply_text or "").strip()
        final_reply_source = "llm_raw"
        extracted = result_dict.get("extracted_entities", {})
        metrics["llm_raw_json"] = {k: v for k, v in result_dict.items() if not str(k).startswith("_")}
        metrics["llm_model_used"] = result_dict.get("_llm_model_used")
        metrics["llm_error"] = result_dict.get("_llm_error")
        metrics["context_summary"] = result_dict.get("_context_summary") or {}

        decision_path = [f"generic_intent:{intent}"]
        booking_opt_in = is_booking_opt_in(message_text, intent)
        deterministic_reply = False
        if is_company_capability_question(message_text):
            capability_reply = build_company_capability_reply(message_text)
            if capability_reply:
                reply_text = capability_reply
                final_reply_source = "capability"
                decision_path.append("reply:company_capability")
                deterministic_reply = True
        elif is_user_business_identity_message(message_text):
            if persist_user_business_identity_context(message_text, recent_history, conversation, memory):
                decision_path.append("persist:user_business_identity")
            identity_rejection = identity_llm_reply_rejection_reason(reply_text)
            if metrics.get("llm_error"):
                identity_rejection = identity_rejection or "llm_error"
            if identity_rejection:
                reply_text = build_user_business_identity_reply(cfg)
                final_reply_source = "config_formatter"
                decision_path.append(f"reply:user_business_identity_config:{identity_rejection}")
            else:
                final_reply_source = "llm_raw"
                decision_path.append("reply:user_business_identity_llm_raw")
            intent = "direct_answer"
            booking_opt_in = False
            deterministic_reply = True
        elif is_service_overview_question(message_text, intent):
            service_overview_rejection = generic_llm_reply_rejection_reason(reply_text)
            if intent == "fallback":
                service_overview_rejection = service_overview_rejection or "llm_intent_fallback"
            if metrics.get("llm_error"):
                service_overview_rejection = service_overview_rejection or "llm_error"
            overview_reply = build_natural_service_overview_reply(cfg)
            if service_overview_rejection and overview_reply:
                reply_text = overview_reply
                final_reply_source = "config_formatter"
                decision_path.append(f"reply:service_overview_config:{service_overview_rejection}")
            else:
                final_reply_source = "llm_raw"
                decision_path.append("reply:service_overview_llm_raw")
            intent = "direct_answer"
            booking_opt_in = False
            deterministic_reply = True
        elif is_preconsultation_explanation_question(message_text):
            reply_text = build_preconsultation_explanation_reply(known_requested_service(conversation, memory))
            intent = "direct_answer"
            booking_opt_in = False
            decision_path.append("reply:preconsultation_explanation")
            deterministic_reply = True
        elif is_price_question(message_text):
            price_reply = build_service_price_reply(cfg, known_requested_service(conversation, memory), memory)
            if price_reply:
                reply_text = price_reply
                intent = "direct_answer"
                booking_opt_in = False
                decision_path.append("reply:service_price")
                deterministic_reply = True
        elif is_business_fit_question(message_text):
            reply_text = recommendation_engine(conversation, message_text, recent_history)
            decision_path.append("reply:business_fit")
            deterministic_reply = True

        if "persist:user_business_identity" not in decision_path and persist_user_business_identity_context(message_text, recent_history, conversation, memory):
            decision_path.append("persist:user_business_identity")

        if stale_active_booking_state:
            booking_opt_in = False
            decision_path.append("fsm:stale_active_state_recovery_no_reply_override")

        completed_followup_reply, completed_followup_label = build_completed_followup_reply(message_text, cfg) if is_confirmed_generic_appointment(conversation) else (None, None)
        if completed_followup_reply:
            reply_text = completed_followup_reply
            intent = "direct_answer"
            booking_opt_in = False
            deterministic_reply = True
            decision_path.append(f"reply:{completed_followup_label}")

        post_confirmation = handle_confirmed_generic_reschedule(conn, conversation, memory, message_text, extracted, payload.instagram_username)
        if post_confirmation:
            reply_text = post_confirmation["reply_text"]
            final_reply_source = "fsm"
            handoff = bool(post_confirmation.get("handoff"))
            decision_label = str(post_confirmation.get("decision_label") or "appointment_reschedule_followup")
            decision_path.append(decision_label)
            update_conversation_memory_after_bot_reply(conversation, reply_text, "|".join(decision_path))
            if decision_label == "appointment_reschedule_confirm_required":
                memory = ensure_conversation_memory(conversation)
                memory["reschedule_requested_date"] = post_confirmation.get("reschedule_requested_date") or memory.get("reschedule_requested_date")
                memory["reschedule_requested_time"] = post_confirmation.get("reschedule_requested_time") or memory.get("reschedule_requested_time")
                memory["open_loop"] = "generic_reschedule_confirmation_pending"
                conversation["memory_state"] = memory
            upsert_conversation(conn, conversation)
            crm_customer = upsert_customer_from_conversation(conn, conversation)
            persist_customer_identity_to_crm(conn, crm_customer, ensure_conversation_memory(conversation))
            if crm_customer:
                schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector", ""))
            if has_outbound_reply_for_trace(conn, payload.sender_id, trace_id):
                return duplicate_process_result(payload, conversation, metrics, "duplicate_outbound_trace_ignored", request_started_at)
            save_message_log(conn, payload.sender_id, "out", reply_text, build_outbound_raw_event(decision_path, trace_id, inbound_dedupe_key, inbound_platform, inbound_message_id))
            metrics["total_ms"] = elapsed_ms(request_started_at)
            queue_crm_sync(background_tasks, conversation, post_confirmation.get("appointment_id"), metrics)
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=True,
                reply_text=reply_text,
                outbound_text=reply_text,
                llm_raw_reply_text=llm_raw_reply_text,
                final_reply_source=final_reply_source,
                handoff=handoff,
                conversation_state=conversation.get("state", "new"),
                appointment_created=False,
                appointment_id=post_confirmation.get("appointment_id"),
                normalized=build_normalized(conversation),
                metrics=metrics,
                decision_path=decision_path,
            )

        # 2. STATE & CRM DETERMINISTIC LAYER
        handoff = False
        active_booking_state = str(conversation.get("state") or "").startswith("collect_")
        if not deterministic_reply and not active_booking_state and intent == "human_handoff":
            decision_path.append("action:handoff")
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["appointment_status"] = "handoff"
            handoff = True

        # Sync deterministic entities immediately. Booking fields must not depend only on LLM extraction.
        state_before_entities = conversation.get("state", "new")
        active_direct_clarification = str(state_before_entities or "").startswith("collect_") and is_active_booking_direct_clarification_question(message_text)
        if active_direct_clarification:
            booking_opt_in = False
            intent = "direct_answer"
        active_state_is_relevant, active_state_label = active_state_relevance(message_text, state_before_entities, cfg)
        llm_active_name_candidate = clean_name_text(extracted.get("lead_name")) or extracted.get("lead_name")
        if (
            str(state_before_entities or "").startswith("collect_")
            and not active_direct_clarification
            and not active_state_is_relevant
            and not conversation.get("full_name")
            and not is_booking_acknowledgement_message(message_text)
            and not is_simple_greeting(message_text)
            and not is_active_salutation_message(message_text)
            and is_valid_name_candidate(llm_active_name_candidate, require_full_name=True)
        ):
            active_state_is_relevant = True
            active_state_label = "name"
            decision_path.append("fsm:active_llm_name_relevant")
        suppress_active_field_updates = str(state_before_entities or "").startswith("collect_") and (active_direct_clarification or not active_state_is_relevant)
        if active_direct_clarification:
            decision_path.append("fsm:active_direct_clarification")
        elif suppress_active_field_updates:
            decision_path.append("fsm:state_irrelevant_skipped")
        username_save_requested = is_username_save_request(message_text)
        username_label = instagram_username_name_label(payload.instagram_username)
        if username_save_requested:
            memory["instagram_identity"] = payload.instagram_username or memory.get("instagram_identity")
            if should_apply_instagram_username_name(conversation, username_label):
                conversation["lead_name"] = username_label
                conversation["full_name"] = username_label
                memory["name_source"] = "instagram_username"
                decision_path.append("detected:name_instagram_username")
            elif username_label:
                memory["name_source"] = memory.get("name_source") or "existing_name_preserved"
                decision_path.append("noted:name_instagram_username")
        deterministic_name = None if suppress_active_field_updates or username_save_requested else extract_name(message_text, state_before_entities)
        name_candidate = deterministic_name
        require_full_name = state_before_entities == "collect_name"
        if is_booking_acknowledgement_message(message_text) or not is_valid_name_candidate(name_candidate, require_full_name=require_full_name):
            name_candidate = None
        if name_candidate:
            name_candidate = clean_name_text(name_candidate) or name_candidate
            conversation["lead_name"] = name_candidate
            conversation["full_name"] = name_candidate
            decision_path.append("detected:name" if deterministic_name else "extracted:name")
        direct_phone_candidate = extract_phone(message_text)
        if suppress_active_field_updates:
            phone_candidate = None
        else:
            phone_candidate = direct_phone_candidate if extracted.get("phone") or direct_phone_candidate else None
        invalid_phone_attempt = (not suppress_active_field_updates) and is_phone_like_attempt(message_text) and is_invalid_phone_attempt(message_text, state_before_entities)
        if phone_candidate:
            conversation["phone"] = phone_candidate
            decision_path.append("detected:phone" if extract_phone(message_text) else "extracted:phone")
        detected_service = detect_requested_service_from_text(message_text, cfg)
        if detected_service:
            remember_requested_service(conversation, memory, detected_service)
            decision_path.append("detected:service")
        elif booking_opt_in:
            carried_service = remember_requested_service(conversation, memory, known_requested_service(conversation, memory))
            if carried_service:
                decision_path.append("carried:service")
        direct_date = None if suppress_active_field_updates else extract_date(message_text)
        date_candidate = direct_date
        if date_candidate:
            try:
                datetime.datetime.strptime(date_candidate, "%Y-%m-%d")
                conversation["requested_date"] = date_candidate
                decision_path.append("detected:date" if direct_date else "extracted:date")
            except Exception:
                pass
        direct_time = None if suppress_active_field_updates else (extract_time_for_state(message_text, state_before_entities) or extract_time(message_text) or extract_generic_datetime_time(message_text))
        time_candidate = direct_time
        if time_candidate:
            try:
                datetime.datetime.strptime(time_candidate, "%H:%M")
                conversation["requested_time"] = time_candidate
                decision_path.append("detected:time" if direct_time else "extracted:time")
            except Exception:
                pass
        if extracted.get("customer_goal"):
            memory["customer_goal"] = extracted["customer_goal"]

        conversation["memory_state"] = memory

        msg_provided_name = bool(extract_name(message_text, conversation.get("state", "new")))
        msg_provided_phone = bool(extract_phone(message_text))
        msg_provided_date = bool(
            extract_date(message_text)
            or extract_time(message_text)
            or extract_generic_datetime_time(message_text)
        )
        user_provided_booking_info = msg_provided_name or msg_provided_phone or msg_provided_date
        question_intents = {"service_question", "price_question", "direct_answer", "fallback"}
        is_question_intent = intent in question_intents
        has_question_mark = "?" in message_text
        should_create_appointment = bool(user_provided_booking_info and not is_question_intent and not has_question_mark)

        # 3. BOOKING FINITE STATE MACHINE (Only if booking intent or inside active flow)
        appointment_created = False
        appointment_id = None
        curr_state = conversation.get("state", "new")
        state_changed_by_fsm = False
        invalid_phone_prompt = False
        
        active_fsm_applies = curr_state.startswith("collect_") and active_state_is_relevant and active_state_label != "llm_flow"
        if suppress_active_field_updates and not active_direct_clarification:
            # VIBE CODING: FSM has no mouth. Preserve LLM reply; only state/DB may change.
            if is_simple_greeting(message_text) or is_active_salutation_message(message_text):
                intent = "direct_answer"
                decision_path.append("fsm:active_greeting_preserve_llm_reply")
            elif (
                curr_state == "collect_name"
                and not conversation.get("full_name")
                and is_collect_name_continue_signal(message_text)
            ):
                intent = "direct_answer"
                decision_path.append("fsm:collect_name_continue_preserve_llm")
            else:
                recovery_reply = build_active_state_recovery_reply(curr_state)
                if recovery_reply:
                    if final_reply_source == "llm_raw" and can_preserve_valid_llm_reply_from_overwrite(reply_text, appointment_created=appointment_created, appointment_id=appointment_id):
                        intent = "direct_answer"
                        decision_path.append("fsm:active_state_recovery_preserved_llm")
                    else:
                        decision_path.append("fsm:active_state_recovery_deferred_to_llm")
        if not handoff and active_state_label != "llm_flow" and not suppress_active_field_updates and (booking_opt_in or intent in ["booking_request", "active_booking"] or active_fsm_applies):
            carried_service = remember_requested_service(conversation, memory, known_requested_service(conversation, memory))
            has_service = bool(carried_service)
            has_phone = bool(conversation.get("phone"))
            has_name = bool(conversation.get("full_name") or conversation.get("lead_name"))
            has_date = bool(conversation.get("requested_date"))
            has_time = bool(conversation.get("requested_time"))

            previous_state = conversation.get("state", "new")
            if invalid_phone_attempt and not has_phone:
                conversation["state"] = "collect_phone"
                invalid_phone_prompt = True
            elif not has_service:
                conversation["state"] = "collect_service"
            elif not has_name:
                conversation["state"] = "collect_name"
            elif not has_phone:
                conversation["state"] = "collect_phone"
            elif not has_date or not has_time:
                conversation["state"] = "collect_datetime"
            elif should_create_appointment:
                conversation["state"] = "completed"
                conversation["appointment_status"] = "confirmed"
                try:
                    created = create_appointment(conn, conversation, payload.instagram_username)
                    appointment_id = int(created[0] if isinstance(created, tuple) else created)
                    appointment_created = True
                    conversation["appointment_id"] = appointment_id
                    handoff = False
                    decision_path.append("fsm:silent_appointment_created")
                except Exception as exc:  # noqa: BLE001
                    logger.error("Silent appointment creation failed: %s", exc)
                    conversation["state"] = "human_handoff"
                    conversation["appointment_status"] = "handoff"
                    conversation["assigned_human"] = True
                    appointment_created = False
                    appointment_id = None
                    handoff = True
                    reply_text = "Randevu kaydını tamamlamak için ekibimize aktarıyorum; kısa süre içinde kontrol edip size dönüş sağlayacağız."
                    final_reply_source = "fsm_guard"
                    decision_path.append("fsm:silent_appointment_failed")
                    decision_path.append("guard:appointment_create_failed")
            else:
                appointment_created = False
                appointment_id = None
                conversation["state"] = previous_state
            state_changed_by_fsm = conversation.get("state") != previous_state

        service_for_booking = known_requested_service(conversation, memory)
        if appointment_created:
            decision_path.append("fsm:confirmation_reply_deferred_to_llm")
        elif (
            booking_opt_in
            and service_for_booking
            and conversation.get("state") in {"collect_name", "collect_phone"}
        ):
            decision_path.append("fsm:service_carryover_deferred_to_llm")
        elif (curr_state.startswith("collect_") and active_state_is_relevant and not active_direct_clarification and (is_llm_error_reply(reply_text) or state_changed_by_fsm or invalid_phone_prompt or active_state_label in {"name_ack", "username_save"})):
            decision_path.append("fsm:active_booking_prompt_deferred_to_llm")

        if active_direct_clarification and (is_llm_error_reply(reply_text) or is_booking_field_collection_reply(reply_text)):
            intent = "direct_answer"
            decision_path.append("fsm:active_direct_clarification_deferred_to_llm")

        # 4. FINAL QUALITY GUARD (Post FSM Check)
        reply_text, guard_label = generic_quality_guard(reply_text, extracted, memory, cfg, message_text)
        if guard_label:
            if guard_label == "prevent_premature_confirm":
                reply_text = build_false_confirmation_guard_reply(conversation, memory)
                final_reply_source = "fsm_guard"
                decision_path.append("guard:block_false_appointment_confirmation")
            elif guard_label == "block_unconfigured_price_or_discount":
                final_reply_source = "fsm_guard"
            elif is_llm_error_reply(reply_text):
                final_reply_source = "fallback"
            decision_path.append(f"guard:{guard_label}")

        if is_appointment_confirmation_like_reply(reply_text) and (not appointment_created or not appointment_id):
            reply_text = build_false_confirmation_guard_reply(conversation, memory)
            final_reply_source = "fsm_guard"
            decision_path.append("guard:block_false_appointment_confirmation")

        fallback_reasons = []
        if intent == "fallback":
            fallback_reasons.append("llm_intent_fallback")
        if metrics.get("llm_error"):
            fallback_reasons.append("llm_error")
        if is_llm_error_reply(reply_text):
            fallback_reasons.append("final_reply_is_fallback_reply")
        metrics["fallback_reason"] = fallback_reasons
        metrics["llm_raw_reply_text"] = llm_raw_reply_text
        metrics["final_reply_source"] = final_reply_source
        
        # --- PHASE 2/3 SHADOW & SCALED PIPELINE START ---
        # Feature flag control
        shadow_mode = os.environ.get("ANSWER_FIRST_PIPELINE", "off")
        # Phase 5 full cutover: when ANSWER_FIRST_PIPELINE=on all scoped enforce flags
        # are automatically active — no need to set each individually in env.
        _full_cutover = shadow_mode == "on"
        enforce_direct_question = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION", "false").lower() == "true"
        enforce_missing_field_prompts = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS", "false").lower() == "true"
        enforce_completed_followups = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS", "false").lower() == "true"
        enforce_info_answers = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_INFO_ANSWERS", "false").lower() == "true"
        enforce_appointment_action_replies = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES", "false").lower() == "true"
        
        if shadow_mode in ("shadow", "on") or enforce_direct_question:
            from app.pipeline_wrapper import run_shadow_pipeline
            try:
                shadow_result = run_shadow_pipeline(
                    message_text=message_text,
                    conversation=conversation, 
                    memory=memory, 
                    extracted=extracted, 
                    result_dict=result_dict, 
                    old_outbound_text=reply_text,
                    commit_changes=False
                )
                metrics["answer_first_shadow"] = shadow_result
                
                # Active Direct Question Enforcement
                if enforce_direct_question:
                    curr_state = conversation.get("state", "new")
                    if curr_state in ("collect_name", "collect_phone", "collect_datetime") and active_direct_clarification:
                        # Use the raw AI candidate (the LLM answer to the direct question).
                        # We do NOT rely on the shadow safety guard here because the guard can
                        # false-positive on replies like "sizi arayacak" which are valid direct
                        # answers to clarification questions, not fake appointment confirmations.
                        # We only block if the reply looks like a confirmed appointment (has a
                        # definite date/time AND a final-call phrase AND a booking keyword).
                        direct_candidate = shadow_result.get("ai_reply_candidate") or shadow_result.get("new_outbound_text")
                        if direct_candidate and not is_appointment_confirmation_like_reply_strict(direct_candidate):
                            reply_text = direct_candidate
                            final_reply_source = "answer_first_enforced"
                            metrics["final_reply_source"] = final_reply_source
                            if "enforce:active_direct_question_answer_first" not in decision_path:
                                decision_path.append("enforce:active_direct_question_answer_first")
                                
            except Exception as e:
                logger.exception("shadow_pipeline_failed message_text=%s", sanitize_text(message_text or "")[:50])
                metrics["answer_first_shadow"] = {"error": str(e)}
        # --- PHASE 4A — FINAL BUILDER MISSING FIELD ENFORCEMENT ---
        if enforce_missing_field_prompts and not handoff and not appointment_created:
            curr_state_4a = conversation.get("state", "new")
            in_collect_state = curr_state_4a in ("collect_name", "collect_phone", "collect_datetime")
            # Only applies to missing field prompt paths — not appointment create/update/reschedule/duplicate
            if in_collect_state and active_state_label != "llm_flow" and final_reply_source in ("fsm", "fsm_direct_answer", "llm_raw", "answer_first_enforced", "fallback"):
                from app.pipeline_wrapper import check_missing_fields, build_final_missing_field_prompt
                try:
                    mf_result = check_missing_fields(conversation, memory)
                    missing = mf_result.get("missing_fields", [])
                    # Determine context flags
                    _is_direct_q = bool(active_direct_clarification)
                    _wants_booking = bool(
                        booking_opt_in
                        or intent in ("booking_request", "active_booking")
                        or active_fsm_applies
                    )
                    composed = build_final_missing_field_prompt(
                        ai_reply_candidate=reply_text,
                        missing_fields=missing,
                        direct_question=_is_direct_q,
                        wants_booking=_wants_booking,
                    )
                    if composed and composed != reply_text:
                        reply_text = composed
                        final_reply_source = "final_builder_missing_field"
                        metrics["final_reply_source"] = final_reply_source
                        decision_path.append("enforce:final_builder_missing_field_prompt")
                except Exception as e:
                    logger.exception("phase4a_final_builder_failed message_text=%s", sanitize_text(message_text or "")[:50])
        # --- PHASE 4A END ---

        # --- PHASE 4D — APPOINTMENT ACTION REPLY FINAL BUILDER ---
        if enforce_appointment_action_replies and not handoff:
            from app.pipeline_wrapper import build_appointment_action_reply, validate_appointment_reply_no_false_confirmation
            try:
                # Determine which action occurred
                _4d_action = "none"
                if appointment_created and appointment_id:
                    _4d_action = "appointment_created"
                elif not appointment_created and "guard:appointment_create_failed" in " ".join(decision_path):
                    _4d_action = "appointment_create_failed"

                # Build action result dict for the Final Builder
                _4d_result_dict = {
                    "action": _4d_action,
                    "db_success": appointment_created,
                    "appointment_created": appointment_created,
                    "appointment_updated": False,
                    "appointment_id": appointment_id,
                    "appointment_date": conversation.get("requested_date"),
                    "appointment_time": conversation.get("requested_time"),
                    "same_appointment_id": False,
                    "reschedule_date": None,
                    "reschedule_time": None,
                    "error": None,
                }

                _4d_reply = build_appointment_action_reply(
                    _4d_result_dict,
                    conversation=conversation,
                )
                _4d_text = _4d_reply.get("outbound_text")
                _4d_src = _4d_reply.get("source", "4d_no_action")

                if _4d_text and _4d_action != "none":
                    reply_text = _4d_text
                    final_reply_source = _4d_src
                    metrics["final_reply_source"] = final_reply_source
                    decision_path.append(f"enforce:4d_{_4d_src}")
                elif _4d_reply.get("block_reason"):
                    decision_path.append(f"4d_block:{_4d_reply['block_reason']}")

                # Invariant guard: reply_text must never contain false confirmation
                _is_safe, _block_reason = validate_appointment_reply_no_false_confirmation(
                    reply_text,
                    appointment_created=appointment_created,
                    appointment_updated=False,
                    appointment_id=appointment_id,
                )
                if not _is_safe:
                    final_reply_source = "4d_false_confirm_blocked"
                    metrics["final_reply_source"] = final_reply_source
                    decision_path.append(f"4d_invariant_block:{_block_reason}")

            except Exception as e:
                logger.exception("phase4d_appointment_action_reply_failed message_text=%s", sanitize_text(message_text or "")[:50])
        # --- PHASE 4D END ---

        # --- PHASE 4C — INFO / CONFIG ANSWER FINAL BUILDER ---
        if enforce_info_answers and not handoff and not appointment_created and not is_confirmed_generic_appointment(conversation):
            # Applies only to info/config answer paths (not FSM collect, not completed, not reschedule)
            _info_paths = (
                "reply:company_capability",
                "reply:service_overview_config",
                "reply:service_overview_llm_raw",
                "reply:preconsultation_explanation",
                "reply:service_price",
                "reply:business_fit",
                "reply:user_business_identity_llm_raw",
                "reply:user_business_identity_config",
            )
            _in_info_path = any(
                any(dp.startswith(p) for p in _info_paths)
                for dp in decision_path
            )
            if _in_info_path:
                from app.pipeline_wrapper import build_info_answer_final
                try:
                    _ai_candidate_4c = (result_dict.get("reply_text") or "").strip() or reply_text
                    _svc_label = known_requested_service(conversation, memory)
                    _is_price_q = is_price_question(message_text)
                    _wants_bkg = bool(booking_opt_in or intent in ("booking_request", "active_booking"))
                    _4c_result = build_info_answer_final(
                        _ai_candidate_4c,
                        cfg=cfg,
                        message_text=message_text,
                        service_label=_svc_label,
                        is_price_q=_is_price_q,
                        wants_booking=_wants_bkg,
                    )
                    _new_text_4c = _4c_result.get("outbound_text")
                    _src_4c = _4c_result.get("source", "info_ai_preserved")
                    if _new_text_4c and _new_text_4c != reply_text:
                        reply_text = _new_text_4c
                        final_reply_source = _src_4c
                        metrics["final_reply_source"] = final_reply_source
                        decision_path.append(f"enforce:4c_{_src_4c}")
                    if _4c_result.get("block_reason"):
                        decision_path.append(f"4c_block:{_4c_result['block_reason']}")
                except Exception as e:
                    logger.exception("phase4c_info_answer_failed message_text=%s", sanitize_text(message_text or "")[:50])
        # --- PHASE 4C END ---

        # --- PHASE 4B — COMPLETED FOLLOW-UP ANSWER-FIRST ENFORCEMENT ---
        if enforce_completed_followups and not handoff and not appointment_created:
            _is_completed = is_confirmed_generic_appointment(conversation)
            if _is_completed:
                from app.pipeline_wrapper import build_completed_followup_answer_first
                try:
                    # AI reply_candidate: use the LLM reply that was produced before FSM overrides.
                    # result_dict["reply_text"] is the raw LLM output; reply_text may already be
                    # a legacy FSM string — we prefer the raw LLM answer when flag=true.
                    _ai_candidate = (result_dict.get("reply_text") or "").strip() or reply_text
                    _4b_result = build_completed_followup_answer_first(
                        _ai_candidate,
                        appointment_created=appointment_created,
                        appointment_id=appointment_id,
                    )
                    _new_text = _4b_result.get("outbound_text")
                    _source = _4b_result.get("source", "completed_followup_ai")
                    if _new_text and _new_text != reply_text:
                        reply_text = _new_text
                        final_reply_source = _source
                        metrics["final_reply_source"] = final_reply_source
                        decision_path.append(f"enforce:4b_{_source}")
                    if _4b_result.get("block_reason"):
                        decision_path.append(f"4b_block:{_4b_result['block_reason']}")
                except Exception as e:
                    logger.exception("phase4b_completed_followup_failed message_text=%s", sanitize_text(message_text or "")[:50])
        # --- PHASE 4B END ---

        # --- PHASE 2/3 SHADOW PIPELINE END ---

        update_conversation_memory_after_bot_reply(conversation, reply_text, "|".join(decision_path))

        # Output payload syncs
        upsert_conversation(conn, conversation)
        crm_customer = upsert_customer_from_conversation(conn, conversation)
        persist_customer_identity_to_crm(conn, crm_customer, ensure_conversation_memory(conversation))
        if crm_customer:
            schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector", ""))
            
        if has_outbound_reply_for_trace(conn, payload.sender_id, trace_id):
            return duplicate_process_result(payload, conversation, metrics, "duplicate_outbound_trace_ignored", request_started_at)
        save_message_log(conn, payload.sender_id, "out", reply_text, build_outbound_raw_event(decision_path, trace_id, inbound_dedupe_key, inbound_platform, inbound_message_id))

        metrics["total_ms"] = elapsed_ms(request_started_at)
        queue_crm_sync(background_tasks, conversation, None, metrics)

        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=True,
            reply_text=reply_text,
            outbound_text=reply_text,
            llm_raw_reply_text=llm_raw_reply_text,
            final_reply_source=final_reply_source,
            handoff=handoff,
            conversation_state=conversation.get("state", "new"),
            appointment_created=appointment_created,
            appointment_id=appointment_id,
            normalized=build_normalized(conversation),
            metrics=metrics,
            decision_path=decision_path,
        )


def call_llm_json(system_prompt: str, user_text: str) -> dict:
    import requests, os, json, re, time
    from app.main import LLM_BASE_URL, LLM_API_KEY, LLM_MODEL
    llm_url = LLM_BASE_URL
    llm_key = LLM_API_KEY
    llm_model = LLM_MODEL
    fallback_model = os.getenv("LLM_FALLBACK_MODEL") or os.getenv("LLM_REPLY_ADVISORY_MODEL")

    models = []
    for model in [llm_model, fallback_model]:
        model = sanitize_text(str(model or ""))
        if model and model not in models:
            models.append(model)
    if not models:
        models = ["llama-3.3-70b-versatile"]

    headers = {"Authorization": f"Bearer {llm_key}", "Content-Type": "application/json"}
    last_error: Exception | None = None
    last_content = None
    for idx, model in enumerate(models):
        payload = {
            "model": model,
            "messages": [{"role": "system", "content": system_prompt}, {"role": "user", "content": user_text}],
            "temperature": 0.0,
            "max_tokens": 1000
        }
        try:
            resp = requests.post(f"{llm_url}/chat/completions", headers=headers, json=payload, timeout=30)
            status_code = getattr(resp, "status_code", 200)
            if status_code in {429, 500, 502, 503, 504} and idx < len(models) - 1:
                logger.warning("generic_core_llm_retry model=%s status=%s", model, status_code)
                time.sleep(0.6)
                continue
            resp.raise_for_status()
            content = resp.json()["choices"][0]["message"]["content"]
            last_content = content
            match = re.search(r'\{.*\}', content, re.DOTALL)
            if match:
                result = json.loads(match.group(0))
                result["_llm_model_used"] = model
                return result
            clean_content = sanitize_text(content)
            if clean_content:
                logger.warning("generic_core_llm_non_json_response using direct_answer fallback")
                return {
                    "intent": "direct_answer",
                    "reply_text": clean_content,
                    "extracted_entities": {},
                    "requires_human": False,
                    "_llm_model_used": model,
                }
            result = json.loads(content)
            if isinstance(result, dict):
                result["_llm_model_used"] = model
            return result
        except Exception as e:
            last_error = e
            if idx < len(models) - 1:
                logger.warning("generic_core_llm_retry_error model=%s error=%s", model, e)
                time.sleep(0.6)
                continue
    raise ValueError(f"LLM JSON Error: {last_error} - content: {last_content if last_content is not None else 'None'}")

def summarize_generic_business_context(context_json: str) -> dict[str, Any]:
    try:
        context = json.loads(context_json or "{}")
    except Exception:
        context = {}
    return {
        "keys": sorted(context.keys()),
        "service_catalog_count": len(context.get("service_catalog") or []),
        "has_service_descriptions": bool(context.get("service_descriptions")),
        "has_service_fit": bool(context.get("service_fit") or context.get("business_fit")),
        "has_unavailable_services": "unavailable_services" in context,
        "has_instruction_override": bool(context.get("instruction_override")),
    }


def invoke_generic_llm(message_text: str, conversation: dict, memory: dict, history: list[dict]) -> dict:
    cfg = get_config()
    business_context = build_generic_business_context(message_text, cfg)
    context_summary = summarize_generic_business_context(business_context)
    
    # Minimize context parsing, formatting user messages
    recent = "\\n".join([f"{msg.get('direction', 'IN').upper()}: {msg.get('message_text', '')}" for msg in history[-10:]])
    
    known_context = {
        key: memory.get(key)
        for key in ["customer_goal", "requested_service", "selected_service", "service_interest", "customer_sector", "customer_subsector", "contact_channel"]
        if memory.get(key)
    }
    available_slots = conversation.get("available_slots") or []
    slot_context = "\nMÜSAİT RANDEVU SLOTLARI (CRM'DE KESİN BOŞ):\n" + "\n".join(f"- {slot}" for slot in available_slots) if available_slots else "\nMÜSAİT RANDEVU SLOTLARI: Sistem şu an kesin boş slot listesi vermedi. Saat uydurma; net slot yoksa ekibin kontrol edeceğini söyle."
    # Exposing missing booking fields to explicitly direct the AI on what to ask if it proceeds to 'active_booking'
    missing = []
    if not known_requested_service(conversation, memory): missing.append("Hizmet Türü")
    if not conversation.get("lead_name"): missing.append("İsim Soyisim")
    if not conversation.get("phone"): missing.append("Telefon Numarası")
    if not conversation.get("requested_date") or not conversation.get("requested_time"): missing.append("Tarih ve Saat")
    
    today = datetime.datetime.now(TZ).date().strftime('%Y-%m-%d')
    system_prompt = f"""Sen {cfg.get('business_name')} firmasının Instagram DM asistanısın. Türkçe, doğal ve kısa yaz. BUGÜNÜN TARİHİ: {today}. Tarih gerekiyorsa YYYY-MM-DD hesapla.

BUSINESS CONTEXT:
{business_context}

BİLİNEN KONUŞMA BAĞLAMI:
{json.dumps(known_context, ensure_ascii=False) if known_context else '{}'}

KONUŞMA STİLİ:
- En son müşteri mesajını merkeze al; önce o mesaja doğrudan cevap ver.
- Önceki konuşmada zaten selamlaştıysanız tekrar "Merhaba/Selam" ile başlama.
- KISA VE NET YAZ: reply_text çoğu durumda 160 karakteri geçmesin; maksimum 2 kısa cümle olsun.
- Uzun açıklama, paragraf, madde madde liste ve satış metni yazma; müşteri detay isterse bile en kritik 1-2 noktayı söyle.
- Instagram DM gibi doğal yaz; cevap tek ekranda hızlı okunmalı.
- Abartılı tepki verme. "Harika", "Muhteşem", "Süper", "Mükemmel", "Şahane", "Çok iyi" gibi coşkulu açılışları kullanma.
- Kullanıcı sadece onay veriyorsa veya önceki soruya cevap veriyorsa tepki cümlesi yazma; doğrudan sonraki adımı sor ya da net cevabı ver. Örnek: "Ön görüşme için adınızı ve soyadınızı alabilir miyim?"
- En fazla 1 net soru sor; birden fazla eksik bilgiyi aynı anda sorma.
- Müşteri randevuya / ön görüşmeye "olur", "tamam", "yapalım" demeden isim, telefon veya tarih isteme. Soru sorduysa sadece cevap ver.
- Müşteri süreç hakkında soru soruyorsa ("nasıl oluyor?", "ne demek?", "anlamadım"), ÖNCE sadece soruyu cevapla. Müşteri "olur", "tamam", "yapalım" demeden isim, telefon veya tarih isteme.
- Genel kurumsal tanıtım, hizmet kataloğu dökümü ve alakasız çapraz satış yapma.
- Sadece Business Context'teki bilgiye dayan; fiyat, süre, hizmet, indirim, çalışma saati veya müsaitlik uydurma.
- Business Context'teki sunulmayan hizmetler bilgisini sadece kullanıcı doğrudan "siz X yapıyor musunuz/veriyor musunuz?" diye sorarsa kullan; müşteri kendi sektörünü söylüyorsa bu listeyi dışlama cevabına çevirme.
- Kullanıcı fiyat sorarsa ilgili hizmet biliniyorsa fiyatı doğrudan söyle; bilinmiyorsa tek soru ile hangi hizmet olduğunu sor.
- Kullanıcı doğrudan soru soruyorsa önce soruyu cevapla; isim/telefon/tarih/saat promptuyla soruyu ezme. Gerekirse cevap sonuna tek cümlelik yumuşak yönlendirme ekle.
- Kullanıcı küçük sohbet veya selam yazdıysa satışa geçmeden kısa ve insani cevap ver.
- Kullanıcı açıkça randevu/ön görüşme/planlama isterse randevu akışına gir ve aşağıdaki eksik alanlardan sadece ilkini sor.

SATIŞ VE ÖN GÖRÜŞME YÖNLENDİRMESİ:
- Müşteri hizmete olumlu ilgi gösterirse sadece bilgi verme; karar vermesini kolaylaştıran kısa bir sonraki adım öner.
- "Evet", "Tamam", "Anladım", "Mantıklı", "Olur", "Görüşelim", "Planlayalım", "İyi olur" gibi olumlu sinyaller, önceki mesajlarda hizmet ilgisi varsa kısa ön görüşmeye yönlendirme için uygundur.
- Açık uçlu ve müşteriye yük bindiren soruları azalt; mümkünse "Size en uygun çözümü netleştirmek için kısa bir ön görüşme planlayabiliriz." gibi net ve nazik kapanış kullan.
- İsim alındıktan sonra tarih/saat sorulacaksa "hangi gün müsaitsiniz?", "yarın öğlen" veya "öğleden sonra" gibi belirsiz sorma. Sadece sistemin verdiği MÜSAİT RANDEVU SLOTLARI listesindeki kesin saatleri doğal dille öner.

RANDEVU SLOTLARI KURALLARI (KRİTİK):
- Eğer sistem sana MÜSAİT RANDEVU SLOTLARI listesi verdiyse, SADECE o saatlerden 2-3 tanesini seçerek müşteriye seçenekli soru sor.
  DOĞRU: "Ahmet Bey için en yakın uygun seçenekler salı 13:00 veya cuma 15:00 görünüyor; hangisi sizin için uygun olur?"
  YANLIŞ: "Hangi gün müsaitsiniz?" (Asla açık uçlu sorma!)
  YANLIŞ: "Yarın öğlen mi yoksa öğleden sonra mı uygun?" (Belirsiz saat sorma!)
  YANLIŞ: "Yarın 10:00 müsait mi?" (Eğer listede yoksa teklif etme!)
- Müşteri dolu bir saat söylerse, nazikçe belirt ve listedeki boş saatleri teklif et: "Maalesef 15:00 dolu, ancak 14:00 veya 16:00 müsait. Uygun mu?"
- Eğer liste boşsa, "Uygunluğumuzu kontrol edip size döneceğiz" de ve ekibe bırak.

TARİH VE DÜZELTME KURALLARI:
- Müşteri "yarın öğlen", "cumartesi 15:00" gibi göreceli tarih/saat söylerse, bunu KABUL ET. "Uygun gün ve saati net yazın" diyerek inat etme.
- Eğer müşteri tarih ve saati zaten verdiyse ve sonradan ismini/telefonunu düzeltmek isterse, düzeltmeyi hemen yap ve özür dile. Tarih/saati tekrar sorma, akışa devam et.
- Tarih ve saat alındığında, görüşmeyi kabul ettiğini gösteren güvenli ve kararlı bir kapanış yap.
- ASLA "Müsaitliği kontrol edeceğim", "Size döneceğim", "Bakıp haber vereceğim" gibi pasif ve şüphe uyandıran ifadeler kullanma. Sen bu süreci yönetiyorsun.
- "Randevunuz oluşturuldu/onaylandı" gibi kesin sistem mesajları verme (Bunu arka plan sistemi yapacak).
- DOĞRU ÖRNEK: "Yarın saat 14:00 için ön görüşmenizi not aldım. Detayları sizinle paylaşacağım."
- YANLIŞ ÖRNEK: "Müsaitliği kontrol edip size döneceğim."
- Her yanıtta en fazla 1 soru sor; cevaplar kısa, doğal, profesyonel ve Instagram DM dilinde kalsın.
- Yanıt uzuyorsa kısalt: önce soruyu cevapla, sonra gerekiyorsa tek kısa yönlendirme ekle.
- İSİM ÇAKIŞMASI KURALI: "{cfg.get('human_contact_name')} Çakmak" tam adı bizim ekip liderimizdir. Müşteri tam olarak "Ali" değil "{cfg.get('human_contact_name')} Çakmak" yazarsa nazikçe "Sizin adınızı ve soyadınızı alabilir miyim?" diye sor. Tek başına "Berkay" normal bir isimdir, engelleme.
- İSİM DÜZELTME KURALI: Kullanıcı ismini düzeltirse ("Ben X değilim, adım Y"), ÖNCELİKLE özür dile ve düzelttiğini belirt ("Kusura bakmayın, hemen düzeltiyorum [yeni isim] Bey"), SONRA bir sonraki adıma geç. Asla düzeltmeyi atlayıp direkt sonraki soruya geçme.
- ENTITY ÇIKARIM KURALLARI (KRİTİK): lead_name çıkarırken SADECE saf isim ve soyismi al. Konuşma dolgusu, zamir ve bağlaçları ("aslında", "ben", "adım", "diye", "yani", "işte") KESİNLİKLE dahil etme. AYNI KURAL phone, requested_date, requested_time için de geçerli: sadece saf veriyi çıkar, ekstra kelime ekleme.
- KONUŞMA BAĞLAMI KURALI: Her yeni mesajı bağımsız değerlendir. Müşteri yeni bir konu açarsa ("dövme yapıyor musunuz?", "randevuyu iptal et", "indirim var mı?"), öncelikle O konuyu cevapla. Önceki konuşma bağlamını sadece aynı konu devam ediyorsa veya eksik bilgi (isim, telefon, tarih) tamamlamak için kullan. Asla eski konuyu yeni sorunun önüne geçirme.

GEÇMİŞ VE ŞİMDİ AYIRIMI (KRİTİK):
- SON gelen mesaj, önceki tüm mesajlardan ve geçmiş bilgilerden daha önceliklidir.
- Eğer müşteri son mesajında adını veya bilgisini düzeltiyorsa (örn: "Ben Burak değilim, adım Selin"), geçmişte ne yazıyorsa yoksay ve YENİ bilgiye göre hitap et.
- Eğer müşteri "Randevumu değiştirmek istiyorum" derse, geçmiş randevu bilgisini hatırla ve kullan. Ancak müşteri yeni bir taleple sıfırdan geldiyse, eski randevu bilgileriyle kendi başına yeni bir randevu OLUŞTURMA; bilgileri mutlaka teyit et.

RANDEVU AKIŞI:
Eğer son konuşmada veya hafızada bir hizmet zaten biliniyorsa (requested_service / selected_service / service_interest), booking opt-in geldiğinde bu hizmeti kullan; "hangi hizmeti araştırıyorsunuz?" diye tekrar sorma.
Şu an randevu için eksik olan kritik bilgiler: {', '.join(missing) if missing else 'YOK. Randevu Onaylanabilir.'}
İsim sorarken "sisteme kaydetmek için" deme; "ön görüşme için" de. Bağlama göre görüşmeye katılacak kişinin adını iste.

İŞLETME BİLGİSİ (Business Context):
{business_context}
{slot_context}

SON KONUŞMA GEÇMİŞİ:
{recent}

Müşterinin yeni mesajını incele. Oku ve aşağıdaki JSON formatına SIKI SIKIYA uygun bir yanıt dön:
{{
    "intent": "direct_answer" | "service_question" | "price_question" | "booking_request" | "active_booking" | "human_handoff" | "fallback",
    "reply_text": "Müşteriye yazacağın Türkçe doğal yanıt",
    "extracted_entities": {{
        "lead_name": "Eğer bu mesajda veya geçmişte müşterinin ismini bulduysan çıkar, yoksa null",
        "phone": "Eğer tam bir telefon numarası verildiyse çıkar, yoksa null",
        "requested_service": "Eğer config.services içinden biri istenmişse o hizmetin ismini yaz, yoksa null",
        "requested_date": "YYYY-MM-DD olarak tarih (varsa), yoksa null",
        "requested_time": "HH:MM olarak saat (varsa), yoksa null",
        "customer_goal": "Müşterinin elde etmek istediği amaç. (Yoksa null)"
    }},
    "requires_human": true | false
}}"""

    try:
        result = call_llm_json(system_prompt, message_text)
        if isinstance(result, dict):
            result["_context_summary"] = context_summary
            result["_llm_error"] = None
        return result
    except Exception as e:
        logger.error(f"Generic engine LLM Error: {e}")
        return {
            "intent": "fallback",
            "reply_text": cfg.get("fallback_reply") or "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
            "extracted_entities": {},
            "requires_human": False,
            "_context_summary": context_summary,
            "_llm_error": str(e),
        }

def reply_mentions_unconfigured_price_or_discount(reply: str | None) -> bool:
    lowered = sanitize_text(reply or "").lower()
    if not lowered:
        return False
    if any(token in lowered for token in ("indirim", "kampanya", "ucretsiz", "ücretsiz")):
        return True
    if "₺" in (reply or ""):
        return True
    if re.search(r"(?<![a-z0-9])(?:tl|try|usd|eur|dolar|euro)(?![a-z0-9])", lowered):
        return True
    if re.search(r"\b\d{2,}(?:[.,]\d{2,3})*\s*(?:lira|try|aylik|aylık)\b", lowered):
        return True
    return bool(re.search(r"\b(?:aylik|aylık)\s+\d", lowered))


def has_configured_price_for_reply(cfg: dict[str, Any], extracted: dict, memory: dict) -> bool:
    service_label = extracted.get("requested_service") or memory.get("requested_service") or memory.get("selected_service") or memory.get("service_interest")
    service = find_service_config(cfg, service_label, memory)
    return bool(service and sanitize_text(service.get("price") or ""))


def build_unconfigured_price_guard_reply(message_text: str | None, extracted: dict, memory: dict) -> str:
    service = service_reply_phrase(extracted.get("requested_service") or memory.get("requested_service") or memory.get("selected_service") or memory.get("service_interest"))
    if is_price_question(message_text or ""):
        return f"{service.capitalize()} için fiyat kapsamınıza göre ön görüşmede netleşir; net rakamı ihtiyacı gördükten sonra paylaşabiliriz."
    return f"{service.capitalize()} tarafında süreci otomatikleştirip mesaj yanıtlama, takip ve randevu akışlarını daha düzenli hale getirebiliriz. İsterseniz kısa bir ön görüşmede ihtiyacınızı netleştirelim."


def compact_overlong_reply(reply: str | None, *, max_chars: int = 300, max_sentences: int = 3) -> str | None:
    clean = re.sub(r"\s+", " ", str(reply or "")).strip()
    if not clean:
        return None
    lowered = sanitize_text(clean).lower()
    if re.search(r"\d", clean) or is_appointment_confirmation_like_reply(clean) or re.search(r"(?<![a-z0-9])(?:tl|try|usd|eur)(?![a-z0-9])", lowered) or any(token in lowered for token in ("fiyat", "ucret", "ücret")):
        return None
    sentences = [part.strip() for part in re.split(r"(?<=[.!?…])\s+", clean) if part.strip()]
    if len(clean) <= max_chars and len(sentences) <= max_sentences:
        return None
    compact = " ".join(sentences[:max_sentences]) if len(sentences) > max_sentences else clean
    if len(compact) > max_chars:
        compact = compact[:max_chars].rsplit(" ", 1)[0].rstrip(" ,;:-") + "."
    return compact if compact and compact != clean else None


def generic_quality_guard(reply: str, extracted: dict, memory: dict, cfg: dict, message_text: str | None = None) -> Tuple[str, Optional[str]]:
    # 1. Config Service Matching
    if extracted.get("requested_service"):
        valid_services = [s.get("name", "").lower() for s in cfg.get("service_catalog", [])] + [s.get("display", "").lower() for s in cfg.get("service_catalog", [])]
        matched = False
        requested_service = extracted["requested_service"].lower()
        for svc in valid_services:
            if svc and (svc in requested_service or requested_service in svc):
                matched = True
        if not matched:
            extracted["requested_service"] = None
            
    # 2. Prevent Booking confirmed without real fields locally
    if reply and "Kaydınız oluşturuldu" in reply and (not memory.get("customer_phone") or not memory.get("requested_service")):
        return "İşlemlerinize devam edebilmem için lütfen bilgileri eksiksiz tamamlayalım.", "prevent_premature_confirm"

    if reply_mentions_unconfigured_price_or_discount(reply) and not has_configured_price_for_reply(cfg, extracted, memory):
        return build_unconfigured_price_guard_reply(message_text, extracted, memory), "block_unconfigured_price_or_discount"

    cleaned_reply = strip_leading_greeting_for_non_greeting(message_text or "", reply)
    if cleaned_reply != reply:
        return cleaned_reply, "strip_repeated_greeting"

    compact_reply = compact_overlong_reply(reply)
    if compact_reply:
        return compact_reply, "compact_overlong_reply"
        
    return reply, None
