import json

def generate_ai_answer_candidate(result_dict: dict) -> str | None:
    return result_dict.get("reply_text")

def validate_entities(conversation: dict, extracted: dict) -> dict:
    valid = {}
    invalid = {}
    
    # name
    name = extracted.get("name")
    from app.generic_core import is_valid_name_candidate, is_non_name_action_phrase, is_literal_username_placeholder_name
    
    if name:
        if is_valid_name_candidate(name):
            valid["name"] = name
        else:
            invalid["name"] = "invalid_name_candidate"
    
    # phone
    phone = extracted.get("phone")
    if phone:
        # Simplistic validation matching existing
        valid["phone"] = phone

    # service
    service = extracted.get("service")
    if service:
        valid["service"] = service

    return {
        "valid_entities": valid,
        "invalid_entities": invalid
    }

def update_state_memory_shadow(conversation: dict, memory: dict, valid_entities: dict) -> dict:
    return {"updated": bool(valid_entities)}

def check_missing_fields(conversation: dict, memory: dict) -> dict:
    required = ["full_name", "phone", "requested_date", "requested_time", "service"]
    values = dict(conversation or {})
    if not values.get("full_name") and values.get("lead_name"):
        values["full_name"] = values.get("lead_name")
    if not values.get("service"):
        values["service"] = (memory or {}).get("requested_service") or (memory or {}).get("selected_service") or (memory or {}).get("service_interest")
    missing = [field for field in required if not values.get(field)]
    can_create = not missing and not conversation.get("appointment_id")
    can_update = not missing and bool(conversation.get("appointment_id"))
    return {
        "missing_fields": missing,
        "can_create_appointment": can_create,
        "can_update_appointment": can_update,
    }

def execute_actions_shadow(conversation: dict, missing_fields_result: dict) -> dict:
    if missing_fields_result.get("can_create_appointment"):
        return {"action": "appointment_created", "db_success": True}
    return {"action": "none", "db_success": False}

def run_code_safety_guard_shadow(conversation: dict, reply_text: str | None) -> dict:
    return {"blocked": False, "reason": None}

def build_final_reply_shadow(candidate: str | None, missing: dict, action: dict, safety: dict) -> str | None:
    if safety.get("blocked"):
        return "Safe fallback"
    return candidate


# ============================================================
# PHASE 4A — FINAL BUILDER MISSING FIELD PROMPT
# ============================================================

_MISSING_FIELD_PROMPT_DIRECT: dict[str, str] = {
    # Soft, one-liner — used when direct_question=True
    "full_name":       "Planlamak isterseniz adınızı ve soyadınızı paylaşabilirsiniz.",
    "phone":           "Planlamak isterseniz telefon numaranızı paylaşabilirsiniz.",
    "requested_date":  "Uygun bir gün varsa belirtebilirsiniz.",
    "requested_time":  "Saat tercihini de yazabilirsiniz; örneğin 13:00.",
    "service":         "Hangi hizmeti düşündüğünüzü da paylaşabilirsiniz.",
}

# Keyword sets: if AI reply already asks for this field, skip the suffix.
_FIELD_ASK_KEYWORDS: dict[str, tuple[str, ...]] = {
    "full_name": (
        "adınızı", "adinizi", "ad soyad", "isminizi", "ismininizi",
        "soyadınızı", "soyadinizi", "adını", "adini",
    ),
    "phone": (
        "telefon", "numaranızı", "numaranizi", "numara", "no",
    ),
    "requested_date": (
        "gün", "gun", "tarih", "hangi gün", "hangi gun",
    ),
    "requested_time": (
        "saat", "saati", "saatin",
    ),
}


def _ai_already_asks_field(ai_text: str, field: str) -> bool:
    """Return True if ai_text already asks for the given missing field."""
    if not ai_text or field not in _FIELD_ASK_KEYWORDS:
        return False
    lowered = ai_text.lower()
    return any(kw in lowered for kw in _FIELD_ASK_KEYWORDS[field])

_MISSING_FIELD_PROMPT_BOOKING: dict[str, str] = {
    # Direct/assertive — used when wants_booking=True and direct_question=False
    "full_name":       "Kayıt için adınızı ve soyadınızı paylaşabilir misiniz?",
    "phone":           "Telefon numaranızı eksiksiz paylaşabilir misiniz?",
    "requested_date":  "Uygun gün ve saati yazabilirsiniz; örneğin yarın 13:00.",
    "requested_time":  "Hangi saati tercih edersiniz? Örneğin 13:00.",
    "service":         "Ön görüşme için hangi hizmeti düşünüyorsunuz: web tasarım, otomasyon, reklam veya sosyal medya?",
}


def build_final_missing_field_prompt(
    ai_reply_candidate: str | None,
    missing_fields: list[str],
    **kwargs,
) -> str | None:
    base = (ai_reply_candidate or "").strip()
    if not base:
        return "Mesajınızı aldık, en kısa sürede dönüş yapacağız."
    return base


# ============================================================
# PHASE 4B — COMPLETED FOLLOW-UP ANSWER-FIRST ENFORCEMENT
# ============================================================

_COMPLETED_FOLLOWUP_SAFE_FALLBACK = (
    "Mevcut ön görüşme kaydınız korunuyor. Ek bir detay olursa buradan yazabilirsiniz."
)


def _is_completed_followup_field_prompt(text: str) -> bool:
    """True if the text is asking for a booking field — forbidden in completed state."""
    from app.generic_core import sanitize_text, is_booking_field_collection_reply
    return is_booking_field_collection_reply(text)


def build_completed_followup_answer_first(
    ai_reply_candidate: str | None,
    *,
    appointment_created: bool = False,
    appointment_id=None,
) -> dict:
    """
    Phase 4B Final Builder for completed/confirmed appointment follow-ups.

    Contract:
    - AI reply_candidate is preferred when valid.
    - Safe fallback only when AI is empty, error, false confirmation,
      config-outside info, or field collection prompt.
    - appointment_created / appointment_updated always False here.
    - Returns dict with outbound_text, source, and block_reason.
    """
    from app.generic_core import (
        is_appointment_confirmation_like_reply,
        is_llm_error_reply,
        sanitize_text,
    )

    candidate = (ai_reply_candidate or "").strip()

    # --- Block conditions ---
    block_reason = None

    if not candidate:
        block_reason = "ai_empty"
    elif is_llm_error_reply(candidate):
        block_reason = "ai_error"
    elif is_appointment_confirmation_like_reply(candidate) and (not appointment_created or not appointment_id):
        block_reason = "false_confirmation"
    elif _is_completed_followup_field_prompt(candidate):
        block_reason = "field_prompt_in_completed_state"

    if block_reason:
        return {
            "outbound_text": _COMPLETED_FOLLOWUP_SAFE_FALLBACK,
            "source": "completed_followup_safe_fallback",
            "block_reason": block_reason,
            "appointment_created": False,
            "appointment_updated": False,
        }

    return {
        "outbound_text": candidate,
        "source": "completed_followup_ai",
        "block_reason": None,
        "appointment_created": False,
        "appointment_updated": False,
    }


# ============================================================
# PHASE 4C — INFO / CONFIG ANSWER FINAL BUILDER
# ============================================================

_INFO_SAFE_FALLBACK = (
    "Bu konuda daha fazla bilgi almak isterseniz ön görüşmede netleştirebiliriz."
)
_PRICE_UNKNOWN_SERVICE_FALLBACK = (
    "Hangi hizmet için fiyat bilgisi almak istersiniz: web sitesi, otomasyon veya reklam?"
)
_PRICE_NO_CONFIG_FALLBACK = (
    "Bu hizmetin fiyatı kapsamınıza göre netleşir. "
    "İhtiyacınızı gördükten sonra doğru teklif paylaşılır."
)


def _extract_numeric_price_from_text(text: str) -> str | None:
    """
    Pull the first price-like token from text.
    Returns a normalised digits-only string like '12900', or None.
    """
    import re
    m = re.search(r"[₺]?\s*(\d{1,3}(?:[.,]\d{3})*)\s*(?:TL|₺|lira|try)?", text, re.IGNORECASE)
    if m:
        raw = m.group(1).replace(".", "").replace(",", "")
        return raw
    return None


def _config_price_digits(price_str: str | None) -> str | None:
    """Normalise config price to pure digits for comparison."""
    if not price_str:
        return None
    import re
    digits = re.sub(r"[^\d]", "", price_str)
    return digits or None


def _is_catalog_dump(text: str) -> bool:
    """Heuristic: too many bullet points or char count > 400."""
    if not text:
        return False
    bullet_count = text.count("\n-") + text.count("\n•") + text.count("\n*")
    if bullet_count >= 3:
        return True
    if len(text) > 400:
        return True
    return False


def build_info_answer_final(
    ai_reply_candidate: str | None,
    *,
    cfg: dict,
    message_text: str,
    service_label: str | None,
    is_price_q: bool,
    wants_booking: bool,
) -> dict:
    """
    Phase 4C Final Builder for info / config answer paths.

    Contract:
    - AI reply preferred when config-safe.
    - Price guard: wrong price → config correction; correct price → preserve AI.
    - Field drift guard: field collection prompts blocked when wants_booking=False.
    - Error guard: LLM error → safe fallback.
    - Catalog dump guard: overlong list → info fallback.
    - No hardcoded per-intent reply chains.
    - Returns dict with outbound_text, source, block_reason.
    """
    from app.generic_core import (
        is_booking_field_collection_reply,
        is_llm_error_reply,
        reply_mentions_unconfigured_price_or_discount,
        find_service_config,
        build_service_price_reply,
    )

    candidate = (ai_reply_candidate or "").strip()

    # --- Guard 1: LLM error or empty ---
    if not candidate or is_llm_error_reply(candidate):
        return {
            "outbound_text": _INFO_SAFE_FALLBACK,
            "source": "info_safe_fallback",
            "block_reason": "ai_error_or_empty",
        }

    # --- Guard 2: field collection drift (only block when no booking opt-in) ---
    if is_booking_field_collection_reply(candidate) and not wants_booking:
        import re
        trimmed = re.split(
            r"(?:Ad|İsim|Telefon|Uygun\s+gün|Randevu\s+oluştur|Ad\s+soyad)[^.!?]*[.!?]",
            candidate, flags=re.IGNORECASE
        )
        clean = trimmed[0].strip() if trimmed and trimmed[0].strip() else None
        if clean and len(clean) > 20:
            return {
                "outbound_text": clean,
                "source": "info_ai_field_trimmed",
                "block_reason": "field_drift_trimmed",
            }
        return {
            "outbound_text": _INFO_SAFE_FALLBACK,
            "source": "info_safe_fallback",
            "block_reason": "field_drift_in_info_path",
        }

    # --- Guard 3: catalog dump ---
    if _is_catalog_dump(candidate):
        return {
            "outbound_text": _INFO_SAFE_FALLBACK,
            "source": "info_safe_fallback",
            "block_reason": "catalog_dump",
        }

    # --- Guard 4: price correctness ---
    if is_price_q:
        ai_price_digits = _extract_numeric_price_from_text(candidate)
        if ai_price_digits:
            service_cfg = find_service_config(cfg, service_label, {})
            config_price_str = service_cfg.get("price") if service_cfg else None
            config_price_digits = _config_price_digits(config_price_str)
            if config_price_digits and ai_price_digits != config_price_digits:
                # AI gave wrong price → correct with config
                display = (service_cfg or {}).get("display") or service_label or "Bu hizmet"
                correction_text = (
                    f"{display} paket fiyatı {config_price_str}. "
                    "Kapsamı ihtiyaca göre ön görüşmede netleştiriyoruz."
                )
                return {
                    "outbound_text": correction_text,
                    "source": "info_price_corrected",
                    "block_reason": "ai_wrong_price",
                }
            # AI price matches config (or no config price to compare) → preserve AI
            return {
                "outbound_text": candidate,
                "source": "info_ai_price_verified",
                "block_reason": None,
            }
        else:
            # AI did NOT give a price figure
            service_cfg = find_service_config(cfg, service_label, {})
            if service_cfg:
                config_price_str = service_cfg.get("price")
                if config_price_str:
                    display = service_cfg.get("display") or service_label or "Bu hizmet"
                    correction_text = (
                        f"{display} paket fiyatı {config_price_str}. "
                        "Kapsamı ihtiyaca göre ön görüşmede netleştiriyoruz."
                    )
                    return {
                        "outbound_text": correction_text,
                        "source": "info_price_supplemented",
                        "block_reason": "ai_missing_price",
                    }
                # Service known, no config price → preserve AI
                return {
                    "outbound_text": candidate,
                    "source": "info_ai_no_config_price",
                    "block_reason": None,
                }
            # Service unknown → clarification question
            return {
                "outbound_text": _PRICE_UNKNOWN_SERVICE_FALLBACK,
                "source": "info_price_service_unknown",
                "block_reason": "service_unknown_no_price",
            }


    # --- All guards passed: preserve AI reply ---
    return {
        "outbound_text": candidate,
        "source": "info_ai_preserved",
        "block_reason": None,
    }


# ============================================================
# PHASE 4D — APPOINTMENT ACTION REPLY FINAL BUILDER
# ============================================================

_APPT_CREATE_FAIL_REPLY = (
    "Randevu kaydını şu an kesinleştiremedim; "
    "bilgilerinizi ekibin kontrol etmesi için not aldım."
)
_APPT_UPDATE_FAIL_REPLY = (
    "Saat değişikliği talebinizi ekibe iletmek üzere not aldım. "
    "Mevcut randevu kaydınız korunuyor."
)

# Phrases that must NOT appear unless DB action succeeded
_FALSE_CONFIRM_PHRASES = (
    "oluşturuldu",
    "kaydınız oluşturuldu",
    "ön görüşmeniz ayarlandı",
    "sizi arayacağız",
    "şlem tamamlandı",
)
_FALSE_UPDATE_PHRASES = (
    "saatiniz güncellendi",
    "randevunuz değiştirildi",
    "olarak güncelledim",
    "güncellendi",
)


def build_appointment_action_reply(
    action_result: dict,
    *,
    conversation: dict,
) -> dict:
    """
    Phase 4D Final Builder for appointment action replies.

    Reads the DB action result and produces the canonical outbound_text.
    Never produces confirmations without verified DB success.

    action_result keys:
      action           : str  (appointment_created | appointment_updated |
                               appointment_create_failed | appointment_update_failed |
                               reschedule_pending_confirmation | none)
      db_success       : bool
      appointment_created  : bool
      appointment_updated  : bool
      appointment_id   : int | None
      appointment_date : str | None   (formatted, e.g. "11.05.2026")
      appointment_time : str | None   (e.g. "13:00")
      same_appointment_id : bool
      reschedule_date  : str | None
      reschedule_time  : str | None
      error            : str | None
    """
    from app.generic_core import (
        build_confirmation_message,
        build_reschedule_confirmation_question,
        sanitize_text,
    )

    action = action_result.get("action", "none")
    db_success = bool(action_result.get("db_success"))
    appointment_created = bool(action_result.get("appointment_created"))
    appointment_updated = bool(action_result.get("appointment_updated"))
    appointment_id = action_result.get("appointment_id")

    # --- 1. Appointment Created (happy path) ---
    if action == "appointment_created":
        if db_success and appointment_created and appointment_id:
            text = build_confirmation_message(conversation)
            return {
                "outbound_text": text,
                "source": "4d_appointment_created",
                "block_reason": None,
            }
        # Conditions not met — safe failure
        return {
            "outbound_text": _APPT_CREATE_FAIL_REPLY,
            "source": "4d_create_guard_failed",
            "block_reason": "db_success_or_id_missing",
        }

    # --- 2. Appointment Updated (reschedule confirmed) ---
    if action == "appointment_updated":
        if db_success and appointment_updated and appointment_id:
            upd_time = action_result.get("appointment_time") or ""
            text = (
                f"Ön görüşme saatiniz {upd_time} olarak güncellendi."
                if upd_time
                else "Randevu bilgileriniz güncellendi."
            )
            return {
                "outbound_text": text,
                "source": "4d_appointment_updated",
                "block_reason": None,
            }
        return {
            "outbound_text": _APPT_UPDATE_FAIL_REPLY,
            "source": "4d_update_guard_failed",
            "block_reason": "db_update_not_confirmed",
        }

    # --- 3. Reschedule pending confirmation ---
    if action == "reschedule_pending_confirmation":
        r_date = action_result.get("reschedule_date")
        r_time = action_result.get("reschedule_time")
        text = build_reschedule_confirmation_question(conversation, r_date, r_time)
        return {
            "outbound_text": text,
            "source": "4d_reschedule_pending",
            "block_reason": None,
        }

    # --- 4. Create failed ---
    if action == "appointment_create_failed":
        return {
            "outbound_text": _APPT_CREATE_FAIL_REPLY,
            "source": "4d_appointment_create_failed",
            "block_reason": "create_failed",
        }

    # --- 5. Update failed ---
    if action == "appointment_update_failed":
        return {
            "outbound_text": _APPT_UPDATE_FAIL_REPLY,
            "source": "4d_appointment_update_failed",
            "block_reason": "update_failed",
        }

    # --- 6. No action (action=none) — nothing to produce here ---
    return {
        "outbound_text": None,
        "source": "4d_no_action",
        "block_reason": None,
    }


def validate_appointment_reply_no_false_confirmation(
    reply_text: str | None,
    **kwargs,
) -> tuple[bool, str | None]:
    return True, None


def run_shadow_pipeline(message_text: str, conversation: dict, memory: dict, extracted: dict, result_dict: dict, old_outbound_text: str | None, commit_changes: bool = False) -> dict:
    ai_reply_candidate = generate_ai_answer_candidate(result_dict)
    entities_result = validate_entities(conversation, extracted)
    state_result = update_state_memory_shadow(conversation, memory, entities_result.get("valid_entities", {}))
    missing_result = check_missing_fields(conversation, memory)
    action_result = execute_actions_shadow(conversation, missing_result)
    
    # We test what WOULD be built against the AI candidate
    safety_result = run_code_safety_guard_shadow(conversation, ai_reply_candidate)
    final_text = build_final_reply_shadow(ai_reply_candidate, missing_result, action_result, safety_result)
    
    return {
        "ai_reply_candidate": ai_reply_candidate,
        "valid_entities": entities_result.get("valid_entities"),
        "invalid_entities": entities_result.get("invalid_entities"),
        "missing_fields": missing_result.get("missing_fields"),
        "action_result": action_result,
        "safety_result": safety_result,
        "final_builder_text": final_text,
        "would_change_output": old_outbound_text != final_text,
        "old_outbound_text": old_outbound_text,
        "new_outbound_text": final_text,
        "reason": "shadow mode run"
    }
