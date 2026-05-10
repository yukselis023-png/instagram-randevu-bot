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
    """
    Produce canonical missing_fields list for the Final Builder.
    Order matters: full_name → phone → requested_date → requested_time → service
    The Final Builder asks only the FIRST missing field.
    """
    missing = []

    if not (conversation.get("full_name") or conversation.get("lead_name")):
        missing.append("full_name")

    if not conversation.get("phone"):
        missing.append("phone")

    if not conversation.get("requested_date"):
        missing.append("requested_date")

    if not conversation.get("requested_time"):
        missing.append("requested_time")

    if not conversation.get("service") and not memory.get("requested_service"):
        missing.append("service")

    return {
        "missing_fields": missing,
        "can_create_appointment": len(missing) == 0,
        "can_update_appointment": False
    }

def execute_actions_shadow(conversation: dict, missing_fields_result: dict) -> dict:
    if missing_fields_result.get("can_create_appointment"):
        return {"action": "appointment_created", "db_success": True}
    return {"action": "none", "db_success": False}

def run_code_safety_guard_shadow(conversation: dict, reply_text: str | None) -> dict:
    from app.generic_core import is_appointment_confirmation_like_reply
    if is_appointment_confirmation_like_reply(reply_text):
        return {"blocked": True, "reason": "false_confirmation"}
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
    *,
    direct_question: bool,
    wants_booking: bool,
) -> str | None:
    """
    Phase 4A Final Reply Builder.

    Priority contract (from spec):
    1. direct_question=True  → AI answer first, then optional soft 1-sentence prompt
    2. direct_question=False + wants_booking=True → net missing field prompt only
    3. direct_question=False + wants_booking=False → AI answer only, no field prompts

    Returns the composed outbound text, or None if no change is needed.
    """
    first_missing = missing_fields[0] if missing_fields else None

    if direct_question:
        # AI answer + optional soft suffix (max 1 sentence, only first missing field)
        base = (ai_reply_candidate or "").strip()
        if not base:
            return None
        if first_missing and first_missing in _MISSING_FIELD_PROMPT_DIRECT:
            suffix = _MISSING_FIELD_PROMPT_DIRECT[first_missing]
            # Avoid duplicating if the AI already mentioned it
            if suffix.lower()[:20] not in base.lower():
                return f"{base} {suffix}"
        return base

    if wants_booking and first_missing:
        # Net prompt for the first missing field — no AI prefix needed
        prompt = _MISSING_FIELD_PROMPT_BOOKING.get(first_missing)
        if prompt:
            return prompt

    # wants_booking=False or no missing fields — return AI reply as-is
    return ai_reply_candidate


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

    # --- Guard 5: hallucinated discount / campaign ---
    if reply_mentions_unconfigured_price_or_discount(candidate):
        service_cfg = find_service_config(cfg, service_label, {})
        config_price_str = (service_cfg or {}).get("price")
        display = (service_cfg or {}).get("display") or service_label
        if config_price_str and display:
            correction_text = (
                f"{display} paket fiyatı {config_price_str}. "
                "Kapsamı ihtiyaca göre ön görüşmede netleştiriyoruz."
            )
        else:
            correction_text = _PRICE_NO_CONFIG_FALLBACK
        return {
            "outbound_text": correction_text,
            "source": "info_discount_hallucination_blocked",
            "block_reason": "unconfigured_discount_or_price",
        }

    # --- All guards passed: preserve AI reply ---
    return {
        "outbound_text": candidate,
        "source": "info_ai_preserved",
        "block_reason": None,
    }


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
