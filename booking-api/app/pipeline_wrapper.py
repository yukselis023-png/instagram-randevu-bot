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
