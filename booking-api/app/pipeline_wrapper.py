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
    missing = []
    if not conversation.get("full_name"):
        missing.append("full_name")
    if not conversation.get("phone"):
        missing.append("phone")
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
