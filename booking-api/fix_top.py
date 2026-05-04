import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

target = """def apply_ai_first_quality_overrides(message_text: str, decision: dict[str, Any], conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:"""

replacement = """def apply_ai_first_quality_overrides(message_text: str, decision: dict[str, Any], conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    try:
        if is_ambiguous_appointment_question(message_text):
            decision["reply_text"] = build_ambiguous_appointment_reply()
            decision["intent"] = "ambiguous_appointment_disambiguation"
            decision["booking_intent"] = False
            decision["missing_fields"] = []
            decision["should_reply"] = True
            return decision
    except: pass
"""
code = code.replace(target, replacement)

# Detailed overview also at top!
target2 = """    if is_service_overview_question(message_text):"""
replacement2 = """    try:
        if is_detailed_service_question(message_text, history):
            decision["reply_text"] = build_detailed_service_reply()
            decision["intent"] = "detailed_service_overview"
            decision["booking_intent"] = False
            decision["missing_fields"] = []
            decision["should_reply"] = True
            return decision
    except: pass
    
    if is_service_overview_question(message_text):"""
code = code.replace(target2, replacement2)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)
print("Added effectively.")
