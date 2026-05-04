import re

# 1. FIX THE TESTS (Monkeypatch needs to return JSON string, not dict)
with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    tcode = f.read()

tcode = tcode.replace(
    'lambda *args, **kwargs: {"reply_text": "Web sitesi tarafinda sik ve guven veren yapi; 12.900 TL", "intent": "service_advice", "booking_intent": False}',
    'lambda *args, **kwargs: \'{"reply_text": "Web sitesi tarafinda sik ve guven veren yapi; 12.900 TL", "intent": "service_advice", "booking_intent": False}\''
)
tcode = tcode.replace(
    'lambda *args, **kwargs: {"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}',
    'lambda *args, **kwargs: \'{"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False}\''
)

with open("tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(tcode)


# 2. FIX THE GUARD logic (Ensure forced fallback replaces the AI's bad answer)
with open("app/main.py", "r", encoding="utf-8") as f:
    mcode = f.read()

guard_term_repair_target = """    elif first["reason"] == "repeated_greeting":
        safe_rep = "İşletmeniz için hangi alanda destek arıyorsunuz?"
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": first["reason"]}

    if is_service_term_clarification(message_text):
        safe_rep = build_service_term_clarification_reply(message_text)
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": "service_term_clarification"}

    repaired = build_safe_reply_builder(message_text, conversation, history, decision_label)"""

guard_term_repair_replacement = """    elif first["reason"] == "repeated_greeting":
        safe_rep = "İşletmeniz için hangi alanda destek arıyorsunuz?"
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": first["reason"]}
    
    if is_service_term_clarification(message_text):
        safe_rep = build_service_term_clarification_reply(message_text)
        return {"reply_text": safe_rep, "passed": True, "repaired": True, "reason": "service_term_clarification"}

    repaired = build_safe_reply_builder(message_text, conversation, history, decision_label)"""

mcode = mcode.replace(guard_term_repair_target, guard_term_repair_replacement)

# To ensure the final_answer_quality_guard actually REJECTS "12.900" for term clarification:
guard_check_target = """def final_answer_quality_guard(
    message_text: str,
    reply_text: str | None,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
    decision_label: str | None = None,
) -> dict[str, Any]:
    whitelist = ["""

guard_check_replacement = guard_check_target.replace("    whitelist = [", """
    # 0. Clarification Guard: if user asked "ne demek" but AI gave price/recommendation
    if is_service_term_clarification(message_text):
        if any(w in str(reply_text).lower() for w in ["tl", "fiyat", "paket", "reklam kampanyasi", "tutar"]):
            return {"passed": False, "reason": "term_clarification"}
            
    whitelist = [""")
mcode = mcode.replace(guard_check_target, guard_check_replacement)


with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(mcode)

print("done")
