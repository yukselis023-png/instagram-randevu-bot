import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

# 1) Restore original signature for apply_ai_first_quality_overrides
sig1_target = "def apply_ai_first_quality_overrides(conversation: dict, message_text: str, history: list, llm_data: dict | None, direct_service_meta: dict | None, direct_service: str | None) -> dict:"
sig1_replacement = "def apply_ai_first_quality_overrides(message_text: str, decision: dict, conversation: dict, history: list = None) -> dict:"

code = code.replace(sig1_target, sig1_replacement)

# 2) Restore original signature for guard_and_repair_final_answer
# In my strict version I wrote: def guard_and_repair_final_answer(history: list, latest_decision: dict, user_msg: str, conversation_state: dict) -> dict:
# Or wait, what exactly is in the file right now? Let's check with strict replace
sig2_old_pattern = r"def guard_and_repair_final_answer\(history.*?-> dict:"
sig2_replacement = """def guard_and_repair_final_answer(message_text: str, reply: str, conversation_state: dict, history: list = None, decision_label: str = None) -> dict:
    history = history or []
    fail_reasons = []
    _reply = reply
    _intent = decision_label
"""
# And I have to remove the first lines of the old guard block that try to extract _reply and _intent
# The old one had:
#    fail_reasons = []
#    _reply = latest_decision.get("reply_text") or ""
#    _intent = latest_decision.get("intent")

old_guard_intro = """def guard_and_repair_final_answer(history: list, latest_decision: dict, user_msg: str, conversation_state: dict) -> dict:
    fail_reasons = []
    _reply = latest_decision.get("reply_text") or ""
    _intent = latest_decision.get("intent")"""

new_guard_intro = """def guard_and_repair_final_answer(user_msg: str, reply: str, conversation_state: dict, history: list = None, decision_label: str = None) -> dict:
    history = history or []
    fail_reasons = []
    _reply = reply
    _intent = decision_label"""

code = code.replace(old_guard_intro, new_guard_intro)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Signatures restored.")
