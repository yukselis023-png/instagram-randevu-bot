import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# 1. fix greeting: replace the fallback "if not ... else build_natural_greeting_reply()" correctly.
# Oh, earlier I forgot to put smalltalk branch BEFORE greeting? Or maybe the greeting wasn't catching it?

# Actually "is_simple_greeting" and "is_smalltalk" should trigger BEFORE the LLM response is returned.

with open("app/main.py", "r", encoding="utf-8") as f:
    parts = f.read().split("def apply_ai_first_quality_overrides(")
    head = parts[0]
    tail_raw = parts[1]
    next_func_idx = tail_raw.find("def build_ai_first_emergency_reply(")
    tail = tail_raw[next_func_idx:]
    
    overrides_body = tail_raw[:next_func_idx]
    
    # fix greeting missing at the top
    missing_greeting = """    if is_smalltalk_message(message_text):
        decision["reply_text"] = build_natural_smalltalk_reply()
        decision["intent"] = "smalltalk"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_simple_greeting(message_text):
        decision["reply_text"] = build_natural_greeting_reply()
        decision["intent"] = "greeting"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
        
    if is_assistant_identity_question(message_text):
        decision["reply_text"] = build_assistant_identity_reply(conversation)
        decision["intent"] = "assistant_identity"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision
"""

    # We replace the assistant_identity block to inject these 3 correctly near the top
    overrides_body = re.sub(
        r"    if is_assistant_identity_question\(message_text\):.*?return decision",
        missing_greeting,
        overrides_body,
        flags=re.DOTALL | re.MULTILINE
    )

new_text = head + "def apply_ai_first_quality_overrides(" + overrides_body + tail
with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(new_text)

print("done")
