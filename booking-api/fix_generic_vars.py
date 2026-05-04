with open("app/generic_core.py", "r", encoding="utf-8") as f:
    text = f.read()

target = '''    memory = ensure_conversation_memory(conversation)
    if result.get("extracted_lead_name"):
        memory["customer_name"] = result["extracted_lead_name"]
    if result.get("extracted_phone"):
        memory["customer_phone"] = result["extracted_phone"]
    conversation["memory_state"] = memory'''

replacement = '''    if result.get("extracted_lead_name"):
        conversation["lead_name"] = result["extracted_lead_name"]
    if result.get("extracted_phone"):
        conversation["phone"] = result["extracted_phone"]'''

text = text.replace(target, replacement)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(text)

with open("tests/test_generic_core_engine.py", "r", encoding="utf-8") as f:
    t = f.read()
    
t = t.replace('mem["customer_name"]', 'conversation.get("lead_name")')
t = t.replace('assert mem["customer_name"] == "Remzi"', 'assert conversation.get("lead_name") == "Remzi"')
t = t.replace('assert mem["customer_phone"] == "05554443322"', 'assert conversation.get("phone") == "05554443322"')
t = t.replace('mem = ensure_conversation_memory(conversation)', '')
with open("tests/test_generic_core_engine.py", "w", encoding="utf-8") as f:
    f.write(t)
