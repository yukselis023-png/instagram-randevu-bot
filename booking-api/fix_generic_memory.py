with open("app/generic_core.py", "r", encoding="utf-8") as f:
    text = f.read()

target = '''    memory = ensure_conversation_memory(conversation)
    if result.get("extracted_lead_name"):
        memory["customer_name"] = result["extracted_lead_name"]
    if result.get("extracted_phone"):
        memory["customer_phone"] = result["extracted_phone"]'''

replacement = '''    memory = ensure_conversation_memory(conversation)
    if result.get("extracted_lead_name"):
        memory["customer_name"] = result["extracted_lead_name"]
    if result.get("extracted_phone"):
        memory["customer_phone"] = result["extracted_phone"]
    conversation["memory_state"] = memory'''

text = text.replace(target, replacement)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(text)

