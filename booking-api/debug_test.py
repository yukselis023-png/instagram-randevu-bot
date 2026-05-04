from app import main

print("Testing web tasarim..")
decision = main.build_ai_first_decision("Web tasarimla web sitesi ayni sey mi?", {"state": "new", "memory_state": {}}, [], {})
print("INTENT DECIDED:", decision.get("intent"))
print("REPLY:", decision.get("reply_text"))
