import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the NameError
old_err_block = """        if is_service_term_clarification(message_text):
            decision["reply_text"] = build_service_term_clarification_reply(message_text)"""

new_err_block = """        if is_service_term_clarification(message_text):
            return False"""

text = text.replace(old_err_block, new_err_block)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

with open("tests/test_dm_quality_scenarios.py", "r", encoding="utf-8") as f:
    t_text = f.read()

t_text = t_text.replace("from app.main import generate_reply, build_ai_first_decision", "from app.main import build_ai_first_decision")

with open("tests/test_dm_quality_scenarios.py", "w", encoding="utf-8") as f:
    f.write(t_text)

print("done")
