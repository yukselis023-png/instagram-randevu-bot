import re
with open("app/generic_core.py", "r", encoding="utf-8") as f:
    code = f.read()

import_str = "import json\nimport re\nimport datetime"

if "import datetime" not in code:
    code = code.replace("import json\nimport re", import_str)

sys_prompt_old = "    system_prompt = f\"\"\"Sen {cfg.get('business_name')} firmasının dijital asistanısın."
sys_prompt_new = """    today = datetime.date.today().strftime('%Y-%m-%d')
    system_prompt = f\"\"\"Sen {cfg.get('business_name')} firmasının dijital asistanısın. Müşterilerle doğal, insansı ve yardımcı bir dilde Türkçe konuş. BUGÜNÜN TARİHİ: {today}. İstenen tarihi YYYY-MM-DD hesapla."""

code = code.replace(sys_prompt_old, sys_prompt_new)

# Protect extracted date
guard_old = """if extracted.get("requested_date"):
            conversation["requested_date"] = extracted["requested_date"]"""
guard_new = """if extracted.get("requested_date"):
            try:
                # Basic validation using datetime
                datetime.datetime.strptime(extracted["requested_date"], "%Y-%m-%d")
                conversation["requested_date"] = extracted["requested_date"]
            except Exception:
                pass"""

code = code.replace(guard_old, guard_new)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(code)
