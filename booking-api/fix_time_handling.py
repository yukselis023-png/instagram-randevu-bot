import re
with open("app/generic_core.py", "r", encoding="utf-8") as f:
    code = f.read()

guard_old = """if extracted.get("requested_time"):
            conversation["requested_time"] = extracted["requested_time"]"""
guard_new = """if extracted.get("requested_time"):
            try:
                datetime.datetime.strptime(extracted["requested_time"], "%H:%M")
                conversation["requested_time"] = extracted["requested_time"]
            except Exception:
                pass"""

code = code.replace(guard_old, guard_new)

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(code)
