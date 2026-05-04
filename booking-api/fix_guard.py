import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

code = code.replace(
    'return {"ok": True, "reasons": [], "repaired": _reply}',
    'return {"ok": True, "reasons": [], "repaired": _reply, "reply_text": _reply}'
)
code = code.replace(
    'return {"ok": False, "reasons": fail_reasons, "repaired": safe_reply}',
    'return {"ok": False, "reasons": fail_reasons, "repaired": safe_reply, "reply_text": safe_reply}'
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(code)

print("Guard dictionary fields fixed.")
