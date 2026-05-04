with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

start = text.find("def apply_ai_first_quality_overrides(")
if start != -1:
    end = text.find("def build_ai_first_emergency_reply", start)
    print(text[start:end])
