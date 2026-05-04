with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace(
    'from app.main import get_config\n    if memory.get("customer_sector") or memory.get("customer_subsector") or memory.get("customer_goal"):',
    'if memory.get("customer_sector"):\n        pass\n    from app.main import get_config\n'
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
