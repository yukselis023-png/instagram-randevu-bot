with open("app/generic_core.py", "r", encoding="utf-8") as f:
    text = f.read()

text = text.replace("import json\n        result = ", "result = ")

with open("app/generic_core.py", "w", encoding="utf-8") as f:
    f.write(text)

