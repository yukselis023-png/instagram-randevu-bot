with open("app/main.py", "r", encoding="utf-8") as f:    text = f.read()

# Make it catch "değil" etc.
old_logic = """    if all(k in lowered for k in ["ayni sey", "web"]) or all(k in lowered for k in ["aynı şey", "web"]):
        return True"""
        
new_logic = """    if (("ayni sey" in lowered or "aynı şey" in lowered) and "web" in lowered) or "farki ne" in lowered or "farkı ne" in lowered:
        return True"""

if old_logic in text:
    text = text.replace(old_logic, new_logic)
else:
    import re
    text = re.sub(r'if all\(k in lowered for k in \["ayni sey", "web"\]\) or all\(k in lowered for k in \["aynı şey", "web"\]\):\s*return True', new_logic, text)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("done")
