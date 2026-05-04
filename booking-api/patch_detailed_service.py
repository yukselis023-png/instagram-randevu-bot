import re

with open("app/main.py", "r", encoding="utf-8") as f:
    code = f.read()

target = """def is_detailed_service_question(text: str, history: list) -> bool:
    lowered = sanitize_text(text).lower()
    is_expanding = any(w in lowered for w in ["bu kadar mi", "bu kadar mı", "baska", "başka", "daha detayli", "daha detaylı", "alt hizmet"])
    if not is_expanding:
        return False
    recent_outbound = get_last_outbound_text(history).lower()
    has_recent_overview = any(w in recent_outbound for w in ["web tasarim", "reklam", "otomasyon", "sosyal medya"])
    return has_recent_overview or "hizmet" in lowered"""

replacement = """def is_detailed_service_question(text: str, history: list) -> bool:
    lowered = sanitize_text(text).lower()
    is_expanding = any(w in lowered for w in ["bu kadar mi", "bu kadar mı", "baska", "başka", "daha detayli", "daha detaylı", "alt hizmet", "detay"])
    # Eger direkt soruyorsa (bu kadar mi falan icinde kelime varsa direk kabul et if very explicit)
    if "bu kadar" in lowered or "neler" in lowered and "baska" in lowered:
        return True
    if not is_expanding:
        return False
    # check history if it was recently discussed
    recent_outbound = get_last_outbound_text(history).lower()
    has_recent_overview = any(w in recent_outbound for w in ["web tasarim", "reklam", "otomasyon", "sosyal medya"])
    return has_recent_overview or "hizmet" in lowered or is_expanding"""

if target in code:
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(code.replace(target, replacement))
    print("Detailed service fixed")
else:
    print("Not found :(")
