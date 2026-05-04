import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

text = re.sub(
    r"def is_business_fit_question\(text: str\) -> bool:.*?(?=def build_business_fit_reply)",
    "def is_business_fit_question(text: str) -> bool:\n" +
    "    try:\n" +
    "        from app.main import sanitize_text, get_config\n" +
    "        lowered = sanitize_text(text).lower()\n" +
    "        triggers = [\"uygun mu\", \"uyar mi\", \"uyar mı\", \"isime yarar\", \"işime yarar\", \"faydali olur\", \"faydalı olur\", \"mantikli mi\", \"mantıklı mı\"]\n" +
    "        if not any(t in lowered for t in triggers):\n" +
    "            return False\n" +
    "        catalog = get_config().get('service_catalog', [])\n" +
    "        for svc in catalog:\n" +
    "            for kw in svc.get('keywords', []):\n" +
    "                if kw in lowered:\n" +
    "                    return True\n" +
    "        return False\n" +
    "    except:\n" +
    "        return False\n\n",
    text,
    flags=re.DOTALL
)

# And replace build_business_fit_reply
# Until next def or end of file
text = re.sub(
    r"def build_business_fit_reply\(.*?\) -> str:.*?(?=\n(?:def |# 6\. business|@))",
    "def build_business_fit_reply(conversation, message_text=None, history=None) -> str:\n" +
    "    try:\n" +
    "        from app.main import sanitize_text, get_config\n" +
    "        catalog = get_config().get('service_catalog', [])\n" +
    "        if message_text:\n" +
    "            lowered = sanitize_text(message_text).lower()\n" +
    "            for svc in catalog:\n" +
    "                for kw in svc.get('keywords', []):\n" +
    "                    if kw in lowered and 'fit_description' in svc:\n" +
    "                        return svc['fit_description']\n" +
    "        return f\"{get_config().get('business_name', '')} hizmetleri genel olarak hedeflerinize uygun olabilir. Detayları kısa bir görüşmede netleştirebiliriz.\"\n" +
    "    except:\n" +
    "        return \"Detayları kısa bir görüşmede netleştirebiliriz.\"",
    text,
    flags=re.DOTALL
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
print("done")
