import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix the broken f-string quotes
text = text.replace(
    'return "{get_config().get("business_name", "İşletme")} olarak ana hizmetlerimiz: {", ".join([s["display"] for s in get_config().get("service_catalog", [])])}"',
    'return f"{get_config().get(\'business_name\', \'İşletme\')} olarak ana hizmetlerimiz: {\', \'.join([s.get(\'display\', \'\') for s in get_config().get(\'service_catalog\', [])])}"'
)

text = text.replace(
    'return "Buradayım. Hangi hizmetimizle ({" ,".join([s["display"] for s in get_config().get("service_catalog", [])])}) ilgilendiğinizi yazarsanız net şekilde yardımcı olayım."',
    'return f"Buradayım. Hangi hizmetimizle ({\', \'.join([s.get(\'display\', \'\') for s in get_config().get(\'service_catalog\', [])])}) ilgilendiğinizi yazarsanız net şekilde yardımcı olayım."'
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

