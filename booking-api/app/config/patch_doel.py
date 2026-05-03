import json
with open("doel.json", "r", encoding="utf-8") as f:
    d = json.load(f)
for s in d['service_catalog']:
    if s['slug'] == 'otomasyon-ai':
        s['keywords'].extend(["otomatik cevap", "mesaj otomasyonu", "sistem kurma"])
with open("doel.json", "w", encoding="utf-8") as f:
    json.dump(d, f, ensure_ascii=False, indent=4)
