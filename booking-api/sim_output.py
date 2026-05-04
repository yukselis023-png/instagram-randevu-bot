import sys
import json
from app.main import generate_reply

test_queries = [
    "Otomasyon hizmeti işime yarar mı?",
    "CRM işime yarar mı?",
    "Web sitesi bana uygun mu?",
    "Sosyal medya yönetimi işime yarar mı?",
    "Reklam hizmeti bana uygun mu?",
    "Otomasyon ne demek?",
    "CRM ne demek?",
    "Web tasarımla web sitesi aynı şey mi?",
    "Ön görüşmede ne konuşacağız?",
    "Ödeme nasıl yapılıyor?",
]

conversation = {
    "state": "new",
    "history": [],
    "memory_state": {"customer_sector": "kuaför"} # Force worst-case scenario: old sector memory exists
}

results = []
for q in test_queries:
    try:
        reply, updated_conv = generate_reply(q, conversation.copy())
        results.append({"q": q, "reply": reply})
    except Exception as e:
        results.append({"q": q, "reply": f"ERROR: {str(e)}"})

for res in results:
    print(f"INPUT:  {res['q']}")
    print(f"OUTPUT: {res['reply']}\n")
