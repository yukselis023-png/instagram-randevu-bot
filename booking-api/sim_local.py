import sys
import json
from app.main import build_ai_first_decision

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

# Provide a harsh conversation state context (e.g. wrong sector)
conversation = {
    "state": "new",
    "memory_state": {"customer_sector": "kuaför", "customer_subsector": "berber"}
}

for q in test_queries:
    try:
        # Mock LLM calls by patching if needed, or see if it naturally falls into our hardcoded overrides before hitting standard LLM.
        decision = build_ai_first_decision(q, conversation.copy(), [], {})
        print(f"Soru: {q}")
        print(f"Cevap: {decision['reply_text']}")
        print(f"Intent: {decision.get('intent', 'N/A')}")
        print("-" * 50)
    except Exception as e:
        print(f"ERROR on '{q}': {e}")
