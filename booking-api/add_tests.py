import re

tests_append = """
def test_term_clarification_otomasyon(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: {"reply_text": "Web sitesi tarafinda sik ve guven veren yapi; 12.900 TL", "intent": "service_advice", "booking_intent": False})
    decision = main.build_ai_first_decision("Otomasyon ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "otomatik yapmasidir" in reply or "tekrar eden isleri" in reply
    assert "12.900" not in reply

def test_term_clarification_crm(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: {"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False})
    decision = main.build_ai_first_decision("CRM ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "musteri takip sistemi" in reply

def test_term_clarification_landing_page(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: {"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False})
    decision = main.build_ai_first_decision("Landing page ne demek?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "reklamdan gelen" in reply or "ozel sayfadir" in reply

def test_term_clarification_web_tasarim(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: {"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False})
    decision = main.build_ai_first_decision("Web tasarimla web sitesi ayni sey mi?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "ayni anlamda kullanilir" in reply or "evet" in reply

def test_term_clarification_sosyal_medya(monkeypatch):
    from app import main
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: {"reply_text": "Web paketimiz var", "intent": "service_advice", "booking_intent": False})
    decision = main.build_ai_first_decision("Sosyal medya yonetimi neyi kapsiyor?", {"state": "new", "memory_state": {}}, [], {})
    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "icerik uretimi" in reply or "paylasim duzeni" in reply

def test_term_clarification_forced_bad_ai(monkeypatch):
    from app import main
    conversation = {"state": "new", "memory_state": {}}
    bad_reply = "Web sitesi tarafinda 12.900 TL"
    guarded = main.guard_and_repair_final_answer("Otomasyon ne demek?", bad_reply, conversation, [], decision_label="service_advice")
    reply = main.sanitize_text(guarded["reply_text"]).lower()
    assert guarded["passed"] is True
    assert "otomatik yapmasidir" in reply or "tekrar eden isleri" in reply
    assert "12.900" not in reply
"""

with open("tests/test_dm_quality_scenarios.py", "a", encoding="utf-8") as f:
    f.write(tests_append)

print("Tests added.")
