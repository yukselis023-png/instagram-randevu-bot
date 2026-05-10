import os
import pytest
from app.main import IncomingMessage
from app.generic_core import process_instagram_message_generic

class MockConn:
    def cursor(self):
        class MockCur:
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def execute(self, *args, **kwargs): pass
            def fetchone(self): return None
            def fetchall(self): return []
        return MockCur()
    def commit(self): pass
    def rollback(self): pass

def test_live_bug_collect_phone_with_direct_clarification(monkeypatch):
    os.environ["ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION"] = "true"
    os.environ["ANSWER_FIRST_PIPELINE"] = "shadow"
    
    conn = MockConn()
    payload = IncomingMessage(
        sender_id="bug_usr",
        message_text="Beni arayacaklar mı ön görüşme yaparsak?"
    )
    
    conversation = {
        "instagram_user_id": "bug_usr",
        "state": "collect_phone",
        "service": "web tasarım",
        "full_name": "Berkay",
        "phone": None
    }
    
    # Mock LLM to return a nice direct answer
    extracted = {
        "reply_text": "Evet, ön görüşmede Berkay Bey veya ekibimiz sizi arayacak."
    }
    
    import app.generic_core
    import app.main
    
    class MockGetConnContext:
        def __enter__(self): return conn
        def __exit__(self, *args): pass

    monkeypatch.setattr(app.generic_core, "get_conn", lambda: MockGetConnContext())
    monkeypatch.setattr(app.generic_core, "try_acquire_inbound_processing_lock", lambda *a: True)
    monkeypatch.setattr(app.generic_core, "has_processed_inbound_message", lambda *a: False)
    monkeypatch.setattr(app.generic_core, "get_or_create_conversation", lambda *a: conversation)
    monkeypatch.setattr(app.generic_core, "get_recent_message_history", lambda *a: [])
    monkeypatch.setattr(app.generic_core, "get_config", lambda *a: {"human_contact_name": "Berkay"})

    monkeypatch.setattr(app.generic_core, "call_llm_json", lambda *args, **kwargs: {
        "reply_text": "Evet, ön görüşmede Berkay Bey sizi arayacak.",
        "reply_text_candidate": "Evet, ön görüşmede Berkay Bey sizi arayacak."
    })
    monkeypatch.setattr(app.generic_core, "recommendation_engine", lambda *args, **kwargs: {})

    class MockTasks:
        def add_task(self, *args, **kwargs): pass

    res = process_instagram_message_generic(payload, MockTasks())
    # Should not prompt for phone
    assert "telefon" not in res.reply_text.lower()
    assert "evet, ön görüşmede" in res.reply_text.lower()
    assert res.final_reply_source == "answer_first_enforced"
    # Make sure phone is NOT saved
    assert conversation.get("phone") is None

