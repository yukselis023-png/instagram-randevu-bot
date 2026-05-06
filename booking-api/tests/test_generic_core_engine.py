import os

from fastapi import BackgroundTasks

import app.generic_core as gc
from app.main import IncomingMessage


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def run_generic_message(monkeypatch, message, llm_result, config, conversation=None):
    conversation = conversation or {"sender_id": "generic-test", "state": "new", "memory_state": {}}

    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "save_message_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_recent_message_history", lambda *args, **kwargs: [])
    monkeypatch.setattr(gc, "upsert_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "schedule_customer_automation_events", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "queue_crm_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_config", lambda: config)
    monkeypatch.setattr(gc, "call_llm_json", lambda *args, **kwargs: llm_result)

    result = gc.process_instagram_message_generic(
        IncomingMessage(sender_id=conversation.get("sender_id", "generic-test"), message_text=message),
        BackgroundTasks(),
    )
    return result, conversation


def test_generic_beauty_journey(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "service_question",
        "reply_text": "Biz hydrafacial ve lazer api yapıyoruz.",
        "extracted_entities": {},
        "requires_human": False,
    }

    result, _conversation = run_generic_message(
        monkeypatch,
        "Hizmetleriniz",
        llm_result,
        {"business_name": "Test Beauty", "business_type": "beauty", "service_catalog": []},
    )

    assert "hydrafacial" in result.reply_text.lower()
    assert "generic_intent:service_question" in result.decision_path[0]


def test_generic_doel_booking_crm(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Numaranızı aldım, sizi arayacağız.",
        "extracted_entities": {"lead_name": "Remzi", "phone": "05554443322"},
        "requires_human": False,
    }
    conversation = {"sender_id": "generic-booking-test", "state": "new", "memory_state": {}}

    result, conversation = run_generic_message(
        monkeypatch,
        "Adım Remzi 05554443322",
        llm_result,
        {"business_name": "DOEL", "service_catalog": []},
        conversation,
    )

    assert conversation.get("lead_name") == "Remzi"
    assert conversation.get("phone") == "05554443322"
    assert "Numaranızı aldım" in result.reply_text
