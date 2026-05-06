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


def test_generic_business_identity_fit_prompt_hides_unavailable_services(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    captured = {}

    def fake_llm(system_prompt, user_text):
        captured["system_prompt"] = system_prompt
        if "unavailable_services" in system_prompt or "cilt bakımı" in system_prompt or "doktor muayenesi" in system_prompt:
            reply = "Merhaba, dövme bizim uzmanlık alanımız dışında. Lazer, cilt bakımı, emlak ve doktor muayenesi de bize uygun değil. Ön görüşme yapalım."
        else:
            reply = "Dövmeciler için sosyal medya, reklam ve portfolyo odaklı web çözümleri uygun olabilir. Önceliğiniz görünürlük mü, randevu talebi mi?"
        return {
            "intent": "direct_answer",
            "reply_text": reply,
            "extracted_entities": {},
            "requires_human": False,
        }

    conversation = {"sender_id": "generic-tattoo-fit-test", "state": "new", "memory_state": {}}
    config = {
        "business_name": "DOEL Digital",
        "service_catalog": [],
        "unavailable_services": ["saç kesimi", "lazer", "cilt bakımı", "dövme", "emlak", "doktor muayenesi"],
    }

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
    monkeypatch.setattr(gc, "call_llm_json", fake_llm)

    result = gc.process_instagram_message_generic(
        IncomingMessage(sender_id="generic-tattoo-fit-test", message_text="Ben dövmeciyim hizmetleriniz herkes için uygun mu?"),
        BackgroundTasks(),
    )

    reply = result.reply_text.lower()
    assert not reply.startswith("merhaba")
    assert "uzmanlık alanımız dışında" not in reply
    assert "lazer" not in reply
    assert "cilt bakımı" not in reply
    assert "emlak" not in reply
    assert "doktor" not in reply
    assert gc.reply_question_count(result.reply_text) <= 1
    assert gc.reply_sentence_count(result.reply_text) <= 3
    assert "unavailable_services" not in captured["system_prompt"]
    assert "cilt bakımı" not in captured["system_prompt"]


def test_generic_capability_question_keeps_unavailable_services_context(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    captured = {}

    def fake_llm(system_prompt, user_text):
        captured["system_prompt"] = system_prompt
        reply = "Hayır, dövme hizmeti vermiyoruz; dijital hizmetler sunuyoruz."
        return {
            "intent": "direct_answer",
            "reply_text": reply,
            "extracted_entities": {},
            "requires_human": False,
        }

    conversation = {"sender_id": "generic-capability-test", "state": "new", "memory_state": {}}
    config = {
        "business_name": "DOEL Digital",
        "service_catalog": [],
        "unavailable_services": ["dövme", "lazer"],
    }

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
    monkeypatch.setattr(gc, "call_llm_json", fake_llm)

    result = gc.process_instagram_message_generic(
        IncomingMessage(sender_id="generic-capability-test", message_text="Siz dövme yapıyor musunuz?"),
        BackgroundTasks(),
    )

    assert "dövme hizmeti vermiyoruz" in result.reply_text.lower()
    assert "dövme" in captured["system_prompt"]


def test_generic_flow_does_not_repeat_greeting_for_business_identity_fit(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    replies = []

    def fake_llm(system_prompt, user_text):
        if user_text == "Kolay gelsin":
            reply = "Teşekkürler, size nasıl yardımcı olabiliriz?"
        else:
            reply = "Dövmeciler için sosyal medya, reklam ve portfolyo odaklı web çözümleri uygun olabilir. Önceliğiniz görünürlük mü, randevu talebi mi?"
        replies.append(reply)
        return {
            "intent": "direct_answer",
            "reply_text": reply,
            "extracted_entities": {},
            "requires_human": False,
        }

    conversation = {"sender_id": "generic-flow-test", "state": "new", "memory_state": {}}
    store = []
    config = {
        "business_name": "DOEL Digital",
        "service_catalog": [],
        "unavailable_services": ["dövme", "lazer", "cilt bakımı", "emlak"],
    }

    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "save_message_log", lambda conn, sender, direction, text, meta: store.append({"direction": direction, "message_text": text}))
    monkeypatch.setattr(gc, "get_recent_message_history", lambda *args, **kwargs: store)
    monkeypatch.setattr(gc, "upsert_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "schedule_customer_automation_events", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "queue_crm_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_config", lambda: config)
    monkeypatch.setattr(gc, "call_llm_json", fake_llm)

    first = gc.process_instagram_message_generic(
        IncomingMessage(sender_id="generic-flow-test", message_text="Kolay gelsin"),
        BackgroundTasks(),
    )
    second = gc.process_instagram_message_generic(
        IncomingMessage(sender_id="generic-flow-test", message_text="Ben dövmeciyim hizmetleriniz herkes için uygun mu?"),
        BackgroundTasks(),
    )

    assert first.reply_text
    assert not second.reply_text.lower().startswith("merhaba")
    assert "dövmeciler" in second.reply_text.lower()
    assert "uzmanlık alanımız dışında" not in second.reply_text.lower()
    assert gc.reply_question_count(second.reply_text) <= 1
    assert gc.reply_sentence_count(second.reply_text) <= 3
