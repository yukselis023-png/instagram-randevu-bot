import pytest
from fastapi import BackgroundTasks

import app.generic_core as gc
from app.main import IncomingMessage


@pytest.mark.parametrize("memory_key", ["requested_service", "selected_service", "service_interest"])
def test_known_requested_service_reads_service_aliases(memory_key):
    memory = {memory_key: "Otomasyon"}

    assert gc.known_requested_service({}, memory) == "Otomasyon"


def test_booking_opt_in_uses_previous_requested_service_and_asks_for_name(monkeypatch):
    store = {}

    def fake_get_or_create(_conn, sender_id, username=None):
        if sender_id not in store:
            store[sender_id] = {
                "sender_id": sender_id,
                "instagram_user_id": sender_id,
                "instagram_username": username,
                "state": "new",
                "memory_state": {},
                "history": [],
                "profile_slug": "doel",
            }
        return store[sender_id]

    class DummyConn:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    def fake_llm(_prompt, message_text):
        lowered = message_text.lower()
        if "otomasyon" in lowered:
            return {
                "intent": "service_question",
                "reply_text": "Otomasyon, DM ve randevu süreçlerini otomatikleştirmek için uygundur.",
                "extracted_entities": {"customer_goal": "DM yanıtlarını hızlandırmak"},
                "requires_human": False,
            }
        if "ne kadar" in lowered:
            return {
                "intent": "price_question",
                "reply_text": "Otomasyon için başlangıç fiyatı 5.000 TL'dir.",
                "extracted_entities": {},
                "requires_human": False,
            }
        if "görüşelim" in lowered or "goruselim" in lowered:
            return {
                "intent": "service_question",
                "reply_text": "Görüşme randevu almak için gerekli bilgileri toplamak isterim. İsminiz ve soyisminiz nedir?",
                "extracted_entities": {},
                "requires_human": False,
            }
        raise AssertionError(f"Unexpected message: {message_text}")

    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", fake_get_or_create)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "save_message_log", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_recent_message_history", lambda *args, **kwargs: [])
    monkeypatch.setattr(gc, "upsert_conversation", lambda _conn, conv: store.__setitem__(conv["sender_id"], conv))
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "schedule_customer_automation_events", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "queue_crm_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        gc,
        "get_config",
        lambda: {
            "business_name": "DOEL Digital",
            "fallback_reply": "Anlaşıldı.",
            "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}],
        },
    )
    monkeypatch.setattr(gc, "call_llm_json", fake_llm)

    sender_id = "generic-core-service-carryover"
    replies = []
    for message in ["Otomasyon işime yarar mı?", "Ne kadar?", "Olur görüşelim"]:
        result = gc.process_instagram_message_generic(
            IncomingMessage(sender_id=sender_id, message_text=message),
            BackgroundTasks(),
        )
        replies.append(result.reply_text)

    final_conversation = store[sender_id]
    final_memory = final_conversation["memory_state"]
    final_reply = replies[-1].lower()

    assert final_memory["requested_service"] == "Otomasyon"
    assert final_conversation["state"] == "collect_name"
    assert "hangi hizmet" not in final_reply
    assert "otomasyon" in final_reply
    assert "ön görüşme" in final_reply
    assert "ad" in final_reply and "soyad" in final_reply


def test_call_llm_json_wraps_plain_text_when_provider_ignores_json(monkeypatch):
    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {
                "choices": [
                    {
                        "message": {
                            "content": "Otomasyon, DM ve randevu süreçlerinizi hızlandırır. İsterseniz ön görüşme planlayabiliriz."
                        }
                    }
                ]
            }

    monkeypatch.setenv("LLM_API_KEY", "test-key")
    monkeypatch.setattr(gc.requests, "post", lambda *args, **kwargs: FakeResponse())

    result = gc.call_llm_json("JSON dön", "Otomasyon işime yarar mı?")

    assert result["intent"] == "direct_answer"
    assert "Otomasyon" in result["reply_text"]
    assert result["extracted_entities"] == {}
