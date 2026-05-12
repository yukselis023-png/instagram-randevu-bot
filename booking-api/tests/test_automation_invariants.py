import os
import random

from fastapi import BackgroundTasks

import app.generic_core as gc
from app.main import IncomingMessage


class DummyConn:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def cursor(self):
        return self

    def execute(self, *args, **kwargs):
        return None

    def fetchone(self):
        return None


def run_message(monkeypatch, message, llm_result, conversation, *, config=None, instagram_username="craft_user"):
    config = config or {
        "business_name": "DOEL Digital",
        "service_catalog": [
            {
                "display": "Otomasyon",
                "name": "Otomasyon",
                "keywords": ["otomasyon", "randevu botu", "dm otomasyonu"],
                "price": "5.000 TL",
                "price_note": "aylık hizmet bedeli",
            }
        ],
        "human_contact_name": "Berkay",
    }
    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "has_outbound_reply_for_trace", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "save_message_log", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "get_recent_message_history", lambda *args, **kwargs: [])
    monkeypatch.setattr(gc, "upsert_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "persist_customer_identity_to_crm", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "schedule_customer_automation_events", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "queue_crm_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_config", lambda: config)
    monkeypatch.setattr(gc, "call_llm_json", lambda *args, **kwargs: llm_result)

    return gc.process_instagram_message_generic(
        IncomingMessage(
            sender_id=conversation.get("sender_id", "automation-invariant-test"),
            instagram_username=instagram_username,
            message_text=message,
            raw_event={"platform": "igdm", "message_id": f"msg-{random.randint(1, 10_000_000)}"},
        ),
        BackgroundTasks(),
    )


def normalized(text):
    return gc.sanitize_text(text or "").lower()


def assert_no_forbidden_confirmation(result):
    reply = normalized(result.outbound_text or result.reply_text)
    forbidden = [
        "randevunuzu olusturdum",
        "randevunuz olusturuldu",
        "kaydiniz olusturuldu",
        "gorusmeniz ayarlandi",
        "on gorusmeniz ayarlandi",
        "sizi arayacagiz",
        "sizi arayacak",
        "islem tamamlandi",
        "saatinizi guncelledim",
    ]
    assert not any(phrase in reply for phrase in forbidden), reply


def test_invariant_false_confirmation_phrases_blocked_without_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    phrases = [
        "Randevunuzu oluşturdum, görüşmek üzere.",
        "Kaydınız oluşturuldu, sizi arayacağız.",
        "Ön görüşmeniz ayarlandı, Berkay Bey sizi arayacaktır.",
        "İşlem tamamlandı, sizi arayacağız.",
    ]
    for phrase in phrases:
        conversation = {
            "sender_id": f"false-confirm-{abs(hash(phrase))}",
            "state": "collect_datetime",
            "full_name": "Berkay Elbir",
            "lead_name": "Berkay Elbir",
            "phone": "+905539088638",
            "service": "Otomasyon",
            "requested_date": "2099-05-10",
            "memory_state": {"requested_service": "Otomasyon"},
        }
        result = run_message(
            monkeypatch,
            "Yarın olsun",
            {"intent": "direct_answer", "reply_text": phrase, "extracted_entities": {}, "requires_human": False},
            conversation,
        )
        assert result.appointment_created is False
        assert result.appointment_id is None
        assert_no_forbidden_confirmation(result)
        assert "guard:block_false_appointment_confirmation" in result.decision_path


def test_invariant_appointment_db_failure_returns_safe_non_confirmation(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "db-failure-invariant",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    def fail_create(*args, **kwargs):
        raise RuntimeError("simulated appointment insert failure")

    monkeypatch.setattr(gc, "create_appointment", fail_create)
    result = run_message(
        monkeypatch,
        "Yarın 13:00",
        {
            "intent": "active_booking",
            "reply_text": "Tamamdır, randevunuzu oluşturdum.",
            "extracted_entities": {"requested_date": "2099-05-10", "requested_time": "13:00"},
            "requires_human": False,
        },
        conversation,
    )
    assert result.appointment_created is False
    assert result.appointment_id is None
    assert result.handoff is False
    assert_no_forbidden_confirmation(result)


def test_invariant_active_state_only_expected_data_progresses(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    dirty_inputs = ["Kolay gelsin", "Merhaba", "Tamam", "Olur", "Görüşelim", "Evet", "Hayır"]
    for message in dirty_inputs:
        conversation = {
            "sender_id": f"dirty-name-{message}",
            "state": "collect_name",
            "service": "Otomasyon",
            "memory_state": {"requested_service": "Otomasyon"},
        }
        result = run_message(
            monkeypatch,
            message,
            {"intent": "direct_answer", "reply_text": "Tabii, buradayım.", "extracted_entities": {"lead_name": message}, "requires_human": False},
            conversation,
        )
        assert conversation.get("full_name") is None
        assert conversation.get("lead_name") is None
        assert result.appointment_created is False


def test_invariant_invalid_phone_never_persists_and_valid_phone_is_canonical(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "phone-invariant",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    invalid = run_message(
        monkeypatch,
        "055555",
        {"intent": "active_booking", "reply_text": "Telefon numaranızı alabilir miyim?", "extracted_entities": {}, "requires_human": False},
        conversation,
    )
    assert invalid.appointment_created is False
    assert conversation.get("phone") is None
    assert conversation.get("state") == "collect_phone"

    valid = run_message(
        monkeypatch,
        "05539088638",
        {"intent": "active_booking", "reply_text": "Teşekkürler.", "extracted_entities": {}, "requires_human": False},
        conversation,
    )
    assert conversation.get("phone") == "+905539088638"
    assert conversation.get("state") == "collect_datetime"
    assert valid.appointment_created is False


def test_invariant_active_direct_questions_are_never_collection_prompts(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    questions = [
        "Kiminle olacak?",
        "Ön görüşmede ne konuşacağız?",
        "Görüşme nereden olacak?",
        "Ödeme nasıl yapılıyor?",
        "Ne kadar?",
        "Anlamadım",
        "Bu ne demek?",
        "Sonradan yazsam olur mu?",
        "Berkay bey mi arayacak?",
    ]
    for question in questions:
        conversation = {
            "sender_id": f"direct-q-{abs(hash(question))}",
            "state": "collect_phone",
            "full_name": "Berkay Elbir",
            "lead_name": "Berkay Elbir",
            "service": "Otomasyon",
            "memory_state": {"requested_service": "Otomasyon"},
        }
        result = run_message(
            monkeypatch,
            question,
            {"intent": "direct_answer", "reply_text": "Bu konuda kısaca bilgi vereyim; ön görüşmede detayları netleştiriyoruz.", "extracted_entities": {}, "requires_human": False},
            conversation,
        )
        reply = normalized(result.reply_text)
        assert result.appointment_created is False
        assert conversation.get("state") == "collect_phone"
        assert conversation.get("phone") is None
        assert "telefon numaranizi" not in reply
        assert "ad soyad" not in reply
        assert "uygun gun" not in reply
        assert "yarim kalmis" not in reply
        assert "fsm:active_state_recovery_reply" not in result.decision_path


def test_invariant_completed_followups_do_not_create_new_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))
    followups = ["Ödeme nasıl oluyor?", "Görüşme nereden olacak?", "Berkay bey mi arayacak?", "Tamam teşekkürler", "Sonradan yazsam olur mu?"]
    conversation = {
        "sender_id": "completed-invariant",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2099-05-10",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    for followup in followups:
        result = run_message(
            monkeypatch,
            followup,
            {
                "intent": "active_booking",
                "reply_text": "Elbette, yardımcı olayım.",
                "extracted_entities": {
                    "lead_name": "Berkay Elbir",
                    "phone": "+905539088638",
                    "requested_service": "Otomasyon",
                    "requested_date": "2099-05-10",
                    "requested_time": "18:00",
                },
                "requires_human": False,
            },
            conversation,
        )
        assert result.appointment_created is False
        assert result.appointment_id is None or result.appointment_id == 77
        assert conversation.get("appointment_id") == 77
        assert conversation.get("state") == "completed"
    assert create_calls == []


def test_invariant_duplicate_short_circuits_before_llm_crm_and_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {"sender_id": "duplicate-invariant", "state": "new", "memory_state": {}}
    calls = {"llm": 0, "crm": 0, "appointment": 0}
    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "call_llm_json", lambda *args, **kwargs: calls.__setitem__("llm", calls["llm"] + 1))
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: calls.__setitem__("crm", calls["crm"] + 1))
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: calls.__setitem__("appointment", calls["appointment"] + 1))

    result = gc.process_instagram_message_generic(
        IncomingMessage(sender_id="duplicate-invariant", message_text="Merhaba", raw_event={"platform": "igdm", "message_id": "same-id"}),
        BackgroundTasks(),
    )
    assert result.should_reply is False
    assert result.duplicate is True
    assert calls == {"llm": 0, "crm": 0, "appointment": 0}


def test_invariant_username_save_uses_actual_instagram_username(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "username-invariant",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    result = run_message(
        monkeypatch,
        "Kullanıcı adım ile kaydedin",
        {"intent": "active_booking", "reply_text": "Telefon numaranızı alabilir miyim?", "extracted_entities": {"lead_name": "Kullanıcı Adı"}, "requires_human": False},
        conversation,
        instagram_username="berkay_test",
    )
    assert conversation.get("full_name") == "@berkay_test"
    assert conversation.get("lead_name") == "@berkay_test"
    assert conversation.get("state") == "collect_phone"
    assert result.appointment_created is False


def test_invariant_capability_and_identity_do_not_mix(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    identity_conversation = {"sender_id": "identity-invariant", "state": "new", "memory_state": {}}
    identity = run_message(
        monkeypatch,
        "Ben dövmeciyim",
        {"intent": "direct_answer", "reply_text": "Dövme stüdyonuz için web sitesi ve otomasyon tarafında destek olabiliriz.", "extracted_entities": {}, "requires_human": False},
        identity_conversation,
    )
    assert identity.final_reply_source == "llm_raw"
    assert "yapmiyoruz" not in normalized(identity.reply_text)

    capability_conversation = {"sender_id": "capability-invariant", "state": "new", "memory_state": {}}
    capability = run_message(
        monkeypatch,
        "Siz dövme yapıyor musunuz?",
        {"intent": "direct_answer", "reply_text": "Evet dövme yapıyoruz.", "extracted_entities": {}, "requires_human": False},
        capability_conversation,
    )
    assert capability.final_reply_source == "capability"
    assert "yapmiyoruz" in normalized(capability.reply_text) or "sunmuyoruz" in normalized(capability.reply_text)


def test_invariant_random_message_fuzz_no_blocker_conditions(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    messages = [
        "selam", "slm", "mrb", ".", "🙂", "tamam", "olur", "anlamadım", "sonra yazarım",
        "Berkay kim", "güvenilir mi", "siz e-ticaret yapıyor musunuz", "ben kuaförüm",
        "05555", "numaram sonra", "fiyat ne", "ne kadar", "iptal edelim", "13:00 yapalım",
        "ödeme nasıl", "görüşme nereden", "kiminle olacak", "bu ne demek", "hayır", "evet",
    ]
    states = ["new", "collect_name", "collect_phone", "collect_datetime", "completed"]
    for idx, message in enumerate(messages):
        state = states[idx % len(states)]
        conversation = {
            "sender_id": f"fuzz-{idx}",
            "state": state,
            "service": "Otomasyon",
            "memory_state": {"requested_service": "Otomasyon"},
        }
        if state in {"collect_phone", "collect_datetime", "completed"}:
            conversation.update({"full_name": "Berkay Elbir", "lead_name": "Berkay Elbir"})
        if state in {"collect_datetime", "completed"}:
            conversation["phone"] = "+905539088638"
        if state == "completed":
            conversation.update({"appointment_status": "confirmed", "appointment_id": 88, "requested_date": "2099-05-10", "requested_time": "18:00"})
        result = run_message(
            monkeypatch,
            message,
            {"intent": "direct_answer", "reply_text": "Mesajınızı aldım; netleştirip yardımcı olayım.", "extracted_entities": {"lead_name": message, "phone": message}, "requires_human": False},
            conversation,
        )
        assert "error" not in normalized(result.reply_text)
        if not result.appointment_created:
            assert_no_forbidden_confirmation(result)
        if state == "collect_name" and message.lower() in {"selam", "slm", "mrb", "tamam", "olur", "hayır", "evet"}:
            assert conversation.get("full_name") is None
        if message == "05555":
            assert conversation.get("phone") != "+905555"
