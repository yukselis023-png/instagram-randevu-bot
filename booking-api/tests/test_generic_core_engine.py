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
    assert conversation.get("phone") == "+905554443322"
    assert "Numaranızı aldım" in result.reply_text


def test_generic_business_identity_fit_prompt_hides_unavailable_services(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    captured = {}

    def fake_llm(system_prompt, user_text):
        captured["system_prompt"] = system_prompt
        if "unavailable_services" in system_prompt or "cilt bakımı" in system_prompt or "doktor muayenesi" in system_prompt:
            reply = "Merhaba, dövme bizim uzmanlık alanımız dışında. Lazer, cilt bakımı, emlak ve doktor muayenesi de bize uygun değil. Ön görüşme yapalım."
        else:
            reply = "Merhaba, dövmeciler için sosyal medya, reklam ve portfolyo odaklı web çözümleri uygun olabilir. Önceliğiniz görünürlük mü, randevu talebi mi?"
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

    reply = gc.sanitize_text(result.reply_text).lower()
    assert "dovme" in reply
    assert any(token in reply for token in ["sosyal medya", "reklam", "web", "portfolyo"])
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
        reply = "Hayır, dövme hizmeti vermiyoruz."
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

    reply = gc.sanitize_text(result.reply_text).lower()
    assert "dovme" in reply
    assert "yapmiyoruz" in reply or "vermiyoruz" in reply
    assert "dijital" in reply or "web sitesi" in reply
    assert "dövme" in captured["system_prompt"]


def test_generic_business_fit_does_not_handoff_when_llm_requires_human(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Bunu ekibimiz değerlendirsin.",
        "extracted_entities": {"requested_service": "Otomasyon"},
        "requires_human": True,
    }
    conversation = {
        "sender_id": "generic-fit-no-handoff-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "recommendation_engine", lambda *args, **kwargs: "Otomasyon bu durumda işe yarar. İsterseniz ön görüşme planlayabiliriz.")

    result, conversation = run_generic_message(
        monkeypatch,
        "Bu benim işime yarar mı?",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.handoff is False
    assert conversation.get("state") != "human_handoff"
    assert "action:handoff" not in result.decision_path
    assert "reply:business_fit" in result.decision_path


def test_generic_service_question_requires_human_does_not_enter_handoff(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "service_question",
        "reply_text": "Ön görüşmede ihtiyacınızı netleştiririz.",
        "extracted_entities": {},
        "requires_human": True,
    }
    conversation = {
        "sender_id": "generic-service-question-no-handoff-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Peki ön görüşmede ne konuşacağız?",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.handoff is False
    assert conversation.get("state") != "human_handoff"
    assert "action:handoff" not in result.decision_path


def test_generic_booking_request_requires_human_still_runs_fsm(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Ön görüşme için bilgilerinizi alayım.",
        "extracted_entities": {},
        "requires_human": True,
    }
    conversation = {
        "sender_id": "generic-booking-requires-human-fsm-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Tamam mantıklı, görüşelim",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.handoff is False
    assert conversation.get("state") == "collect_name"
    assert "action:handoff" not in result.decision_path
    assert "fsm:service_carryover_booking" in result.decision_path


def test_generic_llm_error_reply_never_leaks_to_customer(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"

    def fake_llm(*_args, **_kwargs):
        raise ValueError("LLM JSON Error: 429 Too Many Requests")

    monkeypatch.setattr(gc, "get_config", lambda: {"business_name": "DOEL Digital", "service_catalog": []})
    monkeypatch.setattr(gc, "call_llm_json", fake_llm)

    result = gc.invoke_generic_llm("Ne kadar?", {"state": "new", "memory_state": {}}, {}, [])

    assert result["intent"] == "fallback"
    assert "ERROR" not in result["reply_text"]
    assert "429" not in result["reply_text"]
    assert "Too Many Requests" not in result["reply_text"]


def test_generic_collect_phone_llm_error_keeps_fsm_prompt(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "fallback",
        "reply_text": "ERROR: LLM JSON Error: 429 Too Many Requests",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-collect-phone-error-test",
        "state": "collect_phone",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "055555",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.handoff is False
    assert conversation.get("state") == "collect_phone"
    assert "ERROR" not in result.reply_text
    assert "429" not in result.reply_text
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()


def test_generic_collect_name_detects_plain_name_and_asks_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Teşekkür ederim.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-name-detect-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Berkay Elbir",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("full_name") == "Berkay Elbir"
    assert conversation.get("state") == "collect_phone"
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()


def test_generic_collect_phone_rejects_short_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "human_handoff",
        "reply_text": "İsterseniz mesajınızı ekibe iletebilirim.",
        "extracted_entities": {"phone": "055555"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-invalid-phone-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "055555",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.handoff is False
    assert conversation.get("phone") is None
    assert conversation.get("state") == "collect_phone"
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()


def test_generic_collect_phone_does_not_overwrite_existing_name_from_llm(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Teşekkür ederim.",
        "extracted_entities": {"lead_name": "Berkay"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-name-overwrite-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "055555",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("full_name") == "Berkay Elbir"
    assert conversation.get("state") == "collect_phone"
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()


def test_generic_completed_booking_creates_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    created = {}

    def fake_create_appointment(_conn, conversation, username):
        created["conversation"] = dict(conversation)
        created["username"] = username
        return 123, 0

    llm_result = {
        "intent": "active_booking",
        "reply_text": "Uygun saati aldım.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-complete-booking-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", fake_create_appointment)

    result, conversation = run_generic_message(
        monkeypatch,
        "Yarın akşam 6",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert result.appointment_created is True
    assert result.appointment_id == 123
    assert conversation.get("state") == "completed"
    assert "kaydınız oluşturuldu" in result.reply_text.lower()
    assert "Ad Soyad: Berkay Elbir" in result.reply_text
    assert created["conversation"]["full_name"] == "Berkay Elbir"
    assert created["conversation"]["requested_time"] == "18:00"


def test_generic_after_confirmed_time_only_asks_confirmation_without_creating(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Saatinizi güncelliyorum.",
        "extracted_entities": {"requested_time": "13:00"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-post-confirm-time-only-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2026-05-07",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "13:00 olsun",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert create_calls == []
    assert result.appointment_created is False
    assert result.appointment_id == 77
    assert conversation.get("requested_time") == "18:00"
    assert conversation["memory_state"]["reschedule_requested_time"] == "13:00"
    assert "onay" in gc.sanitize_text(result.reply_text).lower()


def test_generic_after_confirmed_explicit_time_change_updates_without_creating(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    update_calls = []

    def fake_update(_conn, conversation, message_text, username=None):
        update_calls.append((message_text, username))
        conversation["requested_time"] = "13:00"
        conversation["appointment_status"] = "confirmed"
        conversation["state"] = "completed"
        return True, "07.05.2026 saat 13:00 için ön görüşme kaydınız güncellendi.", "appointment_rescheduled"

    llm_result = {
        "intent": "booking_request",
        "reply_text": "Saatinizi güncelliyorum.",
        "extracted_entities": {"requested_time": "13:00"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-post-confirm-explicit-update-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2026-05-07",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))
    monkeypatch.setattr(gc, "try_reschedule_confirmed_appointment", fake_update)

    result, conversation = run_generic_message(
        monkeypatch,
        "randevuyu 13:00 yap",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert create_calls == []
    assert len(update_calls) == 1
    assert result.appointment_created is False
    assert result.appointment_id == 77
    assert conversation.get("requested_time") == "13:00"
    assert "güncellendi" in result.reply_text


def test_generic_after_confirmed_new_datetime_does_not_create_second_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Yeni randevu açıyorum.",
        "extracted_entities": {"requested_date": "2026-05-07", "requested_time": "15:00"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-post-confirm-new-datetime-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2026-05-07",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "Yarın 15:00",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert create_calls == []
    assert result.appointment_created is False
    assert result.appointment_id == 77
    assert conversation.get("requested_time") == "18:00"
    assert conversation["memory_state"]["reschedule_requested_time"] == "15:00"
    assert "onay" in gc.sanitize_text(result.reply_text).lower()


def test_generic_booking_acceptance_is_not_saved_as_name(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Ön görüşme için bilgilerinizi alayım.",
        "extracted_entities": {"lead_name": "Olur Görüşelim"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-booking-acceptance-not-name-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Olur görüşelim",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("full_name") is None
    assert conversation.get("lead_name") is None
    assert conversation.get("state") == "collect_name"
    assert "ad soyad" in gc.sanitize_text(result.reply_text).lower()


def test_generic_name_after_booking_acceptance_is_saved(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "generic-name-after-acceptance-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    config = {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]}

    first, conversation = run_generic_message(
        monkeypatch,
        "Olur görüşelim",
        {
            "intent": "booking_request",
            "reply_text": "Ön görüşme için bilgilerinizi alayım.",
            "extracted_entities": {"lead_name": "Olur Görüşelim"},
            "requires_human": False,
        },
        config,
        conversation,
    )
    second, conversation = run_generic_message(
        monkeypatch,
        "Berkay Elbir",
        {
            "intent": "direct_answer",
            "reply_text": "Teşekkürler.",
            "extracted_entities": {},
            "requires_human": False,
        },
        config,
        conversation,
    )

    assert first.conversation_state == "collect_name"
    assert conversation.get("full_name") == "Berkay Elbir"
    assert second.conversation_state == "collect_phone"
    assert "telefon" in gc.sanitize_text(second.reply_text).lower()


def test_generic_preconsultation_explanation_does_not_start_booking(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Harika, ad soyadınızı alabilir miyim?",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-preconsultation-explain-test",
        "state": "new",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Peki ön görüşmede ne konuşacağız?",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("state") == "new"
    assert "ad soyad" not in reply
    assert "telefon" not in reply
    assert "ihtiyac" in reply or "hedef" in reply


def test_generic_price_question_with_automation_context_does_not_fallback_or_book(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "fallback",
        "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-price-context-test",
        "state": "new",
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "memory_state": {"requested_service": "Otomasyon & Yapay Zeka Çözümleri", "customer_goal": "DM ve randevu karışıklığı"},
    }
    config = {
        "business_name": "DOEL Digital",
        "service_catalog": [{
            "display": "Otomasyon & Yapay Zeka Çözümleri",
            "name": "Otomasyon & Yapay Zeka Çözümleri",
            "keywords": ["otomasyon", "dm otomasyonu", "randevu botu"],
            "price": "5.000 TL",
            "price_note": "ilk 3 ay indirimli aylık hizmet bedeli",
            "summary": "Müşteri mesajlarına 7/24 yanıt, randevuları otomatik ayarlama, teklif ve fatura otomasyonu içerir.",
        }],
    }

    result, conversation = run_generic_message(monkeypatch, "Ne kadar?", llm_result, config, conversation)

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("state") == "new"
    assert "5.000" in result.reply_text or "5000" in reply
    assert "fallback" not in reply
    assert "ad soyad" not in reply
    assert "telefon" not in reply


def test_generic_completed_followup_questions_use_safe_replies_and_keep_state(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "generic-completed-safe-followups-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2026-05-07",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    config = {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}], "human_contact_name": "Berkay"}
    create_calls = []
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    cases = [
        ("Ödeme nasıl yapılıyor?", {"intent": "fallback", "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.", "extracted_entities": {}, "requires_human": False}, "odeme"),
        ("Berkay bey mi arayacak?", {"intent": "direct_answer", "reply_text": "Evet, ben arayacağım.", "extracted_entities": {}, "requires_human": False}, "ekib"),
        ("Tamam teşekkürler", {"intent": "fallback", "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.", "extracted_entities": {}, "requires_human": False}, "rica"),
    ]
    replies = []
    for message, llm_result, expected in cases:
        result, conversation = run_generic_message(monkeypatch, message, llm_result, config, conversation)
        replies.append(gc.sanitize_text(result.reply_text).lower())
        assert result.appointment_created is False
        assert conversation.get("state") == "completed"
        assert result.conversation_state == "completed"
        assert expected in replies[-1]

    assert create_calls == []
    assert "ben arayacagim" not in replies[1]


def test_generic_after_confirmed_payment_question_does_not_reenter_slot_flow(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Ödeme banka havalesi veya online ödeme ile yapılabilir.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-post-confirm-payment-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2026-05-07",
        "requested_time": "18:00",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "Ödeme nasıl yapılıyor?",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert create_calls == []
    assert result.appointment_created is False
    assert conversation.get("state") == "completed"
    assert conversation.get("requested_time") == "18:00"
    assert "odeme" in gc.sanitize_text(result.reply_text).lower()


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
    second_reply = gc.sanitize_text(second.reply_text).lower()
    assert not second_reply.startswith("merhaba")
    assert "dovme" in second_reply
    assert any(token in second_reply for token in ["sosyal medya", "reklam", "web", "portfolyo"])
    assert "uzmanlik alanimiz disinda" not in second_reply
    assert gc.reply_question_count(second.reply_text) <= 1
    assert gc.reply_sentence_count(second.reply_text) <= 3
