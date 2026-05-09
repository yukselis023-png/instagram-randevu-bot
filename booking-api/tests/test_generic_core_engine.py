import os

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


def run_generic_message(monkeypatch, message, llm_result, config, conversation=None, instagram_username=None):
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
        IncomingMessage(sender_id=conversation.get("sender_id", "generic-test"), instagram_username=instagram_username, message_text=message),
        BackgroundTasks(),
    )
    return result, conversation


def test_generic_inbound_save_uses_durable_dedupe_payload(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {"sender_id": "67000808415", "state": "new", "memory_state": {}}
    saved = []

    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: False)
    monkeypatch.setattr(gc, "get_recent_message_history", lambda *args, **kwargs: [])
    monkeypatch.setattr(gc, "upsert_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "schedule_customer_automation_events", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "queue_crm_sync", lambda *args, **kwargs: None)
    monkeypatch.setattr(gc, "get_config", lambda: {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}]})
    monkeypatch.setattr(gc, "call_llm_json", lambda *args, **kwargs: {"intent": "direct_answer", "reply_text": "Merhaba.", "extracted_entities": {}, "requires_human": False})
    monkeypatch.setattr(gc, "save_message_log", lambda *args: saved.append(args) or True)

    result = gc.process_instagram_message_generic(
        IncomingMessage(
            sender_id="67000808415",
            message_text="Kolay gelsin",
            raw_event={"platform": "igdm", "message_id": "32801733821997095931189647533670400", "trace_id": "igdm:67000808415:32801733821997095931189647533670400"},
        ),
        BackgroundTasks(),
    )

    inbound_payload = saved[0][4]
    outbound_payload = saved[-1][4]
    assert result.should_reply is True
    assert inbound_payload["platform"] == "igdm"
    assert inbound_payload["message_id"] == "32801733821997095931189647533670400"
    assert inbound_payload["sender_id"] == "67000808415"
    assert inbound_payload["dedupe_key"] == "igdm:67000808415:32801733821997095931189647533670400"
    assert inbound_payload["trace_id"] == "igdm:67000808415:32801733821997095931189647533670400"
    assert outbound_payload["trace_id"] == inbound_payload["trace_id"]
    assert outbound_payload["dedupe_key"] == inbound_payload["dedupe_key"]


def test_generic_duplicate_inbound_short_circuits_before_llm_and_crm(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {"sender_id": "67000808415", "state": "new", "memory_state": {}}
    calls = {"llm": 0, "save": 0, "crm": 0}

    monkeypatch.setattr(gc, "get_conn", lambda: DummyConn())
    monkeypatch.setattr(gc, "get_or_create_conversation", lambda *args, **kwargs: conversation)
    monkeypatch.setattr(gc, "try_acquire_inbound_processing_lock", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "has_processed_inbound_message", lambda *args, **kwargs: True)
    monkeypatch.setattr(gc, "save_message_log", lambda *args, **kwargs: calls.__setitem__("save", calls["save"] + 1))
    monkeypatch.setattr(gc, "upsert_customer_from_conversation", lambda *args, **kwargs: calls.__setitem__("crm", calls["crm"] + 1))

    def fail_llm(*args, **kwargs):
        calls["llm"] += 1
        raise AssertionError("LLM must not be called for duplicate inbound")

    monkeypatch.setattr(gc, "call_llm_json", fail_llm)

    result = gc.process_instagram_message_generic(
        IncomingMessage(
            sender_id="67000808415",
            message_text="Kolay gelsin",
            raw_event={"platform": "igdm", "message_id": "32801733821997095931189647533670400"},
        ),
        BackgroundTasks(),
    )

    assert result.should_reply is False
    assert result.duplicate is True
    assert result.decision_path == ["duplicate_ignored"]
    assert calls == {"llm": 0, "save": 0, "crm": 0}


def test_generic_service_overview_is_natural_not_catalog_dump(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    config = {
        "business_name": "DOEL Digital",
        "service_catalog": [
            {"display": "Web Tasarim", "summary": "Google uyumlu uzun açıklama"},
            {"display": "Otomasyon & Yapay Zeka Cozumleri", "summary": "Musteri mesajlarına yanıt"},
            {"display": "Performans Pazarlama", "summary": "Meta reklam yönetimi"},
            {"display": "Sosyal Medya", "summary": "İçerik yönetimi"},
        ],
    }

    result, _conversation = run_generic_message(
        monkeypatch,
        "Tam olarak ne yapıyorsunuz?",
        {"intent": "service_question", "reply_text": "Web Tasarim: uzun; Otomasyon: uzun", "extracted_entities": {}, "requires_human": False},
        config,
    )

    assert "Kısaca" in result.reply_text
    assert "web sitesi" in result.reply_text
    assert "reklam yönetimi" in result.reply_text
    assert "mesaj/randevu otomasyonu" in result.reply_text
    assert "Web Tasarim:" not in result.reply_text
    assert "Cozumleri" not in result.reply_text
    assert ";" not in result.reply_text
    assert gc.reply_question_count(result.reply_text) <= 1
    assert gc.reply_sentence_count(result.reply_text) <= 3
    assert "reply:service_overview_config:catalog_dump" in result.decision_path
    assert result.final_reply_source == "config_formatter"
    assert result.outbound_text == result.reply_text


def test_service_overview_uses_valid_llm_raw_reply(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Web tasarım, reklam yönetimi, randevu otomasyonu ve sosyal medya yönetimi konularında profesyonel destek veriyoruz. Önceliğiniz yeni randevular almak mı?"
    result, _conversation = run_generic_message(
        monkeypatch,
        "Hizmetleriniz neler?",
        {"intent": "service_question", "reply_text": llm_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Otomasyon & Yapay Zeka Cozumleri"}]},
    )

    assert result.reply_text == llm_reply
    assert result.outbound_text == llm_reply
    assert result.llm_raw_reply_text == llm_reply
    assert result.final_reply_source == "llm_raw"
    assert "reply:service_overview_llm_raw" in result.decision_path
    assert "Kısaca işletmelerin" not in result.reply_text


def test_direct_answer_service_overview_uses_valid_llm_raw_reply(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Özetle işletmelere yeni müşteriler kazandırıyor ve mesaj/randevu sürecini dijitalleştiriyoruz. Sizin tarafta öncelik yeni müşteri kazanmak mı?"
    result, _conversation = run_generic_message(
        monkeypatch,
        "Tam olarak ne yapıyorsunuz?",
        {"intent": "direct_answer", "reply_text": llm_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Performans Pazarlama"}]},
    )

    assert result.reply_text == llm_reply
    assert result.outbound_text == llm_reply
    assert result.llm_raw_reply_text == llm_reply
    assert result.final_reply_source == "llm_raw"
    assert "reply:service_overview_llm_raw" in result.decision_path


def test_service_overview_falls_back_when_llm_empty(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    result, _conversation = run_generic_message(
        monkeypatch,
        "Hizmetleriniz neler?",
        {"intent": "service_question", "reply_text": "", "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Otomasyon & Yapay Zeka Cozumleri"}]},
    )

    assert result.final_reply_source == "config_formatter"
    assert "reply:service_overview_config:empty" in result.decision_path
    assert "Kısaca" in result.reply_text
    assert result.outbound_text == result.reply_text


def test_service_overview_falls_back_when_llm_returns_fallback(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    result, _conversation = run_generic_message(
        monkeypatch,
        "Tam olarak ne yapıyorsunuz?",
        {
            "intent": "fallback",
            "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
            "extracted_entities": {},
            "requires_human": False,
        },
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Performans Pazarlama"}]},
    )

    assert result.final_reply_source == "config_formatter"
    assert "reply:service_overview_config:fallback_reply" in result.decision_path
    assert "Kısaca" in result.reply_text
    assert "netleştiremedim" not in result.reply_text
    assert result.outbound_text == result.reply_text


def test_identity_message_uses_valid_llm_raw_reply(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Harika! Dövme sanatçıları için özellikle randevu otomasyonu ve portfolyonuzu sergileyecek bir web sitesi veya reklam yönetimi çok etkili oluyor. Sizin için şu an öncelik yeni müşterilere ulaşmak mı yoksa randevuları düzene sokmak mı?"
    result, _conversation = run_generic_message(
        monkeypatch,
        "Ben dövmeciyim, sitenizi gördüm merak edip yazdım",
        {"intent": "direct_answer", "reply_text": llm_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Otomasyon & Yapay Zeka Cozumleri"}]},
    )

    assert result.reply_text == llm_reply
    assert result.outbound_text == llm_reply
    assert result.llm_raw_reply_text == llm_reply
    assert result.final_reply_source == "llm_raw"
    assert "reply:user_business_identity_llm_raw" in result.decision_path
    assert "Kısaca işletmelerin" not in result.reply_text


def test_identity_message_falls_back_when_llm_misreads_capability(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    bad_reply = "Maalesef dövme hizmeti vermiyoruz, uzmanlık alanımız dışında."
    result, _conversation = run_generic_message(
        monkeypatch,
        "Dövmeciyim ben, sitenizi gördüm",
        {"intent": "direct_answer", "reply_text": bad_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Web Tasarim"}, {"display": "Otomasyon & Yapay Zeka Cozumleri"}]},
    )

    reply = result.reply_text.lower()
    assert result.final_reply_source == "config_formatter"
    assert "reply:user_business_identity_config:identity_misread_as_capability" in result.decision_path
    assert "hizmeti vermiyoruz" not in reply
    assert "uzmanlık alanımız dışında" not in reply
    assert "Kısaca" in result.reply_text


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
    reply = gc.sanitize_text(result.reply_text).lower()
    assert result.appointment_created is False
    assert result.appointment_id is None
    assert "sizi arayacagiz" not in reply
    assert "hangi hizmet" in reply
    assert "guard:block_false_appointment_confirmation" in result.decision_path


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
        "service_catalog": [
            {"name": "web", "display": "Web sitesi", "summary": "işletmenin dijital vitrini"},
            {"name": "ads", "display": "Reklam yönetimi", "summary": "yeni müşteri kazanımı"},
        ],
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
    assert any(token in reply for token in ["reklam", "web", "isletmenin dijital vitrini", "yeni musteri"])
    assert not reply.startswith("merhaba")
    assert "bu hizmeti vermiyoruz" not in reply
    assert "hizmetleri disinda" not in reply
    assert "uzmanlık alanımız dışında" not in reply
    assert "lazer" not in reply
    assert "cilt bakımı" not in reply
    assert "emlak" not in reply
    assert "doktor" not in reply
    assert "reply:user_business_identity_llm_raw" in result.decision_path
    assert result.final_reply_source == "llm_raw"
    assert result.llm_raw_reply_text == reply or "dövmeciler" in result.llm_raw_reply_text.lower()
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
    assert result.final_reply_source == "capability"
    assert result.outbound_text == result.reply_text
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


def test_generic_collect_name_username_save_uses_instagram_username_and_asks_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, not aldım. Size ulaşabilmemiz için telefon numaranızı da paylaşabilir misiniz?",
        "extracted_entities": {"lead_name": "Kullanıcı Adı", "phone": None, "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-username-name-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kullanıcı adım ile kaydedin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
        instagram_username="karanlikyukselis",
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("full_name") == "@karanlikyukselis"
    assert conversation.get("lead_name") == "@karanlikyukselis"
    assert conversation.get("memory_state", {}).get("name_source") == "instagram_username"
    assert conversation.get("state") == "collect_phone"
    assert conversation.get("phone") is None
    assert result.appointment_created is False
    assert "telefon" in reply
    assert "onceki gorusme" not in reply
    assert "Kullanıcı Adı" not in str(conversation.get("full_name"))
    assert "fsm:active_state_recovery_reply" not in result.decision_path


def test_generic_collect_name_instagram_name_save_uses_instagram_username(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, kullanıcı adınızla kaydediyorum. Telefon numaranızı paylaşabilir misiniz?",
        "extracted_entities": {"lead_name": "Instagram Adı", "phone": None, "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-instagram-name-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Instagram adımla kaydedin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
        instagram_username="karanlikyukselis",
    )

    assert conversation.get("full_name") == "@karanlikyukselis"
    assert conversation.get("state") == "collect_phone"
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()
    assert "fsm:active_state_recovery_reply" not in result.decision_path


def test_generic_collect_phone_username_save_preserves_clean_name_and_asks_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, not aldım. Size ulaşabilmemiz için telefon numaranızı da paylaşabilir misiniz?",
        "extracted_entities": {"lead_name": "Kullanıcı Adı", "phone": None, "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-username-phone-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kullanıcı adım ile kaydedin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
        instagram_username="karanlikyukselis",
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("full_name") == "Berkay Elbir"
    assert conversation.get("lead_name") == "Berkay Elbir"
    assert conversation.get("phone") is None
    assert conversation.get("state") == "collect_phone"
    assert result.appointment_created is False
    assert "telefon" in reply
    assert "onceki gorusme" not in reply
    assert "noted:name_instagram_username" in result.decision_path
    assert "fsm:active_state_recovery_reply" not in result.decision_path


def test_generic_collect_phone_instagram_name_save_preserves_clean_name(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, not aldım. Telefon numaranızı paylaşabilir misiniz?",
        "extracted_entities": {"lead_name": "Instagram Adı", "phone": None, "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-instagram-phone-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Instagram adımla kaydedin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
        instagram_username="karanlikyukselis",
    )

    assert conversation.get("full_name") == "Berkay Elbir"
    assert conversation.get("phone") is None
    assert conversation.get("state") == "collect_phone"
    assert "telefon" in gc.sanitize_text(result.reply_text).lower()
    assert "fsm:active_state_recovery_reply" not in result.decision_path


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
        "requested_date": "2099-05-07",
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
        "requested_date": "2099-05-07",
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
        "extracted_entities": {"requested_date": "2099-05-07", "requested_time": "15:00"},
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
        "requested_date": "2099-05-07",
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
        "requested_date": "2099-05-07",
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


def test_generic_collect_phone_irrelevant_message_does_not_prompt_or_write_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Kolay gelsin, buradayım. Nasıl yardımcı olabilirim?",
        "extracted_entities": {"lead_name": "Berkay"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-stale-phone-irrelevant-test",
        "state": "collect_phone",
        "full_name": "Eski Müşteri",
        "lead_name": "Eski Müşteri",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kolay gelsin Berkay bey",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("state") == "collect_phone"
    assert conversation.get("phone") is None
    assert conversation.get("full_name") == "Eski Müşteri"
    assert result.appointment_created is False
    assert "telefon" not in reply
    assert "kolay gelsin" in reply or "yardimci" in reply


def test_dirty_collect_phone_greeting_recovers_without_phone_prompt_or_name_write(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Harika, otomasyon için ön görüşme oluşturalım. Telefon numaranızı eksiksiz alabilir miyim?",
        "extracted_entities": {"lead_name": "Kolay Gelsin", "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-dirty-collect-phone-greeting-test",
        "state": "collect_phone",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon", "open_loop": "collect_phone"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kolay gelsin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert conversation.get("full_name") is None
    assert conversation.get("lead_name") is None
    assert conversation.get("phone") is None
    assert result.appointment_created is False
    assert "telefon" not in reply
    assert "yarim kal" in reply or "yardimci" in reply
    assert "fsm:active_state_recovery_reply" in result.decision_path


def test_collect_name_greeting_is_not_saved_as_full_name(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Ad soyadınızı alabilir miyim?",
        "extracted_entities": {"lead_name": "Kolay Gelsin"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-collect-name-greeting-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon", "open_loop": "collect_name"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kolay gelsin",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("full_name") is None
    assert conversation.get("lead_name") is None
    assert result.appointment_created is False
    assert "telefon" not in gc.sanitize_text(result.reply_text).lower()


def test_collect_name_real_full_name_is_saved(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Telefon numaranızı alabilir miyim?",
        "extracted_entities": {"lead_name": "Berkay Elbir"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-collect-name-real-name-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon", "open_loop": "collect_name"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Berkay Elbir",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("full_name") == "Berkay Elbir"
    assert conversation.get("lead_name") == "Berkay Elbir"
    assert conversation.get("state") == "collect_phone"


def test_generic_collect_datetime_irrelevant_message_does_not_prompt_or_create(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "direct_answer",
        "reply_text": "Kolay gelsin, buradayım. Nasıl yardımcı olabilirim?",
        "extracted_entities": {"requested_date": "2099-05-07", "requested_time": "13:00"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-stale-datetime-irrelevant-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "Kolay gelsin Berkay bey",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert create_calls == []
    assert result.appointment_created is False
    assert conversation.get("state") == "collect_datetime"
    assert conversation.get("requested_date") is None
    assert conversation.get("requested_time") is None
    assert "saat" not in reply
    assert "kolay gelsin" in reply or "yardimci" in reply


def test_generic_collect_phone_valid_phone_progresses(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "fallback",
        "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-phone-relevant-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "05539088638",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert conversation.get("phone") == "+905539088638"
    assert conversation.get("state") == "collect_datetime"
    assert "saat" in gc.sanitize_text(result.reply_text).lower()


def test_generic_collect_phone_direct_contact_question_preserves_llm_answer(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Ön görüşmeyi ekip arkadaşımız Berkay ile yapacaksınız. Merak ettiğiniz tüm detayları yanıtlayıp size uygun sistemi birlikte planlıyoruz; sizin için uygun bir zaman var mı?"
    conversation = {
        "sender_id": "generic-active-contact-clarification-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Kiminle olacak ön görüşme anlamadım hiçbir şey?",
        {"intent": "direct_answer", "reply_text": llm_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}], "human_contact_name": "Berkay"},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert result.reply_text == llm_reply
    assert result.final_reply_source == "llm_raw"
    assert result.appointment_created is False
    assert conversation.get("state") == "collect_phone"
    assert conversation.get("phone") is None
    assert "telefon" not in reply
    assert "fsm:active_booking_prompt" not in result.decision_path
    assert "fsm:active_state_recovery_reply" not in result.decision_path
    assert "fsm:active_direct_clarification" in result.decision_path


def test_generic_collect_name_preconsultation_question_does_not_ask_name(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    conversation = {
        "sender_id": "generic-active-preconsult-clarification-test",
        "state": "collect_name",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Ön görüşmede ne konuşacağız?",
        {"intent": "booking_request", "reply_text": "Ad soyadınızı alabilir miyim?", "extracted_entities": {"lead_name": "Ön Görüşme"}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert result.appointment_created is False
    assert conversation.get("state") == "collect_name"
    assert conversation.get("full_name") is None
    assert "ad soyad" not in reply
    assert "telefon" not in reply
    assert "ihtiyac" in reply or "hedef" in reply
    assert "fsm:active_booking_prompt" not in result.decision_path


def test_generic_collect_datetime_location_question_does_not_ask_time(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Görüşme online olarak yapılacak; ekibimiz uygun bağlantıyı paylaşır."
    conversation = {
        "sender_id": "generic-active-location-clarification-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Görüşme nereden olacak?",
        {"intent": "direct_answer", "reply_text": llm_reply, "extracted_entities": {"requested_time": "13:00"}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert result.reply_text == llm_reply
    assert result.appointment_created is False
    assert conversation.get("state") == "collect_datetime"
    assert conversation.get("requested_time") is None
    assert "hangi saat" not in reply
    assert "uygun gun" not in reply
    assert "online" in reply
    assert "fsm:active_booking_prompt" not in result.decision_path


def test_generic_collect_phone_payment_question_does_not_ask_phone(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_reply = "Ödeme detayları ön görüşmede netleşir; uygun olursa havale/EFT veya online ödeme seçenekleri paylaşılır."
    conversation = {
        "sender_id": "generic-active-payment-clarification-test",
        "state": "collect_phone",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }

    result, conversation = run_generic_message(
        monkeypatch,
        "Ödeme nasıl yapılıyor?",
        {"intent": "direct_answer", "reply_text": llm_reply, "extracted_entities": {}, "requires_human": False},
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert result.reply_text == llm_reply
    assert result.appointment_created is False
    assert conversation.get("state") == "collect_phone"
    assert conversation.get("phone") is None
    assert "telefon" not in reply
    assert "odeme" in reply
    assert "fsm:active_booking_prompt" not in result.decision_path


def test_generic_collect_datetime_valid_datetime_progresses_to_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []

    def fake_create(_conn, conversation, username):
        create_calls.append(dict(conversation))
        return 123, 0

    llm_result = {
        "intent": "fallback",
        "reply_text": "Şu an yanıtı netleştiremedim; mesajınızı aldım, birazdan devam edelim.",
        "extracted_entities": {},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-datetime-relevant-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", fake_create)

    result, conversation = run_generic_message(
        monkeypatch,
        "yarın 13:00",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert len(create_calls) == 1
    assert result.appointment_created is True
    assert result.appointment_id == 123
    assert conversation.get("state") == "completed"
    assert conversation.get("requested_date")
    assert conversation.get("requested_time") == "13:00"



def test_generic_collect_datetime_blocks_llm_confirmation_when_time_missing(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, yarın saat 12:00 için randevunuzu oluşturdum. Belirttiğiniz numara üzerinden Berkay Bey sizi arayacaktır.",
        "extracted_entities": {"requested_date": "2099-05-07", "requested_time": None, "requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-false-confirmation-missing-time-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "yarın olsun",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert create_calls == []
    assert result.appointment_created is False
    assert result.appointment_id is None
    assert conversation.get("state") == "collect_datetime"
    assert conversation.get("requested_date")
    assert conversation.get("requested_time") is None
    assert result.final_reply_source == "fsm_guard"
    assert "guard:block_false_appointment_confirmation" in result.decision_path
    assert "olusturdum" not in reply
    assert "olusturuldu" not in reply
    assert "arayacaktir" not in reply
    assert "saat" in reply



def test_generic_collect_datetime_noon_phrase_creates_real_appointment(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []

    def fake_create(_conn, conversation, username):
        create_calls.append(dict(conversation))
        return 124, 0

    llm_result = {
        "intent": "active_booking",
        "reply_text": "Tamamdır, yarın saat 12:00 için randevunuzu oluşturdum.",
        "extracted_entities": {"requested_service": "Otomasyon"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-noon-booking-test",
        "state": "collect_datetime",
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "memory_state": {"requested_service": "Otomasyon"},
    }
    monkeypatch.setattr(gc, "create_appointment", fake_create)

    result, conversation = run_generic_message(
        monkeypatch,
        "Yarın öğlen olsun",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    assert len(create_calls) == 1
    assert result.appointment_created is True
    assert result.appointment_id == 124
    assert conversation.get("state") == "completed"
    assert conversation.get("requested_date")
    assert conversation.get("requested_time") == "12:00"
    assert create_calls[0]["requested_time"] == "12:00"
    assert "guard:block_false_appointment_confirmation" not in result.decision_path



def test_generic_false_confirmation_guard_catches_confirmation_variants(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    config = {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]}
    variants = [
        "Randevunuzu oluşturdum.",
        "Kaydınız oluşturuldu.",
        "Ön görüşme kaydınız tamamlandı.",
        "Yarın saat 12:00 için randevunuz hazır.",
    ]
    for idx, reply_text in enumerate(variants):
        conversation = {
            "sender_id": f"generic-false-confirmation-variant-{idx}",
            "state": "collect_datetime",
            "full_name": "Berkay Elbir",
            "lead_name": "Berkay Elbir",
            "phone": "+905539088638",
            "service": "Otomasyon",
            "memory_state": {"requested_service": "Otomasyon"},
        }
        result, _conversation = run_generic_message(
            monkeypatch,
            "yarın olsun",
            {"intent": "active_booking", "reply_text": reply_text, "extracted_entities": {"requested_service": "Otomasyon"}, "requires_human": False},
            config,
            conversation,
        )
        normalized_reply = gc.sanitize_text(result.reply_text).lower()
        assert result.appointment_created is False
        assert result.appointment_id is None
        assert result.final_reply_source == "fsm_guard"
        assert "guard:block_false_appointment_confirmation" in result.decision_path
        assert "olusturdum" not in normalized_reply
        assert "olusturuldu" not in normalized_reply
        assert "tamamlandi" not in normalized_reply
        assert "hazir" not in normalized_reply


def test_generic_collect_name_booking_ack_keeps_state_without_name(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    llm_result = {
        "intent": "booking_request",
        "reply_text": "Ön görüşme için bilgilerinizi alayım.",
        "extracted_entities": {"lead_name": "Olur Görüşelim"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-collect-name-ack-test",
        "state": "collect_name",
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

    assert conversation.get("state") == "collect_name"
    assert conversation.get("full_name") is None
    assert "ad soyad" in gc.sanitize_text(result.reply_text).lower()


def test_generic_completed_pending_reschedule_location_question_does_not_repeat_confirmation(monkeypatch):
    os.environ["CHATBOT_ENGINE"] = "generic"
    create_calls = []
    llm_result = {
        "intent": "service_question",
        "reply_text": "Görüşme online yapılacak.",
        "extracted_entities": {"requested_date": "2099-05-07", "requested_time": "13:00"},
        "requires_human": False,
    }
    conversation = {
        "sender_id": "generic-pending-reschedule-location-test",
        "state": "completed",
        "appointment_status": "confirmed",
        "appointment_id": 77,
        "full_name": "Berkay Elbir",
        "lead_name": "Berkay Elbir",
        "phone": "+905539088638",
        "service": "Otomasyon",
        "requested_date": "2099-05-07",
        "requested_time": "18:00",
        "memory_state": {
            "requested_service": "Otomasyon",
            "open_loop": "generic_reschedule_confirmation_pending",
            "reschedule_requested_date": "2099-05-07",
            "reschedule_requested_time": "13:00",
        },
    }
    monkeypatch.setattr(gc, "create_appointment", lambda *args, **kwargs: create_calls.append(args) or (999, 0))

    result, conversation = run_generic_message(
        monkeypatch,
        "Görüşme nereden olacak?",
        llm_result,
        {"business_name": "DOEL Digital", "service_catalog": [{"display": "Otomasyon", "name": "Otomasyon"}]},
        conversation,
    )

    reply = gc.sanitize_text(result.reply_text).lower()
    assert create_calls == []
    assert result.appointment_created is False
    assert result.appointment_id is None
    assert conversation.get("state") == "completed"
    assert "onay" not in reply
    assert "online" in reply or "video" in reply


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
        "requested_date": "2099-05-07",
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
        "service_catalog": [
            {"name": "web", "display": "Web sitesi", "summary": "işletme tanıtımı"},
            {"name": "ads", "display": "Reklam yönetimi", "summary": "yeni müşteri kazanımı"},
        ],
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
    assert any(token in second_reply for token in ["reklam", "web", "isletme tanitimi", "yeni musteri"])
    assert "dovme hizmeti vermiyoruz" not in second_reply
    assert "dovme yapmiyoruz" not in second_reply
    assert "uzmanlik alanimiz disinda" not in second_reply
    assert "hizmetleri disinda" not in second_reply
    assert gc.reply_question_count(second.reply_text) <= 1
    assert gc.reply_sentence_count(second.reply_text) <= 3

def test_user_business_identity_handling_with_llm():
    from app.main import is_user_business_identity_message, is_company_capability_question
    from app.generic_core import build_generic_business_context
    import json
    
    # Message A
    msg_a = "Ben dövmeciyim, sitenizi gördüm merak edip yazdım"
    assert is_user_business_identity_message(msg_a) is True
    assert is_company_capability_question(msg_a) is False
    
    ctx_a_str = build_generic_business_context(msg_a, {"business_name": "DOEL", "unavailable_services": {"dovmecilik": "dövme"}})
    ctx_a = json.loads(ctx_a_str)
    assert "unavailable_services" not in ctx_a, "should be popped because it's not a capability question"
    assert "instruction_override" in ctx_a, "should have the explicit instruction override"
    assert "kendi işletme sektörünü" in ctx_a["instruction_override"]
    assert "service_catalog" in ctx_a["instruction_override"]

    # Message A variant with suffix
    msg_a2 = "Dövmeciyim ben, sitenizi gördüm"
    assert is_user_business_identity_message(msg_a2) is True
    assert is_company_capability_question(msg_a2) is False

    # Message B
    msg_b = "Siz dövme yapıyor musunuz?"
    assert is_user_business_identity_message(msg_b) is False
    assert is_company_capability_question(msg_b) is True

    ctx_b_str = build_generic_business_context(msg_b, {"business_name": "DOEL", "unavailable_services": {"dovmecilik": "dövme"}})
    ctx_b = json.loads(ctx_b_str)
    assert "unavailable_services" in ctx_b, "should keep unavailable_services because it is a capability question"
    assert "instruction_override" not in ctx_b



def test_user_business_identity_detection_generic_forms():
    from app.main import is_user_business_identity_message, is_company_capability_question

    identity_messages = [
        "Ben dövmeciyim, sitenizi gördüm merak edip yazdım",
        "Dövmeciyim ben, sitenizi gördüm",
        "Ben kuaförüm",
        "Emlakçıyım",
        "Güzellik merkezim var",
        "Ben güzellik merkezi sahibiyim",
        "Ben diş kliniğiyim",
    ]
    for message in identity_messages:
        assert is_user_business_identity_message(message) is True, message
        assert is_company_capability_question(message) is False, message

    assert is_user_business_identity_message("Siz dövme yapıyor musunuz?") is False
    assert is_company_capability_question("Siz dövme yapıyor musunuz?") is True


def test_config_driven_identity_reply_has_no_sector_specific_hardcode():
    reply = gc.build_user_business_identity_reply({
        "business_name": "Config Test",
        "service_catalog": [
            {"display": "Hizmet A", "summary": "müşteri kazanımı"},
            {"display": "Hizmet B", "service_fit": "süreç yönetimi"},
        ],
    })
    normalized = gc.sanitize_text(reply).lower()
    assert "hizmet a" in normalized
    assert "hizmet b" in normalized
    assert "dovmeci" not in normalized
    assert "tattoo" not in normalized
    assert gc.reply_question_count(reply) <= 1
