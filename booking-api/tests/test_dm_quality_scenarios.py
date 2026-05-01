import json

from app import main
from dm_quality_assertions import (
    assert_booking_progression,
    assert_no_repeated_replies,
    assert_quality_reply,
)


def _ai_json(**overrides):
    data = {
        "reply_text": "Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "extracted_service": None,
        "extracted_name": None,
        "extracted_phone": None,
        "requested_date": None,
        "requested_time": None,
        "missing_fields": [],
        "crm_action": "update_customer",
        "handoff_needed": False,
    }
    data.update(overrides)
    return json.dumps(data, ensure_ascii=False)


def test_dm_quality_service_and_objection_scenarios(monkeypatch):
    cases = [
        {
            "name": "genel hizmet bilgisi",
            "message": "Hizmetleriniz hakkinda detayli bilgi almak istiyorum",
            "conversation": {"state": "new", "memory_state": {}},
            "llm": {},
            "expected_any": ["web", "otomasyon", "reklam", "sosyal medya"],
            "forbidden": ["mesajinizi dikkate", "dogrudan cevap vereyim", "anlasilmadi"],
        },
        {
            "name": "otomasyon bilgisi",
            "message": "Otomasyon hakkinda bilgi verin",
            "conversation": {"state": "new", "memory_state": {}},
            "llm": {},
            "expected_any": ["dm", "randevu", "musteri"],
            "forbidden": ["daha fazla bilgi almak ister misiniz", "anlasilmadi"],
        },
        {
            "name": "guven itirazi",
            "message": "Dolandirici misiniz?",
            "conversation": {"state": "new", "memory_state": {}},
            "llm": {"reply_text": "Transparent hizmet veriyoruz. Hangi konuda bilgi almak isteriz?"},
            "expected_any": ["doel", "seffaf", "guven"],
            "forbidden": ["transparent", "hangi konuda bilgi almak"],
        },
        {
            "name": "web sektor baglami",
            "message": "Dovmeciyim iste uygun olsun sik olsun falan",
            "conversation": {"service": "Web Tasarim - KOBI Paketi", "state": "new", "memory_state": {"customer_sector": "beauty"}},
            "llm": {"reply_text": "Uzgunuz, anlasilmadi. Lutfen daha acik bir sekilde sorunuz."},
            "expected_any": ["web", "dovme", "sik", "gorunurluk"],
            "forbidden": ["anlasilmadi", "lutfen daha acik", "otomasyon"],
        },
        {
            "name": "web hizmeti bilinirken hizmet sormaz",
            "message": "Web sitesi actirmak istiyom",
            "conversation": {"state": "new", "memory_state": {}},
            "llm": {"reply_text": "Web sitesi icin size yardimci olabilirim. Hangi hizmete ihtiyaciniz var?"},
            "expected_any": ["12.900", "7-14", "google", "whatsapp", "on gorusme"],
            "forbidden": ["hangi hizmet", "hangi konuda", "neye ihtiyac"],
        },
    ]

    for case in cases:
        monkeypatch.setattr(main, "call_llm_content", lambda *args, _case=case, **kwargs: _ai_json(**_case["llm"]))
        decision = main.build_ai_first_decision(case["message"], case["conversation"], [], {})

        assert_quality_reply(
            case["name"],
            case["message"],
            decision,
            expected_any=case["expected_any"],
            forbidden=case["forbidden"],
        )


def test_dm_quality_booking_flow_with_instagram_fallback():
    conversation = {
        "instagram_user_id": "qa-flow-001",
        "instagram_username": "qa_flow_001",
        "service": "Web Tasarim - KOBI Paketi",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Adinizi ve soyadinizi yazar misiniz?",
        "intent": "collect_name",
        "should_reply": True,
        "booking_intent": True,
        "missing_fields": ["name"],
        "extracted_name": None,
    }

    changed = main.override_ai_first_collect_name_refusal(
        decision,
        conversation,
        "Paylasamam boyle kaydet",
        state_before_update="collect_name",
    )

    assert changed is True
    assert_booking_progression(
        "instagram fallback booking",
        [
            {"reply_text": "Tabii, Web Tasarim - KOBI Paketi icin on gorusme planlayabiliriz.", "metrics": {"booking_stage": "collect_name"}},
            {"reply_text": decision["reply_text"], "metrics": {"booking_stage": "collect_date", "contact_channel": conversation["memory_state"].get("contact_channel")}},
        ],
        required_stages=["collect_name", "collect_date"],
    )
    assert_quality_reply(
        "instagram fallback booking",
        "Paylasamam boyle kaydet",
        decision,
        expected_any=["instagram", "saat", "on gorusme"],
        forbidden=["telefon", "paylasmazsaniz", "planlayamayiz"],
    )


def test_known_service_booking_intent_starts_consultation(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon tarafinda DM yanitlari, randevu toplama ve CRM takibi tek akista calisir. "
                "Gunluk DM yogunlugunuzu yazarsaniz net oneri yapayim."
            ),
            intent="service_info",
            extracted_service="Otomasyon & Yapay Zeka Çözümleri",
            booking_intent=False,
        ),
    )

    decision = main.build_ai_first_decision(
        "Tamam on gorusme yapalim",
        {
            "service": "Otomasyon & Yapay Zeka Çözümleri",
            "state": "new",
            "booking_kind": None,
            "memory_state": {},
        },
        [],
        {},
    )

    assert decision["should_reply"] is True
    assert decision["booking_intent"] is True
    assert decision["intent"] == "service_consultation_acceptance"
    assert decision["missing_fields"][0] in {"name", "full_name"}
    assert "ad" in main.sanitize_text(decision["reply_text"]).lower()


def test_dm_quality_detects_repeated_replies():
    assert_no_repeated_replies(
        [
            {"reply_text": "Web sitesi tarafinda yardimci olabiliriz."},
            {"reply_text": "Kisa bir on gorusme ile netlestirebiliriz."},
        ]
    )


def test_soft_cta_after_service_info_closeout(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Gorusmek uzere, iyi gunler dilerim.",
            intent="closing",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "new",
        "memory_state": {"last_bot_reply_type": "service_info"},
    }

    decision = main.build_ai_first_decision("Tamam anladim sorum yok eyvallah", conversation, [], {})

    assert decision["should_reply"] is True
    assert decision["booking_intent"] is False
    assert decision["intent"] == "soft_cta"
    normalized_reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "10 dakikalik" in normalized_reply
    assert "on gorusme" in normalized_reply
    assert "otomasyon" in normalized_reply
    assert decision["extracted_name"] is None
    memory = conversation["memory_state"]
    assert memory["soft_cta_offered"] is True
    assert memory["soft_cta_service"] == "Otomasyon & Yapay Zeka Çözümleri"
    assert memory["pending_offer"] == "preconsultation_offer"
    assert memory["last_bot_reply_type"] == "soft_cta"


def test_soft_cta_only_once_per_service(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Gorusmek uzere, iyi gunler dilerim.",
            intent="closing",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "new",
        "memory_state": {
            "last_bot_reply_type": "service_info",
            "soft_cta_offered": True,
            "soft_cta_service": "Otomasyon & Yapay Zeka Çözümleri",
        },
    }

    decision = main.build_ai_first_decision("Tamam anladim", conversation, [], {})

    assert decision["intent"] != "soft_cta"
    assert "10 dakikalik" not in main.sanitize_text(decision["reply_text"]).lower()


def test_soft_cta_does_not_trigger_in_active_booking(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Adinizi ve soyadinizi tam olarak yazar misiniz?",
            intent="collect_name",
            booking_intent=True,
            missing_fields=["name"],
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {"last_bot_reply_type": "service_info"},
    }

    decision = main.build_ai_first_decision("tamam anladim", conversation, [], {})

    assert decision["intent"] != "soft_cta"
    assert "10 dakikalik" not in main.sanitize_text(decision["reply_text"]).lower()


def test_soft_cta_decline_marks_memory(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Tabii, ne zaman isterseniz buradan yazabilirsiniz.",
            intent="closing",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "new",
        "memory_state": {
            "pending_offer": "preconsultation_offer",
            "last_bot_reply_type": "soft_cta",
        },
    }

    decision = main.build_ai_first_decision("sonra yazarim gerek yok", conversation, [], {})

    assert decision["intent"] == "soft_cta_declined"
    assert decision["booking_intent"] is False
    assert "sonra yazar" in main.sanitize_text(decision["reply_text"]).lower()
    memory = conversation["memory_state"]
    assert memory["soft_cta_declined"] is True
    assert memory["pending_offer"] is None
    assert memory["last_bot_reply_type"] == "closing"


def test_soft_cta_acceptance_starts_booking(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Teşekkürler. Ön görüşme kaydını tamamlamak için telefon numaranızı paylaşır mısınız?",
            intent="service_consultation_acceptance",
            booking_intent=True,
            extracted_service="Otomasyon & Yapay Zeka Çözümleri",
            missing_fields=["phone"],
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "new",
        "memory_state": {
            "pending_offer": "preconsultation_offer",
            "last_bot_reply_type": "soft_cta",
        },
    }

    decision = main.build_ai_first_decision("olur", conversation, [], {})

    assert decision["intent"] == "service_consultation_acceptance"
    assert decision["booking_intent"] is True
    assert set(decision["missing_fields"]) & {"name", "full_name"}
    assert "ad" in main.sanitize_text(decision["reply_text"]).lower()
    assert "telefon" not in main.sanitize_text(decision["reply_text"]).lower()


def test_soft_cta_direct_name_continues_booking(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Memnun oldum Berkay. Web tarafında yardımcı olabiliriz.",
            intent="info",
            booking_intent=False,
            extracted_name="Berkay Elbir",
        ),
    )
    conversation = {
        "service": "Web Tasarım - KOBİ Paketi",
        "state": "new",
        "memory_state": {
            "pending_offer": "preconsultation_offer",
            "last_bot_reply_type": "soft_cta",
        },
    }

    decision = main.build_ai_first_decision("Berkay Elbir", conversation, [], {})

    assert decision["booking_intent"] is True
    assert decision["intent"] == "soft_cta_name_received"
    assert decision["extracted_name"] == "Berkay Elbir"
    assert "telefon" in main.sanitize_text(decision["reply_text"]).lower()


def test_completed_state_closeout_does_not_soft_cta(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Rica ederiz, iyi gunler dileriz.",
            intent="closing",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "completed",
        "memory_state": {"last_bot_reply_type": "service_info"},
    }

    decision = main.build_ai_first_decision("tamam anladim tesekkurler", conversation, [], {})

    assert decision["intent"] != "soft_cta"
    assert "10 dakikalik" not in main.sanitize_text(decision["reply_text"]).lower()


def test_completed_state_new_service_request_is_not_blocked(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Web tasarım tarafında yardımcı olabiliriz.",
            intent="service_info",
            booking_intent=False,
            extracted_service="Web Tasarım - KOBİ Paketi",
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "completed",
        "memory_state": {"last_bot_reply_type": "closing"},
    }

    decision = main.build_ai_first_decision("yeni web sitesi görüşelim", conversation, [], {})

    assert decision["intent"] in {"service_consultation_acceptance", "service_info"}
    assert "web" in main.sanitize_text(decision.get("reply_text") or "").lower()
    assert decision["intent"] != "soft_cta"


def test_soft_cta_not_used_for_question(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Otomasyon hizmeti ilk 3 ay aylik 5.000 TL'dir.",
            intent="pricing_info",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "new",
        "memory_state": {"last_bot_reply_type": "service_info"},
    }

    decision = main.build_ai_first_decision("tamam ama fiyat neydi?", conversation, [], {})

    assert decision["intent"] != "soft_cta"
    assert "10 dakikalik" not in main.sanitize_text(decision["reply_text"]).lower()
