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


def test_dm_quality_detects_repeated_replies():
    assert_no_repeated_replies(
        [
            {"reply_text": "Web sitesi tarafinda yardimci olabiliriz."},
            {"reply_text": "Kisa bir on gorusme ile netlestirebiliriz."},
        ]
    )
