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


def test_greeting_and_smalltalk_are_natural_even_if_ai_is_bad(monkeypatch):
    responses = iter(
        [
            _ai_json(reply_text="Merahaba! DOEL Digital olarak size yardımcı olmanın mutluluğunu duyuyoruz.", intent="greeting"),
            _ai_json(reply_text="İyi misiniz?", intent="smalltalk"),
        ]
    )
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: next(responses))

    greeting = main.build_ai_first_decision("Merhaba", {"state": "new", "memory_state": {}}, [], {})
    smalltalk = main.build_ai_first_decision("Nasılsınız", {"state": "new", "memory_state": {}}, [], {})

    assert "merahaba" not in main.sanitize_text(greeting["reply_text"]).lower()
    assert "memnuniyet" not in main.sanitize_text(greeting["reply_text"]).lower()
    assert "nasıl yardımcı" in greeting["reply_text"].lower()
    assert "iyi misiniz" not in smalltalk["reply_text"].lower()
    assert any(
        cue in smalltalk["reply_text"].lower()
        for cue in ["iyiyim", "teşekkür", "yardımcı"]
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


def test_service_overview_question_is_not_locked_to_previous_service(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümleri müşteri mesajlarına 7/24 yanıt, "
                "randevu ve CRM takibi içerir. Hizmet hakkında daha fazla bilgi almak ister misiniz?"
            ),
            intent="service_info",
            extracted_service="Otomasyon & Yapay Zeka Çözümleri",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_service",
        "memory_state": {},
    }

    decision = main.build_ai_first_decision("Hangi hizmetleriniz var", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "web" in reply
    assert "otomasyon" in reply
    assert "reklam" in reply
    assert "sosyal medya" in reply
    assert "daha fazla bilgi almak ister misiniz" not in reply


def test_bare_automation_interest_gets_short_useful_service_intro(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Otomasyon hakkında daha fazla bilgi istiyorsanız, lütfen detaylı bilgi için bana ulaşın.",
            intent="info",
            extracted_service="Otomasyon & Yapay Zeka Çözümleri",
            booking_intent=False,
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Otomasyon", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "dm" in reply
    assert "randevu" in reply
    assert "crm" in reply
    assert "bana ulasin" not in reply
    assert len(decision["reply_text"].split()) <= 35


def test_unrelated_question_gets_direct_answer_without_previous_service_pitch(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Hayır, emlak hizmeti sunmuyoruz. Daha önce Otomasyon & Yapay Zeka "
                "Çözümleri hakkında konuştuğumuz vardı. İlgileniyorsanız detayları görüşebiliriz."
            ),
            intent="off_topic",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_service",
        "memory_state": {},
    }

    decision = main.build_ai_first_decision("Ev satıyor musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "hayir" in reply or "hayır" in reply
    assert "emlak" in reply or "ev" in reply
    assert "sunmuyoruz" in reply or "satmiyoruz" in reply or "satmıyoruz" in reply
    assert "daha once" not in reply and "daha önce" not in reply
    assert "detaylari gorusebiliriz" not in reply


def test_assistant_identity_answer_is_truthful_not_denial(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hayır, ben bir yapay zeka değilim; DOEL Digital'in otomatik yanıt sistemiyim.",
            intent="assistant_identity",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_service",
        "memory_state": {},
    }

    decision = main.build_ai_first_decision("Sen yapay zeka mısın", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "yapay zeka" in reply
    assert "asistan" in reply
    assert "ekibe" in reply or "doel" in reply
    assert "hayir" not in reply and "hayır" not in reply
    assert "degilim" not in reply and "değilim" not in reply


def test_business_fit_question_does_not_become_brochure_dump(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümleri'mizin sizin için yararlı olacağını düşünüyoruz çünkü "
                "müşteri mesajlarına 7/24 yanıt, randevuları otomatik ayarlama, teklif ve fatura otomasyonu "
                "gibi özellikler içerir. Avantajlarını sizinle detaylı bir şekilde görüşmek isteriz. "
                "İlgileniyorsanız, size özel bir teklif hazırlarız."
            ),
            intent="service_info",
            extracted_service="Otomasyon & Yapay Zeka Çözümleri",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_service",
        "memory_state": {},
    }

    decision = main.build_ai_first_decision("İşime yarar mı?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "yarar" in reply or "uygun" in reply or "isletme" in reply or "işletme" in reply
    assert "detayli bir sekilde gorusmek isteriz" not in reply
    assert "size ozel bir teklif hazirlariz" not in reply
    assert len(decision["reply_text"].split()) <= 45


def test_tattoo_sector_memory_merges_visibility_goal_without_losing_subsector():
    conversation = {"state": "new", "memory_state": {}}

    main.update_conversation_memory_from_user_message("Ben dovmeciyim", conversation, [], {})
    memory = conversation["memory_state"]
    assert memory["customer_sector"] == "beauty"
    assert memory["customer_subsector"] == "tattoo"

    main.update_conversation_memory_from_user_message(
        "Sosyal medyada gorunur olmak istiyorum reklam veriyorum",
        conversation,
        [],
        {},
    )

    memory = conversation["memory_state"]
    assert memory["customer_goal"] == "visibility/ads"
    assert memory["customer_sector"] == "beauty"
    assert memory["customer_subsector"] == "tattoo"


def test_tattoo_business_fit_recommends_social_and_ads_before_asking(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Yarar saglayip saglamayacagini net soylemek icin isinizi ve hedefinizi bilmem gerekir.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Bunlardan hangisi isime yarar? Ben dovmeciyim", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "sosyal medya" in reply or "performans reklam" in reply
    assert any(token in reply for token in ["dovme", "portfolyo", "lokasyon", "gorsel", "instagram"])
    assert reply.count("?") <= 1
    assert "isinizi ve hedefinizi bilmem gerekir" not in reply
    assert "yarar saglayip saglamayacagini net soylemek" not in reply


def test_service_overview_with_tattoo_context_does_not_fall_back_to_generic_list(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Web tasarim, otomasyon, reklam ve sosyal medya yonetimi yapiyoruz. Hangisini merak ettiginizi yazarsaniz anlatayim.",
            intent="service_overview",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Hizmetleriniz neler? Ben dovmeciyim", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "tattoo"
    assert "sosyal medya" in reply or "performans reklam" in reply
    assert any(token in reply for token in ["dovme", "portfolyo", "lokasyon", "instagram", "gorsel"])
    assert "hangisini merak ettiginizi" not in reply


def test_final_guard_repairs_generic_overview_when_latest_message_has_sector(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hangi hizmeti merak ettiginizi yazarsaniz fiyat, kapsam ve teslim suresini anlatayim.",
            intent="service_overview",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Hizmetleriniz neler? Ben musluk tamircisiyim", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "plumbing"
    assert "musluk" in reply or "tesisat" in reply or "lokal" in reply
    assert "google" in reply or "landing" in reply or "reklam" in reply
    assert "hangi hizmeti merak" not in reply


def test_tattoo_visibility_ads_goal_gets_direct_recommendation_without_cta(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hedefinizi bilmem gerekir, daha net bilgi verirseniz yardimci olurum.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "tattoo"},
    }

    decision = main.build_ai_first_decision(
        "Sosyal medyada gorunur olmak istiyorum reklam veriyorum",
        conversation,
        [],
        {},
    )

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "sosyal medya" in reply
    assert "performans reklam" in reply or "reklam" in reply
    assert any(token in reply for token in ["kitle", "portfolyo", "instagram", "gorsel"])
    assert "otomasyon" not in reply
    assert "on gorusme" not in reply and "10 dakikalik" not in reply and "randevu" not in reply
    assert "web tasarim" not in reply or "sosyal medya" in reply
    assert "hedefinizi bilmem gerekir" not in reply
    assert conversation["memory_state"]["customer_goal"] == "visibility/ads"
    assert conversation["memory_state"]["customer_subsector"] == "tattoo"


def test_tattoo_dm_and_appointment_goal_allows_automation_recommendation(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Sosyal medya tarafinda ilerleyebiliriz.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "tattoo"},
    }

    decision = main.build_ai_first_decision("DM cok geliyor randevular karisiyor", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "otomasyon" in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert conversation["memory_state"]["customer_goal"] == "dm_automation"


def test_latest_sector_overrides_old_tattoo_memory_for_plumbing(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Dovme isi icin sosyal medya ve portfolyo onceliklidir.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "tattoo"},
    }

    decision = main.build_ai_first_decision("Musluk tamircisiyim ben hangisi isime yarar?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    memory = conversation["memory_state"]
    assert memory["customer_sector"] == "local_service"
    assert memory["customer_subsector"] == "plumbing"
    assert "musluk" in reply or "tesisat" in reply
    assert "web" in reply or "landing" in reply
    assert "google" in reply or "reklam" in reply
    assert "dovme" not in reply and "tattoo" not in reply


def test_latest_sector_overrides_old_plumbing_memory_for_tattoo(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Musluk tamiri icin Google reklam ve landing page uygundur.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "local_service", "customer_subsector": "plumbing"},
    }

    decision = main.build_ai_first_decision("Ben dovmeciyim hangisi isime yarar?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    memory = conversation["memory_state"]
    assert memory["customer_sector"] == "beauty"
    assert memory["customer_subsector"] == "tattoo"
    assert "sosyal medya" in reply or "performans reklam" in reply
    assert "portfolyo" in reply or "instagram" in reply
    assert "musluk" not in reply and "tesisat" not in reply


def test_plumbing_visibility_recommendation_does_not_use_old_tattoo_context(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Instagram portfolyonuzu daha gorunur hale getirebiliriz.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "tattoo"},
    }

    decision = main.build_ai_first_decision("Yok ben muslukcuyum, musteri bulmak istiyorum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "plumbing"
    assert "google" in reply or "web" in reply or "landing" in reply
    assert "acil" in reply or "lokal" in reply or "usta" in reply
    assert "portfolyo" not in reply and "dovme" not in reply


def test_hairdresser_business_fit_uses_beauty_context(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hedefinizi bilmem gerekir.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Ben kuaforum hangisi isime yarar?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "hairdresser"
    assert "sosyal medya" in reply
    assert "reklam" in reply
    assert "randevu" in reply or "lokasyon" in reply or "model" in reply
    assert "hedefinizi bilmem gerekir" not in reply


def test_hairdresser_followup_recommendation_does_not_repeat_previous_reply(monkeypatch):
    previous_reply = (
        "Kuafor/berber tarafinda sosyal medya yonetimi + lokal reklam en mantikli baslangic olur; "
        "musteri model, lokasyon ve guvene bakarak karar verir."
    )
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=previous_reply,
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "hairdresser"},
    }
    history = [{"direction": "out", "message_text": previous_reply}]

    decision = main.build_ai_first_decision("Hangisi isime yarar?", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert reply != main.sanitize_text(previous_reply).lower()
    assert "sosyal medya" in reply
    assert "lokal reklam" in reply or "randevu" in reply
    assert "web sitesi" in reply or "portfolyo" in reply or "model" in reply


def test_real_estate_goal_recommends_web_lead_ads_and_crm(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Daha fazla bilgi ister misiniz?",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    main.update_conversation_memory_from_user_message("Ben emlakciyim", conversation, [], {})
    decision = main.build_ai_first_decision("Musteri bulmak istiyorum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "web" in reply or "landing" in reply
    assert "lead" in reply or "reklam" in reply
    assert "crm" in reply or "otomasyon" in reply
    assert "daha fazla bilgi ister misiniz" not in reply


def test_real_estate_goal_followup_does_not_repeat_previous_reply(monkeypatch):
    previous_reply = (
        "Emlak tarafinda web/landing page + lead reklam en dogru baslangic olur; "
        "ilanlari guven veren bir sayfada toplayip dogru bolgeden talep cekmek gerekir."
    )
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=previous_reply,
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "real_estate", "customer_subsector": "real_estate"},
    }
    history = [{"direction": "out", "message_text": previous_reply}]

    decision = main.build_ai_first_decision("Musteri bulmak istiyorum", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert reply != main.sanitize_text(previous_reply).lower()
    assert "lead" in reply or "reklam" in reply
    assert "crm" in reply
    assert "ilan" in reply or "landing" in reply


def test_price_question_with_business_context_gets_scope_answer_not_evasive(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Net soylemek icin isinizi bilmem gerekir.",
            intent="pricing_info",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "local_service", "customer_subsector": "plumbing"},
    }

    decision = main.build_ai_first_decision("Fiyat ne kadar?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "fiyat" in reply
    assert "kapsam" in reply or "web" in reply or "reklam" in reply or "otomasyon" in reply
    assert "net soylemek icin" not in reply
    assert "isinizi bilmem gerekir" not in reply
    assert len([part for part in decision["reply_text"].replace("?", ".").split(".") if part.strip()]) <= 3


def test_quality_guard_rejects_wrong_sector_ai_candidate():
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "local_service", "customer_subsector": "plumbing"},
    }
    bad_reply = "Dövme işi için sosyal medya portfolyonuzu büyütmek en mantıklı başlangıç olur."

    guarded = main.guard_and_repair_final_answer(
        "Musluk tamircisiyim ben hangisi isime yarar?",
        bad_reply,
        conversation,
        [],
        decision_label="service_advice",
    )

    reply = main.sanitize_text(guarded["reply_text"]).lower()
    assert guarded["passed"] is True
    assert guarded["repaired"] is True
    assert "musluk" in reply or "tesisat" in reply
    assert "dovme" not in reply and "tattoo" not in reply


def test_unrelated_question_does_not_get_sales_pitch_even_with_business_memory(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Dövme işi için sosyal medya reklamlarıyla ilerleyebiliriz.",
            intent="off_topic",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "tattoo"},
    }

    decision = main.build_ai_first_decision("Ev satiyor musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "hayir" in reply or "satmiyoruz" in reply or "sunmuyoruz" in reply
    assert "dovme" not in reply and "sosyal medya reklam" not in reply


def test_completed_booking_thanks_does_not_repeat_appointment_summary(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Sistemimizde onaylı ön görüşme kaydınız 03.05.2026 saat 15:00 olarak görünüyor. "
                "Değişiklik veya iptal ihtiyacınız olursa sizi yetkili ekibimize yönlendirebilirim."
            ),
            intent="confirmed_followup",
            booking_intent=False,
        ),
    )
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "completed",
        "appointment_status": "confirmed",
        "requested_date": "2026-05-03",
        "requested_time": "15:00",
        "memory_state": {},
    }

    decision = main.build_ai_first_decision("Teşekkür ederim", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "rica" in reply or "gorusmede" in reply or "görüşmede" in reply
    assert "sistemimizde" not in reply
    assert "03.05.2026" not in reply
    assert "15:00" not in reply


def test_booking_created_confirmation_is_not_replaced_by_generic_service_reply():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "state": "completed",
        "appointment_status": "confirmed",
        "requested_date": "2026-05-03",
        "requested_time": "11:00",
        "memory_state": {},
    }
    confirmation = (
        "Ön görüşme kaydınız oluşturuldu.\n\n"
        "Ad Soyad: Berkay Elbir\n"
        "Hizmet: Web Tasarım - KOBİ Paketi\n"
        "Tarih: 03.05.2026\n"
        "Saat: 11:00\n"
        "Telefon: +905555555555\n\n"
        "Görüşme günü bu saat için müsaitliğinizi ayarlamanız yeterli."
    )

    guarded = main.guard_and_repair_final_answer(
        "03.05.2026 11:00",
        confirmation,
        conversation,
        [],
        decision_label="ai_first_v5:booking_date_collected",
    )

    assert guarded["repaired"] is False
    assert "on gorusme kaydiniz olusturuldu" in main.sanitize_text(guarded["reply_text"]).lower()
    assert "03.05.2026" in guarded["reply_text"]


def test_soft_cta_after_sector_price_context_without_locked_service(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Pekala, size en uygun hizmeti belirlemek için işletmenizin hedeflerini daha iyi anlamamız gerekiyor. "
                "Hangi hizmetlerimizden yararlanmak istiyorsunuz?"
            ),
            intent="closing",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {
            "customer_sector": "beauty",
            "customer_subsector": "hairdresser",
            "last_bot_reply_type": "pricing_info",
        },
    }

    decision = main.build_ai_first_decision("Mantikli", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["intent"] == "soft_cta"
    assert "10 dakikalik" in reply
    assert "on gorusme" in reply
    assert "hangi hizmet" not in reply


def test_sector_intro_gets_contextual_recommendation_not_generic_fallback(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Buradayım. Web, otomasyon, reklam veya sosyal medya tarafında neyi merak ettiğinizi yazarsanız net şekilde cevaplayayım.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Ben kuaforum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["intent"] == "business_context_intro"
    assert "kuafor" in reply or "berber" in reply
    assert "sosyal medya" in reply or "reklam" in reply
    assert "neyi merak" not in reply


def test_real_estate_intro_sets_memory_and_uses_real_estate_context(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Buradayım. Web, otomasyon, reklam veya sosyal medya tarafında neyi merak ettiğinizi yazarsanız net şekilde cevaplayayım.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Ben emlakciyim", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "real_estate"
    assert "emlak" in reply
    assert "lead" in reply or "reklam" in reply or "crm" in reply
    assert "neyi merak" not in reply


def test_company_capability_haircut_question_does_not_write_hairdresser_memory(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Kuafor/berber tarafinda sosyal medya yonetimi + lokal reklam en mantikli baslangic olur.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Sac kesiyor musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    memory = conversation["memory_state"]
    assert "hayir" in reply
    assert "sac kesimi" in reply or "sac kes" in reply
    assert "sosyal medya yonetimi + lokal reklam" not in reply
    assert memory.get("customer_sector") is None
    assert memory.get("customer_subsector") is None


def test_company_capability_question_general_patterns_do_not_update_sector_memory(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Emlak tarafinda web/landing page + lead reklam en dogru baslangic olur.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    cases = [
        ("Ev satiyor musunuz?", ["ev", "emlak"]),
        ("Musluk tamir ediyor musunuz?", ["musluk", "tamir"]),
        ("Dis cekiyor musunuz?", ["dis", "cek"]),
        ("Kargo yapiyor musunuz?", ["kargo"]),
        ("Kuafor musunuz?", ["kuafor"]),
    ]

    for message, expected_terms in cases:
        conversation = {"service": None, "state": "new", "memory_state": {}}
        decision = main.build_ai_first_decision(message, conversation, [], {})

        reply = main.sanitize_text(decision["reply_text"]).lower()
        assert "hayir" in reply
        assert any(term in reply for term in expected_terms)
        assert "sosyal medya yonetimi + lokal reklam" not in reply
        assert "lead reklam en dogru baslangic" not in reply
        assert conversation["memory_state"].get("customer_sector") is None
        assert conversation["memory_state"].get("customer_subsector") is None


def test_user_correction_company_capability_runs_before_recommendation(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Kuafor/berber tarafinda sosyal medya yonetimi + lokal reklam en mantikli baslangic olur.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "hairdresser"},
    }

    decision = main.build_ai_first_decision("Hayir siz sac kesiyor musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "hayir" in reply
    assert "sac kesimi" in reply or "sac kes" in reply
    assert "lokal reklam" not in reply
    assert "kuafor/berber tarafinda" not in reply


def test_user_business_identity_hairdresser_still_triggers_recommendation(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hayir, biz kuafor hizmeti vermiyoruz.",
            intent="company_capability",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Ben kuaforum, hangisi isime yarar?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert conversation["memory_state"]["customer_subsector"] == "hairdresser"
    assert "sosyal medya" in reply
    assert "lokal reklam" in reply or "reklam" in reply
    assert "biz kuafor hizmeti vermiyoruz" not in reply


def test_assistant_identity_question_gets_digital_assistant_answer(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Evet, yapay zeka ve otomasyon cozumleri konusunda uzman bir ekibiz.",
            intent="assistant_identity",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {"service": None, "state": "new", "memory_state": {}}

    decision = main.build_ai_first_decision("Yapay zeka misin sen?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "doel digital" in reply
    assert "dijital asistan" in reply
    assert "uzman bir ekibiz" not in reply


def test_ping_attention_does_not_reset_context_or_recommend(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Kuafor/berber tarafinda sosyal medya yonetimi + lokal reklam en mantikli baslangic olur.",
            intent="service_advice",
            booking_intent=False,
            missing_fields=[],
        ),
    )
    conversation = {
        "service": None,
        "state": "new",
        "memory_state": {"customer_sector": "beauty", "customer_subsector": "hairdresser"},
    }

    decision = main.build_ai_first_decision("Alo?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "buradayim" in reply
    assert "lokal reklam" not in reply
    assert conversation["memory_state"]["customer_subsector"] == "hairdresser"


def test_forced_bad_ai_capability_reply_is_rejected_and_repaired():
    conversation = {"service": None, "state": "new", "memory_state": {}}
    bad_reply = "Kuafor/berber tarafinda sosyal medya yonetimi + lokal reklam en mantikli baslangic olur."

    guarded = main.guard_and_repair_final_answer(
        "Sac kesiyor musunuz?",
        bad_reply,
        conversation,
        [],
        decision_label="service_advice",
    )

    reply = main.sanitize_text(guarded["reply_text"]).lower()
    assert guarded["passed"] is True
    assert guarded["repaired"] is True
    assert "hayir" in reply
    assert "sac kesimi" in reply or "sac kes" in reply
    assert "lokal reklam" not in reply
