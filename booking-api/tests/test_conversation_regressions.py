from app import main


def _ai_json(**overrides):
    data = {
        "reply_text": "Tamam, kaydı tamamlamak için telefon numaranızı paylaşır mısınız?",
        "intent": "appointment",
        "should_reply": True,
        "booking_intent": True,
        "extracted_service": None,
        "extracted_name": None,
        "extracted_phone": None,
        "requested_date": None,
        "requested_time": None,
        "missing_fields": ["phone"],
        "crm_action": "update_customer",
        "handoff_needed": False,
    }
    data.update(overrides)
    import json

    return json.dumps(data, ensure_ascii=False)


def test_ascii_stored_service_is_displayed_with_turkish_characters_in_clarifications():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    replies = [
        main.build_contextual_clarification_reply(conversation, "Ne on gorusmesi?"),
        main.build_phone_refusal_reply(conversation),
        main.build_collect_name_request_reply(
            conversation,
            "on gorusme",
            main.build_captured_ack_prefix(conversation),
            same_service_restatement=True,
        ),
    ]

    for reply in replies:
        assert "Otomasyon & Yapay Zeka \u00c7\u00f6z\u00fcmleri" in reply
        assert "Cozumleri" not in reply


def test_general_question_does_not_get_hijacked_by_existing_service_context():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    message = "T\u00fcrkiye ba\u015fkenti neresi?"
    matched = main.match_service_candidates(message, conversation.get("service"))
    result = main.maybe_build_information_reply(message, {}, matched, conversation, [])

    assert result["kind"] == "generic_ai"
    assert "Ankara" in result["reply"]
    assert "DOEL AI" not in result["reply"]
    assert "\u00f6n g\u00f6r\u00fc\u015fme" not in result["reply"].lower()


def test_numeric_range_after_volume_question_is_not_treated_as_date():
    message = "30-40"
    history = [{"direction": "out", "message_text": "Günlük mesaj yoğunluğunuz yaklaşık kaç?"}]
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {"last_bot_question_type": "message_volume"},
    }

    assert main.extract_date(message) is None
    result = main.maybe_build_information_reply(message, {}, [], conversation, history)

    assert result["kind"] == "message_volume"
    assert "30-40" in result["reply"]
    assert "2040" not in result["reply"]


def test_numeric_range_without_context_still_gets_useful_answer():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("30-40", {}, [], conversation, [])

    assert result["kind"] == "message_volume"
    assert "30-40" in result["reply"]
    assert "hangi konuda destek" not in result["reply"].lower()
    assert "2040" not in result["reply"]


def test_volume_question_reply_is_remembered_from_service_info_text():
    conversation = {"state": "collect_service", "memory_state": {}}

    main.update_conversation_memory_after_bot_reply(
        conversation,
        "Gelen mesajları anında yanıtlayan DOEL AI sistemimizi entegre ediyoruz. Günlük mesaj yoğunluğunuz yaklaşık kaç?",
        "service_info_continue",
    )

    assert conversation["memory_state"]["last_bot_question_type"] == "message_volume"


def test_full_ai_conversational_mode_defaults_to_enabled():
    assert main.FULL_AI_CONVERSATIONAL_MODE is True


def test_full_ai_conversational_mode_is_forced_on():
    assert main.FULL_AI_CONVERSATIONAL_MODE is True


def test_llm_reply_polish_is_forced_on_for_ai_first():
    assert main.LLM_REPLY_POLISH_ENABLED is True


def test_clarification_about_preconsultation_does_not_repeat_phone_request():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Ne on gorusmesi?", {}, [], conversation, [])

    assert result["kind"] == "clarification"
    reply = result["reply"].lower()
    assert "ön görüşme" in reply or "on gorusme" in reply
    assert "telefon numaran" not in result["reply"].lower()


def test_meeting_method_question_answers_before_collecting_phone():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Nasil gorusecegiz?", {}, [], conversation, [])

    assert result["kind"] == "clarification"
    reply = result["reply"].lower()
    assert "görüşme" in reply or "gorusme" in reply
    assert "telefon numaran" not in result["reply"].lower()


def test_meeting_method_question_keeps_clarification_priority_in_service_state():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Nasil gorusecegiz?", {}, [], conversation, [])

    assert result["kind"] == "clarification"
    assert "telefon numaran" not in result["reply"].lower()


def test_meeting_method_question_has_distinct_answer_from_preconsultation_definition():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    definition = main.maybe_build_information_reply("Ne on gorusmesi?", {}, [], conversation.copy(), [])
    method = main.maybe_build_information_reply("Nasil gorusecegiz?", {}, [], conversation.copy(), [])

    assert definition["kind"] == "clarification"
    assert method["kind"] == "clarification"
    assert definition["reply"] != method["reply"]
    method_reply = method["reply"].lower()
    assert "telefon numaran" not in method_reply
    assert any(keyword in method_reply for keyword in ["buradan", "online", "telefon", "instagram"])


def test_ai_first_method_question_cannot_be_hijacked_to_phone_collection(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "full_name": "Ahmet Yilmaz",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: _ai_json())

    decision = main.build_ai_first_decision("Nasil gorusecegiz?", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "telefon numaran" not in reply
    assert any(keyword in reply for keyword in ["buradan", "online", "telefon", "instagram"])


def test_ai_first_delivery_followup_stays_information_answer(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Evet, kapsam büyürse 4 haftaya çıkabilir; entegrasyon sayısı ve özel istekler süreyi belirler.",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("4 haftaya ciktigi oluyor mu?", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "4 hafta" in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply


def test_ai_first_trust_question_uses_clean_turkish_reassurance(monkeypatch):
    conversation = {"state": "new", "memory_state": {}}

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hayır, DOEL DIGITAL olarak Transparent hizmet veriyoruz. Hangi konuda bilgi almak isteriz?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Dolandirici misiniz?", conversation, [], {})

    reply = decision["reply_text"]
    assert decision["booking_intent"] is False
    assert "Transparent" not in reply
    assert "DOEL Digital" in reply
    assert "bilgi almak isteriz" not in reply
    assert "hiçbir bilgi paylaşmak zorunda değilsiniz" in reply


def test_ai_first_aleykum_greeting_does_not_claim_wellbeing(monkeypatch):
    conversation = {"state": "new", "memory_state": {}}

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Merhaba! İyiyim, size nasıl yardımcı olabilirim?",
            intent="greeting",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Salamun aleykum", conversation, [], {})

    assert decision["reply_text"] == "Aleyküm selam, hoş geldiniz. Size nasıl yardımcı olabilirim?"
    assert decision["booking_intent"] is False


def test_ai_first_yes_to_more_details_explains_automation_without_empty_next_step(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Sistemimizle ilgili daha detaylı bilgi almak ister misiniz?",
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Kurulum sonrası sistemi kullanmaya başlayabilirsiniz. Bir sonraki adımımız ne olacak?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet olur", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "bir sonraki adımımız" not in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_to_more_details_handles_which_service_wording(monkeypatch):
    conversation = {
        "service": None,
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Daha detaylı bilgi için hangi hizmetle ilgileneceğinizi öğrenebilir miyim?",
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Hangi hizmetle ilgileniyorsunuz?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet olur", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "hangi hizmet" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_with_punctuation_to_more_details_explains_automation(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Günde yaklaşık 20-25 mesaj yoğunluğu için Otomasyon & Yapay Zeka Çözümlerimiz uygun görünüyor. "
                "İlk 3 ay aylık 5.000 ₺ olacak. Standart kurulum 3-7 iş günü sürer. "
                "Detaylı bilgi almak isterseniz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümlerimizin detaylarına bakalım. "
                "Fiyatımız ilk 3 ay için aylık 5.000 ₺'dir. Kurulum 3-7 iş günü sürer. "
                "Bir sonraki adımımız ne olacak?"
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "bir sonraki adımımız" not in reply
    assert "ne olacak" not in reply
    assert "hangi hizmet" not in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_after_more_details_for_contact_wording_explains_automation(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Otomasyon & Yapay Zeka Çözümleri'nin teslim süresi standart kurulumlarda 3-7 iş günü sürmektedir. "
                "Teslim süresi hakkında daha detaylı bilgi almak için bizimle iletişime geçebilir veya başka bir konuda yardımcı olmamı ister misiniz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümleri hakkında daha fazla bilgi almak ister misiniz "
                "yoksa başka bir konuda yardımcı olmamı ister misiniz?"
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "daha fazla bilgi almak ister misiniz" not in reply
    assert "yardımcı olmamı ister misiniz" not in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_after_automation_sector_question_still_gives_details(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Günlük ortalama 20-25 mesaj için CRM otomasyonumuz oldukça etkili olabilir. "
                "Müşteri mesajlarına 7/24 yanıt, randevuları otomatik ayarlama gibi özelliklerle işinizi kolaylaştırabilir. "
                "Hangi sektörde faaliyet gösteriyorsunuz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "CRM otomasyonumuz günlük 20-25 mesajı yönetebilir. "
                "Bir sonraki adımda sizinle hangi konularda daha detaylı bilgi paylaşmamızı istersiniz?"
            ),
            intent="provide_info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "bir sonraki adım" not in reply
    assert "hangi konularda" not in reply
    assert "hangi sektör" not in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_after_automation_your_sector_question_still_gives_details(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Günlük ortalama 20-25 mesaj için CRM otomasyonumuz oldukça etkili olabilir. "
                "Bir sonraki adımda, sizinle ilgili daha fazla bilgi edinmek isterim. "
                "İşletmenizin sektörünü bana söyleyebilir misiniz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Günlük 20-25 mesaj için sistemimiz faydalı olacaktır. "
                "Sektörünüzü öğrenebilir miyim?"
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "sektörünüzü" not in reply
    assert "sektörünü" not in reply
    assert "bir sonraki adım" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_after_automation_which_service_question_still_gives_details(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Günlük ortalama 20-25 mesaj için CRM otomasyonumuz oldukça etkili olabilir. "
                "Müşteri mesajlarına 7/24 yanıt ve randevuları otomatik ayarlama gibi özellikler sunar. "
                "Hizmetlerimiz hakkında daha fazla bilgi almak için hangi hizmetimizle ilgileniyorsunuz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "CRM otomasyonumuz günlük 20-25 mesajı yönetebilir. Bir sonraki adımda, "
                "hizmetimizin detayları hakkında daha fazla bilgi verilebilir mi?"
            ),
            intent="provide_info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "bir sonraki adım" not in reply
    assert "hangi hizmet" not in reply
    assert "bilgi verilebilir mi" not in reply
    assert "adınız" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_yes_after_automation_interest_question_does_not_start_booking(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Otomasyon & Yapay Zeka Çözümleri hizmetimizin teslim süresi, standart kurulumlarda 3-7 iş günü, "
                "özel entegrasyonlarda 1-3 haftadır. Bu hizmete ilgi duyuyor musunuz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Tabii, Otomasyon & Yapay Zeka Çözümleri için ön görüşme planlayabiliriz. Önce adınızı ve soyadınızı yazar mısınız?",
            intent="booking",
            booking_intent=True,
            missing_fields=["name"],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "adınızı" not in reply and "soyad" not in reply and "telefon numaran" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_automation_more_details_reply_moves_toward_consultation_without_collecting_name():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }

    reply = main.build_more_details_acceptance_reply(conversation).lower()

    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 i" in reply
    assert "ön görüşme" in reply or "on gorusme" in reply
    assert "ad" not in reply and "soyad" not in reply


def test_ai_first_evet_isterim_after_automation_details_offer_uses_detail_bridge(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Otomasyon & Yapay Zeka Çözümlerimiz, müşteri mesajlarına 7/24 yanıt ve randevuları otomatik ayarlama içerir. "
                "Hizmet hakkında daha fazla bilgi almak ister misiniz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümlerimiz, müşteri mesajlarına 7/24 yanıt ve randevuları otomatik ayarlama içerir. "
                "Hizmet hakkında daha fazla bilgi almak ister misiniz?"
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("evet isterim", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "ön görüşme" in reply or "on gorusme" in reply
    assert "hizmet hakkında daha fazla bilgi almak ister misiniz" not in reply
    assert "ad" not in reply and "soyad" not in reply


def test_ai_first_next_step_prompt_after_automation_details_starts_consultation(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Tabii. Otomasyon sistemi gelen DM'leri karşılar, sık soruları yanıtlar, uygun talepleri randevu veya CRM kaydına çevirir "
                "ve panelde takip edilebilir hale getirir. Standart kurulum 3-7 iş günü sürer. İsterseniz kısa bir ön görüşmede size özel akışı netleştirebiliriz."
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Otomasyon & Yapay Zeka Çözümlerimizin detaylarına dair sorularınız varsa cevaplayabilirim. Hizmetimizle ilgili bilgi almak ister misiniz?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("eee?", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "name" in decision["missing_fields"] or "full_name" in decision["missing_fields"]
    assert "ön görüşme" in reply or "on gorusme" in reply
    assert "ad" in reply and "soyad" in reply
    assert "bilgi almak ister misiniz" not in reply


def test_ai_first_yes_after_automation_price_reply_does_not_use_empty_next_step(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Günlük ortalama 20-25 mesaj için bizim Otomasyon & Yapay Zeka Çözümleri hizmetimiz uygun olabilir. "
                "Fiyat bilgisi için Otomasyon & Yapay Zeka Çözümleri 5.000 ₺ ilk 3 ay indirimli aylık hizmet bedeli sunuyoruz."
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Otomasyon & Yapay Zeka Çözümleri hizmetimizle günlük 20-25 mesajı yönetebilirsiniz. "
                "İlk 3 ay indirimli fiyatımız 5.000 ₺'dir. Bir sonraki adımımız ne olacak?"
            ),
            intent="provide_info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Evet.", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "bir sonraki adım" not in reply
    assert "ne olacak" not in reply
    assert "dm" in reply
    assert "randevu" in reply
    assert "3-7 iş günü" in reply


def test_ai_first_positive_web_details_acceptance_starts_consultation(monkeypatch):
    conversation = {
        "service": "Web Tasarım - KOBİ Paketi",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Websitesini merak ediyorsanız, Web Tasarım - KOBİ Paketimizle ilgilenebilirsiniz. "
                "Fiyatımız 12.900 ₺'dir ve teslimat süresi 7-14 iş günüdür. Daha fazla bilgi almak ister misiniz?"
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Web Tasarım - KOBİ Paketimizle kurumsal web tasarım çözümü sunuyoruz. "
                "İsterseniz detayları netleştirmek için konuşmaya devam edelim."
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Olur", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "name" in decision["missing_fields"] or "full_name" in decision["missing_fields"]
    assert "ön görüşme" in reply or "kısa görüşme" in reply
    assert "ad" in reply and "soyad" in reply
    assert "konuşmaya devam edelim" not in reply


def test_ai_first_positive_web_details_acceptance_infers_service_from_history(monkeypatch):
    conversation = {
        "service": None,
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Websitesi hizmetimiz hakkında bilgi almak ister misiniz? Web Tasarım - KOBİ Paketi'miz "
                "12.900 ₺'dir ve 7-14 iş günü içinde teslim edilir."
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Web Tasarım - KOBİ Paketi'miz 12.900 ₺'dir ve 7-14 iş günü içinde teslim edilir. "
                "İsterseniz detaylı bilgi alabilir veya sorularınızı sorabilirsiniz."
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Olur", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "name" in decision["missing_fields"] or "full_name" in decision["missing_fields"]
    assert "ön görüşme" in reply or "kısa görüşme" in reply
    assert "ad" in reply and "soyad" in reply
    assert "sorularınızı sorabilirsiniz" not in reply


def test_ai_first_web_details_continue_acceptance_starts_consultation(monkeypatch):
    conversation = {
        "service": "Web Tasar\u0131m - KOB\u0130 Paketi",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Evet, website hizmetimiz iyi sonu\u00e7lar getiriyor. Web Tasar\u0131m - KOB\u0130 Paketimizle "
                "kurumsal web tasar\u0131m \u00e7\u00f6z\u00fcm\u00fc sunuyoruz. Google uyumlu, t\u00fcm cihazlara tam uyumlu, "
                "WhatsApp butonlu ve 1 y\u0131l altyap\u0131 garantili. Fiyat\u0131m\u0131z 12.900 \u20ba, teslimat s\u00fcresi "
                "7-14 i\u015f g\u00fcn\u00fc. \u0130sterseniz detaylar\u0131 netle\u015ftirmek i\u00e7in konu\u015fmaya devam edelim."
            ),
        }
    ]

    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text=(
                "Websitemiz iyi sonu\u00e7lar getiriyor. Web Tasar\u0131m - KOB\u0130 Paketimizle kurumsal web tasar\u0131m "
                "\u00e7\u00f6z\u00fcm\u00fc sunuyoruz. Fiyat\u0131m\u0131z 12.900 \u20ba, teslimat s\u00fcresi 7-14 i\u015f g\u00fcn\u00fc. "
                "\u0130sterseniz detaylar\u0131 netle\u015ftirmek i\u00e7in konu\u015fmaya devam edelim."
            ),
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Olur peki", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "name" in decision["missing_fields"] or "full_name" in decision["missing_fields"]
    assert "\u00f6n g\u00f6r\u00fc\u015fme" in reply or "k\u0131sa" in reply
    assert "ad" in reply and "soyad" in reply
    assert "websitemiz iyi sonu\u00e7lar getiriyor" not in reply
    assert "konu\u015fmaya devam edelim" not in reply


def test_phone_reason_question_answers_phone_purpose_in_service_state():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Neden telefon istiyorsun?", {}, [], conversation, [])

    assert result["kind"] == "clarification"
    reply = result["reply"].lower()
    assert "telefon" in reply
    assert "zorunda" in reply or "buradan" in reply
    assert "net cevap vereyim" not in reply
    assert "hangi taraf zor geliyor" not in reply


def test_generic_ai_compose_is_not_blocked_by_legacy_polish_flag(monkeypatch):
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "last_customer_message": "Türkiye başkenti neresi?",
        "memory_state": {},
    }

    monkeypatch.setattr(main, "FULL_AI_CONVERSATIONAL_MODE", True)
    monkeypatch.setattr(main, "LLM_REPLY_POLISH_ENABLED", False)
    monkeypatch.setattr(main, "polish_reply_text", lambda *args, **kwargs: "Türkiye'nin başkenti Ankara'dır.")

    reply, elapsed = main.maybe_polish_reply_text(
        "Taslak cevap",
        conversation,
        [],
        enabled=True,
        decision_label="info:generic_ai",
    )

    assert reply == "Türkiye'nin başkenti Ankara'dır."
    assert elapsed >= 0


def test_generic_ai_draft_answers_common_general_question_without_vague_escape():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    reply = main.build_generic_ai_draft_reply("Türkiye başkenti neresi?", conversation, [])

    assert "Ankara" in reply
    assert "elimizde kesin bilgi" not in reply.lower()
    assert "elimde kesin bilgi" not in reply.lower()


def test_generic_ai_draft_invites_unrelated_questions_without_vague_escape():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    reply = main.build_generic_ai_draft_reply("Alakasız bir soru soruyorum ama cevap verir misin?", conversation, [])

    assert "cevap" in reply.lower()
    assert "elimizde kesin bilgi" not in reply.lower()
    assert "elimde kesin bilgi" not in reply.lower()


def test_angry_complaint_does_not_repeat_collection_prompt():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Sen salak misin alo", {}, [], conversation, [])

    assert result["kind"] == "complaint"
    assert "kusura" in result["reply"].lower()
    assert "telefon numaran" not in result["reply"].lower()


def test_angry_repetition_complaint_is_detected():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Sinirlendim, aynı şeyi tekrar edip duruyorsun.", {}, [], conversation, [])

    assert result["kind"] == "complaint"
    assert "telefon numaran" not in result["reply"].lower()


def test_phone_refusal_gets_information_path_without_phone_pressure():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Telefon vermeden bilgi alabilir miyim?", {}, [], conversation, [])

    assert result["kind"] == "phone_refusal"
    assert "telefon numaran" not in result["reply"].lower()


def test_good_wishes_get_social_reply_not_sales_fallback():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Kolay gelsin", {}, [], conversation, [])

    assert result["kind"] == "smalltalk"
    assert "kolay gelsin" in result["reply"].lower()
    assert "hangi konuda destek" not in result["reply"].lower()


def test_human_request_wins_over_existing_sales_context():
    conversation = {
        "service": "Performans Pazarlama",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("İnsanla görüşebilir miyim?", {}, [], conversation, [])

    assert result["kind"] == "human_handoff"
    assert result["handoff"] is True
    assert result["next_state"] == "human_handoff"


def test_unclear_non_booking_message_uses_ai_generic_path_not_sales_fallback():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Bu sistem bizim ajansa uyar mi?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    assert result["next_state"] == "collect_service"
    assert "hangi konuda destek" not in result["reply"].lower()
    assert "biraz a" not in result["reply"].lower()
    assert "telefon numaran" not in result["reply"].lower()
    assert "müşteri" in result["reply"].lower()
    assert "mÃ" not in result["reply"]
    assert "Ä" not in result["reply"]
    assert "Å" not in result["reply"]


def test_generic_ai_answers_how_it_works_in_clean_turkish():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Nasıl çalışıyor?", {}, [], conversation, [])

    assert result["kind"] == "faq"
    assert "ihtiyaç analizi" in result["reply"].lower()
    assert "telefon numaran" not in result["reply"].lower()
    assert "mÃ" not in result["reply"]
    assert "Ä" not in result["reply"]
    assert "Å" not in result["reply"]


def test_price_question_without_service_uses_clean_turkish():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Fiyat nedir?", {}, [], conversation, [])

    assert result["kind"] == "price_route"
    reply = result["reply"].lower()
    assert "seçilecek hizmete göre değişiyor" in reply
    assert "secilecek" not in reply
    assert "gore" not in reply
    assert "tasarim" not in reply


def test_website_delivery_time_question_is_not_answered_with_price():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("tahmini teslim suresi ne kadar websitesinde", {}, [], conversation, [])

    assert result["kind"] == "delivery_time"
    reply = result["reply"].lower()
    assert "teslim" in reply
    assert "iş günü" in reply or "is gunu" in reply
    assert "12.900" not in reply
    assert "fiyat" not in reply
    assert "telefon" not in reply


def test_website_how_many_days_question_is_not_message_volume():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Web sitesi kac gunde teslim olur?", {}, [], conversation, [])

    assert result["kind"] == "delivery_time"
    reply = result["reply"].lower()
    assert "teslim" in reply
    assert "12.900" not in reply
    assert "yoğunluk" not in reply
    assert "yogunluk" not in reply


def test_automation_delivery_time_gets_automation_specific_answer():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    matched = main.match_service_candidates("Otomasyon teslim süresi ne kadar?", None)
    result = main.maybe_build_information_reply("Otomasyon teslim süresi ne kadar?", {}, matched, conversation, [])

    assert result["kind"] == "delivery_time"
    assert result["set_service"] == "Otomasyon & Yapay Zeka Çözümleri"
    reply = result["reply"].lower()
    assert "otomasyon" in reply
    assert "web sitesi gibi" not in reply
    assert "3-7" in reply or "1-3" in reply


def test_delivery_duration_followup_is_not_treated_as_booking_date():
    message = "4 haftaya ciktigi oluyor mu?"
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    assert main.extract_date(message) is None
    matched = main.match_service_candidates(message, conversation.get("service"))
    result = main.maybe_build_information_reply(message, {}, matched, conversation, [])

    assert result["kind"] == "delivery_time"
    assert result["next_state"] == "collect_service"
    reply = result["reply"].lower()
    assert "otomasyon" in reply
    assert "4 hafta" in reply
    assert "olabilir" in reply or "cikabilir" in reply or "çıkabilir" in reply
    assert "ad soyad" not in reply
    assert "adınızı" not in reply
    assert "adinizi" not in reply
    assert "soyad" not in reply
    assert "05.05" not in reply


def test_active_booking_state_does_not_override_customer_questions():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "requested_date": "2026-05-05",
        "memory_state": {},
    }

    for message in ["Merhaba", "nasilsiniz", "Dolandirici misiniz?", "Bu guvenilir mi?", "Once bilgi verir misiniz?", "Fiyat neydi?"]:
        assert not main.should_enter_booking_collection(
            message,
            {},
            asks_availability=False,
            detected_phone=None,
            detected_date=None,
            detected_time=None,
            conversation=conversation,
            history=[],
        )


def test_active_booking_state_greeting_gets_greeting_instead_of_collect_prompt():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "requested_date": "2026-05-05",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Merhaba", {}, [], conversation, [])

    assert result["kind"] in {"greeting", "greeting_interrupt"}
    reply = result["reply"].lower()
    assert "merhaba" in reply
    assert "ad soyad" not in reply
    assert "soyad" not in reply


def test_salamun_aleykum_gets_direct_greeting_not_generic_fallback():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Salamun aleykum", {}, [], conversation, [])

    assert result["kind"] == "greeting"
    reply = result["reply"].lower()
    assert "aleyk\u00fcm selam" in reply or "aleykum selam" in reply
    assert "mesaj\u0131n\u0131z\u0131 de\u011ferlendirip" not in reply
    assert "vermeye \u00e7al\u0131\u015faca\u011f\u0131m" not in reply


def test_swearing_reaction_to_bad_reply_gets_complaint_recovery():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Bu ne yarragim?", {}, [], conversation, [])

    assert result["kind"] == "complaint"
    reply = result["reply"].lower()
    assert "kusura" in reply or "\u00f6z\u00fcr" in reply
    assert "mesaj\u0131n\u0131z\u0131 de\u011ferlendirip" not in reply
    assert "telefon numaran" not in reply


def test_scam_or_trust_question_gets_answer_instead_of_collect_name_prompt():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "requested_date": "2026-05-05",
        "memory_state": {},
    }

    matched = main.match_service_candidates("Dolandirici misiniz?", conversation.get("service"))
    result = main.maybe_build_information_reply("Dolandirici misiniz?", {}, matched, conversation, [])

    assert result["kind"] in {"trust_question", "generic_ai"}
    reply = result["reply"].lower()
    assert "dolandirici" in reply or "dolandırıcı" in reply or "güven" in reply or "guven" in reply
    assert "ad soyad" not in reply
    assert "adınızı" not in reply
    assert "adinizi" not in reply
    assert "soyad" not in reply
    assert "05.05" not in reply


def test_numeric_date_in_booking_request_is_not_treated_as_time():
    message = "Otomasyon icin 05.05.2026 on gorusme yapalim"

    assert main.extract_date(message) == "2026-05-05"
    assert main.extract_time_for_state(message, "collect_service") is None


def test_ai_compose_is_enabled_for_business_reply_labels():
    labels = [
        "info:price_question",
        "info:price_route",
        "info:price_followup",
        "info:message_volume",
        "info:delivery_time",
        "collect_name",
        "collect_name_invalid",
        "human_handoff",
    ]

    for label in labels:
        assert main.should_ai_compose_reply("reply", label, conversation={"state": "collect_service"})


def test_llm_extractor_runs_for_real_customer_messages(monkeypatch):
    monkeypatch.setattr(main, "LLM_BASE_URL", "https://api.groq.com/openai/v1")
    monkeypatch.setattr(main, "LLM_API_KEY", "test-key")
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }
    messages = [
        "Merhaba",
        "Hizmetlerinizin fiyatları nedir?",
        "Otomasyon teslim süresi ne kadar?",
        "Toplantı yapalım",
        "G",
    ]

    for message in messages:
        assert main.should_call_llm_extractor(message, conversation)


def test_service_correction_overrides_active_booking_service():
    conversation = {
        "service": "Web Tasarım - KOBİ Paketi",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    applied = main.apply_detected_service_to_conversation(conversation, "Yok otomasyon için yapalım", None)

    assert applied == "Otomasyon & Yapay Zeka Çözümleri"
    assert conversation["service"] == "Otomasyon & Yapay Zeka Çözümleri"
    assert conversation["state"] == "collect_name"
    assert conversation["booking_kind"] == "preconsultation"


def test_service_correction_variants_use_current_message_over_old_service():
    cases = [
        ("Web değil otomasyon için", "Otomasyon & Yapay Zeka Çözümleri"),
        ("Hayır reklam için yapalım", "Performans Pazarlama"),
        ("Sosyal medya için görüşelim", "Sosyal Medya Yönetimi"),
    ]

    for message, expected_service in cases:
        conversation = {
            "service": "Web Tasarım - KOBİ Paketi",
            "state": "collect_name",
            "booking_kind": "preconsultation",
            "memory_state": {},
        }

        applied = main.apply_detected_service_to_conversation(conversation, message, None)

        assert applied == expected_service
        assert conversation["service"] == expected_service


def test_same_service_restatement_gets_non_repeated_collect_name_reply():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    assert main.is_same_service_restatement(conversation, "Otomasyon & Yapay Zeka Çözümleri", "Yok otomasyon için yapalım")
    reply = main.build_collect_name_request_reply(
        conversation,
        "ön görüşme",
        "Not aldım; Otomasyon & Yapay Zeka Çözümleri için. ",
        same_service_restatement=True,
    )

    assert reply.startswith("Tamam, Otomasyon")
    assert "adınızı ve soyadınızı" in reply
    assert "Not aldım;" not in reply


def test_single_letter_is_not_accepted_as_full_name():
    assert main.extract_name("G", "collect_name") is None
    assert main.extract_name("a", "collect_name") is None
    assert main.extract_name(".", "collect_name") is None
    assert main.is_invalid_name_attempt("G", "collect_name")
    assert main.is_invalid_name_attempt("a", "collect_name")
    assert main.is_invalid_name_attempt(".", "collect_name")


def test_business_fit_question_after_price_context_is_not_price_followup():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {
            "last_outbound_act": "answered_price",
            "price_context_open": True,
        },
    }
    history = [
        {
            "direction": "out",
            "message_text": "Net fiyat, seçilecek hizmete göre değişiyor.",
        }
    ]

    result = main.maybe_build_information_reply("Bu sistem bizim ajansa uyar mi?", {}, [], conversation, history)

    assert result["kind"] == "generic_ai"
    assert "fiyat" not in result["reply"].lower()
    assert "müşteri takibi" in result["reply"].lower()


def test_short_agency_fit_question_is_not_availability_request():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Ajans icin uygun mu?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    assert "hangi konuda destek" not in result["reply"].lower()
    assert "biraz açar" not in result["reply"].lower()
    assert "müşteri takibi" in result["reply"].lower()


def test_agency_fit_question_answers_even_in_old_collection_state():
    conversation = {
        "service": None,
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Ajans icin uygun mu?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    assert "hangi konuda destek" not in result["reply"].lower()
    assert "telefon numaran" not in result["reply"].lower()


def test_agency_fit_question_is_not_booking_transition():
    message = "Ajans icin uygun mu?"
    conversation = {
        "service": None,
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    assert main.is_business_fit_question(message)
    assert not main.should_enter_booking_collection(
        message,
        {},
        asks_availability=False,
        detected_phone=None,
        detected_date=None,
        detected_time=None,
        conversation=conversation,
        history=[],
    )


def test_human_identity_question_does_not_trigger_handoff():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Insanla mi gorusuyorum?", {}, [], conversation, [])

    assert result["kind"] == "assistant_identity"
    assert result.get("handoff") is not True
    assert result["next_state"] != "human_handoff"
    assert "telefon numaran" not in result["reply"].lower()
    assert "destek" in result["reply"].lower() or "asistan" in result["reply"].lower()


def test_explicit_human_handoff_request_still_handoffs():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Insanla gorusmek istiyorum", {}, [], conversation, [])

    assert result["kind"] == "human_handoff"
    assert result["handoff"] is True
    assert result["next_state"] == "human_handoff"


def test_all_choice_after_general_price_question_lists_all_prices():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {
            "last_outbound_act": "answered_price",
            "price_context_open": True,
        },
    }
    history = [
        {
            "direction": "out",
            "message_text": "Net fiyat, seçilecek hizmete göre değişiyor. Web tasarım, otomasyon & yapay zeka, performans pazarlama veya sosyal medya yönetiminden hangisiyle ilgilendiğinizi yazarsanız size doğru bilgiyi paylaşayım.",
        }
    ]

    result = main.maybe_build_information_reply("Hepsini merak ediyorum", {}, [], conversation, history)

    assert result["kind"] == "price_all_services"
    reply = result["reply"].lower()
    assert "web tasarım" in reply
    assert "12.900" in reply
    assert "otomasyon" in reply
    assert "5.000" in reply
    assert "performans pazarlama" in reply
    assert "7.500" in reply
    assert "hangi hizmet" not in reply


def test_dm_volume_message_overrides_prior_web_context():
    conversation = {
        "service": "Web Tasarım - KOBİ Paketi",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Çok DM geliyor", {}, [], conversation, [])

    assert result["kind"] == "message_volume"
    assert result["set_service"] == "Otomasyon & Yapay Zeka Çözümleri"
    assert "web tasarım" not in result["reply"].lower()
    assert "dm" in result["reply"].lower() or "mesaj" in result["reply"].lower()


def test_question_in_phone_collection_is_answered_before_phone_pressure():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Çözümleri",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Bu sistem güvenli mi?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    assert "telefon numaran" not in result["reply"].lower()
    assert result["reply"].strip()


def test_generic_security_question_gets_direct_answer_not_service_picker():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Bu sistem guvenli mi?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    reply = result["reply"].lower()
    assert "güven" in reply or "guven" in reply
    assert "hangisini geliştirmek" not in reply


def test_security_question_after_price_overview_is_answered_directly():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {
            "last_outbound_act": "answered_price",
            "price_context_open": True,
        },
    }
    history = [
        {
            "direction": "out",
            "message_text": "Tabii, ana hizmet fiyatları şöyle: Web Tasarım - KOBİ Paketi: 12.900 TL; Otomasyon & Yapay Zeka Çözümleri: 5.000 TL; Performans Pazarlama: 7.500 TL.",
        }
    ]

    result = main.maybe_build_information_reply("Bu sistem guvenli mi?", {}, [], conversation, history)

    assert result["kind"] == "generic_ai"
    reply = result["reply"].lower()
    assert "riskleri" in reply
    assert "telefon" not in reply
    assert "web tasar" not in reply


def test_answerable_offtopic_question_gets_answer_not_service_picker():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    result = main.maybe_build_information_reply("Dunyanin baskenti neresi?", {}, [], conversation, [])

    assert result["kind"] == "generic_ai"
    reply = result["reply"].lower()
    assert "başkenti" in reply or "baskenti" in reply
    assert "hangisini geliştirmek" not in reply


def test_version_exposes_ai_first_reply_engine_flags():
    payload = main.version()

    assert payload["reply_engine"] == "ai_first_v5"
    assert payload["ai_first_enabled"] is True
    assert payload["reply_guarantee_enabled"] is True


def test_ai_first_decision_parses_schema_and_guarantees_reply(monkeypatch):
    def fake_call_llm_content(*args, **kwargs):
        return (
            '{"reply_text":"Otomasyon teslimi standart kurulumlarda genelde 3-7 is gunu, '
            'ozel entegrasyonlarda 1-3 hafta surer.","intent":"delivery_time",'
            '"should_reply":true,"booking_intent":false,"extracted_service":"Otomasyon & Yapay Zeka Cozumleri",'
            '"extracted_name":null,"extracted_phone":null,"requested_date":null,"requested_time":null,'
            '"missing_fields":[],"crm_action":"update_customer","handoff_needed":false}'
        )

    monkeypatch.setattr(main, "call_llm_content", fake_call_llm_content)

    decision = main.build_ai_first_decision(
        "Otomasyon teslim suresi ne kadar?",
        {"state": "collect_service", "memory_state": {}},
        [],
        {},
    )

    assert decision["should_reply"] is True
    assert decision["reply_text"].strip()
    assert decision["intent"] == "delivery_time"
    assert decision["booking_intent"] is False
    assert decision["fallback_used"] is False


def test_ai_first_decision_fallback_still_returns_reply(monkeypatch):
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: None)

    decision = main.build_ai_first_decision(
        "Merhaba",
        {"state": "collect_service", "memory_state": {}},
        [],
        {},
    )

    assert decision["should_reply"] is True
    assert decision["reply_text"].strip()
    assert decision["fallback_used"] is True


def test_ai_first_decision_accepts_unstructured_ai_reply(monkeypatch):
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: "Otomasyon teslimi genelde 3-7 is gunu surer; kapsam buyurse 1-3 haftaya cikabilir.",
    )

    decision = main.build_ai_first_decision(
        "Otomasyon teslim suresi ne kadar?",
        {"state": "collect_service", "memory_state": {}},
        [],
        {},
    )

    assert decision["should_reply"] is True
    assert "3-7" in decision["reply_text"]
    assert decision["intent"] == "ai_unstructured_reply"
    assert decision["fallback_used"] is False


def test_ai_first_question_interrupts_active_booking_collection():
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "requested_date": "2026-05-05",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Hayir, dolandirici degiliz; sureci seffaf sekilde anlatarak ilerliyoruz.",
        "intent": "trust_question",
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

    main.apply_ai_first_decision_to_conversation(conversation, decision, "Dolandirici misiniz?")

    assert conversation["state"] == "collect_service"
    assert conversation["last_customer_message"] == "Dolandirici misiniz?"


def test_ai_first_booking_intent_sets_next_missing_field():
    conversation = {
        "service": None,
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }
    decision = {
        "reply_text": "Tabii, otomasyon icin on gorusme planlayabiliriz. Once ad soyadinizi alayim.",
        "intent": "booking_request",
        "should_reply": True,
        "booking_intent": True,
        "extracted_service": "Otomasyon & Yapay Zeka Cozumleri",
        "extracted_name": None,
        "extracted_phone": None,
        "requested_date": None,
        "requested_time": None,
        "missing_fields": ["full_name", "phone", "requested_date", "requested_time"],
        "crm_action": "update_customer",
        "handoff_needed": False,
    }

    main.apply_ai_first_decision_to_conversation(conversation, decision, "Otomasyon icin toplanti yapalim")

    assert main.display_service_name(conversation["service"]) == "Otomasyon & Yapay Zeka \u00c7\u00f6z\u00fcmleri"
    assert conversation["booking_kind"] == "preconsultation"
    assert conversation["state"] == "collect_name"


def test_ai_first_booking_reply_asks_name_before_date_or_phone():
    decision = {
        "reply_text": "Elbette, toplantı için hangi tarih ve saatler size uygun?",
        "intent": "booking",
        "should_reply": True,
        "booking_intent": True,
        "extracted_service": "Otomasyon & Yapay Zeka Cozumleri",
        "extracted_name": None,
        "extracted_phone": None,
        "requested_date": None,
        "requested_time": None,
        "missing_fields": ["requested_date", "requested_time"],
        "crm_action": "update_customer",
        "handoff_needed": False,
    }
    conversation = {"state": "collect_service", "service": None, "memory_state": {}}

    enforced = main.enforce_ai_first_booking_order(decision, conversation, "Toplanti yapalim")

    reply = enforced["reply_text"].lower()
    assert "ad" in reply
    assert "soyad" in reply
    assert "hangi tarih" not in reply
    assert "telefon" not in reply


def test_ai_first_delivery_followup_suppresses_booking_collection():
    decision = {
        "reply_text": "4 hafta bazı kapsamlı entegrasyonlarda mümkün olabilir.",
        "intent": "info",
        "should_reply": True,
        "booking_intent": True,
        "extracted_service": "Otomasyon & Yapay Zeka Cozumleri",
        "extracted_name": None,
        "extracted_phone": None,
        "requested_date": None,
        "requested_time": None,
        "missing_fields": ["full_name"],
        "crm_action": "update_customer",
        "handoff_needed": False,
    }
    conversation = {
        "state": "collect_service",
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "memory_state": {},
    }

    assert main.should_suppress_ai_booking_collection(
        "4 haftaya ciktigi oluyor mu?",
        decision,
        conversation,
        {},
    )


def test_ai_first_booking_after_phone_suggests_crm_slots(monkeypatch):
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }

    def fake_slots(_conn, date_value, _service_name=None):
        return {
            "2026-04-30": ["12:00", "14:00"],
            "2026-05-01": ["11:00"],
        }.get(date_value, [])

    monkeypatch.setattr(main, "get_available_booking_slots_for_date", fake_slots)

    result = main.prepare_ai_first_booking_availability(
        object(),
        conversation,
        detected_date=None,
        detected_time=None,
        start_date_value="2026-04-29",
    )

    reply = result["reply_text"].lower()
    assert "uygun ilk" in reply
    assert "12:00" in reply
    assert "hangi gun" not in reply
    assert "hangi saat" not in reply
    assert conversation["state"] == "collect_time"
    assert conversation["memory_state"]["suggested_booking_slots"][0] == {
        "date": "2026-04-30",
        "time": "12:00",
    }


def test_booking_slot_suggestions_use_clean_hourly_times(monkeypatch):
    monkeypatch.setattr(main, "is_slot_capacity_available", lambda *_args, **_kwargs: True)

    slots = main.get_available_booking_slots_for_date(object(), "2026-04-30", "Web Tasarim - KOBI Paketi")

    assert slots[:4] == ["10:00", "11:00", "12:00", "13:00"]
    assert "11:10" not in slots
    assert "12:20" not in slots


def test_ai_first_time_only_after_unique_suggestion_selects_slot():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {
            "suggested_booking_slots": [
                {"date": "2026-04-30", "time": "12:00"},
                {"date": "2026-04-30", "time": "14:00"},
            ]
        },
    }

    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date=None,
        detected_time="12:00",
        start_date_value="2026-04-29",
    )

    assert result["ready_to_book"] is True
    assert conversation["requested_date"] == "2026-04-30"
    assert conversation["requested_time"] == "12:00"


def test_ai_first_time_only_after_ambiguous_suggestions_asks_date():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {
            "suggested_booking_slots": [
                {"date": "2026-04-30", "time": "12:00"},
                {"date": "2026-05-01", "time": "12:00"},
            ]
        },
    }

    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date=None,
        detected_time="12:00",
        start_date_value="2026-04-29",
    )

    reply = result["reply_text"].lower()
    assert result["ready_to_book"] is False
    assert "12:00" in reply
    assert "hangi g" in reply
    assert conversation["state"] == "collect_date"
    assert conversation["memory_state"]["pending_requested_time"] == "12:00"


def test_ai_first_date_after_pending_time_selects_slot():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_date",
        "booking_kind": "preconsultation",
        "requested_date": "2026-04-30",
        "memory_state": {"pending_requested_time": "12:00"},
    }

    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date="2026-04-30",
        detected_time=None,
        start_date_value="2026-04-29",
    )

    assert result["ready_to_book"] is True
    assert conversation["requested_date"] == "2026-04-30"
    assert conversation["requested_time"] == "12:00"


def test_collect_phone_ignores_llm_booking_datetime_from_phone_message():
    llm_data = {"requested_date": "2026-05-05", "requested_time": "12:00"}

    assert main.should_ignore_llm_booking_datetime_from_phone_message(
        "telefon numaram 05539088633",
        "collect_phone",
        "+905539088633",
        llm_data,
    )


def test_ai_first_name_collection_cannot_drop_booking_flow():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Hos geldiniz, nasil yardimci olabiliriz?",
        "intent": "greeting",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_name",
        extracted_name="Berkay Elbir",
        detected_phone=None,
    )

    assert decision["booking_intent"] is True
    assert "phone" in decision["missing_fields"]
    assert "telefon" in decision["reply_text"].lower()


def test_ai_first_phone_collection_cannot_drop_booking_flow():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Mesajinizi dikkate aliyorum.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_phone",
        extracted_name=None,
        detected_phone="+905539088633",
    )

    assert decision["booking_intent"] is True
    assert "requested_date" in decision["missing_fields"]
    assert "requested_time" in decision["missing_fields"]


def test_ai_first_time_collection_cannot_drop_booking_flow():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Mesajinizi dikkate aliyorum.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_time",
        extracted_name=None,
        detected_phone=None,
        detected_time="12:00",
    )

    assert decision["booking_intent"] is True
    assert "requested_time" in decision["missing_fields"]


def test_ai_first_time_collection_uses_existing_name_without_revalidating_words():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Full Akis Test Kullanici",
        "phone": "+905550000003",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Mesajinizi dikkate aliyorum.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_time",
        extracted_name=None,
        detected_phone=None,
        detected_time="11:00",
    )

    assert decision["booking_intent"] is True
    assert decision["intent"] == "booking_time_collected"


def test_ai_first_message_volume_direct_reply_overrides_fallback(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Gunde 30-40 kisi yaziyor", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["intent"] == "message_volume"
    assert decision["booking_intent"] is False
    assert "30-40" in reply
    assert "otomatik" in reply or "otomasyon" in reply


def test_ai_first_service_overview_overrides_generic_fallback(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum; neye ihtiyaciniz oldugunu yazarsaniz dogrudan cevap vereyim.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Hizmetleriniz hakkında detaylı bilgi almak istiyorum", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["intent"] == "detailed_service_overview"
    assert decision["booking_intent"] is False
    assert "web tasar" in reply
    assert "otomasyon" in reply
    assert "reklam" in reply
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply


def test_ai_first_good_wishes_greeting_does_not_answer_as_wellbeing(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="İyiyim, size nasıl yardımcı olabilirim?",
            intent="smalltalk",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Merhaba kolay gelsin", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["intent"] == "greeting"
    assert decision["booking_intent"] is False
    assert "teşekkür" in reply
    assert "iyiyim" not in reply


def test_ai_first_direct_ascii_preconsultation_starts_booking(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Web tasarim icin on gorusme planlayalim", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "web tasar" in decision["extracted_service"].lower()
    assert "ad" in reply and "soyad" in reply


def test_ai_first_ascii_preconsultation_question_gets_explanation(monkeypatch):
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Ne on gorusmesi?", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is False
    assert "görüşme" in reply or "gorusme" in reply
    assert "ad" not in reply or "soyad" not in reply


def test_ai_first_positive_acceptance_after_service_context_starts_consultation(monkeypatch):
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Evet, web tasarım hizmetimiz ile hedeflediğiniz sonucu elde edebilirsiniz.",
        }
    ]
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Olur peki", conversation, history, {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert "ad" in reply and "soyad" in reply
    assert "web tasar" in decision["extracted_service"].lower()


def test_ai_first_invalid_short_name_in_collect_name_does_not_fallback(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("G", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["booking_intent"] is True
    assert decision["intent"] == "collect_name_invalid"
    assert "ad" in reply and "soyad" in reply
    assert "tam olarak" in reply


def test_delivery_time_question_is_not_treated_as_message_volume():
    assert not main.is_message_volume_answer("Sistem kac gunde acilir?")
    assert main.is_delivery_time_question("Sistem kac gunde acilir?")


def test_ai_first_delivery_time_overrides_message_volume_fallback(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Bu seviye ciddi bir yogunluk.",
            intent="message_volume",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Sistem kac gunde acilir?", conversation, [], {})

    reply = decision["reply_text"].lower()
    assert decision["intent"] == "delivery_time"
    assert "3-7" in reply or "1-3" in reply
    assert "yoğunluk" not in reply


def test_slot_context_extracts_standalone_time_after_state_reset():
    conversation = {
        "state": "collect_service",
        "memory_state": {"suggested_booking_slots": [{"date": "2026-05-01", "time": "11:00"}]},
    }

    assert main.extract_time_from_slot_context("11.00", conversation) == "11:00"


def test_ai_first_time_collection_with_slot_memory_cannot_drop_when_state_reset():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Ali Yilmaz",
        "phone": "+905550000005",
        "state": "collect_service",
        "booking_kind": "preconsultation",
        "memory_state": {"suggested_booking_slots": [{"date": "2026-05-01", "time": "11:00"}]},
    }
    decision = {
        "reply_text": "Mesajinizi dikkate aliyorum.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_service",
        extracted_name=None,
        detected_phone=None,
        detected_time="11:00",
    )

    assert decision["booking_intent"] is True
    assert decision["intent"] == "booking_time_collected"


def test_ai_first_date_and_time_after_suggestions_is_ready_to_book():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {
            "suggested_booking_slots": [
                {"date": "2026-05-01", "time": "10:00"},
                {"date": "2026-05-01", "time": "11:00"},
                {"date": "2026-05-01", "time": "12:00"},
                {"date": "2026-05-01", "time": "13:00"},
            ]
        },
    }

    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date="2026-05-01",
        detected_time="12:00",
        start_date_value="2026-04-30",
    )

    assert result["ready_to_book"] is True
    assert result["reply_text"] is None
    assert result["slot_resolution"] == "ready_to_book"
    assert conversation["requested_date"] == "2026-05-01"
    assert conversation["requested_time"] == "12:00"


def test_time_extraction_handles_bare_hour_with_date_cue():
    assert main.extract_time_for_state("yarın 12", "collect_time") == "12:00"
    assert main.extract_time_for_state("yarın saat 12", "collect_time") == "12:00"
    assert main.extract_time_for_state("yarın 12:00", "collect_time") == "12:00"
    assert main.extract_time_for_state("01.05.2026 12", "collect_time") == "12:00"


def test_ai_first_booking_progress_override_handles_date_and_time_when_ai_says_false():
    message = "Yarın 12.00?"
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": "+905539088633",
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {
            "suggested_booking_slots": [
                {"date": "2026-05-01", "time": "10:00"},
                {"date": "2026-05-01", "time": "11:00"},
                {"date": "2026-05-01", "time": "12:00"},
                {"date": "2026-05-01", "time": "13:00"},
            ]
        },
    }
    decision = {
        "reply_text": "",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.force_ai_first_booking_continuation(
        decision,
        conversation,
        state_before_update="collect_time",
        extracted_name=None,
        detected_phone=None,
        detected_date=main.extract_date(message),
        detected_time=main.extract_time_for_state(message, "collect_time"),
    )
    main.apply_ai_first_decision_to_conversation(conversation, decision, message)
    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date=main.extract_date(message),
        detected_time=main.extract_time_for_state(message, "collect_time"),
        start_date_value="2026-04-30",
    )

    assert decision["booking_intent"] is True
    assert decision["booking_progress_override"] is True
    assert result["ready_to_book"] is True
    assert conversation["requested_date"] == "2026-05-01"
    assert conversation["requested_time"] == "12:00"


def test_ai_first_nonbooking_question_keeps_resumeable_booking_state():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Telefon şart değil; buradan bilgi vermeye devam edebiliriz.",
        "intent": "phone_refusal",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    main.apply_ai_first_decision_to_conversation(conversation, decision, "şart mı?")

    assert conversation["state"] == "collect_phone"
    assert conversation["memory_state"]["open_loop"] == "collect_phone"


def test_collect_phone_hesitation_overrides_weak_ai_reply():
    assert main.is_phone_collection_hesitation("sart mi?")
    assert main.is_phone_collection_hesitation(chr(351) + "art m" + chr(305) + "?")
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Telefon numaranizi paylasir misiniz?",
        "intent": "collect_phone",
        "should_reply": True,
        "booking_intent": True,
        "missing_fields": ["phone"],
    }

    changed = main.override_ai_first_collect_phone_question(
        decision,
        conversation,
        "sart mi?",
        state_before_update="collect_phone",
        detected_phone=None,
    )

    assert changed is True
    assert decision["intent"] == "phone_reason"
    assert decision["booking_intent"] is False
    assert "Telefonu sadece" in decision["reply_text"]


def test_reply_guarantee_returns_emergency_text_for_empty_ai_reply():
    conversation = {"state": "new", "memory_state": {}}

    reply, recovered = main.guarantee_nonempty_reply_text(
        "",
        "Hizmetleriniz hakkında bilgi almak istiyorum",
        conversation,
        "ai_first_v2:fallback_reply",
    )

    assert recovered is True
    assert reply
    assert "web" in reply.lower() or "hizmet" in reply.lower()


def test_reply_engine_reports_ai_first_v5():
    assert main.REPLY_ENGINE == "ai_first_v5"


def test_ai_first_v5_web_emergency_reply_stays_web_context():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }

    reply = main.build_ai_first_emergency_reply("Dijitalde gorunur olmak", conversation)
    normalized = main.sanitize_text(reply).lower()

    assert "web" in normalized or "site" in normalized
    assert "sureci toparlamak" not in normalized
    assert "dm" not in normalized


def test_ai_first_v5_instagram_profile_is_not_accepted_as_full_name():
    assert main.extract_name("Yaziyor Instagram hesabimda", "collect_name") is None
    assert main.is_invalid_name_attempt("Yaziyor Instagram hesabimda", "collect_name")


def test_ai_first_v5_phone_refusal_accepts_instagram_contact_without_pressure():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "state": "collect_phone",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    decision = {
        "reply_text": "Telefon numaranizi paylasir misiniz?",
        "intent": "collect_phone",
        "should_reply": True,
        "booking_intent": True,
        "missing_fields": ["phone"],
    }

    changed = main.override_ai_first_collect_phone_question(
        decision,
        conversation,
        "Paylasamam boyle kaydet",
        state_before_update="collect_phone",
        detected_phone=None,
    )

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert changed is True
    assert decision["intent"] == "phone_refusal"
    assert decision["booking_intent"] is True
    assert decision["missing_fields"] == []
    assert conversation["memory_state"]["contact_channel"] == "instagram_dm"
    assert "zorunda" in reply
    assert "paylasmazsaniz" not in reply
    assert "planlayamayiz" not in reply


def test_ai_first_v5_booking_availability_allows_instagram_contact_without_phone():
    conversation = {
        "service": "Web Tasarim - KOBI Paketi",
        "full_name": "Berkay Elbir",
        "phone": None,
        "state": "collect_time",
        "booking_kind": "preconsultation",
        "memory_state": {"contact_channel": "instagram_dm"},
    }

    result = main.prepare_ai_first_booking_availability(
        None,
        conversation,
        detected_date="2026-05-01",
        detected_time="12:00",
        start_date_value="2026-04-30",
    )

    assert result["ready_to_book"] is True
    assert conversation["requested_date"] == "2026-05-01"
    assert conversation["requested_time"] == "12:00"


def test_ai_first_specific_automation_info_request_overrides_generic_fallback(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum; neye ihtiyaciniz oldugunu yazarsaniz dogrudan cevap vereyim.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Otomasyon hakkinda bilgi verin", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    raw_reply = decision["reply_text"].lower()
    assert decision["should_reply"] is True
    assert decision["booking_intent"] is False
    assert decision["intent"] == "service_info"
    assert "otomasyon" in reply
    assert "dm" in reply or "mesaj" in reply
    assert "müşteri" in raw_reply
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply


def test_ai_first_generic_information_request_gets_service_overview_not_fallback(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum; neye ihtiyaciniz oldugunu yazarsaniz dogrudan cevap vereyim.",
            intent="fallback_reply",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Bilgi edinmek icin yaziyorum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert decision["booking_intent"] is False
    assert decision["intent"] in {"service_overview", "detailed_service_overview"}
    assert "web" in reply
    assert "otomasyon" in reply
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply


def test_ai_first_generic_information_request_replaces_which_topic_question(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Merhabalar, DOEL Digital olarak size nasıl yardımcı olabiliriz? Hangi konuda bilgi edinmek istiyorsunuz?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Bilgi edinmek icin yaziyorum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert decision["intent"] in {"service_overview", "detailed_service_overview"}
    assert "web" in reply
    assert "otomasyon" in reply
    assert "hangi konuda bilgi" not in reply


def test_ai_first_generic_information_request_replaces_empty_helper_question(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Merhaba, DOEL Digital'dan hoş geldiniz. Size nasıl yardımcı olabilirim?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("Bilgi edinmek icin yaziyorum", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert decision["intent"] in {"service_overview", "detailed_service_overview"}
    assert "web" in reply
    assert "otomasyon" in reply
    assert "nasıl yardımcı" not in reply


def test_ai_first_service_info_then_positive_continue_starts_consultation(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "new",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": (
                "Otomasyon sistemi gelen DM'leri karsilar, sik sorulari yanitlar, uygun talepleri "
                "randevu veya CRM kaydina cevirir. Isterseniz kisa bir on gorusmede netlestirebiliriz."
            ),
        }
    ]
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Otomasyon & Yapay Zeka Cozumlerimizle ilgili daha fazla bilgi almak ister misiniz?",
            intent="info",
            booking_intent=False,
            missing_fields=[],
        ),
    )

    decision = main.build_ai_first_decision("evet", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert decision["booking_intent"] is True
    assert decision["intent"] == "service_consultation_acceptance"
    assert "ad" in reply and "soyad" in reply
    assert "daha fazla bilgi almak ister" not in reply


def test_ai_first_collect_name_low_signal_followup_does_not_repeat_booking_prompt(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_name",
        "booking_kind": "preconsultation",
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Tabii, Otomasyon & Yapay Zeka Çözümleri için ön görüşme planlayabiliriz. Önce adınızı ve soyadınızı yazar mısınız?",
        }
    ]
    monkeypatch.setattr(
        main,
        "call_llm_content",
        lambda *args, **kwargs: _ai_json(
            reply_text="Tabii, Otomasyon & Yapay Zeka Cozumleri için ön görüşme planlayabiliriz. Önce adınızı ve soyadınızı yazar mısınız?",
            intent="service_consultation_acceptance",
            booking_intent=True,
            missing_fields=["name"],
        ),
    )

    decision = main.build_ai_first_decision("eee?", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert decision["intent"] == "collect_name_invalid"
    assert "ad" in reply and "soyad" in reply
    assert "planlayabiliriz" not in reply


def test_ai_first_reply_guarantee_replaces_low_quality_generic_fallback():
    conversation = {"state": "new", "memory_state": {}}
    decision = {
        "reply_text": "Anladim. Size yardimci olabilmem icin mesajinizi dikkate aliyorum; neye ihtiyaciniz oldugunu yazarsaniz dogrudan cevap vereyim.",
        "intent": "fallback_reply",
        "should_reply": True,
        "booking_intent": False,
        "missing_fields": [],
    }

    fixed = main.apply_ai_first_quality_overrides(
        "Hizmetleriniz hakkinda detayli bilgi almak istiyorum",
        decision,
        conversation,
        [],
    )

    reply = main.sanitize_text(fixed["reply_text"]).lower()
    assert fixed["should_reply"] is True
    assert fixed["booking_intent"] is False
    assert "web" in reply
    assert "otomasyon" in reply
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply


def test_ai_first_reasks_ai_when_positive_followup_gets_generic_fallback(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Otomasyon, mesajların otomatik cevaplanmasını, randevuların planlanmasını ve müşteri takibini kolaylaştırır.",
        }
    ]
    responses = iter(
        [
            _ai_json(
                reply_text="Anladım. Size yardımcı olabilmem için mesajınızı dikkate alıyorum; neye ihtiyacınız olduğunu yazarsanız doğrudan cevap vereyim.",
                intent="info",
                booking_intent=False,
                missing_fields=[],
            ),
            _ai_json(
                reply_text="Evet, bu yapı özellikle yoğun DM alan işletmelerde ciddi zaman kazandırır. İsterseniz günlük mesaj akışınıza göre size uygun kurulumu netleştirelim.",
                intent="positive_followup",
                booking_intent=False,
                extracted_service="Otomasyon & Yapay Zeka Cozumleri",
                missing_fields=[],
            ),
        ]
    )
    calls = []

    def fake_llm(*args, **kwargs):
        calls.append(kwargs)
        return next(responses)

    monkeypatch.setattr(main, "call_llm_content", fake_llm)

    decision = main.build_ai_first_decision("Güzelmiş", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert len(calls) == 2
    assert decision["should_reply"] is True
    assert decision["intent"] == "positive_followup"
    assert decision["ai_repair_used"] is True
    assert "zaman" in reply or "kurulum" in reply
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply


def test_ai_first_reasks_ai_for_service_advice_when_reply_is_not_consultative(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    responses = iter(
        [
            _ai_json(
                reply_text="Hangi hizmete ihtiyacınız olduğunu yazarsanız fiyat, kapsam ve teslim süresini anlatacağım.",
                intent="info",
                booking_intent=False,
                missing_fields=[],
            ),
            _ai_json(
                reply_text="Önce en büyük ihtiyacınızı netleştirelim: daha çok müşteri kazanmak mı, DM yoğunluğunu azaltmak mı, yoksa güven veren bir web sitesi kurmak mı istiyorsunuz?",
                intent="service_advice",
                booking_intent=False,
                missing_fields=[],
            ),
        ]
    )
    calls = []

    def fake_llm(*args, **kwargs):
        calls.append(kwargs)
        return next(responses)

    monkeypatch.setattr(main, "call_llm_content", fake_llm)

    decision = main.build_ai_first_decision("Bana hangisi lazim bilmiyorum yardimci olur musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert len(calls) == 2
    assert decision["intent"] == "service_advice"
    assert decision["ai_repair_used"] is True
    assert "dm" in reply or "web" in reply or "musteri" in reply
    assert "hangi hizmete ihtiyaciniz" not in reply


def test_ai_first_service_benefit_question_has_useful_emergency_when_repair_fails(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    responses = iter(
        [
            _ai_json(
                reply_text="Anladım. Size yardımcı olabilmem için mesajınızı dikkate alıyorum; neye ihtiyacınız olduğunu yazarsanız doğrudan cevap vereyim.",
                intent="fallback_reply",
                booking_intent=False,
                missing_fields=[],
            ),
            "",
        ]
    )
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: next(responses))

    decision = main.build_ai_first_decision("Otomasyon ne isime yarayacak", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "mesajinizi dikkate" not in reply
    assert "otomasyon" in reply
    assert "mesaj" in reply or "randevu" in reply or "musteri" in reply


def test_ai_first_service_choice_help_has_useful_emergency_when_repair_fails(monkeypatch):
    conversation = {"service": None, "state": "new", "booking_kind": None, "memory_state": {}}
    responses = iter(
        [
            _ai_json(
                reply_text="Sorunuzu doğrudan cevaplayayım; bildiğim kısmı net aktarırım, emin olmadığım yerde de uydurmadan belirtirim.",
                intent="fallback_reply",
                booking_intent=False,
                missing_fields=[],
            ),
            "",
        ]
    )
    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: next(responses))

    decision = main.build_ai_first_decision("Bana hangisi lazim bilmiyorum yardimci olur musunuz?", conversation, [], {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert "sorunuzu dogrudan" not in reply
    assert "hangi hizmete ihtiyaciniz" not in reply
    assert "hedef" in reply or "dm" in reply or "web" in reply or "musteri" in reply


def test_ai_first_never_returns_generic_fallback_when_ai_repair_fails(monkeypatch):
    conversation = {
        "service": "Otomasyon & Yapay Zeka Cozumleri",
        "state": "collect_service",
        "booking_kind": None,
        "memory_state": {},
    }
    history = [
        {
            "direction": "out",
            "message_text": "Otomasyon, mesajların otomatik cevaplanmasını, randevuların planlanmasını ve müşteri takibini kolaylaştırır.",
        }
    ]
    responses = iter(
        [
            _ai_json(
                reply_text="Anladım. Size yardımcı olabilmem için mesajınızı dikkate alıyorum; neye ihtiyacınız olduğunu yazarsanız doğrudan cevap vereyim.",
                intent="fallback_reply",
                booking_intent=False,
                missing_fields=[],
            ),
            "",
        ]
    )

    monkeypatch.setattr(main, "call_llm_content", lambda *args, **kwargs: next(responses))

    decision = main.build_ai_first_decision("Güzelmiş", conversation, history, {})

    reply = main.sanitize_text(decision["reply_text"]).lower()
    assert decision["should_reply"] is True
    assert "mesajinizi dikkate" not in reply
    assert "neye ihtiyaciniz" not in reply
    assert "otomasyon" in reply or "devam" in reply or "netlestirelim" in reply


def test_parse_json_like_handles_json_encoded_as_string():
    content = '"{\\"reply_text\\": \\"Tabii, randevu planlayabiliriz.\\", \\"intent\\": \\"appointment\\", \\"should_reply\\": true}"'

    parsed = main.parse_json_like(content)

    assert parsed["reply_text"].startswith("Tabii")
    assert parsed["intent"] == "appointment"
    assert parsed["should_reply"] is True
