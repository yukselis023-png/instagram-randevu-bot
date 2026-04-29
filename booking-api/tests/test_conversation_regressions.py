from app import main


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

    assert payload["reply_engine"] == "ai_first_v2"
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
