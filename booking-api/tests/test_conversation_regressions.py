from app import main


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
