from app import main


def test_numeric_range_after_volume_question_is_not_treated_as_date():
    message = "30-40"
    history = [{"direction": "out", "message_text": "Gunde yaklasik kac mesaj geliyor?"}]
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
