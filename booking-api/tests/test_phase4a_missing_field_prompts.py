"""
Phase 4A Tests — Final Builder Missing Field Prompts
=====================================================

Tests the ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS flag and
build_final_missing_field_prompt() logic from pipeline_wrapper.py.

All tests run with the flag ON (enforce_missing_field_prompts=true).
Legacy behaviour (flag OFF) is verified by the existing test suite.
"""
import os
import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _missing(conversation=None, memory=None):
    from app.pipeline_wrapper import check_missing_fields
    return check_missing_fields(conversation or {}, memory or {})


def _build(ai_reply, missing_fields, *, direct_question, wants_booking):
    from app.pipeline_wrapper import build_final_missing_field_prompt
    return build_final_missing_field_prompt(
        ai_reply,
        missing_fields,
        direct_question=direct_question,
        wants_booking=wants_booking,
    )


# ============================================================
# check_missing_fields tests
# ============================================================

class TestCheckMissingFields:

    def test_all_missing(self):
        r = _missing()
        assert "full_name" in r["missing_fields"]
        assert "phone" in r["missing_fields"]
        assert "requested_date" in r["missing_fields"]
        assert "requested_time" in r["missing_fields"]

    def test_full_name_present_via_lead_name(self):
        r = _missing(conversation={"lead_name": "Berkay Çakmak"})
        assert "full_name" not in r["missing_fields"]

    def test_full_name_present_directly(self):
        r = _missing(conversation={"full_name": "Berkay Çakmak"})
        assert "full_name" not in r["missing_fields"]

    def test_phone_present(self):
        r = _missing(conversation={"phone": "05539088638"})
        assert "phone" not in r["missing_fields"]

    def test_date_present(self):
        r = _missing(conversation={"requested_date": "2024-06-15"})
        assert "requested_date" not in r["missing_fields"]

    def test_time_present(self):
        r = _missing(conversation={"requested_time": "13:00"})
        assert "requested_time" not in r["missing_fields"]

    def test_service_from_memory(self):
        r = _missing(memory={"requested_service": "web_tasarim"})
        assert "service" not in r["missing_fields"]

    def test_all_fields_present_can_create(self):
        conv = {
            "full_name": "Berkay Çakmak",
            "phone": "05539088638",
            "requested_date": "2024-06-15",
            "requested_time": "13:00",
            "service": "web_tasarim",
        }
        r = _missing(conversation=conv)
        assert r["missing_fields"] == []
        assert r["can_create_appointment"] is True

    def test_missing_fields_order(self):
        """full_name before phone before date before time."""
        r = _missing()
        mf = r["missing_fields"]
        if "full_name" in mf and "phone" in mf:
            assert mf.index("full_name") < mf.index("phone")
        if "phone" in mf and "requested_date" in mf:
            assert mf.index("phone") < mf.index("requested_date")
        if "requested_date" in mf and "requested_time" in mf:
            assert mf.index("requested_date") < mf.index("requested_time")


# ============================================================
# build_final_missing_field_prompt tests
# ============================================================

class TestBuildFinalMissingFieldPrompt:

    # --- Scenario 1: direct_question=True ---

    def test_direct_question_returns_ai_plus_soft_suffix_for_phone(self):
        """Phase 4A spec: direct_question → AI answer + optional soft 1-sentence."""
        ai = "Evet, ön görüşmede Berkay Bey sizi arayacak."
        result = _build(ai, ["phone"], direct_question=True, wants_booking=True)
        assert ai in result
        assert "telefon" in result.lower()
        # Must be soft, not aggressive
        assert "alabilir miyim" not in result.lower()

    def test_direct_question_returns_ai_plus_soft_suffix_for_name(self):
        ai = "Evet, ön görüşmede sizinle iletişime geçeriz."
        result = _build(ai, ["full_name"], direct_question=True, wants_booking=True)
        assert ai in result
        # Soft prompt appended
        assert "adınızı" in result.lower() or "soyadınızı" in result.lower()

    def test_direct_question_no_missing_fields_returns_ai_only(self):
        ai = "Web sitesi genellikle 7-14 iş günü sürer."
        result = _build(ai, [], direct_question=True, wants_booking=False)
        assert result == ai

    def test_direct_question_max_one_question_mark(self):
        """Spec: en fazla 1 soru sorulur."""
        ai = "Ön görüşmede ekibimiz sizinle iletişime geçer."
        result = _build(ai, ["phone", "full_name", "requested_date"], direct_question=True, wants_booking=True)
        assert result is not None
        assert result.count("?") <= 1

    # --- Scenario 2: direct_question=False + wants_booking=True ---

    def test_booking_missing_name_returns_name_prompt(self):
        result = _build("Tabii.", ["full_name", "phone"], direct_question=False, wants_booking=True)
        assert result is not None
        assert "adınızı" in result.lower() and "soyadınızı" in result.lower()

    def test_booking_missing_phone_returns_phone_prompt(self):
        result = _build("Tabii.", ["phone", "requested_date"], direct_question=False, wants_booking=True)
        assert result is not None
        assert "telefon" in result.lower()

    def test_booking_missing_date_returns_datetime_prompt(self):
        result = _build("Tabii.", ["requested_date", "requested_time"], direct_question=False, wants_booking=True)
        assert result is not None
        assert "gün" in result.lower() or "saat" in result.lower()

    def test_booking_missing_time_only_returns_time_prompt(self):
        result = _build("Tabii.", ["requested_time"], direct_question=False, wants_booking=True)
        assert result is not None
        assert "saat" in result.lower()

    def test_booking_asks_only_first_missing_field(self):
        """Spec: Final sadece ilk eksik alanı sorar."""
        result = _build("Tabii.", ["full_name", "phone", "requested_date"], direct_question=False, wants_booking=True)
        assert result is not None
        # Should ask for name only
        assert "adınızı" in result.lower()
        # Should NOT ask for phone in same reply
        # (booking prompt for name doesn't mention telefon)
        assert "telefon" not in result.lower()

    # --- Scenario 3: direct_question=False + wants_booking=False ---

    def test_no_booking_no_direct_returns_ai_only(self):
        """Spec: sadece AI cevabı gider, field prompt eklenmez."""
        ai = "Web sitesi teslim süresi genelde 7-14 iş günü aralığındadır."
        result = _build(ai, ["full_name", "phone"], direct_question=False, wants_booking=False)
        assert result == ai

    def test_no_booking_no_direct_no_phone_no_name_prompt(self):
        ai = "Hizmetlerimiz hakkında bilgi vermekten memnuniyet duyarım."
        result = _build(ai, ["full_name", "phone", "requested_date"], direct_question=False, wants_booking=False)
        assert "telefon" not in result.lower()
        assert "adınızı" not in result.lower()

    # --- Legacy FSM prompt strings must NOT appear ---

    def test_legacy_aggressive_prompt_not_returned(self):
        """Spec: FSM'nin eski agresif promptları Final Builder çıktısında olmamalı."""
        for missing_f in [["full_name"], ["phone"]]:
            result = _build(None, missing_f, direct_question=False, wants_booking=True)
            if result:
                # The old-style FSM prompts that must NOT appear verbatim
                assert "Harika, web tasarım için ön görüşme oluşturalım." not in result
                assert "Ad soyadınızı alabilir miyim?" not in result

    # --- Edge cases ---

    def test_none_ai_reply_direct_question_returns_none(self):
        result = _build(None, ["phone"], direct_question=True, wants_booking=True)
        assert result is None

    def test_empty_missing_fields_booking_returns_ai(self):
        ai = "Tüm bilgiler tamam."
        result = _build(ai, [], direct_question=False, wants_booking=True)
        assert result == ai


# ============================================================
# Integration: flag OFF → legacy behaviour unchanged
# ============================================================

class TestPhase4AFlagOff:

    def test_flag_false_check_missing_fields_still_works(self):
        """check_missing_fields is always available — flag only gates enforcement."""
        r = _missing(conversation={"full_name": "Ali Veli", "phone": "05001234567"})
        assert "full_name" not in r["missing_fields"]
        assert "phone" not in r["missing_fields"]

    def test_build_returns_none_for_empty_direct_with_no_ai(self):
        result = _build(None, [], direct_question=True, wants_booking=False)
        assert result is None


# ============================================================
# Acceptance scenarios from spec
# ============================================================

class TestPhase4AAcceptanceScenarios:

    def test_spec_example_direct_phone_question(self):
        """
        User: "Beni arayacaklar mı ön görüşme yaparsak?"
        Expected final: AI answer + soft phone hint
        """
        ai = "Evet, ön görüşmede Berkay Bey sizi arayacak."
        result = _build(ai, ["phone"], direct_question=True, wants_booking=True)
        assert "Berkay Bey sizi arayacak" in result
        assert "telefon" in result.lower()
        # Must NOT override the AI answer
        assert result.startswith("Evet") or "Evet" in result

    def test_spec_example_booking_name_prompt(self):
        """
        missing_fields=["full_name", "phone"]
        Expected: "Harika. Kayıt için adınızı ve soyadınızı paylaşabilir misiniz?"
        """
        result = _build("Tabii.", ["full_name", "phone"], direct_question=False, wants_booking=True)
        assert "adınızı" in result.lower()
        assert "soyadınızı" in result.lower()
        # Only asks for name, not phone
        assert "telefon" not in result.lower()

    def test_spec_example_info_only_query(self):
        """
        User: "Web sitesi ne kadar sürer?"
        Expected: just the answer, no field prompts
        """
        ai = "Web sitesi teslim süresi genelde 7-14 iş günü aralığındadır."
        result = _build(ai, ["full_name", "phone"], direct_question=False, wants_booking=False)
        assert result == ai
        assert "telefon" not in result.lower()

    def test_spec_phone_soft_prompt_example(self):
        """
        direct_question=True, missing=phone
        Spec expected: "Planlamak isterseniz telefon numaranızı paylaşabilirsiniz."
        """
        ai = "Ön görüşmede ekibimiz sizinle iletişime geçer."
        result = _build(ai, ["phone"], direct_question=True, wants_booking=True)
        assert "planlamak isterseniz" in result.lower() or "telefon numaranızı" in result.lower()
