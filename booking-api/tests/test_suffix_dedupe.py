"""
Suffix Dedupe Tests — Final Polish
===================================

Tests for _ai_already_asks_field() and the deduplication logic in
build_final_missing_field_prompt() when direct_question=True.

Spec:
- If AI reply already asks for the first missing field → Final Builder skips its suffix.
- If AI reply does NOT ask → Final Builder appends the soft suffix as before.
- Phone, date, time dedupe mirrors the same logic.
"""
import pytest


def _build(ai_reply, missing_fields, *, direct_question=True, wants_booking=True):
    from app.pipeline_wrapper import build_final_missing_field_prompt
    return build_final_missing_field_prompt(
        ai_reply,
        missing_fields,
        direct_question=direct_question,
        wants_booking=wants_booking,
    )


def _already_asks(text, field):
    from app.pipeline_wrapper import _ai_already_asks_field
    return _ai_already_asks_field(text, field)


# ============================================================
# _ai_already_asks_field unit tests
# ============================================================

class TestAiAlreadyAsksField:

    # --- full_name ---
    def test_full_name_detected_adınızı(self):
        assert _already_asks("Adınızı ve soyadınızı paylaşabilir misiniz?", "full_name") is True

    def test_full_name_detected_isminizi(self):
        assert _already_asks("isminizi öğrenebilir miyim?", "full_name") is True

    def test_full_name_detected_ad_soyad(self):
        assert _already_asks("Ad soyad bilgisi alabilir miyim?", "full_name") is True

    def test_full_name_not_detected_unrelated(self):
        assert _already_asks("Evet, ekibimiz sizi arayacak.", "full_name") is False

    # --- phone ---
    def test_phone_detected_telefon(self):
        assert _already_asks("Telefon numaranızı paylaşabilir misiniz?", "phone") is True

    def test_phone_detected_numaranızı(self):
        assert _already_asks("Numaranızı alabilir miyim?", "phone") is True

    def test_phone_not_detected_unrelated(self):
        assert _already_asks("Evet, size yardımcı olabiliriz.", "phone") is False

    # --- requested_date ---
    def test_date_detected_gün(self):
        assert _already_asks("Uygun bir gün belirtebilir misiniz?", "requested_date") is True

    def test_date_detected_tarih(self):
        assert _already_asks("Tarih tercihinizi yazabilirsiniz.", "requested_date") is True

    def test_date_not_detected_unrelated(self):
        assert _already_asks("Evet, görüşme ayarlayabiliriz.", "requested_date") is False

    # --- requested_time ---
    def test_time_detected_saat(self):
        assert _already_asks("Hangi saati tercih edersiniz?", "requested_time") is True

    def test_time_not_detected_unrelated(self):
        assert _already_asks("Evet, ekibimiz sizinle iletişime geçer.", "requested_time") is False

    # --- unknown field always False ---
    def test_unknown_field_returns_false(self):
        assert _already_asks("Herhangi bir şey söyle", "unknown_field") is False

    # --- empty / None ---
    def test_empty_text_returns_false(self):
        assert _already_asks("", "full_name") is False

    def test_none_text_returns_false(self):
        assert _already_asks(None, "full_name") is False


# ============================================================
# build_final_missing_field_prompt — dedupe integration
# ============================================================

class TestSuffixDedupeInFinalBuilder:

    # Scenario 1: AI already asks full_name → no suffix appended
    def test_full_name_suffix_skipped_when_ai_already_asks(self):
        ai = "Evet, Berkay Bey veya ekibimiz sizi arayacak. Devam edebilmek için adınızı ve soyadınızı paylaşabilir misiniz?"
        result = _build(ai, ["full_name"])
        # Should equal base only — no extra suffix
        assert result == ai
        # Confirm suffix text is NOT appended
        assert "Planlamak isterseniz" not in result

    # Scenario 2: AI does NOT ask full_name → LLM reply returned as-is (no suffix)
    def test_full_name_no_suffix_when_ai_silent(self):
        ai = "Evet, Berkay Bey veya ekibimiz sizi arayacak."
        result = _build(ai, ["full_name"])
        assert result == ai
        assert "Planlamak isterseniz" not in result

    # Scenario 3: AI already asks phone → no suffix
    def test_phone_suffix_skipped_when_ai_already_asks(self):
        ai = "Sizi bilgilendireceğiz. Telefon numaranızı paylaşabilir misiniz?"
        result = _build(ai, ["phone"])
        assert result == ai
        assert "Planlamak isterseniz telefon" not in result

    # Scenario 4: AI does NOT ask phone → LLM reply returned as-is (no suffix)
    def test_phone_no_suffix_when_ai_silent(self):
        ai = "Tabii, size yardımcı olabiliriz."
        result = _build(ai, ["phone"])
        assert result == ai
        assert "Planlamak isterseniz" not in result

    # Scenario 5: AI already asks date → no suffix
    def test_date_suffix_skipped_when_ai_already_asks(self):
        ai = "Görüşme için uygun bir gün belirtebilirsiniz."
        result = _build(ai, ["requested_date"])
        assert result == ai
        assert "Uygun bir gün varsa belirtebilirsiniz." not in result

    # Scenario 6: AI does NOT ask date → LLM reply returned as-is (no suffix)
    def test_date_no_suffix_when_ai_silent(self):
        ai = "Evet, görüşme düzenleyebiliriz."
        result = _build(ai, ["requested_date"])
        assert result == ai
        assert "Uygun bir gün" not in result

    # Scenario 7: AI already asks time → no suffix
    def test_time_suffix_skipped_when_ai_already_asks(self):
        ai = "Hangi saati tercih edersiniz? Örneğin 14:00."
        result = _build(ai, ["requested_time"])
        assert result == ai
        assert "Saat tercihini de yazabilirsiniz" not in result

    # Scenario 8: AI does NOT ask time → LLM reply returned as-is (no suffix)
    def test_time_no_suffix_when_ai_silent(self):
        ai = "Evet, müsait olduğunuzda görüşebiliriz."
        result = _build(ai, ["requested_time"])
        assert result == ai
        assert "Saat tercihini" not in result

    # Scenario 9: wants_booking=False, direct_question=False → AI reply returned as-is (no change)
    def test_no_booking_no_direct_question_returns_ai_as_is(self):
        ai = "Sizi bilgilendiririz."
        result = _build(ai, ["full_name"], direct_question=False, wants_booking=False)
        assert result == ai

    # Scenario 10: direct_question=True, empty AI reply → None returned
    def test_empty_ai_reply_returns_none(self):
        result = _build("", ["full_name"], direct_question=True)
        assert result is None

    # Scenario 11: no missing fields, direct_question=True → AI returned as-is
    def test_no_missing_fields_returns_base(self):
        ai = "Evet, sizi arayacağız."
        result = _build(ai, [], direct_question=True)
        assert result == ai
