"""
Phase 4B Tests — Completed Follow-up Answer-First Enforcement
=============================================================

Tests the ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS flag and
build_completed_followup_answer_first() logic from pipeline_wrapper.py.

Core contract:
- AI reply_candidate preferred when valid
- Safe fallback ONLY when AI is empty / error / false_confirmation / field_prompt
- appointment_created=False always in this path
- No new appointment, no missing field prompts
"""
import pytest


SAFE_FALLBACK = "Mevcut ön görüşme kaydınız korunuyor. Ek bir detay olursa buradan yazabilirsiniz."


def _build(ai_reply, *, appointment_created=False, appointment_id=None):
    from app.pipeline_wrapper import build_completed_followup_answer_first
    return build_completed_followup_answer_first(
        ai_reply,
        appointment_created=appointment_created,
        appointment_id=appointment_id,
    )


# ============================================================
# AI reply preserved (valid candidate)
# ============================================================

class TestCompletedFollowupAIPreserved:

    def test_payment_question_ai_preserved(self):
        """completed + payment question → AI answer preserved."""
        ai = "Ödeme detayı ön görüşmede netleşir."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["source"] == "completed_followup_ai"
        assert r["block_reason"] is None
        assert r["appointment_created"] is False

    def test_location_question_ai_preserved(self):
        ai = "Görüşme online yapılır, ekibimiz detayları iletir."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["source"] == "completed_followup_ai"

    def test_contact_question_ai_preserved(self):
        ai = "Evet, ekibimiz uygunluk durumuna göre sizinle dönüş yapacak."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["source"] == "completed_followup_ai"

    def test_later_question_ai_preserved(self):
        ai = "Tabii, ne zaman isterseniz buradan yazabilirsiniz."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["source"] == "completed_followup_ai"

    def test_unknown_answerable_message_ai_preserved(self):
        """Spec: unknown but answerable → AI preserved, hardcoded fallback NOT used."""
        ai = "Ön görüşmeden sonra ihtiyacınıza uygun çözüm kapsamı netleşir ve size yol haritası paylaşılır."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["source"] == "completed_followup_ai"
        assert r["outbound_text"] != SAFE_FALLBACK

    def test_appointment_created_is_always_false(self):
        ai = "Görüşmede konuşuruz."
        r = _build(ai, appointment_created=False)
        assert r["appointment_created"] is False
        assert r["appointment_updated"] is False

    def test_generic_safe_message_ai_preserved(self):
        ai = "Ek bir sorunuz varsa buradayım."
        r = _build(ai)
        assert r["outbound_text"] == ai

    def test_closing_ai_preserved(self):
        ai = "Rica ederiz, iyi günler."
        r = _build(ai)
        assert r["outbound_text"] == ai


# ============================================================
# Safe fallback triggered (AI invalid)
# ============================================================

class TestCompletedFollowupSafeFallback:

    def test_empty_ai_returns_fallback(self):
        r = _build("")
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "ai_empty"
        assert r["source"] == "completed_followup_safe_fallback"

    def test_none_ai_returns_fallback(self):
        r = _build(None)
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "ai_empty"

    def test_llm_error_reply_returns_fallback(self):
        r = _build("Error: LLM json error occurred")
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "ai_error"

    def test_too_many_requests_returns_fallback(self):
        r = _build("Error: too many requests")
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "ai_error"

    def test_false_confirmation_no_appointment_blocked(self):
        """AI produces fake confirmation without real DB action → blocked."""
        ai = "Randevunuz oluşturuldu. Sizi arayacağız."
        r = _build(ai, appointment_created=False, appointment_id=None)
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "false_confirmation"

    def test_false_confirmation_with_real_appointment_preserved(self):
        """If appointment was actually created (real DB), preserve the confirmation."""
        ai = "Randevunuz oluşturuldu. Sizi arayacağız."
        r = _build(ai, appointment_created=True, appointment_id=123)
        # Should NOT be blocked since the appointment was genuinely created
        assert r["block_reason"] is None
        assert r["source"] == "completed_followup_ai"

    def test_field_prompt_in_completed_state_blocked(self):
        """AI drifts to asking for name/phone in completed state → blocked."""
        ai = "Ad soyadınızı alabilir miyim?"
        r = _build(ai)
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "field_prompt_in_completed_state"

    def test_phone_prompt_in_completed_state_blocked(self):
        ai = "Telefon numaranızı alabilir miyim?"
        r = _build(ai)
        assert r["outbound_text"] == SAFE_FALLBACK
        assert r["block_reason"] == "field_prompt_in_completed_state"

    def test_no_missing_field_prompt_in_output(self):
        """Blocked AI → fallback has no field collection strings."""
        r = _build("Telefon numaranızı eksiksiz alabilir miyim?")
        out = r["outbound_text"].lower()
        assert "telefon numaranızı" not in out or "kaydınız korunuyor" in out

    def test_fallback_never_triggers_new_appointment(self):
        r = _build(None)
        assert r["appointment_created"] is False
        assert r["appointment_updated"] is False


# ============================================================
# Spec acceptance scenarios
# ============================================================

class TestPhase4BAcceptanceScenarios:

    def test_spec_payment_followup(self):
        """completed + 'Ödeme nasıl oluyor?' → AI cevabı korunur."""
        ai = "Ödeme detayı ön görüşmede netleşir; uygun olursa havale/EFT veya online ödeme seçenekleri paylaşılır."
        r = _build(ai)
        assert "ödeme" in r["outbound_text"].lower() or "öde" in r["outbound_text"].lower()
        assert r["source"] == "completed_followup_ai"
        assert r["appointment_created"] is False

    def test_spec_location_followup(self):
        """completed + 'Nereden görüşeceğiz?' → AI korunur, saat istenmez."""
        ai = "Görüşme online yapılır; ekibimiz uygun bağlantı bilgisini paylaşır."
        r = _build(ai)
        assert "online" in r["outbound_text"].lower() or "görüş" in r["outbound_text"].lower()
        assert r["source"] == "completed_followup_ai"

    def test_spec_contact_followup(self):
        """completed + 'Berkay bey mi arayacak?' → AI korunur, telefon promptu yok."""
        ai = "Evet, ekibimiz uygunluk durumuna göre sizinle dönüş yapacak."
        r = _build(ai)
        assert "ekibimiz" in r["outbound_text"].lower() or "dönüş" in r["outbound_text"].lower()
        assert r["source"] == "completed_followup_ai"
        assert "telefon" not in r["outbound_text"].lower()

    def test_spec_later_followup(self):
        """completed + 'Sonradan yazsam olur mu?' → AI korunur."""
        ai = "Tabii, ne zaman isterseniz buradan yazabilirsiniz. Mevcut kaydınız korunuyor."
        r = _build(ai)
        assert r["source"] == "completed_followup_ai"
        assert r["appointment_created"] is False

    def test_spec_unknown_answerable_ai_not_hardcoded(self):
        """
        completed + 'Peki sonra nasıl ilerleyeceğiz?'
        Spec: AI preserved, hardcoded fallback NOT used.
        """
        ai = "Ön görüşmeden sonra ihtiyacınıza uygun çözüm kapsamı netleşir."
        r = _build(ai)
        assert r["outbound_text"] == ai
        assert r["outbound_text"] != SAFE_FALLBACK

    def test_spec_ai_false_confirmation_blocked(self):
        """completed + AI produces 'randevunuz oluşturuldu' → blocked → fallback."""
        ai = "Harika, ön görüşmeniz oluşturuldu. Sizi arayacağız."
        r = _build(ai, appointment_created=False, appointment_id=None)
        assert r["outbound_text"] == SAFE_FALLBACK
        assert "oluşturuldu" not in r["outbound_text"]

    def test_spec_ai_field_prompt_blocked(self):
        """completed + AI drifts to field collection → blocked → fallback."""
        ai = "Tabii. Telefon numaranızı alabilir miyim?"
        r = _build(ai)
        assert r["outbound_text"] == SAFE_FALLBACK


# ============================================================
# Invariants: no new appointment ever
# ============================================================

class TestPhase4BInvariants:

    def test_appointment_created_never_true(self):
        for ai in [
            "Ödeme detayı netleşir.",
            None,
            "",
            "Error: llm json error",
            "Randevunuz oluşturuldu.",
        ]:
            r = _build(ai)
            assert r["appointment_created"] is False, f"Failed for ai={ai!r}"

    def test_appointment_updated_never_true(self):
        for ai in ["Görüşürüz.", None, ""]:
            r = _build(ai)
            assert r["appointment_updated"] is False

    def test_safe_fallback_text_has_no_field_prompts(self):
        r = _build(None)
        out = r["outbound_text"].lower()
        forbidden = ["ad soyadınızı", "telefon numaranızı", "uygun gün", "randevu oluşturalım"]
        for phrase in forbidden:
            assert phrase not in out, f"Forbidden phrase found: {phrase!r}"
