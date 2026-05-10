"""
Phase 4D Tests — Appointment Action Reply Final Builder
======================================================

Tests the ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES flag and
build_appointment_action_reply() / validate_appointment_reply_no_false_confirmation()
logic from pipeline_wrapper.py.

Core contract:
- DB success required for confirmation text
- No false confirmation when appointment_created=False
- No false update confirmation when appointment_updated=False
- Reschedule pending → confirmation question only, no "güncellendi"
- Action failed → safe failure reply
"""
import pytest

APPT_CREATE_FAIL = (
    "Randevu kaydını şu an kesinleştiremedim; bilgilerinizi ekibin kontrol etmesi için not aldım."
)
APPT_UPDATE_FAIL = (
    "Saat değişikliği talebinizi ekibe iletmek üzere not aldım. Mevcut randevu kaydınız korunuyor."
)

# A minimal conversation dict that satisfies build_confirmation_message
_CONV_FULL = {
    "full_name": "Berkay Çakmak",
    "phone": "05539088638",
    "service": "Web Tasarım",
    "requested_date": "2026-05-11",
    "requested_time": "13:00:00",
    "state": "completed",
    "appointment_status": "confirmed",
    "booking_kind": "preconsultation",
}


def _build(action_result, *, conv=None):
    from app.pipeline_wrapper import build_appointment_action_reply
    return build_appointment_action_reply(action_result, conversation=conv or _CONV_FULL)


def _validate(reply_text, *, appointment_created=False, appointment_updated=False, appointment_id=None):
    from app.pipeline_wrapper import validate_appointment_reply_no_false_confirmation
    return validate_appointment_reply_no_false_confirmation(
        reply_text,
        appointment_created=appointment_created,
        appointment_updated=appointment_updated,
        appointment_id=appointment_id,
    )


# ============================================================
# 1. appointment_created — happy path
# ============================================================

class TestAppointmentCreated:

    def test_create_success_returns_confirmation(self):
        r = _build({
            "action": "appointment_created",
            "db_success": True,
            "appointment_created": True,
            "appointment_updated": False,
            "appointment_id": 42,
            "appointment_date": "11.05.2026",
            "appointment_time": "13:00",
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["source"] == "4d_appointment_created"
        assert r["block_reason"] is None
        # Confirmation contains date and time
        assert "11.05.2026" in r["outbound_text"] or "13:00" in r["outbound_text"]

    def test_create_no_db_success_returns_failure(self):
        r = _build({
            "action": "appointment_created",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": None,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["outbound_text"] == APPT_CREATE_FAIL
        assert r["block_reason"] == "db_success_or_id_missing"

    def test_create_no_appointment_id_returns_failure(self):
        r = _build({
            "action": "appointment_created",
            "db_success": True,
            "appointment_created": True,
            "appointment_updated": False,
            "appointment_id": None,   # <--- missing id
            "appointment_date": "11.05.2026", "appointment_time": "13:00",
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["outbound_text"] == APPT_CREATE_FAIL
        assert r["block_reason"] == "db_success_or_id_missing"

    def test_create_success_outbound_has_no_phone_prompt(self):
        r = _build({
            "action": "appointment_created",
            "db_success": True,
            "appointment_created": True,
            "appointment_updated": False,
            "appointment_id": 99,
            "appointment_date": "11.05.2026", "appointment_time": "13:00",
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert "telefon numaranızı" not in r["outbound_text"].lower()
        assert "adınızı" not in r["outbound_text"].lower()


# ============================================================
# 2. appointment_updated — reschedule confirmed
# ============================================================

class TestAppointmentUpdated:

    def test_update_success_returns_update_confirmation(self):
        r = _build({
            "action": "appointment_updated",
            "db_success": True,
            "appointment_created": False,
            "appointment_updated": True,
            "appointment_id": 42,
            "appointment_date": "11.05.2026",
            "appointment_time": "13:00",
            "same_appointment_id": True,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["source"] == "4d_appointment_updated"
        assert "13:00" in r["outbound_text"]
        assert "güncellendi" in r["outbound_text"].lower()
        assert r["block_reason"] is None

    def test_update_no_db_success_returns_failure(self):
        r = _build({
            "action": "appointment_updated",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": 42,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["outbound_text"] == APPT_UPDATE_FAIL
        assert r["block_reason"] == "db_update_not_confirmed"

    def test_update_no_appointment_id_returns_failure(self):
        r = _build({
            "action": "appointment_updated",
            "db_success": True,
            "appointment_created": False,
            "appointment_updated": True,
            "appointment_id": None,
            "appointment_date": "11.05.2026", "appointment_time": "13:00",
            "same_appointment_id": True,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["outbound_text"] == APPT_UPDATE_FAIL

    def test_update_failure_text_has_no_guencellendi(self):
        r = _build({
            "action": "appointment_updated",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": None,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert "güncellendi" not in r["outbound_text"].lower()


# ============================================================
# 3. reschedule_pending_confirmation
# ============================================================

class TestReschedulePending:

    def test_pending_returns_confirmation_question(self):
        r = _build({
            "action": "reschedule_pending_confirmation",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": 42,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": True,
            "reschedule_date": None,
            "reschedule_time": "13:00",
            "error": None,
        })
        assert r["source"] == "4d_reschedule_pending"
        assert "13:00" in r["outbound_text"]
        assert "onaylıyor musunuz" in r["outbound_text"].lower()

    def test_pending_does_not_say_guncellendi(self):
        r = _build({
            "action": "reschedule_pending_confirmation",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": 42,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": True,
            "reschedule_date": None, "reschedule_time": "13:00",
            "error": None,
        })
        assert "güncellendi" not in r["outbound_text"].lower()
        assert r["block_reason"] is None

    def test_pending_appointment_count_invariant(self):
        """Pending reschedule must NOT set appointment_updated=True in reply."""
        r = _build({
            "action": "reschedule_pending_confirmation",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": 42,
            "reschedule_date": "2026-05-12", "reschedule_time": "13:00",
            "same_appointment_id": True, "error": None,
            "appointment_date": None, "appointment_time": None,
        })
        assert "güncellendi" not in r["outbound_text"].lower()
        assert "oluşturuldu" not in r["outbound_text"].lower()


# ============================================================
# 4. appointment_create_failed
# ============================================================

class TestAppointmentCreateFailed:

    def test_create_failed_returns_safe_reply(self):
        r = _build({
            "action": "appointment_create_failed",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": None,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": "db timeout",
        })
        assert r["outbound_text"] == APPT_CREATE_FAIL
        assert r["source"] == "4d_appointment_create_failed"
        assert "oluşturuldu" not in r["outbound_text"]

    def test_create_failed_no_false_confirmation(self):
        r = _build({
            "action": "appointment_create_failed",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": None,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert "sizi arayacağız" not in r["outbound_text"].lower()
        assert "oluşturuldu" not in r["outbound_text"].lower()


# ============================================================
# 5. appointment_update_failed
# ============================================================

class TestAppointmentUpdateFailed:

    def test_update_failed_returns_safe_reply(self):
        r = _build({
            "action": "appointment_update_failed",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": 42,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": True,
            "reschedule_date": None, "reschedule_time": None, "error": "db error",
        })
        assert r["outbound_text"] == APPT_UPDATE_FAIL
        assert "güncellendi" not in r["outbound_text"].lower()
        assert r["source"] == "4d_appointment_update_failed"


# ============================================================
# 6. action=none (no appointment action)
# ============================================================

class TestActionNone:

    def test_no_action_returns_none_text(self):
        r = _build({
            "action": "none",
            "db_success": False,
            "appointment_created": False,
            "appointment_updated": False,
            "appointment_id": None,
            "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False,
            "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["outbound_text"] is None
        assert r["source"] == "4d_no_action"


# ============================================================
# 7. validate_appointment_reply_no_false_confirmation
# ============================================================

class TestFalseConfirmationGuard:

    def test_safe_reply_passes(self):
        is_safe, reason = _validate("Bilgilerinizi not aldım.", appointment_created=False)
        assert is_safe is True
        assert reason is None

    def test_false_confirmation_blocked(self):
        is_safe, reason = _validate(
            "Ön görüşmeniz oluşturuldu saat 13:00.",
            appointment_created=False, appointment_id=None
        )
        assert is_safe is False
        assert "oluşturuldu" in reason

    def test_false_update_blocked(self):
        is_safe, reason = _validate(
            "Saatiniz güncellendi.",
            appointment_updated=False
        )
        assert is_safe is False
        assert "güncellendi" in reason

    def test_real_confirmation_safe(self):
        """When appointment was actually created, confirmation is safe."""
        is_safe, reason = _validate(
            "Ön görüşme kaydınız oluşturuldu.",
            appointment_created=True, appointment_id=42
        )
        assert is_safe is True

    def test_real_update_safe(self):
        """When appointment was actually updated, update text is safe."""
        is_safe, reason = _validate(
            "Saatiniz güncellendi.",
            appointment_updated=True
        )
        assert is_safe is True

    def test_sizi_arayacagiz_blocked_without_create(self):
        is_safe, reason = _validate(
            "Ön görüşmeniz ayarlandı, sizi arayacağız.",
            appointment_created=False, appointment_id=None
        )
        assert is_safe is False

    def test_empty_reply_is_safe(self):
        is_safe, reason = _validate(None, appointment_created=False)
        assert is_safe is True
        assert reason is None


# ============================================================
# 8. Spec acceptance scenarios
# ============================================================

class TestPhase4DAcceptanceScenarios:

    def test_spec_create_success_confirmation(self):
        """Spec: appointment_created=true, appointment_id exists → confirmation."""
        r = _build({
            "action": "appointment_created",
            "db_success": True, "appointment_created": True, "appointment_updated": False,
            "appointment_id": 1, "appointment_date": "11.05.2026", "appointment_time": "13:00",
            "same_appointment_id": False, "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert r["source"] == "4d_appointment_created"
        assert "11.05.2026" in r["outbound_text"] or "13:00" in r["outbound_text"]

    def test_spec_forced_db_fail_no_confirmation(self):
        """Spec: forced DB fail → safe failure, no 'oluşturuldu'."""
        r = _build({
            "action": "appointment_create_failed",
            "db_success": False, "appointment_created": False, "appointment_updated": False,
            "appointment_id": None, "appointment_date": None, "appointment_time": None,
            "same_appointment_id": False, "reschedule_date": None, "reschedule_time": None, "error": "timeout",
        })
        assert "oluşturuldu" not in r["outbound_text"]
        assert "not aldım" in r["outbound_text"]

    def test_spec_reschedule_pending_only_question(self):
        """Spec: reschedule pending → confirmation question, not 'güncellendi'."""
        r = _build({
            "action": "reschedule_pending_confirmation",
            "db_success": False, "appointment_created": False, "appointment_updated": False,
            "appointment_id": 42, "appointment_date": None, "appointment_time": None,
            "same_appointment_id": True, "reschedule_date": None, "reschedule_time": "13:00", "error": None,
        })
        assert "güncellendi" not in r["outbound_text"].lower()
        assert "onaylıyor musunuz" in r["outbound_text"].lower()

    def test_spec_reschedule_confirm_success(self):
        """Spec: reschedule confirmed + DB updated → update confirmation."""
        r = _build({
            "action": "appointment_updated",
            "db_success": True, "appointment_created": False, "appointment_updated": True,
            "appointment_id": 42, "appointment_date": "11.05.2026", "appointment_time": "13:00",
            "same_appointment_id": True, "reschedule_date": None, "reschedule_time": None, "error": None,
        })
        assert "13:00" in r["outbound_text"]
        assert "güncellendi" in r["outbound_text"].lower()

    def test_spec_update_forced_fail(self):
        """Spec: reschedule update forced fail → no 'güncellendi', safe reply."""
        r = _build({
            "action": "appointment_update_failed",
            "db_success": False, "appointment_created": False, "appointment_updated": False,
            "appointment_id": 42, "appointment_date": None, "appointment_time": None,
            "same_appointment_id": True, "reschedule_date": None, "reschedule_time": None, "error": "db fail",
        })
        assert "güncellendi" not in r["outbound_text"].lower()
        assert "not aldım" in r["outbound_text"]


# ============================================================
# 9. Invariants
# ============================================================

class TestPhase4DInvariants:

    def test_all_failure_replies_have_no_create_confirmation(self):
        for action in ("appointment_create_failed", "appointment_update_failed"):
            r = _build({
                "action": action,
                "db_success": False, "appointment_created": False, "appointment_updated": False,
                "appointment_id": None, "appointment_date": None, "appointment_time": None,
                "same_appointment_id": False, "reschedule_date": None, "reschedule_time": None, "error": None,
            })
            text = r["outbound_text"].lower()
            assert "oluşturuldu" not in text, f"False confirm in {action}"
            assert "sizi arayacağız" not in text, f"False confirm in {action}"

    def test_source_always_present(self):
        for action in ("appointment_created", "appointment_updated", "appointment_create_failed",
                       "appointment_update_failed", "reschedule_pending_confirmation", "none"):
            r = _build({
                "action": action,
                "db_success": False, "appointment_created": False, "appointment_updated": False,
                "appointment_id": None, "appointment_date": None, "appointment_time": None,
                "same_appointment_id": False, "reschedule_date": None, "reschedule_time": None, "error": None,
            })
            assert "source" in r and r["source"]
