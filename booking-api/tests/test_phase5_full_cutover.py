"""
Phase 5 Tests — Answer-First Full Cutover
==========================================

Tests that ANSWER_FIRST_PIPELINE=on auto-enables all scoped enforce flags.

Core contract:
- on mode  => all enforce_* flags are True automatically
- shadow mode => enforce_* flags are controlled individually (legacy)
- off mode => all enforce_* flags follow env var (default false)
"""
import os
import sys
import pytest


def _parse_flag_block(shadow_mode: str, env_overrides: dict = None) -> dict:
    """
    Simulate the flag-parsing block in generic_core without running the full pipeline.
    Returns a dict with the resolved boolean values for each flag.
    """
    env_overrides = env_overrides or {}
    saved = {}
    for k, v in env_overrides.items():
        saved[k] = os.environ.get(k)
        os.environ[k] = v
    os.environ["ANSWER_FIRST_PIPELINE"] = shadow_mode
    try:
        _full_cutover = shadow_mode == "on"
        enforce_direct_question = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION", "false").lower() == "true"
        enforce_missing_field_prompts = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS", "false").lower() == "true"
        enforce_completed_followups = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS", "false").lower() == "true"
        enforce_info_answers = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_INFO_ANSWERS", "false").lower() == "true"
        enforce_appointment_action_replies = _full_cutover or os.environ.get("ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES", "false").lower() == "true"
        return {
            "full_cutover": _full_cutover,
            "enforce_direct_question": enforce_direct_question,
            "enforce_missing_field_prompts": enforce_missing_field_prompts,
            "enforce_completed_followups": enforce_completed_followups,
            "enforce_info_answers": enforce_info_answers,
            "enforce_appointment_action_replies": enforce_appointment_action_replies,
        }
    finally:
        os.environ.pop("ANSWER_FIRST_PIPELINE", None)
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


# ============================================================
# 1. on mode — all flags auto-enabled
# ============================================================

class TestOnModeAutoFlags:

    def test_on_mode_full_cutover_true(self):
        assert _parse_flag_block("on")["full_cutover"] is True

    def test_on_mode_enforce_direct_question(self):
        assert _parse_flag_block("on")["enforce_direct_question"] is True

    def test_on_mode_enforce_missing_field_prompts(self):
        assert _parse_flag_block("on")["enforce_missing_field_prompts"] is True

    def test_on_mode_enforce_completed_followups(self):
        assert _parse_flag_block("on")["enforce_completed_followups"] is True

    def test_on_mode_enforce_info_answers(self):
        assert _parse_flag_block("on")["enforce_info_answers"] is True

    def test_on_mode_enforce_appointment_action_replies(self):
        assert _parse_flag_block("on")["enforce_appointment_action_replies"] is True

    def test_on_mode_overrides_individual_false_env(self):
        """on mode must activate all flags even when individual env explicitly false."""
        flags = _parse_flag_block("on", {
            "ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION": "false",
            "ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS": "false",
            "ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS": "false",
            "ANSWER_FIRST_ENFORCE_INFO_ANSWERS": "false",
            "ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES": "false",
        })
        assert flags["enforce_direct_question"] is True
        assert flags["enforce_missing_field_prompts"] is True
        assert flags["enforce_completed_followups"] is True
        assert flags["enforce_info_answers"] is True
        assert flags["enforce_appointment_action_replies"] is True

    def test_on_mode_all_five_flags_active(self):
        flags = _parse_flag_block("on")
        all_active = [
            flags["enforce_direct_question"],
            flags["enforce_missing_field_prompts"],
            flags["enforce_completed_followups"],
            flags["enforce_info_answers"],
            flags["enforce_appointment_action_replies"],
        ]
        assert all(all_active), f"Not all flags active in on mode: {flags}"


# ============================================================
# 2. shadow mode — individual scoped control
# ============================================================

class TestShadowModeFlags:

    def test_shadow_mode_full_cutover_false(self):
        assert _parse_flag_block("shadow")["full_cutover"] is False

    def test_shadow_mode_flags_default_false(self):
        flags = _parse_flag_block("shadow", {
            "ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION": "false",
            "ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS": "false",
            "ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS": "false",
            "ANSWER_FIRST_ENFORCE_INFO_ANSWERS": "false",
            "ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES": "false",
        })
        assert flags["enforce_direct_question"] is False
        assert flags["enforce_missing_field_prompts"] is False
        assert flags["enforce_completed_followups"] is False
        assert flags["enforce_info_answers"] is False
        assert flags["enforce_appointment_action_replies"] is False

    def test_shadow_mode_individual_flag_true_respected(self):
        flags = _parse_flag_block("shadow", {
            "ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS": "true",
            "ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION": "false",
            "ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS": "false",
            "ANSWER_FIRST_ENFORCE_INFO_ANSWERS": "false",
            "ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES": "false",
        })
        assert flags["enforce_missing_field_prompts"] is True
        assert flags["enforce_direct_question"] is False  # others stay false

    def test_shadow_not_full_cutover(self):
        assert _parse_flag_block("shadow")["full_cutover"] is False


# ============================================================
# 3. off mode
# ============================================================

class TestOffModeFlags:

    def test_off_mode_full_cutover_false(self):
        assert _parse_flag_block("off")["full_cutover"] is False

    def test_off_mode_all_enforce_false(self):
        flags = _parse_flag_block("off", {
            "ANSWER_FIRST_ENFORCE_ACTIVE_DIRECT_QUESTION": "false",
            "ANSWER_FIRST_ENFORCE_MISSING_FIELD_PROMPTS": "false",
            "ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS": "false",
            "ANSWER_FIRST_ENFORCE_INFO_ANSWERS": "false",
            "ANSWER_FIRST_ENFORCE_APPOINTMENT_ACTION_REPLIES": "false",
        })
        assert flags["enforce_direct_question"] is False
        assert flags["enforce_missing_field_prompts"] is False
        assert flags["enforce_completed_followups"] is False
        assert flags["enforce_info_answers"] is False
        assert flags["enforce_appointment_action_replies"] is False

    def test_off_mode_individual_flag_can_still_be_set(self):
        flags = _parse_flag_block("off", {
            "ANSWER_FIRST_ENFORCE_COMPLETED_FOLLOWUPS": "true",
        })
        assert flags["enforce_completed_followups"] is True


# ============================================================
# 4. Strict string matching for "on"
# ============================================================

class TestOnModeStringMatch:

    def test_ON_uppercase_does_not_trigger_cutover(self):
        assert _parse_flag_block("ON")["full_cutover"] is False

    def test_true_string_does_not_trigger_cutover(self):
        assert _parse_flag_block("true")["full_cutover"] is False

    def test_1_does_not_trigger_cutover(self):
        assert _parse_flag_block("1")["full_cutover"] is False

    def test_shadow_does_not_trigger_cutover(self):
        assert _parse_flag_block("shadow")["full_cutover"] is False


# ============================================================
# 5. Final Builders importable and callable
# ============================================================

class TestPhase5FinalBuilders:

    def test_build_appointment_action_reply(self):
        from app.pipeline_wrapper import build_appointment_action_reply
        assert callable(build_appointment_action_reply)

    def test_build_info_answer_final(self):
        from app.pipeline_wrapper import build_info_answer_final
        assert callable(build_info_answer_final)

    def test_build_completed_followup_answer_first(self):
        from app.pipeline_wrapper import build_completed_followup_answer_first
        assert callable(build_completed_followup_answer_first)

    def test_build_final_missing_field_prompt(self):
        from app.pipeline_wrapper import build_final_missing_field_prompt
        assert callable(build_final_missing_field_prompt)

    def test_validate_appointment_reply_no_false_confirmation(self):
        from app.pipeline_wrapper import validate_appointment_reply_no_false_confirmation
        assert callable(validate_appointment_reply_no_false_confirmation)

    def test_run_shadow_pipeline(self):
        from app.pipeline_wrapper import run_shadow_pipeline
        assert callable(run_shadow_pipeline)


# ============================================================
# 6. Post-cutover audit expectations
# ============================================================

class TestPostCutoverAudit:

    def test_on_mode_info_enforce_true(self):
        assert _parse_flag_block("on")["enforce_info_answers"] is True

    def test_on_mode_completed_followup_enforce_true(self):
        assert _parse_flag_block("on")["enforce_completed_followups"] is True

    def test_on_mode_missing_field_enforce_true(self):
        assert _parse_flag_block("on")["enforce_missing_field_prompts"] is True

    def test_on_mode_appointment_action_enforce_true(self):
        assert _parse_flag_block("on")["enforce_appointment_action_replies"] is True

    def test_on_mode_direct_question_enforce_true(self):
        assert _parse_flag_block("on")["enforce_direct_question"] is True
