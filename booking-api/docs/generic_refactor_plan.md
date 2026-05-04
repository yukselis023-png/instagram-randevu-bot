# Chatbot Generic Core Refactor Plan

This plan executes a massive simplification of the `main.py` architecture, stripping out the 13,000+ lines of fragile, hardcoded intent rules and replacing them with a purely declarative, LLM-first engine.

## Phase 1: Business Knowledge Layer (Config Schema Update)
We will expand the existing `doel.json`, `beauty.json`, and `dental.json` files to fully encompass the required structure:
- `business_name`, `business_type`
- `services`, `service_descriptions`, `service_prices`
- `booking_type`, `working_hours`, `unavailable_services`
- `common_questions`, `human_contact`, `tone`, `crm_fields`

## Phase 2: AI Reply Layer & Minimal Routing
Create a new generic routing engine (`app/generic_core.py`) that uses a unified AI system prompt.
The LLM will be tasked to emit one of exactly 7 routes:
1. `direct_answer`
2. `service_question`
3. `price_question`
4. `booking_request`
5. `active_booking`
6. `human_handoff`
7. `fallback`

We will delete all 100+ legacy `is_xxx_question()` and `build_xxx_reply()` functions from `main.py`.

## Phase 3: Action Layer & State Machine
Implement strict deterministic logic (outside the LLM) ONLY for:
- Phone & Name collection status.
- Booking intent validation.
- Date/Time availability matching (e.g., checking `working_hours`).
- Triggering CRM Lead creation syncing (`crm_fields`).
- Human handoff escalation.

## Phase 4: Final Guard
Create a stripped-down `guard_response()` interceptor that ONLY blocks:
- Inventing non-existent services.
- Fabricating prices.
- Irrelevant/off-topic answers.
- Falsely claiming a booking or CRM record is completed.
- Returning unavailable/invalid dates.
- Ignoring a direct request to speak to a human.

## Phase 5: Integration & Massive Pruning
1. Point `main.py`'s `generate_reply` (or `/api/process-instagram-message` endpoint) directly to `generic_core.py`.
2. Delete the legacy 10k+ lines in `main.py` (`apply_ai_first_quality_overrides`, sector-specific heuristics, string counters).
3. Ensure the CRM output syncs exactly with DOEL CRM formats (`lead_name`, `phone`, `instagram_username`, etc.).

## Phase 6: Testing
Rewrite `tests/test_dm_quality_scenarios.py` to solely use the 3 generic configs (DOEL, Beauty, Dental). Assert that DOEL specific terms ("reklam", "otomasyon") never leak into other profiles. All routes (Service -> Price -> Fit -> Booking -> CRM -> Handoff) will be verified per profile.