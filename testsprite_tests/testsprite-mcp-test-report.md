# TestSprite AI Testing Report (MCP)

---

## 1️⃣ Document Metadata

- **Project Name:** instagram-randevu-bot
- **Date:** 2026-05-20
- **Prepared by:** TestSprite AI Team + Craft Agent QA loop
- **Target:** Local fixed backend at `http://localhost:18000`
- **Run scope:** Backend API regression, Instagram message processing, CRM/customer/appointment APIs, automation jobs, campaign endpoints, LLM health.

---

## 2️⃣ Requirement Validation Summary

### Requirement: Service health and provider diagnostics

#### TC001 — `get_health_endpoint_returns_service_and_database_status`
- **Status:** ✅ Passed
- **Test Code:** [TC001_get_health_endpoint_returns_service_and_database_status.py](./TC001_get_health_endpoint_returns_service_and_database_status.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/2584188d-a14c-4394-beec-77cdc4b2bec5
- **Analysis / Findings:** Health endpoint is reachable and reports backend/database availability. Local Docker service is healthy.

#### TC010 — `get_api_llm_health_returns_llm_provider_connectivity_status`
- **Status:** ✅ Passed
- **Test Code:** [TC010_get_api_llm_health_returns_llm_provider_connectivity_status.py](./TC010_get_api_llm_health_returns_llm_provider_connectivity_status.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/b7dbe602-a60e-4970-b1b2-a9ff34e96dc0
- **Analysis / Findings:** LLM health endpoint returns connectivity/provider status. Separate local matrix previously showed transient DNS/provider fallback risk, but this TestSprite check passed.

### Requirement: Instagram DM processing and dedupe

#### TC002 — `post_process_instagram_message_handles_valid_and_duplicate_messages`
- **Status:** ❌ Failed
- **Test Code:** [TC002_post_process_instagram_message_handles_valid_and_duplicate_messages.py](./TC002_post_process_instagram_message_handles_valid_and_duplicate_messages.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/ee49ef39-9ab7-445e-884e-c7195a6c8724
- **Error:** `AssertionError: First response should have should_reply=True`
- **Analysis / Findings:** Endpoint works in manual local probes, including `appointment_created=true` happy paths and duplicate metadata. Failure likely caused by test payload not matching current DM contract or low-signal message being intentionally ignored. Needs generated test adjustment or endpoint compatibility shim. Not currently reproducing the original critical live bug: local patched live-like flow creates appointment successfully.

### Requirement: Customer and appointment CRM APIs

#### TC003 — `get_api_customers_instagram_user_id_returns_customer_detail_or_404`
- **Status:** ✅ Passed
- **Test Code:** [TC003_get_api_customers_instagram_user_id_returns_customer_detail_or_404.py](./TC003_get_api_customers_instagram_user_id_returns_customer_detail_or_404.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/e9510d8a-d86f-4c09-be37-852aaca31de3
- **Analysis / Findings:** Customer detail/404 behavior is valid.

#### TC004 — `patch_api_appointments_appointment_id_updates_appointment_fields`
- **Status:** ✅ Passed
- **Test Code:** [TC004_patch_api_appointments_appointment_id_updates_appointment_fields.py](./TC004_patch_api_appointments_appointment_id_updates_appointment_fields.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/2a36ebf5-ee2e-4be9-a04c-ee3e3eb4364a
- **Analysis / Findings:** Appointment update endpoint persists editable fields.

#### TC005 — `post_api_appointments_appointment_id_attendance_marks_attendance_status`
- **Status:** ❌ Failed
- **Test Code:** [TC005_post_api_appointments_appointment_id_attendance_marks_attendance_status.py](./TC005_post_api_appointments_appointment_id_attendance_marks_attendance_status.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/d017b5cb-c572-42aa-bbc2-42fb7cc53951
- **Error:** `AssertionError: No appointment found to mark attendance`
- **Analysis / Findings:** Failure is due to fixture/precondition discovery rather than endpoint assertion. Manual local appointment creation now works. Test should create its own appointment or select from `/api/appointments` with compatible status before attendance mark.

### Requirement: Automation queue

#### TC006 — `get_internal_automation_claim_returns_due_automation_jobs`
- **Status:** ✅ Passed
- **Test Code:** [TC006_get_internal_automation_claim_returns_due_automation_jobs.py](./TC006_get_internal_automation_claim_returns_due_automation_jobs.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/dbe59172-2187-4a2f-9be5-34e83bc04d2d
- **Analysis / Findings:** Claim endpoint is reachable and returns a valid response shape.

#### TC007 — `post_internal_automation_mark_persists_automation_result`
- **Status:** ❌ Failed
- **Test Code:** [TC007_post_internal_automation_mark_persists_automation_result.py](./TC007_post_internal_automation_mark_persists_automation_result.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/1c814115-3291-438a-96ea-4b5875139cb9
- **Error:** `AssertionError: No automation jobs available to mark from /internal/automation/claim`
- **Analysis / Findings:** Precondition issue: no due job available during mark test. Need deterministic seed endpoint/fixture or test-owned automation event creation.

### Requirement: Campaign APIs

#### TC008 — `get_api_campaigns_lists_existing_campaigns`
- **Status:** ❌ Failed
- **Test Code:** [TC008_get_api_campaigns_lists_existing_campaigns.py](./TC008_get_api_campaigns_lists_existing_campaigns.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/1a39bb13-f8f5-4860-a4ac-44ed2b71a0da
- **Error:** `AssertionError: Unexpected status code on campaign creation: 400`
- **Analysis / Findings:** Campaign creation contract mismatch or missing required fields. Endpoint rejects generated payload with 400. Needs schema alignment and negative/positive tests.

#### TC009 — `post_api_campaigns_creates_new_campaign`
- **Status:** ❌ Failed
- **Test Code:** [TC009_post_api_campaigns_creates_new_campaign.py](./TC009_post_api_campaigns_creates_new_campaign.py)
- **Result:** https://www.testsprite.com/dashboard/mcp/tests/f34f6fca-1f7c-486d-802a-3b82381ef1fa/f4358394-b1b2-48e6-8c9d-d33e89000f5b
- **Error:** `AssertionError: Expected 200 OK but got 400`
- **Analysis / Findings:** Same campaign schema mismatch as TC008. Requires endpoint schema inspection/fix or TestSprite payload update.

---

## 3️⃣ Coverage & Matching Metrics

- **Total tests:** 10
- **Passed:** 5
- **Failed:** 5
- **Pass rate:** 50.00%

| Requirement | Total Tests | ✅ Passed | ❌ Failed |
|---|---:|---:|---:|
| Service health and provider diagnostics | 2 | 2 | 0 |
| Instagram DM processing and dedupe | 1 | 0 | 1 |
| Customer and appointment CRM APIs | 3 | 2 | 1 |
| Automation queue | 2 | 1 | 1 |
| Campaign APIs | 2 | 0 | 2 |

Additional non-TestSprite local regression:

- `pytest tests -q` → **465 passed, 35 skipped, 2 warnings**
- Live-like local flow → unavailable slot then selected alternative creates DB appointment.
- Capacity race probe → fixed from 6/6 overbook to 2 created at capacity=2; remaining requests conflict/handoff.
- Correct live CRM UI smoke (`https://doel-crm.vercel.app`) → renders dashboard, preconsultations, appointments, customers; synced synthetic records visible.

---

## 4️⃣ Key Gaps / Risks

1. **TestSprite DM test contract mismatch** — TC002 expects `should_reply=true`, but current behavior can intentionally suppress/alter replies depending payload/dedupe/low-signal rules. Need inspect generated test payload and align with API contract.
2. **Attendance/automation tests need deterministic fixtures** — TC005/TC007 fail because no suitable appointment/job exists at execution moment.
3. **Campaign endpoints remain failing** — generated campaign create payload returns 400. Need inspect request schema and add positive contract examples.
4. **CRM sync edge remains** — local API logs show some `crm_auth_failed` and live CRM conflict for over-capacity tests. Correct live CRM shows synced records, so auth works in some paths; conflict handling needs clearer UX.
5. **Prod deployment pending** — local fixed API passes core bug scenario; production/live Instagram path still needs deploy approval + controlled retest.

---
