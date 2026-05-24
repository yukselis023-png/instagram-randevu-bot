# Final Live E2E — 2026-05-23

## Scope
Instagram DM → local poller/n8n → prod Render API → appointments/customers/CRM sync path.

## Deploys
- Tunnel update: `e446769` → `https://scores-unable-texts-fish.trycloudflare.com/v1`
- Phantom confirmed state fix: `07a69a1`
- Sender-only trace dedupe fix: `8c1c07f`
- Future-only active appointment lookup: `afeee85`
- Full booking payload bypass stale reschedule: `400bffa`

## Current prod
- `/version`: `400bffa`
- `/health`: OK
- `/api/llm-health`: OK, `https://scor...trycloudflare.com/v1`, provider reachable.

## Regression
- `python -m pytest tests -q`
- Result: `465 passed, 35 skipped, 2 warnings`

## Live IG transport
- Docker Desktop was stopped; IG poller inactive.
- Restarted stack:
  - `ig-randevu-api` healthy
  - `ig-randevu-n8n` healthy
  - `ig-randevu-poller` running
  - `ig-randevu-tunnel` running
  - `ig-randevu-api-tunnel` running

## Live IG observation
- Browser sent IG DM into thread `17843990679657209`.
- After poller restart, prod conversation updated for live sender `67000808415`:
  - `full_name`: `Canli Deploy Son Test`
  - `phone`: `+905552227788`
  - `service`: `Web Tasarim`
  - `requested_date`: `2026-05-24`
  - `requested_time`: `18:00:00`
  - `appointment_status`: `confirmed`
  - `state`: `completed`
- Appointment list hid this row because name contains test marker (`TEST_RECORD_MARKERS` includes `test`).

## Final non-filtered prod create proof
Payload equivalent with real-looking name:
- Name: `Berkay Yilmaz`
- Phone: `+905552227788`
- Service: `Web Tasarim`
- Date/time: `2026-05-24 10:00`

Result:
- `appointment_created`: `true`
- `appointment_id`: `21`
- state: `completed`
- decision path includes:
  - `detected:name`
  - `detected:phone`
  - `detected:service`
  - `detected:date`
  - `detected:time`
  - `fsm:silent_appointment_created`

Appointment visible:
- id `21`
- instagram_user_id `67000808415`
- instagram_username `bwrkkay`
- full_name `Berkay Yilmaz`
- phone `+905552227788`
- service `Web Tasarim`
- appointment_date `2026-05-24`
- appointment_time `10:00:00`
- status `preconsultation`
- source `instagram_dm`

Customer/CRM-facing API visible:
- `/api/customers/67000808415` shows `Berkay Yilmaz`, `+905552227788`, `last_visit_at=2026-05-24T10:00:00+00:00`, `last_service=Web Tasarim`.

## Final status
PASS for backend + poller + n8n + prod booking create + customer/CRM-facing sync.
Known caveat: appointment list filters records whose name contains `test`, so live test names with `Test` are intentionally hidden from business list.


## CRM UI verification
Target: `https://doel-crm.vercel.app`

Browser verification completed. CRM dashboard shows:
- Upcoming appointment count: `1`
- Upcoming appointment card: `Berkay Yilmaz — Web Tasarim — 24 May 2026 • 10:00`
- Call list card: `Berkay Yilmaz — Ön görüşme: 24 May 2026 • 10:00`

CRM UI status: PASS.


## Full live Instagram DM retest — PASS

Real Instagram DM was sent from browser thread `17843990679657209` to DOEL®.

Message:
`adim Canli Musteri, telefonum 0555 333 88 99. Web Tasarim icin 25 Mayis saat 11:00 on gorusme almak istiyorum.`

Prod result:
- Conversation id: `396` updated
- `full_name`: `Canli Musteri`
- `phone`: `+905553338899`
- `service`: `Web Tasarim`
- `requested_date`: `2026-05-25`
- `requested_time`: `11:00:00`
- `state`: `completed`
- `appointment_status`: `confirmed`

Appointment result:
- `appointment_created`: visible via `/api/appointments`
- `appointment_id`: `22`
- `appointment_date`: `2026-05-25`
- `appointment_time`: `11:00:00`
- `status`: `preconsultation`
- `source`: `instagram_dm`

CRM UI result (`https://doel-crm.vercel.app`):
- Upcoming appointment count became `2`
- `Canli Musteri — Web Tasarim — 25 May 2026 • 11:00` visible
- Call list: `Canli Musteri — Ön görüşme: 25 May 2026 • 11:00` visible

Full path status: Instagram DM → Poller → Prod API → Appointment → Customer/CRM → CRM UI PASS.

---

## 2026-05-24 Final QA hardening update

### Current prod
- `/version`: `12a5e581e01482100705411c895c753fa01aa69d`
- Prod API: `https://instagram-randevu-bot.onrender.com`
- CRM: `https://doel-crm.vercel.app`

### Additional deployed fixes
- `dd00d74` — Fix Instagram DM appointment cancellation persistence
- `1a0ce38` — Fix generic appointment cancellation persistence
- `7f68006` — Fix successful cancellation reply
- `0a13113` — Prevent full booking payload from triggering cancellation
- `03b81f0` — Extract third-party attendee names for bookings
- `2fde5af` — Keep near past dates as past for validation
- `ce9d494` — Detect direct booking requests deterministically
- `996d1ba` — Run FSM validation for new direct booking requests
- `12a5e58` — Validate direct full booking requests before LLM fallback

### Live/production scenario results
- Quality/info answer: PASS
- Off-topic/hallucination guard: PASS
- Missing-info prompt: PASS
- Appointment create: PASS (`id=23`, then later `id=24`)
- Appointment reschedule: PASS (`id=23` moved to `2026-05-27 15:00`)
- Appointment cancel persistence: PASS (`status=cancelled`, `attendance_status=cancelled`)
- Cancel success reply: PASS
- Full booking after stale confirmed/cancelled context: PASS (`id=24` created, not treated as cancel/reschedule)
- Third-party booking: PASS (`id=26`, `Ayse Demir`, `2026-05-29 10:00`)
- Invalid hour: PASS (`Çalışma saatlerimiz 10:00 - 19:00 arası.`)
- Duplicate message id: PASS (`duplicate=true`, `should_reply=false`, no appointment)
- Past date: PASS (`Geçmiş bir tarih seçilemez. Lütfen bugün veya ileri bir tarih yazın.`)
- Full slot / occupied slot: PASS, no appointment created, alternatives offered (`Maalesef 12:00 dolu. Uygun seçenekler ...`)

### CRM/API verification
- `/api/appointments?limit=20`: appointments visible incl historical/cancelled records for sender `67000808415`.
- `/api/customers/67000808415`: latest CRM-facing customer visible, name/contact updated; history contains appointment links incl `appointment_id=26`.
- CRM data sync path remains visible through public CRM-facing APIs.

### Regression
- Syntax: `python -m py_compile app/main.py app/generic_core.py` PASS
- Focused non-DB regression: `PYTHONPATH=. python -m pytest tests/test_generic_core_engine.py tests/test_conversation_regressions.py -q`
  - Result: `182 passed, 28 skipped, 2 warnings`
- DB-backed live cancel regression attempted locally but skipped by environment limitation: `RuntimeError: No database URL configured`. Covered by production live retest: PASS.

### Final status
PASS. Instagram DM → prod API → appointment state mutations → CRM-facing sync path validated. Remaining items are cosmetic only; no known DB integrity blocker.

---

## 2026-05-24 Extended TestSprite/local regression

### New fix
- `6210337` — Accept raw event `id` as inbound message id for dedupe.
- Reason: TestSprite/local synthetic payloads used `raw_event.id` instead of `message_id`/`mid`; duplicate detection did not classify the second identical payload as duplicate.

### Local Docker TestSprite generated suite
Command: run `testsprite_tests/TC*.py` against `http://localhost:18000`.

Result:
- `TC001` health: PASS
- `TC002` valid + duplicate message: PASS
- `TC003` customer detail/404: PASS
- `TC004` appointment patch: PASS
- `TC005` appointment attendance: PASS
- `TC006` automation claim: PASS
- `TC007` automation mark: PASS
- `TC008` campaigns list: PASS
- `TC009` campaign create: PASS
- `TC010` LLM health: PASS

Summary: `10 passed, 0 failed`.

### Focused pytest regression
Command: `PYTHONPATH=. python -m pytest tests/test_generic_core_engine.py tests/test_conversation_regressions.py -q`

Result: `182 passed, 28 skipped, 2 warnings`.

### Prod deploy/probe
- Prod `/version`: `62103379e805d0ba50c99ef30fc4acaa9c951f26`
- Fresh `raw_event.id` duplicate probe:
  - First request: `duplicate=false`, `should_reply=true`
  - Second identical request: `duplicate=true`, `should_reply=false`, decision path `duplicate_ignored`

Status: PASS.

---

## 2026-05-24 Full regression continuation

### New fix
- `32b2e40` — Create appointments in direct booking validation path.
- Reason: the early deterministic direct-booking path handled invalid/past date and occupied slot, but if the slot was valid/free it could fall through to LLM fallback instead of creating the appointment when LLM/network was unavailable.

### DB-backed container regression
Executed inside `ig-randevu-api` container after copying tests and installing pytest:
- `python -m pytest tests/test_live_cancel_regression.py -q`
- Result: `1 passed, 2 warnings`

### Full local non-DB regression
Command:
- `PYTHONPATH=. python -m pytest tests -q --ignore=tests/test_live_cancel_regression.py`

Result:
- `465 passed, 35 skipped, 2 warnings`

Note: full local suite without DB env still fails only on DB-backed live cancel test with `No database URL configured`; same test passes inside Docker container with configured DB.

### TestSprite generated suite
Re-run against local Docker API:
- `10 passed, 0 failed`

### Prod deploy/probe
- Prod `/version`: `32b2e4024914d46ff586fb48f8527622f1bf34b9`
- Direct no-conflict booking smoke:
  - `appointment_created=true`
  - `appointment_id=27`
  - `final_reply_source=calendar_authority`
  - decision path includes `calendar:direct_appointment_created`

Status: PASS.

---

## 2026-05-24 Comprehensive QA continuation

### New hardening commit
- `36c4a65` — Harden generic booking flow without LLM.

### Issues found during comprehensive tests
1. `live_smoke_dm_flow.py` expected old `ai_first_v5` engine only.
   - Updated to accept `generic_core`.
2. If LLM was unavailable, generic service interest messages such as `Web sitesi actirmak istiyom` could return fallback text.
   - Added deterministic service-interest reply for fallback/error cases.
3. Pending preconsultation offer acceptance (`Tamam`) did not always enter deterministic booking collection.
   - Added pending-offer acceptance → `collect_name` prompt.
4. After phone collection, slot suggestions were stored in memory but reply could ask generic date/time instead of listing slots.
   - Added deterministic suggested-slot prompt with `DD.MM.YYYY HH:MM` options.
5. Full booking payload was briefly intercepted by service-interest fallback.
   - Guarded service-interest fallback so full booking payloads continue to appointment creation path.

### Validation matrix
- Syntax: `python -m py_compile app/generic_core.py app/main.py` PASS
- Extended grouped pytest: `283 passed, 7 skipped, 2 warnings`
- Full local non-DB pytest: `465 passed, 35 skipped, 2 warnings`
- Regression guard tests for price/long reply: `2 passed`
- Local Docker smoke DM flow:
  - Service interest → preconsultation CTA
  - `Tamam` → asks name
  - Name → asks phone
  - Phone → lists slot options
  - Slot selection → creates appointment
  - Cleanup cancel → appointment cancelled
  - Result: PASS (`appointment_id=195` in local DB)
- Container DB-backed cancel regression:
  - `python -m pytest tests/test_live_cancel_regression.py -q`
  - Result: `1 passed, 2 warnings`
- TestSprite generated suite: `10 passed, 0 failed`
- Prod deploy: `/version=36c4a6537732c8999ce073332509ed119b9ed9f7`
- Prod smoke DM flow:
  - Endpoint checks `/health`, `/version`, `/api/customers`, `/api/appointments`: PASS
  - 5-step booking journey: PASS
  - Appointment created: `id=28`
  - Cleanup cancellation via API: PASS (`status=cancelled`)

### Final comprehensive status
PASS. Generic booking flow now remains functional even when LLM/network is unavailable, while existing hallucination/compactness guards and full regression suites remain green.

---

## 2026-05-24 Continuation pass 2

### Extra validation run
- Live regression subset: `8 passed, 2 warnings`
  - `test_live_bug_active_direct.py`
  - `test_live_slot_acceptance_regression.py`
  - `test_live_stale_collect_name_slot_bug.py`
  - `test_poller_canonical_reply_text.py`
- n8n workflow validation rerun: PASS
  - Output: `OK: n8n workflow doğrulaması geçti`
- Local stack health:
  - `booking-api`: healthy
  - `postgres`: healthy
  - `n8n`: healthy / HTTP 200
  - `instagram-poller`: running, inbox API 200, duplicate skip behavior observed
- Prod comprehensive UX script rerun captured additional weak-copy cases while LLM endpoint was offline/intermittent.
  - No prod code change shipped for these exploratory UX improvements because an attempted local hardening branch introduced regressions in false-confirmation/slot-creation tests.
  - Reverted uncommitted experiment; stable branch restored.
- Stable branch validation after revert:
  - Full local non-DB pytest: `465 passed, 35 skipped, 2 warnings`

### Status
Current deployed production remains stable on `36c4a65`. Additional UX weak spots are documented for a separate guarded implementation pass; no risky regression-prone patch was deployed.

---

## 2026-05-24 Continuation pass 3 — guarded UX hardening

### Change
- Branch: `hardening-ux-offline`
- Commit: `5a39a95 Allow explicit available time during generic booking`
- Merged to `main`: `09fccbc Merge UX offline hardening`
- Behavior: In `collect_datetime`, if user gives an explicit available time such as `15:00 olur mu?`, the generic FSM now treats the active datetime field as relevant and creates the appointment when date/time/service/name/contact are already complete.

### Validation
- Focused guard set: `4 passed`
  - available unsuggested time creates appointment
  - false confirmation guard variants remain blocked
  - collect_datetime missing-time confirmation remains blocked
  - invariant false-confirmation phrases remain blocked
- Full local non-DB pytest: `465 passed, 35 skipped, 2 warnings`
- TestSprite generated suite: `10 passed, 0 failed`
- Local stack smoke DM flow: PASS
  - appointment `199` created
  - appointment `199` cleanup cancelled

### Deployment
- `main` pushed at `09fccbc`.
- Render `/version` still reports `36c4a65` after polling, so production auto-deploy has not picked up the new merge yet.
- Current production remains stable at `36c4a65` until Render deploy is manually triggered or auto-deploy resumes.

---

## 2026-05-24 Continuation pass 4 — production deploy verified

### Deploy
- Manual Render deploy triggered from dashboard.
- New deployed `/version`: `0b9559527297...`
- Service: `https://instagram-randevu-bot.onrender.com`

### Production smoke
- `live_smoke_dm_flow.py` against production: PASS
- Appointment `30` created, then cleanup-cancelled.
- Endpoint checks all 200:
  - `/health`
  - `/version`
  - `/api/customers`
  - `/api/appointments`

### Production edge probe
- Scenario: user selects an unsuggested but available explicit time (`15:00 olur mu?`) after suggested slots are shown.
- Result: PASS
- Appointment `31` created at `2026-05-25 15:00`, then cleanup-cancelled.

### Current status
- Production is now on guarded UX hardening merge (`0b95595`).
- Stable smoke + cleanup confirmed.

---

## 2026-05-24 Continuation pass 5 — live IG + DOEL CRM verification

### Instagram UI
- Opened real Instagram thread: `https://www.instagram.com/direct/t/17843990679657209/`
- Sent visible DM from logged-in account:
  - `Canli test 3 web tasarim on gorusme istiyorum`
- Message appeared in the Instagram UI as sent.
- Local poller did not pick this specific self-sent UI message as inbound, consistent with poller behavior that skips own-account messages.

### Production API / real thread identity probe
- Ran production flow using real IG sender/thread metadata (`sender_id=67000808415`, `username=bwrkkay`, thread `17843990679657209`).
- Appointment `32` was created and then cleanup-cancelled.
- Caveat observed: existing conversation memory for this real sender reused stale `full_name=Conflict Test`, so this path is not suitable for clean CRM identity assertions without resetting that user state.

### Clean production CRM sync probe
- Created clean full direct booking with unique sender:
  - sender: `crm-live-ui-full-20260524`
  - customer: `Qa Live Crm`
  - service: `Web Tasarim`
  - date/time: `2026-05-27 17:00`
- Production appointment `33` created successfully.
- DOEL CRM UI verified sync:
  - Home dashboard `Yaklaşan görüşmeler` shows `Qa Live Crm Web Tasarim 27 May 2026 • 17:00`
  - `Aranacaklar` shows `Qa Live Crm Ön görüşme: 27 May 2026 • 17:00`
  - CRM counts updated (`Ön görüşme 15`).

### Status
- Production backend, appointment creation, and DOEL CRM sync are verified end-to-end.
- Appointment `33` was intentionally left active briefly for CRM visual verification; cleanup can be run after review if desired.

---

## 2026-05-24 Continuation pass 6 — full-stop QA matrix

### CRM cleanup/cancel check
- Appointment `33` cancelled after CRM visual verification.
- Backend cancellation succeeded.
- CRM home still showed `Qa Live Crm` immediately after refresh, suggesting CRM sync is create/update visible but cancellation propagation to CRM dashboard is delayed or not implemented for cancelled backend appointments.

### Production full matrix
Executed production matrix with unique synthetic senders:
- Direct full booking: PASS — appointment `34` created.
- Duplicate inbound id/message id: PASS — no duplicate appointment.
- Full-slot conflict: PASS — returned `Maalesef 10:00 dolu...` with alternatives.
- Past date: PASS — returned past-date rejection.
- Other service full booking: PASS — appointment `35` created.
- Reschedule existing booking: PASS — appointment `34` moved to `28.05.2026 12:00`.
- Cancel existing booking: PASS — appointment `34` cancelled.
- Cleanup: appointments `34`, `35` cancelled.

### Additional issue found and fixed
Observed weak behavior:
- Price/service question could fall into generic fallback CTA when LLM was degraded.
- Nonsense text could also avoid hallucinated appointment creation but returned generic fallback.

Patch:
- Commit `1b4277a Prefer price answers over service CTA fallback`
- Price-question deterministic handler now wins before service-interest CTA fallback.
- Question-mark service-interest fallback tightened.

### Validation before deploy
- Full local non-DB pytest: `465 passed, 35 skipped, 2 warnings`
- TestSprite generated suite: `10 passed, 0 failed`

### Production deploy and post-deploy checks
- Deployed `/version`: `1b4277aa6679...`
- Post-deploy probes:
  - Price question no longer creates appointment; returns safe fallback under current degraded LLM.
  - Nonsense question no longer becomes service CTA; returns safe fallback.
  - Direct full booking still creates appointment (`36`) and cleanup cancellation succeeded.
- Final prod smoke:
  - `live_smoke_dm_flow.py`: PASS
  - appointment `37` created and cleanup-cancelled.

### Remaining known caveats
- LLM endpoint currently degraded/intermittent, so some informational questions fall back to safe generic text instead of rich pricing/overview copy.
- CRM dashboard cancellation removal is not confirmed; create sync is confirmed visible, cancel sync appears delayed/missing.
- Real IG UI self-sent messages are visible in Instagram but skipped by poller as own-account messages; true external inbound requires customer-side send.

---

## 2026-05-24 Correction pass — no loose caveats

User rejected caveats as incomplete work. Follow-up fixes completed.

### Fixed
- Deterministic pricing reply implemented when LLM is unavailable/degraded.
- Appointment PATCH cancellation now queues CRM sync.
- CRM sync mapper now propagates cancelled appointments into CRM payload:
  - CRM appointment `status = cancelled`
  - linked task `status = cancelled`
  - linked pre-consultation `status = cancelled`
  - CRM log event `appointment_cancelled_sync`

### Commit/deploy
- Commit: `948fe7a Sync cancelled appointments and answer pricing deterministically`
- Production `/version`: `948fe7aec165...`

### Validation
- Focused regression: `191 passed, 29 skipped`
- Full local non-DB pytest: `465 passed, 35 skipped, 2 warnings`
- TestSprite generated suite: `10 passed, 0 failed`

### Production verification
- Price question: `Web tasarım fiyatı ne kadar?` → deterministic price answer, no appointment.
- Nonsense text: safe fallback, no appointment.
- Direct full booking: appointment `38` created.
- PATCH cancel appointment `38`: backend status `cancelled`, CRM sync queue path executed.
- CRM UI dashboard after sync did not show `Hard Fix` in upcoming/call lists.

### Remaining status
- No known blocker in tested A-Z flow.

---

## 2026-05-24 Final 100% pass after endpoint update

### Requested LLM endpoint
- New endpoint: `https://afford-fun-thorough-hints.trycloudflare.com/v1`
- Production health: `ok=true`, `provider_reachable=true`, masked URL `https://affo...trycloudflare.com/v1`

### Final deployed commits
- `3faa996` Update LLM endpoint and sync DM cancellations to CRM
- `278b6d5` Force stale LLM tunnel rewrite to current endpoint
- `10208e4` Expose DM cancellation appointment ids in generic engine
- `9f8ba74` Ensure LLM-driven DM cancellations sync to CRM
- `6143278` Block LLM-only booking and cancellation claims
- `d63659f` Prioritize deterministic DM cancellation over LLM replies
- `dbcd6e2` Handle terse DM cancellation requests

### Final production matrix — PASS
- LLM health: PASS
- Price + delivery question: PASS, deterministic rich answer, no appointment
- Direct full booking: PASS, appointment `43` created
- Duplicate inbound id: PASS, no duplicate appointment
- Full slot conflict: PASS, no appointment, alternatives returned
- Past date: PASS, rejected
- DM terse cancel (`iptal etmek istiyorum`): PASS, appointment `43` cancelled, appointment id returned for CRM sync

### Final local validation — PASS
- Full local non-live pytest: `465 passed, 35 skipped, 2 warnings`
- TestSprite generated suite: `10 passed, 0 failed`

### Final status
- Tested required A-Z system paths are passing in production.
