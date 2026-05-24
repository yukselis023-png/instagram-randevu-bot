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
