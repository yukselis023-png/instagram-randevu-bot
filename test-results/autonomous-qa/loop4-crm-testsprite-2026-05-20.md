# Autonomous QA Loop 4 — Live CRM + TestSprite

## Correct CRM target

User corrected CRM URL:
`https://doel-crm.vercel.app`

Login was already active after verification.

## Live CRM browser smoke

### Dashboard
Loaded successfully.
Visible synced test records:
- Ayse Yilmaz — Web Tasarim — 21 May 2026 10:00
- Con User — Otomasyon — 21 May 2026 14:00
- Serkan Recber — Otomasyon — 21 May 2026 15:00
- Kap Ali — Otomasyon — 21 May 2026 16:00

### Ön görüşmeler
URL:
`https://doel-crm.vercel.app/preconsultations`

Result:
- Rendered successfully.
- Open preconsultations count: 6.
- Search `Ayse` narrowed list to synced test record.

### Randevular
URL:
`https://doel-crm.vercel.app/appointments`

Result:
- Rendered successfully.
- Current appointment list empty (`Randevular0`), expected because bot-created records are preconsultations.

### Müşteriler
URL:
`https://doel-crm.vercel.app/customers`

Result:
- Rendered successfully.
- Synced customers visible:
  - Kap Ali
  - Ayse Yilmaz
  - Con User
  - Serkan Recber
  - Ahmet Vatansever
  - Selin Yilmaz

### Browser console/network
Only persistent console error:
`Permissions policy violation: unload is not allowed in this document.`

No blocking app runtime error seen.
Network failed list showed stale Instagram WebSocket entries from existing browser session, not CRM API failures.

## TestSprite rerun

Command executed via TestSprite MCP CLI.

Raw report:
`testsprite_tests/tmp/raw_report.md`

Completed report:
`testsprite_tests/testsprite-mcp-test-report.md`

Result:
- Total: 10
- Passed: 5
- Failed: 5
- Pass rate: 50%

Passed:
- TC001 health
- TC003 customer detail / 404
- TC004 appointment patch
- TC006 automation claim shape
- TC010 LLM health

Failed:
- TC002 DM process duplicate/should_reply expectation
- TC005 attendance mark lacks suitable appointment fixture
- TC007 automation mark lacks due job fixture
- TC008 campaign list/create setup returns 400
- TC009 campaign create returns 400

Interpretation:
- Core live bug fixed locally by separate local probes: DB appointment creation now happens for live-like unavailable-slot acceptance.
- TestSprite remaining failures are mostly fixture/schema/contract mismatches, except campaign endpoints may need real schema fix.

## Remaining engineering queue

1. Inspect TestSprite-generated TC002 payload; align should_reply/dedupe contract or add compatibility.
2. Add deterministic test fixture setup for attendance and automation mark.
3. Inspect/fix campaign create schema or update API contract docs.
4. Improve race conflict UX: full slot during create currently can produce handoff for some concurrent losers; better to return alternatives.
5. Prod deploy approval + controlled live IG retest.
