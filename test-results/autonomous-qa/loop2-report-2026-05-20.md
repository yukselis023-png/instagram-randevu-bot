# Autonomous QA Loop 2 — 2026-05-20

## Actions

- Rebuilt local Docker API after code fixes.
- Found local DB SSL regression after rebuild: `sslmode=require` broke local Postgres (`server does not support SSL`).
- Fixed DB connection to use `sslmode=disable` for local hosts (`postgres`, `localhost`, `127.0.0.1`) and `require` elsewhere.
- Validated n8n workflow JSON.
- Ran local security/malformed/rate probes.
- Ran local API deep matrix.
- Ran exact live-like slot flow locally.
- Added stronger false-confirmation guard for phrases like `ön görüşme ... not aldım` without appointment creation.

## Regression

Command:
`cd booking-api && python -m pytest tests -q`

Result:
`463 passed, 35 skipped, 2 warnings`

Artifact:
`test-results/autonomous-qa/pytest-loop2-pass.log`

## Local infra

- Local API reachable after rebuild: `/health` OK.
- n8n reachable: `http://localhost:5678/` HTTP 200.
- Workflow validation: OK.

Artifacts:
- `test-results/autonomous-qa/n8n-workflow-validation.log`
- `test-results/autonomous-qa/local-security-matrix.json`
- `test-results/autonomous-qa/local-deep-matrix.json`
- `test-results/autonomous-qa/local-live-like-slot-flow.json`

## New findings

### High: single-shot name extraction unreliable
Local matrix single-shot full message often ended in `collect_name` despite name in text.
Example conflict probe:
- message includes `Adım Conflict Web 1`
- normalized `full_name=null`, phone/date/time/service set
- bot falsely replied `Yarın saat 10:00 için ön görüşmenizi not aldım...`
- no appointment created

Mitigation added:
- stronger false-confirmation guard patterns for `not aldım` appointment-like replies.

Still needs deeper fix:
- deterministic name extraction should catch `Adım Conflict Web 1` or reject invalid numeric suffix and ask name without confirmation.

### Medium: local LLM dependency flaky
Earlier local matrix produced generic fallback due DNS failure resolving Cloudflare LLM base URL.
System recovered with fallback, but UX poor.

### Medium/High: CRM sync auth failing locally
Docker logs show `crm_auth_failed` for test senders. Needs CRM credential/config validation or local CRM disabled mode.

### Medium: concurrency capacity questionable
10 concurrent Otomasyon appointments created same `14:00`; capacity says `Otomasyon & Yapay Zeka Cozumleri` capacity 2, but appointment service stored as `Otomasyon`, likely slug mismatch or alias issue.
Needs capacity alias mapping test/fix.

### Low/Env: debug/admin route mismatch before rebuild
Before rebuild `/debug-env` and cleanup route returned 404 because container was running stale code. Rebuild fixed app code load.

## Current state

Local unit/regression excellent: 463 pass.
System-level QA still shows real issues:
1. false confirmations from LLM around `not aldım` (guard improved, need retest in container after rebuild)
2. name extraction edge cases
3. service capacity alias mismatch
4. CRM sync auth local failures
5. prod old code not deployed

## Next loop

1. Rebuild API after latest guard patch.
2. Re-run local conflict probe; confirm false confirmation blocked.
3. Add tests/fix for `Adım <name>` extraction with numeric pollution.
4. Fix service capacity alias mapping (`Otomasyon` vs `Otomasyon & Yapay Zeka Cozumleri`).
5. CRM UI smoke via browser.
6. TestSprite rerun.
7. Prepare deploy/live retest checklist.
