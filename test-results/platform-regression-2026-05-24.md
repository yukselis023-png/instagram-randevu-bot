# Platform Regression — 2026-05-24

## Final Test Results
- Local pytest: 465 passed, 35 skipped, 2 warnings
- TestSprite: 10 passed, 0 failed

## Prod Platform API
| Endpoint | Status |
|----------|--------|
| `GET /api/platform/status` | ✅ 200 |
| `GET /api/tenants` | ✅ 200 |
| `GET /api/tenants/{slug}` | ✅ 200 |
| `POST /api/tenants` | ✅ 200 |
| `PATCH /api/tenants/{slug}/config` | ✅ 200 |
| `POST /api/tenant/{slug}/scrape` | ✅ 200, 5 services extracted |
| `POST /api/channel/webchat` | ✅ booking + cancel E2E |
| `GET /api/channel/whatsapp` | ✅ verify |
| `POST /api/followup/run` | ✅ 200 |
| `POST /api/handoff/notify` | ✅ 200 |
| `POST /api/process-instagram-message` | ✅ price + booking + cancel intact |

## Deployed
- Commit: `d085868`
- LLM endpoint: `afford-fun-thorough-hints.trycloudflare.com`
