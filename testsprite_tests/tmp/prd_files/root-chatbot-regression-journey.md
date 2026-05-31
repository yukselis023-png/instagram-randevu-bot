# Chatbot Regression Journey

## Final stabilization baseline

- Stable behavior commit: `16fd1f585b8c6744c189f01975e0f3bcd2df0202`
- Scope of that stabilization:
  - Durable API-level inbound idempotency for generic Instagram DM processing.
  - Natural generic service overview formatting.
  - No new sector-specific conversation rules.
  - FSM, booking, CRM sync, and business config schema unchanged.

## Duplicate inbound validation

Same Instagram-style inbound payload was sent twice with the same `platform`, `sender_id`, `message_id`, and `trace_id`.

Result:

- First request: `should_reply=true`, `duplicate=false`
- Second request: `should_reply=false`, `duplicate=true`
- Second request did not call the LLM and did not create a second outbound response.

## Service overview validation

Input:

> Tam olarak ne yapıyorsunuz?

Stable output style:

> Kısaca işletmelerin dijitalde daha profesyonel görünmesini ve daha fazla müşteri almasını sağlıyoruz. web sitesi, mesaj/randevu otomasyonu, reklam yönetimi ve sosyal medya tarafında destek veriyoruz. Önceliğiniz daha fazla müşteri kazanmak mı, yoksa gelen mesaj/randevu sürecini düzenlemek mi?

Validation:

- No raw `Label: detail; Label: detail` catalog dump.
- Uses configured `service_catalog` generically.
- Short DM-style answer with one clear question.

## 20-message production journey

Result:

- `FAILS=[]`
- `appointment_created_count=1`
- `final_state=completed`
- `full_name` captured correctly: `Ahmet Yilmaz` / intended `Ahmet Yılmaz`
- `phone` captured correctly: `+905551234567`
- `appointment_id=69`

## Known operational open items

- Dirty Instagram thread history is not reliable for fresh-account acceptance; use a new Instagram account/thread for true clean acceptance.
- Cloudflared Quick Tunnel is not a permanent production solution. If the local PC/tunnel stops, production LLM calls can fail.
- `/api/llm-health` must stay aligned with the production primary model (`LLM_MODEL`) rather than old micro-model quota state.

## Recommended production roadmap

1. Replace Cloudflared Quick Tunnel with a durable hosted LLM endpoint or managed provider endpoint.
2. Keep `LLM_MODEL` as the source of truth for primary reply health checks.
3. Monitor `/api/llm-health` for primary model reachability and latency.
4. Keep duplicate-inbound regression in release checks by replaying the same `message_id` twice and verifying the second response is `duplicate=true` / `should_reply=false`.
