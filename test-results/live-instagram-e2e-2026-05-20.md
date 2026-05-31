# Instagram Randevu Bot — Real Instagram Live E2E

Date: 2026-05-20 21:36-21:43 GMT+3
Tester: Craft Agent
Project: `C:\Users\oyunc\Desktop\instagram-randevu-bot`
Prod API: `https://instagram-randevu-bot.onrender.com`
Instagram thread: `bwrkkay` ↔ `doeldigital`

## Flow sent via real Instagram UI

1. `CANLI TEST 2026-05-20 21:36 - Merhaba, web sitesi ve Instagram DM otomasyonu için bilgi almak istiyorum.`
   - Bot replied: `Web tasarım paketimiz tek seferlik 12.900 TL, DM otomasyonu ise aylık 5.000 TL'dir. Detayları görüşmek için kısa bir ön görüşme planlayalım mı?`
   - Result: ✅ reply delivered in Instagram

2. `Evet planlayalım. Adım Canlı Test Kullanıcısı, telefonum 0555 222 33 44. Yarın saat 15:00 uygun mu?`
   - Note: Browser filled text twice visually in textbox before send, but backend processed extracted values once.
   - Bot replied: `Memnun oldum. Maalesef yarın 15:00 dolu, ancak 10:00, 11:00 veya 12:00 saatlerimiz müsait. Sizin için hangisi uygun?`
   - Result: ✅ reply delivered; name/phone/service/date/time extracted

3. `11:00 uygun, lütfen kaydı oluşturun.`
   - Bot replied in Instagram: `Yarın saat 11:00 için ön görüşmenizi not aldım. Detayları sizinle paylaşacağım.`
   - Result: ✅ confirmation reply delivered in Instagram

## Backend observations

After message 2:
- `/api/conversations?limit=5` shows user `67000808415`, username `bwrkkay`
- full_name: `Canli Test Kullanicisi`
- phone: `+905552223344`
- service: `Web Tasar�m`
- requested_date: `2026-05-21`
- requested_time: `15:00:00`
- state: `collect_name`
- appointment_status: `collecting`
- suggested slots: `10:00`, `11:00`, `12:00`

After message 3:
- Instagram confirmation reply appeared.
- Backend `/api/debug-state/67000808415` still showed:
  - requested_time: `15:00:00`
  - state: `collect_name`
  - appointment_status: `collecting`
  - appointment_id: `null`
- `/api/appointments` still showed only older appointment id `9` for same IG user (`Serkan Recber`, `15:00`). No new appointment for `Canli Test Kullanicisi` / `11:00` appeared during polling.

## Result

✅ Real IG inbound works.
✅ Bot replies are sent back to Instagram.
✅ Multi-turn info extraction works enough to capture name/phone/service/date/time.
✅ Bot correctly detects requested 15:00 is unavailable and suggests alternatives.
✅ Bot sends final confirmation for selected 11:00.

❌ Critical bug: final confirmation says appointment noted, but backend appointment table did not create/update a matching appointment.
❌ Critical bug: conversation state remains stale/wrong (`collect_name`, `collecting`, `appointment_id: null`) after final confirmation.
❌ Critical data bug: requested_time remains `15:00:00` in state after user selected `11:00`.
❌ Existing appointment for same IG user id may be interfering with new flow / duplicate guard / state update.

## Severity

High. User-facing Instagram flow claims appointment is booked, but backend persistence does not confirm it.

## Recommended fixes

1. Slot acceptance branch must update requested_time to selected suggested slot before confirmation.
2. Confirmation reply must only be sent after `create_appointment` succeeds.
3. If appointment creation fails, reply should ask user to retry or route human handoff.
4. Conversation state after final confirmation should become `completed`, `appointment_status=confirmed`, `appointment_id=<new id>`.
5. Existing IG user history should not block new appointment creation unless explicit duplicate appointment detected for same date/time/service.
6. Add regression test: real-like sequence initial inquiry → accept consult → provide name/phone/requested unavailable time → select suggested slot → assert appointment row exists for selected slot.
