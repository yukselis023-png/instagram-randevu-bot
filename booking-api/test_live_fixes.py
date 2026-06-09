#!/usr/bin/env python3
"""Fix sonrasi tam aks testi."""
import json, os, sys, requests, time, uuid

BACKEND_URL = os.environ.get("BACKEND_URL", "https://instagram-randevu-bot.onrender.com")
TEST_SENDER = f"fixtest_{uuid.uuid4().hex[:8]}"
results = []

def send_message(text):
    resp = requests.post(
        f"{BACKEND_URL}/api/process-instagram-message",
        json={"sender_id": TEST_SENDER, "message_text": text, "instagram_username": "testuser_fix"},
        timeout=30
    )
    if resp.status_code != 200:
        print(f"[FAIL] HTTP {resp.status_code}: {resp.text[:300]}")
        return None
    return resp.json()

# 1. Booking start
r = send_message("yeni randevu olusturmak istiyorum")
assert r and r.get("should_reply"), "No response"
print(f"[PASS] 1. Start: {r['reply_text'][:80]}")
results.append(("Start", True))

# 2. Servis sec
r = send_message("otomasyon")
assert r and r.get("should_reply"), "No response"
print(f"[PASS] 2. Service: {r['reply_text'][:80]}")
results.append(("Service", True))

# 3. Name
r = send_message("Ahmet Yilmaz")
assert r and r.get("should_reply"), "No response"
reply = r.get("reply_text", "")
print(f"[PASS] 3. Name: {reply[:80]}")
results.append(("Name", True))

# 4. Phone
r = send_message("05321234567")
assert r and r.get("should_reply"), "No response"
print(f"[PASS] 4. Phone: {r['reply_text'][:80]}")
results.append(("Phone", True))

# 5. Date+time
r = send_message("5 haziran 14:00")
assert r and r.get("should_reply"), "No response"
reply = r.get("reply_text", "")
appt_created = bool(r.get("appointment_created"))
state = r.get("conversation_state", "")
print(f"[{'PASS' if appt_created else 'INFO'}] 5. DateTime: {reply[:80]}")
print(f"    appointment_created={appt_created}, state={state}, id={r.get('appointment_id')}")
results.append(("DateTime", appt_created))

# 6. "ee hadi?" context test
r = send_message("ee hadi?")
assert r and r.get("should_reply"), "No response"
reply = r.get("reply_text", "")
bad = "gun" in reply.lower() and "saat" in reply.lower()
results.append(("Context after complete", not bad))
if bad:
    print(f"[FAIL] 6. 'ee hadi?' -> {reply[:80]}")
else:
    print(f"[PASS] 6. 'ee hadi?' OK: {reply[:80]}")

# 7. "randevum vardi hatirliyor musun?"
r = send_message("randevum vardi hatirliyor musun?")
assert r and r.get("should_reply"), "No response"
reply = r.get("reply_text", "")
print(f"[PASS] 7. Recall: {reply[:80]}")
results.append(("Recall", True))

# Stats
passed = sum(1 for _, ok in results if ok)
total = len(results)
print(f"\n{'='*50}")
print(f"RESULTS: {passed}/{total} passed")
for name, ok in results:
    print(f"  {'[PASS]' if ok else '[FAIL]'} {name}")

# CRM check
try:
    LIVE_CRM_URL = "https://rnjkilyiqnqiyqhwqdly.supabase.co"
    ANON_KEY = "sb_publishable_DhA_HhaX9lX8HOzTiep9gQ__sthJ0fx"
    crm_resp = requests.get(
        f"{LIVE_CRM_URL}/rest/v1/appointments?select=id,customer_name,service,date,time,status&user_id=eq.00000000-0000-0000-0000-000000000000&order=created_at.desc&limit=3",
        headers={"apikey": ANON_KEY}
    )
    if crm_resp.status_code == 200:
        appts = crm_resp.json()
        print(f"\n[INFO] CRM appts (last 3): {json.dumps(appts, indent=2, ensure_ascii=False)[:500]}")
    else:
        print(f"\n[INFO] CRM check failed: {crm_resp.status_code}")
except Exception as e:
    print(f"\n[INFO] CRM check error: {e}")
