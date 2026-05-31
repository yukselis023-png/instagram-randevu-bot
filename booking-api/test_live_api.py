import requests
import time
import uuid
import sys
import json

TARGET_VERSION = "2ebab7b"
BASE_URL = "https://instagram-randevu-bot.onrender.com"
API_URL = f"{BASE_URL}/api/process-instagram-message"

def wait_for_deploy():
    print(f"Waiting for version to contain {TARGET_VERSION}...")
    for _ in range(40):
        try:
            r = requests.get(f"{BASE_URL}/version")
            if r.status_code == 200:
                ver = r.json().get("version", "")
                if TARGET_VERSION in ver:
                    print(f"\n[OK] Deploy detected! Active version: {ver}")
                    return ver
            print(".", end="", flush=True)
        except Exception as e:
            print("x", end="", flush=True)
        time.sleep(5)
    print("\nTimeout waiting for deploy!")
    sys.exit(1)

def send_message(sender_id, text, idx):
    payload = {
        "sender_id": sender_id,
        "instagram_username": "live_test_user",
        "message_text": text
    }
    st = time.time()
    try:
        r = requests.post(API_URL, json=payload, timeout=20)
        dur = time.time() - st
        if r.status_code == 200:
            data = r.json()
            return data.get("reply_text") or "(No Reply text in json)", dur, data
        else:
            return f"HTTP {r.status_code} - {r.text}", dur, None
    except Exception as e:
        return f"Error: {e}", time.time() - st, None

def run_isolated_test(scenario_name, texts):
    print(f"\n--- Running Isolated Test: {scenario_name} ---")
    sid = f"iso_{uuid.uuid4().hex[:6]}"
    for i, t in enumerate(texts):
        print(f"User [{sid}]: {t}")
        reply, dur, _ = send_message(sid, t, i)
        print(f"Bot  [{dur:.2f}s]: {reply}\n")
    return sid

def run_20_message_journey():
    print("\n--- Running 20-Message Journey ---")
    sid = f"journey_{uuid.uuid4().hex[:6]}"
    
    messages = [
        "Ben dövmeciyim, sitenizi gördüm merak edip yazdım",
        "Ne gibi hizmetleriniz var, bana ne uyar?",
        "Tamam fiyatlar nedir bu dijital pazarlama işinde?",
        "Bana çok pahalı geldi, biraz indirim yapar mısınız?",
        "O zaman olur gibi, nasıl randevu alabiliriz?",
        "Perşembe sabah 10 uygun mudur?",
        "Evet",
        "Pardon cidden siz dövme yapıyor musunuz yoksa?", # Capability sorusu
        "Anladım, adım Vural Kaygusuz",
        "Telefon 0532 999 88 77",
        "Teşekkürler, ön görüşmede görüşmek üzere",
        "Adresi atar mısınız?",
        "Sitenizi kim kodladı?",
        "Hangi teknolojiyi kullandınız?",
        "E ticaret yapıyor musunuz?",
        "Ben de aynı botu istiyorum fiyat nedir?",
        "Aslında iptal etmek istiyorum.",
        "Şaka şaka, kalsın.",
        "Görüşürüz.",
        "Sağ olun."
    ]
    
    for i, t in enumerate(messages):
        print(f"User [{i+1}/20]: {t}")
        reply, dur, data = send_message(sid, t, i)
        print(f"Bot  [{dur:.2f}s]: {reply}\n")
        
        # log down the response
        filename = f"generic-live20-step{i+1}-{sid}.json"
        with open(filename, "w", encoding="utf-8") as f:
            if data:
                json.dump(data, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    act_ver = wait_for_deploy()
    print("Version verified. Starting tests...\n")
    
    run_isolated_test("Target A", ["Ben dövmeciyim, sitenizi gördüm merak edip yazdım"])
    run_isolated_test("Target B", ["Dövmeciyim ben, sitenizi gördüm"])
    run_isolated_test("Target C", ["Siz dövme yapıyor musunuz?"])
    
    run_20_message_journey()
    print("\nDone.")
