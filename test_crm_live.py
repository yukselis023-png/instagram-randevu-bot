import urllib.request
import json

BASE_URL = "http://127.0.0.1:18000"

def get(path):
    print(f"\n--- GET {path} ---")
    try:
        req = urllib.request.Request(f"{BASE_URL}{path}")
        with urllib.request.urlopen(req, timeout=5) as res:
            data = json.loads(res.read().decode('utf-8'))
            print(json.dumps(data, indent=2, ensure_ascii=False))
    except Exception as e:
        print("Hata:", e)

def post(path, payload):
    print(f"\n--- POST {path} ---")
    try:
        data = json.dumps(payload).encode('utf-8')
        req = urllib.request.Request(f"{BASE_URL}{path}", data=data, headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=5) as res:
            resp_data = json.loads(res.read().decode('utf-8'))
            print(json.dumps(resp_data, indent=2, ensure_ascii=False))
    except Exception as e:
        print("Hata:", e)

if __name__ == "__main__":
    print("Mevcut Segmentleri Çekiyoruz...")
    get("/api/crm/segments")
    
    print("\nROI / Kazanç Özetini Çekiyoruz...")
    get("/api/roi-summary")
    
    print("\nMüşteri randevu durumunu No-Show yapalım (Önceki test kaydı var varsayarak)")
    # Not: Gerçek bir trace_id vermek gerekir. Ancak mock için endpointi gösteriyoruz.
    print("-> Bu test için Panel üzerinden (crm_panel.html) bir müşterinin durumunu No-Show yapmayı deneyebilirsin.")
