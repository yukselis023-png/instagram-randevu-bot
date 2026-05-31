import urllib.request, json, sys, time, random, string
BASE = 'https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
uid = ''.join(random.choices(string.ascii_lowercase, k=6))
SENDER = f'strict-journey-{uid}'

def norm(s): return s.lower().replace('ı','i').replace('ş','s').replace('ğ','g').replace('ü','u').replace('ö','o').replace('ç','c')
def post(msg):
    req = urllib.request.Request(BASE, data=json.dumps({'sender_id': SENDER, 'message_text': msg}).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as resp: return json.load(resp)

msgs = [
  ("merhaba",                                           ['merhaba', 'size nasil yardimci'], None),
  ("Bir arkadaşım sizi önerdi onun için yazdım",        ['tesekkur'], ['nasil yardimci']),
  ("Ben randevularınızı merak ettim?",                  ['iki sekilde yardimci', 'on gorusme planlayabiliriz', 'hangisini merak'], ['kuafor', 'berber', 'salon']),
  ("Hizmetleriniz neler?",                              ['web tasarim', 'reklam'], None),
  ("Bu kadar mı hizmetleriniz?",                        ['bunlarin altinda instagram yonetimi', 'reklam kurulumu', 'landing page'], ['size nasil yardimci', 'otomasyon & yapay zeka']), 
  ("siz saç kesiyor muydunuz?",                         ['hayir'],            ['en mantikli', 'berber']),
  ("İşletmeniz ne zaman kuruldu?",                      ['kurulus','yonlendirebilirim'], ['paket']),
  ("Berkay bey orada mı?",                              ['berkay bey degilim'], ['buradayim']),
  ("Berkay siz misiniz?",                               ['berkay bey degilim'], ['uzman bir ekibiz']),
  ("güzellik salonu için sosyal medya yapıyor musunuz?",None,                 ['hayir', 'biz guzellik']),
  ("Sadece düz müşteri istiyorum",                      ['direkt musteri kazanmaksa'], None),
  ("Website hizmetinizi gördüm, bana uygun mu merak ediyorum",['web sitesinden beklentiniz onemli'], None),
  ("fiyatları nedir?",                                  ['fiyat','kapsam'],   ['Size nasil yardimci']),
  ("aylık ne kadar tutar?",                             ['fiyat'],            None),
  ("tamam ilginç geldi, nasıl başlayabiliriz?",         None,                 None),
  ("önce görüşme yapalım mı?",                          ['adinizi','soyadinizi'], None),
  ("sosyal medya yönetimi için",                        ['adinizi','soyadinizi'], ['topluluk']),
  ("Ön görüşmede ne konuşacağız?",                      ['hedefini','kapsam','mevcut durumunu'], ['soyadinizi yazar misiniz']),
  ("Ayşe Kaya",                                         ['telefon','numara'], None),
  ("ama ben sadece fiyat bilgisi almak istemiştim",     None,                 None),
]

issues = []
version = "Unknown"
try:
    with urllib.request.urlopen("https://instagram-randevu-bot.onrender.com/version", timeout=5) as r:
        version = json.load(r).get('version','?')[:12]
except: pass

print(f"\n=======================================================")
print(f"YENİ FULL JOURNEY ({SENDER}) - BUG 1,2,3 FIXED")
print(f"Commit/Version in Production: {version}")
print(f"=======================================================\n")
for i,(msg,must_have,must_not) in enumerate(msgs,1):
    try:
        d = post(msg); reply = d.get('reply_text',''); path = d.get('decision_path',[]); n = norm(reply)
        fails = [f"EKSIK:'{w}'" for w in (must_have or []) if norm(w) not in n] + [f"OLMAMALI:'{w}'" for w in (must_not or []) if norm(w) in n]
        print(f"[{'OK' if not fails else 'XX'} M{i:02d}] User: {msg}\n  BOT: {reply[:150]}...\n  PATH: {path[-1] if path else '?'}")
        if fails: print(f"  FAIL: {fails}"); issues.append(f"M{i}: {fails}")
    except Exception as e: print(f"[ERR M{i:02d}] User: {msg}\n  ERROR {e}"); issues.append(f"M{i}: ERROR {e}")
    time.sleep(0.3)

print(f"\nSONUC: {len(msgs)-len(issues)}/{len(msgs)} PASS | {len(issues)} SORUN")
if not issues: print(f"{len(msgs)}/{len(msgs)} - TUM MESAJLAR VE KALITE SENARYOLARI KUSURSUZ CALISIYOR!")
