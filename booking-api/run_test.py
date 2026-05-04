import urllib.request, json, sys, time, random, string

BASE = 'https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
uid = ''.join(random.choices(string.ascii_lowercase, k=6))
SENDER = f'journey-finalest-{uid}'

def pr(t): sys.stdout.buffer.write((t+"\n").encode('utf-8')); sys.stdout.buffer.flush()
def norm(s): return s.lower().replace('ı','i').replace('ş','s').replace('ğ','g').replace('ü','u').replace('ö','o').replace('ç','c')
def post(msg):
    req = urllib.request.Request(BASE, data=json.dumps({'sender_id': SENDER, 'message_text': msg}).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as resp: return json.load(resp)

msgs = [
  # BUG 3: Size nasil yardimci olabiliriz only here!
  ("merhaba",                                           ['merhaba', 'size nasil yardimci'], None),
  ("Bir arkadaşım sizi önerdi onun için yazdım",        ['tesekkur', 'web sitesi', 'reklam'], ['nasıl yardımcı']),
  # BUG 1: Ambiguous Appointment Disambiguation
  ("Ben randevularınızı merak ettim?",                  ['iki sekilde yardimci', 'on gorusme planlayabiliriz ya da isletmeniz', 'hangisini merak'], ['kuafor', 'berber', 'salon']),
  # BUG 2: Repeated Service List Fix
  ("Hizmetleriniz neler?",                              ['web tasarim', 'reklam'], None),
  ("Bu kadar mı hizmetleriniz?",                        ['bunlarin altinda instagram yonetimi', 'reklam kurulumu', 'landing page'], ['Size nasil yardimci olabiliriz']), 
  
  ("siz saç kesiyor muydunuz?",                         ['hayir'],            ['en mantikli', 'berber']),
  ("İşletmeniz ne zaman kuruldu?",                      ['kurulus','yonlendirebilirim'], ['paket']),
  ("Berkay bey orada mı?",                              ['berkay bey degilim'], ['buradayim']),
  ("Berkay siz misiniz?",                               ['berkay bey degilim'], ['uzman bir ekibiz']),
  ("güzellik salonu için sosyal medya yapıyor musunuz?",None,                 ['hayir', 'biz guzellik']),
  ("Sadece düz müşteri istiyorum",                      ['direkt musteri kazanmaksa'], None),
  ("Website hizmetinizi gördüm, bana uygun mu merak ediyorum",['web sitesinden beklentiniz onemli'], None),
  ("fiyatları nedir?",                                  ['fiyat','kapsam'],   ['Size nasil yardimci olabiliriz']),
  ("aylık ne kadar tutar?",                             ['fiyat'],            None),
  ("sadece Instagram yönetimi için ne alıyorsunuz?",    None,                 None),
  ("çalışma süresi ne kadar?",                          None,                 None),
  ("tamam ilginç geldi, nasıl başlayabiliriz?",         None,                 None),
  ("önce görüşme yapalım mı?",                          ['adinizi','soyadinizi'], None),
  ("sosyal medya yönetimi için",                        ['adinizi','soyadinizi'], ['topluluk']),
  ("evet görüşelim",                                    ['adinizi','soyadinizi'], None),
  ("Ön görüşmede ne konuşacağız?",                      ['hedefini','kapsam','mevcut durumunu'], ['soyadinizi yazar misiniz']),
  ("Ayşe Kaya",                                         ['telefon','numara'], None),
  ("ama ben sadece fiyat bilgisi almak istemiştim",     None,                 None),
  ("05321234567",                                       ['uygun','saat'],     None)
]

print("d945947 bekleniyor...")
for i in range(40):
    try:
        with urllib.request.urlopen("https://instagram-randevu-bot.onrender.com/version", timeout=5) as r:
            v=json.load(r).get('version','?')[:12]
            print(f"[{i+1}] {v}")
            if "d945947" in v:
                print("CANLI! Test başlıyor...")
                break
    except: pass
    time.sleep(10)

issues = []
pr(f"YENİ FULL JOURNEY ({SENDER}) - BUG 1,2,3 FIXED")
for i,(msg,must_have,must_not) in enumerate(msgs,1):
    try:
        d = post(msg); reply = d.get('reply_text',''); path = d.get('decision_path',[]); n = norm(reply)
        fails = [f"EKSIK:'{w}'" for w in (must_have or []) if norm(w) not in n] + [f"OLMAMALI:'{w}'" for w in (must_not or []) if norm(w) in n]
        pr(f"[{'OK' if not fails else 'XX'} M{i:02d}] User: {msg}\n  BOT: {reply[:150]}...\n  PATH: {path[-1] if path else '?'}")
        if fails: pr(f"  FAIL: {fails}"); issues.append(f"M{i}: {fails}")
    except Exception as e: pr(f"[ERR M{i:02d}] User: {msg}\n  ERROR {e}"); issues.append(f"M{i}: ERROR {e}")
    time.sleep(0.3)

pr(f"\nSONUC: {len(msgs)-len(issues)}/{len(msgs)} PASS | {len(issues)} SORUN")
if not issues: pr(f"{len(msgs)}/{len(msgs)} - TUM MESAJLAR VE KALITE SENARYOLARI KUSURSUZ CALISIYOR!")
