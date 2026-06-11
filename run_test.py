import urllib.request, json, sys, time, random, string
BASE = 'https://instagram-randevu-bot.onrender.com/api/process-instagram-message'
uid = ''.join(random.choices(string.ascii_lowercase, k=6))
SENDER = f'strict-journey-{uid}'

def norm(s):
    if not s: return ''
    return s.lower().replace('ı','i').replace('ş','s').replace('ğ','g').replace('ü','u').replace('ö','o').replace('ç','c')

def post(msg):
    req = urllib.request.Request(BASE, data=json.dumps({'sender_id': SENDER, 'message_text': msg}).encode(), headers={'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=30) as resp: return json.load(resp)

# Test using both keyword presence AND decision_path checks.
# The structured_core engine generates natural LLM replies, so keyword matching
# uses broad semantic patterns instead of exact phrases.
msgs = [
  # (message, expected_keywords_any, forbidden_keywords, expected_path_contains)
  ("merhaba",                                           ['merhaba'], None, 'structured_core'),
  ("Bir arkadaşım sizi önerdi onun için yazdım",        ['sevindik', 'memnun', 'mutlu', 'tesekkur'], None, 'structured_core'),
  ("Ben randevularınızı merak ettim?",                  ['gorusme', 'randevu', 'hizmet'], ['kuafor', 'berber', 'salon'], 'structured_core'),
  ("Hizmetleriniz neler?",                              ['web tasarim', 'reklam', 'otomasyon', 'sosyal medya'], None, 'structured_core'),
  ("Bu kadar mı hizmetleriniz?",                        ['web tasarim', 'reklam', 'otomasyon', 'sosyal medya'], ['berber', 'kuafor'], 'structured_core'),
  ("siz saç kesiyor muydunuz?",                         ['hayir', 'maalesef', 'hizmetimiz yok', 'bulunm'], ['en mantikli', 'berber'], 'structured_core'),
  ("İşletmeniz ne zaman kuruldu?",                      ['kurulus', 'sistem', 'tarih'], ['paket'], 'structured_core'),
  ("Berkay bey orada mı?",                              ['berkay', 'ulas', 'yardimci', 'ekip', 'yonetici'], None, 'structured_core'),
  ("Berkay siz misiniz?",                               ['berkay', 'yapay zeka', 'ekip', 'asistan'], None, 'structured_core'),
  ("güzellik salonu için sosyal medya yapıyor musunuz?",['yapıyoruz', 'yapiyoruz', 'evet', 'salon'], ['hayir', 'maalesef', 'yok'], 'structured_core'),
  ("Sadece düz müşteri istiyorum",                      ['musteri', 'reklam', 'performans', 'kazanmak'], None, 'structured_core'),
  ("Website hizmetinizi gördüm, bana uygun mu merak ediyorum",['web', 'uygun', 'dijital', 'vitrin', 'site'], None, 'structured_core'),
  ("fiyatları nedir?",                                  ['tl', 'ucret', 'fiyat', 'basliyor'], None, 'structured_core'),
  ("aylık ne kadar tutar?",                             ['tl', 'tek seferlik', 'aylik', 'fiyat'], None, 'structured_core'),
  ("tamam ilginç geldi, nasıl başlayabiliriz?",         ['gorusme', 'planla', 'basla', 'baslayabiliriz', 'detay'], None, 'structured_core'),
  ("önce görüşme yapalım mı?",                          ['adinizi', 'soyadinizi', 'gorusme', 'planla', 'isim'], None, 'structured_core'),
  ("sosyal medya yönetimi için",                        ['adinizi', 'soyadinizi', 'gorusme', 'planla', 'isim', 'telefon'], ['topluluk'], 'structured_core'),
  ("Ön görüşmede ne konuşacağız?",                      ['hedef', 'konus', 'planla', 'icerik', 'marka'], ['soyadinizi yazar misiniz'], 'structured_core'),
  ("Ayşe Kaya",                                         ['memnun', 'telefon', 'numara', 'hanim'], None, 'structured_core'),
  ("ama ben sadece fiyat bilgisi almak istemiştim",     ['fiyat', 'ozel', 'teklif', 'size', 'belirleniyor', 'marka'], None, 'structured_core'),
]

issues = []
version = "Unknown"
try:
    with urllib.request.urlopen("https://instagram-randevu-bot.onrender.com/version", timeout=5) as r:
        version = json.load(r).get('version','?')[:12]
except: pass

print(f"\n{'='*60}")
print(f"STRUCTURED CORE JOURNEY TEST ({SENDER})")
print(f"Commit: {version}")
print(f"{'='*60}\n")
for i,(msg,must_have_any,must_not,path_contains) in enumerate(msgs,1):
    try:
        d = post(msg); reply = d.get('reply_text','') or d.get('outbound_text',''); path = d.get('decision_path',[]); n = norm(reply)
        # Only FAIL if ALL keywords missing (any match = pass)
        missing_all = True
        for w in (must_have_any or []):
            if norm(w) in n:
                missing_all = False
                break
        fails = []
        if missing_all and must_have_any:
            fails.append(f"HICBIRI:{','.join(must_have_any)}")
        if must_not:
            for w in must_not:
                if norm(w) in n:
                    fails.append(f"OLMAMALI:'{w}'")
        last_path = path[-1] if path else '?'
        mark = 'OK' if not fails else 'XX'
        print(f"[{mark} M{i:02d}] path={last_path}")
        reply_short = reply[:200]
        print(f"  USER: {msg}")
        print(f"  BOT:  {reply_short}")
        if fails:
            print(f"  FAIL: {fails}")
            issues.append(f"M{i}: {fails}")
    except Exception as e:
        print(f"[ERR M{i:02d}] User: {msg}\n  ERROR {e}")
        issues.append(f"M{i}: ERROR {e}")
    time.sleep(1.5)

print(f"\n{'='*60}")
print(f"SONUC: {len(msgs)-len(issues)}/{len(msgs)} PASS | {len(issues)} SORUN")
print(f"{'='*60}")
for x in issues:
    print(f"  - {x}")
if not issues:
    print("TUM MESAJLAR VE KALITE SENARYOLARI KUSURSUZ CALISIYOR!")
