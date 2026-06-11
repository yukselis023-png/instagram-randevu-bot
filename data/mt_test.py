import urllib.request, json, time, random, string

BASE = 'https://instagram-randevu-bot.onrender.com'
uid = ''.join(random.choices(string.ascii_lowercase, k=4))

def post(msg, sender=None, tenant='doel'):
    s = sender or f'mt_{uid}'
    url = f'{BASE}/api/process-instagram-message'
    mid = f'mid_{s}_{int(time.time())}'
    body = json.dumps({
        'sender_id': s, 'message_text': msg,
        'instagram_username': f'ig_{tenant}',
        'raw_event': {'source':'mttest','platform':'instagram_dm',
                      'message_id':mid, 'trace_id':f'tr_{mid}',
                      'message':{'text':msg},'sender':{'id':s}}
    }).encode()
    req = urllib.request.Request(url, data=body, headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)

# TEST 1: DOEL tenant (default)
print('=== TEST 1: DOEL Tenant ===')
S1 = f'mt_doel_{uid}'
d = post('Merhaba, web tasarim paketiniz ne kadar?', S1)
reply = d.get('reply_text','') or d.get('outbound_text','') or ''
print(f'  reply: {reply[:150]}')

# TEST 2: Try custom tenant slug
print('\n=== TEST 2: Custom Tenant (dental) ===')
S2 = f'mt_dental_{uid}'
d = post('Merhaba, dis beyazlatma hizmetiniz var mi?', S2, tenant='dental')
reply = d.get('reply_text','') or d.get('outbound_text','') or ''
print(f'  reply: {reply[:150]}')

# TEST 3: Check tenant config is loaded (generic business context)
print('\n=== TEST 3: Business Identity Check ===')
for name, slug in [('DOEL', 'doel'), ('Dental', 'dental')]:
    S = f'mt_id_{name}_{uid}'
    d = post('Isletmenizin adi nedir?', S, tenant=slug)
    reply = d.get('reply_text','') or d.get('outbound_text','') or ''
    print(f'  [{name}] {reply[:150]}')
    time.sleep(2)

print('\nDone')
