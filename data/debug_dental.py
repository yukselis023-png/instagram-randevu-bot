import urllib.request, json, time, random, string

BASE = 'https://instagram-randevu-bot.onrender.com'
uid = ''.join(random.choices(string.ascii_lowercase, k=4))

def post(msg, sender=None, tenant='doel'):
    s = sender or f'debug_{uid}'
    url = f'{BASE}/api/process-instagram-message'
    mid = f'mid_{s}_{int(time.time())}'
    body = json.dumps({
        'sender_id': s, 'message_text': msg,
        'instagram_username': f'ig_{tenant}',
        'raw_event': {'source':'debug','platform':'instagram_dm',
                      'message_id':mid, 'trace_id':f'tr_{mid}',
                      'message':{'text':msg},'sender':{'id':s}}
    }).encode()
    req = urllib.request.Request(url, data=body, headers={'Content-Type':'application/json'})
    with urllib.request.urlopen(req, timeout=120) as resp:
        return json.load(resp)

S = f'debug_d_{uid}'
d = post('Adiniz nedir?', S, tenant='dental')
reply = d.get('reply_text','') or d.get('outbound_text','') or ''
dp = d.get('decision_path',['?'])
print(f'REPLY: {reply[:200]}')
print(f'PATH: {dp}')

