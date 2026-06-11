import urllib.request, json, time, random

BASE = 'https://instagram-randevu-bot.onrender.com'
uid = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz', k=4))

# Direct test with curl-like approach
sender = f'test_{uid}'
msg = 'Adiniz nedir?'
body = json.dumps({
    'sender_id': sender,
    'message_text': msg,
    'instagram_username': f'u_{uid}',
    'raw_event': {
        'source': 'direct',
        'platform': 'instagram_dm',
        'tenant': 'dental',
        'message_id': f'mid_{sender}',
        'trace_id': f'tr_{sender}',
        'message': {'text': msg},
        'sender': {'id': sender}
    }
}).encode()

print('Sending body:')
print(body.decode()[:300])
print()

req = urllib.request.Request(f'{BASE}/api/process-instagram-message', data=body, headers={'Content-Type':'application/json'})
resp = urllib.request.urlopen(req, timeout=60)
d = json.load(resp)
reply = d.get('reply_text','') or d.get('outbound_text','') or ''
dp = d.get('decision_path',['?'])
print(f'REPLY: {reply[:200]}')
print(f'PATH: {dp}')
