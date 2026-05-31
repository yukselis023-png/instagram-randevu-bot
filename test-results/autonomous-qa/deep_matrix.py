import json, time, urllib.request, urllib.error, concurrent.futures, uuid
BASE='https://instagram-randevu-bot.onrender.com'
TS='20260520-auto'

def post(path,payload):
    data=json.dumps(payload,ensure_ascii=False).encode()
    req=urllib.request.Request(BASE+path,data=data,headers={'Content-Type':'application/json'},method='POST')
    try:
        with urllib.request.urlopen(req,timeout=45) as r:
            return r.status,json.loads(r.read().decode('utf-8','replace'))
    except urllib.error.HTTPError as e:
        return e.code,e.read().decode('utf-8','replace')[:1000]
    except Exception as e:
        return 'ERR',repr(e)

def get(path):
    try:
        with urllib.request.urlopen(BASE+path,timeout=30) as r:
            return r.status,json.loads(r.read().decode('utf-8','replace'))
    except Exception as e:
        return 'ERR',repr(e)

def send(sid,msg,n=1):
    return post('/api/process-instagram-message',{'sender_id':sid,'instagram_username':sid.replace('-','_'),'message_text':msg,'raw_event':{'platform':'igdm','message_id':f'{sid}-{n}'},'trace_id':f'{sid}-{n}'})

def flow(name,msgs):
    sid=f'auto-{TS}-{name}-{uuid.uuid4().hex[:6]}'
    out=[]
    for i,m in enumerate(msgs,1):
        st,res=send(sid,m,i); out.append({'step':i,'msg':m,'status':st,'res':res}); time.sleep(1)
    dbg=get('/api/debug-state/'+sid)
    return {'sid':sid,'steps':out,'debug':dbg}

scenarios={
 'single_shot_full':['Merhaba web tasarım için ön görüşme istiyorum. Adım Test Otomasyon, telefonum 0555 444 33 22. Yarın 12:00 uygun mu?'],
 'multi_turn_happy':['Merhaba web sitesi istiyorum','Evet ön görüşme planlayalım','Adım Test Otomasyon','Telefonum 0555 444 33 23','Yarın 12:00 uygun mu?'],
 'invalid_phone_recovery':['Web tasarım için görüşme istiyorum','Adım Test Hatalı','05555','Telefonum 0555 444 33 24','Yarın 13:00 uygun mu?'],
 'malformed_noise':[';;;;','<script>alert(1)</script>','Web tasarım fiyatı nedir?'],
 'empty_message':[''],
}
results=[]
for name,msgs in scenarios.items():
    results.append(flow(name,msgs))

def conc(i):
    return flow(f'concurrent-{i}', [f'Merhaba otomasyon için ön görüşme istiyorum. Adım Con User {i}, telefonum 0555 700 0{i:03d}. Yarın 14:00 uygun mu?'])
with concurrent.futures.ThreadPoolExecutor(max_workers=5) as ex:
    for r in ex.map(conc, range(1,11)):
        results.append(r)
print(json.dumps(results,ensure_ascii=False,indent=2))
