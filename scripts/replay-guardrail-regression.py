import json
import sys
import time
import urllib.request

if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

BASE = 'http://127.0.0.1:18000/api/process-instagram-message'
RUN_TAG = str(int(time.time()))

CASES = {
    'service-price-flow': [
        'Merhabalar bilgi almak istiyorum',
        'Otomasyon',
        'Fiyat ne kadar',
        'Aylık mı 5000 TL',
    ],
    'price-objection-flow': [
        'Merhabalar bilgi almak istiyorum',
        'Otomasyon süreci için bilgi almak istiyorum',
        '5000 lira çok fazla indirim yapın',
    ],
    'live-negotiation-flow': [
        'Merhabalar bilgi almak istiyorum',
        'Otomasyon',
        'Fiyat çokmuş ya',
        'Aylık mi peki bu?',
        '4 bin TL olur mu',
        'Param yetmiyor almaya 4 bin TL olursa alicam',
    ],
    'owner-handoff-flow': [
        'Merhabalar bilgi almak istiyorum',
        'Otomasyon',
        '500 TL olur mu?',
    ],
    'owner-handoff-then-advice': [
        'Merhabalar bilgi almak istiyorum',
        'Otomasyon',
        '500 TL olur mu?',
        'Önerin var mı?',
    ],
    'style-flow-beauty': [
        'Sıkıldım',
        'Ben güzellik salonu işletiyorum bana ne lazım?',
        'Hepsi',
        'Olur',
    ],
    'style-flow-beauty-name': [
        'Sıkıldım',
        'Ben güzellik salonu işletiyorum bana ne lazım?',
        'Hepsi',
        'Evet',
        'Berkay elbir',
    ],
    'priority-choice-dm': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'DM cevapları',
    ],
    'priority-choice-appointment': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'Randevu tarafı',
    ],
    'direct-painpoint-beauty': [
        'Güzellik salonum var, DM ve randevuya yetişemiyorum',
    ],
    'phone-purpose-question': [
        'Güzellik salonu için çok DM geliyor. Yetişemiyorum. Bunun hakkında yardımcı olmanızı istiyorum. Bana hangi hizmeti önerirsiniz?',
        'Hepsi',
        'Evet',
        'Berkay elbir',
        'Bu ne için gerekli vermesem olur mu?',
    ],
    'phone-refusal': [
        'Güzellik salonu için çok DM geliyor. Yetişemiyorum. Bunun hakkında yardımcı olmanızı istiyorum. Bana hangi hizmeti önerirsiniz?',
        'Hepsi',
        'Evet',
        'Berkay elbir',
        'Bu ne için gerekli vermesem olur mu?',
        'Paylaşmak istemiyorum',
    ],
    'offer-hesitation': [
        'Güzellik salonu işletiyorum',
        '0:09',
        'Hepsi',
        'Bilmiyorum',
    ],
    'offer-hesitation-realistic': [
        'Güzellik salonu için çok DM geliyor. Yetişemiyorum. Bunun hakkında yardımcı olmanızı istiyorum. Bana hangi hizmeti önerirsiniz?',
        'Hepsi',
        'Bilmiyorum',
    ],
    'beauty-sector-intro': [
        'Güzellik salonu işletiyorum',
    ],
    'phone-refusal-then-create-request': [
        'Güzellik salonu için çok DM geliyor. Yetişemiyorum. Bunun hakkında yardımcı olmanızı istiyorum. Bana hangi hizmeti önerirsiniz?',
        'Hepsi',
        'Evet',
        'Berkay elbir',
        'Bu ne için gerekli vermesem olur mu?',
        'Paylaşmak istemiyorum',
        'Yok on görüşmeyi adım ve soy adımı kullanarak oluşturun',
    ],
    'customer-followup-falan': [
        'Merhaba',
        'Ben güzellik salonu işletiyorum',
        'Müşteri takip falan',
    ],
    'real-estate-volume-loop': [
        'Merhaba, emlak tarafındayım',
        'Mesajlara yanıt zor oluyor',
        'Gecikme',
        '300 kişi yazıyor',
    ],
    'automation-hepsi-yapalim': [
        'Merhaba',
        'Otomasyon hakkında bilgi verin',
        'Hepsi',
        'Yapalım',
    ],
    'automation-repeat-avoid': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'DM tarafı',
        'Gecikme',
        '250 kişi yazıyor',
        'Tamam',
    ],
    'technical-api-complaint': [
        'Apıye istek v.s gitmiyor yazınca otomatik mesaj atıyor o yüzden instagram chatinde sıkıntı.',
    ],
    'service-overview-detailed': [
        'Esenlikler',
        'Hizmetleriniz neler',
        'Detaylı bilgi verin her biri için',
    ],
    'decline-closeout-cooldown': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'İstemiyom',
        'Pekala',
    ],
    'offer-hadi-acceptance': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'Hepsi',
        'Hadi',
    ],
    'offer-clarification-after-hesitation': [
        'Merhaba bilgi almak istiyorum',
        'Otomasyon',
        'Hepsi',
        'Olabilir de olmayada bilir',
        'Detaylı anlat anlamadım',
    ],
    'tattoo-owner-flow': [
        'Naber lan',
        'Nasıl yardımcı olabilirsin',
        'Ben dovmeciyim',
    ],
}


def post(sender: str, text: str, idx: int):
    payload = {
        'sender_id': sender,
        'instagram_username': sender,
        'message_text': text,
        'raw_event': {'message_id': f'{sender}-{idx}', 'thread_id': f'thread-{sender}'},
    }
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    req = urllib.request.Request(BASE, data=data, headers={'Content-Type': 'application/json; charset=utf-8'})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode())


all_results = []
for case_name, messages in CASES.items():
    sender = f'guardrail-{case_name}-{RUN_TAG}'
    convo = []
    for idx, msg in enumerate(messages, start=1):
        body = post(sender, msg, idx)
        convo.append({
            'in': msg,
            'reply': body.get('reply_text'),
            'state': body.get('conversation_state'),
            'decision_path': body.get('decision_path'),
            'normalized': body.get('normalized'),
        })
    all_results.append({'case': case_name, 'conversation': convo})

print(json.dumps(all_results, ensure_ascii=False, indent=2))
