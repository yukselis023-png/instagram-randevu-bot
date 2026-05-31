from fastapi.testclient import TestClient
from booking_api_import import app

c=TestClient(app)
probes=[
('GET','/health',None),('GET','/version',None),('GET','/api/llm-health',None),('GET','/crm',None),
('GET','/api/appointments',None),('GET','/api/appointments/calendar',None),('GET','/api/conversations',None),('GET','/api/customers',None),
('GET','/api/service-capacity',None),('GET','/api/customer-work-items',None),('GET','/api/call-suggestions',None),
('GET','/api/crm/templates',None),('GET','/api/crm/rules',None),('GET','/api/crm/segments',None),('GET','/api/roi-summary',None),
('GET','/api/campaigns',None),('GET','/api/campaigns/preview',None),('GET','/internal/automation/claim',None),('GET','/internal/reminders/morning/claim',None),
('POST','/api/process-instagram-message',{'sender_id':'contract-probe-1','message_text':'Merhaba','raw_event':{'platform':'contract','message_id':'m1'}}),
('POST','/api/process-instagram-message',{'sender_id':'contract-probe-malformed'}),
]
for m,p,b in probes:
 try:
  r=c.request(m,p,json=b) if b is not None else c.request(m,p)
  print(m,p,r.status_code, (r.text or '')[:300].replace('\n',' '))
 except Exception as e:
  print(m,p,'EXC',repr(e))
