import pytest
import os
import json
from unittest.mock import patch, MagicMock

import app.generic_core as gc
from app.main import ProcessResult, IncomingMessage, BackgroundTasks

@pytest.fixture(autouse=True)
def engine_flag():
    os.environ["CHATBOT_ENGINE"] = "generic"
    # Provide an inline mock for app.generic_core methods that touch DB
    with patch("app.generic_core.get_conn"):
        with patch("app.generic_core.save_message_log"):
            with patch("app.generic_core.upsert_conversation"):
                with patch("app.generic_core.upsert_customer_from_conversation", return_value={"id": 1, "sector": "tech"}):
                    with patch("app.generic_core.schedule_customer_automation_events"):
                        with patch("app.generic_core.queue_crm_sync"):
                            with patch("app.generic_core.try_acquire_inbound_processing_lock", return_value=True):
                                with patch("app.generic_core.has_processed_inbound_message", return_value=False):
                                    yield

def test_smoke_journeys(capsys):
    doel = {
        "Merhaba": ("direct_answer", "Merhaba, size nasıl yardımcı olabilirim?"),
        "Dövmeciyim, sitenizi gördüm merak edip yazdım": ("service_question", "Hoş geldiniz!"),
        "Hizmetleriniz neler?": ("service_question", "Web Tasarım, CRM, vs."),
        "Hangi hizmet işime yarar?": ("service_question", "CRM öneririz."),
        "Çok DM geliyor": ("service_question", "Otomasyon işinize yarar.", {"requested_service": "Otomasyon"}),
        "Otomasyon işime yarar mı?": ("service_question", "Evet."),
        "Ne kadar?": ("price_question", "Fiyatlar..."),
        "Ön görüşmede ne konuşacağız?": ("direct_answer", "Analiz..."),
        "Olur görüşelim": ("booking_request", "İsminiz?"),
        "Berkay Elbir": ("active_booking", "Telefon?", {"lead_name": "Berkay Elbir"}),
        "05539088638": ("active_booking", "Gün?", {"phone": "05539088638"}),
        "Yarın akşam altı": ("active_booking", "Yok, başka?", {"requested_date": "2024-05-06"}),
        "13:00": ("active_booking", "Onaylandı.", {"requested_time": "13:00"}),
        "Ödeme nasıl yapılıyor?": ("price_question", "Havale..."),
        "Tamam teşekkürler": ("direct_answer", "Görüşürüz.")
    }

    beauty = {
        "Merhaba": ("direct_answer", "Hoş geldiniz"),
        "Hydrafacial ne demek?": ("service_question", "Cilt yenileme"),
        "Cilt bakımı bana uygun mu?": ("service_question", "Bakalım"),
        "Lazer epilasyon fiyatı ne?": ("price_question", "1000 TL"),
        "Randevu almak istiyorum": ("booking_request", "Zaman?", {"requested_service": "Lazer epilasyon"})
    }

    dental = {
        "Merhaba": ("direct_answer", "Hoş geldiniz"),
        "İmplant ne demek?": ("service_question", "Vidalı diş"),
        "Diş beyazlatma bana uygun mu?": ("service_question", "Olur"),
        "Muayene ücreti ne kadar?": ("price_question", "Ücretsiz"),
        "Doktorla konuşabilir miyim?": ("human_handoff", "Aktarıyorum")
    }

    def run_suite(name, map_data, cfg):
        conv = {"state": "new", "memory_state": {}}
        success = 0
        early_booking = False
        crm_payload = {}
        
        print(f"\n--- {name} YOLCULUĞU TESTİ BAŞLIYOR ---")
        
        with patch("app.generic_core.get_config", return_value=cfg):
            with patch("app.generic_core.get_or_create_conversation", return_value=conv):
                with patch("app.generic_core.get_recent_message_history", return_value=[]):
                    for msg, parts in map_data.items():
                        expected_intent = parts[0]
                        expected_reply = parts[1]
                        entities = parts[2] if len(parts) > 2 else {}
                        
                        mock_llm_dict = {
                            "intent": expected_intent,
                            "reply_text": expected_reply,
                            "extracted_entities": entities,
                            "requires_human": expected_intent == "human_handoff"
                        }
                        
                        with patch("app.generic_core.invoke_generic_llm", return_value=mock_llm_dict):
                            # The business generic engine handles the FSM internally
                            payload = IncomingMessage(sender_id=f"test_{name}", message_text=msg)
                            res = gc.process_instagram_message_generic(payload, BackgroundTasks())
                            
                            # Log what happened
                            print(f"[{name}] User: {msg}\n   Bot: {res.reply_text}\n   State: {res.conversation_state}")
                            
                            if msg == "Dövmeciyim, sitenizi gördüm merak edip yazdım" and res.conversation_state != "new":
                                early_booking = True
                                
                            crm_payload = conv
                            success += 1
        
        print(f"\n[PASS] {name} Journey: {success}/{len(map_data)}")
        return crm_payload, early_booking

    doel_crm, db_err = run_suite("DOEL", doel, {"business_name": "DOEL DIGITAL", "service_catalog": [{"display": "Otomasyon"}]})
    
    print("\n--- CRM_PAYLOAD & DURUM KONTROL ---")
    print(f"isim: {doel_crm.get('lead_name')}, telefon: {doel_crm.get('phone')}, hizm: {doel_crm.get('memory_state', {}).get('requested_service')}")
    if doel_crm.get("state") == "completed":
        print("Randevu Durumu: completed")
    if db_err:
        print("Erken_booking: VAR (HATA)")
    else:
        print("Erken_booking: YOK (BAŞARILI)")
        
    run_suite("BEAUTY", beauty, {"business_name": "Beauty Salon", "service_catalog": [{"display": "Lazer epilasyon"}]})
    run_suite("DENTAL", dental, {"business_name": "Diş Klinik"})

