import os
os.environ["CHATBOT_ENGINE"] = "generic"

from app.generic_core import process_instagram_message_generic
from app.main import ProcessResult, IncomingMessage, BackgroundTasks, Base
from sqlalchemy import create_engine
import json

from unittest.mock import patch, MagicMock

# Setup in-memory sqlite
engine = create_engine("sqlite:///:memory:")
Base.metadata.create_all(engine)

def get_test_conn():
    return engine.begin()

doel_messages = [
    ("Merhaba", "direct_answer", "Merhaba, size nasıl yardımcı olabilirim?"),
    ("Dövmeciyim, sitenizi gördüm merak edip yazdım", "service_question", "Hoş geldiniz!"),
    ("Hizmetleriniz neler?", "service_question", "Web Tasarım, CRM, Otomasyon..."),
    ("Hangi hizmet işime yarar?", "service_question", "Dövme sektörüne CRM çok faydalıdır."),
    ("Çok DM geliyor", "service_question", "DM otomasyonumuzla rahat edersiniz."),
    ("Otomasyon işime yarar mı?", "service_question", "Evet otomasyon büyük kolaylık sağlar!"),
    ("Ne kadar?", "price_question", "Fiyatlarımız 10 binden başlar."),
    ("Ön görüşmede ne konuşacağız?", "direct_answer", "Projenizin analizini konuşuruz."),
    ("Olur görüşelim", "booking_request", "Tarihi ve saati konuşmadan önce isminiz?"),
    ("Berkay Elbir", "active_booking", "Teşekkürler, numaranız?", {"lead_name": "Berkay Elbir"}),
    ("05539088638", "active_booking", "Ne zaman müsait?", {"phone": "05539088638"}),
    ("Yarın akşam altı", "active_booking", "Akşam 6 uygun değil başka var mı?", {"requested_date": "2024-05-06"}),
    ("13:00", "active_booking", "Onaylandı.", {"requested_time": "13:00", "requested_service": "Otomasyon"}),
    ("Ödeme nasıl yapılıyor?", "price_question", "Havale/EFT kabul ediyoruz."),
    ("Tamam teşekkürler", "direct_answer", "Görüşmek üzere!")
]

beauty_messages = [
    ("Merhaba", "direct_answer", "Hoş geldiniz."),
    ("Hydrafacial ne demek?", "service_question", "Cilt yenilemedir."),
    ("Cilt bakımı bana uygun mu?", "service_question", "Tabii ki."),
    ("Lazer epilasyon fiyatı ne?", "price_question", "1000 TL."),
    ("Randevu almak istiyorum", "booking_request", "Seve seve.")
]

dental_messages = [
    ("Merhaba", "direct_answer", "Hoş geldiniz"),
    ("İmplant ne demek?", "service_question", "Vidalı diştir."),
    ("Diş beyazlatma bana uygun mu?", "service_question", "Bakıp kontrol edebiliriz."),
    ("Muayene ücreti ne kadar?", "price_question", "Ücretsiz."),
    ("Doktorla konuşabilir miyim?", "human_handoff", "Aktarıyorum.")
]

def run_test(name, messages, cfg_mock):
    print(f"\n--- {name} YOLCULUĞU TESTİ ---")
    
    with patch("app.main.get_conn", return_value=get_test_conn()):
        with patch("app.main.Base", return_value=Base):
            with patch("app.generic_core.get_conn", return_value=get_test_conn()):
                with patch("app.generic_core.invoke_generic_llm") as mock_llm:
                    pass
