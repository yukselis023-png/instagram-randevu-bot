import pytest
import copy
from app import main
import json
from pathlib import Path

CONFIG_DIR = Path(__file__).resolve().parents[1] / "app" / "config"
with open(CONFIG_DIR / "doel.json", "r", encoding="utf-8") as f: DOEL_CONF = json.load(f)
with open(CONFIG_DIR / "beauty.json", "r", encoding="utf-8") as f: BEAUTY_CONF = json.load(f)
with open(CONFIG_DIR / "dental.json", "r", encoding="utf-8") as f: DENTAL_CONF = json.load(f)

@pytest.fixture
def override_config(monkeypatch):
    def _override(conf_dict):
        monkeypatch.setattr("app.main.get_config", lambda: conf_dict)
        monkeypatch.setattr("app.main.DOEL_SERVICE_CATALOG", conf_dict.get("service_catalog", []))
    return _override

def test_doel_cross_business_guard(override_config):
    override_config(DOEL_CONF)
    decision = main.build_ai_first_decision("hydrafacial ne demek?", {}, [], {})
    reply = decision["reply_text"].lower()
    assert "hydrafacial" not in reply or "kapsamındadır" in reply or "ön görüşmemizde" in reply

def test_beauty_salon_config(override_config):
    override_config(BEAUTY_CONF)
    decision1 = main.build_ai_first_decision("hydrafacial ne demek?", {}, [], {})
    assert "cildi temizleme" in decision1["reply_text"].lower()
    
    decision2 = main.build_ai_first_decision("cilt bakımı bana uygun mu?", {}, [], {})
    assert "canlılık hedefliyorsanız" in decision2["reply_text"].lower()
    
    decision3 = main.build_ai_first_decision("lazer epilasyon fiyatı ne?", {"service":"lazer epilasyon"}, [], {})
    assert "3.500" in decision3["reply_text"].lower()
    
    

def test_dental_clinic_config(override_config):
    override_config(DENTAL_CONF)
    decision1 = main.build_ai_first_decision("implant nedir?", {}, [], {})
    assert "yapay diş kökü" in decision1["reply_text"].lower()
    
    decision2 = main.build_ai_first_decision("diş beyazlatma işime yarar mı?", {}, [], {})
    assert "diş hekiminin kısa değerlendirmesi" in decision2["reply_text"].lower()
    
    decision3 = main.build_ai_first_decision("implant fiyatı ne kadar?", {"service":"implant"}, [], {})
    assert "klinik muayenede belirlenir" in decision3["reply_text"].lower()

def test_config_memory_isolation(override_config):
    override_config(DENTAL_CONF)
    conversation = {
        "memory_state": {"customer_goal": "otomasyon_talebi"}
    }
    decision = main.build_ai_first_decision("implant nedir?", conversation, [], {})
    reply = decision["reply_text"].lower()
    assert "yapay diş kökü" in reply
    assert "otomasyon" not in reply

def test_journey_dental(override_config):
    override_config(DENTAL_CONF)
    conv = {"state": "new", "memory_state": {}, "missing_fields": []}
    
    # 1. selam
    d1 = main.build_ai_first_decision("merhaba", conv, [], {})
    assert "merhaba" in d1["reply_text"].lower()
    
    # 2. hizmet açıklama
    conv["state"] = d1.get("conversation_state") or "collect_service"
    d2 = main.build_ai_first_decision("implant nedir", conv, [], {})
    assert "yapay diş kökü" in d2["reply_text"].lower()
    
    # 3. randevu
    conv["state"] = "collect_name"
    conv["service"] = "implant"
    d3 = main.build_ai_first_decision("Yüksel Yiğit", conv, [], {})
    assert "telefon" in d3["reply_text"].lower() or "adınızı" in d3["reply_text"].lower()
