import pytest
import os
import json
from unittest.mock import patch, MagicMock

import app.generic_core as gc
from app.main import ensure_conversation_memory, ProcessResult

@pytest.fixture(autouse=True)
def engine_flag():
    os.environ["CHATBOT_ENGINE"] = "generic"
    yield
    del os.environ["CHATBOT_ENGINE"]

@patch("app.generic_core.call_llm_content")
def test_generic_beauty_journey(mock_llm):
    # Setup our config mock manually or rely on fixture
    mock_llm.return_value = {
        "intent": "service_question",
        "reply_text": "Biz hydrafacial ve lazer api yapıyoruz.",
        "extracted_lead_name": None,
        "extracted_phone": None
    }
    
    with patch("app.generic_core.get_config") as mock_conf:
        mock_conf.return_value = {"business_name": "Test Beauty", "business_type": "beauty"}
        
        reply, path = gc.generic_engine_router("Hizmetleriniz", {}, [])
        
        assert "hydrafacial" in reply.lower()
        assert "service_question" in path[0]


@patch("app.generic_core.call_llm_content")
def test_generic_doel_booking_crm(mock_llm):
    # Simulate a user providing phone number
    mock_llm.return_value = {
        "intent": "active_booking",
        "reply_text": "Numaranızı aldım, sizi arayacağız.",
        "extracted_lead_name": "Remzi",
        "extracted_phone": "05554443322"
    }
    
    with patch("app.generic_core.get_config") as mock_conf:
        mock_conf.return_value = {"business_name": "DOEL"}
        
        conversation = {"memory_state": {}}
        reply, path = gc.generic_engine_router("Adım Remzi 05554443322", conversation, [])
        
        
        assert conversation.get("lead_name") == "Remzi"
        assert conversation.get("phone") == "05554443322"
        assert "Numaranızı aldım" in reply

