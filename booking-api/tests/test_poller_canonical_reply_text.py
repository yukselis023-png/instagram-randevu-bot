import importlib.util
import sys
import types
from pathlib import Path


class _DummyClient:
    pass


def load_poller_module():
    sys.modules.setdefault("speech_recognition", types.SimpleNamespace())
    instagrapi_module = types.ModuleType("instagrapi")
    instagrapi_module.Client = _DummyClient
    exceptions_module = types.ModuleType("instagrapi.exceptions")
    exceptions_module.LoginRequired = type("LoginRequired", (Exception,), {})
    sys.modules["instagrapi"] = instagrapi_module
    sys.modules["instagrapi.exceptions"] = exceptions_module

    repo_root = Path(__file__).resolve().parents[2]
    module_path = repo_root / "instagram-poller" / "app" / "main.py"
    spec = importlib.util.spec_from_file_location("instagram_poller_main_for_tests", module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_canonical_reply_text_prefers_outbound_text_when_reply_text_matches():
    poller = load_poller_module()

    result = {"outbound_text": "canonical", "reply_text": "canonical"}

    assert poller.get_canonical_reply_text(result) == "canonical"


def test_canonical_reply_text_prefers_outbound_text_when_fields_differ():
    poller = load_poller_module()

    result = {"outbound_text": "canonical outbound", "reply_text": "legacy reply"}

    assert poller.get_canonical_reply_text(result) == "canonical outbound"


def test_canonical_reply_text_falls_back_to_reply_text_when_outbound_missing():
    poller = load_poller_module()

    result = {"reply_text": "legacy reply"}

    assert poller.get_canonical_reply_text(result) == "legacy reply"


def test_normalize_empty_reply_fallback_sets_reply_text_and_outbound_text_equal():
    poller = load_poller_module()

    result = poller.normalize_processing_response({"should_reply": True})

    assert result["outbound_text"] == poller.SAFE_PROCESSING_FALLBACK_REPLY
    assert result["reply_text"] == poller.SAFE_PROCESSING_FALLBACK_REPLY
    assert poller.get_canonical_reply_text(result) == poller.SAFE_PROCESSING_FALLBACK_REPLY


def test_should_fallback_uses_canonical_outbound_text():
    poller = load_poller_module()

    result = {"should_reply": True, "outbound_text": "canonical outbound", "reply_text": ""}

    assert poller.should_fallback_processing_result(result) is False
