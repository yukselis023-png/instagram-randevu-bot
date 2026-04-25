import unittest

from app import main


class ReplyQualityTests(unittest.TestCase):
    def test_normalize_restores_common_turkish_reply_words(self):
        reply = main.normalize_llm_reply_text("Yarin web tasarim icin gorusebiliriz.")

        self.assertEqual(reply, "Yarın web tasarım için görüşebiliriz.")
        self.assertEqual(
            main.normalize_llm_reply_text("Gelen mesajlarıza otomatik cevap verir."),
            "Gelen mesajlarınıza otomatik cevap verir.",
        )

    def test_guardrails_reject_truncated_price_reply(self):
        draft = "Web Tasarım - KOBİ Paketi 12.900 TL (tek seferlik). Projeniz kurumsal site mi, satış odaklı landing page mi?"
        candidate = "Web tasarim fiyati 12."
        result = main.apply_reply_guardrails(
            draft,
            candidate,
            "Merhaba, web tasarım fiyatınız nedir?",
            {"service": "Web Tasarım - KOBİ Paketi", "state": "collect_service"},
            "info:price_question",
            [],
        )

        self.assertEqual(result, draft)

    def test_guardrails_reject_early_phone_request_for_info_question(self):
        draft = "Instagram DM otomasyonu gelen mesajları karşılar, sık soruları yanıtlar ve uygun müşteriyi randevu akışına taşır. Günlük mesaj yoğunluğunuz yaklaşık kaç?"
        candidate = "Size özel otomasyon yapısını netleştirmek için kısa bir ön görüşme planlayalım. Telefon numaranızı paylaşır mısınız?"
        result = main.apply_reply_guardrails(
            draft,
            candidate,
            "Instagram mesajlarına otomatik cevap veren bir sistem istiyorum, nasıl çalışıyor?",
            {"service": "Otomasyon ve Yapay Zeka", "state": "collect_service"},
            "info:service_info",
            [],
        )

        self.assertEqual(result, draft)

    def test_guardrails_reject_early_phone_request_for_quality_collect_service(self):
        draft = "Bu durumda en mantıklı başlangıç DM ve randevu otomasyonu olur; müşteri takibini de aynı akışa bağlayabiliriz. Günlük DM yoğunluğunuz yaklaşık kaç?"
        candidate = "Güzellik salonunuz için DM, randevu ve müşteri takibi birlikte karışıyorsa otomasyon daha mantıklı olur. Telefon numaranızı paylaşır mısınız?"
        result = main.apply_reply_guardrails(
            draft,
            candidate,
            "Ben güzellik salonuyum; DM, randevu ve müşteri takibi birlikte karışıyor. Bana hangisi daha mantıklı olur?",
            {"service": "Otomasyon ve Yapay Zeka", "state": "collect_service"},
            "collect_service",
            [],
        )

        self.assertEqual(result, draft)

    def test_service_info_followup_does_not_jump_to_calendar(self):
        service = main.match_service_catalog("Instagram mesajlarına otomatik cevap veren sistem", None)
        reply = main.build_service_info_reply(service, {"state": "collect_service"})

        self.assertNotIn("Hangi gün", reply)
        self.assertIn("mesaj yoğunluğunuz", reply)

    def test_model_routing_uses_8b_for_simple_replies(self):
        profile = main.get_ai_compose_profile("info:greeting", {})

        self.assertEqual(profile["models"], ["llama-3.1-8b-instant"])

    def test_model_routing_uses_scout_then_8b_for_normal_replies(self):
        profile = main.get_ai_compose_profile("info:service_info", {})

        self.assertEqual(
            profile["models"],
            ["meta-llama/llama-4-scout-17b-16e-instruct", "llama-3.1-8b-instant"],
        )

    def test_model_routing_uses_70b_then_scout_then_8b_for_quality_replies(self):
        profile = main.get_ai_compose_profile("info:service_advice", {})

        self.assertEqual(
            profile["models"],
            [
                "llama-3.3-70b-versatile",
                "meta-llama/llama-4-scout-17b-16e-instruct",
                "llama-3.1-8b-instant",
            ],
        )

    def test_model_routing_uses_quality_chain_for_complex_collect_service(self):
        profile = main.get_ai_compose_profile(
            "collect_service",
            {"last_customer_message": "DM, randevu ve müşteri takibi birlikte karışıyor. Hangisi daha mantıklı?"},
        )

        self.assertEqual(
            profile["models"],
            [
                "llama-3.3-70b-versatile",
                "meta-llama/llama-4-scout-17b-16e-instruct",
                "llama-3.1-8b-instant",
            ],
        )

    def test_call_llm_content_falls_back_when_first_model_errors(self):
        calls = []
        original_post = main.requests.post
        original_base_url = main.LLM_BASE_URL
        original_api_key = main.LLM_API_KEY

        class FakeResponse:
            def __init__(self, status_code, payload=None, text=""):
                self.status_code = status_code
                self._payload = payload or {}
                self.text = text

            def json(self):
                return self._payload

        def fake_post(url, headers, json, timeout):
            calls.append(json["model"])
            if len(calls) == 1:
                return FakeResponse(429, text="rate limit")
            return FakeResponse(200, {"choices": [{"message": {"content": "yedek model cevabı"}}]})

        try:
            main.LLM_BASE_URL = "https://api.groq.com/openai/v1"
            main.LLM_API_KEY = "test-key"
            main.requests.post = fake_post
            result = main.call_llm_content(
                [{"role": "user", "content": "test"}],
                models=["first-model", "second-model"],
                timeout=5,
            )
        finally:
            main.requests.post = original_post
            main.LLM_BASE_URL = original_base_url
            main.LLM_API_KEY = original_api_key

        self.assertEqual(result, "yedek model cevabı")
        self.assertEqual(calls, ["first-model", "second-model"])


if __name__ == "__main__":
    unittest.main()
