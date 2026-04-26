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

    def test_normalize_strips_internal_safe_draft_prefix(self):
        self.assertEqual(
            main.normalize_llm_reply_text("Güvenli taslak cevap: Telefon numaranızı paylaşır mısınız?"),
            "Telefon numaranızı paylaşır mısınız?",
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

    def test_guardrails_reject_price_reply_that_jumps_to_calendar(self):
        draft = "Otomasyon ve Yapay Zeka Cozumleri 5.000 TL'den basliyor (ilk 3 ay indirimli aylik hizmet bedeli). DM, randevu yoksa musteri takibi mi daha oncelikli?"
        candidate = "Otomasyon ve Yapay Zeka hizmetimizin fiyati 5.000 TL/ay. Kisa bir gorusme yapabiliriz, hangi gun ve saat sizin icin uygundur?"
        result = main.apply_reply_guardrails(
            draft,
            candidate,
            "Fiyat nedir?",
            {"service": "Otomasyon & Yapay Zeka Cozumleri", "state": "collect_service"},
            "info:price_question",
            [],
        )

        self.assertEqual(result, draft)

    def test_service_info_followup_does_not_jump_to_calendar(self):
        service = main.match_service_catalog("Instagram mesajlarına otomatik cevap veren sistem", None)
        reply = main.build_service_info_reply(service, {"state": "collect_service"})

        self.assertNotIn("Hangi gün", reply)
        self.assertIn("mesaj yoğunluğunuz", reply)

    def test_price_question_after_automation_context_stays_price_reply(self):
        service = main.match_service_catalog("Otomasyon ve yapay zeka hizmeti", None)
        conversation = {"service": service["display"], "state": "collect_service", "memory_state": {}}
        history = [
            {
                "direction": "out",
                "message_text": "Tamam, otomasyon tarafinda ilerleyebiliriz. Once hangi sureci toparlamak istediginizi netlestirelim: DM, randevu yoksa musteri takibi mi?",
            }
        ]
        matched_services = main.match_service_candidates("Fiyat nedir?", conversation["service"])

        result = main.maybe_build_information_reply("Fiyat nedir?", {}, matched_services, conversation, history)

        self.assertEqual(result["kind"], "price_question")
        self.assertIn("5.000", result["reply"])
        self.assertNotIn("Hangi gün", result["reply"])
        self.assertNotIn("hangi gün", result["reply"].lower())
        self.assertNotIn("hangi saat", result["reply"].lower())

    def test_consultation_acceptance_enters_booking_collection(self):
        conversation = {
            "service": "Otomasyon ve Yapay Zeka",
            "state": "collect_service",
            "booking_kind": None,
            "memory_state": {},
        }

        self.assertTrue(main.explicitly_starts_consultation_collection("Tamam görüşelim"))
        self.assertTrue(
            main.should_enter_booking_collection(
                "Tamam görüşelim",
                {},
                asks_availability=False,
                detected_phone=None,
                detected_date=None,
                detected_time=None,
                conversation=conversation,
                history=[],
            )
        )
        self.assertEqual(main.infer_booking_kind("Tamam görüşelim", {}, conversation, []), "preconsultation")

    def test_advisory_question_with_randevu_word_does_not_enter_booking_collection(self):
        message = "Instagram DM otomasyonu istiyorum. Günde yaklaşık 80 DM geliyor, randevu ve takip kaçırıyoruz. Hangisi mantıklı?"
        conversation = {
            "service": None,
            "state": "collect_service",
            "booking_kind": None,
            "memory_state": {},
        }

        self.assertTrue(main.is_service_advice_request(message, {}))
        self.assertFalse(main.message_shows_booking_intent(message, {}))
        self.assertFalse(
            main.should_enter_booking_collection(
                message,
                {},
                asks_availability=False,
                detected_phone=None,
                detected_date=None,
                detected_time=None,
                conversation=conversation,
                history=[],
            )
        )

    def test_advisory_question_with_volume_uses_volume_instead_of_reasking(self):
        message = "Günde 80 DM geliyor, randevu ve müşteri takibi karışıyor. Hangisi mantıklı?"
        conversation = {
            "service": None,
            "state": "collect_service",
            "booking_kind": None,
            "memory_state": {},
        }
        matched_services = main.match_service_candidates(message, None)

        result = main.maybe_build_information_reply(message, {}, matched_services, conversation, [])

        self.assertEqual(result["kind"], "message_volume")
        self.assertIn("80", result["reply"])
        self.assertNotIn("kaç", result["reply"].lower())
        self.assertNotIn("kac", result["reply"].lower())

    def test_randevu_painpoint_does_not_count_as_booking_intent(self):
        message = "Günde yaklaşık 80 DM geliyor, tekrar eden sorular ve randevu kaçıyor."
        conversation = {
            "service": "Otomasyon & Yapay Zeka Çözümleri",
            "state": "collect_service",
            "booking_kind": None,
            "memory_state": {},
        }

        self.assertFalse(main.message_shows_booking_intent(message, {}))
        self.assertFalse(
            main.should_enter_booking_collection(
                message,
                {},
                asks_availability=False,
                detected_phone=None,
                detected_date=None,
                detected_time=None,
                conversation=conversation,
                history=[],
            )
        )

    def test_business_sector_detection_does_not_match_words_inside_unrelated_words(self):
        history = [
            {"direction": "in", "message_text": "Otomasyon ve yapay zeka hizmeti istiyorum"},
            {"direction": "in", "message_text": "Tamam goruselim"},
        ]

        self.assertIsNone(main.detect_business_sector("Carsamba saat 12:00 uygun", history))
        self.assertIsNone(main.detect_business_sector("Planlayalim", []))

    def test_business_sector_detection_still_matches_explicit_real_estate_terms(self):
        self.assertEqual(main.detect_business_sector("Emlak ofisim icin otomasyon istiyorum", []), "real_estate")
        self.assertEqual(main.detect_business_sector("Arsa ilanlari icin takip lazim", []), "real_estate")

    def test_model_routing_uses_8b_for_simple_replies(self):
        profile = main.get_ai_compose_profile("info:greeting", {})

        self.assertEqual(profile["models"], ["llama-3.1-8b-instant"])

    def test_model_routing_uses_scout_then_8b_for_normal_replies(self):
        profile = main.get_ai_compose_profile("info:service_info", {})

        self.assertEqual(
            profile["models"],
            ["meta-llama/llama-4-scout-17b-16e-instruct", "llama-3.1-8b-instant"],
        )

    def test_price_replies_skip_llm_polish_for_deterministic_numbers(self):
        self.assertFalse(
            main.should_ai_compose_reply(
                "info",
                "info:price_question",
                conversation={"service": "Otomasyon & Yapay Zeka Cozumleri"},
            )
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


    def test_service_capacity_defaults_allow_two_preconsultations(self):
        self.assertEqual(main.get_default_service_capacity("Ön Görüşme"), 2)
        self.assertEqual(main.get_default_service_capacity("Otomasyon & Yapay Zeka Çözümleri"), 2)
        self.assertEqual(main.get_default_service_capacity("Web Tasarım - KOBİ Paketi"), 1)

    def test_capacity_rule_allows_until_service_limit(self):
        self.assertTrue(main.is_slot_capacity_available_from_counts(1, 2))
        self.assertFalse(main.is_slot_capacity_available_from_counts(2, 2))
        self.assertFalse(main.is_slot_capacity_available_from_counts(3, 2))

    def test_call_suggestion_scores_due_support_and_renewal(self):
        today = main.date(2026, 5, 1)
        customer = {
            "id": 10,
            "instagram_user_id": "lead-1",
            "full_name": "Ayşe Demir",
            "next_automation_at": "2026-05-01T09:00:00+03:00",
            "subscription_renewal_date": "2026-05-01",
            "no_show_count": 1,
            "segment": "new_customer",
        }
        work_items = [
            {"kind": "support", "status": "open", "due_at": "2026-05-01T10:00:00+03:00"},
            {"kind": "refund", "status": "open", "due_at": "2026-05-02T10:00:00+03:00"},
        ]
        appointments = [
            {"status": "preconsultation", "appointment_date": "2026-05-01", "appointment_time": "14:00"},
        ]

        suggestion = main.build_call_suggestion(customer, work_items, appointments, today)

        self.assertGreaterEqual(suggestion["score"], 100)
        self.assertIn("Açık destek talebi", suggestion["reasons"])
        self.assertIn("Bugün abonelik yenileme", suggestion["reasons"])
        self.assertIn("Bugün ön görüşme", suggestion["reasons"])


if __name__ == "__main__":
    unittest.main()
