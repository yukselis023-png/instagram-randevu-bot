"""
Phase 4C Tests — Info / Config Answer Final Builder
====================================================

Tests the ANSWER_FIRST_ENFORCE_INFO_ANSWERS flag and
build_info_answer_final() logic from pipeline_wrapper.py.

Core contract:
- AI reply preferred when config-safe
- Price guard: wrong price → config correction; correct price → preserve AI
- Field drift guard: field collection prompts blocked (no booking opt-in)
- Error / catalog-dump guard → safe fallback
- No hardcoded per-intent reply chains
"""
import pytest

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

# Minimal config used across tests  (mimics a Doel Digital client config)
_CFG = {
    "service_catalog": [
        {
            "slug": "web_tasarim",
            "name": "web_tasarim",
            "display": "Web Tasarım",
            "price": "12.900 TL",
            "summary": "5-sayfalı kurumsal web sitesi",
            "keywords": ["web", "web sitesi", "site"],
        },
        {
            "slug": "otomasyon",
            "name": "Otomasyon",
            "display": "Otomasyon",
            "price": "3.500 TL",
            "summary": "DM otomasyon ve randevu akışı",
            "keywords": ["otomasyon", "dm"],
        },
    ]
}

_EMPTY_CFG: dict = {"service_catalog": []}

INFO_SAFE_FALLBACK = "Bu konuda daha fazla bilgi almak isterseniz ön görüşmede netleştirebiliriz."
PRICE_UNKNOWN_SERVICE = "Hangi hizmet için fiyat bilgisi almak istersiniz: web sitesi, otomasyon veya reklam?"
PRICE_NO_CONFIG_FALLBACK = "Bu hizmetin fiyatı kapsamınıza göre netleşir."


def _build(ai_reply, *, cfg=None, message_text="", service_label=None, is_price_q=False, wants_booking=False):
    from app.pipeline_wrapper import build_info_answer_final
    return build_info_answer_final(
        ai_reply,
        cfg=cfg if cfg is not None else _CFG,
        message_text=message_text,
        service_label=service_label,
        is_price_q=is_price_q,
        wants_booking=wants_booking,
    )


# ============================================================
# Guard 1 — LLM error / empty
# ============================================================

class TestGuard1ErrorEmpty:

    def test_empty_returns_safe_fallback(self):
        r = _build("")
        assert r["outbound_text"] == INFO_SAFE_FALLBACK
        assert r["block_reason"] == "ai_error_or_empty"

    def test_none_returns_safe_fallback(self):
        r = _build(None)
        assert r["block_reason"] == "ai_error_or_empty"

    def test_llm_error_string_returns_safe_fallback(self):
        r = _build("Error: LLM json decode error")
        assert r["block_reason"] == "ai_error_or_empty"
        assert r["outbound_text"] == INFO_SAFE_FALLBACK

    def test_too_many_requests_returns_safe_fallback(self):
        r = _build("Error: too many requests")
        assert r["block_reason"] == "ai_error_or_empty"


# ============================================================
# Guard 2 — Field collection drift
# ============================================================

class TestGuard2FieldDrift:

    def test_phone_prompt_blocked_when_no_booking(self):
        ai = "Web tasarım hakkında bilgi verebilirim. Telefon numaranızı alabilir miyim?"
        r = _build(ai, wants_booking=False)
        assert "telefon numaranızı" not in r["outbound_text"].lower()
        assert r["block_reason"] in ("field_drift_trimmed", "field_drift_in_info_path")

    def test_name_prompt_blocked_when_no_booking(self):
        ai = "Hizmetlerimiz hakkında bilgi vermekten memnuniyet duyarım. Ad soyadınızı alabilir miyim?"
        r = _build(ai, wants_booking=False)
        assert "ad soyadınızı" not in r["outbound_text"].lower()

    def test_field_prompt_allowed_when_wants_booking(self):
        """When booking opt-in is true, field prompt is OK (Phase 4A handles it)."""
        ai = "Web tasarım paketimiz 12.900 TL. Adınızı ve soyadınızı alabilir miyim?"
        r = _build(ai, wants_booking=True, is_price_q=True, service_label="web_tasarim")
        # When wants_booking=True field drift guard does NOT block
        assert r["block_reason"] != "field_drift_in_info_path"


# ============================================================
# Guard 3 — Catalog dump
# ============================================================

class TestGuard3CatalogDump:

    def test_long_reply_returns_safe_fallback(self):
        long_ai = "Hizmetlerimiz şunlardır:\n" + "\n- ".join([f"Hizmet {i}" for i in range(10)])
        r = _build(long_ai)
        assert r["block_reason"] == "catalog_dump"
        assert r["outbound_text"] == INFO_SAFE_FALLBACK

    def test_short_reply_not_catalog_dump(self):
        ai = "Web sitesi, otomasyon ve reklam yönetimi hizmetleri sunuyoruz."
        r = _build(ai)
        assert r["block_reason"] != "catalog_dump"
        assert r["outbound_text"] == ai


# ============================================================
# Guard 4 — Price correctness
# ============================================================

class TestGuard4PriceCorrectness:

    def test_ai_correct_price_preserved(self):
        """AI gives exact config price → preserved."""
        ai = "Web sitesi paketi 12.900 TL."
        r = _build(ai, is_price_q=True, service_label="web_tasarim")
        assert r["outbound_text"] == ai
        assert r["source"] == "info_ai_price_verified"
        assert r["block_reason"] is None

    def test_ai_wrong_price_corrected(self):
        """AI gives 8.000 TL but config says 12.900 TL → corrected."""
        ai = "Web sitesi 8.000 TL."
        r = _build(ai, is_price_q=True, service_label="web_tasarim")
        assert r["block_reason"] == "ai_wrong_price"
        assert "12.900" in r["outbound_text"]
        assert "8.000" not in r["outbound_text"]
        assert r["source"] == "info_price_corrected"

    def test_ai_no_price_service_known_supplemented(self):
        """AI gives vague answer but service known → config price added."""
        ai = "Kapsama göre değişebilir, ön görüşmede netleştiriyoruz."
        r = _build(ai, is_price_q=True, service_label="web_tasarim")
        assert "12.900" in r["outbound_text"]
        assert r["source"] == "info_price_supplemented"

    def test_ai_no_price_service_unknown_clarification(self):
        """AI gives no price and service unknown → clarification question."""
        ai = "Fiyat kapsama göre değişir."
        r = _build(ai, is_price_q=True, service_label=None)
        assert r["block_reason"] == "service_unknown_no_price"
        assert "hangi hizmet" in r["outbound_text"].lower()
        assert r["source"] == "info_price_service_unknown"

    def test_ai_correct_price_otomasyon_preserved(self):
        ai = "Otomasyon paketi 3.500 TL."
        r = _build(ai, is_price_q=True, service_label="Otomasyon")
        assert r["outbound_text"] == ai
        assert r["source"] == "info_ai_price_verified"

    def test_ai_wrong_price_otomasyon_corrected(self):
        ai = "Otomasyon 1.000 TL."
        r = _build(ai, is_price_q=True, service_label="Otomasyon")
        assert "3.500" in r["outbound_text"]
        assert r["block_reason"] == "ai_wrong_price"

    def test_service_known_no_config_price_ai_preserved(self):
        """Service in catalog but no price field → AI preserved."""
        no_price_cfg = {
            "service_catalog": [{"slug": "seo", "name": "SEO", "display": "SEO", "keywords": ["seo"]}]
        }
        ai = "SEO çalışmalarının maliyeti kapsamınıza göre netleşir."
        r = _build(ai, cfg=no_price_cfg, is_price_q=True, service_label="seo")
        assert r["outbound_text"] == ai
        assert r["source"] == "info_ai_no_config_price"


# ============================================================
# Guard 5 — Discount / campaign hallucination
# ============================================================

class TestGuard5DiscountHallucination:

    def test_campaign_blocked(self):
        ai = "Bu ay kampanyalı 3.000 TL ile web sitesi yapıyoruz!"
        r = _build(ai, service_label="web_tasarim")
        assert r["block_reason"] == "unconfigured_discount_or_price"
        assert "kampanya" not in r["outbound_text"].lower()

    def test_indirim_blocked(self):
        ai = "Özel indirimli fiyat: 5.000 TL."
        r = _build(ai, service_label="web_tasarim")
        assert r["block_reason"] == "unconfigured_discount_or_price"
        assert "indirim" not in r["outbound_text"].lower()

    def test_campaign_no_config_price_uses_scope_fallback(self):
        ai = "Bu ay ücretsiz deneme sunuyoruz."
        r = _build(ai, cfg=_EMPTY_CFG, service_label=None)
        assert r["block_reason"] == "unconfigured_discount_or_price"
        assert "kaydınız korunuyor" not in r["outbound_text"]


# ============================================================
# All guards passed — AI preserved
# ============================================================

class TestAIPreserved:

    def test_service_overview_short_ai_preserved(self):
        ai = "Web sitesi, otomasyon ve reklam yönetimi konularında hizmet sunuyoruz."
        r = _build(ai, message_text="Hizmetleriniz neler?")
        assert r["outbound_text"] == ai
        assert r["source"] == "info_ai_preserved"
        assert r["block_reason"] is None

    def test_capability_yes_ai_preserved(self):
        ai = "Evet, web sitesi tasarımı ve geliştirmesi yapıyoruz."
        r = _build(ai, message_text="Siz web sitesi yapıyor musunuz?")
        assert r["outbound_text"] == ai

    def test_preconsultation_ai_preserved(self):
        ai = "Ön görüşmede ihtiyacınızı, mevcut sürecinizi ve hedefinizi netleştiriyoruz."
        r = _build(ai, message_text="Ön görüşmede ne konuşacağız?")
        assert r["outbound_text"] == ai
        assert r["block_reason"] is None

    def test_business_fit_ai_preserved(self):
        ai = "Güzellik salonu için web sitesi ve sosyal medya yönetimi en uygun paket olur."
        r = _build(ai, message_text="Ben güzellik salonuyum, bana ne uygun?")
        assert r["outbound_text"] == ai

    def test_business_identity_ai_preserved(self):
        """User says 'Ben dövmeciyim' → AI reply about DOEL digital services preserved."""
        ai = "DOEL Digital olarak dövme salonları için web sitesi, reklam ve sosyal medya hizmetleri sunuyoruz."
        r = _build(ai, message_text="Ben dövmeciyim.")
        assert r["outbound_text"] == ai


# ============================================================
# Spec acceptance scenarios
# ============================================================

class TestPhase4CAcceptanceScenarios:

    def test_spec_web_site_price_correct(self):
        """'Web sitesi ne kadar?' + AI gives 12.900 TL → AI preserved."""
        ai = "Web sitesi paketi 12.900 TL."
        r = _build(ai, message_text="Web sitesi ne kadar?", is_price_q=True, service_label="web_tasarim")
        assert r["outbound_text"] == ai
        assert r["block_reason"] is None

    def test_spec_web_site_price_wrong(self):
        """'Web sitesi ne kadar?' + AI gives 8.000 TL → corrected to 12.900 TL."""
        ai = "Web sitesi 8.000 TL."
        r = _build(ai, message_text="Web sitesi ne kadar?", is_price_q=True, service_label="web_tasarim")
        assert "12.900" in r["outbound_text"]
        assert "8.000" not in r["outbound_text"]

    def test_spec_unknown_price_question(self):
        """'Ne kadar?' with unknown service → clarification, no random price."""
        ai = "Fiyatlar kapsama göre değişir."
        r = _build(ai, message_text="Ne kadar?", is_price_q=True, service_label=None)
        assert "hangi hizmet" in r["outbound_text"].lower()

    def test_spec_phone_prompt_not_added_to_price_answer(self):
        """'Web sitesi ne kadar?' → no phone/name prompt in output."""
        ai = "Web sitesi paketi 12.900 TL. Telefon numaranızı alabilir miyim?"
        r = _build(ai, message_text="Web sitesi ne kadar?", is_price_q=True,
                   service_label="web_tasarim", wants_booking=False)
        assert "telefon numaranızı" not in r["outbound_text"].lower()

    def test_spec_campaign_hallucination(self):
        """AI says 'Bu ay kampanyalı 3.000 TL' → blocked, config price or scope fallback."""
        ai = "Bu ay kampanyalı 3.000 TL ile web sitesi yapıyoruz!"
        r = _build(ai, message_text="Web sitesi ne kadar?", is_price_q=True, service_label="web_tasarim")
        assert "3.000" not in r["outbound_text"]
        assert "kampanya" not in r["outbound_text"].lower()

    def test_spec_tatoo_capability_question(self):
        """'Siz dövme yapıyor musunuz?' → AI reply preserved (short capability answer)."""
        ai = "Hayır, dövme hizmeti vermiyoruz. DOEL Digital olarak web sitesi, reklam ve otomasyon hizmetleri sunuyoruz."
        r = _build(ai, message_text="Siz dövme yapıyor musunuz?")
        assert r["outbound_text"] == ai

    def test_spec_tatoo_identity_no_rejection(self):
        """'Ben dövmeciyim.' → AI answer about digital services preserved."""
        ai = "Dövme salonları için web sitesi ve Instagram reklamları ile yeni müşteri tabanı oluşturabilirsiniz."
        r = _build(ai, message_text="Ben dövmeciyim.")
        assert r["outbound_text"] == ai
        assert "biz dövme yapmıyoruz" not in r["outbound_text"].lower()

    def test_spec_preconsultation_no_field_prompt(self):
        """'Ön görüşmede ne konuşacağız?' → explanation, no ad/telefon/tarih."""
        ai = "Ön görüşmede ihtiyacınızı ve hedeflerinizi netleştiriyoruz."
        r = _build(ai, message_text="Ön görüşmede ne konuşacağız?")
        assert r["outbound_text"] == ai
        assert "telefon" not in r["outbound_text"].lower()
        assert "adınızı" not in r["outbound_text"].lower()

    def test_spec_ai_vague_price_with_service_known_gets_config(self):
        """AI says 'kapsama göre değişir' but service is web_tasarim → config price added."""
        ai = "Kapsama göre değişebilir, ön görüşmede netleştiriyoruz."
        r = _build(ai, message_text="Web sitesi ne kadar?", is_price_q=True, service_label="web_tasarim")
        assert "12.900" in r["outbound_text"]


# ============================================================
# Invariants
# ============================================================

class TestPhase4CInvariants:

    def test_output_never_hardcoded_if_else_chain(self):
        """Hardcoded per-intent fallback strings must NOT appear as primary output."""
        safe_messages = [
            "Ön görüşmede ne konuşacağız?",
            "Hizmetleriniz neler?",
            "Ben güzellik salonuyum.",
        ]
        for msg in safe_messages:
            ai = "Kısa, doğal bir cevap."
            r = _build(ai, message_text=msg)
            # AI preserved (no hardcoded override)
            assert r["outbound_text"] == ai, f"Hardcoded override for: {msg}"

    def test_no_appointment_create_in_info_path(self):
        """build_info_answer_final never sets appointment_created."""
        r = _build("Hizmetlerimiz var.", message_text="Hizmetleriniz neler?")
        assert "appointment_created" not in r or r.get("appointment_created") is None

    def test_source_always_present(self):
        for ai in ["Kısa cevap.", None, "", "Error: LLM error"]:
            r = _build(ai)
            assert "source" in r
            assert r["source"]
