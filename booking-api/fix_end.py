import codecs
content = r"""            metrics=metrics,
            decision_path=decision_path,
        )


def invoke_generic_llm(message_text: str, conversation: dict, memory: dict, history: list[dict]) -> dict:
    cfg = get_config()
    business_context = json.dumps(cfg, ensure_ascii=False)
    
    # Minimize context parsing, formatting user messages
    recent = "\n".join([f"{msg.get('direction', 'IN').upper()}: {msg.get('message_text', '')}" for msg in history[-10:]])
    
    # Exposing missing booking fields to explicitly direct the AI on what to ask if it proceeds to 'active_booking'
    missing = []
    if not memory.get("requested_service"): missing.append("Hizmet Türü")
    if not conversation.get("phone"): missing.append("Telefon Numarası")
    if not conversation.get("lead_name"): missing.append("İsim Soyisim")
    if not conversation.get("requested_date") or not conversation.get("requested_time"): missing.append("Tarih ve Saat")
    
    system_prompt = f\"\"\"Sen {cfg.get('business_name')} firmasının dijital asistanısın. Müşterilerle doğal, insansı ve yardımcı bir dilde Türkçe konuş.
Senin GÖREVİN:
1. Önce müşterinin sorduğu soruyu net, doğru ve doğrudan Business bilgisine dayanarak yanıtla. Bilmediğin bilgiyi asla uydurma.
2. Soru cevaplandıktan sonra, eğer bir hizmete ilgi gösteriyorsa VEYA doğrudan randevu/ön görüşme almak istiyorsa, onu Booking flow'a (randevu flow) yönlendir.
3. Rakamları uydurma! Fiyat, saat veya hizmet config'de yoksa söyleme!

Kritik Kural = Birisi 'randevu almak istiyorum' derse, randevu akışına gir ve missing fields arrayinden BİRİNCİ sırada eksik olanı GÜZEL bir dille sor. Hepsini aynı anda sorma!
Şu an randevu için eksik olan kritik bilgiler: {', '.join(missing) if missing else 'YOK. Randevu Onaylanabilir.'}

İŞLETME BİLGİSİ (Business Context):
{business_context}

SON KONUŞMA GEÇMİŞİ:
{recent}

Müşterinin yeni mesajını incele. Oku ve aşağıdaki JSON formatına SIKI SIKIYA uygun bir yanıt dön:
{{
    "intent": "direct_answer" | "service_question" | "price_question" | "booking_request" | "active_booking" | "human_handoff" | "fallback",
    "reply_text": "Müşteriye yazacağın Türkçe doğal yanıt",
    "extracted_entities": {{
        "lead_name": "Eğer bu mesajda veya geçmişte müşterinin ismini bulduysan çıkar, yoksa null",
        "phone": "Eğer tam bir telefon numarası verildiyse çıkar, yoksa null",
        "requested_service": "Eğer config.services içinden biri istenmişse o hizmetin ismini yaz, yoksa null",
        "requested_date": "YYYY-MM-DD olarak tarih (varsa), yoksa null",
        "requested_time": "HH:MM olarak saat (varsa), yoksa null",
        "customer_goal": "Müşterinin elde etmek istediği amaç. (Yoksa null)"
    }},
    "requires_human": true | false
}}\"\"\"

    try:
        content = call_llm_content(system_prompt=system_prompt, user_prompt=message_text, is_json=True)
        return json.loads(content) if isinstance(content, str) else content
    except Exception as e:
        logger.error(f"Generic engine LLM Error: {e}")
        return {
            "intent": "fallback",
            "reply_text": cfg.get("fallback_reply", "Şu an cevap veremiyorum, lütfen daha sonra tekrar yazın."),
            "extracted_entities": {},
            "requires_human": False
        }

def generic_quality_guard(reply: str, extracted: dict, memory: dict, cfg: dict) -> Tuple[str, Optional[str]]:
    # 1. Config Service Matching
    if extracted.get("requested_service"):
        valid_services = [s.get("name", "").lower() for s in cfg.get("service_catalog", [])] + [s.get("display", "").lower() for s in cfg.get("service_catalog", [])]
        matched = False
        for svc in valid_services:
            if svc and svc in extracted["requested_service"].lower():
                matched = True
        if not matched:
            extracted["requested_service"] = None
            
    # 2. Prevent Booking confirmed without real fields locally
    if "Kaydınız oluşturuldu" in reply and (not memory.get("customer_phone") or not memory.get("requested_service")):
        return "İşlemlerinize devam edebilmem için lütfen bilgileri eksiksiz tamamlayalım.", "prevent_premature_confirm"
        
    return reply, None
"""

with codecs.open("app/generic_core.py", "r", "utf-8") as f:
    text = f.read()

text = text.split("            metrics=metri")[0] + content

with codecs.open("app/generic_core.py", "w", "utf-8") as f:
    f.write(text)

