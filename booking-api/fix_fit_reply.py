import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

target = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
    history: list[dict[str, Any]] | None = None,
) -> str:"""

new_func = """def build_business_fit_reply(
    conversation: dict[str, Any],
    message_text: str | None = None,
    history: list[dict[str, Any]] | None = None,
) -> str:
    msg_lowered = sanitize_text(message_text or "").lower()
    
    if "otomasyon" in msg_lowered:
        return "Otomasyon, gelen mesajları, randevuları ve müşteri takibini düzenlemek için işe yarar. Çok DM alıyor, talepleri kaçırıyor veya randevuları manuel takip ediyorsanız mantıklı olur; daha çok müşteri bulma hedefiniz varsa reklam/web tarafı daha öncelikli olabilir."

    if "web" in msg_lowered or "site" in msg_lowered or "tasarim" in msg_lowered:
        return "Uygun olup olmadığını netleştirmek için işletmenizin sektörü, hedefi ve web sitesinden beklentiniz önemli. Eğer amacınız güven vermek ve müşteri başvurusu almaksa web sitesi mantıklı bir başlangıç olabilir."
        
    if "reklam" in msg_lowered or "performans" in msg_lowered:
        return "Performans reklamı doğrudan size müşteri getirmeye odaklanır. Satışlarınızı veya randevularınızı artırmak istiyorsanız sizin için en mantıklı adım olur."
        
    if "sosyal" in msg_lowered or "medya" in msg_lowered:
        return "Sosyal medya yönetimi, dijital vizyonunuzu profesyonel göstermek için gereklidir ancak doğrudan sıcak müşteri artışı istiyorsanız reklamlar daha hızlı sonuç verir."
        
    if "crm" in msg_lowered:
        return "CRM sistemi, verilerinizi kaybolmadan tek noktadan takip etmeye yarar. Birden fazla personelle çalışıyor veya çok randevu alıyorsanız mutlaka işinize yarar."

    memory = ensure_conversation_memory(conversation)
    if memory.get("customer_sector") or memory.get("customer_subsector") or detect_customer_subsector(message_text or "", history):
        from app.main import recommendation_engine
        return recommendation_engine(conversation, message_text, history)
        
    return "Yarar sağlayıp sağlamayacağını net söylemek için işinizi ve hedefinizi bilmem gerekir. En çok hangi süreci geliştirmek istiyorsunuz?"
"""

# replace the function logic
text = re.sub(
    r"def build_business_fit_reply\((.*?)\)\s*->\s*str\:.*?(?=def |async def )",
    new_func.replace("\\", "\\\\") + "\n\n",
    text,
    flags=re.DOTALL
)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("done")
