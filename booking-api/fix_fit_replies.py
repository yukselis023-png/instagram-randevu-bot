with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

crm_old = """    if "crm" in msg_lowered:
        return "CRM sistemi, verilerinizi kaybolmadan tek noktadan takip etmeye yarar. Birden fazla personelle çalışıyor veya çok randevu alıyorsanız mutlaka işinize yarar.\""""

social_old = """    if "sosyal" in msg_lowered or "medya" in msg_lowered:
        return "Sosyal medya yönetimi, dijital vizyonunuzu profesyonel göstermek için gereklidir ancak doğrudan sıcak müşteri artışı istiyorsanız reklamlar daha hızlı sonuç verir.\""""

crm_new = """    if "crm" in msg_lowered:
        return "CRM, gelen müşteri taleplerini, randevuları ve takip süreçlerini düzenlemek istiyorsanız işe yarar. Eğer müşteriler karışıyor, geri dönüşler unutuluyor veya randevuları manuel takip ediyorsanız mantıklı olur.\""""

social_new = """    if "sosyal" in msg_lowered or "medya" in msg_lowered:
        return "Sosyal medya yönetimi, markanızın daha profesyonel görünmesi, düzenli içerik paylaşması ve güven oluşturması için işe yarar. Instagram’da daha görünür olmak ve hesabı düzenli yönetmek istiyorsanız mantıklı olur; direkt müşteri kazanımı hedefleniyorsa reklamla birlikte düşünülmeli.\""""

if crm_old in text and social_old in text:
    text = text.replace(crm_old, crm_new)
    text = text.replace(social_old, social_new)
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(text)
    print("Replaced explicitly!")
else:
    print("Could not exact match, regex replacing...")
    # fallback to regex
    import re
    text = re.sub(
        r'if "crm" in msg_lowered:\s*return "[^"]+"',
        crm_new.strip(),
        text
    )
    text = re.sub(
        r'if "sosyal" in msg_lowered or "medya" in msg_lowered:\s*return "[^"]+"',
        social_new.strip(),
        text
    )
    with open("app/main.py", "w", encoding="utf-8") as f:
        f.write(text)

print("done patching main.py")
