import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Greetings array replacing "Web tasarım, otomasyon, reklam veya..." with just a generic dynamic message
text = re.sub(
    r'Web tasarım, otomasyon, reklam veya sosyal medya tarafında hangi konuyla ilgileniyorsunuz\?',
    '{get_config().get("booking_mode", "randevu")} işleminiz için hangi hizmetle ilgilendiğinizi belirtebilir misiniz?',
    text
)

text = re.sub(
    r'Web tasarım, otomasyon, reklam veya sosyal medya tarafında hangisini merak ettiğinizi yazarsanız net bilgi vereyim\.',
    'Hangi hizmeti merak ettiğinizi yazarsanız net bilgi vereyim.',
    text
)

text = re.sub(
    r'Fiyat hizmete ve kapsama göre değişir\. Web tasarım.*?\.',
    'Fiyat hizmete ve kapsama göre değişir. Hangi hizmetimizle ilgilendiğinizi iletirseniz daha net bilgi paylaşabilirim.',
    text
)

# Fix apply_ai_first_quality_overrides fallback where it returns:
# return "Buradayım. Web, otomasyon, reklam veya sosyal medya tarafında neyi merak ettiğinizi yazarsanız net şekilde cevaplayayım."
# Wait, we already replaced this in the previous block. Let's make sure it's really gone.

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)
