import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Fix price hardcodes
old_str_1 = 'return "Fiyat seçilecek sisteme göre değişir; web/landing page, reklam ve otomasyon ayrı kapsamlarla hazırlanıyor. İşletmenize en uygun başlangıcı netleştirirsek gereksiz maliyet çıkarmadan fiyat verebiliriz."'
old_str_2 = 'return "Fiyat seçilecek hizmete göre değişir; web, reklam ve otomasyon ayrı kapsamlarla hazırlanıyor. İhtiyacınıza uygun başlangıcı netleştirirsek doğru fiyatı çıkarabiliriz."'

new_str = '''from app.main import get_config
    return f"Fiyat seçilecek hizmete göre değişiyor. Detayları kısa bir {get_config().get('booking_mode', 'görüşme')}de netleştirebiliriz."'''
    
text = text.replace(old_str_1, new_str)
text = text.replace(old_str_2, new_str)

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

print("done patching main")

with open("tests/test_generic_business_configs.py", "r", encoding="utf-8") as f:
    test_text = f.read()

test_text = test_text.replace(
    'conv["state"] = "collect_name"',
    'conv["state"] = "collect_name"\n        conv["service"] = "İmplant"'
)
with open("tests/test_generic_business_configs.py", "w", encoding="utf-8") as f:
    f.write(test_text)

print("done patching test")
