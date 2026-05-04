import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

old_func = """def build_delivery_time_reply(service: dict[str, Any] | None = None) -> str:
    if service:
        display = str(service.get("display") or "Bu hizmet").strip()
        delivery_time = str(service.get("delivery_time") or "").strip()
        if delivery_time:
            return f"{display} için tahmini teslim süresi genelde {delivery_time}. Kapsam, entegrasyon sayısı ve hazır içerikler süreyi değiştirebilir."
    return "Tahmini teslim süresi kapsam netleşince doğru aralıkla paylaşılır. En doğru süre için ihtiyacı kısaca görmemiz gerekir."
"""

new_func = """def build_delivery_time_reply(service: dict[str, Any] | None = None) -> str:
    if service:
        display = str(service.get("display") or "Bu hizmet").strip()
        delivery_time = str(service.get("delivery_time") or "").strip()
        if delivery_time:
            return f"{display} için tahmini teslim süresi genelde {delivery_time}. Kapsam, entegrasyon sayısı ve hazır içerikler süreyi değiştirebilir."
    return "Tahmini teslim süresi kapsam netleşince doğru aralıkla paylaşılır. En doğru süre için ihtiyacı kısaca görmemiz gerekir."
"""

# Wait, the failure is because delivery_time attribute in doel.json was omitted or something.
# But wait, my script output 'i gn' in 'web tasarm iin...'. Wait, if it outputs the FALLBACK (kapsam netleince), it means delivery_time was missing for `web-tasarim`!
# Let me just check my config structure first.
