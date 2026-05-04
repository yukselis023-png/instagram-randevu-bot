import re

with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

# Remove the inner `if is_payment_question(message_text):` inside completed block
# and place it as a top-level block under 4. direct_answers

def remove_inner(text):
    old_inner = """        if is_payment_question(message_text):
            decision["reply_text"] = "Görüşmede ödeme detaylarını konuşuruz; şu an için bir ön ödeme talep etmiyoruz."
            decision["intent"] = "payment_info"
            decision["booking_intent"] = False
            decision["missing_fields"] = []
            decision["should_reply"] = True
            return decision"""
    
    # We replace it with nothing
    t2 = text.replace(old_inner, "")
    
    new_top = """    # 4. direct_answers
    if is_payment_question(message_text):
        decision["reply_text"] = "Görüşmede ödeme detaylarını konuşuruz; şu an için bir ön ödeme talep etmiyoruz."
        decision["intent"] = "payment_info"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        decision["should_reply"] = True
        return decision

    if is_ambiguous_appointment_question(message_text):"""
    
    t3 = t2.replace("    # 4. direct_answers\n    if is_ambiguous_appointment_question(message_text):", new_top)
    return t3

new_text = remove_inner(text)
with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(new_text)

print("done")
