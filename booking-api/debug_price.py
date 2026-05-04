import re
with open("app/main.py", "r", encoding="utf-8") as f:
    text = f.read()

debug_inject = '''
    if is_price_question(message_text):
        print("XXX PRICE QUESTION TRIGGERED XXX")
'''

text = text.replace('    if is_price_question(message_text):', debug_inject + '    if is_price_question(message_text):')

with open("app/main.py", "w", encoding="utf-8") as f:
    f.write(text)

