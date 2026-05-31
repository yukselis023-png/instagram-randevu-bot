from pathlib import Path
path = Path('booking-api/app/main.py')
text = path.read_text(encoding='utf-8')
orig = text
replacements = [
    (
        '''NAME_PATTERNS = [
    re.compile(r"\b(?:ben|adım|ismim|ad soyad)\s+([a-zçğıöşü\s]{2,40})", re.IGNORECASE),
    re.compile(r"\b(?:ismim de)\s+([a-zçğıöşü\s]{2,40})", re.IGNORECASE),
]
''',
        '''NAME_PATTERNS = [
    re.compile(r"\b(?:benim\s+adım(?:\s+soyadım)?|adım(?:\s+soyadım)?|ad\s*soyad(?:ım)?|ismim|isim\s*soyisim|adım\s*:)\s+([a-zçğıöşü\s]{2,60})", re.IGNORECASE),
    re.compile(r"\b(?:ismim\s+de|müşteri\s+adı|musteri\s+adi)\s+([a-zçğıöşü\s]{2,60})", re.IGNORECASE),
]
MONTH_NAME_MAP = {
    "ocak": 1,
    "şubat": 2,
    "subat": 2,
    "mart": 3,
    "nisan": 4,
    "mayıs": 5,
    "mayis": 5,
    "haziran": 6,
    "temmuz": 7,
    "ağustos": 8,
    "agustos": 8,
    "eylül": 9,
    "eylul": 9,
    "ekim": 10,
    "kasım": 11,
    "kasim": 11,
    "aralık": 12,
    "aralik": 12,
}
''',
    ),
    (
        '''def extract_name(text: str, state: str) -> str | None:
    if "?" in text or is_service_overview_question(text) or is_price_question(text) or match_faq_response(text):
        return None
    if is_assistant_identity_question(text) or is_owner_check_message(text) or is_booking_assumption_rejection(text):
        return None
    if is_presence_check_message(text) or is_smalltalk_message(text) or is_low_signal_message(text):
        return None
    if is_all_choice_message(text) or is_confirmation_acceptance_message(text) or is_offer_hesitation_message(text) or is_request_reason_question(text):
        return None
    if detect_business_sector(text) or is_business_context_intro_message(text) or is_business_need_analysis_message(text):
        return None
    if match_objection_type(text) or is_service_advice_request(text) or is_comparison_request(text, match_service_candidates(text, None)):
        return None
    if match_service_candidates(text, None):
        return None
    if extract_phone(text) or extract_date(text) or extract_time_for_state(text, state) or extract_preferred_period(text):
        return None
    for pattern in NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            return titlecase_name(match.group(1))
    clean = sanitize_text(re.sub(r"[^a-zA-ZçğıöşüÇĞİÖŞÜ\s]", "", text))
    words = clean.split()
    if state != "collect_name":
        return None
    if 1 <= len(words) <= 3 and not any(w.lower() in NON_NAME_WORDS for w in words):
        return titlecase_name(clean)
    return None
''',
        '''def extract_name(text: str, state: str) -> str | None:
    for pattern in NAME_PATTERNS:
        match = pattern.search(text)
        if match:
            candidate = titlecase_name(match.group(1))
            if candidate:
                return candidate
    if "?" in text or is_service_overview_question(text) or is_price_question(text) or match_faq_response(text):
        return None
    if is_assistant_identity_question(text) or is_owner_check_message(text) or is_booking_assumption_rejection(text):
        return None
    if is_presence_check_message(text) or is_smalltalk_message(text) or is_low_signal_message(text):
        return None
    if is_all_choice_message(text) or is_confirmation_acceptance_message(text) or is_offer_hesitation_message(text) or is_request_reason_question(text):
        return None
    if detect_business_sector(text) or is_business_context_intro_message(text) or is_business_need_analysis_message(text):
        return None
    if match_objection_type(text) or is_service_advice_request(text) or is_comparison_request(text, match_service_candidates(text, None)):
        return None
    if match_service_candidates(text, None):
        return None
    if extract_phone(text) or extract_date(text) or extract_time_for_state(text, state) or extract_preferred_period(text):
        return None
    clean = sanitize_text(re.sub(r"[^a-zA-ZçğıöşüÇĞİÖŞÜ\s]", "", text))
    words = clean.split()
    if state != "collect_name":
        return None
    if 1 <= len(words) <= 3 and not any(w.lower() in NON_NAME_WORDS for w in words):
        return titlecase_name(clean)
    return None
''',
    ),
    (
        '''def extract_date(text: str) -> str | None:
    if not has_date_cue(text):
        return None
    lowered = text.lower()
    today = datetime.now(TZ).date()
''',
        '''def extract_date(text: str) -> str | None:
    if not has_date_cue(text):
        return None
    lowered = text.lower()
    today = datetime.now(TZ).date()
    month_named = re.search(
        r"\b(\d1,2})\s+(ocak|şubat|subat|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|eylül|eylul|ekim|kasım|kasim|aralık|aralik)(?:\s+(\d2,4}))?\b",
        lowered,
        re.IGNORECASE,
    )
    if month_named:
        day = int(month_named.group(1))
        month = MONTH_NAME_MAP[month_named.group(2).lower()]
        year_raw = month_named.group(3)
        year = int(year_raw) if year_raw else today.year
        if year_raw and len(year_raw) == 2:
            year += 200
        try:
            parsed = date(year, month, day)
            if not year_raw and parsed < today:
                parsed = date(today.year + 1, month, day)
            return parsed.isoformat()
        except ValueError:
            pass
''',
    ),
    (
        '''        if extracted_name and (not conversation.get("full_name") or conversation.get("state") == "collect_name"):
            conversation["full_name"] = extracted_name
        elif not conversation.get("full_name") and detected_name:
            conversation["full_name"] = detected_name
        if not conversation.get("phone") and detected_phone:
            conversation["phone"] = detected_phone
        if picked_service and (not conversation.get("service") or conversation.get("state") == "collect_service"):
            conversation["service"] = picked_service
        elif not conversation.get("service") and detected_service:
            conversation["service"] = detected_service
        if not conversation.get("requested_date") and detected_date:
            conversation["requested_date"] = detected_date
        if not conversation.get("requested_time") and detected_time:
            conversation["requested_time"] = detected_time
''',
        '''        current_name = sanitize_text(conversation.get("full_name") or "")
        username_like_name = bool(current_name) and current_name.lower() in {
            sanitize_text(payload.instagram_username or "").lower(),
            sanitize_text(payload.sender_id or "").lower(),
            sanitize_text(conversation.get("instagram_user_id") or "").lower(),
        }
        if extracted_name and (not current_name or conversation.get("state") == "collect_name" or username_like_name):
            conversation["full_name"] = extracted_name
        elif not current_name and detected_name:
            conversation["full_name"] = detected_name
        if detected_phone and normalize_phone(conversation.get("phone")) != normalize_phone(detected_phone):
            conversation["phone"] = detected_phone
        if picked_service and (not conversation.get("service") or conversation.get("state") == "collect_service"):
            conversation["service"] = picked_service
        elif not conversation.get("service") and detected_service:
            conversation["service"] = detected_service
        if detected_date and normalize_date_string(conversation.get("requested_date")) != detected_date:
            conversation["requested_date"] = detected_date
        if detected_time and normalize_time_string(conversation.get("requested_time")) != detected_time:
            conversation["requested_time"] = detected_time
''',
    ),
]
for old, new in replacements:
    if old not in text:
        raise SystemExit('replacement block not found: ' + old[:120])
    text = text.replace(old, new, 1)
if text == orig:
    raise SystemExit('no changes made')
path.write_text(text, encoding='utf-8')
print('patched', path)
