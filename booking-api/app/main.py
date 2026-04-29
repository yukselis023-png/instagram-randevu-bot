import json
import hashlib
import logging
import os
import re
import threading
import unicodedata
import time as time_module
from datetime import date, datetime, time, timedelta
from typing import Any
from uuid import uuid4
from zoneinfo import ZoneInfo

import requests
from dateparser.search import search_dates
from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import psycopg
from psycopg.rows import dict_row

TIMEZONE = os.getenv("TIMEZONE", "Europe/Istanbul")
TZ = ZoneInfo(TIMEZONE)
APP_BUILD_VERSION = os.getenv("APP_BUILD_VERSION") or os.getenv("RENDER_GIT_COMMIT") or "local"
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://n8n:n8n@postgres:5432/n8n")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("booking_api")
CRM_SYNC_GUARD_LOCK = threading.Lock()
CRM_SYNC_LATEST_OBSERVED: dict[str, str] = {}
LIVE_CRM_AUTH_CACHE_LOCK = threading.Lock()
LIVE_CRM_AUTH_CACHE: dict[str, Any] = {}
LIVE_CRM_SERVICES_CACHE_LOCK = threading.Lock()
LIVE_CRM_SERVICES_CACHE: dict[str, float] = {}
LIVE_CRM_SLOT_CACHE_LOCK = threading.Lock()
LIVE_CRM_SLOT_CACHE: dict[str, dict[str, Any]] = {}
LIVE_CRM_TASK_GUARD_LOCK = threading.Lock()
LIVE_CRM_TASK_LATEST_OBSERVED: dict[str, str] = {}
BUSINESS_NAME = os.getenv("BUSINESS_NAME", "İşletme")
BUSINESS_PHONE = os.getenv("BUSINESS_PHONE", "").strip()
BUSINESS_EMAIL = os.getenv("BUSINESS_EMAIL", "").strip()
BUSINESS_WEBSITE = os.getenv("BUSINESS_WEBSITE", "").strip()
BUSINESS_TAGLINE = os.getenv("BUSINESS_TAGLINE", "").strip()
DEFAULT_SERVICE_NAME = os.getenv("DEFAULT_SERVICE_NAME", "Ön görüşme")
WORKING_HOURS_START = os.getenv("WORKING_HOURS_START", "10:00")
WORKING_HOURS_END = os.getenv("WORKING_HOURS_END", "19:00")
SLOT_DURATION_MINUTES = int(os.getenv("SLOT_DURATION_MINUTES", "60"))
SLOT_BUFFER_MINUTES = int(os.getenv("SLOT_BUFFER_MINUTES", "10"))
APPOINTMENT_LOOKAHEAD_DAYS = int(os.getenv("APPOINTMENT_LOOKAHEAD_DAYS", "30"))
AI_FIRST_BOOKING_SLOT_LIMIT = int(os.getenv("AI_FIRST_BOOKING_SLOT_LIMIT", "4"))
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "https://api.groq.com/openai/v1").rstrip("/")
LLM_API_KEY = os.getenv("LLM_API_KEY", "").strip()
LLM_MODEL = os.getenv("LLM_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct")
LLM_FALLBACK_MODEL = os.getenv("LLM_FALLBACK_MODEL", "llama-3.1-8b-instant")
LLM_EXTRACT_TIMEOUT_SECONDS = float(os.getenv("LLM_EXTRACT_TIMEOUT_SECONDS", "6"))
LLM_REPLY_POLISH_TIMEOUT_SECONDS = float(os.getenv("LLM_REPLY_POLISH_TIMEOUT_SECONDS", "8"))
LLM_REPLY_MICRO_MODEL = os.getenv("LLM_REPLY_MICRO_MODEL", "llama-3.1-8b-instant").strip() or LLM_MODEL
LLM_REPLY_ADVISORY_MODEL = os.getenv("LLM_REPLY_ADVISORY_MODEL", "meta-llama/llama-4-scout-17b-16e-instruct").strip() or LLM_MODEL
LLM_REPLY_QUALITY_MODEL = (
    os.getenv("LLM_REPLY_QUALITY_MODEL")
    or os.getenv("LLM_QUALITY_MODEL")
    or "llama-3.3-70b-versatile"
).strip()
LLM_REPLY_MICRO_TIMEOUT_SECONDS = float(os.getenv("LLM_REPLY_MICRO_TIMEOUT_SECONDS", "6.5"))
LLM_REPLY_ADVISORY_TIMEOUT_SECONDS = float(os.getenv("LLM_REPLY_ADVISORY_TIMEOUT_SECONDS", str(LLM_REPLY_POLISH_TIMEOUT_SECONDS)))
LLM_REPLY_MICRO_MAX_TOKENS = int(os.getenv("LLM_REPLY_MICRO_MAX_TOKENS", "48"))
LLM_REPLY_ADVISORY_MAX_TOKENS = int(os.getenv("LLM_REPLY_ADVISORY_MAX_TOKENS", "90"))
LLM_REPLY_POLISH_ENABLED = True
FULL_AI_CONVERSATIONAL_MODE = True
REPLY_ENGINE = "ai_first_v2"
AI_FIRST_ENABLED = True
REPLY_GUARANTEE_ENABLED = True
CRM_SYNC_ENABLED = os.getenv("CRM_SYNC_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
CRM_SUPABASE_URL = os.getenv("CRM_SUPABASE_URL", "").rstrip("/")
CRM_SUPABASE_ANON_KEY = os.getenv("CRM_SUPABASE_ANON_KEY", "")
CRM_SUPABASE_EMAIL = os.getenv("CRM_SUPABASE_EMAIL", "").strip()
CRM_SUPABASE_PASSWORD = os.getenv("CRM_SUPABASE_PASSWORD", "")
CRM_WORKSPACE_ID = os.getenv("CRM_WORKSPACE_ID", "").strip()
CRM_SYNC_SOURCE = os.getenv("CRM_SYNC_SOURCE", "instagram_dm").strip() or "instagram_dm"
CRM_SYNC_TIMEOUT_SECONDS = int(os.getenv("CRM_SYNC_TIMEOUT_SECONDS", "10"))
CRM_SYNC_EVENT_LIMIT = int(os.getenv("CRM_SYNC_EVENT_LIMIT", "50"))
MAX_VOICE_NOTE_URL_LENGTH = int(os.getenv("MAX_VOICE_NOTE_URL_LENGTH", "1200000"))
LIVE_CRM_ENABLED = os.getenv("LIVE_CRM_ENABLED", "false").lower() in {"1", "true", "yes", "on"}
LIVE_CRM_SUPABASE_URL = os.getenv("LIVE_CRM_SUPABASE_URL", "").rstrip("/")
LIVE_CRM_SUPABASE_ANON_KEY = os.getenv("LIVE_CRM_SUPABASE_ANON_KEY", "")
LIVE_CRM_EMAIL = os.getenv("LIVE_CRM_EMAIL", "").strip()
LIVE_CRM_PASSWORD = os.getenv("LIVE_CRM_PASSWORD", "")
LIVE_CRM_PRECONSULTATION_STATUS = os.getenv("LIVE_CRM_PRECONSULTATION_STATUS", "preconsultation").strip() or "preconsultation"
LIVE_CRM_PRECONSULTATION_SERVICE = os.getenv("LIVE_CRM_PRECONSULTATION_SERVICE", "Ön Görüşme").strip() or "Ön Görüşme"
IG_LOGIN_USERNAME = os.getenv("IG_LOGIN_USERNAME", "").strip()
IG_BUSINESS_USER_ID = os.getenv("IG_BUSINESS_USER_ID", "").strip()
MORNING_REMINDER_ENABLED = os.getenv("MORNING_REMINDER_ENABLED", "true").lower() in {"1", "true", "yes", "on"}
MORNING_REMINDER_WINDOW_START = os.getenv("MORNING_REMINDER_WINDOW_START", "08:30")
MORNING_REMINDER_WINDOW_END = os.getenv("MORNING_REMINDER_WINDOW_END", "11:30")
MORNING_REMINDER_CLAIM_TIMEOUT_MINUTES = int(os.getenv("MORNING_REMINDER_CLAIM_TIMEOUT_MINUTES", "15"))
SERVICES_HINTS = [s.strip() for s in os.getenv("SERVICES_HINTS", "danışmanlık,görüşme").split(",") if s.strip()]
DOEL_SERVICE_CATALOG = [
    {
        "slug": "web-tasarim",
        "display": "Web Tasarım - KOBİ Paketi",
        "keywords": [
            "web tasarım", "web tasarim", "web sitesi", "website", "kurumsal site", "kurumsal web",
            "landing page", "landing", "açılış sayfası", "acilis sayfasi", "site tasarımı", "site tasarimi",
            "internet sitesi", "kurumsal web sitesi", "site yenileme",
        ],
        "price": "12.900 ₺",
        "price_note": "tek seferlik paket fiyatı",
        "delivery_time": "7-14 iş günü",
        "summary": "Google uyumlu, tüm cihazlara tam uyumlu, WhatsApp butonlu, 1 yıl altyapı garantili ve otomasyon altyapısına uygun kurumsal web tasarım çözümü.",
    },
    {
        "slug": "otomasyon-ai",
        "display": "Otomasyon & Yapay Zeka Çözümleri",
        "keywords": [
            "otomasyon", "yapay zeka", "chatbot", "n8n", "ai", "otomasyon sistemi", "dm otomasyonu",
            "instagram bot", "instagram dm bot", "yapay zeka bot", "randevu botu", "müşteri takibi",
            "musteri takibi", "crm", "entegrasyon", "workflow", "iş akışı", "is akisi", "süreç otomasyonu",
            "surec otomasyonu", "otomatik cevap", "mesaj otomasyonu", "sistem kurma",
        ],
        "price": "5.000 ₺",
        "price_note": "ilk 3 ay indirimli aylık hizmet bedeli",
        "delivery_time": "standart kurulumlarda 3-7 iş günü, özel entegrasyonlarda 1-3 hafta",
        "summary": "Müşteri mesajlarına 7/24 yanıt, randevuları otomatik ayarlama, teklif ve fatura otomasyonu, Instagram yorumlarına otomatik cevap ve Excel kayıt akışları içerir.",
    },
    {
        "slug": "performans-pazarlama",
        "display": "Performans Pazarlama",
        "keywords": [
            "performans pazarlama", "reklam yönetimi", "meta reklam", "tiktok reklam", "facebook reklam",
            "instagram reklam", "instagramdan reklam", "reklam çıkmak", "reklam cıkmak", "meta ads",
            "facebook ads", "lead", "müşteri kazanmak", "musteri kazanmak", "potansiyel müşteri",
            "potansiyel musteri", "dijital reklam", "kampanya yönetimi", "kampanya yonetimi", "reklam",
        ],
        "price": "7.500 ₺",
        "price_note": "aylık danışmanlık bedeli, reklam bütçesi dahil değildir",
        "summary": "Meta ve TikTok reklam yönetimi, hedef kitle ve rakip analizi, kreatif reklam tasarımları, haftalık raporlama ve optimizasyon ile yeniden pazarlama desteği sunar.",
    },
    {
        "slug": "sosyal-medya-yonetimi",
        "display": "Sosyal Medya Yönetimi",
        "keywords": [
            "sosyal medya", "içerik yönetimi", "icerik yonetimi", "topluluk yönetimi", "community management",
            "sayfa yönetimi", "sayfa yonetimi", "hesap yönetimi", "hesap yonetimi", "içerik üretimi",
            "icerik uretimi", "paylaşım planı", "paylasim plani", "reels yönetimi", "reels yonetimi",
        ],
        "price": "Özel teklif",
        "price_note": "marka ihtiyacına göre belirlenir",
        "summary": "Topluluk inşası, kriz yönetimi, içerik planlama ve markanın sesini büyüten sürdürülebilir sosyal medya yönetimi sunar.",
    },
    {
        "slug": "marka-stratejisi",
        "display": "Marka Stratejisi & Danışmanlık",
        "keywords": [
            "marka stratejisi", "danışmanlık", "marka danışmanlığı", "strateji", "konumlandırma",
            "markalasma", "markalaşma", "büyüme planı", "buyume plani", "go to market",
        ],
        "price": "Özel teklif",
        "price_note": "analiz kapsamına göre şekillenir",
        "summary": "Pazar analizi, rakip zekası, marka konumlandırma ve büyüme yol haritası hazırlığı sağlar.",
    },
    {
        "slug": "kreatif-produksiyon",
        "display": "Kreatif Prodüksiyon",
        "keywords": [
            "kreatif prodüksiyon", "kreatif produksiyon", "video çekimi", "video cekimi", "fotoğraf çekimi",
            "fotograf cekimi", "prodüksiyon", "produksiyon", "reels çekimi", "reels cekimi", "creative",
        ],
        "price": "Özel teklif",
        "price_note": "proje kapsamına göre belirlenir",
        "summary": "Premium estetik standartta görsel üretim, sinematik video ve dikkat durduran kreatif içerik prodüksiyonu sunar.",
    },
]

WORK_START = time.fromisoformat(WORKING_HOURS_START)
WORK_END = time.fromisoformat(WORKING_HOURS_END)
MORNING_REMINDER_START = time.fromisoformat(MORNING_REMINDER_WINDOW_START)
MORNING_REMINDER_END = time.fromisoformat(MORNING_REMINDER_WINDOW_END)

app = FastAPI(title="Instagram Booking API", version="1.0.0")
ALLOWED_ORIGINS = [
    origin.strip()
    for origin in os.getenv(
        "CORS_ALLOW_ORIGINS",
        "http://127.0.0.1:4173,http://localhost:4173,http://127.0.0.1:4299,http://localhost:4299,https://doel-crm.vercel.app",
    ).split(",")
    if origin.strip()
]
ALLOWED_ORIGIN_REGEX = os.getenv(
    "CORS_ALLOW_ORIGIN_REGEX",
    r"https://[a-z0-9-]+(?:-[a-z0-9-]+)*\.vercel\.app|http://127\.0\.0\.1:\d+|http://localhost:\d+",
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=ALLOWED_ORIGIN_REGEX,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS conversations (
    id BIGSERIAL PRIMARY KEY,
    instagram_user_id TEXT NOT NULL UNIQUE,
    instagram_username TEXT,
    full_name TEXT,
    phone TEXT,
    service TEXT,
    requested_date DATE,
    requested_time TIME,
    appointment_status TEXT NOT NULL DEFAULT 'collecting',
    state TEXT NOT NULL DEFAULT 'new',
    booking_kind TEXT,
    preferred_period TEXT,
    assigned_human BOOLEAN NOT NULL DEFAULT FALSE,
    last_customer_message TEXT,
    llm_notes TEXT,
    memory_state JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS appointments (
    id BIGSERIAL PRIMARY KEY,
    instagram_user_id TEXT NOT NULL,
    instagram_username TEXT,
    full_name TEXT NOT NULL,
    phone TEXT NOT NULL,
    service TEXT NOT NULL,
    appointment_date DATE NOT NULL,
    appointment_time TIME NOT NULL,
    status TEXT NOT NULL DEFAULT 'confirmed',
    source TEXT NOT NULL DEFAULT 'instagram_dm',
    notes TEXT,
    approval_status TEXT,
    approval_reason TEXT,
    rejection_reason TEXT,
    cancellation_reason TEXT,
    refund_status TEXT,
    refund_amount NUMERIC(12,2),
    refund_reason TEXT,
    capacity_units INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS message_logs (
    id BIGSERIAL PRIMARY KEY,
    instagram_user_id TEXT NOT NULL,
    direction TEXT NOT NULL,
    message_text TEXT,
    raw_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (direction IN ('in', 'out', 'system'))
);

CREATE INDEX IF NOT EXISTS idx_message_logs_inbound_message_id
    ON message_logs (instagram_user_id, (raw_payload->>'message_id'))
    WHERE direction = 'in' AND raw_payload ? 'message_id';

CREATE TABLE IF NOT EXISTS appointment_reminders (
    id BIGSERIAL PRIMARY KEY,
    appointment_id BIGINT NOT NULL REFERENCES appointments(id) ON DELETE CASCADE,
    reminder_kind TEXT NOT NULL,
    instagram_user_id TEXT NOT NULL,
    claim_token TEXT,
    claimed_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (appointment_id, reminder_kind)
);

CREATE TABLE IF NOT EXISTS customers (
    id BIGSERIAL PRIMARY KEY,
    instagram_user_id TEXT NOT NULL UNIQUE,
    instagram_username TEXT,
    full_name TEXT,
    phone TEXT,
    sector TEXT,
    segment TEXT,
    notes TEXT,
    preferences JSONB NOT NULL DEFAULT '{}'::jsonb,
    discount_code TEXT,
    custom_offer TEXT,
    subscription_renewal_date DATE,
    consent_status TEXT,
    consent_updated_at TIMESTAMPTZ,
    voice_note_url TEXT,
    customer_type TEXT,
    approval_status TEXT,
    approval_reason TEXT,
    rejection_reason TEXT,
    last_visit_at TIMESTAMPTZ,
    last_service TEXT,
    total_visits INTEGER NOT NULL DEFAULT 0,
    total_spend NUMERIC(12,2) NOT NULL DEFAULT 0,
    no_show_count INTEGER NOT NULL DEFAULT 0,
    last_contact_at TIMESTAMPTZ,
    next_automation_at TIMESTAMPTZ,
    next_automation_type TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS customer_service_history (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    appointment_id BIGINT REFERENCES appointments(id) ON DELETE SET NULL,
    service_name TEXT NOT NULL,
    service_category TEXT,
    visit_date DATE,
    visit_time TIME,
    spend_amount NUMERIC(12,2) NOT NULL DEFAULT 0,
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS message_templates (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    sector TEXT,
    trigger_type TEXT NOT NULL,
    content TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS automation_rules (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    title TEXT NOT NULL,
    sector TEXT,
    trigger_type TEXT NOT NULL,
    days_after INTEGER NOT NULL DEFAULT 0,
    template_slug TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS automation_events (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES customers(id) ON DELETE CASCADE,
    rule_id BIGINT REFERENCES automation_rules(id) ON DELETE SET NULL,
    template_slug TEXT NOT NULL,
    event_type TEXT NOT NULL,
    scheduled_at TIMESTAMPTZ NOT NULL,
    claim_token TEXT,
    claimed_at TIMESTAMPTZ,
    sent_at TIMESTAMPTZ,
    status TEXT NOT NULL DEFAULT 'queued',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS service_capacity_rules (
    id BIGSERIAL PRIMARY KEY,
    service_slug TEXT NOT NULL UNIQUE,
    service_name TEXT NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 1,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (capacity >= 1 AND capacity <= 20)
);

CREATE TABLE IF NOT EXISTS customer_work_items (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT REFERENCES customers(id) ON DELETE CASCADE,
    instagram_user_id TEXT,
    kind TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    reason TEXT,
    note TEXT,
    due_at TIMESTAMPTZ,
    resolved_at TIMESTAMPTZ,
    assigned_to TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS crm_campaigns (
    id BIGSERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    template_slug TEXT NOT NULL,
    segment TEXT,
    sector TEXT,
    inactivity_days INTEGER,
    attendance_status TEXT,
    audience_size INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'draft',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_customers_segment ON customers(segment);
CREATE INDEX IF NOT EXISTS idx_customers_next_automation_at ON customers(next_automation_at);
CREATE INDEX IF NOT EXISTS idx_customer_service_history_customer_id ON customer_service_history(customer_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_automation_events_status_scheduled_at ON automation_events(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_appointments_date_time_status ON appointments(appointment_date, appointment_time, status);
CREATE INDEX IF NOT EXISTS idx_customer_work_items_status_due ON customer_work_items(status, due_at);
CREATE INDEX IF NOT EXISTS idx_customer_work_items_customer_id ON customer_work_items(customer_id, created_at DESC);
"""

NAME_PATTERNS = [
    re.compile(r"(?:benim\s+adım(?:\s+soyadım)?|adım(?:\s+soyadım)?|ad\s*soyad(?:ım)?|ismim|isim\s*soyisim|adım\s*:)\s+([a-zçğıöşü\s]{2,60})", re.IGNORECASE),
    re.compile(r"(?:ismim\s+de|müşteri\s+adı|musteri\s+adi)\s+([a-zçğıöşü\s]{2,60})", re.IGNORECASE),
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
PHONE_PATTERN = re.compile(r"(?:(?:\+?90)|0)?\s*5\d{2}[\s\-]?\d{3}[\s\-]?\d{2}[\s\-]?\d{2}")
EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
TIME_PATTERN = re.compile(r"\b([01]?\d|2[0-3])[:.]([0-5]\d)\b")
HOUR_WORD_PATTERN = re.compile(r"\bsaat\s*([01]?\d|2[0-3])\b", re.IGNORECASE)
STANDALONE_TIME_PATTERN = re.compile(r"^\s*([01]?\d|2[0-3])[:.]([0-5]\d)\s*\??\s*$")
VOICE_DURATION_PLACEHOLDER_PATTERN = re.compile(r"^\s*([0-5])[:.]([0-5]\d)\s*$")
MESSAGE_VOLUME_PATTERN = re.compile(r"\b(\d{2,5})\s*(kişi|kisi|mesaj|dm|lead)\b", re.IGNORECASE)
NUMERIC_RANGE_ANSWER_PATTERN = re.compile(r"^\s*(\d{1,5})\s*[-–]\s*(\d{1,5})\s*\??\s*$")
PURE_NUMERIC_DATE_PATTERN = re.compile(r"^\s*\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\s*\??\s*$")
DATE_CUE_PATTERN = re.compile(
    r"(bugün\w*|bugun\w*|yarın\w*|yarin\w*|ertesi\s+gün\w*|ertesi\s+gun\w*|sonraki\s+gün\w*|sonraki\s+gun\w*|bir\s+sonraki\s+gün\w*|bir\s+sonraki\s+gun\w*|öbür\s+gün\w*|obur\s+gun\w*|evvelsi\s+gün\w*|evvelsi\s+gun\w*|evelsi\s+gün\w*|evelsi\s+gun\w*|haftaya\w*|pazartesi\w*|salı\w*|sali\w*|çarşamba\w*|carsamba\w*|perşembe\w*|persembe\w*|cuma\w*|cumartesi\w*|pazar\w*|ocak\w*|şubat\w*|subat\w*|mart\w*|nisan\w*|mayıs\w*|mayis\w*|haziran\w*|temmuz\w*|ağustos\w*|agustos\w*|eylül\w*|eylul\w*|ekim\w*|kasım\w*|kasim\w*|aralık\w*|aralik\w*|\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?)",
    re.IGNORECASE,
)
HUMAN_KEYWORDS = ["temsilci", "insan", "yetkili", "canlı destek", "canlı biri", "biri arasın", "siz arayın", "geri dönüş", "geri donus"]
CANCEL_KEYWORDS = ["iptal", "ertele", "değiştir", "degistir"]
NEW_APPOINTMENT_KEYWORDS = ["randevu", "uygun", "saat", "müsait", "musait", "gelmek istiyorum"]
WEEKDAY_MAP = {
    "pazartesi": 0,
    "salı": 1,
    "sali": 1,
    "çarşamba": 2,
    "carsamba": 2,
    "perşembe": 3,
    "persembe": 3,
    "cuma": 4,
    "cumartesi": 5,
    "pazar": 6,
}
NON_NAME_WORDS = {
    "merhaba", "selam", "sa", "randevu", "yarın", "bugün", "cumartesi", "pazar", "pazartesi", "salı",
    "çarşamba", "perşembe", "cuma", "saat", "müsait", "musait", "uygun", "saç", "cilt", "bakım",
    "botoks", "dolgu", "epilasyon", "lazer", "manikür", "pedikür", "fiyat", "bilgi", "sabah",
    "öğleden", "ogleden", "öğleden sonra", "ogleden sonra", "akşamüstü", "aksamustu",
    "otomasyon", "web", "reklam", "pazarlama", "tasarım", "tasarim", "yapay", "zeka", "dövme", "dovme", "dövmeci", "dovmeci", "dövmeciyim", "dovmeciyim", "tattoo",
    "devam", "anladım", "anladim", "başlayalım", "baslayalim", "oldu", "tamam", "evet", "hayır", "hayir",
    "biliyorum", "istiyorum", "istemiyorum", "gerek", "gerekli", "lazım", "lazim",
    "pahalı", "pahali", "ucuz", "indirim", "bütçe", "butce", "para", "ödeme", "odeme",
    "hepsi", "hiçbiri", "hicbiri", "şimdi", "simdi", "sonra", "daha",
    "teşekkür", "tesekkur", "teşekkürler", "tesekkurler", "sağol", "sagol",
    "nasıl", "nasil", "nedir", "kadar", "nerede", "hangi", "kimse",
}
GREETING_MESSAGES = {
    "merhaba", "merhabalar", "selam", "selamlar", "sa", "hey", "esenlikler", "günaydın", "gunaydin",
    "iyi akşamlar", "iyi aksamlar", "iyi günler", "iyi gunler", "iyi geceler", "iyi geceler"
}
LOW_SIGNAL_MESSAGES = {
    "?", "??", "tamam", "ok", "okay", "okey", "peki", "olur", "anladım", "anladim", "teşekkürler", "tesekkurler", "sağ ol", "sag ol"
}
REACTION_MESSAGES = {
    "hay allahım", "hay allahim", "hay allah", "allah allah", "off", "uff", "eh", "eyyyy", "eyyyy yo", "eyo", "yo", "lan", "ya of"
}
PRESENCE_CHECK_KEYWORDS = [
    "orda kimse var", "orada kimse var", "kimse var mı", "kimse var mi", "burada mısınız", "burada misiniz",
    "burda mısınız", "burda misiniz", "aktif misiniz", "bakıyor musunuz", "bakiyor musunuz", "bakan var mı", "bakan var mi",
    "pist", "pisst", "psst", "pışt", "pistt",
]
SMALLTALK_KEYWORDS = [
    "nasıl gidiyor", "nasil gidiyor", "nasılsın", "nasilsin", "nasılsınız", "nasilsiniz",
    "iyi misin", "iyi misiniz", "naber", "ne haber", "keyifler nasıl", "keyifler nasil",
]
FATIGUE_PAINPOINT_KEYWORDS = ["sıkıldım", "sikildim", "yoruldum", "çok yoğunum", "cok yogunum", "bunaldım", "bunaldim", "yetişemiyorum", "yetisemiyorum", "hay allah", "hay allahım", "hay allahim", "allah allah", "off", "uff", "yok artık", "yok artik"]
TECHNICAL_ISSUE_CONTEXT_KEYWORDS = ["api", "apı", "istek", "request", "webhook", "chat", "chatte", "chatinde", "instagram chat", "bot", "chatbot", "otomatik mesaj", "otomatik cevap"]
TECHNICAL_ISSUE_PROBLEM_KEYWORDS = ["gitmiyor", "gitmiyo", "gitmedi", "göndermiyor", "gondermiyor", "çalışmıyor", "calismiyor", "bozuk", "olmuyor", "olmuo", "yanlış", "yanlis", "hata", "bug", "sorun", "sıkıntı", "sikinti", "saçmalıyor", "sacmaliyor"]
TECHNICAL_ISSUE_DIRECT_PHRASES = ["apiye istek gitmiyor", "api'ye istek gitmiyor", "api istek gitmiyor", "otomatik mesaj atıyor", "otomatik cevap atıyor", "yanlış cevap veriyor", "yanlis cevap veriyor", "chatte sıkıntı", "chatte sikinti", "chatinde sıkıntı", "chatinde sikinti", "instagram chatinde", "instagram chatte"]
BUSINESS_NEED_ANALYSIS_KEYWORDS = [
    "bana ne lazım", "bana ne lazim", "bana ne gerekir", "neye ihtiyacım var", "neye ihtiyacim var",
    "bana ne lazım sizce", "ne önerirsin", "ne onerirsin",
    "işimi görür mü", "isimi gorur mu", "işime yarar mı", "isime yarar mi",
    "bana yarar mı", "bana yarar mi", "benim için uygun mu", "benim icin uygun mu",
    "bana uygun mu", "bana uygun olur mu", "bana göre mi", "bana gore mi",
    "işe yarar mı", "ise yarar mi", "faydası var mı", "faydasi var mi",
    "ne kadar etkili", "gerçekten işe yarıyor mu", "işe yarıyor mu",
    "sektörüme uygun mu", "sektorume uygun mu", "benim sektörüm için",
]
BUSINESS_CONTEXT_INTRO_KEYWORDS = ["işletiyorum", "isletiyorum", "salonum var", "merkezim var", "işletmem var", "isletmem var", "kliniğim var", "klinigim var"]
BEAUTY_BUSINESS_KEYWORDS = ["güzellik salonu", "guzellik salonu", "güzellik merkezi", "guzellik merkezi", "kuaför", "kuafor", "cilt bakım", "cilt bakımı", "cilt bakimi", "protez tırnak", "protez tirnak", "epilasyon", "lazer", "bakım merkezi", "bakim merkezi", "dövme", "dovme", "dövmeci", "dovmeci", "dövmeciyim", "dovmeciyim", "tattoo", "tattoo studio", "tattoo stüdyo", "tattoo studyo"]
REAL_ESTATE_BUSINESS_KEYWORDS = ["emlak", "gayrimenkul", "ilan", "portföy", "portfoy", "arsa", "daire", "konut", "kiralık", "kiralik", "satılık", "satilik", "yer gösterme", "yer gosterme"]
DM_DELAY_KEYWORDS = ["gecikme", "geç dönüş", "gec donus", "geç cevap", "gec cevap", "geç yanıt", "gec yanit", "yanıt zor", "yanit zor", "yavaş dönüş", "yavas donus"]
REPEATED_MESSAGE_ISSUE_KEYWORDS = ["tekrar eden mesaj", "tekrar eden mesajlar", "aynı sorular", "ayni sorular", "aynı şeyler", "ayni seyler", "aynı mesajlar", "ayni mesajlar", "sürekli aynı", "surekli ayni"]
MESSAGE_VOLUME_KEYWORDS = ["çok kişi yazıyor", "cok kisi yaziyor", "çok mesaj geliyor", "cok mesaj geliyor", "çok mesaj", "cok mesaj", "çok dm geliyor", "cok dm geliyor", "çok dm", "cok dm", "günde", "gunde", "günlük", "gunluk", "mesaj trafiği", "mesaj trafigi", "dm trafiği", "dm trafigi", "çok talep geliyor", "cok talep geliyor", "çok yoğun", "cok yogun"]
ALL_CHOICE_MESSAGES = {"hepsi", "hepsi lazım", "hepsi lazim", "hepsi olur", "hepsi olsun", "hepsi gerekli", "hepsi ya", "hepsi ya genel olarak", "genel olarak hepsi", "hepsi gerekiyor", "hepsi var", "hepsi yoruyor", "hep birlikte", "tamamı", "tamami", "her şey", "her sey", "hepsi hepsi", "ikisi", "ikisi de", "ikiside", "iki side", "ikidi de", "ikiside ya", "her ikisi", "ikisi de lazım", "ikisi de lazim"}
CONFIRMATION_ACCEPTANCE_MESSAGES = {
    "olur", "tamam", "evet", "uygun", "olabilir", "olur tabii", "olur tabi", "olur olur",
    "yapalım", "yapalim", "hadi yapalım", "hadi yapalim", "yapalım o zaman", "yapalim o zaman",
    "tamam yapalım", "tamam yapalim", "başlayalım", "baslayalim", "görüşelim", "goruselim",
}
CONVERSATION_MEMORY_DEFAULTS = {
    "customer_goal": None,
    "customer_sector": None,
    "pain_points": [],
    "last_bot_question_type": None,
    "answered_question_types": [],
    "open_loop": None,
    "pending_offer": None,
    "offer_status": "none",
    "last_recommended_solution": None,
    "topics_already_explained": [],
    "conversation_summary": None,
    "last_priority_choice": None,
    "last_dm_issue_choice": None,
    "message_volume_estimate": None,
    "reschedule_requested_time": None,
    "reschedule_requested_date": None,
    "suggested_booking_slots": [],
    "pending_requested_time": None,
}
OWNER_CHECK_KEYWORDS = [
    "sahibiyle mi görüşüyorum", "sahibi misiniz", "işletme sahibi", "firma sahibi", "kurucu ile mi görüşüyorum",
    "gerçek biri misin", "gercek biri misin", "siz bot musunuz", "bot musun", "asistan mısın", "asistan misin",
    "yetkili misiniz", "yetkili misin", "yetkili biri misiniz",
]
ASSISTANT_IDENTITY_KEYWORDS = [
    "adın ne", "adin ne", "adınız ne", "adiniz ne", "kimsin", "kimsiniz", "kimle görüşüyorum", "kiminle görüşüyorum", "sen misin", "siz misiniz",
    "insanla mı görüşüyorum", "insanla mi gorusuyorum", "insanla mı gorusuyorum", "insanla mi görüşüyorum",
]
CLARIFICATION_KEYWORDS = [
    "nasıl yani", "nasil yani", "ne demek", "anlamadım", "anlamadim", "tam olarak ne", "yani ne", "nasıl oluyor", "nasil oluyor",
]
REQUEST_REASON_KEYWORDS = [
    "ne için gerekli", "ne icin gerekli", "neden gerekli", "niye gerekli", "ne için lazım", "ne icin lazim",
    "ne için istiyorsunuz", "ne icin istiyorsunuz", "neden istiyorsunuz", "niye istiyorsunuz", "bu ne için", "bu ne icin",
    "ne işe yarayacak", "ne ise yarayacak", "telefon neden gerekli", "telefon niye gerekli", "telefon ne için", "telefon ne icin",
    "neden istiyorsun", "niye istiyorsun", "ne icin istiyorsun",
    "neden telefon istiyorsun", "niye telefon istiyorsun", "telefonu neden istiyorsun", "telefonu niye istiyorsun",
    "numara neden gerekli", "numara niye gerekli", "numarayi neden istiyorsun", "numarayi niye istiyorsun",
]
PHONE_REFUSAL_KEYWORDS = [
    "hayır", "hayir", "istemiyorum", "paylaşmak istemiyorum", "paylasmak istemiyorum", "vermek istemiyorum",
    "telefon vermek istemiyorum", "numara vermek istemiyorum", "telefon paylaşmak istemiyorum", "telefonu paylaşmak istemiyorum",
    "telefon vermeden", "numara vermeden",
    "gerek yok", "şimdilik vermeyeyim", "simdilik vermeyeyim", "buradan konuşalım", "burdan konusalim",
]
OFFER_HESITATION_KEYWORDS = [
    "bilmiyorum", "bilmiyom", "emin degilim", "kararsizim", "su an emin degilim",
    "bir dusuneyim", "bakariz", "simdilik bilmiyorum",
    "sonra belki", "belki sonra", "belki ileride", "ileride belki",
    "dusunecegim", "dusuneyim", "dusunmek istiyorum", "dusunmem lazim",
    "su an belli degil", "belli degil", "belli degilim",
    "acelem yok", "acelemiz yok", "vakit var", "henuzerken", "erken daha",
    "sonra konusalim", "sonra gorusuruz", "sonra yazarim", "sonra yazarim",
]
BOOKING_RESET_KEYWORDS = [
    "ne randevusu", "hangi randevu", "ben randevu istemedim", "randevu istemedim", "ne görüşmesi", "hangi görüşme",
    "ben bilgi almak istiyorum", "sadece bilgi almak istiyorum", "yanlış anladınız", "yanlis anladiniz",
]
BOOKING_OWNERSHIP_REJECTION_KEYWORDS = [
    "ben almadım", "ben almadim", "ben istemedim", "ben yapmadım", "ben yapmadim", "bana ait değil", "bana ait degil",
    "bu ben değilim", "bu ben degilim", "o ben değilim", "o ben degilim", "yanlış kişi", "yanlis kisi",
    "yanlış numara", "yanlis numara", "haberim yok", "ben randevu almadım", "ben randevu almadim",
]
SERVICE_OVERVIEW_KEYWORDS = [
    "hangi hizmet", "hangi hizmetler", "hangi hizmetleriniz var", "hangi hizmetler var",
    "ne hizmet", "ne hizmeti", "ne hizmetler",
    "hizmet veriyorsunuz", "hizmet veriyor",
    "hizmetleriniz", "hizmetleriniz neler", "hizmetler neler",
    "neler yapıyorsunuz", "neler yapiyorsunuz",
    "ne yapıyorsunuz", "ne yapiyorsunuz",
    "ne iş yapıyorsunuz", "ne is yapiyorsunuz",
    "nasıl yardımcı olabilirsiniz", "nasil yardimci olabilirsiniz",
    "hangi alanlarda yardımcı oluyorsunuz", "hangi alanlarda yardimci oluyorsunuz",
    "neler sunuyorsunuz", "ne sunuyorsunuz",
    "paketler", "neler var",
    "ne işle uğraşıyorsunuz", "ne isle ugrasiyorsunuz",
    "faaliyet alanı", "faaliyet alani",
    "siz ne yapıyorsunuz", "siz ne yapiyorsunuz",
]
WORKING_SCHEDULE_KEYWORDS = [
    "hangi günler çalış", "hangi gunler calis", "hangi gün çalış", "hangi gun calis",
    "çalışma gün", "calisma gun", "çalışıyor musunuz", "calisiyor musunuz",
    "hangi saatler", "kaçta açıksınız", "kacta aciksiniz", "hafta içi", "hafta ici",
    "cumartesi açık", "cumartesi acik", "pazar açık", "pazar acik"
]
PRICE_KEYWORDS = ["fiyat", "ücret", "ucret", "kaç tl", "kac tl", "fiyatı", "fiyati", "bedel", "ne kadar", "kaç para", "kac para", "bütçe", "butce", "paket fiyat", "aylık", "aylik", "tek seferlik", "tek sefer", "aylık mı", "aylik mi"]
PRICE_FOLLOWUP_KEYWORDS = ["aylık", "aylik", "aylık mı", "aylik mi", "tek sefer", "tek seferlik", "bu aylık", "bu aylik", "aylık peki", "aylik peki"]
PRICE_NEGOTIATION_KEYWORDS = ["olur mu", "yapar mısınız", "yapar misiniz", "indirim", "son fiyat", "net fiyat", "bir şey olur mu", "bir sey olur mu", "çıkar mı", "cikar mi", "iner mi"]
BUDGET_LIMIT_KEYWORDS = ["param yetmiyor", "bütçem yetmiyor", "butcem yetmiyor", "bütçem", "butcem", "yetmiyor", "çıkamam", "cikamam", "zorlar", "karşılayamam", "karsilayamam", "en fazla"]
PURCHASE_IF_DISCOUNTED_KEYWORDS = ["olursa alırım", "olursa alirim", "olursa alıcam", "olursa alicam", "o fiyata alırım", "o fiyata alirim", "alırım", "alirim", "alıcam", "alicam", "başlarız", "baslariz"]
DETAIL_KEYWORDS = ["bilgi", "detay", "içerik", "icerik", "kapsam", "nasıl", "nasil", "nedir", "ne var"]
FAQ_RESPONSES = {
    "process": "Süreç genelde kısa ihtiyaç analiziyle başlar. Ardından uygun kapsam netleşir, onay sonrası uygulama planı çıkarılır ve teslim takvimi paylaşılır.",
    "budget": "Net bütçe, seçilen hizmetin kapsamına göre değişir. En doğru rakamı ihtiyaçlarınızı netleştirince paylaşabiliyoruz.",
    "reporting": "Çalışma modeline göre düzenli performans özeti ve durum takibi paylaşılır. Hangi hizmetle ilgilendiğinizi söylerseniz raporlama yapısını net anlatayım.",
    "sector": "Farklı sektörlerde çalışabiliyoruz. En sağlıklı yönlendirme için markanızı ve ihtiyacınızı kısaca yazmanız yeterli.",
}
FAQ_KEYWORDS = {
    "process": ["süreç", "nasıl işliyor", "nasıl çalışıyor", "nasıl ilerliyor"],
    "budget": ["minimum bütçe", "min bütçe", "alt limit", "minimum butce", "bütçe"],
    "reporting": ["raporlama", "rapor", "haftalık rapor", "aylık rapor"],
    "sector": ["hangi sektör", "hangi marka", "e-ticaret", "e ticaret", "kimlerle çalışıyorsunuz", "kimlerle calisiyorsunuz"],
}
OBJECTION_KEYWORDS = {
    "hesitation": [
        "düşüneyim", "dusuneyim", "sonra yazayım", "sonra yazarım", "şimdilik", "simdilik",
        "kararsızım", "kararsizim", "emin değilim", "emin degilim", "istemiyorum", "istemiyom",
        "istemem", "istemem ya", "ilgilenmiyorum", "ilgilenmiyom", "gerek yok", "yok gerek",
        "şu an istemiyorum", "simdilik istemiyorum", "şimdilik kalsın", "simdilik kalsin", "kalsın", "kalsin",
        "boşver", "bosver", "gerek duymuyorum", "istemiyorum ya"
    ],
    "price": [
        "pahalı", "pahali", "bütçemi aşıyor", "butcemi asiyor", "yüksek geldi", "yuksek geldi", "çok pahalı", "cok pahali",
        "çok fazla", "cok fazla", "fazla geldi", "yüksek", "yuksek", "indirim", "indirim yap", "indirim yapın", "indirim yapar mısınız",
        "uygun olur mu", "biraz düş", "biraz dus", "pazarlık", "pazarlik", "fiyat çokmuş", "fiyat cokmus", "çokmuş", "cokmus",
        "param yetmiyor", "bütçem yetmiyor", "butcem yetmiyor", "4 bin olur mu", "4000 olur mu", "olursa alırım", "olursa alirim", "olursa alıcam", "olursa alicam"
    ],
}
SERVICE_ADVICE_KEYWORDS = [
    "bana ne uygun", "hangisi uygun", "hangisi daha iyi", "hangisi mantıklı", "hangisi mantikli",
    "ne öner", "ne oner", "önerir misiniz", "onerir misiniz", "önerin", "onerin", "sizce", "beni yönlendir",
    "beni yonlendir", "hangi çözüm", "hangi cozum", "ne tavsiye", "yardımcı olur musunuz",
]
COMPARISON_KEYWORDS = [
    "mı yoksa", "mi yoksa", "ya da", "veya", "hangisi daha iyi", "hangisi mantıklı",
    "hangisi mantikli", "arasında", "arasinda", "farkı ne", "farki ne",
]
BOOKING_INTENT_KEYWORDS = [
    "randevu", "görüşme", "gorusme", "toplantı", "toplanti", "saat", "müsait", "musait",
    "uygun musunuz", "uygun mu", "başlayalım", "baslayalim", "planlayalım", "planlayalim", "görüşelim", "goruselim", "konuşalım", "konusalim",
]
PRECONSULTATION_INTENT_KEYWORDS = [
    "bilgi almak istiyorum", "bilgi alabilir miyim", "fiyat alabilir miyim", "bakalım", "düşünüyorum", "dusunuyorum",
    "kararsızım", "kararsizim", "emin değilim", "emin degilim", "bana ne uygun", "hangisi daha uygun",
    "bir konuşalım", "bir goruselim", "ön görüşme", "on gorusme", "detay alayım", "fikir almak istiyorum", "toplantı", "toplanti",
]
DIRECT_APPOINTMENT_KEYWORDS = [
    "ne zaman başlayabiliriz", "ne zaman baslayabiliriz", "başlamak istiyorum", "baslamak istiyorum", "ilerlemek istiyorum",
    "devam edelim", "başlayalım", "baslayalim", "oluşturalım", "olusturalim", "oluştur", "olustur", "oluşturun", "olusturun",
    "randevu oluşturalım", "randevu olusturalim", "randevuyu oluştur", "randevuyu olustur", "randevuyu oluşturun", "randevuyu olusturun",
    "ön görüşmeyi oluştur", "on gorusmeyi olustur", "ön görüşmeyi oluşturun", "on gorusmeyi olusturun",
    "kaydı aç", "kaydi ac", "kaydı açın", "kaydi acin", "kaydını aç", "kaydini ac", "kaydını açın", "kaydini acin",
    "ön görüşme oluştur", "on gorusme olustur", "ön görüşme oluşturun", "on gorusme olusturun", "toplantı yapalım", "toplanti yapalim",
    "fiyat tamam", "uygunsa başlayalım", "teknik detayları konuşalım", "teknik detaylari konusalim", "işe başlayalım", "ise baslayalim",
]
MORNING_PERIOD_KEYWORDS = ["sabah", "sabah olur", "erken", "erken saat", "öğleden önce", "ogleden once"]
AFTERNOON_PERIOD_KEYWORDS = ["öğleden sonra", "ogleden sonra", "öğleden", "ogleden", "ikindi", "öğleden sonrasında", "ogleden sonrasinda", "akşamüstü", "aksamustu"]
SERVICE_REASON_MAP = {
    "web-tasarim": "kurumsal görünüm, güven veren yapı ve dönüşüm odaklı bir site ihtiyacını karşılar",
    "otomasyon-ai": "gelen DM'leri, randevu akışını ve tekrar eden süreçleri otomatikleştirmek için en doğru çözümdür",
    "performans-pazarlama": "Instagram ve Meta reklamları üzerinden yeni talep ve müşteri kazanımını hızlandırır",
    "sosyal-medya-yonetimi": "içerik planı, paylaşım disiplini ve hesap yönetimini düzenli şekilde yürütmek için uygundur",
    "marka-stratejisi": "marka konumlandırması ve büyüme yönünü netleştirmek gerektiğinde doğru başlangıç olur",
    "kreatif-produksiyon": "görsel kaliteyi yükselten video ve kreatif içerik üretimi için uygundur",
}
SERVICE_FOCUS_MAP = {
    "web-tasarim": "kurumsal ve dönüşüm odaklı bir web sitesi kurmak",
    "otomasyon-ai": "DM, randevu ve tekrar eden süreçleri otomatikleştirmek",
    "performans-pazarlama": "reklam üzerinden yeni talep ve müşteri kazanmak",
    "sosyal-medya-yonetimi": "içerik ve hesap yönetimini düzenli yürütmek",
    "marka-stratejisi": "marka konumlandırmasını ve büyüme yönünü netleştirmek",
    "kreatif-produksiyon": "güçlü video ve kreatif içerik üretmek",
}
SERVICE_CLARIFYING_QUESTIONS = {
    "web-tasarim": "Projeniz kurumsal site mi, satış odaklı bir landing page mi?",
    "otomasyon-ai": "Günlük mesaj yoğunluğunuz yaklaşık kaç?",
    "performans-pazarlama": "Önceliğiniz yeni müşteri kazanımı mı, dönüşüm maliyetini düşürmek mi?",
    "sosyal-medya-yonetimi": "İçerik üretimi mi, hesap yönetimi mi sizin için daha kritik?",
    "marka-stratejisi": "Marka tarafında en çok konumlandırma mı, satış dili mi netleşsin?",
    "kreatif-produksiyon": "Video mu, reklam kreatifi mi, sosyal medya görselleri mi öncelikli?",
}
LIVE_CRM_AUTH_CACHE_SECONDS = int(os.getenv("LIVE_CRM_AUTH_CACHE_SECONDS", "3000"))
LIVE_CRM_SERVICES_CACHE_SECONDS = int(os.getenv("LIVE_CRM_SERVICES_CACHE_SECONDS", "21600"))
LIVE_CRM_SLOT_CACHE_SECONDS = int(os.getenv("LIVE_CRM_SLOT_CACHE_SECONDS", "15"))
STALE_CONVERSATION_MINUTES = int(os.getenv("STALE_CONVERSATION_MINUTES", "30"))
HISTORY_MESSAGE_LIMIT = int(os.getenv("HISTORY_MESSAGE_LIMIT", "12"))
AVAILABILITY_KEYWORDS = ["boş", "bos", "müsait", "musait", "uygun", "hangi saat", "kaçta", "kacta", "saatler", "boşluk", "bosluk"]
CRM_AUTO_NOTE_PREFIXES = (
    "Kaynak:",
    "Instagram Hesabı:",
    "Instagram Business User ID:",
    "AI Modeli:",
    "Instagram User ID:",
    "Akış durumu:",
    "Ön görüşme:",
    "Son mesaj:",
)


class IncomingMessage(BaseModel):
    sender_id: str = Field(..., min_length=1)
    message_text: str | None = ""
    recipient_id: str | None = None
    instagram_username: str | None = None
    raw_event: dict[str, Any] | None = None
    trace_id: str | None = None


class ProcessResult(BaseModel):
    sender_id: str
    should_reply: bool = True
    reply_text: str | None = None
    handoff: bool = False
    conversation_state: str
    appointment_created: bool = False
    appointment_id: int | None = None
    normalized: dict[str, Any] = Field(default_factory=dict)
    metrics: dict[str, Any] = Field(default_factory=dict)
    decision_path: list[str] = Field(default_factory=list)


class ReminderClaim(BaseModel):
    reminder_id: int
    appointment_id: int
    instagram_user_id: str
    full_name: str
    booking_kind: str
    service: str
    appointment_date: str
    appointment_time: str
    reminder_text: str
    claim_token: str


class ReminderClaimResponse(BaseModel):
    reminders: list[ReminderClaim] = Field(default_factory=list)


class ReminderMarkRequest(BaseModel):
    reminder_id: int
    claim_token: str = Field(..., min_length=1)
    sent: bool = True
    error: str | None = None


class VoiceFallbackEvent(BaseModel):
    sender_id: str = Field(..., min_length=1)
    instagram_username: str | None = None
    fallback_reply: str = Field(..., min_length=1)
    transcription_error: str | None = None
    raw_event: dict[str, Any] | None = None


class VoiceFallbackResponse(BaseModel):
    ok: bool = True
    should_reply: bool = True
    reply_text: str | None = None
    duplicate: bool = False
    normalized: dict[str, Any] = Field(default_factory=dict)


class CustomerCard(BaseModel):
    id: int
    instagram_user_id: str
    instagram_username: str | None = None
    full_name: str | None = None
    phone: str | None = None
    sector: str | None = None
    segment: str | None = None
    notes: str | None = None
    discount_code: str | None = None
    custom_offer: str | None = None
    subscription_renewal_date: str | None = None
    consent_status: str | None = None
    consent_updated_at: str | None = None
    voice_note_url: str | None = None
    customer_type: str | None = None
    approval_status: str | None = None
    approval_reason: str | None = None
    rejection_reason: str | None = None
    last_service: str | None = None
    total_visits: int = 0
    total_spend: float = 0
    no_show_count: int = 0
    last_contact_at: str | None = None
    last_visit_at: str | None = None
    next_automation_at: str | None = None
    next_automation_type: str | None = None


class CustomerListResponse(BaseModel):
    customers: list[CustomerCard] = Field(default_factory=list)


class CustomerDetailResponse(BaseModel):
    customer: dict[str, Any]
    history: list[dict[str, Any]] = Field(default_factory=list)
    upcoming_automations: list[dict[str, Any]] = Field(default_factory=list)


class CustomerTimelineResponse(BaseModel):
    customer: dict[str, Any]
    timeline: list[dict[str, Any]] = Field(default_factory=list)


class CustomerNoteUpdateRequest(BaseModel):
    notes: str | None = None
    preferences: dict[str, Any] | None = None
    discount_code: str | None = None
    custom_offer: str | None = None
    subscription_renewal_date: str | None = None
    consent_status: str | None = None
    voice_note_url: str | None = None
    customer_type: str | None = None
    approval_status: str | None = None
    approval_reason: str | None = None
    rejection_reason: str | None = None


class AttendanceUpdateRequest(BaseModel):
    attendance_status: str = Field(..., min_length=1)
    note: str | None = None


class AppointmentUpdateRequest(BaseModel):
    status: str | None = None
    note: str | None = None
    approval_status: str | None = None
    approval_reason: str | None = None
    rejection_reason: str | None = None
    cancellation_reason: str | None = None
    refund_status: str | None = None
    refund_amount: float | None = None
    refund_reason: str | None = None


class ServiceCapacityUpdateRequest(BaseModel):
    capacity: int = Field(..., ge=1, le=20)
    service_name: str | None = None


class CustomerWorkItemCreateRequest(BaseModel):
    instagram_user_id: str | None = None
    customer_id: int | None = None
    kind: str = Field(..., min_length=1)
    status: str | None = "open"
    reason: str | None = None
    note: str | None = None
    due_at: str | None = None
    assigned_to: str | None = None
    payload: dict[str, Any] | None = None


class CustomerWorkItemUpdateRequest(BaseModel):
    status: str | None = None
    reason: str | None = None
    note: str | None = None
    due_at: str | None = None
    resolved_at: str | None = None
    assigned_to: str | None = None
    payload: dict[str, Any] | None = None


class AutomationClaimItem(BaseModel):
    event_id: int
    customer_id: int
    instagram_user_id: str
    template_slug: str
    event_type: str
    scheduled_at: str
    message_text: str


class AutomationClaimResponse(BaseModel):
    events: list[AutomationClaimItem] = Field(default_factory=list)


class AutomationMarkRequest(BaseModel):
    event_id: int
    sent: bool = True
    error: str | None = None
    retry_at: str | None = None


class CampaignCreateRequest(BaseModel):
    title: str = Field(default="Özel Toplu Mesaj")
    template_slug: str | None = None
    custom_message: str | None = None
    segment: str | None = None
    sector: str | None = None
    inactivity_days: int | None = None
    attendance_status: str | None = None


@app.on_event("startup")
def on_startup() -> None:
    wait_for_database()
    run_migrations()
    if is_live_crm_configured():
        try:
            headers, user_id = live_crm_auth_session()
            if headers and user_id:
                logger.info("live_crm_auth_warmed user_id=%s", user_id)
        except Exception:  # noqa: BLE001
            logger.exception("live_crm_auth_warm_failed")


@app.get("/health")
def health() -> dict[str, Any]:
    return {"status": "ok", "time": datetime.now(TZ).isoformat()}


@app.get("/version")
def version() -> dict[str, Any]:
    llm_key_hash = hashlib.sha256(LLM_API_KEY.encode("utf-8")).hexdigest()[:12] if LLM_API_KEY else None
    return {
        "version": APP_BUILD_VERSION,
        "time": datetime.now(TZ).isoformat(),
        "full_ai_conversational_mode": FULL_AI_CONVERSATIONAL_MODE,
        "llm_reply_polish_enabled": LLM_REPLY_POLISH_ENABLED,
        "llm_configured": bool(LLM_BASE_URL and LLM_API_KEY),
        "llm_base_url_configured": bool(LLM_BASE_URL),
        "llm_api_key_configured": bool(LLM_API_KEY),
        "llm_api_key_length": len(LLM_API_KEY),
        "llm_api_key_hash": llm_key_hash,
        "llm_model": LLM_MODEL,
        "llm_reply_advisory_model": LLM_REPLY_ADVISORY_MODEL,
        "llm_reply_quality_model": LLM_REPLY_QUALITY_MODEL,
        "reply_engine": REPLY_ENGINE,
        "ai_first_enabled": AI_FIRST_ENABLED,
        "reply_guarantee_enabled": REPLY_GUARANTEE_ENABLED,
    }


@app.get("/api/llm-health")
def llm_health() -> dict[str, Any]:
    llm_key_hash = hashlib.sha256(LLM_API_KEY.encode("utf-8")).hexdigest()[:12] if LLM_API_KEY else None
    if not LLM_BASE_URL or not LLM_API_KEY:
        return {
            "ok": False,
            "configured": False,
            "base_url_configured": bool(LLM_BASE_URL),
            "api_key_configured": bool(LLM_API_KEY),
            "api_key_length": len(LLM_API_KEY),
            "api_key_hash": llm_key_hash,
            "model": LLM_MODEL,
        }
    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }
    try:
        response = requests.post(
            f"{LLM_BASE_URL}/chat/completions",
            headers=headers,
            json={
                "model": LLM_REPLY_MICRO_MODEL or LLM_MODEL,
                "messages": [
                    {"role": "system", "content": "Return exactly OK."},
                    {"role": "user", "content": "health"},
                ],
                "temperature": 0,
                "max_tokens": 8,
            },
            timeout=10,
        )
        preview = sanitize_text(response.text)[:240]
        return {
            "ok": response.status_code < 400,
            "configured": True,
            "status_code": response.status_code,
            "model": LLM_REPLY_MICRO_MODEL or LLM_MODEL,
            "base_url": LLM_BASE_URL,
            "api_key_length": len(LLM_API_KEY),
            "api_key_hash": llm_key_hash,
            "body_preview": preview,
        }
    except Exception as exc:  # noqa: BLE001
        return {
            "ok": False,
            "configured": True,
            "status_code": None,
            "model": LLM_REPLY_MICRO_MODEL or LLM_MODEL,
            "base_url": LLM_BASE_URL,
            "api_key_length": len(LLM_API_KEY),
            "api_key_hash": llm_key_hash,
            "error": sanitize_text(str(exc))[:240],
        }


@app.get("/crm", response_class=HTMLResponse)
def crm_panel() -> HTMLResponse:
    panel_path = os.path.join(os.path.dirname(__file__), "crm_panel.html")
    with open(panel_path, "r", encoding="utf-8") as handle:
        return HTMLResponse(handle.read())


def is_within_morning_reminder_window(now: datetime | None = None) -> bool:
    if not MORNING_REMINDER_ENABLED:
        return False
    current = now or datetime.now(TZ)
    current_time = current.timetz().replace(tzinfo=None)
    return MORNING_REMINDER_START <= current_time <= MORNING_REMINDER_END


def build_morning_reminder_text(appointment: dict[str, Any]) -> str:
    booking_kind = "preconsultation" if str(appointment.get("status") or "") == "preconsultation" else "appointment"
    booking_label = "ön görüşmeniz" if booking_kind == "preconsultation" else "randevunuz"
    requested_date = date.fromisoformat(str(appointment.get("appointment_date"))).strftime("%d.%m.%Y")
    requested_time = str(appointment.get("appointment_time") or "")[:5]
    service = sanitize_text(str(appointment.get("service") or ""))
    service_line = f" ({service})" if service else ""
    return (
        f"Günaydın, bugün saat {requested_time}'teki {booking_label} için küçük bir hatırlatma bırakmak istedim{service_line}. "
        f"Lütfen {requested_date} {requested_time} için müsaitliğinizi ayarlayın. "
        "Bir degisiklik ihtiyaciniz olursa buradan yazabilirsiniz."
    )


def claim_due_morning_reminders(conn: psycopg.Connection, limit: int = 10) -> list[dict[str, Any]]:
    if not is_within_morning_reminder_window():
        return []

    today_value = datetime.now(TZ).date().isoformat()
    claim_token = str(uuid4())
    claim_started_at = datetime.now(TZ)
    stale_before = claim_started_at - timedelta(minutes=MORNING_REMINDER_CLAIM_TIMEOUT_MINUTES)

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO appointment_reminders (appointment_id, reminder_kind, instagram_user_id)
            SELECT a.id, 'morning_of', a.instagram_user_id
            FROM appointments a
            WHERE a.status IN ('confirmed', 'preconsultation')
              AND a.appointment_date = %s
              AND COALESCE(a.instagram_user_id, '') <> ''
            ON CONFLICT (appointment_id, reminder_kind) DO NOTHING
            """,
            (today_value,),
        )
        cur.execute(
            """
            WITH picked AS (
                SELECT r.id
                FROM appointment_reminders r
                JOIN appointments a ON a.id = r.appointment_id
                WHERE r.reminder_kind = 'morning_of'
                  AND r.sent_at IS NULL
                  AND a.status IN ('confirmed', 'preconsultation')
                  AND a.appointment_date = %s
                  AND COALESCE(a.instagram_user_id, '') <> ''
                  AND (r.claim_token IS NULL OR r.claimed_at IS NULL OR r.claimed_at < %s)
                ORDER BY a.appointment_time ASC, r.id ASC
                LIMIT %s
                FOR UPDATE SKIP LOCKED
            )
            UPDATE appointment_reminders AS r
            SET claim_token = %s,
                claimed_at = %s,
                updated_at = NOW(),
                last_error = NULL
            FROM picked
            WHERE r.id = picked.id
            RETURNING r.id, r.appointment_id, r.instagram_user_id, r.claim_token
            """,
            (today_value, stale_before, limit, claim_token, claim_started_at),
        )
        claimed_rows = cur.fetchall()
        reminder_ids = [row["id"] for row in claimed_rows]
        if not reminder_ids:
            conn.commit()
            return []
        cur.execute(
            """
            SELECT r.id AS reminder_id,
                   r.appointment_id,
                   r.instagram_user_id,
                   r.claim_token,
                   a.full_name,
                   a.service,
                   a.appointment_date,
                   a.appointment_time,
                   a.status
            FROM appointment_reminders r
            JOIN appointments a ON a.id = r.appointment_id
            WHERE r.id = ANY(%s)
            ORDER BY a.appointment_time ASC, r.id ASC
            """,
            (reminder_ids,),
        )
        rows = [serialize_row(row) for row in cur.fetchall()]
    conn.commit()
    for row in rows:
        row["booking_kind"] = "preconsultation" if str(row.get("status") or "") == "preconsultation" else "appointment"
        row["reminder_text"] = build_morning_reminder_text(row)
    return rows


def mark_morning_reminder(conn: psycopg.Connection, reminder_id: int, claim_token: str, sent: bool, error: str | None = None) -> bool:
    with conn.cursor() as cur:
        if sent:
            cur.execute(
                """
                UPDATE appointment_reminders
                SET sent_at = NOW(),
                    updated_at = NOW(),
                    last_error = NULL
                WHERE id = %s AND claim_token = %s
                """,
                (reminder_id, claim_token),
            )
        else:
            cur.execute(
                """
                UPDATE appointment_reminders
                SET claim_token = NULL,
                    claimed_at = NULL,
                    updated_at = NOW(),
                    last_error = %s
                WHERE id = %s AND claim_token = %s
                """,
                (sanitize_text(error or "send_failed")[:500], reminder_id, claim_token),
            )
        updated = cur.rowcount > 0
    conn.commit()
    return updated


@app.get("/internal/reminders/morning/claim", response_model=ReminderClaimResponse)
def claim_morning_reminders(limit: int = 10) -> ReminderClaimResponse:
    with get_conn() as conn:
        rows = claim_due_morning_reminders(conn, limit=max(1, min(limit, 25)))
    reminders = [ReminderClaim(**row) for row in rows]
    return ReminderClaimResponse(reminders=reminders)


@app.post("/internal/reminders/morning/mark")
def mark_morning_reminder_endpoint(payload: ReminderMarkRequest) -> dict[str, Any]:
    with get_conn() as conn:
        ok = mark_morning_reminder(conn, payload.reminder_id, payload.claim_token, payload.sent, payload.error)
    return {"ok": ok}


@app.post("/internal/messages/voice-fallback", response_model=VoiceFallbackResponse)
def record_voice_fallback(payload: VoiceFallbackEvent, background_tasks: BackgroundTasks) -> VoiceFallbackResponse:
    fallback_reply = sanitize_text(payload.fallback_reply or "")
    raw_event_for_log = dict(payload.raw_event or {})
    inbound_message_id = extract_inbound_message_id(raw_event_for_log)
    if inbound_message_id and not raw_event_for_log.get("message_id"):
        raw_event_for_log["message_id"] = inbound_message_id
    raw_event_for_log["type"] = "voice_fallback_inbound"
    if payload.transcription_error:
        raw_event_for_log["transcription_error"] = sanitize_text(payload.transcription_error)[:500]

    with get_conn() as conn:
        conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
        if payload.instagram_username:
            conversation["instagram_username"] = payload.instagram_username
        reconcile_confirmed_conversation(conn, conversation)
        sanitize_conversation_state(conversation)
        ensure_conversation_memory(conversation)
        sync_conversation_memory_summary(conversation)

        duplicate_inbound = has_processed_inbound_message(conn, payload.sender_id, inbound_message_id)
        duplicate_outbound = has_outbound_after_inbound(conn, payload.sender_id, inbound_message_id)
        if duplicate_inbound and duplicate_outbound:
            return VoiceFallbackResponse(
                ok=True,
                should_reply=False,
                reply_text=None,
                duplicate=True,
                normalized=build_normalized(conversation),
            )

        if not duplicate_inbound:
            save_message_log(conn, payload.sender_id, "in", None, raw_event_for_log)

        if fallback_reply and not duplicate_outbound:
            save_message_log(
                conn,
                payload.sender_id,
                "out",
                fallback_reply,
                {
                    "type": "voice_fallback",
                    "inbound_message_id": inbound_message_id,
                    "decision_path": ["voice_fallback"],
                },
            )

        conversation["last_customer_message"] = "[voice message transcription failed]"
        conversation["llm_notes"] = sanitize_text(payload.transcription_error or "voice_transcription_failed")[:500]
        upsert_conversation(conn, conversation)
        queue_crm_sync(background_tasks, conversation, None, {"extract_ms": 0, "polish_ms": 0, "crm_ms": 0, "total_ms": 0})
        return VoiceFallbackResponse(
            ok=True,
            should_reply=bool(fallback_reply) and not duplicate_outbound,
            reply_text=fallback_reply or None,
            duplicate=duplicate_inbound,
            normalized=build_normalized(conversation),
        )


@app.get("/api/appointments")
def list_appointments(
    limit: int = 50,
    date_from: str | None = None,
    date_to: str | None = None,
    status: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    conditions = ["TRUE"]
    params: list[Any] = []
    if date_from:
        conditions.append("appointment_date >= %s::date")
        params.append(date_from)
    else:
        conditions.append("appointment_date >= CURRENT_DATE")
    if date_to:
        conditions.append("appointment_date <= %s::date")
        params.append(date_to)
    if status:
        conditions.append("status = %s")
        params.append(sanitize_text(status).lower())
    if kind == "preconsultation":
        conditions.append("status = 'preconsultation'")
    elif kind == "appointment":
        conditions.append("status <> 'preconsultation'")
    where_sql = " AND ".join(conditions)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, instagram_user_id, instagram_username, full_name, phone, service,
                       appointment_date, appointment_time, status, source, notes,
                       attendance_status, attendance_marked_at, approval_status, approval_reason,
                       rejection_reason, cancellation_reason, refund_status, refund_amount,
                       refund_reason, capacity_units, created_at, updated_at
                FROM appointments
                WHERE {where_sql}
                ORDER BY appointment_date ASC, appointment_time ASC
                LIMIT %s
                """,
                (*params, max(1, min(limit, 300))),
            )
            rows = cur.fetchall()
    return {"appointments": filter_business_records([serialize_row(row) for row in rows])}


@app.get("/api/appointments/calendar")
def get_appointment_calendar(date: str) -> dict[str, Any]:
    normalized_date = normalize_date_string(date)
    if not normalized_date:
        raise HTTPException(status_code=400, detail="Invalid date")
    with get_conn() as conn:
        return build_calendar_slots(conn, normalized_date)


@app.get("/api/conversations")
def list_conversations(limit: int = 50) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, instagram_user_id, full_name, phone, service, requested_date, requested_time, appointment_status, state, updated_at
                FROM conversations
                ORDER BY updated_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
    return {"conversations": [serialize_row(row) for row in rows]}


@app.get("/api/customers", response_model=CustomerListResponse)
def list_customers(
    limit: int = 50,
    segment: str | None = None,
    sector: str | None = None,
    attendance_status: str | None = None,
    search: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
) -> CustomerListResponse:
    where_clause, params = build_customer_filter_clause(
        segment=segment,
        sector=sector,
        attendance_status=attendance_status,
        search=search,
        created_from=created_from,
        created_to=created_to,
    )
    query = f"""
        SELECT c.id, c.instagram_user_id, c.instagram_username, c.full_name, c.phone, c.sector, c.segment, c.notes,
               c.discount_code, c.custom_offer, c.subscription_renewal_date, c.consent_status, c.consent_updated_at,
               c.voice_note_url, c.customer_type, c.approval_status, c.approval_reason, c.rejection_reason,
               c.last_service, c.total_visits, c.total_spend, c.no_show_count, c.last_contact_at, c.last_visit_at,
               c.next_automation_at, c.next_automation_type
        FROM customers c
        {where_clause}
        ORDER BY c.updated_at DESC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (*params, max(1, min(limit, 200))))
            rows = [CustomerCard(**row) for row in filter_business_records([serialize_row(row) for row in cur.fetchall()])]
    return CustomerListResponse(customers=rows)


@app.get("/api/customers/ops/{list_slug}", response_model=CustomerListResponse)
def list_customer_operations(list_slug: str, limit: int = 50) -> CustomerListResponse:
    normalized = sanitize_text(list_slug).lower()
    mapping = {
        "no-show": {"segment": "no_show_customer"},
        "no_show": {"segment": "no_show_customer"},
        "inactive": {"segment": "inactive_customer"},
        "loyal": {"segment": "loyal_customer"},
        "high-value": {"segment": "high_value_customer"},
        "high_value": {"segment": "high_value_customer"},
    }
    if normalized == "upcoming-automations" or normalized == "upcoming_automations":
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT id, instagram_user_id, instagram_username, full_name, phone, sector, segment, notes,
                           discount_code, custom_offer, subscription_renewal_date, consent_status, consent_updated_at,
                           voice_note_url, customer_type, approval_status, approval_reason, rejection_reason,
                           last_service, total_visits, total_spend, no_show_count, last_contact_at, last_visit_at,
                           next_automation_at, next_automation_type
                    FROM customers
                    WHERE next_automation_at IS NOT NULL
                    ORDER BY next_automation_at ASC, updated_at DESC
                    LIMIT %s
                    """,
                    (max(1, min(limit, 200)),),
                )
                rows = [CustomerCard(**row) for row in filter_business_records([serialize_row(row) for row in cur.fetchall()])]
        return CustomerListResponse(customers=rows)
    if normalized not in mapping:
        raise HTTPException(status_code=400, detail="Unknown operations list")
    return list_customers(limit=limit, **mapping[normalized])


@app.get("/api/customers/{instagram_user_id}", response_model=CustomerDetailResponse)
def get_customer_detail(instagram_user_id: str) -> CustomerDetailResponse:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE instagram_user_id = %s", (instagram_user_id,))
            customer = cur.fetchone()
            if not customer:
                raise HTTPException(status_code=404, detail="Customer not found")
            cur.execute(
                """
                SELECT h.*, a.attendance_status
                FROM customer_service_history h
                LEFT JOIN appointments a ON a.id = h.appointment_id
                WHERE h.customer_id = %s
                ORDER BY h.created_at DESC
                LIMIT 50
                """,
                (customer["id"],),
            )
            history = [serialize_row(row) for row in cur.fetchall()]
            cur.execute(
                """
                SELECT id, template_slug, event_type, scheduled_at, sent_at, status, payload
                FROM automation_events
                WHERE customer_id = %s
                ORDER BY scheduled_at ASC, id ASC
                LIMIT 50
                """,
                (customer["id"],),
            )
            events = [serialize_row(row) for row in cur.fetchall()]
    return CustomerDetailResponse(customer=serialize_row(customer), history=history, upcoming_automations=events)


@app.get("/api/customers/{instagram_user_id}/timeline", response_model=CustomerTimelineResponse)
def get_customer_timeline(instagram_user_id: str, limit: int = 100) -> CustomerTimelineResponse:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers WHERE instagram_user_id = %s", (instagram_user_id,))
            customer = cur.fetchone()
            if not customer:
                raise HTTPException(status_code=404, detail="Customer not found")
            timeline: list[dict[str, Any]] = []
            cur.execute(
                """
                SELECT id, direction, message_text, created_at
                FROM message_logs
                WHERE instagram_user_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (instagram_user_id, max(1, min(limit, 200))),
            )
            for row in cur.fetchall():
                item = serialize_row(row)
                timeline.append({"type": "message", **item})
            cur.execute(
                """
                SELECT id, service, appointment_date, appointment_time, status, attendance_status, created_at, updated_at
                FROM appointments
                WHERE instagram_user_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (instagram_user_id, max(1, min(limit, 200))),
            )
            for row in cur.fetchall():
                item = serialize_row(row)
                timeline.append({"type": "appointment", **item})
            cur.execute(
                """
                SELECT id, service_name, service_category, visit_date, visit_time, spend_amount, notes, created_at
                FROM customer_service_history
                WHERE customer_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (customer["id"], max(1, min(limit, 200))),
            )
            for row in cur.fetchall():
                item = serialize_row(row)
                timeline.append({"type": "history", **item})
            cur.execute(
                """
                SELECT id, template_slug, event_type, scheduled_at, sent_at, status, created_at, updated_at
                FROM automation_events
                WHERE customer_id = %s
                ORDER BY scheduled_at DESC, id DESC
                LIMIT %s
                """,
                (customer["id"], max(1, min(limit, 200))),
            )
            for row in cur.fetchall():
                item = serialize_row(row)
                timeline.append({"type": "automation", **item})
    timeline.sort(key=lambda item: item.get("created_at") or item.get("scheduled_at") or "", reverse=True)
    return CustomerTimelineResponse(customer=serialize_row(customer), timeline=timeline[: max(1, min(limit, 200))])


@app.patch("/api/customers/{instagram_user_id}")
def update_customer_notes(instagram_user_id: str, payload: CustomerNoteUpdateRequest) -> dict[str, Any]:
    payload_fields = getattr(payload, "model_fields_set", getattr(payload, "__fields_set__", set()))
    updates: list[str] = []
    params: list[Any] = []

    def add_text_update(field_name: str, column_name: str) -> None:
        if field_name not in payload_fields:
            return
        value = sanitize_text(getattr(payload, field_name) or "") or None
        if value is None:
            return
        updates.append(f"{column_name} = %s")
        params.append(value)

    add_text_update("notes", "notes")
    add_text_update("discount_code", "discount_code")
    add_text_update("custom_offer", "custom_offer")
    add_text_update("customer_type", "customer_type")
    add_text_update("approval_status", "approval_status")
    add_text_update("approval_reason", "approval_reason")
    add_text_update("rejection_reason", "rejection_reason")

    if "preferences" in payload_fields and payload.preferences is not None:
        updates.append("preferences = preferences || %s::jsonb")
        params.append(json.dumps(payload.preferences))

    if "subscription_renewal_date" in payload_fields:
        renewal_date = normalize_date_string(payload.subscription_renewal_date)
        if renewal_date:
            updates.append("subscription_renewal_date = %s::date")
            params.append(renewal_date)

    if "consent_status" in payload_fields:
        consent_value = sanitize_text(payload.consent_status or "") or None
        if consent_value is not None:
            updates.append("consent_status = %s")
            params.append(consent_value)
            updates.append("consent_updated_at = NOW()")

    if "voice_note_url" in payload_fields:
        updates.append("voice_note_url = %s")
        params.append(validate_voice_note_url(payload.voice_note_url))

    with get_conn() as conn:
        with conn.cursor() as cur:
            if updates:
                updates.append("updated_at = NOW()")
                params.append(instagram_user_id)
                cur.execute(
                    f"""
                    UPDATE customers
                    SET {", ".join(updates)}
                    WHERE instagram_user_id = %s
                    RETURNING *
                    """,
                    tuple(params),
                )
            else:
                cur.execute("SELECT * FROM customers WHERE instagram_user_id = %s", (instagram_user_id,))
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Customer not found")
        conn.commit()
    return {"customer": serialize_row(row)}


@app.post("/api/appointments/{appointment_id}/attendance")
def mark_appointment_attendance(appointment_id: int, payload: AttendanceUpdateRequest) -> dict[str, Any]:
    status_value = sanitize_text(payload.attendance_status).lower()
    if status_value not in {"scheduled", "completed", "no_show", "canceled"}:
        raise HTTPException(status_code=400, detail="Invalid attendance status")
    note_value = sanitize_text(payload.note or "") or None
    customer_row = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            appended_note = f"\n{note_value}" if note_value else ""
            cur.execute(
                """
                UPDATE appointments
                SET attendance_status = %s,
                    attendance_marked_at = NOW(),
                    notes = COALESCE(notes, '') || %s,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (status_value, appended_note, appointment_id),
            )
            appointment = cur.fetchone()
            if not appointment:
                raise HTTPException(status_code=404, detail="Appointment not found")
            cur.execute("SELECT * FROM customers WHERE instagram_user_id = %s", (appointment["instagram_user_id"],))
            customer = cur.fetchone()
            if customer:
                if status_value == "no_show":
                    cur.execute(
                        "UPDATE customers SET no_show_count = no_show_count + 1, updated_at = NOW() WHERE id = %s RETURNING *",
                        (customer["id"],),
                    )
                    customer = cur.fetchone()
                if status_value == "completed":
                    cur.execute(
                        "UPDATE customers SET last_visit_at = NOW(), updated_at = NOW() WHERE id = %s RETURNING *",
                        (customer["id"],),
                    )
                    customer = cur.fetchone()
                if customer:
                    customer_data = serialize_row(customer)
                    segment = infer_customer_segment(customer_data)
                    cur.execute(
                        "UPDATE customers SET segment = %s, updated_at = NOW() WHERE id = %s RETURNING *",
                        (segment, customer["id"]),
                    )
                    customer_row = cur.fetchone()
        conn.commit()
        if customer_row and status_value == "no_show":
            schedule_customer_automation_events(conn, int(customer_row["id"]), serialize_row(customer_row).get("sector"), no_show=True)
    return {"appointment": serialize_row(appointment), "customer": serialize_row(customer_row) if customer_row else None}


@app.patch("/api/appointments/{appointment_id}")
def update_appointment(appointment_id: int, payload: AppointmentUpdateRequest) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            note_value = sanitize_text(payload.note or "") or None
            appended_note = f"\n{note_value}" if note_value else ""
            cur.execute(
                """
                UPDATE appointments
                SET status = COALESCE(%s, status),
                    notes = COALESCE(notes, '') || %s,
                    approval_status = COALESCE(%s, approval_status),
                    approval_reason = COALESCE(%s, approval_reason),
                    rejection_reason = COALESCE(%s, rejection_reason),
                    cancellation_reason = COALESCE(%s, cancellation_reason),
                    refund_status = COALESCE(%s, refund_status),
                    refund_amount = COALESCE(%s::numeric, refund_amount),
                    refund_reason = COALESCE(%s, refund_reason),
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (
                    sanitize_text(payload.status or "").lower() or None,
                    appended_note,
                    sanitize_text(payload.approval_status or "") or None,
                    sanitize_text(payload.approval_reason or "") or None,
                    sanitize_text(payload.rejection_reason or "") or None,
                    sanitize_text(payload.cancellation_reason or "") or None,
                    sanitize_text(payload.refund_status or "") or None,
                    payload.refund_amount,
                    sanitize_text(payload.refund_reason or "") or None,
                    appointment_id,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Appointment not found")
        conn.commit()
    return {"appointment": serialize_row(row)}


@app.get("/api/service-capacity")
def list_service_capacity() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT service_slug, service_name, capacity, active, updated_at
                FROM service_capacity_rules
                ORDER BY service_name ASC
                """
            )
            rows = cur.fetchall()
    rules = [serialize_row(row) for row in rows]
    return {"rules": rules, "service_capacity": rules}


@app.patch("/api/service-capacity/{service_slug}")
def update_service_capacity(service_slug: str, payload: ServiceCapacityUpdateRequest) -> dict[str, Any]:
    cleaned_slug = sanitize_service_slug(service_slug)
    service_name = sanitize_text(payload.service_name or service_slug) or cleaned_slug
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO service_capacity_rules (service_slug, service_name, capacity, active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (service_slug) DO UPDATE SET
                    service_name = EXCLUDED.service_name,
                    capacity = EXCLUDED.capacity,
                    active = true,
                    updated_at = NOW()
                RETURNING *
                """,
                (cleaned_slug, service_name, int(payload.capacity)),
            )
            row = cur.fetchone()
        conn.commit()
    serialized = serialize_row(row)
    return {"rule": serialized, "service_capacity": serialized}


@app.get("/api/customer-work-items")
def list_customer_work_items(
    status: str | None = None,
    kind: str | None = None,
    due: str | None = None,
    limit: int = 100,
) -> dict[str, Any]:
    conditions = ["TRUE"]
    params: list[Any] = []
    if status:
        conditions.append("w.status = %s")
        params.append(sanitize_text(status).lower())
    if kind:
        conditions.append("w.kind = %s")
        params.append(sanitize_text(kind).lower())
    if due == "today":
        conditions.append("w.due_at::date = CURRENT_DATE")
    where_sql = " AND ".join(conditions)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT w.*, c.full_name, c.phone, c.segment, c.subscription_renewal_date
                FROM customer_work_items w
                LEFT JOIN customers c ON c.id = w.customer_id
                WHERE {where_sql}
                ORDER BY w.due_at ASC NULLS LAST, w.created_at DESC
                LIMIT %s
                """,
                (*params, max(1, min(limit, 300))),
            )
            rows = cur.fetchall()
    items = [serialize_row(row) for row in rows]
    return {"items": items, "work_items": items}


@app.post("/api/customer-work-items")
def create_customer_work_item(payload: CustomerWorkItemCreateRequest) -> dict[str, Any]:
    kind = sanitize_text(payload.kind).lower()
    status_value = sanitize_text(payload.status or "open").lower() or "open"
    payload_json = json.dumps(payload.payload or {})
    with get_conn() as conn:
        with conn.cursor() as cur:
            customer_id = payload.customer_id
            instagram_user_id = sanitize_text(payload.instagram_user_id or "") or None
            if not customer_id and instagram_user_id:
                cur.execute("SELECT id FROM customers WHERE instagram_user_id = %s", (instagram_user_id,))
                customer = cur.fetchone()
                customer_id = int(customer["id"]) if customer else None
            cur.execute(
                """
                INSERT INTO customer_work_items (
                    customer_id, instagram_user_id, kind, status, reason, note,
                    due_at, assigned_to, payload
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::timestamptz, %s, %s::jsonb)
                RETURNING *
                """,
                (
                    customer_id,
                    instagram_user_id,
                    kind,
                    status_value,
                    sanitize_text(payload.reason or "") or None,
                    sanitize_text(payload.note or "") or None,
                    payload.due_at,
                    sanitize_text(payload.assigned_to or "") or None,
                    payload_json,
                ),
            )
            row = cur.fetchone()
        conn.commit()
    serialized = serialize_row(row)
    return {"item": serialized, "work_item": serialized}


@app.patch("/api/customer-work-items/{item_id}")
def update_customer_work_item(item_id: int, payload: CustomerWorkItemUpdateRequest) -> dict[str, Any]:
    payload_json = json.dumps(payload.payload) if payload.payload is not None else None
    resolved_at = payload.resolved_at
    status_value = sanitize_text(payload.status or "") or None
    if status_value and status_value.lower() in {"done", "resolved", "closed"} and not resolved_at:
        resolved_at = datetime.now(TZ).isoformat()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE customer_work_items
                SET status = COALESCE(%s, status),
                    reason = COALESCE(%s, reason),
                    note = COALESCE(%s, note),
                    due_at = COALESCE(%s::timestamptz, due_at),
                    resolved_at = COALESCE(%s::timestamptz, resolved_at),
                    assigned_to = COALESCE(%s, assigned_to),
                    payload = CASE WHEN %s::jsonb IS NULL THEN payload ELSE payload || %s::jsonb END,
                    updated_at = NOW()
                WHERE id = %s
                RETURNING *
                """,
                (
                    status_value.lower() if status_value else None,
                    sanitize_text(payload.reason or "") or None,
                    sanitize_text(payload.note or "") or None,
                    payload.due_at,
                    resolved_at,
                    sanitize_text(payload.assigned_to or "") or None,
                    payload_json,
                    payload_json,
                    item_id,
                ),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Work item not found")
        conn.commit()
    serialized = serialize_row(row)
    return {"item": serialized, "work_item": serialized}


@app.get("/api/call-suggestions")
def get_call_suggestions(date: str | None = None, limit: int = 20) -> dict[str, Any]:
    target_date = parse_date_like(date) or datetime.now(TZ).date()
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM customers ORDER BY updated_at DESC LIMIT 300")
            customers = filter_business_records([serialize_row(row) for row in cur.fetchall()])
            cur.execute(
                """
                SELECT *
                FROM customer_work_items
                WHERE status NOT IN ('done', 'resolved', 'closed', 'cancelled', 'canceled')
                """
            )
            work_items = filter_business_records([serialize_row(row) for row in cur.fetchall()])
            cur.execute(
                """
                SELECT id, instagram_user_id, full_name, phone, service, appointment_date, appointment_time, status
                FROM appointments
                WHERE appointment_date BETWEEN %s::date AND %s::date
                """,
                (target_date.isoformat(), target_date.isoformat()),
            )
            appointments = filter_business_records([serialize_row(row) for row in cur.fetchall()])

    items_by_customer: dict[Any, list[dict[str, Any]]] = {}
    appts_by_customer: dict[str, list[dict[str, Any]]] = {}
    for item in work_items:
        key = item.get("customer_id") or item.get("instagram_user_id")
        items_by_customer.setdefault(key, []).append(item)
    for appointment in appointments:
        appts_by_customer.setdefault(str(appointment.get("instagram_user_id") or ""), []).append(appointment)

    suggestions = []
    for customer in customers:
        customer_items = items_by_customer.get(customer.get("id"), []) + items_by_customer.get(customer.get("instagram_user_id"), [])
        customer_appointments = appts_by_customer.get(str(customer.get("instagram_user_id") or ""), [])
        suggestion = build_call_suggestion(customer, customer_items, customer_appointments, target_date)
        if suggestion["score"] > 0:
            suggestions.append(suggestion)
    suggestions.sort(key=lambda item: (-int(item["score"]), str(item.get("full_name") or "")))
    return {"date": target_date.isoformat(), "suggestions": suggestions[: max(1, min(limit, 100))]}


@app.get("/api/crm/templates")
def list_crm_templates() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slug, title, sector, trigger_type, content, active, updated_at
                FROM message_templates
                ORDER BY trigger_type ASC, slug ASC
                """
            )
            rows = cur.fetchall()
    return {"templates": [serialize_row(row) for row in rows]}


@app.get("/api/crm/rules")
def list_crm_rules() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, slug, title, sector, trigger_type, days_after, template_slug, active, updated_at
                FROM automation_rules
                ORDER BY trigger_type ASC, days_after ASC, slug ASC
                """
            )
            rows = cur.fetchall()
    return {"rules": [serialize_row(row) for row in rows]}


@app.get("/api/crm/segments")
def get_crm_segment_summary() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COALESCE(segment, 'unknown') AS segment, COUNT(*) AS count
                FROM customers
                GROUP BY COALESCE(segment, 'unknown')
                ORDER BY count DESC, segment ASC
                """
            )
            rows = cur.fetchall()
    return {"segments": [serialize_row(row) for row in rows]}


@app.get("/api/roi-summary")
def get_roi_summary() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS count FROM message_logs WHERE direction = 'out'")
            answered = int(cur.fetchone()["count"])
            cur.execute("SELECT COUNT(*) AS count FROM automation_events WHERE status = 'sent' AND event_type IN ('control','maintenance','recovery')")
            recovered = int(cur.fetchone()["count"])
            cur.execute("SELECT COUNT(*) AS count FROM automation_events WHERE status = 'sent' AND event_type = 'no_show'")
            prevented_no_show = int(cur.fetchone()["count"])
            
            # Gerçek gelir hesaplaması için veritabanındaki ortalama sepet/hizmet tutarını çek (yoksa default 900 TL)
            cur.execute("SELECT COALESCE(AVG(spend_amount), 900) AS avg_spend FROM customer_service_history WHERE spend_amount > 0")
            avg_spend = float(cur.fetchone()["avg_spend"])
            
            # Sadece "başarıyla atılmış" kurtarma mesajlarının reel parasal dönüşü hesaplanıyor
            real_recovered_revenue = (recovered * avg_spend)
            real_prevented_no_show_revenue = (prevented_no_show * avg_spend)
            
            # Müşteri temsilcisi maliyeti / lead kurtarma bedeli (Her cevaplanan mesaj ortalama 450 TL değer yaratır)
            estimated_saved = answered * 450
            
            total_real_impact = real_recovered_revenue + real_prevented_no_show_revenue + estimated_saved
            
            # Grafikler için son 6 ayın gerçek istatistiği dinamik hesaplanır
            cur.execute("""
                WITH months AS (
                    SELECT to_char(generate_series(CURRENT_DATE - INTERVAL '5 months', CURRENT_DATE, '1 month'), 'Mon') AS month_name,
                           date_trunc('month', generate_series(CURRENT_DATE - INTERVAL '5 months', CURRENT_DATE, '1 month')) AS month_date
                ),
                monthly_stats AS (
                    SELECT 
                        date_trunc('month', created_at) AS month_date,
                        COUNT(CASE WHEN direction = 'out' THEN 1 END) AS msgs,
                        0 AS recoveries
                    FROM message_logs 
                    WHERE created_at >= CURRENT_DATE - INTERVAL '5 months'
                    GROUP BY 1
                    
                    UNION ALL
                    
                    SELECT 
                        date_trunc('month', scheduled_at) AS month_date,
                        0 AS msgs,
                        COUNT(*) AS recoveries
                    FROM automation_events 
                    WHERE status = 'sent' AND scheduled_at >= CURRENT_DATE - INTERVAL '5 months'
                    GROUP BY 1
                )
                SELECT 
                    m.month_name AS name,
                    COALESCE(SUM(s.msgs) * 450 + SUM(s.recoveries) * %s, 0) AS kazanc,
                    COALESCE(SUM(s.recoveries), 0) AS kurtarilan
                FROM months m
                LEFT JOIN monthly_stats s ON m.month_date = s.month_date
                GROUP BY m.month_name, m.month_date
                ORDER BY m.month_date ASC
            """, (avg_spend,))
            
            monthly_history = cur.fetchall()
            
            # İngilizce ay kısaltmalarını Türkçeye çevirme
            tr_months = {
                "Jan": "Oca", "Feb": "Şub", "Mar": "Mar", 
                "Apr": "Nis", "May": "May", "Jun": "Haz", 
                "Jul": "Tem", "Aug": "Ağu", "Sep": "Eyl", 
                "Oct": "Eki", "Nov": "Kas", "Dec": "Ara"
            }
            
            for row in monthly_history:
                row["name"] = tr_months.get(row["name"], row["name"])

    return {
        "answered_messages_count": answered,
        "recovered_customers_count": recovered,
        "prevented_no_show_count": prevented_no_show,
        "estimated_revenue_saved": estimated_saved + real_prevented_no_show_revenue,
        "estimated_revenue_recovered": real_recovered_revenue,
        "estimated_total_impact": total_real_impact,
        "graph_data": monthly_history
    }


@app.get("/internal/automation/claim", response_model=AutomationClaimResponse)
def claim_due_automation_events(limit: int = 20) -> AutomationClaimResponse:
    now = datetime.now(TZ)
    claim_token = str(uuid4())
    stale_before = now - timedelta(minutes=10)
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                WITH picked AS (
                    SELECT id
                    FROM automation_events
                    WHERE scheduled_at <= %s
                      AND (
                        status = 'queued'
                        OR (status = 'claimed' AND claimed_at < %s)
                        OR (status = 'failed' AND scheduled_at <= %s)
                      )
                    ORDER BY scheduled_at ASC, id ASC
                    LIMIT %s
                    FOR UPDATE SKIP LOCKED
                )
                UPDATE automation_events e
                SET status = 'claimed', claim_token = %s, claimed_at = %s, updated_at = NOW()
                FROM picked
                WHERE e.id = picked.id
                RETURNING e.id, e.customer_id, e.template_slug, e.event_type, e.scheduled_at
                """,
                (now, stale_before, now, max(1, min(limit, 50)), claim_token, now),
            )
            claimed = cur.fetchall()
            rows = []
            for item in claimed:
                cur.execute(
                    """
                    SELECT c.instagram_user_id, c.full_name, t.content
                    FROM customers c
                    JOIN message_templates t ON t.slug = %s
                    WHERE c.id = %s
                    """,
                    (item["template_slug"], item["customer_id"]),
                )
                detail = cur.fetchone()
                if not detail:
                    continue
                payload_text = sanitize_text(str(detail.get("content") or "")).replace("{{full_name}}", sanitize_text(str(detail.get("full_name") or "")) or "Merhaba")
                rows.append(AutomationClaimItem(
                    event_id=int(item["id"]),
                    customer_id=int(item["customer_id"]),
                    instagram_user_id=str(detail["instagram_user_id"]),
                    template_slug=str(item["template_slug"]),
                    event_type=str(item["event_type"]),
                    scheduled_at=str(item["scheduled_at"].isoformat()),
                    message_text=payload_text,
                ))
        conn.commit()
    return AutomationClaimResponse(events=rows)


@app.post("/internal/automation/mark")
def mark_automation_event(payload: AutomationMarkRequest) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            if payload.sent:
                cur.execute(
                    "UPDATE automation_events SET status = 'sent', sent_at = NOW(), updated_at = NOW(), last_error = NULL WHERE id = %s RETURNING *",
                    (payload.event_id,),
                )
            else:
                retry_at = sanitize_text(payload.retry_at or "") or None
                if retry_at:
                    cur.execute(
                        "UPDATE automation_events SET status = 'queued', scheduled_at = %s::timestamptz, updated_at = NOW(), last_error = %s, claim_token = NULL, claimed_at = NULL WHERE id = %s RETURNING *",
                        (retry_at, sanitize_text(payload.error or '')[:500] or None, payload.event_id),
                    )
                else:
                    cur.execute(
                        "UPDATE automation_events SET status = 'failed', updated_at = NOW(), last_error = %s, claim_token = NULL WHERE id = %s RETURNING *",
                        (sanitize_text(payload.error or '')[:500] or None, payload.event_id),
                    )
            row = cur.fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Automation event not found")
        conn.commit()
    return {"event": serialize_row(row)}


@app.get("/api/campaigns")
def list_campaigns() -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, title, template_slug, segment, sector, inactivity_days, attendance_status, audience_size, status, created_at, updated_at FROM crm_campaigns ORDER BY created_at DESC, id DESC"
            )
            rows = cur.fetchall()
    return {"campaigns": [serialize_row(row) for row in rows]}


@app.post("/api/campaigns")
def create_campaign(payload: CampaignCreateRequest) -> dict[str, Any]:
    if not payload.template_slug and not payload.custom_message:
        raise HTTPException(status_code=400, detail="Bir şablon seçilmeli veya özel mesaj girilmelidir.")

    # Eğer custom_message geldiyse, arka planda anlık bir şablon yaratalım
    active_template_slug = payload.template_slug
    if payload.custom_message:
        import uuid
        custom_slug = f"custom_msg_{uuid.uuid4().hex[:8]}"
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO message_templates (slug, title, sector, trigger_type, content, active)
                    VALUES (%s, 'Özel Mesaj', 'general', 'custom', %s, true)
                    """,
                    (custom_slug, payload.custom_message)
                )
            conn.commit()
        active_template_slug = custom_slug

    where_clause, params = build_customer_filter_clause(
        segment=payload.segment,
        sector=payload.sector,
        attendance_status=payload.attendance_status,
    )
    inactivity_clause = ""
    if payload.inactivity_days:
        inactivity_clause = " AND COALESCE(c.last_contact_at, c.created_at) <= NOW() - make_interval(days => %s)"
        params.append(int(payload.inactivity_days))
    query = f"SELECT COUNT(*) AS count FROM customers c {where_clause if where_clause else 'WHERE TRUE'}{inactivity_clause}"
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            audience_size = int(cur.fetchone()["count"])
            
            if audience_size == 0:
                raise HTTPException(status_code=400, detail="Bu kriterlere uyan müşteri bulunamadı.")
                
            cur.execute(
                """
                INSERT INTO crm_campaigns (title, template_slug, segment, sector, inactivity_days, attendance_status, audience_size, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, 'draft')
                RETURNING *
                """,
                (payload.title, active_template_slug, payload.segment, payload.sector, payload.inactivity_days, payload.attendance_status, audience_size),
            )
            row = cur.fetchone()
        conn.commit()
    return {"campaign": serialize_row(row)}


@app.post("/api/campaigns/{campaign_id}/execute")
def execute_campaign(campaign_id: int) -> dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. Kampanyayı seç
            cur.execute("SELECT * FROM crm_campaigns WHERE id = %s", (campaign_id,))
            campaign = cur.fetchone()
            if not campaign:
                raise HTTPException(status_code=404, detail="Kampanya bulunamadı")
            if campaign["status"] != "draft":
                raise HTTPException(status_code=400, detail="Sadece taslak halindeki kampanyalar başlatılabilir")

            # 2. Uygun müşterileri bul
            where_clause, params = build_customer_filter_clause(
                segment=campaign["segment"],
                sector=campaign["sector"],
                attendance_status=campaign["attendance_status"],
            )
            inactivity_clause = ""
            if campaign["inactivity_days"]:
                inactivity_clause = " AND COALESCE(c.last_contact_at, c.created_at) <= NOW() - make_interval(days => %s)"
                params.append(int(campaign["inactivity_days"]))
                
            query = f"SELECT id, instagram_user_id FROM customers c {where_clause if where_clause else 'WHERE TRUE'}{inactivity_clause}"
            cur.execute(query, tuple(params))
            targets = cur.fetchall()

            # 3. Automation Events tablosuna bas
            inserted_count = 0
            for customer in targets:
                cur.execute(
                    """
                    INSERT INTO automation_events (customer_id, template_slug, event_type, status, scheduled_at, payload)
                    VALUES (%s, %s, %s, 'queued', NOW(), %s)
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    (customer["id"], campaign["template_slug"], "campaign_blast", json.dumps({"campaign_id": campaign_id}))
                )
                if cur.fetchone():
                    inserted_count += 1
            
            # 4. Kampanyayı gönderildi (processing/sent) olarak işaretle
            cur.execute("UPDATE crm_campaigns SET status = 'processing', updated_at = NOW() WHERE id = %s RETURNING *", (campaign_id,))
            updated_campaign = cur.fetchone()
        conn.commit()

    # N8N'e Webhook tetiklemesi yap (Eğer URL varsa, sistemi ateşle)
    webhook_url = os.getenv("N8N_CRON_WEBHOOK_URL", "")
    if webhook_url:
        import requests
        try:
            requests.post(webhook_url, json={"trigger": "campaign_execute", "campaign_id": campaign_id}, timeout=3)
        except Exception:
            pass

    return {"status": "processing", "targets_queued": inserted_count, "campaign": serialize_row(updated_campaign)}

@app.get("/api/campaigns/preview")
def preview_campaign_audience(
    template_slug: str,
    segment: str | None = None,
    sector: str | None = None,
    inactivity_days: int | None = None,
    attendance_status: str | None = None,
    limit: int = 50,
) -> dict[str, Any]:
    where_clause, params = build_customer_filter_clause(segment=segment, sector=sector, attendance_status=attendance_status)
    inactivity_clause = ""
    if inactivity_days:
        inactivity_clause = " AND COALESCE(c.last_contact_at, c.created_at) <= NOW() - make_interval(days => %s)"
        params.append(int(inactivity_days))
    query = f"""
        SELECT c.id, c.instagram_user_id, c.instagram_username, c.full_name, c.phone, c.sector, c.segment, c.last_contact_at, c.next_automation_type
        FROM customers c
        {where_clause if where_clause else 'WHERE TRUE'}{inactivity_clause}
        ORDER BY c.updated_at DESC
        LIMIT %s
    """
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (*params, max(1, min(limit, 200))))
            rows = [serialize_row(row) for row in cur.fetchall()]
    return {"template_slug": template_slug, "count": len(rows), "customers": rows}


@app.post("/api/process-instagram-message", response_model=ProcessResult)
def process_instagram_message(payload: IncomingMessage, background_tasks: BackgroundTasks) -> ProcessResult:
    request_started_at = time_module.perf_counter()
    metrics = {
        "extract_ms": 0,
        "polish_ms": 0,
        "crm_ms": 0,
        "reply_engine": REPLY_ENGINE,
        "ai_model_used": None,
        "ai_decision_intent": None,
        "fallback_used": False,
        "crm_sync_queued": False,
    }
    trace_id = sanitize_text(payload.trace_id or ((payload.raw_event or {}).get("trace_id") if isinstance(payload.raw_event, dict) else "") or ((payload.raw_event or {}).get("message_id") if isinstance(payload.raw_event, dict) else "") or payload.sender_id)
    used_llm_extractor = False
    decision_path: list[str] = []
    message_text = sanitize_text(payload.message_text or "")
    if not message_text:
        metric_snapshot = {
            **metrics,
            "total_ms": elapsed_ms(request_started_at),
            "message_type": "ignored",
            "used_llm_extractor": used_llm_extractor,
            "decision_path": ["ignored:empty"],
        }
        return ProcessResult(
            sender_id=payload.sender_id,
            should_reply=False,
            reply_text=None,
            handoff=False,
            conversation_state="ignored",
            normalized={},
            metrics=metric_snapshot,
            decision_path=["ignored:empty"],
        )

    inbound_message_id = extract_inbound_message_id(payload.raw_event)
    raw_event_for_log = dict(payload.raw_event or {})
    if trace_id and not raw_event_for_log.get("trace_id"):
        raw_event_for_log["trace_id"] = trace_id
    logger.info("api_inbound_dm trace_id=%s sender_id=%s message_id=%s text=%s", trace_id, payload.sender_id, inbound_message_id, message_text[:160])
    if inbound_message_id and not raw_event_for_log.get("message_id"):
        raw_event_for_log["message_id"] = inbound_message_id

    with get_conn() as conn:
        conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
        reconcile_confirmed_conversation(conn, conversation)
        sanitize_conversation_state(conversation)
        ensure_conversation_memory(conversation)
        sync_conversation_memory_summary(conversation)
        if should_trace_decline_memory(message_text, conversation):
            log_decline_memory_trace("next_inbound_load", payload.sender_id, trace_id, conversation)
        processing_lock_acquired = try_acquire_inbound_processing_lock(conn, payload.sender_id, inbound_message_id)
        if not processing_lock_acquired:
            duplicate_path = ["duplicate_inflight_ignored"]
            logger.info("instagram_message_duplicate_inflight sender_id=%s message_id=%s", payload.sender_id, inbound_message_id)
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=False,
                reply_text=None,
                handoff=conversation.get("state") == "human_handoff",
                conversation_state=conversation.get("state", "new"),
                normalized=build_normalized(conversation),
                metrics={
                    **metrics,
                    "total_ms": elapsed_ms(request_started_at),
                    "message_type": "duplicate",
                    "used_llm_extractor": used_llm_extractor,
                    "decision_path": duplicate_path,
                },
                decision_path=duplicate_path,
            )
        duplicate_inbound = has_processed_inbound_message(conn, payload.sender_id, inbound_message_id)
        if duplicate_inbound:
            duplicate_path = ["duplicate_ignored"]
            logger.info("instagram_message_duplicate_ignored sender_id=%s message_id=%s", payload.sender_id, inbound_message_id)
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=False,
                reply_text=None,
                handoff=conversation.get("state") == "human_handoff",
                conversation_state=conversation.get("state", "new"),
                normalized=build_normalized(conversation),
                metrics={
                    **metrics,
                    "total_ms": elapsed_ms(request_started_at),
                    "message_type": "duplicate",
                    "used_llm_extractor": used_llm_extractor,
                    "decision_path": duplicate_path,
                },
                decision_path=duplicate_path,
            )
        save_message_log(conn, payload.sender_id, "in", message_text, raw_event_for_log)
        recent_history = get_recent_message_history(conn, payload.sender_id)

        def finalize_result(
            reply_text: str | None,
            *,
            handoff: bool = False,
            message_type: str = "reply",
            appointment_created_value: bool = False,
            appointment_id_value: int | None = None,
            should_polish: bool = False,
            decision_label: str | None = None,
        ) -> ProcessResult:
            nonlocal metrics

            compose_enabled = bool(should_polish) and should_ai_compose_reply(
                message_type,
                decision_label,
                handoff=handoff,
                appointment_created=appointment_created_value,
                conversation=conversation,
            )
            if not compose_enabled and should_polish:
                label_check = sanitize_text(decision_label or "").lower()
                compose_enabled = label_check not in SKIP_POLISH_LABELS
            final_reply, polish_ms = maybe_polish_reply_text(
                reply_text,
                conversation,
                recent_history,
                enabled=compose_enabled,
                decision_label=decision_label,
            )
            metrics["polish_ms"] += polish_ms
            final_path = list(decision_path)
            if decision_label:
                final_path.append(decision_label)
            if not final_reply:
                fallback_reply = (reply_text or "").strip()
                if not fallback_reply:
                    fallback_reply = build_emergency_reply(message_text, conversation, decision_label)
                final_reply = fallback_reply
                final_path.append("reply_fallback:guaranteed")
                logger.warning(
                    "instagram_message_reply_fallback trace_id=%s sender_id=%s decision_label=%s compose_enabled=%s",
                    trace_id,
                    payload.sender_id,
                    decision_label,
                    compose_enabled,
                )
            if final_reply:
                update_conversation_memory_after_bot_reply(conversation, final_reply, decision_label)
            if should_trace_decline_memory(message_text, conversation):
                log_decline_memory_trace("before_upsert", payload.sender_id, trace_id, conversation, extra={"decision_label": decision_label, "final_reply": final_reply})
            upsert_conversation(conn, conversation)
            crm_customer = upsert_customer_from_conversation(conn, conversation)
            if crm_customer:
                schedule_customer_automation_events(conn, int(crm_customer["id"]), crm_customer.get("sector"))
            if should_trace_decline_memory(message_text, conversation):
                persisted_conversation = get_or_create_conversation(conn, payload.sender_id, payload.instagram_username)
                sanitize_conversation_state(persisted_conversation)
                ensure_conversation_memory(persisted_conversation)
                sync_conversation_memory_summary(persisted_conversation)
                log_decline_memory_trace("after_upsert", payload.sender_id, trace_id, persisted_conversation, extra={"decision_label": decision_label})
            if final_reply:
                save_message_log(
                    conn,
                    payload.sender_id,
                    "out",
                    final_reply,
                    {
                        "type": message_type,
                        "inbound_message_id": inbound_message_id,
                        "latency": {
                            "extract_ms": metrics["extract_ms"],
                            "polish_ms": metrics["polish_ms"],
                            "crm_ms": metrics["crm_ms"],
                        },
                        "decision_path": final_path,
                    },
                )

            total_ms = elapsed_ms(request_started_at)
            metric_snapshot = {
                **metrics,
                "total_ms": total_ms,
                "message_type": message_type,
                "used_llm_extractor": used_llm_extractor,
                "decision_path": final_path,
            }
            logger.info(
                "instagram_message_processed trace_id=%s sender_id=%s state=%s appointment_created=%s total_ms=%s extract_ms=%s polish_ms=%s crm_ms=%s used_llm_extractor=%s path=%s",
                trace_id,
                payload.sender_id,
                conversation.get("state", "new"),
                appointment_created_value,
                total_ms,
                metrics["extract_ms"],
                metrics["polish_ms"],
                metrics["crm_ms"],
                used_llm_extractor,
                " > ".join(final_path),
            )
            metrics["crm_sync_queued"] = queue_crm_sync(
                background_tasks,
                conversation,
                appointment_id_value if appointment_created_value else None,
                metric_snapshot,
            )
            metric_snapshot["crm_sync_queued"] = metrics["crm_sync_queued"]
            return ProcessResult(
                sender_id=payload.sender_id,
                should_reply=bool(final_reply),
                reply_text=final_reply,
                handoff=handoff,
                conversation_state=conversation.get("state", "new"),
                appointment_created=appointment_created_value,
                appointment_id=appointment_id_value,
                normalized=build_normalized(conversation),
                metrics=metric_snapshot,
                decision_path=final_path,
            )

        if should_reset_stale_conversation(conversation, message_text):
            reset_conversation_for_restart(conversation, clear_identity=True)
            decision_path.append("reset_stale")

        lower_text = message_text.lower()
        has_confirmed_booking = conversation.get("appointment_status") == "confirmed" or conversation.get("state") == "completed"
        if has_confirmed_booking and is_booking_ownership_rejection(message_text):
            contact_text = build_contact_text()
            reply = (
                "Anladım, bu kayıt size ait değilse kusura bakmayın. "
                "Bu görüşmeyi burada durduruyorum ve durumu yetkili ekibimize iletiyorum. "
                f"İsterseniz {contact_text} üzerinden de bize ulaşabilirsiniz."
            )
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["appointment_status"] = "handoff"
            conversation["booking_kind"] = None
            conversation["last_customer_message"] = message_text
            return finalize_result(reply, handoff=True, message_type="handoff", decision_label="confirmed_identity_mismatch_handoff")
        if has_confirmed_booking and (not conversation.get("requested_date") or not conversation.get("requested_time")):
            reconcile_confirmed_conversation(conn, conversation)
        memory = ensure_conversation_memory(conversation)
        if has_confirmed_booking:
            if not memory.get("reschedule_requested_date") and conversation.get("requested_date"):
                memory["reschedule_requested_date"] = conversation.get("requested_date")
            if not memory.get("reschedule_requested_time") and conversation.get("requested_time"):
                memory["reschedule_requested_time"] = normalize_time_string(conversation.get("requested_time"))
        reschedule_followup_open = has_confirmed_booking and memory.get("open_loop") == "reschedule_date_or_time_followup"
        short_reschedule_followup = len(sanitize_text(message_text).split()) <= 8 and any(token in lower_text for token in ["tamam", "peki", "olsa", "olsun", "alayim", "alalım", "alalim", "cekelim", "çekelim", "kaydiralim", "kaydıralım", "degistirelim", "değiştirelim", "guncelleyelim", "güncelleyelim"])
        if reschedule_followup_open and (extract_date(message_text) or extract_time(message_text) or has_date_cue(message_text) or short_reschedule_followup):
            conversation["last_customer_message"] = message_text
            rescheduled, reply, reschedule_label = try_reschedule_confirmed_appointment(conn, conversation, message_text, payload.instagram_username)
            if rescheduled:
                return finalize_result(reply, message_type="appointment", should_polish=False, decision_label=reschedule_label or "appointment_rescheduled", appointment_created_value=True)
            return finalize_result(reply, message_type="appointment", should_polish=False, decision_label="appointment_reschedule_followup")
        if has_confirmed_booking and (any(k in lower_text for k in CANCEL_KEYWORDS) or wants_change_after_confirmation(message_text, conversation)):
            detected_date = extract_date(message_text)
            detected_time = extract_time(message_text)
            if detected_date or detected_time:
                try:
                    rescheduled, reply, reschedule_label = try_reschedule_confirmed_appointment(conn, conversation, message_text, payload.instagram_username)
                except Exception:
                    contact_text = build_contact_text()
                    reply = (
                        f"Mevcut ön görüşmeniz {get_confirmed_appointment_summary(conversation)} için onaylı görünüyor. "
                        "Saat veya tarih değişikligi talebinizi teknik nedenle su an otomatik guncelleyemedim. "
                        f"Isterseniz {contact_text} uzerinden de bize ulasabilirsiniz."
                    )
                    conversation["state"] = "human_handoff"
                    conversation["assigned_human"] = True
                    conversation["last_customer_message"] = message_text
                    return finalize_result(reply, handoff=True, message_type="handoff", decision_label="confirmed_change_handoff")
                conversation["last_customer_message"] = message_text
                if rescheduled:
                    return finalize_result(reply, message_type="appointment", should_polish=False, decision_label=reschedule_label or "appointment_rescheduled", appointment_created_value=True)
                return finalize_result(reply, message_type="appointment", should_polish=False, decision_label="appointment_reschedule_followup")
            contact_text = build_contact_text()
            reply = (
                f"Mevcut ön görüşmeniz {get_confirmed_appointment_summary(conversation)} için onaylı görünüyor. "
                "Saat veya tarih değişikliğiyle iptal taleplerinizi yetkili ekibimize iletiyorum. "
                f"İsterseniz {contact_text} üzerinden de bize ulaşabilirsiniz."
            )
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["last_customer_message"] = message_text
            return finalize_result(reply, handoff=True, message_type="handoff", decision_label="confirmed_change_handoff")

        if has_confirmed_booking and wants_new_booking_after_confirmation(message_text):
            reset_conversation_for_restart(conversation)
            decision_path.append("restart_after_confirmation")
        elif has_confirmed_booking:
            conversation["last_customer_message"] = message_text
            reply = build_post_confirmation_followup_reply(conversation, message_text)
            return finalize_result(reply, message_type="info", should_polish=True, decision_label="confirmed_followup")

        memory = ensure_conversation_memory(conversation)
        if memory.get("offer_status") == "declined" and (is_closeout_message(message_text) or is_low_signal_message(message_text)):
            conversation["last_customer_message"] = message_text
            return finalize_result(
                "Tabii, acelesi yok. Aklınıza takılan bir şey olursa ya da ilerleyen günlerde bakmak isterseniz buradayım.",
                message_type="reply",
                decision_label="info:decline_cooldown",
            )

        llm_data: dict[str, Any] = {}
        if should_call_llm_extractor(message_text, conversation):
            used_llm_extractor = True
            decision_path.append("llm_extractor")
            extract_started_at = time_module.perf_counter()
            llm_data = call_llm_extractor(message_text, conversation, recent_history)
            metrics["extract_ms"] = elapsed_ms(extract_started_at)

        state_before_update = sanitize_text(conversation.get("state") or "new")
        detected_phone = extract_phone(message_text)
        ignored_llm_booking_datetime = should_ignore_llm_booking_datetime_from_phone_message(
            message_text,
            state_before_update,
            detected_phone,
            llm_data,
        )
        llm_requested_time = None if ignored_llm_booking_datetime else normalize_time_string(llm_data.get("requested_time"))
        llm_requested_date = None if ignored_llm_booking_datetime else normalize_date_string(llm_data.get("requested_date"))
        if ignored_llm_booking_datetime:
            decision_path.append("ignored_llm_datetime_from_phone")
        detected_time = extract_time_for_state(message_text, state_before_update) or llm_requested_time
        detected_date = extract_date(message_text) or llm_requested_date
        detected_period = extract_preferred_period(message_text) or infer_period_from_time(detected_time)
        llm_service_hint = llm_data.get("recommended_service") or llm_data.get("service")
        llm_name_candidate = titlecase_name(llm_data.get("name"))
        if detect_business_sector(message_text, recent_history) or is_business_context_intro_message(message_text, recent_history):
            llm_name_candidate = None
        extracted_name = extract_name(message_text, conversation.get("state", "new")) or llm_name_candidate
        detected_name = extracted_name or conversation.get("full_name")
        picked_service = pick_service(message_text, llm_service_hint)
        detected_service = picked_service or conversation.get("service")
        inferred_booking_kind = infer_booking_kind(message_text, llm_data, conversation)
        explicit_booking_intent = message_shows_booking_intent(message_text, llm_data)

        current_name = sanitize_text(conversation.get("full_name") or "")
        username_like_name = bool(current_name) and current_name.lower() in {
            sanitize_text(payload.instagram_username or "").lower(),
            sanitize_text(payload.sender_id or "").lower(),
            sanitize_text(conversation.get("instagram_user_id") or "").lower(),
        }
        if extracted_name and (not current_name or conversation.get("state") == "collect_name" or username_like_name):
            conversation["full_name"] = extracted_name
        elif not current_name and detected_name:
            conversation["full_name"] = detected_name
        if detected_phone and canonical_phone(conversation.get("phone")) != canonical_phone(detected_phone):
            conversation["phone"] = detected_phone
        if (
            detected_phone
            and sanitize_text(conversation.get("state") or "") == "collect_phone"
            and not detected_date
            and not detected_time
        ):
            conversation["requested_date"] = None
            conversation["requested_time"] = None
            memory = ensure_conversation_memory(conversation)
            memory["pending_requested_time"] = None
            memory["suggested_booking_slots"] = []
            conversation["memory_state"] = memory
        applied_service = apply_detected_service_to_conversation(conversation, message_text, llm_service_hint)
        if applied_service:
            picked_service = applied_service
        elif not conversation.get("service") and detected_service:
            conversation["service"] = detected_service
        if detected_date and normalize_date_string(conversation.get("requested_date")) != detected_date:
            conversation["requested_date"] = detected_date
        if detected_time and normalize_time_string(conversation.get("requested_time")) != detected_time:
            conversation["requested_time"] = detected_time
        if detected_time and not conversation.get("preferred_period"):
            conversation["preferred_period"] = infer_period_from_time(detected_time)
        if not conversation.get("preferred_period") and detected_period:
            conversation["preferred_period"] = detected_period
        if not conversation.get("booking_kind") and inferred_booking_kind and (
            explicit_booking_intent or detected_date or detected_time or detected_phone or inferred_booking_kind == "appointment"
        ):
            conversation["booking_kind"] = inferred_booking_kind

        sanitize_conversation_state(conversation)
        conversation["last_customer_message"] = message_text
        conversation["llm_notes"] = llm_data.get("notes")
        if AI_FIRST_ENABLED:
            if should_trace_decline_memory(message_text, conversation, llm_data):
                log_decline_memory_trace("before_user_memory_update", payload.sender_id, trace_id, conversation, extra={"llm_objection_type": llm_data.get("objection_type")})
            update_conversation_memory_from_user_message(
                message_text,
                conversation,
                recent_history,
                llm_data,
                extracted_name=extracted_name,
                detected_phone=detected_phone,
                detected_date=detected_date,
                detected_time=detected_time,
            )
            if should_trace_decline_memory(message_text, conversation, llm_data):
                log_decline_memory_trace("after_user_memory_update", payload.sender_id, trace_id, conversation, extra={"llm_objection_type": llm_data.get("objection_type")})
            decision_path.append(REPLY_ENGINE)
            ai_decision = build_ai_first_decision(message_text, conversation, recent_history, llm_data)
            if ignored_llm_booking_datetime:
                ai_decision["requested_date"] = None
                ai_decision["requested_time"] = None
            force_ai_first_booking_continuation(
                ai_decision,
                conversation,
                state_before_update=state_before_update,
                extracted_name=extracted_name,
                detected_phone=detected_phone,
                detected_time=detected_time,
            )
            metrics["ai_model_used"] = ai_decision.get("ai_model_used")
            metrics["ai_decision_intent"] = ai_decision.get("intent")
            metrics["fallback_used"] = bool(ai_decision.get("fallback_used"))
            apply_ai_first_decision_to_conversation(conversation, ai_decision, message_text)
            appointment_created = False
            appointment_id = None
            final_reply = ai_decision.get("reply_text")
            availability_failed = False
            if llm_bool(ai_decision.get("booking_intent")):
                try:
                    availability_result = prepare_ai_first_booking_availability(
                        conn,
                        conversation,
                        detected_date=detected_date,
                        detected_time=detected_time,
                    )
                    if availability_result.get("reply_text"):
                        final_reply = availability_result["reply_text"]
                        decision_path.append("ai_first_booking_availability")
                    elif availability_result.get("ready_to_book"):
                        decision_path.append("ai_first_booking_slot_resolved")
                except Exception:  # noqa: BLE001
                    logger.exception("ai_first_booking_availability_failed sender_id=%s", payload.sender_id)
                    conversation["state"] = "human_handoff"
                    conversation["assigned_human"] = True
                    final_reply = "Müsaitlik kontrolünde sorun yaşadım, kaydınızı manuel kontrol için ekibe iletiyorum."
                    decision_path.append("ai_first_booking_availability_error")
                    availability_failed = True
            if (
                llm_bool(ai_decision.get("booking_intent"))
                and not availability_failed
                and conversation.get("service")
                and conversation.get("full_name")
                and conversation.get("phone")
                and conversation.get("requested_date")
                and conversation.get("requested_time")
            ):
                validation_error = validate_slot(conversation["requested_date"], conversation["requested_time"])
                if validation_error:
                    conversation["requested_time"] = None
                    conversation["state"] = "collect_time"
                    final_reply = validation_error
                    decision_path.append("ai_first_booking_invalid_slot")
                else:
                    existing = find_existing_appointment(conn, conversation["requested_date"], conversation["requested_time"], conversation.get("service"))
                    if existing:
                        suggestions = suggest_alternatives(conn, conversation["requested_date"], conversation["requested_time"], conversation.get("service"))
                        suggestion_text = ", ".join(suggestions) if suggestions else "aynı gün içinde başka bir uygun saat"
                        conversation["requested_time"] = None
                        conversation["state"] = "collect_time"
                        final_reply = (
                            f"Seçtiğiniz saat dolu görünüyor. Uygun alternatif saatler: {suggestion_text}. "
                            "Size uygun olanı yazarsanız devam edebilirim."
                        )
                        decision_path.append("ai_first_booking_slot_taken")
                    else:
                        try:
                            appointment_id, crm_ms = create_appointment(conn, conversation, payload.instagram_username)
                            metrics["crm_ms"] = crm_ms
                            active_appointment = find_active_appointment_for_user(
                                conn,
                                conversation.get("instagram_user_id"),
                                preferred_date=conversation.get("requested_date"),
                                preferred_time=conversation.get("requested_time"),
                            )
                            if active_appointment:
                                conversation["requested_date"] = active_appointment.get("appointment_date") or conversation.get("requested_date")
                                conversation["requested_time"] = active_appointment.get("appointment_time") or conversation.get("requested_time")
                                conversation["appointment_status"] = "confirmed"
                                conversation["state"] = "completed"
                                appointment_created = True
                                final_reply = build_confirmation_message(conversation)
                                decision_path.append("ai_first_booking_created")
                            else:
                                conversation["state"] = "human_handoff"
                                conversation["assigned_human"] = True
                                final_reply = (
                                    "Kaydınızı oluştururken teknik bir tutarsızlık algıladım; yanlış onay vermemek için sizi yetkili ekibimize yönlendiriyorum. "
                                    f"İsterseniz {build_contact_text()} üzerinden de bize ulaşabilirsiniz."
                                )
                                decision_path.append("ai_first_booking_integrity_handoff")
                        except HTTPException as exc:
                            if exc.status_code == 409 and isinstance(exc.detail, dict) and exc.detail.get("type") == "slot_conflict":
                                conversation["requested_time"] = None
                                conversation["state"] = "collect_time"
                                alternatives = suggest_alternatives(conn, conversation["requested_date"], conversation.get("requested_time"), conversation.get("service"))
                                alt_text = ", ".join(alternatives) if alternatives else "başka bir uygun saat"
                                final_reply = f"Seçtiğiniz saat dolmuş görünüyor. Uygun alternatif: {alt_text}. Hangisi size uyar?"
                                decision_path.append("ai_first_booking_slot_conflict")
                            else:
                                raise
            return finalize_result(
                final_reply,
                handoff=bool(ai_decision.get("handoff_needed")),
                message_type="appointment" if appointment_created else ("handoff" if ai_decision.get("handoff_needed") else "reply"),
                appointment_created_value=appointment_created,
                appointment_id_value=appointment_id,
                should_polish=False,
                decision_label=f"{REPLY_ENGINE}:{ai_decision.get('intent', 'reply')}",
            )
        if is_invalid_name_attempt(message_text, conversation.get("state", "new")):
            return finalize_result(
                "Adınızı ve soyadınızı tam olarak yazar mısınız?",
                message_type="clarify",
                decision_label="collect_name_invalid",
            )
        conversation["llm_notes"] = llm_data.get("notes")
        if should_trace_decline_memory(message_text, conversation, llm_data):
            log_decline_memory_trace("before_user_memory_update", payload.sender_id, trace_id, conversation, extra={"llm_objection_type": llm_data.get("objection_type")})
        update_conversation_memory_from_user_message(
            message_text,
            conversation,
            recent_history,
            llm_data,
            extracted_name=extracted_name,
            detected_phone=detected_phone,
            detected_date=detected_date,
            detected_time=detected_time,
        )
        if should_trace_decline_memory(message_text, conversation, llm_data):
            log_decline_memory_trace("after_user_memory_update", payload.sender_id, trace_id, conversation, extra={"llm_objection_type": llm_data.get("objection_type")})
        direct_matched_services = match_service_candidates(message_text, None)
        direct_service_match = bool(picked_service or direct_matched_services)
        matched_services = match_service_candidates(message_text, conversation.get("service") or llm_service_hint)
        matched_service = matched_services[0] if matched_services else None
        business_fit_question = is_business_fit_question(message_text)
        asks_availability = False if business_fit_question else wants_availability_information(message_text, llm_data)
        booking_transition_allowed = should_enter_booking_collection(
            message_text,
            llm_data,
            asks_availability=asks_availability,
            detected_phone=detected_phone,
            detected_date=detected_date,
            detected_time=detected_time,
            conversation=conversation,
            history=recent_history,
        )
        if business_fit_question:
            booking_transition_allowed = False
        info_result = maybe_build_information_reply(message_text, llm_data, matched_services, conversation, recent_history, direct_service_match=direct_service_match)

        if info_result and not booking_transition_allowed and not detected_date and not detected_time and not detected_phone and llm_data.get("intent") != "appointment":
            if info_result.get("clear_booking"):
                clear_booking_assumption(conversation)
            set_service = sanitize_text(info_result.get("set_service") or "")
            if set_service and (not conversation.get("service") or info_result.get("kind") == "message_volume"):
                conversation["service"] = set_service
            forced_booking_kind = normalize_booking_kind(info_result.get("set_booking_kind"))
            if forced_booking_kind:
                conversation["booking_kind"] = forced_booking_kind
            elif not conversation.get("booking_kind") and not info_result.get("clear_booking") and explicit_booking_intent:
                inferred_info_kind = infer_booking_kind(message_text, llm_data, conversation, matched_services)
                if inferred_info_kind and (conversation.get("service") or set_service or inferred_info_kind == "appointment"):
                    conversation["booking_kind"] = inferred_info_kind
            next_state = info_result.get("next_state")
            reply_text = info_result["reply"]
            info_handoff = bool(info_result.get("handoff"))
            force_next_state = bool(info_result.get("force_next_state"))
            if next_state == "collect_name" and not explicit_booking_intent and not force_next_state:
                next_state = "collect_service"
            if next_state == "collect_name" and "ad soyad" not in sanitize_text(reply_text).lower():
                reply_text = f"{reply_text} Uygunsanız önce ad soyadınızı alayım."
            if info_handoff:
                conversation["assigned_human"] = True
                conversation["appointment_status"] = "handoff"
                if not next_state:
                    next_state = "human_handoff"
            if next_state:
                conversation["state"] = next_state
            return finalize_result(
                reply_text,
                handoff=info_handoff,
                message_type="handoff" if info_handoff else "info",
                should_polish=True,
                decision_label=f"info:{info_result.get('kind', 'generic')}",
            )

        if matched_service and conversation.get("service") != matched_service["display"]:
            conversation["service"] = matched_service["display"]
            sanitize_conversation_state(conversation)

        wants_human = llm_bool(llm_data.get("wants_human")) or any(k in lower_text for k in HUMAN_KEYWORDS)
        if wants_human:
            reply = (
                "Tabii, sizi yetkili ekibimize yönlendiriyorum. "
                "Uygunsanız adınızı ve telefon numaranızı bırakın, ekibimiz size kısa sürede dönüş sağlasın."
            )
            conversation["state"] = "human_handoff"
            conversation["assigned_human"] = True
            conversation["appointment_status"] = "handoff"
            return finalize_result(reply, handoff=True, message_type="handoff", decision_label="human_handoff")

        if is_low_signal_message(message_text) and not any([detected_phone, detected_time, detected_date]) and not direct_service_match:
            memory = ensure_conversation_memory(conversation)
            if memory.get("offer_status") == "declined" or (conversation.get("service") and sanitize_text(message_text).lower() in {"tesekkurler", "tesekkur", "sag ol", "sagol", "eyvallah", "tamam", "peki", "pekala"}):
                conversation["last_customer_message"] = message_text
                reply = "Tabii, acelesi yok. Aklınıza takılan bir şey olursa ya da ilerleyen günlerde bakmak isterseniz buradayım."
                return finalize_result(reply, message_type="reply", decision_label="info:decline_cooldown")
            reply = build_next_step_reply(conn, conversation)
            return finalize_result(reply, message_type="clarify", decision_label="clarify_next_step")

        if not conversation.get("booking_kind") and conversation.get("service") and booking_transition_allowed:
            conversation["booking_kind"] = infer_booking_kind(message_text, llm_data, conversation, matched_services) or ("appointment" if asks_availability else "preconsultation")
        booking_label = get_booking_label(conversation)
        ack_prefix = build_captured_ack_prefix(conversation)
        same_service_restatement = is_same_service_restatement(conversation, picked_service, message_text)
        reply = None
        appointment_created = False
        appointment_id = None
        should_polish_reply = True
        final_decision = "reply"

        memory = ensure_conversation_memory(conversation)
        if memory.get("offer_status") == "declined" and (is_closeout_message(message_text) or is_low_signal_message(message_text)):
            reply = "Tabii, acelesi yok. Aklınıza takılan bir şey olursa ya da ilerleyen günlerde bakmak isterseniz buradayım."
            final_decision = "info:decline_cooldown"
            return finalize_result(reply, message_type="reply", decision_label=final_decision)

        if not conversation.get("service") and get_booking_kind(conversation) != "preconsultation":
            conversation["state"] = "collect_service"
            if conversation.get("requested_date") and asks_availability:
                open_slots = get_available_slots_for_date(conn, conversation["requested_date"], conversation.get("service"))
                if open_slots:
                    reply = build_availability_reply(conversation["requested_date"], open_slots, ask_service=True)
                    final_decision = "collect_service_with_availability"
                else:
                    next_days = find_next_available_days(conn, conversation["requested_date"], service_name=conversation.get("service"))
                    reply = build_no_availability_reply(conversation["requested_date"], next_days, ask_service=True)
                    final_decision = "collect_service_no_availability"
            elif is_simple_greeting(message_text) or is_good_wishes_message(message_text):
                reply = "Merhaba, hoş geldiniz. Size hangi konuda yardımcı olabilirim?"
                if is_good_wishes_message(message_text):
                    reply = build_good_wishes_reply()
                    final_decision = "info:smalltalk"
                else:
                    final_decision = "greeting_collect_service"
            else:
                reply = "Tabii. Size en doğru şekilde yardımcı olabilmem için biraz açar mısınız? Şu an en çok hangi konuda destek arıyorsunuz?"
                final_decision = "collect_service"
        elif not conversation.get("full_name"):
            if conversation.get("service") and not booking_transition_allowed:
                conversation["state"] = "collect_service"
                service_meta = match_service_catalog(conversation.get("service"), conversation.get("service"))
                reply = build_service_info_reply(service_meta, conversation) if service_meta else "Size doğru yön verebilmem için ihtiyacınızı biraz daha netleştirir misiniz?"
                final_decision = "service_info_continue"
            else:
                conversation["state"] = "collect_name"
                reply = build_collect_name_request_reply(conversation, booking_label, ack_prefix, same_service_restatement)
                final_decision = "collect_name"
        elif not conversation.get("phone"):
            if not booking_transition_allowed:
                conversation["state"] = "collect_service"
                service_meta = match_service_catalog(conversation.get("service"), conversation.get("service"))
                reply = build_service_info_reply(service_meta, conversation) if service_meta else "Önce size en doğru çözümü netleştirelim. En çok hangi konuda destek arıyorsunuz?"
                final_decision = "service_info_continue"
            else:
                conversation["state"] = "collect_phone"
                if explicit_booking_intent and conversation.get("full_name"):
                    reply = build_missing_phone_for_booking_reply(conversation)
                    final_decision = "collect_phone_required_for_booking"
                else:
                    reply = f"{ack_prefix}Devam edebilmem için telefon numaranızı da paylaşır mısınız?".strip()
                    final_decision = "collect_phone"
        elif not conversation.get("requested_date"):
            if not booking_transition_allowed:
                conversation["state"] = "collect_service"
                service_meta = match_service_catalog(conversation.get("service"), conversation.get("service"))
                reply = build_service_info_reply(service_meta, conversation) if service_meta else "İhtiyacınızı biraz daha açarsanız size doğru yönü net söyleyebilirim."
                final_decision = "service_info_continue"
            else:
                conversation["state"] = "collect_date"
                reply = build_collect_date_reply(ack_prefix)
                final_decision = "collect_date"
        elif conversation.get("requested_time") and not conversation.get("preferred_period"):
            conversation["preferred_period"] = infer_period_from_time(conversation.get("requested_time"))

        if reply is None:
            if not conversation.get("preferred_period"):
                conversation["state"] = "collect_period"
                reply = "Sabah mı, öğleden sonra mı daha uygunsunuz?"
                final_decision = "collect_period"
            elif not conversation.get("requested_time"):
                conversation["state"] = "collect_time"
                open_slots = get_available_slots_for_date(conn, conversation["requested_date"], conversation.get("service"))
                filtered_slots = filter_slots_by_period(open_slots, conversation.get("preferred_period"))
                if filtered_slots:
                    reply = build_availability_reply(conversation["requested_date"], filtered_slots, period=conversation.get("preferred_period"))
                    final_decision = "collect_time"
                elif open_slots:
                    reply = f"{format_human_date(conversation['requested_date'])} için {get_period_label(conversation.get('preferred_period'))} tarafında boşluk görünmüyor. İsterseniz diğer zaman dilimine de bakabilirim."
                    final_decision = "collect_time_period_full"
                else:
                    next_days = find_next_available_days(conn, conversation["requested_date"], service_name=conversation.get("service"))
                    reply = build_no_availability_reply(conversation["requested_date"], next_days)
                    final_decision = "collect_time_no_availability"
            else:
                validation_error = validate_slot(conversation["requested_date"], conversation["requested_time"])
                if validation_error:
                    conversation["requested_time"] = None
                    conversation["state"] = "collect_time"
                    reply = validation_error
                    final_decision = "invalid_slot"
                else:
                    existing = find_existing_appointment(conn, conversation["requested_date"], conversation["requested_time"], conversation.get("service"))
                    if existing:
                        suggestions = suggest_alternatives(conn, conversation["requested_date"], conversation["requested_time"], conversation.get("service"))
                        suggestion_text = ", ".join(suggestions) if suggestions else "aynı gün içinde başka bir uygun saat"
                        conversation["requested_time"] = None
                        conversation["state"] = "collect_time"
                        reply = (
                            f"Seçtiğiniz saat ne yazık ki dolu görünüyor. Uygun alternatif saatler: {suggestion_text}. "
                            "Size uygun olanı yazarsanız hemen devam edebilirim."
                        )
                        final_decision = "slot_taken"
                    else:
                        try:
                            appointment_id, crm_ms = create_appointment(conn, conversation, payload.instagram_username)
                            metrics["crm_ms"] = crm_ms
                            active_appointment = find_active_appointment_for_user(
                                conn,
                                conversation.get("instagram_user_id"),
                                preferred_date=conversation.get("requested_date"),
                                preferred_time=conversation.get("requested_time"),
                            )
                            if not active_appointment:
                                conversation["state"] = "human_handoff"
                                conversation["assigned_human"] = True
                                reply = (
                                    "Kaydinizi olustururken teknik bir tutarsizlik algiladim; yanlis onay vermemek icin sizi yetkili ekibimize yonlendiriyorum. "
                                    f"Isterseniz {build_contact_text()} uzerinden de bize ulasabilirsiniz."
                                )
                                final_decision = "booking_integrity_handoff"
                            else:
                                conversation["requested_date"] = active_appointment.get("appointment_date") or conversation.get("requested_date")
                                conversation["requested_time"] = active_appointment.get("appointment_time") or conversation.get("requested_time")
                                conversation["appointment_status"] = "confirmed"
                                conversation["state"] = "completed"
                                appointment_created = True
                                should_polish_reply = False
                                reply = build_confirmation_message(conversation)
                                final_decision = "appointment_created"
                        except HTTPException as exc:
                            if exc.status_code == 409 and isinstance(exc.detail, dict) and exc.detail.get("type") == "existing_customer_appointment":
                                existing_date = exc.detail.get("date")
                                existing_time = str(exc.detail.get("time") or "")[:5]
                                active_appointment = find_active_appointment_for_user(
                                    conn,
                                    conversation.get("instagram_user_id"),
                                    preferred_date=existing_date,
                                    preferred_time=existing_time,
                                )
                                if not active_appointment:
                                    conversation["state"] = "human_handoff"
                                    conversation["assigned_human"] = True
                                    reply = (
                                        "Aktif kaydinizi dogrularken teknik bir tutarsizlik algiladim; yanlis yonlendirme yapmamak icin sizi yetkili ekibimize yonlendiriyorum. "
                                        f"Isterseniz {build_contact_text()} uzerinden de bize ulasabilirsiniz."
                                    )
                                    final_decision = "booking_integrity_handoff"
                                else:
                                    conversation["requested_date"] = active_appointment.get("appointment_date") or existing_date or conversation.get("requested_date")
                                    conversation["requested_time"] = active_appointment.get("appointment_time") or existing_time or conversation.get("requested_time")
                                    conversation["appointment_status"] = "confirmed"
                                    conversation["state"] = "completed"
                                    reply = (
                                        f"Sistemimizde zaten {get_confirmed_appointment_summary(conversation)} için aktif bir {get_booking_label(conversation)} kaydınız görünüyor. "
                                        "Yeni bir kayıt açmak yerine mevcut kaydı baz alıyorum. Tarih veya saat değişikliği isterseniz sizi yetkili ekibimize yönlendirebilirim."
                                    )
                                    final_decision = "existing_customer_appointment"
                            elif exc.status_code == 409 and isinstance(exc.detail, dict) and exc.detail.get("type") == "slot_conflict":
                                conversation["requested_time"] = None
                                conversation["state"] = "collect_time"
                                alternatives = suggest_alternatives(conn, conversation["requested_date"], conversation.get("requested_time"), conversation.get("service"))
                                if alternatives:
                                    alt_text = ", ".join(alternatives)
                                    reply = f"Seçtiğiniz saat tam o sırada dolmuş. Uygun alternatifler: {alt_text}. Hangisi size uyar?"
                                else:
                                    reply = "Seçtiğiniz saat maalesef doldu. Başka bir saat belirtir misiniz?"
                                final_decision = "slot_conflict_race"
                            elif exc.status_code == 503:
                                conversation["state"] = "human_handoff"
                                conversation["assigned_human"] = True
                                reply = (
                                    "Şu an CRM bağlantısında geçici bir sorun görünüyor; bu yüzden yanlış kayıt oluşturmamak için sizi yetkili ekibimize yönlendiriyorum. "
                                    f"İsterseniz {build_contact_text()} üzerinden de bize ulaşabilirsiniz."
                                )
                                final_decision = "crm_error_handoff"
                            else:
                                raise


        return finalize_result(
            reply,
            handoff=conversation.get("state") == "human_handoff",
            message_type="reply",
            appointment_created_value=appointment_created,
            appointment_id_value=appointment_id,
            should_polish=should_polish_reply,
            decision_label=final_decision,
        )


def wait_for_database(retries: int = 30, sleep_seconds: int = 2) -> None:
    last_error: Exception | None = None
    for _ in range(retries):
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
                    cur.fetchone()
                return
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time_module.sleep(sleep_seconds)
    raise RuntimeError(f"Database connection failed after retries: {last_error}")


def seed_default_crm_templates(conn: psycopg.Connection) -> None:
    templates = [
        ("control-21d", "21 Gun Kontrol", "beauty", "control", "Merhaba {{full_name}}, son isleminizin uzerinden 21 gun gecti. Kontrol zamani geldiyse size uygun bir randevu olusturabiliriz."),
        ("maintenance-60d", "2 Ay Bakim", None, "maintenance", "Merhaba {{full_name}}, bakim zamani yaklasmis olabilir. Uygunsaniz size yeni bir randevu olusturalim."),
        ("recovery-180d", "6 Ay Geri Kazanim", None, "recovery", "Merhaba {{full_name}}, sizi bir suredir goremiyoruz. Uygunsaniz size ozel yeni bir randevu planlayabiliriz."),
        ("no-show-followup", "No Show Takip", None, "no_show", "Merhaba {{full_name}}, bugun size ulasamadik. Bir sorun mu oldu? Isterseniz size yeni bir randevu olusturalim."),
        ("satisfaction-followup", "Memnuniyet Takip", None, "satisfaction", "Merhaba {{full_name}}, hizmetimizden memnun kaldiniz mi? Dilerseniz yorumunuzu da paylasabilirsiniz."),
        ("tattoo-aftercare-7d", "Tattoo Aftercare Takip", "tattoo", "aftercare", "Merhaba {{full_name}}, dovmeniz sonrasi ilk hafta nasil geciyor? Isterseniz bakim surecini birlikte kontrol edelim."),
        ("tattoo-retouch-45d", "Tattoo Retouch Hatirlatma", "tattoo", "retouch", "Merhaba {{full_name}}, dovmeniz icin retouch zamani yaklasiyorsa size uygun bir kontrol randevusu planlayabiliriz."),
        ("tattoo-recovery-120d", "Tattoo Uyuyan Musteri Geri Kazanim", "tattoo", "recovery", "Merhaba {{full_name}}, yeni dovme, ek seans veya retouch icin yeniden yardimci olabiliriz. Isterseniz kisa bir gorusme planlayalim."),
        ("beauty-control-21d", "Beauty 21 Gun Kontrol", "beauty", "control", "Merhaba {{full_name}}, son isleminizin uzerinden 21 gun gecti. Kontrol veya tamamlayici islem icin uygunlugunuzu alabilir miyim?"),
        ("beauty-maintenance-45d", "Beauty Bakim Hatirlatma", "beauty", "maintenance", "Merhaba {{full_name}}, bakim zamani yaklasiyor olabilir. Size uygun bir saat ayarlayabiliriz."),
        ("beauty-satisfaction-3d", "Beauty Memnuniyet Takip", "beauty", "satisfaction", "Merhaba {{full_name}}, son isleminizden memnun kaldiniz mi? Geri bildiriminiz bizim icin degerli."),
        ("realestate-interest-2d", "Emlak Ilgi Takibi", "real_estate", "interest_followup", "Merhaba {{full_name}}, baktiginiz ilanla ilgili yeni bir gelisme olursa size hemen bilgi verebilirim. Hala ilgileniyor musunuz?"),
        ("realestate-finance-5d", "Emlak Finansman Hatirlatma", "real_estate", "finance_followup", "Merhaba {{full_name}}, kredi ve butce tarafini netlestirmek isterseniz size uygun secenekleri birlikte gozden gecirebiliriz."),
        ("realestate-recovery-30d", "Emlak Pasif Lead Geri Kazanim", "real_estate", "recovery", "Merhaba {{full_name}}, kriterlerinize uygun yeni ilanlar veya alternatifler ciktiysa sizinle paylasabilirim. Isterseniz kisa bir on gorusme planlayalim."),
    ]
    with conn.cursor() as cur:
        for slug, title, sector, trigger_type, content in templates:
            cur.execute(
                """
                INSERT INTO message_templates (slug, title, sector, trigger_type, content)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    title = EXCLUDED.title,
                    sector = EXCLUDED.sector,
                    trigger_type = EXCLUDED.trigger_type,
                    content = EXCLUDED.content,
                    updated_at = NOW()
                """,
                (slug, title, sector, trigger_type, content),
            )


def seed_default_automation_rules(conn: psycopg.Connection) -> None:
    rules = [
        ("control-21d", "21 Gun Kontrol", "beauty", "control", 21, "control-21d"),
        ("maintenance-60d", "2 Ay Bakim", None, "maintenance", 60, "maintenance-60d"),
        ("recovery-180d", "6 Ay Geri Kazanim", None, "recovery", 180, "recovery-180d"),
        ("no-show-followup", "No Show Takip", None, "no_show", 0, "no-show-followup"),
        ("tattoo-aftercare-7d", "Tattoo Aftercare Takip", "tattoo", "aftercare", 7, "tattoo-aftercare-7d"),
        ("tattoo-retouch-45d", "Tattoo Retouch", "tattoo", "retouch", 45, "tattoo-retouch-45d"),
        ("tattoo-recovery-120d", "Tattoo Recovery", "tattoo", "recovery", 120, "tattoo-recovery-120d"),
        ("beauty-control-21d", "Beauty Kontrol", "beauty", "control", 21, "beauty-control-21d"),
        ("beauty-maintenance-45d", "Beauty Bakim", "beauty", "maintenance", 45, "beauty-maintenance-45d"),
        ("beauty-satisfaction-3d", "Beauty Memnuniyet", "beauty", "satisfaction", 3, "beauty-satisfaction-3d"),
        ("realestate-interest-2d", "Emlak Ilgi Takibi", "real_estate", "interest_followup", 2, "realestate-interest-2d"),
        ("realestate-finance-5d", "Emlak Finansman Takibi", "real_estate", "finance_followup", 5, "realestate-finance-5d"),
        ("realestate-recovery-30d", "Emlak Recovery", "real_estate", "recovery", 30, "realestate-recovery-30d"),
    ]
    with conn.cursor() as cur:
        for slug, title, sector, trigger_type, days_after, template_slug in rules:
            cur.execute(
                """
                INSERT INTO automation_rules (slug, title, sector, trigger_type, days_after, template_slug)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (slug) DO UPDATE SET
                    title = EXCLUDED.title,
                    sector = EXCLUDED.sector,
                    trigger_type = EXCLUDED.trigger_type,
                    days_after = EXCLUDED.days_after,
                    template_slug = EXCLUDED.template_slug,
                    updated_at = NOW()
                """,
                (slug, title, sector, trigger_type, days_after, template_slug),
            )


def seed_default_service_capacity_rules(conn: psycopg.Connection) -> None:
    defaults = [
        ("on-gorusme", LIVE_CRM_PRECONSULTATION_SERVICE, 2),
        ("otomasyon-ai", "Otomasyon & Yapay Zeka Çözümleri", 2),
    ]
    for service in DOEL_SERVICE_CATALOG:
        defaults.append((str(service.get("slug") or ""), str(service.get("display") or ""), get_default_service_capacity(str(service.get("display") or ""))))
    with conn.cursor() as cur:
        for slug, service_name, capacity in defaults:
            cleaned_slug = sanitize_service_slug(slug or service_name)
            cleaned_name = sanitize_text(service_name) or cleaned_slug
            cur.execute(
                """
                INSERT INTO service_capacity_rules (service_slug, service_name, capacity, active)
                VALUES (%s, %s, %s, true)
                ON CONFLICT (service_slug) DO UPDATE SET
                    service_name = EXCLUDED.service_name,
                    capacity = GREATEST(service_capacity_rules.capacity, EXCLUDED.capacity),
                    active = true,
                    updated_at = NOW()
                """,
                (cleaned_slug, cleaned_name, int(capacity)),
            )


def run_migrations() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS booking_kind TEXT")
            cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS preferred_period TEXT")
            cur.execute("ALTER TABLE conversations ADD COLUMN IF NOT EXISTS memory_state JSONB NOT NULL DEFAULT '{}'::jsonb")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS attendance_status TEXT NOT NULL DEFAULT 'scheduled'")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS attendance_marked_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS approval_status TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS approval_reason TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS rejection_reason TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS cancellation_reason TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS refund_status TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS refund_amount NUMERIC(12,2)")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS refund_reason TEXT")
            cur.execute("ALTER TABLE appointments ADD COLUMN IF NOT EXISTS capacity_units INTEGER NOT NULL DEFAULT 1")
            cur.execute("ALTER TABLE appointments DROP CONSTRAINT IF EXISTS appointments_appointment_date_appointment_time_key")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS preferences JSONB NOT NULL DEFAULT '{}'::jsonb")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS discount_code TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS custom_offer TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS subscription_renewal_date DATE")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS consent_status TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS consent_updated_at TIMESTAMPTZ")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS voice_note_url TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS customer_type TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS approval_status TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS approval_reason TEXT")
            cur.execute("ALTER TABLE customers ADD COLUMN IF NOT EXISTS rejection_reason TEXT")
            cur.execute("ALTER TABLE automation_events ADD COLUMN IF NOT EXISTS claim_token TEXT")
            cur.execute("ALTER TABLE automation_events ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ")
        seed_default_crm_templates(conn)
        seed_default_automation_rules(conn)
        seed_default_service_capacity_rules(conn)
        conn.commit()


def get_conn() -> psycopg.Connection:
    return psycopg.connect(DATABASE_URL, row_factory=dict_row)


def serialize_row(row: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date, time)):
            out[key] = value.isoformat()
        else:
            out[key] = value
    return out


TEST_RECORD_MARKERS = ("codex", "test", "probe", "prod-sync", "perfect-")
TEST_RECORD_IDENTITY_FIELDS = (
    "instagram_user_id",
    "instagram_username",
    "full_name",
    "customer_name",
    "name",
)


def is_test_record(record: dict[str, Any]) -> bool:
    identity = " ".join(
        str(record.get(field) or "")
        for field in TEST_RECORD_IDENTITY_FIELDS
    ).lower()
    return any(marker in identity for marker in TEST_RECORD_MARKERS)


def filter_business_records(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [record for record in records if not is_test_record(record)]


def build_customer_filter_clause(
    *,
    segment: str | None = None,
    sector: str | None = None,
    attendance_status: str | None = None,
    search: str | None = None,
    created_from: str | None = None,
    created_to: str | None = None,
) -> tuple[str, list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    if segment:
        if segment == "prospect":
            # Hiç randevusu olmayan veya sadece iptal/ön görüşmede kalmış kişiler
            conditions.append("NOT EXISTS (SELECT 1 FROM appointments a WHERE a.instagram_user_id = c.instagram_user_id AND a.status IN ('completed', 'confirmed'))")
        elif segment == "loyal_customer":
            # En az 1 kez başarıyla randevuya gelmiş kişiler
            conditions.append("EXISTS (SELECT 1 FROM appointments a WHERE a.instagram_user_id = c.instagram_user_id AND a.status IN ('completed', 'confirmed'))")
        elif segment == "no_show_customer":
            # İptal veya No-Show durumunda olan biri
            conditions.append("EXISTS (SELECT 1 FROM appointments a WHERE a.instagram_user_id = c.instagram_user_id AND (a.status = 'cancelled' OR a.attendance_status = 'no_show'))")
        else:
            conditions.append("c.segment = %s")
            params.append(segment)
    if sector:
        conditions.append("c.sector = %s")
        params.append(sector)
    if attendance_status:
        conditions.append(
            "EXISTS (SELECT 1 FROM appointments a WHERE a.instagram_user_id = c.instagram_user_id AND a.attendance_status = %s)"
        )
        params.append(attendance_status)
    if search:
        like = f"%{sanitize_text(search)}%"
        conditions.append("(c.instagram_user_id ILIKE %s OR COALESCE(c.instagram_username,'') ILIKE %s OR COALESCE(c.full_name,'') ILIKE %s OR COALESCE(c.phone,'') ILIKE %s)")
        params.extend([like, like, like, like])
    if created_from:
        conditions.append("c.created_at >= %s::timestamptz")
        params.append(created_from)
    if created_to:
        conditions.append("c.created_at <= %s::timestamptz")
        params.append(created_to)
    if not conditions:
        return "", params
    return "WHERE " + " AND ".join(conditions), params


def infer_customer_segment(customer: dict[str, Any]) -> str:
    total_visits = int(customer.get("total_visits") or 0)
    no_show_count = int(customer.get("no_show_count") or 0)
    total_spend = float(customer.get("total_spend") or 0)
    last_visit_at = customer.get("last_visit_at")
    if no_show_count >= 1:
        return "no_show_customer"
    if total_spend >= 20000 or total_visits >= 8:
        return "high_value_customer"
    if total_visits >= 4:
        return "loyal_customer"
    if last_visit_at:
        try:
            last_dt = datetime.fromisoformat(str(last_visit_at).replace("Z", "+00:00"))
            if datetime.now(TZ) - last_dt.astimezone(TZ) > timedelta(days=180):
                return "inactive_customer"
        except Exception:
            pass
    if total_visits <= 1:
        return "new_customer"
    return "active_customer"


def upsert_customer_from_conversation(conn: psycopg.Connection, conversation: dict[str, Any]) -> dict[str, Any] | None:
    sender_id = sanitize_text(str(conversation.get("instagram_user_id") or ""))
    if not sender_id:
        return None
    memory = ensure_conversation_memory(conversation)
    sector = memory.get("customer_sector")
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customers (
                instagram_user_id, instagram_username, full_name, phone, sector,
                last_service, last_contact_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, NOW(), NOW())
            ON CONFLICT (instagram_user_id) DO UPDATE SET
                instagram_username = COALESCE(EXCLUDED.instagram_username, customers.instagram_username),
                full_name = COALESCE(EXCLUDED.full_name, customers.full_name),
                phone = COALESCE(EXCLUDED.phone, customers.phone),
                sector = COALESCE(EXCLUDED.sector, customers.sector),
                last_service = COALESCE(EXCLUDED.last_service, customers.last_service),
                last_contact_at = NOW(),
                updated_at = NOW()
            RETURNING *
            """,
            (
                sender_id,
                conversation.get("instagram_username"),
                conversation.get("full_name"),
                conversation.get("phone"),
                sector,
                conversation.get("service"),
            ),
        )
        row = cur.fetchone()
    customer = serialize_row(row) if row else None
    if customer:
        segment = infer_customer_segment(customer)
        with conn.cursor() as cur:
            cur.execute("UPDATE customers SET segment = %s, updated_at = NOW() WHERE id = %s RETURNING *", (segment, customer["id"]))
            refreshed = cur.fetchone()
        customer = serialize_row(refreshed) if refreshed else customer
    conn.commit()
    return customer


def record_customer_history(conn: psycopg.Connection, customer_id: int, conversation: dict[str, Any], appointment_id: int | None = None) -> None:
    requested_date = normalize_date_string(conversation.get("requested_date"))
    requested_time = normalize_time_string(conversation.get("requested_time"))
    if not requested_date and not appointment_id:
        return
    service_name = conversation.get("service") or LIVE_CRM_PRECONSULTATION_SERVICE
    service_meta = match_service_catalog(service_name, service_name) if service_name else None
    service_category = service_meta.get("slug") if service_meta else None
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO customer_service_history (
                customer_id, appointment_id, service_name, service_category,
                visit_date, visit_time, notes
            ) VALUES (%s, %s, %s, %s, %s::date, %s::time, %s)
            """,
            (
                customer_id,
                appointment_id,
                service_name,
                service_category,
                requested_date,
                requested_time,
                sanitize_text(conversation.get("llm_notes") or "")[:500] or None,
            ),
        )
        cur.execute(
            """
            UPDATE customers
            SET last_visit_at = CASE
                    WHEN %s::date IS NOT NULL AND %s::time IS NOT NULL THEN (%s::date + %s::time)
                    ELSE last_visit_at
                END,
                last_service = COALESCE(%s, last_service),
                total_visits = total_visits + 1,
                updated_at = NOW()
            WHERE id = %s
            """,
            (
                requested_date,
                requested_time,
                requested_date,
                requested_time,
                conversation.get("service") or LIVE_CRM_PRECONSULTATION_SERVICE,
                customer_id,
            ),
        )
    conn.commit()


def schedule_customer_automation_events(conn: psycopg.Connection, customer_id: int, sector: str | None = None, *, base_time: datetime | None = None, no_show: bool = False) -> None:
    anchor = base_time or datetime.now(TZ)
    with conn.cursor() as cur:
        if no_show:
            cur.execute(
                """
                INSERT INTO automation_events (customer_id, rule_id, template_slug, event_type, scheduled_at, payload)
                SELECT %s, id, template_slug, trigger_type, %s, '{}'::jsonb
                FROM automation_rules
                WHERE trigger_type = 'no_show' AND active = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM automation_events e
                      WHERE e.customer_id = %s AND e.rule_id = automation_rules.id
                  )
                ON CONFLICT DO NOTHING
                """,
                (customer_id, anchor, customer_id),
            )
        else:
            allowed_triggers = ['control', 'maintenance', 'recovery', 'aftercare', 'retouch', 'satisfaction', 'interest_followup', 'finance_followup']
            if sector:
                cur.execute(
                    """
                    INSERT INTO automation_events (customer_id, rule_id, template_slug, event_type, scheduled_at, payload)
                    SELECT %s,
                           id,
                           template_slug,
                           trigger_type,
                           %s + make_interval(days => days_after),
                           '{}'::jsonb
                    FROM automation_rules
                    WHERE active = TRUE
                      AND trigger_type = ANY(%s)
                      AND (sector IS NULL OR sector = %s)
                      AND NOT EXISTS (
                          SELECT 1 FROM automation_events e
                          WHERE e.customer_id = %s AND e.rule_id = automation_rules.id
                      )
                    ON CONFLICT DO NOTHING
                    """,
                    (customer_id, anchor, allowed_triggers, sector, customer_id),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO automation_events (customer_id, rule_id, template_slug, event_type, scheduled_at, payload)
                    SELECT %s,
                           id,
                           template_slug,
                           trigger_type,
                           %s + make_interval(days => days_after),
                           '{}'::jsonb
                    FROM automation_rules
                    WHERE active = TRUE
                      AND trigger_type = ANY(%s)
                      AND NOT EXISTS (
                          SELECT 1 FROM automation_events e
                          WHERE e.customer_id = %s AND e.rule_id = automation_rules.id
                      )
                    ON CONFLICT DO NOTHING
                    """,
                    (customer_id, anchor, allowed_triggers, customer_id),
                )
        cur.execute(
            """
            UPDATE customers
            SET next_automation_at = (
                    SELECT MIN(scheduled_at) FROM automation_events
                    WHERE customer_id = %s AND status = 'queued'
                ),
                next_automation_type = (
                    SELECT event_type FROM automation_events
                    WHERE customer_id = %s AND status = 'queued'
                    ORDER BY scheduled_at ASC, id ASC
                    LIMIT 1
                ),
                updated_at = NOW()
            WHERE id = %s
            """,
            (customer_id, customer_id, customer_id),
        )
    conn.commit()


def sanitize_text(value: str) -> str:
    text = re.sub(r"\s+", " ", value or "").strip()
    if not text:
        return ""
    text = unicodedata.normalize("NFKC", text)
    replacements = str.maketrans({
        "İ": "I", "ı": "i",
        "Ş": "S", "ş": "s",
        "Ğ": "G", "ğ": "g",
        "Ü": "U", "ü": "u",
        "Ö": "O", "ö": "o",
        "Ç": "C", "ç": "c",
    })
    return text.translate(replacements)


def validate_voice_note_url(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = str(value).strip()
    if not cleaned:
        return None
    if len(cleaned) > MAX_VOICE_NOTE_URL_LENGTH:
        raise HTTPException(status_code=400, detail="Sesli not kaydı çok büyük")
    if cleaned.startswith("data:") and not cleaned.startswith("data:audio/"):
        raise HTTPException(status_code=400, detail="Sesli not formatı desteklenmiyor")
    return cleaned


def sanitize_service_slug(value: str | None) -> str:
    cleaned = sanitize_text(value or "").lower()
    if not cleaned:
        return "general"
    normalized = unicodedata.normalize("NFKD", cleaned).encode("ascii", "ignore").decode("ascii")
    normalized = re.sub(r"[^a-z0-9]+", "-", normalized).strip("-")
    return normalized or "general"


def get_default_service_capacity(service_name: str | None) -> int:
    slug = sanitize_service_slug(service_name)
    if slug in {"on-gorusme", "preconsultation"}:
        return 2
    if "otomasyon" in slug or "yapay-zeka" in slug or slug == "otomasyon-ai":
        return 2
    return 1


def is_slot_capacity_available_from_counts(current_count: int, capacity: int) -> bool:
    return max(0, int(current_count or 0)) < max(1, int(capacity or 1))


def parse_date_like(value: Any) -> date | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(TZ).date() if value.tzinfo else value.date()
    if isinstance(value, date):
        return value
    text = str(value)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.astimezone(TZ).date() if parsed.tzinfo else parsed.date()
    except ValueError:
        try:
            return date.fromisoformat(text[:10])
        except ValueError:
            return None


def work_item_is_open(item: dict[str, Any]) -> bool:
    return sanitize_text(str(item.get("status") or "open")).lower() not in {"done", "resolved", "closed", "cancelled", "canceled"}


def build_call_suggestion(
    customer: dict[str, Any],
    work_items: list[dict[str, Any]] | None,
    appointments: list[dict[str, Any]] | None,
    target_date: date,
) -> dict[str, Any]:
    score = 0
    reasons: list[str] = []
    instagram_user_id = customer.get("instagram_user_id")

    if parse_date_like(customer.get("subscription_renewal_date")) == target_date:
        score += 35
        reasons.append("Bugün abonelik yenileme")
    if parse_date_like(customer.get("next_automation_at")) == target_date:
        score += 15
        reasons.append("Bugün otomasyon takibi")
    if int(customer.get("no_show_count") or 0) > 0:
        score += 15
        reasons.append("No-show geçmişi")
    if sanitize_text(str(customer.get("segment") or "")).lower() in {"new_customer", "hot_lead", "lead"}:
        score += 10
        reasons.append("Sıcak lead")

    for item in work_items or []:
        if not work_item_is_open(item):
            continue
        kind = sanitize_text(str(item.get("kind") or "")).lower()
        due_today = parse_date_like(item.get("due_at")) == target_date
        if due_today and kind == "support":
            score += 45
            reasons.append("Açık destek talebi")
        elif due_today and kind == "refund":
            score += 40
            reasons.append("Refund takibi")
        elif due_today and kind in {"reminder", "followup"}:
            score += 25
            reasons.append("Sonraya hatırlatma")
        elif due_today:
            score += 20
            reasons.append("Bugün aksiyon bekliyor")
        elif kind == "refund":
            score += 20
            reasons.append("Açık refund takibi")

    for appointment in appointments or []:
        if parse_date_like(appointment.get("appointment_date") or appointment.get("date")) != target_date:
            continue
        status = sanitize_text(str(appointment.get("status") or "")).lower()
        if status == "preconsultation":
            score += 30
            reasons.append("Bugün ön görüşme")
        elif status in {"confirmed", "scheduled"}:
            score += 25
            reasons.append("Bugün randevu")

    deduped_reasons = list(dict.fromkeys(reasons))
    return {
        "customer_id": customer.get("id"),
        "instagram_user_id": instagram_user_id,
        "full_name": customer.get("full_name") or instagram_user_id or "Müşteri",
        "phone": customer.get("phone"),
        "score": score,
        "reasons": deduped_reasons,
        "next_action": deduped_reasons[0] if deduped_reasons else "Genel kontrol",
    }


def is_simple_greeting(text: str) -> bool:
    return is_greeting_like_message(text)


def is_good_wishes_message(text: str) -> bool:
    lowered = sanitize_text(text).lower().strip(".!?, ")
    phrases = {
        "kolay gelsin",
        "iyi calismalar",
        "iyi çalışmalar",
        "hayirli isler",
        "hayırlı işler",
        "basarilar",
        "başarılar",
    }
    return lowered in phrases or any(phrase in lowered for phrase in phrases)


def is_low_signal_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return True
    if lowered in LOW_SIGNAL_MESSAGES:
        return True
    return re.fullmatch(r"[\?\!\.\s]+", text or "") is not None


def is_closeout_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return lowered in {"tamam", "peki", "pekala", "tesekkur", "tesekkurler", "sag ol", "sagol", "eyvallah", "gorusuruz", "iyi calismalar"}


def is_presence_check_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return False
    if any(keyword in lowered for keyword in PRESENCE_CHECK_KEYWORDS):
        return True
    if "?" not in text or len(lowered.split()) > 6:
        return False
    return any(cue in lowered for cue in ["kimse", "burada", "burda", "orda", "orada", "aktif", "bakan", "bakıyor", "bakiyor"])


def is_smalltalk_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return False
    if any(keyword in lowered for keyword in SMALLTALK_KEYWORDS):
        return True
    return any(
        phrase in lowered
        for phrase in [
            "nasilsin", "nasılsın", "nasilsiniz", "nasılsınız",
            "napiyosun", "napiyosunuz", "napiyorsun", "napiyorsunuz",
            "ne yapiyorsun", "ne yapiyorsunuz", "ne yapıyorsun", "ne yapıyorsunuz",
            "naber", "ne haber", "iyi misin", "iyi misiniz",
        ]
    )


def is_reaction_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return False
    if any(keyword in lowered for keyword in ["otomasyon", "yapay zeka", "chatbot", "dm", "randevu", "crm", "hizmet"]):
        return False
    if lowered in REACTION_MESSAGES:
        return True
    short_tokens = set(re.findall(r"\b\w+\b", lowered))
    if any(phrase in lowered for phrase in ["hay allah", "allah allah", "eyyyy"]):
        return len(lowered.split()) <= 4
    return any(token in short_tokens for token in ["yo", "off", "uff"]) and len(lowered.split()) <= 4


def is_technical_issue_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return False
    if any(phrase in lowered for phrase in TECHNICAL_ISSUE_DIRECT_PHRASES):
        return True
    has_context = any(keyword in lowered for keyword in TECHNICAL_ISSUE_CONTEXT_KEYWORDS)
    has_problem = any(keyword in lowered for keyword in TECHNICAL_ISSUE_PROBLEM_KEYWORDS)
    return has_context and has_problem


def is_voice_duration_placeholder_message(text: str) -> bool:
    cleaned = sanitize_text(text)
    return bool(VOICE_DURATION_PLACEHOLDER_PATTERN.match(cleaned))


def build_voice_duration_placeholder_reply() -> str:
    return "Ses kaydını tam çözememiş olabilirim. İsterseniz 1-2 cümleyle yazın; size en doğru şekilde yardımcı olayım."


def build_smalltalk_reply(conversation: dict[str, Any]) -> str:
    if has_resumeable_booking_context(conversation):
        return f"İyidir, teşekkür ederim. {build_booking_resume_hint(conversation)}"
    return "İyidir, teşekkür ederim. Size nasıl yardımcı olabilirim?"


def build_good_wishes_reply() -> str:
    return "Teşekkür ederiz, size de kolay gelsin."


def build_technical_issue_reply(conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    _ = history
    if has_resumeable_booking_context(conversation):
        return "Anladım, burada teknik bir aksaklık yaşanmış gibi duruyor. Hangi mesajdan sonra yanlış otomatik cevap gittiğini kısaca yazar mısınız?"
    return "Anladım, burada teknik bir aksaklık olmuş gibi duruyor. Hangi mesajı yazınca yanlış otomatik cevap gittiğini kısaca paylaşır mısınız?"


def is_fatigue_painpoint_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if any(keyword in lowered for keyword in ["otomasyon", "yapay zeka", "chatbot", "dm", "randevu", "crm", "hizmet"]):
        return False
    return any(keyword in lowered for keyword in FATIGUE_PAINPOINT_KEYWORDS)


def is_business_need_analysis_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in BUSINESS_NEED_ANALYSIS_KEYWORDS)


def is_business_context_intro_message(text: str, history: list[dict[str, Any]] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    if not lowered or "?" in lowered:
        return False
    if is_all_choice_message(text) or is_confirmation_acceptance_message(text) or is_offer_hesitation_message(text):
        return False
    sector_keywords = BEAUTY_BUSINESS_KEYWORDS + REAL_ESTATE_BUSINESS_KEYWORDS
    has_sector_in_current_message = contains_business_keyword(lowered, sector_keywords)
    if not has_sector_in_current_message:
        return False
    if any(keyword in lowered for keyword in BUSINESS_CONTEXT_INTRO_KEYWORDS):
        return True
    return len(lowered.split()) <= 4


def contains_business_keyword(text: str, keywords: list[str]) -> bool:
    lowered = sanitize_text(text).lower()
    for keyword in keywords:
        normalized_keyword = sanitize_text(keyword).lower().strip()
        if not normalized_keyword:
            continue
        keyword_pattern = r"\s+".join(re.escape(part) for part in normalized_keyword.split())
        if re.search(rf"(?<![0-9a-z_]){keyword_pattern}(?![0-9a-z_])", lowered):
            return True
    return False


def detect_business_sector(text: str, history: list[dict[str, Any]] | None = None) -> str | None:
    combined = sanitize_text(text)
    for item in history or []:
        if item.get("direction") == "in":
            combined = f"{combined} {sanitize_text(item.get('message_text') or '')}".strip()
    lowered = combined.lower()
    if contains_business_keyword(lowered, BEAUTY_BUSINESS_KEYWORDS):
        return "beauty"
    if contains_business_keyword(lowered, REAL_ESTATE_BUSINESS_KEYWORDS):
        return "real_estate"
    return None


def is_all_choice_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    compact = re.sub(r"\s+", "", lowered)
    if lowered in ALL_CHOICE_MESSAGES:
        return True
    if compact in {"ikiside", "ikidide", "ikisideya", "herikisi", "ikisidelazim"}:
        return True
    if lowered.startswith("hepsi") and len(lowered.split()) <= 5:
        return True
    return False


def is_confirmation_acceptance_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if lowered in CONFIRMATION_ACCEPTANCE_MESSAGES:
        return True
    prefixes = ["yapalım", "yapalim", "hadi", "hadi yapalım", "hadi yapalim", "başlayalım", "baslayalim"]
    return any(lowered.startswith(prefix) for prefix in prefixes) and len(lowered.split()) <= 5


def get_last_outbound_text(history: list[dict[str, Any]] | None) -> str:
    for item in reversed(history or []):
        if item.get("direction") == "out":
            return sanitize_text(item.get("message_text") or "")
    return ""


def recent_outbound_offered_consultation(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    cues = [
        "ön görüşme", "on gorusme", "tanışma randevusu", "tanishma randevusu", "kısa bir görüşme", "kisa bir gorusme",
        "oluşturalım mı", "olusturalim mi", "planlayalım mı", "planlayalim mi", "görüşelim mi", "goruselim mi",
        "detayları netleştirelim mi", "detaylari netlestirelim mi", "uygun bir vaktinizde", "uygun bir vakitte",
        "telefon görüşmesi", "telefon gorusmesi", "görüşme yapalım mı", "gorusme yapalim mi", "detayları konuşalım mı", "detaylari konusalim mi",
        "toplantı yapalım mı", "toplanti yapalim mi", "toplantı planlayalım mı", "toplanti planlayalim mi"
    ]
    return any(cue in last_outbound for cue in cues)


def recent_outbound_offered_more_details(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return False
    cues = [
        "daha detaylı bilgi almak ister",
        "daha detayli bilgi almak ister",
        "daha detaylı bilgi almak için",
        "daha detayli bilgi almak icin",
        "daha detaylı bilgi için",
        "daha detayli bilgi icin",
        "detaylı bilgi almak ister",
        "detayli bilgi almak ister",
        "daha fazla bilgi almak ister",
        "daha fazla bilgi için",
        "daha fazla bilgi icin",
        "hizmet hakkında daha fazla bilgi",
        "hizmet hakkinda daha fazla bilgi",
        "sistemimizle ilgili daha detaylı bilgi",
        "sistemimizle ilgili daha detayli bilgi",
        "detaylı bilgi verebilirim",
        "detayli bilgi verebilirim",
        "detaylı anlatayım",
        "detayli anlatayim",
    ]
    return any(cue in last_outbound for cue in cues)


def recent_outbound_can_accept_automation_details(
    history: list[dict[str, Any]] | None,
    conversation: dict[str, Any] | None = None,
) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return False
    context_cues = [
        "otomasyon",
        "crm",
        "dm",
        "mesaj",
        "randevuları otomatik",
        "randevulari otomatik",
    ]
    has_automation_context = any(cue in last_outbound for cue in context_cues)
    service_name = display_service_name((conversation or {}).get("service"))
    has_automation_service = not service_name or "otomasyon" in service_name.lower()
    if recent_outbound_offered_more_details(history) and (has_automation_context or has_automation_service):
        return True
    if not has_automation_context:
        return False
    if recent_outbound_offered_consultation(history):
        return False
    question_cues = [
        "hangi sekt",
        "hangi iş modeli",
        "hangi is modeli",
        "hangi hizmet",
        "sektörünüz",
        "sektorunuz",
        "sektörünü",
        "sektorunu",
        "faaliyet göster",
        "faaliyet goster",
        "ilgi duyuyor",
        "uygun olabilir",
        "uygun görünüyor",
        "uygun gorunuyor",
        "fiyat",
        "5.000",
        "teslim süresi",
        "teslim suresi",
        "3-7 iş günü",
        "3-7 is gunu",
    ]
    return any(cue in last_outbound for cue in question_cues)


def is_positive_more_details_acceptance(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    normalized = re.sub(r"\s+", " ", lowered).strip()
    normalized = re.sub(r"[.!?…,:;]+$", "", normalized).strip()
    return normalized in {
        "evet",
        "evet olur",
        "evet tabii",
        "evet tabi",
        "olur",
        "olur tabii",
        "olur tabi",
        "tamam",
        "tamam olur",
        "anlat",
        "anlatın",
        "anlatin",
        "detay ver",
        "detaylı anlat",
        "detayli anlat",
    }


def build_more_details_acceptance_reply(conversation: dict[str, Any]) -> str:
    service = display_service_name(conversation.get("service"))
    if service and "Otomasyon" not in service:
        return (
            f"Tabii. {service} tarafında kapsam, süreç ve fiyat netliği için önce ihtiyacı kısaca anlamamız gerekir. "
            "Buradan temel bilgileri paylaşabilirim; isterseniz hangi hedef için düşündüğünüzü yazın, size net şekilde anlatayım."
        )
    return (
        "Tabii. Otomasyon sistemi gelen DM'leri karşılar, sık soruları yanıtlar, uygun talepleri randevu veya CRM kaydına çevirir "
        "ve panelde takip edilebilir hale getirir. Standart kurulum 3-7 iş günü sürer; özel entegrasyon varsa 1-3 haftaya çıkabilir. "
        "İsterseniz işletmenizdeki DM akışını yazın, hangi parçaların otomatikleşeceğini net söyleyeyim."
    )


def infer_recent_service_for_consultation(
    history: list[dict[str, Any]] | None,
    conversation: dict[str, Any] | None = None,
) -> str:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return ""
    service = display_service_name((conversation or {}).get("service"))
    service_meta = match_service_catalog(service, service) if service else None
    if not service_meta:
        service_meta = match_service_catalog(last_outbound, last_outbound)
        service = display_service_name(str((service_meta or {}).get("display") or ""))
    return service


def recent_outbound_can_start_service_consultation(
    history: list[dict[str, Any]] | None,
    conversation: dict[str, Any] | None = None,
) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return False
    service = infer_recent_service_for_consultation(history, conversation)
    service_meta = match_service_catalog(service, service) if service else None
    if not service or "otomasyon" in service.lower():
        return False
    if recent_outbound_offered_consultation(history):
        return True
    detail_offer = recent_outbound_offered_more_details(history) or "bilgi almak ister" in last_outbound
    if not detail_offer:
        return False
    service_slug = sanitize_text(str((service_meta or {}).get("slug") or "")).lower()
    service_keywords = [sanitize_text(str(item)).lower() for item in (service_meta or {}).get("keywords", [])]
    service_cues = [service.lower(), service_slug, *service_keywords]
    return any(cue and cue in last_outbound for cue in service_cues)


def build_service_consultation_acceptance_reply(conversation: dict[str, Any]) -> str:
    service = display_service_name(conversation.get("service")) or "Web Tasarım - KOBİ Paketi"
    return (
        f"Tabii. {service} için detayları netleştirmek adına kısa bir ön görüşme planlayabiliriz. "
        "Ön görüşme kaydını açabilmem için adınızı ve soyadınızı yazar mısınız?"
    )


def recent_outbound_requested_priority(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return False
    consult_cues = [
        "ön görüşme", "on gorusme", "kısa bir görüşme", "kisa bir gorusme", "planlayalım", "planlayalim",
        "detayları netleştirelim", "detaylari netlestirelim", "görüşelim", "goruselim", "ad soyadınızı", "ad soyadinizi"
    ]
    if any(cue in last_outbound for cue in consult_cues):
        return False
    cues = ["hangi süreç", "hangi surec", "öncelik", "oncelik", "en çok", "en cok", "hangi taraf", "dm cevapları", "dm cevaplari", "randevu takibi", "teklif/fatura", "crm takibi", "müşteri takibi", "musteri takibi"]
    return any(cue in last_outbound for cue in cues)


def recent_outbound_requested_dm_issue(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    cues = [
        "gecikme mi", "geç cevap vermek mi", "gec cevap vermek mi", "tekrar eden mesajlar mı", "tekrar eden mesajlar mi",
        "aynı soruların", "ayni sorularin", "sürekli gelmesi mi", "surekli gelmesi mi",
    ]
    return any(cue in last_outbound for cue in cues)


def recent_outbound_requested_message_volume(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    cues = [
        "günlük mesaj trafiğiniz", "gunluk mesaj trafiginiz", "gün içinde yaklaşık kaç", "gun icinde yaklasik kac",
        "günde yaklaşık kaç", "gunde yaklasik kac", "günde ortalama kaç", "gunde ortalama kac",
        "kaç kişi yazıyor", "kac kisi yaziyor", "kaç mesaj geliyor", "kac mesaj geliyor", "kaç mesaj alıyorsunuz", "kac mesaj aliyorsunuz",
        "gunluk mesaj yogunlugunuz", "mesaj yogunlugunuz yaklasik kac",
    ]
    return any(cue in last_outbound for cue in cues)


def recent_outbound_answered_price(history: list[dict[str, Any]] | None) -> bool:
    last_outbound = get_last_outbound_text(history).lower()
    if not last_outbound:
        return False
    return any(cue in last_outbound for cue in ["tl", "₺", "'den başlıyor", "den basliyor", "fiyat", "ücret", "ucret", "aylık hizmet bedeli", "aylik hizmet bedeli"])


def detect_price_scope_clarification(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    scope_phrases = [
        "butun hizmetler mi", "bütün hizmetler mi", "hepsi mi", "hepsi dahil mi", "hepsi buna dahil mi",
        "bu fiyata hepsi mi", "bu fiyata hepsi dahil mi", "tamami mi", "tamamı mı", "hepsi bunun icinde mi", "hepsi bunun içinde mi",
    ]
    return lowered in scope_phrases or any(phrase in lowered for phrase in scope_phrases)


def infer_recent_outbound_act(history: list[dict[str, Any]] | None) -> str | None:
    if recent_outbound_answered_price(history):
        return "answered_price"
    if recent_outbound_offered_consultation(history):
        return "offered_consultation"
    if recent_outbound_requested_priority(history):
        return "asked_priority"
    if recent_outbound_requested_dm_issue(history):
        return "asked_dm_issue"
    if recent_outbound_requested_message_volume(history):
        return "asked_message_volume"
    last_outbound = get_last_outbound_text(history).lower()
    if any(token in last_outbound for token in ["nasıl yardımcı", "nasil yardimci", "buradayız", "buradayiz", "nasılsınız", "nasilsiniz"]):
        return "social_reply"
    return None


def is_short_followup_message(text: str) -> bool:
    cleaned = sanitize_text(text)
    if not cleaned:
        return False
    if extract_phone(cleaned) or extract_date(cleaned) or extract_time(cleaned):
        return False
    return len(cleaned.split()) <= 6


def infer_contextual_followup_role(
    message_text: str,
    conversation: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
    llm_data: dict[str, Any] | None = None,
) -> str | None:
    conversation = conversation or {}
    memory = ensure_conversation_memory(conversation)
    cleaned = sanitize_text(message_text)
    lowered = cleaned.lower()
    if not cleaned or not is_short_followup_message(cleaned):
        return None

    recent_act = infer_recent_outbound_act(history) or sanitize_text(str(memory.get("last_outbound_act") or "")).lower() or None
    open_loop = sanitize_text(str(memory.get("open_loop") or "")).lower()
    price_context_open = bool(memory.get("price_context_open") or open_loop == "price_clarification")
    explicit_price_followup = is_price_followup_message(cleaned, llm_data) or detect_price_scope_clarification(cleaned)
    weak_price_followup = (
        "?" in cleaned
        and any(token in lowered for token in ["aylık", "aylik", "tek sefer", "bu fiyat", "bu ucret", "bu ücret", "dahil", "tamamı", "tamami"])
    )
    if recent_act == "answered_price" or price_context_open:
        if explicit_price_followup or weak_price_followup:
            return "price_clarification"

    if memory.get("offer_status") == "declined" and (is_closeout_message(cleaned) or is_low_signal_message(cleaned)):
        return "decline_cooldown"
    if recent_act == "offered_consultation" and (is_confirmation_acceptance_message(cleaned) or is_offer_hesitation_message(cleaned) or is_all_choice_message(cleaned)):
        return "offer_followup"
    if recent_act == "asked_priority" and (detect_priority_choice(cleaned) or is_all_choice_message(cleaned)):
        return "answer_to_previous_question"
    if recent_act == "asked_dm_issue" and detect_dm_issue_choice(cleaned):
        return "answer_to_previous_question"
    if recent_act == "asked_message_volume" and is_message_volume_answer(cleaned):
        return "answer_to_previous_question"
    return None


def detect_dm_issue_choice(text: str) -> str | None:
    lowered = sanitize_text(text).lower()
    if any(keyword in lowered for keyword in DM_DELAY_KEYWORDS):
        return "delay"
    if any(keyword in lowered for keyword in REPEATED_MESSAGE_ISSUE_KEYWORDS):
        return "repetition"
    return None


def extract_message_volume_estimate(text: str) -> str | None:
    cleaned = sanitize_text(text)
    range_match = NUMERIC_RANGE_ANSWER_PATTERN.match(cleaned)
    if range_match:
        return f"{range_match.group(1)}-{range_match.group(2)}"
    direct = MESSAGE_VOLUME_PATTERN.search(cleaned)
    if direct:
        return direct.group(1)
    alternate = re.search(r"\b(günde|gunde|günlük|gunluk)\s*(\d{2,5})\b", cleaned, re.IGNORECASE)
    if alternate:
        return alternate.group(2)
    return None


def is_message_volume_answer(text: str) -> bool:
    cleaned = sanitize_text(text)
    lowered = cleaned.lower()
    if NUMERIC_RANGE_ANSWER_PATTERN.match(cleaned):
        return True
    if MESSAGE_VOLUME_PATTERN.search(cleaned):
        return True
    if re.search(r"\b(günde|gunde|günlük|gunluk)\s*(\d{2,5})\b", lowered):
        return True
    return any(keyword in lowered for keyword in MESSAGE_VOLUME_KEYWORDS)


def detect_priority_choice(text: str) -> str | None:
    lowered = sanitize_text(text).lower()
    if lowered in {"tamam", "peki", "pekala", "tesekkur", "tesekkurler", "sag ol", "sagol", "eyvallah"}:
        return None
    compact = re.sub(r"\s+", "", lowered)
    if lowered in {"ikisi", "ikisi de", "ikiside", "iki side", "ikidi de", "her ikisi", "ikisi de lazım", "ikisi de lazim"} or compact in {"ikiside", "ikidide", "ikisi delazim", "ikisidelazim", "herikisi"}:
        return "all"
    choice_map = {
        "dm": ["dm", "dm cevapları", "dm cevaplari", "mesaj", "mesajlar", "mesaj cevapları", "mesaj cevaplari", "mesaj falan", "dm falan"],
        "appointment": ["randevu", "randevu takibi", "takvim", "uygun saat", "müsaitlik", "musaitlik", "randevu falan"],
        "invoice": ["teklif", "fatura", "teklif/fatura", "ödeme", "odeme", "fatura falan", "teklif falan"],
        "crm": ["crm", "müşteri takibi", "musteri takibi", "müşteri kayıt", "musteri kayit", "lead takibi", "müşteri takip", "musteri takip", "müşteri takip falan", "musteri takip falan", "crm falan"],
    }
    for choice, keywords in choice_map.items():
        if any(keyword in lowered for keyword in keywords):
            return choice
    automation_cues = ["otomasyon", "yapay zeka", "ai", "bağla", "bagla", "kur", "kuralım", "kuralim"]
    if any(cue in lowered for cue in automation_cues):
        return "dm"
    return None


def build_priority_choice_reply(choice: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    sector = detect_business_sector(conversation.get("last_customer_message") or "", history)
    if choice == "dm":
        if sector == "beauty":
            return "Anladım, yük daha çok DM tarafında. Salonlarda en çok zaman kaybı genelde geç dönüşten ve aynı soruların tekrar tekrar gelmesinden oluyor. Orayı toparladığımızda gün ciddi rahatlıyor. Sizi en çok geç cevap vermek mi yoruyor, yoksa aynı soruların sürekli gelmesi mi?"
        if sector == "real_estate":
            return "Anladım, yük daha çok mesaj tarafında. Emlakta en büyük kayıp genelde sıcak talebe geç dönmekten ve aynı ilan sorularına tekrar tekrar dönmekten oluyor. Sizi en çok geç dönüş mü yoruyor, yoksa aynı soruların sürekli gelmesi mi?"
        return "Anladım, yük daha çok DM tarafında. Orada geç dönüşü ve aynı soruların tekrarını azalttığımızda iş akışı ciddi rahatlıyor. Sizi en çok gecikme mi yoruyor, yoksa tekrar eden mesajlar mı?"
    if choice == "appointment":
        if sector == "beauty":
            return "Anladım, asıl yük randevu tarafında. Güzellik salonlarında karışıklık genelde uygun saat bulma, iptal/erteleme ve hatırlatma eksikliğinde oluyor. En çok hangi kısım yoruyor: planlama mı, boşluk yönetimi mi, yoksa iptal/erteleme mi?"
        if sector == "real_estate":
            return "Anladım, asıl yük randevu tarafında. Emlakta genelde yer gösterme planı, uygun saat bulma ve iptal/erteleme kısmı operasyonu yoruyor. En çok hangi kısım zorluyor: planlama mı, boş saatleri doldurmak mı, yoksa iptal/erteleme mi?"
        return "Anladım, asıl yük randevu tarafında. Uygun saat, takip ve hatırlatma tarafı toparlandığında operasyon çok rahatlar. Şu an en çok planlama mı yoksa iptal/erteleme kısmı mı yoruyor?"
    if choice == "invoice":
        return "Anladım, teklif ve fatura tarafı yoruyor. Orada tekrar eden hazırlık ve takip işlerini otomatikleştirmek iyi rahatlatır. Şu an en çok zaman alan kısım teklif hazırlamak mı yoksa takip etmek mi?"
    if choice == "crm":
        return "Anladım, müşteri takibi tarafı dağınık kalıyor. Tüm kayıtları tek yerde topladığınızda kimle ne konuşulduğunu takip etmek çok kolaylaşıyor. Şu an en çok nerede kopuyor: yeni talepler mi, mevcut müşteri kayıtları mı?"
    return "Anladım. En çok yük olan tarafı netleştirdiğimizde size daha mantıklı bir kurgu önerebilirim."


def build_dm_issue_followup_reply(issue: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    sector = detect_business_sector(conversation.get("last_customer_message") or "", history)
    if issue == "delay":
        if sector == "beauty":
            return "Haklısınız, geç dönüş salon tarafında çoğu zaman müşterinin başka yere kaymasına neden oluyor. Sağlıklı bir kurgu önerebilmem için gün içinde size yaklaşık kaç kişi yazıyor?"
        if sector == "real_estate":
            return "Haklısınız, emlak tarafında geç dönüş sıcak talebin kaçmasına sebep olabiliyor. Size en mantıklı akışı önerebilmem için gün içinde yaklaşık kaç kişi yazıyor?"
        return "Haklısınız, geç dönüş bir süre sonra fırsat kaybına dönüyor. Size en mantıklı akışı önerebilmem için gün içinde yaklaşık kaç kişi yazıyor?"
    if sector == "beauty":
        return "Anladım, aynı soruların dönmesi salon tarafında ekibi çok yoruyor. En çok ne tekrar ediyor: fiyat, işlem süresi, boş saatler mi?"
    if sector == "real_estate":
        return "Anladım, aynı ilan sorularının tekrar etmesi emlak tarafında ciddi zaman yiyor. En çok ne dönüyor: fiyat, konum, ilan detayı mı yoksa yer gösterme talebi mi?"
    return "Anladım, aynı soruların tekrar etmesi ciddi zaman kaybı yaratıyor. En çok hangi soru tekrar ediyor: fiyat, süreç detayı yoksa uygunluk mu?"


def build_message_volume_reply(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    sector = detect_business_sector(conversation.get("last_customer_message") or "", history)
    volume = extract_message_volume_estimate(message_text)
    volume_prefix = f"Günde yaklaşık {volume} kişi yazıyorsa bu ciddi bir yoğunluk." if volume else "Bu seviye ciddi bir yoğunluk."
    if sector == "beauty":
        return f"{volume_prefix} Bu noktada en mantıklı yapı ilk mesajları otomatik karşılayıp sık soruları anında cevaplamak ve randevu akışını tek yerde toplamak olur. İsterseniz size uygun yapıyı netleştirmek için 10 dakikalık kısa bir ön görüşme planlayalım."
    if sector == "real_estate":
        return f"{volume_prefix} Bu yoğunlukta en mantıklı yapı ilk mesajları otomatik karşılayıp sıcak talebi ayırmak ve yer gösterme planını düzenli yürütmek olur. İsterseniz size uygun akışı netleştirmek için 10 dakikalık kısa bir ön görüşme planlayalım."
    return f"{volume_prefix} Bu durumda ilk mesajları otomatik karşılayıp sık soruları sistemin cevaplaması ve talepleri düzenli ayırması en mantıklı çözüm olur. İsterseniz size uygun yapıyı netleştirmek için 10 dakikalık kısa bir ön görüşme planlayalım."


def build_fatigue_painpoint_reply(conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    sector = detect_business_sector(conversation.get("last_customer_message") or "", history)
    lowered = sanitize_text(conversation.get("last_customer_message") or "").lower()
    if any(token in lowered for token in ["hay allah", "allah allah", "off", "uff", "yok artık", "yok artik", "eyyyy", "yo"]):
        return "Anladım, bir şey canınızı sıkmış gibi duruyor. İsterseniz sakince bakalım; şu an en çok neye takıldınız?"
    if sector == "beauty":
        return "Haklısınız, salon tarafında bir yandan DM'ler bir yandan randevu düzeni derken gün çok hızlı dağılabiliyor. Bu yükü biraz hafifletebiliriz. Şu an sizi en çok geç cevaplar mı yoruyor, yoksa randevu düzeni mi?"
    if sector == "real_estate":
        return "Haklısınız, emlak tarafında bir yandan ilan mesajları bir yandan yer gösterme trafiği derken gün çok hızlı dağılıyor. Bu yükü hafifletebiliriz. Sizi en çok mesajlara geç dönmek mi yoruyor, yoksa randevu/yer gösterme takibi mi?"
    return "Haklısınız, aynı işleri sürekli elle yönetmek bir süre sonra gerçekten yorucu oluyor. Bu yükü hafifletecek bir yapı kurabiliriz. Şu an sizi en çok hangi taraf yoruyor?"


def build_business_owner_need_reply(sector: str | None = None) -> str:
    if sector == "beauty":
        return (
            "Guzellik salonlarinda en buyuk kayip gec donulen DM'lerden ve plansiz randevulardan geliyor. "
            "Bir musteri sabah yazar, aksam baska salona gider. Sistemi kurduktan sonra "
            "her mesaj 2 dakika icinde karsilanir, randevular otomatik planlanir. "
            "Sizin tarafta su an en cok hangisi sorun: mesajlara yetisememe mi, randevu duzeninin dagılması mi?"
        )
    if sector == "real_estate":
        return (
            "Emlakta sicak talep cok cabuk soguyor; bir saat gec dondunuz, musteri rakibe gitti. "
            "Otomasyonla gelen her talep aninda karsilanir, yer gosterme plani otomatik olusur. "
            "Su an sizi en cok ne yoruyor: mesaj trafiği mi, yoksa gorusme planlaması mi?"
        )
    return (
        "Cogu isletme ayni problemle karsilasiyor: mesajlara yetisememe, randevu karmasasi, musteri takibinin dagılması. "
        "Bunlarin hepsini tek bir otomasyonla toparlayabiliriz. "
        "Sizde hangisi en buyuk yuk olusturuyor, oradan baslayalim?"
    )


def build_sector_intro_reply(sector: str | None = None, conversation: dict[str, Any] | None = None) -> str:
    service_name = sanitize_text((conversation or {}).get("service") or "").lower()
    is_web_design_flow = any(keyword in service_name for keyword in ["web", "site", "kurumsal"])
    if is_web_design_flow:
        if sector == "beauty":
            return "Anladim. Dovme studyonuz icin guven veren, calismalarinizi guclu gosteren ve WhatsApp'tan kolay basvuru alan bir yapi kurabiliriz. Isterseniz size uygun site yapisini netlestirmek icin kisa bir on gorusme planlayalim."
        if sector == "real_estate":
            return "Anladim. Bu tarafta portfoyu guven veren sekilde sunan ve basvuru toplayan bir site en hizli sonucu verir. Isterseniz size uygun yapıyı netlestirip kisa bir on gorusme planlayalim."
        return "Anladim. Bu sektor icin guven veren, hizmetlerinizi net gosteren ve basvuru/WhatsApp donusumu alan bir site kurgulayabiliriz. Isterseniz size uygun yapıyı netlestirip kisa bir on gorusme planlayalim."
    if sector == "beauty":
        return "Anladım, bu tarafta genelde DM trafiği, randevu düzeni ve tekrar eden sorular vakit alıyor. Sizde en çok hangi kısım yoruyor?"
    if sector == "real_estate":
        return "Anladım, emlakta genelde mesaj trafiği ve yer gösterme planı vakit alıyor. Sizi en çok mesajlar mı, yoksa görüşme planı mı yoruyor?"
    return "Anladım. Genelde mesaj yönetimi ve müşteri takibi bu tarafta en çok vakit alan kısım oluyor. Sizde hangisi öne çıkıyor?"


def build_multi_need_confirmed_reply(sector: str | None = None) -> str:
    if sector == "beauty":
        return "Anladım. DM, randevu ve müşteri takibini tek akışta toparlayabiliriz. İsterseniz kısa bir ön görüşme planlayalım."
    if sector == "real_estate":
        return "Anladım. Mesajları, sıcak talebi ve görüşme planını tek akışta toparlayabiliriz. İsterseniz kısa bir ön görüşme planlayalım."
    return "Anladım. Tüm akışı tek yerde toparlayabiliriz. İsterseniz kısa bir ön görüşme planlayalım."


def build_offer_acceptance_reply(conversation: dict[str, Any]) -> str:
    booking_label = get_booking_label(conversation)
    return f"Tamamdır, kısa bir {booking_label} planlayalım. Ad soyadınızı alayım."


def reset_conversation_for_restart(conversation: dict[str, Any], clear_identity: bool = False) -> None:
    conversation["service"] = None
    conversation["requested_date"] = None
    conversation["requested_time"] = None
    conversation["booking_kind"] = None
    conversation["preferred_period"] = None
    conversation["appointment_status"] = "collecting"
    conversation["state"] = "new"
    conversation["assigned_human"] = False
    memory = ensure_conversation_memory(conversation)
    memory["pending_offer"] = None
    memory["offer_status"] = "none"
    memory["open_loop"] = None
    memory["last_bot_question_type"] = None
    sync_conversation_memory_summary(conversation)
    if clear_identity:
        conversation["full_name"] = None
        conversation["phone"] = None


def ensure_conversation_memory(conversation: dict[str, Any]) -> dict[str, Any]:
    raw = conversation.get("memory_state")
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except Exception:
            raw = {}
    raw = raw if isinstance(raw, dict) else {}

    memory: dict[str, Any] = {}
    for key, default in CONVERSATION_MEMORY_DEFAULTS.items():
        value = raw.get(key, default)
        if key == "suggested_booking_slots":
            slots: list[dict[str, str]] = []
            for item in value if isinstance(value, list) else []:
                if not isinstance(item, dict):
                    continue
                slot_date = normalize_date_string(item.get("date"))
                slot_time = normalize_time_string(item.get("time"))
                if slot_date and slot_time:
                    slots.append({"date": slot_date, "time": slot_time})
                if len(slots) >= AI_FIRST_BOOKING_SLOT_LIMIT:
                    break
            memory[key] = slots
            continue
        if isinstance(default, list):
            cleaned_items: list[str] = []
            for item in value if isinstance(value, list) else []:
                clean = sanitize_text(str(item or ""))
                if clean and clean not in cleaned_items:
                    cleaned_items.append(clean)
            memory[key] = cleaned_items
        else:
            if value is None:
                memory[key] = default
            elif isinstance(default, str):
                clean = sanitize_text(str(value))
                memory[key] = clean or default
            else:
                clean = sanitize_text(str(value))
                memory[key] = clean or None
    conversation["memory_state"] = memory
    return memory


def remember_memory_value(memory: dict[str, Any], key: str, value: str | None, *, limit: int = 8) -> None:
    clean = sanitize_text(value or "")
    if not clean:
        return
    current = [sanitize_text(str(item or "")) for item in memory.get(key) or []]
    current = [item for item in current if item]
    if clean not in current:
        current.append(clean)
    memory[key] = current[-limit:]


def build_conversation_memory_summary(conversation: dict[str, Any]) -> str | None:
    memory = ensure_conversation_memory(conversation)
    parts: list[str] = []
    if memory.get("customer_sector"):
        parts.append(f"sektör={memory['customer_sector']}")
    if memory.get("customer_goal"):
        parts.append(f"hedef={memory['customer_goal']}")
    if memory.get("pain_points"):
        parts.append("sorun=" + ", ".join(memory["pain_points"][:3]))
    if memory.get("pending_offer"):
        parts.append(f"teklif={memory['pending_offer']}:{memory.get('offer_status')}")
    if memory.get("open_loop"):
        parts.append(f"açık_adım={memory['open_loop']}")
    if memory.get("answered_question_types"):
        parts.append("cevaplanan=" + ", ".join(memory["answered_question_types"][-4:]))
    summary = " | ".join(part for part in parts if part)
    return summary[:320] if summary else None


def sync_conversation_memory_summary(conversation: dict[str, Any]) -> None:
    memory = ensure_conversation_memory(conversation)
    memory["conversation_summary"] = build_conversation_memory_summary(conversation)


def reply_offers_consultation(text: str | None) -> bool:
    lowered = sanitize_text(text or "").lower()
    if not lowered:
        return False
    cues = [
        "ön görüşme", "on gorusme", "kısa bir görüşme", "kisa bir gorusme", "görüşelim", "goruselim",
        "planlayalım", "planlayalim", "netleştirelim", "netlestirelim", "toplantı", "toplanti",
    ]
    return any(cue in lowered for cue in cues)


def infer_reply_question_type(reply_text: str | None, decision_label: str | None = None, conversation: dict[str, Any] | None = None) -> str | None:
    lowered = sanitize_text(reply_text or "").lower()
    if not lowered:
        return None
    if any(token in lowered for token in ["ad soyad", "adınız ve soyadınız", "adiniz ve soyadiniz"]):
        return "name"
    if any(token in lowered for token in ["telefon numaran", "telefon numaranı", "telefon numaranizi", "telefon numarası", "telefon numarasi"]):
        return "phone"
    if any(token in lowered for token in ["uygun gün", "hangi gün", "hangi tarih", "uygun tarih", "size uygun gün"]):
        return "date"
    if "sabah mı" in lowered or "öğleden sonra mı" in lowered or "ogleden sonra mi" in lowered:
        return "period"
    if any(token in lowered for token in ["hangi saat", "boşluk", "bosluk", "uygun saat", "hangi saatler"]):
        return "time"
    if any(token in lowered for token in ["hangi taraf yoruyor", "en çok hangi taraf", "en cok hangi taraf", "en çok hangi kısım", "en cok hangi kisim", "hangisi aksiyor", "hangi surec yoruyor", "hangi süreç yoruyor", "hangi platformda", "mesaj trafigi mi", "mesaj trafiği mi", "yer gosterme plani mi", "yer gösterme planı mı"]):
        return "priority"
    if any(token in lowered for token in ["gecikme mi", "geç dönüş mü", "gec donus mu", "tekrar eden mesajlar", "tekrar eden sorular", "aynı sorular", "ayni sorular", "gec donusler mi", "geç dönüşler mi"]):
        return "dm_issue"
    if any(token in lowered for token in ["kaç kişi yazıyor", "kac kisi yaziyor", "kaç mesaj geliyor", "kac mesaj geliyor"]):
        return "message_volume"
    if any(token in lowered for token in ["gunluk mesaj yogunlugunuz", "mesaj yogunlugunuz yaklasik kac"]):
        return "message_volume"
    if reply_offers_consultation(lowered) and "?" in lowered:
        return "offer_response"
    label = sanitize_text(decision_label or "").lower()
    if label in {"collect_service", "greeting_collect_service"}:
        return "service"
    return None


def update_conversation_memory_from_user_message(
    message_text: str,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None,
    llm_data: dict[str, Any] | None,
    *,
    extracted_name: str | None = None,
    detected_phone: str | None = None,
    detected_date: str | None = None,
    detected_time: str | None = None,
) -> None:
    llm_data = llm_data or {}
    memory = ensure_conversation_memory(conversation)
    sector = detect_business_sector(message_text, history)
    if sector:
        memory["customer_sector"] = sector

    goal = sanitize_text(str(llm_data.get("what_user_needs") or infer_user_need(message_text, conversation, history) or ""))
    if goal:
        memory["customer_goal"] = goal[:240]

    if recent_outbound_requested_priority(history) or memory.get("last_bot_question_type") == "priority":
        priority_choice = detect_priority_choice(message_text)
        if priority_choice or is_all_choice_message(message_text):
            remember_memory_value(memory, "answered_question_types", "priority")
            memory["open_loop"] = "priority_answered"
            if priority_choice:
                memory["last_priority_choice"] = priority_choice
                mapped = {
                    "dm": "dm",
                    "appointment": "randevu",
                    "crm": "müşteri_takibi",
                    "invoice": "teklif_fatura",
                }.get(priority_choice)
                if mapped:
                    remember_memory_value(memory, "pain_points", mapped)
            else:
                for item in ["dm", "randevu", "müşteri_takibi"]:
                    remember_memory_value(memory, "pain_points", item)

    if recent_outbound_requested_dm_issue(history) or memory.get("last_bot_question_type") == "dm_issue":
        dm_issue_choice = detect_dm_issue_choice(message_text)
        if dm_issue_choice:
            remember_memory_value(memory, "answered_question_types", "dm_issue")
            memory["last_dm_issue_choice"] = dm_issue_choice
            remember_memory_value(memory, "pain_points", "geç_dönüş" if dm_issue_choice == "delay" else "tekrar_eden_sorular")
            memory["open_loop"] = "dm_issue_answered"

    if recent_outbound_requested_message_volume(history) or memory.get("last_bot_question_type") == "message_volume":
        if is_message_volume_answer(message_text):
            remember_memory_value(memory, "answered_question_types", "message_volume")
            estimate = extract_message_volume_estimate(message_text)
            if estimate:
                memory["message_volume_estimate"] = estimate
            memory["open_loop"] = "message_volume_answered"

    if extracted_name:
        remember_memory_value(memory, "answered_question_types", "name")
        memory["open_loop"] = "name_received"
    if detected_phone:
        remember_memory_value(memory, "answered_question_types", "phone")
        memory["open_loop"] = "phone_received"
    if detected_date:
        remember_memory_value(memory, "answered_question_types", "date")
        memory["open_loop"] = "date_received"
    if detected_time:
        remember_memory_value(memory, "answered_question_types", "time")
        memory["open_loop"] = "time_received"

    if (recent_outbound_offered_consultation(history) or memory.get("pending_offer") == "preconsultation_offer") and is_confirmation_acceptance_message(message_text):
        memory["pending_offer"] = "preconsultation_offer"
        memory["offer_status"] = "accepted"
        memory["open_loop"] = "collect_name"
    elif (recent_outbound_offered_consultation(history) or memory.get("pending_offer") == "preconsultation_offer") and is_offer_hesitation_message(message_text):
        memory["pending_offer"] = "preconsultation_offer"
        memory["offer_status"] = "hesitant"
        memory["open_loop"] = "offer_response"

    if conversation.get("state") == "collect_phone" and is_phone_share_refusal(message_text):
        remember_memory_value(memory, "answered_question_types", "phone_refusal")
        memory["pending_offer"] = None
        memory["offer_status"] = "declined"
        memory["open_loop"] = "decline_cooldown"

    objection_type = sanitize_text(str(llm_data.get("objection_type") or match_objection_type(message_text) or "")).lower()
    if objection_type == "hesitation" and not message_shows_booking_intent(message_text, llm_data):
        memory["pending_offer"] = None
        memory["offer_status"] = "declined"
        memory["open_loop"] = "decline_cooldown"
        memory["last_bot_question_type"] = None
        memory["last_priority_choice"] = None
        memory["last_dm_issue_choice"] = None

    if is_request_reason_question(message_text) or is_clarification_request(message_text):
        memory["open_loop"] = "clarification"

    if is_price_question(message_text) or is_price_followup_message(message_text, llm_data):
        memory["current_topic"] = "price"
        memory["price_context_open"] = True
        memory["open_loop"] = "price_clarification"

    if llm_bool(llm_data.get("did_user_accept_previous_offer")):
        memory["pending_offer"] = "preconsultation_offer"
        memory["offer_status"] = "accepted"
        memory["open_loop"] = "collect_name"

    recent_outbound_act = infer_recent_outbound_act(history)
    if recent_outbound_act:
        memory["last_outbound_act"] = recent_outbound_act
    followup_role = infer_contextual_followup_role(message_text, conversation, history, llm_data)
    if followup_role:
        memory["last_followup_role"] = followup_role
        if followup_role == "price_clarification":
            memory["open_loop"] = "price_clarification"

    message_role = sanitize_text(str(llm_data.get("message_role") or infer_message_role(message_text, conversation, history) or "")).lower()
    if message_role == "answer_to_previous_question" and memory.get("last_bot_question_type"):
        remember_memory_value(memory, "answered_question_types", memory.get("last_bot_question_type"))

    sync_conversation_memory_summary(conversation)


def update_conversation_memory_after_bot_reply(conversation: dict[str, Any], reply_text: str | None, decision_label: str | None = None) -> None:
    memory = ensure_conversation_memory(conversation)
    lowered_label = sanitize_text(decision_label or "").lower()
    question_type = infer_reply_question_type(reply_text, decision_label, conversation)
    last_customer_message = sanitize_text(conversation.get("last_customer_message") or "")
    last_objection_type = match_objection_type(last_customer_message)

    if question_type:
        memory["last_bot_question_type"] = question_type
    elif lowered_label in {"appointment_created", "existing_customer_appointment", "human_handoff", "crm_error_handoff"}:
        memory["last_bot_question_type"] = None

    if lowered_label == "info:objection" and last_objection_type == "hesitation":
        memory["pending_offer"] = None
        memory["offer_status"] = "declined"
        memory["open_loop"] = "decline_cooldown"
        memory["last_bot_question_type"] = None
        memory["last_priority_choice"] = None
        memory["last_dm_issue_choice"] = None
    elif memory.get("offer_status") == "declined" and lowered_label in {"service_info_continue", "info:objection", "info:decline_cooldown", "info:smalltalk", "info:greeting", "info:presence_check", "clarify_next_step", "info:generic", "reply_fallback:guaranteed"}:
        memory["pending_offer"] = None
        memory["offer_status"] = "declined"
        memory["open_loop"] = "decline_cooldown"
        memory["last_bot_question_type"] = None
        memory["last_priority_choice"] = None
        memory["last_dm_issue_choice"] = None
    elif reply_requests_booking_details(reply_text or ""):
        memory["pending_offer"] = None
        memory["offer_status"] = "accepted"
        memory["open_loop"] = f"collect_{question_type}" if question_type in {"name", "phone", "date", "period", "time"} else "booking_collection"
    elif reply_offers_consultation(reply_text):
        memory["pending_offer"] = "preconsultation_offer"
        if memory.get("offer_status") != "accepted":
            memory["offer_status"] = "offered"
        memory["open_loop"] = "offer_response"

    if lowered_label in {"appointment_created", "existing_customer_appointment"}:
        memory["pending_offer"] = None
        memory["offer_status"] = "accepted"
        memory["open_loop"] = "completed"
    elif lowered_label in {"human_handoff", "crm_error_handoff", "confirmed_change_handoff"}:
        memory["pending_offer"] = None
        memory["open_loop"] = "handoff"
    elif lowered_label == "info:clarification" and conversation.get("state") in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        memory["open_loop"] = conversation.get("state")
    elif lowered_label == "info:phone_refusal":
        memory["pending_offer"] = None
        memory["offer_status"] = "declined"
        memory["open_loop"] = "decline_cooldown"

    if conversation.get("service") and lowered_label in {"service_info_continue", "info:service_info", "info:service_advice", "info:comparison"}:
        remember_memory_value(memory, "topics_already_explained", f"service:{conversation['service']}")
    if "price" in lowered_label:
        remember_memory_value(memory, "topics_already_explained", f"price:{conversation.get('service') or 'general'}")
        memory["current_topic"] = "price"
        memory["price_context_open"] = True
        memory["last_outbound_act"] = "answered_price"
        memory["open_loop"] = "price_clarification"
    elif lowered_label in {"info:decline_cooldown", "info:smalltalk", "info:greeting", "info:presence_check", "info:service_info", "service_info_continue"} and memory.get("current_topic") != "price":
        memory["price_context_open"] = False
    if lowered_label in {"info:message_volume", "info:multi_need_confirmed", "info:service_advice", "info:comparison"}:
        memory["last_recommended_solution"] = sanitize_text(reply_text or "")[:240] or memory.get("last_recommended_solution")

    sync_conversation_memory_summary(conversation)


def build_contact_text() -> str:
    parts: list[str] = []
    if BUSINESS_PHONE and "X" not in BUSINESS_PHONE:
        parts.append(BUSINESS_PHONE)
    if BUSINESS_EMAIL:
        parts.append(BUSINESS_EMAIL)
    if BUSINESS_WEBSITE:
        parts.append(BUSINESS_WEBSITE)
    return " | ".join(parts)


def build_working_hours_text() -> str:
    return f"Pazartesi-Cumartesi {WORKING_HOURS_START}-{WORKING_HOURS_END}"


def build_collect_date_reply(ack_prefix: str) -> str:
    return f"{ack_prefix}Size uygun gün nedir? Çalışma saatlerimiz {build_working_hours_text()} arası.".strip()


def extract_inbound_message_id(raw_event: dict[str, Any] | None) -> str | None:
    if not isinstance(raw_event, dict):
        return None

    candidates = [
        raw_event.get("message_id"),
        raw_event.get("mid"),
        ((raw_event.get("message") or {}).get("mid") if isinstance(raw_event.get("message"), dict) else None),
        (((raw_event.get("raw_event") or {}).get("message") or {}).get("mid") if isinstance(raw_event.get("raw_event"), dict) else None),
    ]
    for value in candidates:
        clean = sanitize_text(str(value or ""))
        if clean:
            return clean
    return None


def has_processed_inbound_message(conn: psycopg.Connection, sender_id: str, message_id: str | None) -> bool:
    if not message_id:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM message_logs
            WHERE instagram_user_id = %s
              AND direction = 'in'
              AND raw_payload->>'message_id' = %s
            LIMIT 1
            """,
            (sender_id, message_id),
        )
        return cur.fetchone() is not None


def has_outbound_after_inbound(conn: psycopg.Connection, sender_id: str, message_id: str | None) -> bool:
    if not message_id:
        return False
    with conn.cursor() as cur:
        cur.execute(
            """
            WITH inbound AS (
                SELECT max(created_at) AS inbound_created_at
                FROM message_logs
                WHERE instagram_user_id = %s
                  AND direction = 'in'
                  AND raw_payload->>'message_id' = %s
            )
            SELECT 1
            FROM message_logs, inbound
            WHERE message_logs.instagram_user_id = %s
              AND message_logs.direction = 'out'
              AND inbound.inbound_created_at IS NOT NULL
              AND message_logs.created_at >= inbound.inbound_created_at
            LIMIT 1
            """,
            (sender_id, message_id, sender_id),
        )
        return cur.fetchone() is not None


def try_acquire_inbound_processing_lock(conn: psycopg.Connection, sender_id: str, message_id: str | None) -> bool:
    if not message_id:
        return True
    lock_key = f"ig_inbound:{sender_id}:{message_id}"
    with conn.cursor() as cur:
        cur.execute("SELECT pg_try_advisory_xact_lock(hashtext(%s)) AS locked", (lock_key,))
        row = cur.fetchone()
    return bool(row and row.get("locked"))


def get_confirmed_appointment_summary(conversation: dict[str, Any]) -> str:
    requested_date = conversation.get("requested_date")
    requested_time = str(conversation.get("requested_time") or "")[:5]
    if requested_date and requested_time:
        return f"{format_human_date(requested_date)} saat {requested_time}"
    if requested_date:
        return format_human_date(requested_date)
    return "belirlenen uygun zaman dilimi"


def find_active_appointment_for_user(
    conn: psycopg.Connection,
    sender_id: str | None,
    *,
    preferred_date: Any = None,
    preferred_time: Any = None,
) -> dict[str, Any] | None:
    if not sender_id:
        return None
    normalized_date = normalize_date_string(preferred_date)
    normalized_time = normalize_time_string(preferred_time)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, instagram_username, full_name, phone, service, appointment_date, appointment_time, status, created_at, updated_at
            FROM appointments
            WHERE instagram_user_id = %s
              AND status IN ('confirmed', 'preconsultation')
            ORDER BY
              CASE
                WHEN %s::date IS NOT NULL AND %s::time IS NOT NULL
                 AND appointment_date = %s::date AND appointment_time = %s::time THEN 0
                ELSE 1
              END,
              updated_at DESC,
              appointment_date DESC,
              appointment_time DESC,
              id DESC
            LIMIT 1
            """,
            (sender_id, normalized_date, normalized_time, normalized_date, normalized_time),
        )
        row = cur.fetchone()
        return serialize_row(row) if row else None


def find_latest_confirmed_appointment_for_user(conn: psycopg.Connection, sender_id: str | None) -> dict[str, Any] | None:
    return find_active_appointment_for_user(conn, sender_id)


def reconcile_confirmed_conversation(conn: psycopg.Connection, conversation: dict[str, Any]) -> None:
    if conversation.get("appointment_status") != "confirmed" and conversation.get("state") != "completed":
        return

    latest_appointment = find_active_appointment_for_user(
        conn,
        conversation.get("instagram_user_id"),
        preferred_date=conversation.get("requested_date"),
        preferred_time=conversation.get("requested_time"),
    )
    if not latest_appointment:
        if conversation.get("appointment_status") == "confirmed":
            conversation["appointment_status"] = "collecting"
        return

    if conversation.get("appointment_status") != "handoff":
        conversation["appointment_status"] = "confirmed"
    conversation["booking_kind"] = "preconsultation" if latest_appointment.get("status") == "preconsultation" else "appointment"
    conversation["full_name"] = latest_appointment.get("full_name") or conversation.get("full_name")
    conversation["phone"] = latest_appointment.get("phone") or conversation.get("phone")
    conversation["service"] = latest_appointment.get("service") or conversation.get("service")
    conversation["requested_date"] = latest_appointment.get("appointment_date") or conversation.get("requested_date")
    conversation["requested_time"] = latest_appointment.get("appointment_time") or conversation.get("requested_time")
    if conversation.get("state") != "human_handoff":
        conversation["state"] = "completed"
        conversation["assigned_human"] = False


def wants_new_booking_after_confirmation(message_text: str) -> bool:
    lowered = sanitize_text(message_text).lower()
    return any(
        phrase in lowered
        for phrase in [
            "yeni randevu",
            "yeniden randevu",
            "tekrar randevu",
            "başka bir randevu",
            "ayrı bir randevu",
            "yeni bir görüşme",
            "başka bir görüşme",
        ]
    )


def wants_change_after_confirmation(message_text: str, conversation: dict[str, Any]) -> bool:
    lowered = sanitize_text(message_text).lower()
    current_date = conversation.get("requested_date")
    current_time = str(conversation.get("requested_time") or "")[:5]
    detected_date = extract_date(message_text)
    detected_time = extract_time(message_text)
    has_other_slot = bool(
        (detected_date and detected_date != current_date)
        or (detected_time and detected_time != current_time)
    )

    explicit_change = any(
        phrase in lowered
        for phrase in [
            "başka saat",
            "başka gün",
            "yeni saat",
            "yeni gün",
            "güncelle",
            "guncelle",
            "öne al",
            "one al",
            "ileri al",
            "kaydır",
            "kaydir",
            "yerine",
            "değiştirelim",
            "degistirelim",
        ]
    )
    tentative_change = has_other_slot and any(
        phrase in lowered
        for phrase in ["yapalım", "olur mu", "uyar mı", "uyar mi", "alabilir miyiz", "çekebilir miyiz", "cekebilir miyiz"]
    )
    return explicit_change or tentative_change or has_other_slot


def build_collected_booking_bits(conversation: dict[str, Any]) -> list[str]:
    bits: list[str] = []
    service = display_service_name(conversation.get("service"))
    requested_date = conversation.get("requested_date")
    requested_time = normalize_time_string(conversation.get("requested_time"))
    if service:
        bits.append(f"{service} için")
    if requested_date and requested_time:
        bits.append(f"{format_human_date(requested_date)} saat {requested_time}")
    elif requested_date:
        bits.append(format_human_date(requested_date))
    elif requested_time:
        bits.append(f"saat {requested_time}")
    return bits


def build_captured_ack_prefix(conversation: dict[str, Any]) -> str:
    bits = build_collected_booking_bits(conversation)
    if not bits:
        return ""
    return f"Not aldım; {' '.join(bits)}. "


def is_same_service_restatement(conversation: dict[str, Any], picked_service: str | None, message_text: str) -> bool:
    if not picked_service or sanitize_text(conversation.get("state") or "") != "collect_name":
        return False
    if extract_name(message_text, "collect_name"):
        return False
    current_service = sanitize_text(conversation.get("service") or "")
    current_match = match_service_catalog(current_service, current_service) if current_service else None
    current_display = (current_match or {}).get("display") or current_service
    return bool(current_display and current_display == picked_service)


def build_collect_name_request_reply(conversation: dict[str, Any], booking_label: str, ack_prefix: str, same_service_restatement: bool = False) -> str:
    if same_service_restatement and conversation.get("service"):
        service = display_service_name(conversation.get("service"))
        return f"Tamam, {service} için devam ediyoruz. {booking_label.capitalize()} kaydını açabilmem için adınızı ve soyadınızı yazar mısınız?"
    return f"{ack_prefix}{booking_label.capitalize()} kaydını açabilmem için önce adınız ve soyadınızı paylaşır mısınız?".strip()


def build_post_confirmation_followup_reply(conversation: dict[str, Any], message_text: str) -> str:
    lowered = sanitize_text(message_text).lower()
    summary = get_confirmed_appointment_summary(conversation)
    booking_label = get_booking_label(conversation)
    contact_text = build_contact_text()
    current_date = conversation.get("requested_date")
    current_time = str(conversation.get("requested_time") or "")[:5]
    detected_date = extract_date(message_text)
    detected_time = extract_time(message_text)
    mentions_other_slot = bool(
        (detected_date and detected_date != current_date)
        or (detected_time and detected_time != current_time)
    )

    if any(keyword in lowered for keyword in ["arayacak", "arar", "ulaş", "ulas", "geri dönüş", "geri donus"]):
        reply = f"Evet, {booking_label} kaydınız {summary} için planlı görünüyor."
        if conversation.get("phone"):
            reply += f" Ekibimiz gerekli olduğunda {conversation['phone']} numarası üzerinden sizinle iletişime geçecektir."
        else:
            reply += " Ekibimiz gerekli olduğunda sizinle iletişime geçecektir."
    elif any(keyword in lowered for keyword in ["onay", "kesin", "teyit", "tamam mı", "tamam mi"]) or lowered in {"?", "tamam", "peki", "ok", "teşekkürler", "tesekkurler"}:
        reply = f"{booking_label.capitalize()} kaydınız onaylı görünüyor: {summary}."
    else:
        reply = f"Sistemimizde onaylı {booking_label} kaydınız {summary} olarak görünüyor."

    if mentions_other_slot:
        reply += " Şu an kayıtlı tarih ve saat budur; değişiklik isterseniz sizi yetkili ekibimize yönlendirebilirim."
    else:
        reply += " Değişiklik veya iptal ihtiyacınız olursa sizi yetkili ekibimize yönlendirebilirim."

    if contact_text:
        reply += f" İhtiyaç olursa {contact_text} üzerinden bize ulaşabilirsiniz."
    return reply


def parse_reschedule_followup_request(text: str, base_date_value: Any, base_time_value: Any) -> tuple[str | None, str | None]:
    base_date = normalize_date_string(base_date_value)
    base_time = normalize_time_string(base_time_value)
    cleaned = sanitize_text(text)
    lowered = cleaned.lower()

    explicit_time = normalize_time_string(extract_time(text))
    explicit_date: str | None = None
    has_relative_followup_date = bool(re.search(r"evvelsi\s+gün\w*|evvelsi\s+gun\w*|evelsi\s+gün\w*|evelsi\s+gun\w*|öbür\s+gün\w*|obur\s+gun\w*|ertesi\s+gün\w*|ertesi\s+gun\w*|sonraki\s+gün\w*|sonraki\s+gun\w*|bir\s+sonraki\s+gün\w*|bir\s+sonraki\s+gun\w*|yarın\w*|yarin\w*|bugün\w*|bugun\w*", lowered))

    if has_relative_followup_date and base_date:
        try:
            base_dt = date.fromisoformat(base_date)
        except ValueError:
            base_dt = None
        if base_dt is not None:
            if re.search(r"evvelsi\s+gün\w*|evvelsi\s+gun\w*|evelsi\s+gün\w*|evelsi\s+gun\w*", lowered):
                explicit_date = (base_dt + timedelta(days=1)).isoformat()
            elif re.search(r"öbür\s+gün\w*|obur\s+gun\w*", lowered):
                explicit_date = (base_dt + timedelta(days=2)).isoformat()
            elif re.search(r"ertesi\s+gün\w*|ertesi\s+gun\w*|sonraki\s+gün\w*|sonraki\s+gun\w*|bir\s+sonraki\s+gün\w*|bir\s+sonraki\s+gun\w*|yarın\w*|yarin\w*", lowered):
                explicit_date = (base_dt + timedelta(days=1)).isoformat()
            elif re.search(r"bugün\w*|bugun\w*", lowered):
                explicit_date = base_dt.isoformat()

    if not explicit_date:
        explicit_date = extract_date(text)

    detected_date = normalize_date_string(explicit_date or base_date)
    detected_time = normalize_time_string(explicit_time or base_time)

    only_relative_date = bool(explicit_date) and not explicit_time
    only_time_update = bool(explicit_time) and not explicit_date

    if only_relative_date and base_time:
        detected_time = base_time
    if only_time_update and base_date:
        detected_date = base_date

    if not detected_date and has_date_cue(cleaned):
        detected_date = base_date
    if not detected_time and cleaned:
        short_followup = len(lowered.split()) <= 6 or any(token in lowered for token in ["olsa", "olsun", "alayim", "alalim", "cekelim", "kaydiralim", "degistirelim", "guncelleyelim"])
        if short_followup:
            detected_time = base_time

    return detected_date, detected_time


def try_reschedule_confirmed_appointment(conn: psycopg.Connection, conversation: dict[str, Any], message_text: str, username: str | None = None) -> tuple[bool, str, str | None]:
    memory = ensure_conversation_memory(conversation)
    base_date = memory.get("reschedule_requested_date") or conversation.get("requested_date")
    base_time = memory.get("reschedule_requested_time") or conversation.get("requested_time")
    detected_date, detected_time = parse_reschedule_followup_request(message_text, base_date, base_time)
    followup_open = memory.get("open_loop") == "reschedule_date_or_time_followup"
    has_existing_context = bool(base_date or base_time or conversation.get("requested_date") or conversation.get("requested_time"))
    lowered_message = sanitize_text(message_text).lower()
    if not detected_date and base_date:
        try:
            base_dt = date.fromisoformat(normalize_date_string(base_date))
        except Exception:
            base_dt = None
        if base_dt is not None:
            if re.search(r"evvelsi\s+gün\w*|evvelsi\s+gun\w*|evelsi\s+gün\w*|evelsi\s+gun\w*", lowered_message):
                detected_date = (base_dt + timedelta(days=1)).isoformat()
            elif re.search(r"öbür\s+gün\w*|obur\s+gun\w*", lowered_message):
                detected_date = (base_dt + timedelta(days=2)).isoformat()
            elif re.search(r"ertesi\s+gün\w*|ertesi\s+gun\w*|sonraki\s+gün\w*|sonraki\s+gun\w*|bir\s+sonraki\s+gün\w*|bir\s+sonraki\s+gun\w*|yarın\w*|yarin\w*", lowered_message):
                detected_date = (base_dt + timedelta(days=1)).isoformat()
            elif re.search(r"bugün\w*|bugun\w*", lowered_message):
                detected_date = base_dt.isoformat()
    if not detected_date or not detected_time:
        if not detected_date and base_date and re.search(r"gün|gun|yarın|yarin|bugün|bugun|öbür|obur|evvelsi|evelsi|ertesi|sonraki", lowered_message):
            try:
                base_dt = date.fromisoformat(normalize_date_string(base_date))
            except Exception:
                base_dt = None
            if base_dt is not None:
                if re.search(r"evvelsi|evelsi", lowered_message):
                    detected_date = (base_dt + timedelta(days=1)).isoformat()
                elif re.search(r"öbür|obur", lowered_message):
                    detected_date = (base_dt + timedelta(days=2)).isoformat()
                elif re.search(r"ertesi|sonraki|yarın|yarin", lowered_message):
                    detected_date = (base_dt + timedelta(days=1)).isoformat()
                elif re.search(r"bugün|bugun", lowered_message):
                    detected_date = base_dt.isoformat()
        if detected_date and not detected_time and (followup_open or has_existing_context):
            if base_time:
                detected_time = normalize_time_string(base_time)
            else:
                memory["reschedule_requested_date"] = detected_date
                memory["open_loop"] = "reschedule_date_or_time_followup"
                memory["last_bot_question_type"] = "time"
                return False, f"{format_human_date(detected_date)} icin hangi saati istersiniz? Ornek: 16:00", None
        if detected_time and not detected_date and (followup_open or has_existing_context):
            if base_date:
                detected_date = normalize_date_string(base_date)
            else:
                memory["reschedule_requested_time"] = detected_time
                memory["open_loop"] = "reschedule_date_or_time_followup"
                memory["last_bot_question_type"] = "date"
                return False, f"{detected_time} icin hangi gunu istersiniz? Ornek: 14.04.2026", None
        if has_existing_context:
            detected_date = normalize_date_string(detected_date or base_date or conversation.get("requested_date"))
            detected_time = normalize_time_string(detected_time or base_time or conversation.get("requested_time"))
        if not detected_date or not detected_time:
            return False, "Hangi gunu ya da saati degistirmek istediginizi yazabilirsiniz; diger bilgiyi mevcut kaydinizdan tamamlayayim.", None

    validation_error = validate_slot(detected_date, detected_time)
    if validation_error:
        return False, validation_error, None

    current_date = normalize_date_string(conversation.get("requested_date"))
    current_time = normalize_time_string(conversation.get("requested_time"))
    if detected_date == current_date and detected_time == current_time:
        booking_label = get_booking_label(conversation)
        return True, f"{booking_label.capitalize()} kaydiniz zaten {format_human_date(detected_date)} saat {detected_time} icin onayli gorunuyor.", "confirmed_followup"

    slot_conflict = find_existing_appointment(conn, detected_date, detected_time, conversation.get("service"))
    if slot_conflict:
        memory["reschedule_requested_date"] = detected_date
        memory["reschedule_requested_time"] = detected_time
        memory["open_loop"] = "reschedule_date_or_time_followup"
        memory["last_bot_question_type"] = "date"
        alternatives = suggest_alternatives(conn, detected_date, detected_time, conversation.get("service"))
        if alternatives:
            alt_text = ", ".join(alternatives)
            return False, f"{format_human_date(detected_date)} icin {detected_time} dolu gorunuyor. Uygun saatler: {alt_text}. Isterseniz bunlardan birini ya da baska bir gunu yazabilirsiniz.", None
        next_days = find_next_available_days(conn, detected_date, service_name=conversation.get("service"))
        return False, build_no_availability_reply(detected_date, next_days), None

    with conn.cursor() as cur:
        cur.execute(
            """
            WITH target AS (
                SELECT id
                FROM appointments
                WHERE instagram_user_id = %s
                  AND status IN ('confirmed', 'preconsultation')
                ORDER BY
                  CASE
                    WHEN %s::date IS NOT NULL AND %s::time IS NOT NULL
                     AND appointment_date = %s::date AND appointment_time = %s::time THEN 0
                    ELSE 1
                  END,
                  updated_at DESC,
                  appointment_date DESC,
                  appointment_time DESC,
                  id DESC
                LIMIT 1
            )
            UPDATE appointments AS a
            SET appointment_date = %s::date,
                appointment_time = %s::time,
                instagram_username = COALESCE(%s, a.instagram_username),
                full_name = COALESCE(%s, a.full_name),
                phone = COALESCE(%s, a.phone),
                service = COALESCE(%s, a.service),
                updated_at = NOW()
            FROM target
            WHERE a.id = target.id
            RETURNING a.id, a.status
            """,
            (
                conversation.get("instagram_user_id"),
                normalize_date_string(conversation.get("requested_date")) or detected_date,
                normalize_time_string(conversation.get("requested_time")) or detected_time,
                normalize_date_string(conversation.get("requested_date")) or detected_date,
                normalize_time_string(conversation.get("requested_time")) or detected_time,
                detected_date,
                detected_time,
                username or conversation.get("instagram_username"),
                conversation.get("full_name"),
                conversation.get("phone"),
                conversation.get("service"),
            ),
        )
        updated = cur.fetchone()

    if not updated:
        conn.rollback()
        return False, "Mevcut kaydinizi guncellerken bir sorun olustu. Lutfen tekrar dener misiniz?", None

    conversation["requested_date"] = detected_date
    conversation["requested_time"] = detected_time
    conversation["appointment_status"] = "confirmed"
    conversation["state"] = "completed"
    conversation["assigned_human"] = False
    memory["reschedule_requested_date"] = None
    memory["reschedule_requested_time"] = None
    memory["open_loop"] = "completed"

    if is_live_crm_configured():
        try:
            booking_kind = get_booking_kind(conversation)
            if booking_kind == "appointment":
                live_crm_upsert_appointment(conversation)
            else:
                live_crm_upsert_preconsultation(conversation)
                live_crm_ensure_task_for_conversation(dict(conversation))
        except Exception:
            conn.rollback()
            raise

    customer = upsert_customer_from_conversation(conn, conversation)
    if customer:
        schedule_customer_automation_events(
            conn,
            int(customer["id"]),
            customer.get("sector"),
            base_time=datetime.fromisoformat(f"{detected_date}T{detected_time}:00").replace(tzinfo=TZ),
        )

    conn.commit()
    booking_label = get_booking_label(conversation)
    return True, f"{format_human_date(detected_date)} saat {detected_time} icin {booking_label} kaydiniz guncellendi.", "appointment_rescheduled"


def build_open_loop_resume_reply(conversation: dict[str, Any]) -> str | None:
    memory = ensure_conversation_memory(conversation)
    question_type = memory.get("last_bot_question_type")
    service = sanitize_text(conversation.get("service") or "")
    service_meta = match_service_catalog(service, service) if service else None

    if question_type == "priority" and service_meta:
        return build_contextual_service_followup(service_meta, conversation)
    if question_type == "dm_issue":
        return "Tamam. DM tarafında sizi en çok geç dönüş mü zorluyor, yoksa tekrar eden sorular mı?"
    if question_type == "message_volume":
        if memory.get("customer_sector") == "real_estate":
            return "Tamam. Gün içinde yaklaşık kaç kişi yazıyor?"
        return "Tamam. Gün içinde ortalama kaç kişi yazıyor?"
    if question_type == "service":
        return "Tamam. Size doğru yön verebilmem için hangi hizmetle ilgilendiğinizi yazar mısınız?"
    if question_type == "offer_response" and memory.get("pending_offer") == "preconsultation_offer":
        return "Tamam. Uygunsanız kısa bir ön görüşme planlayabiliriz."
    return None


def build_next_step_reply(conn: psycopg.Connection, conversation: dict[str, Any]) -> str:
    booking_label = get_booking_label(conversation)
    ack_prefix = build_captured_ack_prefix(conversation)
    open_loop_reply = build_open_loop_resume_reply(conversation)
    if open_loop_reply and conversation.get("state") == "collect_service":
        return open_loop_reply
    if not conversation.get("service") and get_booking_kind(conversation) != "preconsultation":
        conversation["state"] = "collect_service"
        return f"{ack_prefix}Size doğru yönlendirme yapabilmem için hangi hizmetle ilgilendiğinizi yazar mısınız?".strip()

    if not conversation.get("full_name"):
        conversation["state"] = "collect_name"
        return f"{ack_prefix}{booking_label.capitalize()} kaydını açabilmem için önce ad soyadınızı paylaşır mısınız?".strip()

    if not conversation.get("phone"):
        conversation["state"] = "collect_phone"
        return f"{ack_prefix}Devam edebilmem için telefon numaranızı da paylaşır mısınız?".strip()

    if not conversation.get("requested_date"):
        conversation["state"] = "collect_date"
        return f"{ack_prefix}Size uygun gün nedir? Çalışma saatlerimiz {build_working_hours_text()} arası.".strip()

    if not conversation.get("preferred_period"):
        conversation["state"] = "collect_period"
        return "Sabah mı, öğleden sonra mı daha uygunsunuz?"

    if not conversation.get("requested_time"):
        conversation["state"] = "collect_time"
        open_slots = get_available_slots_for_date(conn, conversation["requested_date"], conversation.get("service"))
        filtered_slots = filter_slots_by_period(open_slots, conversation.get("preferred_period"))
        if filtered_slots:
            return build_availability_reply(conversation["requested_date"], filtered_slots, period=conversation.get("preferred_period"))
        if open_slots:
            return f"{format_human_date(conversation['requested_date'])} için {get_period_label(conversation.get('preferred_period'))} tarafında boşluk görünmüyor. İsterseniz diğer zaman dilimine de bakabilirim."
        next_days = find_next_available_days(conn, conversation["requested_date"], service_name=conversation.get("service"))
        return build_no_availability_reply(conversation["requested_date"], next_days)

    return "Devam edebilmem için eksik kalan bilgiyi yazabilirsiniz."


def should_reset_stale_conversation(conversation: dict[str, Any], message_text: str) -> bool:
    updated_at = conversation.get("updated_at")
    if not updated_at:
        return False

    if isinstance(updated_at, datetime):
        updated_dt = updated_at
    else:
        try:
            updated_dt = datetime.fromisoformat(str(updated_at))
        except ValueError:
            return False

    state = sanitize_text(conversation.get("state") or "")
    now = datetime.now(TZ)
    is_stale = now - updated_dt >= timedelta(minutes=STALE_CONVERSATION_MINUTES)
    lowered = sanitize_text(message_text).lower()
    matched_services = match_service_candidates(message_text, conversation.get("service"))
    info_restart_cue = any([
        is_simple_greeting(message_text),
        is_service_overview_question(message_text),
        is_working_schedule_question(message_text),
        is_price_question(message_text),
        bool(match_faq_response(message_text)),
        bool(match_objection_type(message_text)),
        is_service_advice_request(message_text),
        is_comparison_request(message_text, matched_services),
        is_owner_check_message(message_text),
        is_assistant_identity_question(message_text),
        is_presence_check_message(message_text),
        is_smalltalk_message(message_text),
        "bilgi" in lowered,
    ])

    if state in {"completed", "human_handoff"}:
        return is_simple_greeting(message_text) or (is_stale and info_restart_cue)

    if not is_stale:
        return False
    if state not in {"collect_service", "collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        return False
    if extract_phone(message_text) or extract_date(message_text) or extract_time_for_state(message_text, state):
        return False
    if message_shows_booking_intent(message_text, {}):
        return False
    return info_restart_cue


def sanitize_conversation_state(conversation: dict[str, Any]) -> None:
    memory = ensure_conversation_memory(conversation)
    service = sanitize_text(conversation.get("service") or "")
    if service:
        matched_catalog = match_service_catalog(service, service)
        if matched_catalog:
            conversation["service"] = matched_catalog["display"]
        elif is_invalid_service_candidate(service):
            conversation["service"] = None

    conversation["booking_kind"] = normalize_booking_kind(conversation.get("booking_kind"))
    preferred_period = sanitize_text(conversation.get("preferred_period") or "").lower()
    conversation["preferred_period"] = preferred_period if preferred_period in {"morning", "afternoon"} else None

    requested_date = conversation.get("requested_date")
    if requested_date:
        normalized_date = normalize_date_string(requested_date)
        if not normalized_date or date.fromisoformat(normalized_date) < datetime.now(TZ).date():
            conversation["requested_date"] = None
            conversation["requested_time"] = None
            conversation["preferred_period"] = None
        else:
            conversation["requested_date"] = normalized_date

    if conversation.get("requested_time") and not conversation.get("requested_date"):
        conversation["requested_time"] = None
    elif conversation.get("requested_time"):
        conversation["requested_time"] = normalize_time_string(conversation.get("requested_time"))
        if not conversation.get("preferred_period"):
            conversation["preferred_period"] = infer_period_from_time(conversation.get("requested_time"))

    if conversation.get("preferred_period") and not conversation.get("requested_date"):
        conversation["preferred_period"] = None

    if not conversation.get("service") and get_booking_kind(conversation) != "preconsultation" and conversation.get("state") in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        conversation["state"] = "collect_service"
    elif not conversation.get("full_name") and conversation.get("state") in {"collect_phone", "collect_date", "collect_period", "collect_time"}:
        conversation["state"] = "collect_name"
    elif not conversation.get("phone") and conversation.get("state") in {"collect_date", "collect_period", "collect_time"}:
        conversation["state"] = "collect_phone"
    elif not conversation.get("requested_date") and conversation.get("state") in {"collect_period", "collect_time"}:
        conversation["state"] = "collect_date"
    elif not conversation.get("preferred_period") and conversation.get("state") == "collect_time":
        conversation["state"] = "collect_period"

    if conversation.get("state") in {"completed", "human_handoff"}:
        memory["pending_offer"] = None
    sync_conversation_memory_summary(conversation)


def titlecase_name(value: str | None) -> str | None:
    if not value:
        return None
    clean = sanitize_text(re.sub(r"[^a-zA-ZçğıöşüÇĞİÖŞÜ\s]", "", value))
    if not clean:
        return None
    words = [w for w in clean.split() if w]
    if not 1 <= len(words) <= 4:
        return None
    if any(len(w) < 2 for w in words):
        return None
    if any(w.lower() in NON_NAME_WORDS for w in words):
        return None
    return " ".join(word[:1].upper() + word[1:].lower() for word in words)


def extract_name(text: str, state: str) -> str | None:
    normalized = sanitize_text(text)
    lowered = normalized.lower()
    explicit_prefixes = [
        'benim adim soyadim ',
        'adim soyadim ',
        'benim adim ',
        'adim ',
        'ismim de ',
        'ismim ',
        'isim soyisim ',
        'musteri adi ',
    ]
    for prefix in explicit_prefixes:
        idx = lowered.find(prefix)
        if idx != -1:
            tail = normalized[idx + len(prefix):]
            tail = re.split(r'[,.!?]| telefon| tel no| numaram| numara| işlet| islet| sektör| sektor| kuaför| kuafor| randevu| ön görüş| on gorus', tail, maxsplit=1, flags=re.IGNORECASE)[0]
            candidate = titlecase_name(sanitize_text(tail).strip(' :-'))
            if candidate and len(candidate.split()) <= 4:
                return candidate
    if '?' in text or is_service_overview_question(text) or is_price_question(text) or match_faq_response(text):
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
    clean = sanitize_text(re.sub(r'[^a-zA-ZçğıöşüÇĞİÖŞÜ\s]', '', text))
    words = clean.split()
    if state != 'collect_name':
        return None
    if 1 <= len(words) <= 3 and not any(len(w) < 2 for w in words) and not any(w.lower() in NON_NAME_WORDS for w in words):
        return titlecase_name(clean)
    return None


def is_invalid_name_attempt(text: str, state: str) -> bool:
    if state != "collect_name":
        return False
    if extract_name(text, state):
        return False
    if match_service_candidates(text, None):
        return False
    if "?" in sanitize_text(text):
        return False
    clean = sanitize_text(re.sub(r"[^a-zA-ZçğıöşüÇĞİÖŞÜ\s]", "", text))
    words = clean.split()
    return not words or any(len(word) < 2 for word in words)


def canonical_phone(value: Any) -> str | None:
    if not value:
        return None
    digits = re.sub(r"\D", "", str(value))
    if digits.startswith("90") and len(digits) == 12:
        return "+" + digits
    if digits.startswith("0") and len(digits) == 11:
        return "+90" + digits[1:]
    if len(digits) == 10:
        return "+90" + digits
    return None


def extract_phone(text: str) -> str | None:
    match = PHONE_PATTERN.search(text)
    if not match:
        return None
    digits = re.sub(r"\D", "", match.group(0))
    if digits.startswith("90") and len(digits) == 12:
        return f"+{digits}"
    if digits.startswith("0") and len(digits) == 11:
        return f"+90{digits[1:]}"
    if len(digits) == 10:
        return f"+90{digits}"
    return None


def extract_time(text: str) -> str | None:
    for match in TIME_PATTERN.finditer(text):
        raw = match.group(0)
        context_before = text[max(0, match.start() - 12):match.start()].lower()
        context_after = text[match.end():match.end() + 24].lower()
        if "." in raw:
            has_explicit_time_cue = re.search(r"(saat|saatte|saati)\s*$", context_before)
            has_change_cue = re.search(r"(a|e|ya|ye)\s*(al|aldir|aldır|cek|çek|kaydir|kaydır|tas|taşı|degis|değiş|guncelle|güncelle)", context_after)
            if not has_explicit_time_cue and not has_change_cue:
                continue
        hour = int(match.group(1))
        minute = int(match.group(2))
        return f"{hour:02d}:{minute:02d}"
    match = HOUR_WORD_PATTERN.search(text)
    if match:
        hour = int(match.group(1))
        return f"{hour:02d}:00"
    return None


def extract_time_for_state(text: str, state: str | None = None) -> str | None:
    cleaned = sanitize_text(text)
    if not cleaned:
        return None

    state = sanitize_text(state or "")
    standalone = STANDALONE_TIME_PATTERN.match(cleaned)
    numeric_date_like = PURE_NUMERIC_DATE_PATTERN.match(cleaned) is not None
    contains_numeric_date = re.search(r"\b\d{1,2}[./-]\d{1,2}(?:[./-]\d{2,4})?\b", cleaned) is not None
    explicit_time_context = has_date_cue(cleaned) or any(cue in cleaned.lower() for cue in ["saat", "uygun", "müsait", "musait", "randevu", "görüşme", "gorusme", "kaçta", "kacta", "?"])

    if is_voice_duration_placeholder_message(cleaned) and state not in {"collect_period", "collect_time"}:
        return None

    if standalone and state in {"collect_period", "collect_time"} and not numeric_date_like:
        return f"{int(standalone.group(1)):02d}:{int(standalone.group(2)):02d}"

    if state not in {"collect_date", "collect_period", "collect_time"} and not explicit_time_context:
        return None

    direct = extract_time(text)
    if direct:
        return direct

    if state not in {"collect_date", "collect_period", "collect_time"} and not has_date_cue(cleaned):
        return None
    if numeric_date_like and state not in {"collect_period", "collect_time"}:
        return None

    dotted = TIME_PATTERN.search(cleaned)
    if dotted and contains_numeric_date and state not in {"collect_period", "collect_time"}:
        return None
    if dotted and (has_date_cue(cleaned) or state in {"collect_period", "collect_time"}):
        return f"{int(dotted.group(1)):02d}:{int(dotted.group(2)):02d}"
    return None


def should_ignore_llm_booking_datetime_from_phone_message(
    message_text: str,
    state: str | None,
    detected_phone: str | None,
    llm_data: dict[str, Any] | None,
) -> bool:
    if sanitize_text(state or "") != "collect_phone" or not detected_phone or not isinstance(llm_data, dict):
        return False
    if not (llm_data.get("requested_date") or llm_data.get("requested_time")):
        return False
    if extract_date(message_text) or extract_time_for_state(message_text, state):
        return False
    return True


def force_ai_first_booking_continuation(
    decision: dict[str, Any],
    conversation: dict[str, Any],
    *,
    state_before_update: str | None,
    extracted_name: str | None,
    detected_phone: str | None,
    detected_time: str | None = None,
) -> None:
    state = sanitize_text(state_before_update or "")
    service = sanitize_text(conversation.get("service") or "")
    has_name = bool(titlecase_name(extracted_name) or titlecase_name(conversation.get("full_name")))
    has_phone = bool(canonical_phone(detected_phone) or canonical_phone(conversation.get("phone")))
    if state == "collect_name" and service and titlecase_name(extracted_name):
        decision["booking_intent"] = True
        decision["intent"] = "booking_name_collected"
        decision["should_reply"] = True
        decision["missing_fields"] = ["phone", "requested_date", "requested_time"]
        decision["reply_text"] = "Teşekkürler. Ön görüşme kaydını tamamlamak için telefon numaranızı paylaşır mısınız?"
    elif state == "collect_phone" and service and has_name and has_phone:
        decision["booking_intent"] = True
        decision["intent"] = "booking_phone_collected"
        decision["should_reply"] = True
        decision["missing_fields"] = ["requested_date", "requested_time"]
        decision["reply_text"] = "Telefon numaranızı aldım. Uygun saatleri kontrol ediyorum."
    elif state in {"collect_time", "collect_period"} and service and has_name and has_phone and normalize_time_string(detected_time):
        decision["booking_intent"] = True
        decision["intent"] = "booking_time_collected"
        decision["should_reply"] = True
        decision["missing_fields"] = ["requested_time"]
        decision["reply_text"] = "Saati aldım. Uygunluğu kontrol ediyorum."


def is_delivery_duration_followup(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    has_duration_amount = re.search(r"\b\d+\s*(?:is\s*)?(?:gun\w*|gün\w*|hafta\w*|ay\w*)\b", lowered)
    if not has_duration_amount:
        return False
    duration_cues = [
        "cik",
        "çık",
        "uz",
        "sur",
        "sür",
        "sure",
        "süre",
        "teslim",
        "termin",
        "tamamlan",
        "ne kadar",
    ]
    return any(cue in lowered for cue in duration_cues)


def extract_duration_phrase(text: str) -> str | None:
    lowered = sanitize_text(text).lower()
    match = re.search(r"\b(\d+)\s*((?:is\s*)?(?:gun\w*|gün\w*|hafta\w*|ay\w*))\b", lowered)
    if not match:
        return None
    amount = match.group(1)
    unit = match.group(2).strip()
    if unit.startswith("gun") or unit.startswith("gün"):
        unit = "gün"
    elif unit.startswith("hafta"):
        unit = "hafta"
    elif unit.startswith("ay"):
        unit = "ay"
    elif "gun" in unit or "gün" in unit:
        unit = "iş günü"
    return f"{amount} {unit}"


def has_date_cue(text: str) -> bool:
    if NUMERIC_RANGE_ANSWER_PATTERN.match(sanitize_text(text)):
        return False
    if is_delivery_duration_followup(text):
        return False
    return DATE_CUE_PATTERN.search(text) is not None


def extract_date(text: str) -> str | None:
    if not has_date_cue(text):
        return None

    lowered = text.lower()
    today = datetime.now(TZ).date()

    month_named = re.search(
        r"(\d1,2})\s+(ocak|şubat|subat|mart|nisan|mayıs|mayis|haziran|temmuz|ağustos|agustos|eylül|eylul|ekim|kasım|kasim|aralık|aralik)(?:\s+(\d2,4}))?",
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

    if re.search(r"evvelsi\s+gün\w*|evvelsi\s+gun\w*|evelsi\s+gün\w*|evelsi\s+gun\w*", lowered):
        return (today + timedelta(days=3)).isoformat()
    if re.search(r"öbür\s+gün\w*|obur\s+gun\w*", lowered):
        return (today + timedelta(days=2)).isoformat()
    if re.search(r"ertesi\s+gün\w*|ertesi\s+gun\w*|sonraki\s+gün\w*|sonraki\s+gun\w*|bir\s+sonraki\s+gün\w*|bir\s+sonraki\s+gun\w*", lowered):
        return (today + timedelta(days=1)).isoformat()
    if re.search(r"yarın\w*|yarin\w*", lowered):
        return (today + timedelta(days=1)).isoformat()
    if re.search(r"bugün\w*|bugun\w*", lowered):
        return today.isoformat()

    explicit = re.search(r"\b(\d{1,2})[./-](\d{1,2})(?:[./-](\d{2,4}))?\b", lowered)
    if explicit:
        day = int(explicit.group(1))
        month = int(explicit.group(2))
        year_raw = explicit.group(3)
        year = int(year_raw) if year_raw else today.year
        if year_raw and len(year_raw) == 2:
            year += 2000
        try:
            parsed = date(year, month, day)
            if not year_raw and parsed < today:
                parsed = date(today.year + 1, month, day)
            return parsed.isoformat()
        except ValueError:
            pass

    for word, weekday in WEEKDAY_MAP.items():
        if re.search(rf"\b{re.escape(word)}\b", lowered):
            delta = (weekday - today.weekday()) % 7
            if delta == 0:
                delta = 7
            return (today + timedelta(days=delta)).isoformat()

    results = search_dates(
        text,
        languages=["tr"],
        settings={
            "RELATIVE_BASE": datetime.now(TZ),
            "PREFER_DATES_FROM": "future",
            "TIMEZONE": TIMEZONE,
            "DATE_ORDER": "DMY",
        },
    )
    if not results:
        return None
    for source_text, dt in results:
        if isinstance(dt, datetime):
            snippet = source_text.lower()
            if any(ch.isdigit() for ch in snippet) or has_date_cue(snippet):
                return dt.date().isoformat()
    return None


def normalize_date_string(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    try:
        return date.fromisoformat(str(value)).isoformat()
    except ValueError:
        return None


def normalize_time_string(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.time().strftime("%H:%M")
    if isinstance(value, time):
        return value.strftime("%H:%M")
    try:
        parsed = time.fromisoformat(str(value))
        return parsed.strftime("%H:%M")
    except ValueError:
        match = TIME_PATTERN.search(str(value))
        if match:
            return f"{int(match.group(1)):02d}:{int(match.group(2)):02d}"
    return None


def match_service_candidates(text: str | None, fallback_service: str | None = None) -> list[dict[str, Any]]:
    combined = sanitize_text(" ".join(part for part in [text or "", fallback_service or ""] if part)).lower()
    if not combined:
        return []

    scored: list[tuple[int, dict[str, Any]]] = []
    for service in DOEL_SERVICE_CATALOG:
        score = 0
        display = sanitize_text(service.get("display") or "").lower()
        if display and display in combined:
            score += 8
        for keyword in service.get("keywords", []):
            normalized_keyword = sanitize_text(keyword).lower()
            if normalized_keyword and normalized_keyword in combined:
                score += max(2, len(normalized_keyword.split()))
        if score:
            scored.append((score, service))

    scored.sort(key=lambda item: (-item[0], item[1].get("display") or ""))
    return [service for _, service in scored]


def match_service_catalog(text: str | None, fallback_service: str | None = None) -> dict[str, Any] | None:
    candidates = match_service_candidates(text, fallback_service)
    return candidates[0] if candidates else None


def display_service_name(value: str | None) -> str:
    cleaned = sanitize_text(value or "")
    if not cleaned:
        return ""
    matched = match_service_catalog(cleaned, cleaned)
    return (matched or {}).get("display") or (value or "").strip()


def is_service_overview_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if any(keyword in lowered for keyword in SERVICE_OVERVIEW_KEYWORDS):
        return True
    detail_cues = ["her biri", "herbir", "tek tek", "ayri ayri", "ayrı ayrı", "tum hizmet", "tüm hizmet", "hepsi icin", "hepsi için"]
    service_cues = ["hizmet", "hizmetler", "web", "otomasyon", "reklam", "sosyal medya", "bilgi"]
    if any(cue in lowered for cue in detail_cues) and any(cue in lowered for cue in service_cues):
        return True
    if any(token in lowered for token in ["detayli bilgi", "detaylı bilgi", "hakkinda bilgi", "hakkında bilgi"]) and any(cue in lowered for cue in detail_cues):
        return True
    return False


def is_price_question(text: str) -> bool:
    lowered = text.lower()
    if is_delivery_time_question(text):
        return False
    return any(keyword in lowered for keyword in PRICE_KEYWORDS)


def is_delivery_time_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if is_delivery_duration_followup(text):
        return True
    delivery_cues = [
        "teslim", "teslimat", "termin", "hazir olur", "hazır olur", "ne zaman hazir",
        "ne zaman hazır", "kac gunde", "kaç günde", "kac gun", "kaç gün",
        "ne kadar surer", "ne kadar sürer", "sure ne", "süre ne", "suresi", "süresi",
    ]
    service_cues = [
        "web", "website", "websitesi", "site", "tasarim", "tasarım", "landing",
        "otomasyon", "reklam", "sosyal medya", "hizmet",
    ]
    return any(cue in lowered for cue in delivery_cues) and any(cue in lowered for cue in service_cues)


def extract_budget_amount(text: str) -> int | None:
    lowered = sanitize_text(text).lower().replace("₺", " tl ")
    scaled = re.search(r"\b(\d+(?:[\.,]\d+)?)\s*(bin|k)\b", lowered)
    if scaled:
        raw = scaled.group(1).replace(",", ".")
        try:
            return int(float(raw) * 1000)
        except ValueError:
            return None

    tl_match = re.search(r"\b(\d{4,6})\b", lowered)
    if tl_match:
        try:
            return int(tl_match.group(1))
        except ValueError:
            return None
    return None


def format_try_amount(amount: int | None) -> str | None:
    if amount is None:
        return None
    return f"{amount:,}".replace(",", ".") + " ₺"


def is_price_followup_message(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    if sanitize_text(str(llm_data.get("sub_intent") or "")).lower() == "price_followup":
        return True
    return any(keyword in lowered for keyword in PRICE_FOLLOWUP_KEYWORDS)


def is_budget_limit_message(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    sub_intent = sanitize_text(str(llm_data.get("sub_intent") or "")).lower()
    if sub_intent == "budget_limit":
        return True
    if any(keyword in lowered for keyword in BUDGET_LIMIT_KEYWORDS):
        return True
    budget_amount = extract_budget_amount(text)
    return budget_amount is not None and any(token in lowered for token in ["olur mu", "yetmiyor", "veremem", "çıkamam", "cikamam", "alırım", "alirim", "alıcam", "alicam"])


def is_purchase_if_discounted_message(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    sub_intent = sanitize_text(str(llm_data.get("sub_intent") or "")).lower()
    if sub_intent == "purchase_if_discounted":
        return True
    return any(keyword in lowered for keyword in PURCHASE_IF_DISCOUNTED_KEYWORDS)


def is_price_negotiation_message(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    sub_intent = sanitize_text(str(llm_data.get("sub_intent") or "")).lower()
    if sub_intent in {"price_negotiation", "budget_limit", "purchase_if_discounted"}:
        return True
    if sanitize_text(str(llm_data.get("objection_type") or "")).lower() == "price":
        return True
    if match_objection_type(text) == "price":
        return True
    if is_budget_limit_message(text, llm_data) or is_purchase_if_discounted_message(text, llm_data):
        return True
    return any(keyword in lowered for keyword in PRICE_NEGOTIATION_KEYWORDS)


def match_faq_response(text: str) -> str | None:
    lowered = text.lower()
    for key, keywords in FAQ_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return FAQ_RESPONSES[key]
    return None


def is_greeting_like_message(text: str) -> bool:
    cleaned = sanitize_text(text)
    lowered = cleaned.lower().strip(".!?,")
    if not lowered:
        return False
    if any(marker in cleaned for marker in ["👋", "🙋", "🤝"]) and len(cleaned) <= 6:
        return True
    compact = re.sub(r"[^a-z0-9]+", "", lowered)
    if compact in {"salamunaleykum", "selamunaleykum", "selaminaleykum", "selamunalekum", "aleykumselam", "aleykumselamlar", "sa"}:
        return True
    if lowered in GREETING_MESSAGES:
        return True
    if lowered in {"nerhaba", "meraba", "merhba", "mrb", "mrhb", "slm", "selm"}:
        return True
    if lowered.startswith(("merhab", "selam", "esenlik")) and len(lowered.split()) <= 3:
        return True
    return False


def is_invalid_service_candidate(text: str | None) -> bool:
    cleaned = sanitize_text(text or "")
    if not cleaned:
        return True
    if match_service_catalog(cleaned, cleaned):
        return False

    lowered = cleaned.lower()
    if is_greeting_like_message(cleaned) or is_low_signal_message(cleaned) or is_presence_check_message(cleaned):
        return True
    if is_service_overview_question(cleaned) or is_price_question(cleaned) or match_faq_response(cleaned):
        return True
    if is_service_advice_request(cleaned) or is_comparison_request(cleaned, match_service_candidates(cleaned, cleaned)):
        return True
    if extract_name(cleaned, "collect_name") or extract_phone(cleaned) or extract_time(cleaned) or extract_date(cleaned):
        return True
    if any(keyword in lowered for keyword in [*HUMAN_KEYWORDS, *AVAILABILITY_KEYWORDS, *CANCEL_KEYWORDS]):
        return True
    if "?" in cleaned:
        return True
    return len(cleaned.split()) <= 3


def build_services_overview_reply() -> str:
    return (
        "Merhaba 👋\n"
        "Web Sitesi kurulumu • Mesaj Otomasyonu • Reklamcılık\n"
        "Hangi hizmetimiz hakkında bilgi almak istersiniz?"
    )


def build_detailed_services_overview_reply() -> str:
    return (
        "Elbette. Kısaca özetleyeyim:\n\n"
        "Web Tasarım: Markanıza özel, mobil uyumlu ve dönüşüm odaklı siteler.\n"
        "Otomasyon: Müşteri mesajlarını karşılayan, bilgi veren ve görüşme planlayan AI sistemleri.\n"
        "Reklam: Yeni başvuru kazanmanızı sağlayan sponsorlu reklamlar.\n\n"
        "Size en uygun hizmeti netleştirmek için çok kısa bir telefon görüşmesi planlayabiliriz. Hangi gün ve saat uygundur?"
    )


def is_working_schedule_question(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in WORKING_SCHEDULE_KEYWORDS)


def is_company_background_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    patterns = [
        "ne zamandir",
        "ne zamandır",
        "kac yildir",
        "kaç yıldır",
        "sektorde",
        "sektörde",
        "deneyiminiz",
        "tecrubeniz",
        "tecrübeniz",
        "hakkinizda",
        "hakkınızda",
        "kimsiniz",
        "ne is yapiyorsunuz",
        "ne iş yapıyorsunuz",
        "ajans misiniz",
        "ajans mısınız",
    ]
    return any(pattern in lowered for pattern in patterns)


def build_company_background_reply() -> str:
    return (
        f"{BUSINESS_NAME} olarak web tasarım, otomasyon, reklam yönetimi ve sosyal medya alanlarında markalara profesyonel destek veriyoruz. "
        "Dijital tarafta işleri daha düzenli, hızlı ve verimli hale getirmeye odaklanıyoruz."
    )


def build_working_schedule_reply() -> str:
    return f"Çalışma günlerimiz {build_working_hours_text()} arasıdır."


def build_combined_intro_reply(*, include_identity: bool = True, include_services: bool = True, include_schedule: bool = True) -> str:
    parts: list[str] = []
    if include_identity:
        parts.append(f"Merhaba, {BUSINESS_NAME} tarafındayız.")
    if include_services:
        parts.append("Web tasarım, otomasyon, reklam ve sosyal medya gibi alanlarda destek veriyoruz.")
    if include_schedule:
        parts.append(build_working_schedule_reply())
    parts.append("İhtiyacınızı kısaca yazın; sizi doğru taraftan yönlendirelim.")
    return " ".join(parts)


def build_service_context_intro(service: dict[str, Any]) -> str:
    slug = service.get("slug") or ""
    if slug == "web-tasarim":
        return "İşletmenizin değerini yansıtan, yeni müşteriler kazanmanızı sağlayacak özel web siteleri kuruyoruz."
    if slug == "otomasyon-ai":
        return "Gelen mesajları anında yanıtlayan ve sizi bekletmeden görüşme planlayan DOEL AI sistemimizi sisteme entegre ediyoruz."
    if slug == "performans-pazarlama":
        return "Doğru hedef kitleye çıkılan sponsorlu reklamlarla işletmenize yeni başvurular ve satışlar kazandırıyor, bunu yaparken dilereseniz AI mesaj otomasyonu ile süreci destekliyoruz."
    if slug == "sosyal-medya-yonetimi":
        return "Sosyal medya görünürlüğünüzü profesyonelce yönetip marka değerinizi artırıyoruz."
    if slug == "marka-stratejisi":
        return "Markanızın büyüme hedeflerini netleştiriyoruz."
    if slug == "kreatif-produksiyon":
        return "İlgi çekici kreatif içerikler üretiyoruz."
    return "Bu alanda işinize değer katacak profesyonel çözümler üretiyoruz."


def build_service_info_reply(service: dict[str, Any], conversation: dict[str, Any] | None = None) -> str:
    suffix = _price_reply_suffix(service, conversation)
    return f"{build_service_context_intro(service)} {suffix}"


def is_recurring_service(service: dict[str, Any]) -> bool:
    return (service.get("slug") or "") in {"otomasyon-ai", "performans-pazarlama", "sosyal-medya-yonetimi"}


def _price_reply_suffix(service: dict[str, Any], conversation: dict[str, Any] | None = None) -> str:
    if conversation and conversation.get("booking_kind"):
        return "Detayları ön görüşmede konuşalım mı?"
    return build_contextual_service_followup(service, conversation)


def _price_question_suffix(conversation: dict[str, Any] | None = None) -> str:
    if conversation and conversation.get("booking_kind"):
        return "Detayları ön görüşmede netleştirebiliriz."
    return "Kapsam ve hedefe göre netleşir. İsterseniz ihtiyacınıza uygun yapıyı kısa bir ön görüşmede netleştirebiliriz."


def build_price_question_reply(service: dict[str, Any], conversation: dict[str, Any] | None = None) -> str:
    suffix = _price_question_suffix(conversation)
    if str(service.get("price") or "").lower().startswith("özel"):
        return f"{service['display']} için fiyat ihtiyaca göre değişiyor. {suffix}"
    if is_recurring_service(service):
        return f"{service['display']} {service['price']}'den başlıyor ({service['price_note']}). {suffix}"
    return f"{service['display']} {service['price']} ({service['price_note']}). {suffix}"


def build_delivery_time_reply(service: dict[str, Any] | None = None) -> str:
    if service:
        display = str(service.get("display") or "Bu hizmet").strip()
        delivery_time = str(service.get("delivery_time") or "").strip()
        if delivery_time:
            return f"{display} için tahmini teslim süresi genelde {delivery_time}. Kapsam, entegrasyon sayısı ve hazır içerikler süreyi değiştirebilir."
        return f"{display} için tahmini teslim süresi kapsam netleşince doğru aralıkla paylaşılır. En doğru süre için ihtiyacı kısaca görmemiz gerekir."
    return "Tahmini teslim süresi hizmetin kapsamına göre değişir. Kapsam netleşince doğru tarih aralığını paylaşabiliriz."


def build_delivery_duration_followup_reply(service: dict[str, Any] | None, message_text: str) -> str:
    display = str((service or {}).get("display") or "Bu hizmet").strip()
    duration = extract_duration_phrase(message_text) or "bu seviyeye"
    return (
        f"Evet, {display} tarafında {duration} seviyesine çıkabilir; özellikle çoklu entegrasyon, özel CRM/n8n akışı, "
        "onay süreçleri, revizyonlar veya hazır içerik eksikliği varsa süre uzar. Standart kurulumlarda hedefimiz daha kısa tutmaktır."
    )


def build_booking_ready_service_reply(service: dict[str, Any], *, price_context: bool = False) -> str:
    if price_context:
        if str(service.get("price") or "").lower().startswith("özel"):
            opening = f"{service['display']} için fiyat ihtiyaca göre değişiyor."
        elif is_recurring_service(service):
            opening = f"{service['display']} {service['price']}'den başlıyor ({service['price_note']})."
        else:
            opening = f"{service['display']} {service['price']} ({service['price_note']})."
    else:
        opening = build_service_context_intro(service)
    return f"{opening} Uygunsanız bunu kısa bir ön görüşmede netleştirebiliriz; önce ad soyadınızı alayım."


def build_price_followup_reply(service: dict[str, Any], message_text: str, conversation: dict[str, Any] | None = None) -> str:
    lowered = sanitize_text(message_text).lower()
    asks_monthly = "aylık" in lowered or "aylik" in lowered
    asks_one_time = "tek sefer" in lowered
    suffix = _price_reply_suffix(service, conversation)

    if is_recurring_service(service):
        if asks_one_time:
            return f"Tek seferlik değil, {service['price_note']}. {suffix}"
        if asks_monthly:
            return f"Evet, {service['price_note']}. {suffix}"
        return f"Bu hizmet {service['price_note']}. {suffix}"

    if asks_monthly:
        return f"Aylık değil, {service['price_note']}. {suffix}"

    return build_price_question_reply(service, conversation)


def build_price_scope_clarification_reply(service: dict[str, Any] | None = None) -> str:
    if service:
        display = str(service.get("display") or "Bu fiyat")
        return f"Hayır, bu fiyat tüm hizmetler için değil. {display} için başlangıç fiyatıdır. İsterseniz diğer hizmetlerin ücret aralığını da kısaca paylaşayım."
    return "Hayır, tüm hizmetler dahil değil. Paylaşılan rakam başlangıç fiyatıdır ve hizmete göre değişir. İsterseniz hangi hizmet için düşündüğünüzü yazın, net söyleyeyim."


def build_all_services_price_reply() -> str:
    priced_items: list[str] = []
    special_offer_items: list[str] = []
    for service in DOEL_SERVICE_CATALOG:
        display = str(service.get("display") or "").strip()
        price = str(service.get("price") or "").strip().replace("₺", "TL")
        note = str(service.get("price_note") or "").strip()
        if not display or not price:
            continue
        line = f"{display}: {price}"
        if note:
            line += f" ({note})"
        if price.lower().startswith("özel"):
            special_offer_items.append(line)
        else:
            priced_items.append(line)
    joined = "; ".join([*priced_items, *special_offer_items])
    return f"Tabii, ana hizmet fiyatları şöyle: {joined}. İsterseniz en merak ettiğiniz kalemden başlayıp detaylandırayım."


def build_price_negotiation_reply(service: dict[str, Any], message_text: str) -> str:
    budget_amount = extract_budget_amount(message_text)
    budget_text = format_try_amount(budget_amount)

    if budget_text:
        return (
            f"Anlıyorum, fiyat tarafındaki özel değerlendirmeyi ben netleştiremiyorum. {service['display']} için paylaştığınız {budget_text} teklif notunu şirket sahibimize ileteyim; uygun görürse size buradan dönüş sağlasın."
        )

    return (
        f"Anlıyorum, fiyat tarafındaki son değerlendirmeyi ben yapamıyorum. {service['display']} için notunuzu şirket sahibimize ileteyim; uygun görürse size buradan dönüş sağlasın."
    )


def build_faq_reply(answer: str) -> str:
    return f"{answer} İsterseniz ihtiyacınızı da yazın; buradan en mantıklı şekilde yönlendirelim."


def is_owner_check_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in OWNER_CHECK_KEYWORDS)


def is_assistant_identity_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in ASSISTANT_IDENTITY_KEYWORDS)


def is_clarification_request(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in CLARIFICATION_KEYWORDS) or is_meeting_clarification_question(text)


def is_meeting_clarification_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    phrases = [
        "ne on gorusmesi",
        "ne ön görüşmesi",
        "hangi gorusme",
        "hangi görüşme",
        "nasil gorusecegiz",
        "nasıl görüşeceğiz",
        "nasıl görüşecegiz",
        "nereden gorusecegiz",
        "nereden görüşeceğiz",
    ]
    return any(phrase in lowered for phrase in phrases)


def is_meeting_method_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    method_phrases = [
        "nasil gorusecegiz",
        "nasil gorusulur",
        "nasil yapacagiz",
        "nereden gorusecegiz",
        "nerede gorusecegiz",
        "gorusecegiz nasil",
        "online mi gorusecegiz",
        "telefonla mi gorusecegiz",
    ]
    return any(phrase in lowered for phrase in method_phrases)


def is_phone_reason_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    has_phone_subject = any(term in lowered for term in ["telefon", "numara", "numarayi", "iletisim"])
    asks_reason = any(term in lowered for term in ["neden", "niye", "ne icin", "nicin", "gerekli", "lazim", "istiyorsun", "istiyorsunuz"])
    return has_phone_subject and asks_reason


def is_request_reason_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return is_phone_reason_question(lowered) or any(keyword in lowered for keyword in REQUEST_REASON_KEYWORDS)


def is_angry_complaint_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    complaint_phrases = [
        "yarr",
        "amk",
        "aq",
        "siktir",
        "siktig",
        "sikiyim",
        "bune",
        "bu ne lan",
        "salak",
        "aptal",
        "anlamiyorsun",
        "anlamıyorsun",
        "cevap vermiyorsun",
        "ayni mesaji",
        "aynı mesajı",
        "ayni mesaj",
        "aynı mesaj",
        "bot gibi",
        "oto mesaj",
        "otomatik mesaj",
        "sinirlendim",
        "tekrar edip duruyorsun",
        "ayni seyi tekrar",
        "ayni şeyi tekrar",
        "sacma",
        "saçma",
    ]
    return any(phrase in lowered for phrase in complaint_phrases)


def is_trust_or_scam_question(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if "?" not in lowered:
        return False
    trust_terms = [
        "dolandirici",
        "dolandırıcı",
        "sahte",
        "guvenilir",
        "güvenilir",
        "guvenebilir",
        "güvenebilir",
        "guveneyim",
        "güveneyim",
        "gercek misiniz",
        "gerçek misiniz",
        "emin olabilir",
    ]
    return any(term in lowered for term in trust_terms)


def build_trust_or_scam_reply() -> str:
    return (
        "Hayır, dolandırıcı değiliz. DOEL Digital olarak web, reklam ve yapay zeka otomasyon hizmetleri veriyoruz. "
        "Karar vermeden önce referans, kapsam, fiyat ve süreç bilgisini buradan net sorabilirsiniz; içinize sinmezse hiçbir bilgi paylaşmak zorunda değilsiniz."
    )


def build_angry_complaint_reply() -> str:
    return (
        "Kusura bakmayın, sizi yanlış yönlendirdim ve aynı akışı tekrar ettim. "
        "Şu an telefon istemeden devam edebiliriz. Merak ettiğiniz konuyu yazın; önce onu net cevaplayayım."
    )


def is_phone_share_refusal(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword == lowered or keyword in lowered for keyword in PHONE_REFUSAL_KEYWORDS)


def is_offer_hesitation_message(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword == lowered or keyword in lowered for keyword in OFFER_HESITATION_KEYWORDS)


def build_phone_refusal_reply(conversation: dict[str, Any]) -> str:
    service = display_service_name(conversation.get("service"))
    if service:
        return f"Tamam, sorun değil; telefonu paylaşmak zorunda değilsiniz. İsterseniz {service} tarafında bilgi vermeye buradan devam edeyim. Daha sonra ön görüşme planlamak isterseniz numarayı o aşamada paylaşabilirsiniz."
    return "Tamam, sorun değil; telefonu paylaşmak zorunda değilsiniz. İsterseniz bilgi vermeye buradan devam edelim. Daha sonra ön görüşme planlamak isterseniz numarayı o aşamada paylaşabilirsiniz."


def build_missing_phone_for_booking_reply(conversation: dict[str, Any]) -> str:
    booking_label = get_booking_label(conversation)
    return f"Ad soyadınızı aldım. Ama {booking_label} kaydını açabilmem için bir telefon numarası da gerekiyor. Paylaşmak istemiyorsanız sorun değil; buradan bilgi vermeye devam edebilirim. Paylaşırsanız kaydı hemen oluşturalım."


def build_offer_hesitation_reply(conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    sector = detect_business_sector(conversation.get("last_customer_message") or "", history)
    service = display_service_name(conversation.get("service"))
    service_hint = f" {service} konusunda" if service else ""
    if sector == "beauty":
        return (
            f"Cok normal{service_hint}, hemen karar vermek zor olabilir. "
            "On gorusmeyi de tam bu yuzden oneriyorum: salonunuza gercekten ne kadar katki saglar, "
            "10 dakikada somut rakamlarla gosteririm. Satis baskisi yok, once anlayalim, "
            "sonra siz karar verin. Hangi gun muygun?"
        )
    return (
        f"Anlasildı{service_hint}. Hemen karar vermenizi beklemiyorum. "
        "On gorusmede size gercekten ne kadar fayda saglar, somut olarak aktaririm; "
        "sonra siz degerlendirirsiniz. 15 dakikanizi alirim, baskisi olmaz. "
        "Hangi gun size uyar?"
    )


def build_booking_resume_hint(conversation: dict[str, Any]) -> str:
    if not has_resumeable_booking_context(conversation):
        return "Size nasıl yardımcı olabilirim?"
    state = conversation.get("state", "new")
    requested_date = conversation.get("requested_date")
    service = display_service_name(conversation.get("service"))
    if state == "collect_service" and service:
        service_meta = match_service_catalog(service, service)
        if service_meta:
            return build_contextual_service_followup(service_meta, conversation)
        return f"İsterseniz {service} için kaldığımız yerden devam edebiliriz."
    if state == "collect_time" and requested_date:
        return f"İsterseniz {format_human_date(requested_date)} için size uyan saati netleştirebiliriz."
    if state == "collect_period":
        return "İsterseniz sabah mı öğleden sonra mı daha uygun olduğunuzu yazabilirsiniz."
    if state == "collect_date":
        return "İsterseniz size uygun günü yazabilirsiniz."
    if state == "collect_phone":
        return "Ön görüşme kaydına daha sonra devam edebiliriz; isterseniz önce sorunuzu yanıtlayayım."
    if state == "collect_name":
        return "Ön görüşme kaydına daha sonra devam edebiliriz; isterseniz önce sorunuzu yanıtlayayım."
    return "Size nasıl yardımcı olabilirim?"


def build_simple_greeting_reply(message_text: str | None = None) -> str:
    lowered = sanitize_text(message_text or "").lower()
    compact = re.sub(r"[^a-z0-9]+", "", lowered)
    if compact in {"salamunaleykum", "selamunaleykum", "selaminaleykum", "selamunalekum", "aleykumselam", "aleykumselamlar"}:
        return "Aleyküm selam, hoş geldiniz. Nasıl yardımcı olabilirim?"
    return "Merhaba, hoş geldiniz. Nasıl yardımcı olabilirim?"


def build_owner_check_reply(conversation: dict[str, Any]) -> str:
    if has_resumeable_booking_context(conversation):
        return f"Burada size DOEL DIGITAL adına yardımcı oluyorum. {build_booking_resume_hint(conversation)}"
    return "Burada size DOEL DIGITAL adına yardımcı oluyorum."


def build_assistant_identity_reply(conversation: dict[str, Any]) -> str:
    if has_resumeable_booking_context(conversation):
        return f"Ben burada DOEL DIGITAL adına size destek oluyorum. {build_booking_resume_hint(conversation)}"
    return "Ben burada DOEL DIGITAL adına size destek oluyorum."


def build_greeting_interrupt_reply(conversation: dict[str, Any]) -> str:
    return f"Merhaba, buradayım. {build_booking_resume_hint(conversation)}"


def has_resumeable_booking_context(conversation: dict[str, Any]) -> bool:
    state = sanitize_text(conversation.get("state") or "")
    if state not in {"collect_service", "collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        return False
    return bool(conversation.get("service") or conversation.get("requested_date") or conversation.get("requested_time") or conversation.get("booking_kind"))


def is_booking_assumption_rejection(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in BOOKING_RESET_KEYWORDS)


def is_booking_ownership_rejection(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    return any(keyword in lowered for keyword in BOOKING_OWNERSHIP_REJECTION_KEYWORDS)


def clear_booking_assumption(conversation: dict[str, Any]) -> None:
    conversation["service"] = None
    conversation["requested_date"] = None
    conversation["requested_time"] = None
    conversation["preferred_period"] = None
    conversation["booking_kind"] = None
    conversation["appointment_status"] = "collecting"
    conversation["state"] = "collect_service"


def build_booking_assumption_reset_reply() -> str:
    return "Haklısınız, yanlış anladıysam kusura bakmayın. Şu an bir randevu oluşturmuyorum. Önce hangi konuda bilgi almak istediğinizi yazın, size net şekilde yardımcı olayım."


def build_contextual_clarification_reply(conversation: dict[str, Any], message_text: str | None = None) -> str:
    state = conversation.get("state", "new")
    service = display_service_name(conversation.get("service"))
    booking_label = get_booking_label(conversation)
    memory = ensure_conversation_memory(conversation)
    lowered_message = sanitize_text(message_text or "").lower()
    if is_phone_reason_question(lowered_message):
        return (
            "Telefonu sadece ön görüşme planlanırsa size doğrudan ulaşmak ve görüşme detayını netleştirmek için isteriz. "
            "Telefon paylaşmak zorunda değilsiniz; bilgi vermeye buradan devam edebiliriz."
        )
    if is_meeting_method_question(lowered_message):
        return (
            "Önce buradan ihtiyacınızı netleştiriyoruz. Uygun görürseniz devamında telefon ya da online görüşme ile ilerliyoruz; "
            "telefon paylaşmadan da temel bilgileri buradan anlatabilirim."
        )
    if is_meeting_clarification_question(lowered_message):
        focus = service or "ihtiyacınız"
        return (
            f"Ön görüşme, {focus} için size uygun çözümü hızlıca netleştirdiğimiz kısa bir görüşmedir. "
            "Genelde Instagram üzerinden başlatıp ardından telefon ya da online görüşme ile ilerliyoruz. "
            "İsterseniz önce merak ettiğiniz kısmı buradan cevaplayayım."
        )
    if state == "collect_service":
        if memory.get("pending_offer") == "preconsultation_offer":
            focus = service or "bu süreç"
            return f"Tabii. Demek istediğim şu: {focus} tarafında size uygun bir sistem kurup kuramayacağımızı 10 dakikalık kısa bir ön görüşmede netleştirebiliriz. İsterseniz devam edelim; istemezseniz buradan bilgi vermeye devam edebilirim."
        if service:
            return f"Tabii. {service} tarafında size nasıl destek olabileceğimizi netleştirelim istiyorum. En çok hangi taraf zor geliyor?"
        return "Yani size hangi konuda destek olacağımızı netleştirelim istiyorum. Web tasarım, otomasyon, reklam yönetimi veya sosyal medya tarafından hangisi size daha yakın?"
    if state == "collect_name":
        return f"Yani {booking_label} kaydını açabilmek için önce ad soyad bilginizi istiyorum."
    if state == "collect_phone":
        return "Telefonu sadece 10 dakikalık ön görüşme detayını netleştirmek ve gerektiğinde size doğrudan ulaşabilmek için istiyorum. Paylaşmak istemezseniz sorun değil; bilgi vermeye buradan devam edebiliriz."
    if state == "collect_date":
        if service:
            return f"Yani {service} için {booking_label} planlayalım istiyorum. Size uygun günü yazmanız yeterli."
        return "Yani size uygun günü soruyorum. Örneğin yarın, cuma veya 12.04 yazabilirsiniz."
    if state == "collect_period":
        return "Yani sabah mı öğleden sonra mı daha uygun olduğunuzu soruyorum."
    if state == "collect_time":
        requested_date = conversation.get("requested_date")
        if requested_date:
            return f"Yani {format_human_date(requested_date)} için size uyan net saati sormuştum. Uygun saati yazarsanız devam edebiliriz."
        return "Yani size uyan net saati seçmenizi istiyorum. Listedeki saatlerden birini yazabilirsiniz."
    return "Elbette. İhtiyacınızı kısaca yazarsanız size en doğru şekilde yardımcı olayım."


def match_objection_type(text: str) -> str | None:
    lowered = sanitize_text(text).lower()
    if not lowered:
        return None
    hesitation_patterns = [
        r"\bistemiyom\b", r"\bistemiyorum\b", r"\bilgilenmiyom\b", r"\bilgilenmiyorum\b",
        r"\bgerek yok\b", r"\byok gerek\b", r"\bistemem\b", r"\bkals[ıi]n\b", r"\bbo[sş]ver\b",
    ]
    if any(re.search(pattern, lowered) for pattern in hesitation_patterns):
        return "hesitation"
    for objection_type, keywords in OBJECTION_KEYWORDS.items():
        if any(keyword in lowered for keyword in keywords):
            return objection_type
    return None


def build_objection_reply(objection_type: str, conversation: dict[str, Any]) -> str:
    if objection_type == "price":
        if conversation.get("service"):
            return (
                f"Anlıyorum. {conversation['service']} tarafında bütçeyi daha mantıklı hale getirmek için ya kapsamı daraltabiliriz ya da önce en kritik parçadan başlayabiliriz. "
                "Size net öneri yapabilmem için şu an en öncelikli ihtiyacınız nedir?"
            )
        return (
            "Anlıyorum. Bütçe konusu önemli. Size ezbere fiyat dayatmak yerine önce ihtiyacınızı netleştirelim; sonra en mantıklı başlangıç seçeneğini söyleyeyim. "
            "Şu an en çok hangi konuda destek arıyorsunuz?"
        )
    return "Anladım, sorun değil. İsterseniz sonra yine yazabilirsiniz."


def is_service_advice_request(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    if llm_data.get("intent") in {"service_advice", "comparison"}:
        return True
    return any(keyword in lowered for keyword in SERVICE_ADVICE_KEYWORDS)


def is_comparison_request(text: str, matched_services: list[dict[str, Any]] | None = None, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    if llm_data.get("intent") == "comparison":
        return True
    if any(keyword in lowered for keyword in COMPARISON_KEYWORDS):
        return True
    return len(matched_services or []) >= 2 and (" mi " in f" {lowered} " or " mı " in f" {lowered} ")


def llm_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    lowered = sanitize_text(str(value or "")).lower()
    return lowered in {"1", "true", "yes", "y", "evet"}


def llm_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def llm_booking_confidence(llm_data: dict[str, Any] | None = None) -> float:
    llm_data = llm_data or {}
    for key in ["booking_confidence", "confidence"]:
        parsed = llm_float(llm_data.get(key))
        if parsed is not None:
            return max(0.0, min(parsed, 1.0))
    return 0.0


def message_shows_booking_intent(text: str, llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(text).lower()
    llm_data = llm_data or {}
    if (is_service_advice_request(text, llm_data) or is_comparison_request(text, None, llm_data)) and not explicitly_starts_consultation_collection(text):
        return False
    non_booking_priority_answers = [
        "randevu tarafı", "randevu tarafi", "randevu takibi", "dm cevapları", "dm cevaplari",
        "crm takibi", "müşteri takibi", "musteri takibi", "teklif/fatura",
        "randevu kaçıyor", "randevu kaciyor", "randevu kaçır", "randevu kacir",
        "randevu ve takip", "randevu takip",
    ]
    if any(phrase in lowered for phrase in non_booking_priority_answers):
        return False
    if any(keyword in lowered for keyword in DIRECT_APPOINTMENT_KEYWORDS):
        return True
    if llm_data.get("intent") in {"appointment", "availability"} or llm_bool(llm_data.get("wants_booking")):
        return True
    return any(re.search(rf"\b{re.escape(keyword)}\b", lowered) for keyword in BOOKING_INTENT_KEYWORDS)


def accepts_pending_consultation_offer(
    message_text: str,
    conversation: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
    llm_data: dict[str, Any] | None = None,
) -> bool:
    llm_data = llm_data or {}
    conversation = conversation or {}
    memory = ensure_conversation_memory(conversation)
    has_pending_offer = memory.get("pending_offer") == "preconsultation_offer" or recent_outbound_offered_consultation(history)
    if not has_pending_offer:
        return False
    if is_confirmation_acceptance_message(message_text):
        return True
    if llm_bool(llm_data.get("did_user_accept_previous_offer")):
        return True
    reply_strategy = sanitize_text(str(llm_data.get("reply_strategy") or "")).lower()
    return reply_strategy in {"start_booking", "accept_offer", "collect_booking_details"}


def should_enter_booking_collection(
    message_text: str,
    llm_data: dict[str, Any] | None = None,
    *,
    asks_availability: bool = False,
    detected_phone: str | None = None,
    detected_date: str | None = None,
    detected_time: str | None = None,
    conversation: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
) -> bool:
    llm_data = llm_data or {}
    conversation = conversation or {}
    if is_business_fit_question(message_text):
        return False
    active_booking_state = sanitize_text(conversation.get("state") or "") in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}
    if active_booking_state and has_resumeable_booking_context(conversation):
        if customer_question_should_pause_booking_collection(
            message_text,
            llm_data,
            asks_availability=asks_availability,
            detected_phone=detected_phone,
            detected_date=detected_date,
            detected_time=detected_time,
            conversation=conversation,
            history=history,
        ):
            return False
        return True
    if asks_availability or detected_phone or detected_date or detected_time:
        return True
    if accepts_pending_consultation_offer(message_text, conversation, history, llm_data):
        return True
    if any(keyword in sanitize_text(message_text).lower() for keyword in DIRECT_APPOINTMENT_KEYWORDS):
        return True
    if message_shows_booking_intent(message_text, llm_data):
        return True
    return llm_bool(llm_data.get("wants_booking")) and llm_booking_confidence(llm_data) >= 0.9


def customer_question_should_pause_booking_collection(
    message_text: str,
    llm_data: dict[str, Any] | None = None,
    *,
    asks_availability: bool = False,
    detected_phone: str | None = None,
    detected_date: str | None = None,
    detected_time: str | None = None,
    conversation: dict[str, Any] | None = None,
    history: list[dict[str, Any]] | None = None,
) -> bool:
    cleaned = sanitize_text(message_text)
    llm_data = llm_data or {}
    conversation = conversation or {}
    if asks_availability or detected_phone or detected_date or detected_time:
        return False
    if extract_phone(cleaned) or extract_date(cleaned) or extract_time_for_state(cleaned, conversation.get("state")):
        return False
    if accepts_pending_consultation_offer(cleaned, conversation, history, llm_data):
        return False
    lowered = cleaned.lower()
    if any(keyword in lowered for keyword in DIRECT_APPOINTMENT_KEYWORDS):
        return False
    if message_shows_booking_intent(cleaned, llm_data) or wants_availability_information(cleaned, llm_data):
        return False
    if llm_data.get("intent") == "appointment" or (llm_bool(llm_data.get("wants_booking")) and llm_booking_confidence(llm_data) >= 0.9):
        return False
    if any(
        [
            is_simple_greeting(cleaned),
            is_good_wishes_message(cleaned),
            is_smalltalk_message(cleaned),
            is_presence_check_message(cleaned),
            is_angry_complaint_message(cleaned),
            is_trust_or_scam_question(cleaned),
            is_request_reason_question(cleaned),
            is_clarification_request(cleaned),
            is_phone_share_refusal(cleaned),
            is_price_question(cleaned),
            is_price_followup_message(cleaned, llm_data),
            is_delivery_time_question(cleaned),
            is_service_overview_question(cleaned),
            is_working_schedule_question(cleaned),
            is_company_background_question(cleaned),
            is_assistant_identity_question(cleaned),
            is_owner_check_message(cleaned),
        ]
    ):
        return True
    if "?" not in cleaned:
        return False
    return True


def normalize_booking_kind(value: str | None) -> str | None:
    lowered = sanitize_text(value or "").lower()
    if lowered in {"appointment", "scheduled", "randevu"}:
        return "appointment"
    if lowered in {"preconsultation", "pre_consultation", "ön görüşme", "on gorusme", "görüşme", "gorusme"}:
        return "preconsultation"
    return None


def get_booking_kind(conversation: dict[str, Any]) -> str | None:
    return normalize_booking_kind(conversation.get("booking_kind"))


def get_booking_label(conversation: dict[str, Any]) -> str:
    return "randevu" if get_booking_kind(conversation) == "appointment" else "ön görüşme"


def infer_booking_kind(message_text: str, llm_data: dict[str, Any] | None = None, conversation: dict[str, Any] | None = None, matched_services: list[dict[str, Any]] | None = None) -> str | None:
    llm_data = llm_data or {}
    conversation = conversation or {}
    existing = normalize_booking_kind(conversation.get("booking_kind"))
    if existing:
        return existing
    if accepts_pending_consultation_offer(message_text, conversation, None, llm_data):
        return "preconsultation"
    if explicitly_starts_consultation_collection(message_text):
        return "preconsultation"
    lowered = sanitize_text(message_text).lower()
    has_preconsultation_intent = any(keyword in lowered for keyword in PRECONSULTATION_INTENT_KEYWORDS)
    has_direct_appointment_intent = any(keyword in lowered for keyword in DIRECT_APPOINTMENT_KEYWORDS)
    if has_preconsultation_intent:
        return "preconsultation"
    if has_direct_appointment_intent:
        return "appointment"
    if llm_data.get("intent") in {"appointment", "availability"} or llm_bool(llm_data.get("wants_booking")):
        return "appointment"
    if llm_data.get("intent") in {"service_advice", "comparison"}:
        return "preconsultation"
    if is_service_advice_request(message_text, llm_data) or is_comparison_request(message_text, matched_services, llm_data) or is_price_question(message_text):
        return "preconsultation"
    if conversation.get("state") in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        return normalize_booking_kind(conversation.get("booking_kind")) or "preconsultation"
    return None


def infer_period_from_time(time_value: str | None) -> str | None:
    normalized = normalize_time_string(time_value)
    if not normalized:
        return None
    try:
        minutes = to_minutes(normalized)
    except ValueError:
        return None
    return "morning" if minutes < 12 * 60 else "afternoon"


def extract_preferred_period(text: str) -> str | None:
    cleaned = sanitize_text(text)
    lowered = cleaned.lower()
    if any(keyword in lowered for keyword in MORNING_PERIOD_KEYWORDS):
        return "morning"
    if any(keyword in lowered for keyword in AFTERNOON_PERIOD_KEYWORDS):
        return "afternoon"
    if PURE_NUMERIC_DATE_PATTERN.match(cleaned):
        return None
    inferred = infer_period_from_time(extract_time_for_state(text, "collect_period") or extract_time(text))
    return inferred


def get_period_label(period: str | None) -> str:
    return "sabah" if period == "morning" else "öğleden sonra"


def filter_slots_by_period(open_slots: list[str], preferred_period: str | None) -> list[str]:
    if preferred_period not in {"morning", "afternoon"}:
        return open_slots
    filtered: list[str] = []
    for slot in open_slots:
        try:
            minutes = to_minutes(slot)
        except ValueError:
            continue
        if preferred_period == "morning" and minutes < 12 * 60:
            filtered.append(slot)
        if preferred_period == "afternoon" and minutes >= 12 * 60:
            filtered.append(slot)
    return filtered


def build_service_recommendation_reason(service: dict[str, Any]) -> str:
    return SERVICE_REASON_MAP.get(service.get("slug") or "", "bu ihtiyaca en yakın çözümü sunar")


def build_service_focus(service: dict[str, Any]) -> str:
    return SERVICE_FOCUS_MAP.get(service.get("slug") or "", service.get("display") or "bu ihtiyaç")


def build_service_clarifying_question(service: dict[str, Any]) -> str:
    return SERVICE_CLARIFYING_QUESTIONS.get(
        service.get("slug") or "",
        "İhtiyacınızı biraz daha açarsanız size en doğru yönü net söyleyebilirim.",
    )


def build_contextual_service_followup(service: dict[str, Any], conversation: dict[str, Any] | None = None) -> str:
    if not conversation:
        return build_service_clarifying_question(service)

    memory = ensure_conversation_memory(conversation)
    answered = set(memory.get("answered_question_types") or [])
    slug = service.get("slug") or ""

    if memory.get("pending_offer") == "preconsultation_offer" and memory.get("offer_status") in {"offered", "hesitant"}:
        return "Süreci netleştirmek için çok kısa bir ön görüşme yapabiliriz. Hangi gün ve saat sizin için uygundur?"

    if slug == "otomasyon-ai":
        priority = memory.get("last_priority_choice")
        dm_issue = memory.get("last_dm_issue_choice")
        if "message_volume" in answered or dm_issue or memory.get("message_volume_estimate"):
            return "Burada en mantıklı çözüm DM, randevu ve müşteri takibini tek akışta toplamak olur. Uygunsanız bunu kısa bir ön görüşmede netleştirelim. Hangi gün ve saat uygundur?"
        if priority == "dm" and "dm_issue" not in answered:
            return "DM tarafında sizi en çok geç dönüş mü zorluyor, yoksa tekrar eden sorular mı?"
        if priority == "appointment":
            return "Randevu tarafında sizi en çok planlama mı, yoksa iptal ve ertelemeler mi zorluyor?"
        if priority in {"crm", "invoice"}:
            return "Bu tarafta yük birkaç adıma yayılıyor gibi. Uygunsanız bunu kısa bir ön görüşmede netleştirebiliriz."
        if "priority" in answered:
            return "Burada tek bir özellikten çok, uçtan uca düzenli bir akış gerekiyor. Uygunsanız bunu kısa bir ön görüşmede netleştirebiliriz."

    return build_service_clarifying_question(service)


def build_service_advice_reply(message_text: str, matched_services: list[dict[str, Any]], llm_data: dict[str, Any], conversation: dict[str, Any]) -> dict[str, Any]:
    booking_intent = message_shows_booking_intent(message_text, llm_data)
    if not matched_services:
        reply = "Hedefinize göre en doğru hizmeti belirleyelim. Kısa bir ön görüşme ayarlayalım mı?"
        return {
            "reply": reply,
            "kind": "service_advice",
            "next_state": "collect_service",
            "set_service": None,
        }

    primary = matched_services[0]
    secondary = matched_services[1] if len(matched_services) > 1 else None
    comparison_mode = is_comparison_request(message_text, matched_services, llm_data)

    if comparison_mode and secondary:
        reply = (
            f"Size {primary['display']} önerebilirim. "
            f"Detayları kısa bir ön görüşmede netleştirelim mi?"
        )
        return {
            "reply": reply,
            "kind": "comparison",
            "next_state": "collect_name" if booking_intent else "collect_service",
            "set_service": None,
        }

    reply = f"Size {primary['display']} önerebilirim."
    if booking_intent:
        reply += " Önce ad soyadınızı paylaşın, görüşmeyi ayarlayalım."
    else:
        reply += " " + build_contextual_service_followup(primary, conversation)

    return {
        "reply": reply,
        "kind": "service_advice",
        "next_state": "collect_name" if booking_intent else "collect_service",
        "set_service": primary["display"] if len(matched_services) == 1 else None,
    }


def maybe_build_information_reply(message_text: str, llm_data: dict[str, Any], matched_services: list[dict[str, Any]], conversation: dict[str, Any], history: list[dict[str, Any]] | None = None, *, direct_service_match: bool = False) -> dict[str, Any] | None:
    lowered = message_text.lower()
    current_state = conversation.get("state", "new")
    matched_service = matched_services[0] if matched_services else None
    has_booking_context = has_resumeable_booking_context(conversation)
    memory = ensure_conversation_memory(conversation)
    detail_keyword_match = any(keyword in lowered for keyword in DETAIL_KEYWORDS)
    asks_detail = detail_keyword_match or ("?" in message_text and current_state in {"new", "collect_service"})
    asks_identity = is_assistant_identity_question(message_text)
    asks_services = is_service_overview_question(message_text)
    recent_outbound_text = get_last_outbound_text(history).lower()
    recent_overview_context = any(cue in recent_outbound_text for cue in ["web tasarim", "otomasyon", "reklam", "sosyal medya"])
    asks_detailed_service_overview = (asks_services and asks_detail) or (detail_keyword_match and recent_overview_context)
    asks_schedule = is_working_schedule_question(message_text)
    if (asks_identity and asks_services) or (asks_identity and asks_schedule) or (asks_services and asks_schedule):
        return {
            "reply": build_combined_intro_reply(
                include_identity=asks_identity,
                include_services=asks_services,
                include_schedule=asks_schedule,
            ),
            "kind": "combined_intro",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if not asks_identity and not is_owner_check_message(message_text) and (llm_bool(llm_data.get("wants_human")) or any(keyword in sanitize_text(message_text).lower() for keyword in HUMAN_KEYWORDS)):
        return {
            "reply": "Tabii, sizi yetkili ekibimize yönlendiriyorum. Uygunsanız adınızı ve telefon numaranızı bırakın, ekibimiz size kısa sürede dönüş sağlasın.",
            "kind": "human_handoff",
            "next_state": "human_handoff",
            "set_service": conversation.get("service"),
            "handoff": True,
        }
    if ensure_conversation_memory(conversation).get("offer_status") == "declined" and (is_closeout_message(message_text) or is_low_signal_message(message_text)):
        return {
            "reply": "Tabii, acelesi yok. Aklınıza takılan bir şey olursa ya da ilerleyen günlerde bakmak isterseniz buradayım.",
            "kind": "decline_cooldown",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_owner_check_message(message_text):
        return {
            "reply": build_owner_check_reply(conversation),
            "kind": "owner_check",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_assistant_identity_question(message_text):
        return {
            "reply": build_assistant_identity_reply(conversation),
            "kind": "assistant_identity",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_presence_check_message(message_text):
        return {
            "reply": "Evet, buradayım. Nasıl yardımcı olabilirim?",
            "kind": "presence_check",
            "next_state": conversation.get("state", "collect_service") if has_booking_context else "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_good_wishes_message(message_text):
        return {
            "reply": build_good_wishes_reply(),
            "kind": "smalltalk",
            "next_state": conversation.get("state", "collect_service") if has_booking_context else "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_voice_duration_placeholder_message(message_text) and current_state in {"new", "collect_service"}:
        return {
            "reply": build_voice_duration_placeholder_reply(),
            "kind": "voice_placeholder",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_angry_complaint_message(message_text):
        return {
            "reply": build_angry_complaint_reply(),
            "kind": "complaint",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
            "clear_booking": True,
        }
    if is_trust_or_scam_question(message_text):
        return {
            "reply": build_trust_or_scam_reply(),
            "kind": "trust_question",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_delivery_time_question(message_text):
        delivery_service = matched_service or match_service_catalog(message_text, conversation.get("service"))
        if is_delivery_duration_followup(message_text):
            reply_text = build_delivery_duration_followup_reply(delivery_service, message_text)
        else:
            reply_text = build_delivery_time_reply(delivery_service)
        return {
            "reply": reply_text,
            "kind": "delivery_time",
            "next_state": "collect_service",
            "set_service": (delivery_service or {}).get("display") or conversation.get("service"),
        }
    if current_state in {"new", "collect_service", "human_handoff"} and is_message_volume_answer(message_text):
        return {
            "reply": build_message_volume_reply(message_text, conversation, history),
            "kind": "message_volume",
            "next_state": "collect_service",
            "set_service": "Otomasyon & Yapay Zeka Çözümleri",
        }
    if is_technical_issue_message(message_text):
        return {
            "reply": build_technical_issue_reply(conversation, history),
            "kind": "technical_issue",
            "next_state": conversation.get("state", "new") if has_booking_context else "new",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_request_reason_question(message_text) or is_clarification_request(message_text):
        return {
            "reply": build_contextual_clarification_reply(conversation, message_text),
            "kind": "clarification",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_phone_share_refusal(message_text):
        return {
            "reply": build_phone_refusal_reply(conversation),
            "kind": "phone_refusal",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
            "clear_booking": True,
        }
    if is_price_question(message_text):
        if matched_service:
            return {
                "reply": build_price_question_reply(matched_service, conversation),
                "kind": "price_question",
                "next_state": "collect_service",
                "set_service": matched_service["display"],
            }
        return {
            "reply": "Net fiyat, seçilecek hizmete göre değişiyor. Web tasarım, otomasyon & yapay zeka, performans pazarlama veya sosyal medya yönetiminden hangisiyle ilgilendiğinizi yazarsanız size doğru bilgiyi paylaşayım.",
            "kind": "price_route",
            "next_state": "collect_service",
            "set_service": None,
        }
    if asks_detailed_service_overview:
        return {
            "reply": build_detailed_services_overview_reply(),
            "kind": "overview",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if asks_services:
        return {
            "reply": build_services_overview_reply(),
            "kind": "overview",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    explicit_automation_request = bool(
        any(keyword in lowered for keyword in ["otomasyon", "yapay zeka", "ai", "chatbot", "dm", "randevu", "crm"])
        and any(keyword in lowered for keyword in ["bağla", "bagla", "kur", "kuralım", "kuralim", "istiyorum", "lazım", "lazim", "gerek", "olsun"])
    )
    if explicit_automation_request:
        return {
            "reply": "Tamam, otomasyon tarafında ilerleyebiliriz. Önce hangi süreci toparlamak istediğinizi netleştirelim: DM, randevu yoksa müşteri takibi mi?",
            "kind": "service_advice",
            "next_state": "collect_service",
            "set_service": "Otomasyon & Yapay Zeka Çözümleri",
        }
    if is_fatigue_painpoint_message(message_text) or is_reaction_message(message_text):
        return {
            "reply": build_fatigue_painpoint_reply(conversation, history),
            "kind": "fatigue_painpoint",
            "next_state": conversation.get("state", "collect_service") if has_booking_context else "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_smalltalk_message(message_text):
        return {
            "reply": build_smalltalk_reply(conversation),
            "kind": "smalltalk",
            "next_state": conversation.get("state", "collect_service") if has_booking_context else "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if is_simple_greeting(message_text) and not has_booking_context and not is_business_context_intro_message(message_text, history):
        return {
            "reply": build_simple_greeting_reply(message_text),
            "kind": "greeting",
            "next_state": "collect_service",
            "set_service": None,
        }
    if is_booking_assumption_rejection(message_text):
        return {
            "reply": build_booking_assumption_reset_reply(),
            "kind": "booking_reset",
            "next_state": "collect_service",
            "set_service": None,
            "clear_booking": True,
        }
    if is_simple_greeting(message_text) and has_booking_context:
        return {
            "reply": build_greeting_interrupt_reply(conversation),
            "kind": "greeting_interrupt",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    followup_role = infer_contextual_followup_role(message_text, conversation, history, llm_data)
    recent_outbound_act = infer_recent_outbound_act(history)
    if is_all_choice_message(message_text) and (recent_outbound_act == "answered_price" or recent_outbound_answered_price(history) or memory.get("price_context_open")):
        return {
            "reply": build_all_services_price_reply(),
            "kind": "price_all_services",
            "next_state": "collect_service",
            "set_service": None,
        }
    if followup_role == "price_clarification" and recent_outbound_act == "answered_price":
        scoped_service = matched_service or (service_catalog.get(conversation.get("service")) if conversation.get("service") else None)
        return {
            "reply": build_price_scope_clarification_reply(scoped_service),
            "kind": "price_followup",
            "next_state": "collect_service",
            "set_service": (scoped_service or {}).get("display") or conversation.get("service"),
        }
    recent_outbound_text = get_last_outbound_text(history).lower()
    recent_overview_offer = bool(any(cue in recent_outbound_text for cue in ["web tasarim", "otomasyon", "reklam", "sosyal medya"]) and (recent_outbound_offered_consultation(history) or memory.get("pending_offer") == "preconsultation_offer"))
    if not conversation.get("service") and recent_overview_offer and is_all_choice_message(message_text):
        return {
            "reply": "Anladim, birden fazla alan ilginizi cekiyor. En hizli ilerlemek icin once ana onceligi netlestirelim: en acil ihtiyaciniz web tasarim mi, otomasyon mu, reklam yonetimi mi yoksa sosyal medya mi?",
            "kind": "overview_service_selection",
            "next_state": "collect_service",
            "set_service": None,
        }
    if is_confirmation_acceptance_message(message_text) and (recent_outbound_offered_consultation(history) or memory.get("pending_offer") == "preconsultation_offer"):
        if not conversation.get("service") and recent_overview_offer:
            return {
                "reply": "Harika. O zaman once size en uygun basligi netlestirelim: web tasarim, otomasyon, reklam yonetimi veya sosyal medya tarafindan hangisi sizin icin daha oncelikli?",
                "kind": "overview_service_selection",
                "next_state": "collect_service",
                "set_service": None,
            }
        return {
            "reply": build_offer_acceptance_reply(conversation),
            "kind": "confirmation_acceptance",
            "next_state": "collect_name",
            "set_service": conversation.get("service") or "Otomasyon & Yapay Zeka Çözümleri",
            "set_booking_kind": "preconsultation",
            "force_next_state": True,
        }
    if is_offer_hesitation_message(message_text) and (recent_outbound_offered_consultation(history) or memory.get("pending_offer") == "preconsultation_offer"):
        return {
            "reply": build_offer_hesitation_reply(conversation, history),
            "kind": "offer_hesitation",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or "Otomasyon & Yapay Zeka Çözümleri",
        }
    if is_business_need_analysis_message(message_text):
        sector = detect_business_sector(message_text, history)
        return {
            "reply": build_business_owner_need_reply(sector),
            "kind": "business_owner_need_analysis",
            "next_state": "collect_service",
            "set_service": "Otomasyon & Yapay Zeka Çözümleri" if sector == "beauty" else conversation.get("service"),
        }
    if is_business_context_intro_message(message_text, history):
        sector = detect_business_sector(message_text, history)
        current_service = sanitize_text(conversation.get("service") or "").lower()
        is_web_design_flow = any(keyword in current_service for keyword in ["web", "site", "kurumsal"])
        return {
            "reply": build_sector_intro_reply(sector, conversation),
            "kind": "sector_intro",
            "next_state": "collect_name" if is_web_design_flow else "collect_service",
            "set_service": conversation.get("service") or ("Otomasyon & Yapay Zeka Çözümleri" if sector == "beauty" and not is_web_design_flow else conversation.get("service")),
            "set_booking_kind": "preconsultation" if is_web_design_flow else None,
        }
    priority_choice = detect_priority_choice(message_text) if recent_outbound_requested_priority(history) else None
    if priority_choice and priority_choice != "all":
        return {
            "reply": build_priority_choice_reply(priority_choice, conversation, history),
            "kind": "priority_choice",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or "Otomasyon & Yapay Zeka Çözümleri",
        }
    if is_all_choice_message(message_text) and recent_outbound_requested_priority(history):
        sector = detect_business_sector(message_text, history)
        return {
            "reply": build_multi_need_confirmed_reply(sector),
            "kind": "multi_need_confirmed",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or ("Otomasyon & Yapay Zeka Çözümleri" if sector in {"beauty", "real_estate"} else None),
        }
    dm_issue_choice = detect_dm_issue_choice(message_text) if (recent_outbound_requested_dm_issue(history) or memory.get("last_bot_question_type") == "dm_issue") else None
    if dm_issue_choice:
        sector = detect_business_sector(message_text, history)
        return {
            "reply": build_dm_issue_followup_reply(dm_issue_choice, conversation, history),
            "kind": "dm_issue_detail",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or ("Otomasyon & Yapay Zeka Çözümleri" if sector in {"beauty", "real_estate"} else None),
        }
    if (recent_outbound_requested_message_volume(history) or memory.get("last_bot_question_type") == "message_volume") and is_message_volume_answer(message_text):
        sector = detect_business_sector(message_text, history)
        return {
            "reply": build_message_volume_reply(message_text, conversation, history),
            "kind": "message_volume",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or ("Otomasyon & Yapay Zeka Çözümleri" if sector in {"beauty", "real_estate"} else None),
        }
    if current_state == "collect_phone" and is_phone_share_refusal(message_text):
        return {
            "reply": build_phone_refusal_reply(conversation),
            "kind": "phone_refusal",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
            "clear_booking": True,
        }
    if memory.get("offer_status") == "declined" and memory.get("open_loop") == "decline_cooldown" and is_phone_share_refusal(message_text):
        return {
            "reply": build_phone_refusal_reply(conversation),
            "kind": "phone_refusal",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
            "clear_booking": True,
        }
    if is_request_reason_question(message_text) and current_state in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
        return {
            "reply": build_contextual_clarification_reply(conversation, message_text),
            "kind": "clarification",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_clarification_request(message_text):
        return {
            "reply": build_contextual_clarification_reply(conversation, message_text),
            "kind": "clarification",
            "next_state": conversation.get("state", "collect_service") or "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_company_background_question(message_text):
        return {
            "reply": build_company_background_reply(),
            "kind": "company_background",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    if matched_service and is_price_negotiation_message(message_text, llm_data):
        return {
            "reply": build_price_negotiation_reply(matched_service, message_text),
            "kind": "price_owner_handoff",
            "next_state": "human_handoff",
            "set_service": matched_service["display"],
            "handoff": True,
        }
    if recent_outbound_answered_price(history) and detect_price_scope_clarification(message_text):
        return {
            "reply": build_price_scope_clarification_reply(matched_service or (service_catalog.get(conversation.get("service")) if conversation.get("service") else None)),
            "kind": "price_followup",
            "next_state": "collect_service",
            "set_service": (matched_service or (service_catalog.get(conversation.get("service")) if conversation.get("service") else None) or {}).get("display"),
        }
    if matched_service and is_price_followup_message(message_text, llm_data):
        return {
            "reply": build_price_followup_reply(matched_service, message_text, conversation),
            "kind": "price_followup",
            "next_state": "collect_service",
            "set_service": matched_service["display"],
        }
    objection_type = match_objection_type(message_text)
    if objection_type:
        objection_reply = build_objection_reply(objection_type, conversation)
        if objection_type == "hesitation":
            memory["pending_offer"] = None
            memory["offer_status"] = "declined"
            memory["open_loop"] = "decline_cooldown"
            memory["last_bot_question_type"] = None
            memory["last_priority_choice"] = None
            memory["last_dm_issue_choice"] = None
            sync_conversation_memory_summary(conversation)
            return {
                "reply": objection_reply,
                "kind": "objection",
                "next_state": conversation.get("state", "collect_service") or "collect_service",
                "set_service": conversation.get("service"),
            }
        return {
            "reply": objection_reply,
            "kind": "objection",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
        }
    if (is_service_advice_request(message_text, llm_data) or is_comparison_request(message_text, matched_services, llm_data)) and is_message_volume_answer(message_text):
        sector = detect_business_sector(message_text, history)
        return {
            "reply": build_message_volume_reply(message_text, conversation, history),
            "kind": "message_volume",
            "next_state": "collect_service",
            "set_service": conversation.get("service") or ("Otomasyon & Yapay Zeka Çözümleri" if sector in {"beauty", "real_estate"} or matched_services else None),
        }
    if (
        not direct_service_match
        and not is_working_schedule_question(message_text)
        and not is_company_background_question(message_text)
        and not is_service_overview_question(message_text)
        and not match_faq_response(message_text)
        and should_use_generic_ai_reply(message_text, llm_data, conversation)
    ):
        return {
            "reply": build_generic_ai_draft_reply(message_text, conversation, history),
            "kind": "generic_ai",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
        }
    if is_service_advice_request(message_text, llm_data) or is_comparison_request(message_text, matched_services, llm_data):
        return build_service_advice_reply(message_text, matched_services, llm_data, conversation)
    if direct_service_match and matched_service and current_state == "collect_service" and not message_shows_booking_intent(message_text, llm_data) and not asks_detail and not is_price_question(message_text) and not is_price_followup_message(message_text, llm_data) and not is_price_negotiation_message(message_text, llm_data) and len(sanitize_text(message_text).split()) <= 3:
        return {
            "reply": build_service_info_reply(matched_service, conversation),
            "kind": "service_info",
            "next_state": "collect_service",
            "set_service": matched_service["display"],
        }
    if matched_service and (is_price_question(message_text) or (llm_data.get("intent") == "info" and asks_detail) or asks_detail):
        booking_intent = message_shows_booking_intent(message_text, llm_data)
        if booking_intent:
            reply = build_booking_ready_service_reply(matched_service, price_context=is_price_question(message_text))
        else:
            reply = build_price_question_reply(matched_service, conversation) if is_price_question(message_text) else build_service_info_reply(matched_service, conversation)
        return {
            "reply": reply,
            "kind": "price_question" if is_price_question(message_text) else "service_info",
            "next_state": "collect_name" if booking_intent else "collect_service",
            "set_service": matched_service["display"],
        }
    if is_price_question(message_text) and not matched_service:
        return {
            "reply": "Net fiyat, seçilecek hizmete göre değişiyor. Web tasarım, otomasyon & yapay zeka, performans pazarlama veya sosyal medya yönetiminden hangisiyle ilgilendiğinizi yazarsanız size doğru bilgiyi paylaşayım.",
            "kind": "price_route",
            "next_state": "collect_service",
            "set_service": None,
        }
    if is_service_overview_question(message_text):
        return {
            "reply": build_services_overview_reply(),
            "kind": "overview",
            "next_state": "collect_service",
            "set_service": None,
        }
    if is_working_schedule_question(message_text):
        return {
            "reply": build_working_schedule_reply() + " İsterseniz ilgilendiğiniz hizmeti de yazın, sizi doğru alana yönlendireyim.",
            "kind": "working_schedule",
            "next_state": "collect_service",
            "set_service": conversation.get("service") if has_booking_context else None,
        }
    faq_answer = match_faq_response(message_text)
    if faq_answer:
        return {
            "reply": build_faq_reply(faq_answer),
            "kind": "faq",
            "next_state": "collect_service" if not conversation.get("service") else conversation.get("state", "collect_service"),
            "set_service": conversation.get("service"),
        }
    if should_use_generic_ai_reply(message_text, llm_data, conversation):
        return {
            "reply": build_generic_ai_draft_reply(message_text, conversation, history),
            "kind": "generic_ai",
            "next_state": "collect_service",
            "set_service": conversation.get("service"),
        }
    return None


def should_use_generic_ai_reply(message_text: str, llm_data: dict[str, Any] | None, conversation: dict[str, Any]) -> bool:
    cleaned = sanitize_text(message_text)
    if not cleaned:
        return False
    if is_business_fit_question(cleaned):
        return True
    current_state = sanitize_text(conversation.get("state") or "new")
    if current_state not in {"new", "collect_service", "human_handoff"}:
        if "?" not in cleaned:
            return False
        if extract_phone(cleaned) or extract_date(cleaned) or extract_time_for_state(cleaned, current_state):
            return False
    llm_data = llm_data or {}
    if llm_data.get("intent") == "appointment":
        return False
    if is_low_signal_message(cleaned) and "?" not in cleaned:
        return False
    return not bool(
        message_shows_booking_intent(cleaned, llm_data)
        or wants_availability_information(cleaned, llm_data)
        or extract_phone(cleaned)
        or extract_date(cleaned)
        or extract_time_for_state(cleaned, current_state)
    )


def is_business_fit_question(message_text: str) -> bool:
    lowered = sanitize_text(message_text).lower()
    if "?" not in lowered:
        return False
    fit_words = ["uygun", "uyar", "olur mu", "mantıklı", "mantikli"]
    business_words = ["ajans", "firma", "şirket", "sirket", "işletme", "isletme", "marka", "sektör", "sektor", "bizim", "bize"]
    if not any(word in lowered for word in fit_words):
        return False
    if not any(word in lowered for word in business_words):
        return False
    return not bool(extract_date(lowered) or extract_time_for_state(lowered, "collect_service"))


def build_generic_ai_draft_reply(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    lowered = sanitize_text(message_text).lower()
    service = display_service_name(conversation.get("service"))
    if any(token in lowered for token in ["turkiye baskenti", "turkiye'nin baskenti", "turkiyenin baskenti", "türkiye baskenti"]):
        return "Türkiye'nin başkenti Ankara'dır."
    if any(token in lowered for token in ["güvenli", "guvenli", "güvenlik", "guvenlik", "güvenli mi", "guvenli mi"]):
        return "Evet, doğru kurulumda güvenli çalışır; erişimler, müşteri verisi ve otomasyon adımları kontrollü şekilde yapılandırılır. İsterseniz hangi verilerin işleneceğini yazın, riskleri net söyleyeyim."
    if any(token in lowered for token in ["dünyanın başkenti", "dunyanin baskenti", "dünya başkenti", "dunya baskenti"]):
        return "Dünyanın tek bir başkenti yok; her ülkenin kendi başkenti var. İsterseniz belirli bir ülkeyi yazın, başkentini söyleyeyim."
    if any(token in lowered for token in ["uyar", "uygun", "mantıklı mı", "mantikli mi", "bize olur mu"]):
        return "Evet, tekrar eden mesaj, randevu veya müşteri takibi varsa bu sistem size uygun olabilir. En çok hangi süreci hızlandırmak istiyorsunuz?"
    if any(token in lowered for token in ["nasıl çalış", "nasil calis", "nasıl oluyor", "nasil oluyor", "sistem nasıl", "sistem nasil"]):
        return "Sistem gelen mesajı anlayıp uygun cevabı verir, gerekirse randevu veya müşteri kaydına bağlar. Hangi akışı otomatikleştirmek istiyorsunuz?"
    if any(token in lowered for token in ["ne yapıyorsunuz", "ne yapiyorsunuz", "ne iş", "ne is", "kimsiniz"]):
        return "DOEL; web tasarım, yapay zeka otomasyon, reklam ve sosyal medya süreçlerinde markalara destek verir. Şu an hangi tarafı geliştirmek istiyorsunuz?"
    if any(token in lowered for token in ["cevap verir misin", "soru soruyorum", "alakasiz", "alakasız"]):
        return "Evet, sorabilirsiniz; bildiğim konularda doğrudan cevaplarım, DOEL hizmetleriyle ilgiliyse ayrıca net yönlendiririm."
    if service:
        return f"{service} tarafında yardımcı olabilirim. Sorunuzu netleştirirseniz size en pratik yolu söyleyeyim."
    if "?" in lowered:
        return "Sorunuzu doğrudan cevaplayayım; bildiğim kısmı net aktarırım, emin olmadığım yerde de uydurmadan belirtirim."
    return "Anladım. Buradan mesajınızı değerlendirip size en uygun cevabı vermeye çalışacağım."


def pick_service(text: str, llm_service: str | None) -> str | None:
    cleaned_text = sanitize_text(text)
    if is_invalid_service_candidate(cleaned_text):
        return None

    matched_services = match_service_candidates(cleaned_text, llm_service)
    if is_service_advice_request(cleaned_text) or is_comparison_request(cleaned_text, matched_services):
        return None
    if matched_services:
        return matched_services[0]["display"]

    hint_match = match_service_catalog(llm_service, cleaned_text)
    if hint_match:
        return hint_match["display"]
    return None


def apply_detected_service_to_conversation(conversation: dict[str, Any], message_text: str, llm_service_hint: str | None = None) -> str | None:
    direct_service = pick_service(message_text, None)
    picked_service = direct_service or pick_service(message_text, llm_service_hint)
    if not picked_service:
        return None

    current_service = sanitize_text(conversation.get("service") or "")
    state = sanitize_text(conversation.get("state") or "new")
    current_match = match_service_catalog(current_service, current_service) if current_service else None
    current_display = (current_match or {}).get("display") or current_service
    active_booking_states = {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}
    explicit_override = bool(direct_service and current_display and direct_service != current_display and state in active_booking_states)

    if not current_display or state == "collect_service" or explicit_override:
        conversation["service"] = picked_service
        return picked_service
    return None


def elapsed_ms(started_at: float) -> int:
    return int((time_module.perf_counter() - started_at) * 1000)


def should_call_llm_extractor(message_text: str, conversation: dict[str, Any]) -> bool:
    cleaned = sanitize_text(message_text)
    if not LLM_BASE_URL or not LLM_API_KEY:
        return False
    if not cleaned:
        return False
    if is_voice_duration_placeholder_message(message_text):
        return False
    return True


SKIP_POLISH_LABELS = {
    "collect_service_with_availability",
    "collect_service_no_availability",
    "collect_time",
    "collect_time_period_full",
    "collect_time_no_availability",
    "collect_period",
    "slot_taken",
    "invalid_slot",
    "slot_conflict_race",
    "crm_error_handoff",
    "appointment_created",
}


def should_ai_compose_reply(
    message_type: str,
    decision_label: str | None,
    *,
    handoff: bool = False,
    appointment_created: bool = False,
    conversation: dict[str, Any] | None = None,
) -> bool:
    if not FULL_AI_CONVERSATIONAL_MODE and not LLM_REPLY_POLISH_ENABLED:
        return False

    _ = (message_type, handoff, appointment_created, conversation)
    return True


MICRO_COMPOSE_LABELS = {
    "greeting_collect_service",
    "collect_service",
    "collect_name",
    "collect_phone",
    "collect_date",
    "collect_period",
    "collect_time",
    "collect_date_after_availability",
    "collect_service_with_availability",
    "collect_phone_required_for_booking",
    "clarify_next_step",
    "info:presence_check",
    "info:greeting",
    "info:greeting_interrupt",
    "info:smalltalk",
    "info:voice_placeholder",
}
GUARDED_COMPOSE_LABELS = {
    "confirmed_followup",
    "existing_customer_appointment",
    "slot_taken",
    "invalid_slot",
    "appointment_created",
    "crm_error_handoff",
    "human_handoff",
    "confirmed_change_handoff",
    "confirmed_identity_mismatch_handoff",
}
QUALITY_COMPOSE_LABELS = {
    "info:business_owner_need_analysis",
    "info:comparison",
    "info:dm_issue_detail",
    "info:fatigue_painpoint",
    "info:message_volume",
    "info:multi_need_confirmed",
    "info:objection",
    "info:offer_hesitation",
    "info:price_negotiation",
    "info:priority_choice",
    "info:service_advice",
    "info:technical_issue",
}


def unique_model_chain(*models: str | None) -> list[str]:
    chain: list[str] = []
    for model in models:
        model = (model or "").strip()
        if model and model not in chain:
            chain.append(model)
    return chain


def is_quality_model_question(text: str | None) -> bool:
    lowered = sanitize_text(text or "").lower()
    if not lowered:
        return False
    if any(phrase in lowered for phrase in ["hangisi daha mantikli", "hangisi mantikli", "ne daha mantikli", "bana hangisi"]):
        return True
    strategic_terms = ["dm", "randevu", "musteri takibi", "crm", "otomasyon", "fatura", "teklif"]
    if sum(1 for term in strategic_terms if term in lowered) >= 2 and any(term in lowered for term in ["karis", "toparla", "birlikte", "hepsi", "oner", "oneri"]):
        return True
    return False


def is_normal_model_question(text: str | None) -> bool:
    lowered = sanitize_text(text or "").lower()
    if not lowered:
        return False
    if is_quality_model_question(lowered):
        return True
    if len(lowered.split()) >= 4:
        return True
    return any(term in lowered for term in ["nasil", "fiyat", "ucret", "sistem", "otomasyon", "web", "reklam", "sosyal medya"])


def explicitly_starts_consultation_collection(text: str) -> bool:
    lowered = sanitize_text(text or "").lower()
    phrases = [
        "randevu almak", "randevu alalım", "randevu alalim", "randevu oluştur",
        "randevu olustur", "görüşme planla", "gorusme planla", "görüşelim",
        "goruselim", "konuşalım", "konusalim", "toplantı yapalım", "toplanti yapalim",
        "başlayalım", "baslayalim", "devam edelim", "beni arayın", "beni arayin",
        "telefonla konuşalım", "telefonla konusalim",
    ]
    return any(phrase in lowered for phrase in phrases)


def build_ai_reply_goal(decision_label: str | None, conversation: dict[str, Any]) -> str:
    label = sanitize_text(decision_label or "").lower()
    goal_map = {
        "greeting_collect_service": "Reply to the greeting naturally in Turkish, sound human, and ask how you can help without listing all services.",
        "collect_service": "Respond naturally in Turkish, answer what they actually asked, avoid vague corporate phrases, mention only concrete help areas if needed, and ask one useful follow-up without listing all services unless they explicitly asked. If the message is only smalltalk or wellbeing talk like nasılsın / napıyorsunuz / ne yapıyorsunuz, reply socially in one short sentence first and do not propose a meeting or consultation.",
        "collect_name": "Ask for their full name first in a short, natural Turkish sentence.",
        "collect_phone": "Acknowledge briefly and ask for their phone number in one short Turkish sentence.",
        "collect_date": "Ask which day works for them, mentioning working hours briefly.",
        "collect_period": "Ask whether morning or afternoon is better in one short Turkish sentence.",
        "collect_date_after_availability": "Acknowledge the request and ask which day works for them.",
        "clarify_next_step": "Ask for the single most important missing detail.",
        "confirmed_followup": "Reassure the customer about their confirmed booking in a calm human tone.",
        "info:greeting": "Reply to a simple greeting naturally in short Turkish and ask how you can help.",
        "info:greeting_interrupt": "Briefly greet the customer in natural Turkish and continue from the current conversation step without sounding robotic.",
        "info:fatigue_painpoint": "Treat the message as frustration or fatigue, acknowledge the emotion like a human in Turkish, never greet again, and ask one calm follow-up that helps clarify what is bothering them.",
        "info:business_owner_need_analysis": "Reply like a sharp consultant in natural Turkish, use the business context, and recommend the most logical need without listing everything.",
        "info:sector_intro": "Treat the message as a sector/business context introduction, acknowledge it naturally, infer the most relevant pressure points, and ask one useful discovery question.",
        "info:priority_choice": "Treat the message as an answer to your previous prioritization question and continue from that answer without restarting discovery.",
        "info:dm_issue_detail": "Treat the message as a concrete DM problem answer, acknowledge it naturally, and move to the next logical diagnostic step without looping back.",
        "info:message_volume": "Treat the message as a workload-volume answer, quantify the pressure briefly, recommend the most logical automation direction, and do not repeat earlier discovery questions.",
        "info:offer_hesitation": "Treat the customer as interested but undecided about the proposed meeting; reassure them briefly and continue with one low-pressure clarifying step instead of restarting the same loop.",
        "info:phone_refusal": "Acknowledge the customer's refusal calmly, do not pressure them, explain the alternative path briefly, and keep the conversation human and open.",
        "info:multi_need_confirmed": "Acknowledge that multiple needs exist, summarize them clearly, and offer a short next step without repeating the same discovery question.",
        "info:confirmation_acceptance": "Treat the customer as having accepted the proposed next step and move directly into booking collection in a natural Turkish sentence.",
        "info:presence_check": "Confirm briefly that you are here in natural Turkish and invite them to say what they need in one short sentence.",
        "info:voice_placeholder": "Politely explain that the voice message may not have been understood correctly and ask the customer to write it briefly in natural Turkish.",
        "info:technical_issue": "Treat the message as a technical complaint, acknowledge the issue calmly in Turkish, never switch into sales discovery, and ask only one short clarifying question about which message triggered the wrong auto-reply.",
        "info:owner_check": "Answer directly in short natural Turkish that you are helping on behalf of the business. Do not sound evasive or robotic. Keep it to one short sentence unless a booking context needs one short continuation.",
        "info:assistant_identity": "Answer directly who you are in short natural Turkish as the business DM assistant/support side. Do not sound vague, robotic, or overly corporate.",
        "info:clarification": "Clarify the previous step in simple Turkish and help the customer continue without sounding repetitive.",
        "info:booking_reset": "Acknowledge the misunderstanding briefly, drop any booking assumption, and return to a normal information conversation in natural Turkish.",
        "info:service_advice": "Answer like a consultant, not a brochure: recommend the most logical service briefly, explain why simply, and ask one useful follow-up question.",
        "info:comparison": "Compare the most relevant options briefly, explain the decision logic simply, and ask one useful follow-up question.",
        "info:service_info": "Answer what the customer asked in natural Turkish without dumping a service brochure; mention only the most relevant benefit and one useful next step.",
        "info:price_question": "Answer the pricing question directly in natural Turkish, then ask one smart follow-up that helps qualify the lead.",
        "info:price_followup": "Answer the price follow-up clearly in natural Turkish, resolve the exact scope confusion first, and do not drift back into generic discovery before answering.",
        "info:price_negotiation": "Respond like a smart sales assistant in natural Turkish: acknowledge the budget concern, avoid repeating the full service paragraph, and ask one useful qualifying question.",
        "info:price_route": "Acknowledge the price question and ask which service they want pricing for.",
        "info:company_background": "Answer the company/about/experience question directly in short natural Turkish. If the exact year or duration is not known in the facts, do not invent it; instead give a brief honest introduction about who the business is, what it does, and its working focus. Do not redirect into service selection, pricing, or discovery unless the customer asks for that next.",
        "info:overview": "Summarize the core services naturally without sounding like a catalog and ask which one is most relevant to them.",
        "info:faq": "Answer the question clearly in natural Turkish and gently guide the next step.",
        "info:generic_ai": "Answer the customer's message directly in natural Turkish. Do not fall back to vague discovery phrases. If the exact answer needs context, give the most useful short answer first and ask only one focused follow-up question. Do not ask for phone, date, time, meeting, or appointment unless the customer explicitly asked to start booking.",
        "info:objection": "Respond reassuringly without pressure and keep the conversation open.",
        "info:decline_cooldown": "The customer has already declined or is closing the conversation. Reply in one very short, polite Turkish sentence, acknowledge the close naturally, and do not ask any new question or reopen sales.",
        "collect_time": "Present the available time slots briefly and ask which one works best.",
        "collect_service_with_availability": "Show available slots for the requested date and ask which service they need.",
        "collect_service_no_availability": "Explain no slots are available for that date, suggest alternatives, and ask which service they need.",
        "collect_time_period_full": "Explain no slots in the requested period, offer to check other times.",
        "collect_time_no_availability": "Explain no availability on that date and suggest the nearest alternatives.",
        "collect_phone_required_for_booking": "Acknowledge their name and explain that a phone number is needed to complete the booking.",
        "service_info_continue": "Continue the service information conversation naturally without repeating what was already said.",
        "slot_taken": "Explain the selected time is taken, present alternatives, and ask which one works.",
        "invalid_slot": "Explain the time slot issue clearly and ask them to pick a valid time.",
        "existing_customer_appointment": "Inform them they already have an active booking and offer to help with changes if needed.",
        "appointment_created": "Confirm the booking in natural Turkish while keeping every factual detail from the fallback reply exactly correct.",
        "crm_error_handoff": "Explain the temporary system issue calmly in natural Turkish and direct the customer to the safe next step without inventing details.",
        "human_handoff": "Explain naturally that a human teammate will continue and keep the wording concise and reassuring.",
        "confirmed_change_handoff": "Explain naturally that an already confirmed booking change request is being handed to the team, keeping the factual details intact.",
        "confirmed_identity_mismatch_handoff": "Explain naturally that the booking seems to belong to someone else and that the issue is being handed to the team, without changing any facts.",
        "slot_conflict_race": "Explain the slot just got taken and present alternatives.",
        "info:smalltalk": "Reply to smalltalk naturally in one short Turkish sentence. Be social first, simple, and human. Do not propose a meeting or consultation on smalltalk.",
    }
    customer_message = conversation.get("last_customer_message") or ""
    if label == "collect_service" and is_quality_model_question(customer_message):
        return "Recommend the most logical solution in Turkish, explain why briefly, and ask one discovery question. Do not ask for phone, date, time, meeting, or appointment unless the customer explicitly asked to start booking."
    if label in goal_map:
        return goal_map[label]
    if conversation.get("state") == "collect_name":
        return goal_map["collect_name"]
    if conversation.get("state") == "collect_phone":
        return goal_map["collect_phone"]
    if conversation.get("state") == "collect_date":
        return goal_map["collect_date"]
    if conversation.get("state") == "collect_period":
        return goal_map["collect_period"]
    return "Write a short, human, helpful Turkish reply that moves the conversation forward naturally."


def build_compact_known_facts(conversation: dict[str, Any], *, include_contact: bool = False) -> dict[str, Any]:
    compact: dict[str, Any] = {}
    for key in ["state", "service", "requested_date"]:
        value = conversation.get(key)
        if value:
            compact[key] = value
    requested_time = str(conversation.get("requested_time") or "")[:5]
    if requested_time:
        compact["requested_time"] = requested_time
    appointment_status = conversation.get("appointment_status")
    if appointment_status:
        compact["appointment_status"] = appointment_status
    memory = ensure_conversation_memory(conversation)
    if not memory.get("conversation_summary"):
        memory["conversation_summary"] = build_conversation_memory_summary(conversation)
    memory_compact = {
        key: memory.get(key)
        for key in [
            "customer_goal",
            "customer_sector",
            "pain_points",
            "last_bot_question_type",
            "answered_question_types",
            "open_loop",
            "pending_offer",
            "offer_status",
            "conversation_summary",
            "last_priority_choice",
            "last_dm_issue_choice",
            "message_volume_estimate",
        ]
        if memory.get(key)
    }
    if memory_compact:
        compact["memory"] = memory_compact
    if include_contact:
        if conversation.get("phone"):
            compact["phone"] = conversation["phone"]
        if conversation.get("full_name"):
            compact["full_name"] = conversation["full_name"]
    return compact


def build_recent_history_lines(history: list[dict[str, Any]] | None, limit: int) -> list[str]:
    if limit <= 0:
        return []
    compact_history: list[str] = []
    for item in (history or [])[-limit:]:
        direction = "müşteri" if item.get("direction") == "in" else "asistan"
        compact_history.append(f"{direction}: {sanitize_text(item.get('message_text') or '')}")
    return compact_history


def get_ai_compose_profile(decision_label: str | None, conversation: dict[str, Any]) -> dict[str, Any]:
    label = sanitize_text(decision_label or "").lower()
    customer_message = conversation.get("last_customer_message") or ""
    reply_model = LLM_MODEL or "meta-llama/llama-4-scout-17b-16e-instruct"
    micro_model = LLM_REPLY_MICRO_MODEL or reply_model
    advisory_model = LLM_REPLY_ADVISORY_MODEL or reply_model
    fallback_model = LLM_FALLBACK_MODEL or micro_model
    quality_model = LLM_REPLY_QUALITY_MODEL or advisory_model
    micro_chain = unique_model_chain(micro_model)
    normal_chain = unique_model_chain(advisory_model, fallback_model, micro_model)
    quality_chain = unique_model_chain(quality_model, advisory_model, fallback_model, micro_model)
    if label == "collect_service" and is_quality_model_question(customer_message):
        return {
            "profile": "quality_collect_service",
            "timeout": max(LLM_REPLY_ADVISORY_TIMEOUT_SECONDS, 7.0),
            "max_tokens": max(120, LLM_REPLY_ADVISORY_MAX_TOKENS),
            "temperature": 0.2,
            "models": quality_chain,
            "history_limit": 6,
            "include_fallback": False,
            "include_contact": False,
            "prefer_plain_prompt": True,
            "allow_retry": True,
            "fast_path": False,
        }
    if label == "collect_service" and is_normal_model_question(customer_message):
        return {
            "profile": "normal_collect_service",
            "timeout": max(LLM_REPLY_ADVISORY_TIMEOUT_SECONDS, 7.0),
            "max_tokens": max(96, LLM_REPLY_ADVISORY_MAX_TOKENS),
            "temperature": 0.2,
            "models": normal_chain,
            "history_limit": 5,
            "include_fallback": False,
            "include_contact": False,
            "prefer_plain_prompt": True,
            "allow_retry": True,
            "fast_path": False,
        }
    if label in MICRO_COMPOSE_LABELS:
        micro_timeout = LLM_REPLY_MICRO_TIMEOUT_SECONDS
        if label == "collect_service":
            micro_timeout = max(micro_timeout, 4.6)
        if label in {"info:smalltalk", "info:presence_check", "info:greeting", "greeting_collect_service", "info:greeting_interrupt"}:
            micro_timeout = max(micro_timeout, 4.8)
        return {
            "profile": "micro",
            "timeout": micro_timeout,
            "max_tokens": max(48, min(LLM_REPLY_MICRO_MAX_TOKENS, 64)),
            "temperature": 0.15,
            "models": micro_chain or normal_chain,
            "history_limit": 4,
            "include_fallback": False,
            "include_contact": False,
            "prefer_plain_prompt": True,
            "allow_retry": False,
            "fast_path": True,
        }
    if label == "info:company_background":
        return {
            "profile": "background_fast",
            "timeout": max(9.5, LLM_REPLY_ADVISORY_TIMEOUT_SECONDS),
            "max_tokens": 28,
            "temperature": 0.0,
            "models": micro_chain or normal_chain,
            "history_limit": 0,
            "include_fallback": True,
            "include_contact": False,
            "prefer_plain_prompt": True,
            "allow_retry": True,
            "fast_path": True,
            "background_fast": True,
        }
    if label in GUARDED_COMPOSE_LABELS:
        return {
            "profile": "guarded",
            "timeout": LLM_REPLY_ADVISORY_TIMEOUT_SECONDS,
            "max_tokens": max(100, LLM_REPLY_ADVISORY_MAX_TOKENS),
            "temperature": 0.2,
            "models": normal_chain,
            "history_limit": 4,
            "include_fallback": True,
            "include_contact": True,
            "prefer_plain_prompt": True,
            "allow_retry": True,
        }
    advisory_timeout = LLM_REPLY_ADVISORY_TIMEOUT_SECONDS
    fast_service_label = label in {"info:service_info", "service_info_continue", "info:service_advice", "info:comparison", "info:objection", "info:decline_cooldown", "info:price_question", "info:price_followup", "info:price_route", "info:company_background"}
    if fast_service_label:
        if label.startswith("info:price"):
            advisory_timeout = max(advisory_timeout, 6.2)
        elif label == "info:company_background":
            advisory_timeout = max(advisory_timeout, 7.2)
        else:
            advisory_timeout = max(advisory_timeout, 5.8)
    return {
        "profile": "advisory",
        "timeout": max(advisory_timeout, 7.0),
        "max_tokens": max(120, LLM_REPLY_ADVISORY_MAX_TOKENS),
        "temperature": 0.2,
        "models": quality_chain if label in QUALITY_COMPOSE_LABELS else normal_chain,
        "history_limit": 6,
        "include_fallback": False,
        "include_contact": False,
        "prefer_plain_prompt": True,
        "allow_retry": True,
        "fast_path": False,
    }


def maybe_polish_reply_text(
    draft_reply: str | None,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
    enabled: bool = False,
    decision_label: str | None = None,
) -> tuple[str | None, int]:
    if not draft_reply:
        return None, 0
    if not enabled or not (FULL_AI_CONVERSATIONAL_MODE or LLM_REPLY_POLISH_ENABLED):
        return draft_reply, 0

    started_at = time_module.perf_counter()
    try:
        polished = polish_reply_text(draft_reply, conversation, history, decision_label)
        final_text = (polished or "").strip()
        if not final_text:
            return draft_reply, elapsed_ms(started_at)
        return final_text, elapsed_ms(started_at)
    except Exception:
        logger.warning("polish_reply_text failed; returning draft_reply", exc_info=True)
        return draft_reply, elapsed_ms(started_at)


def build_emergency_reply(message_text: str, conversation: dict[str, Any], decision_label: str | None = None) -> str:
    lowered = sanitize_text(message_text).lower()
    if is_technical_issue_message(message_text):
        return "Anladım, teknik bir sorun görünüyor. Hangi mesaja yanlış ya da eksik otomatik cevap gittiğini kısa bir örnekle yazar mısınız?"
    if is_service_overview_question(message_text):
        if any(keyword in lowered for keyword in DETAIL_KEYWORDS):
            return build_detailed_services_overview_reply()
        return build_services_overview_reply()
    if is_simple_greeting(message_text):
        return "Merhaba, yardımcı olayım. Web tasarım, otomasyon, reklam veya sosyal medya tarafında hangi konuyla ilgileniyorsunuz?"
    if ensure_conversation_memory(conversation).get("offer_status") == "declined" and (is_closeout_message(message_text) or is_low_signal_message(message_text)):
        return "Tabii, acelesi yok. Aklınıza takılan bir şey olursa ya da ilerleyen günlerde bakmak isterseniz buradayım."
    if conversation.get("service"):
        service_meta = match_service_catalog(conversation.get("service"), conversation.get("service"))
        if service_meta:
            return build_service_info_reply(service_meta, conversation)
    contact = build_contact_text()
    if conversation.get("appointment_status") == "confirmed":
        return f"Şu an teknik bir yoğunluk var, işleminiz için yetkili ekibe yönlendiriyorum. Dilerseniz {contact} ulaşabilirsiniz."
    return "Size en doğru şekilde yardımcı olabilmem için kısaca ilgilendiğiniz hizmeti (web, otomasyon, reklam vb.) yazar mısınız?"


def summarize_memory_trace(memory: dict[str, Any] | None) -> dict[str, Any]:
    memory = memory or {}
    return {
        "pending_offer": memory.get("pending_offer"),
        "offer_status": memory.get("offer_status"),
        "open_loop": memory.get("open_loop"),
        "last_bot_question_type": memory.get("last_bot_question_type"),
        "last_priority_choice": memory.get("last_priority_choice"),
        "last_dm_issue_choice": memory.get("last_dm_issue_choice"),
        "current_topic": memory.get("current_topic"),
        "conversation_summary": memory.get("conversation_summary"),
    }


def should_trace_decline_memory(message_text: str, conversation: dict[str, Any], llm_data: dict[str, Any] | None = None) -> bool:
    lowered = sanitize_text(message_text).lower()
    memory = ensure_conversation_memory(conversation)
    objection_type = sanitize_text(str((llm_data or {}).get("objection_type") or match_objection_type(message_text) or "")).lower()
    return bool(
        memory.get("offer_status") == "declined"
        or objection_type == "hesitation"
        or lowered in {"pekala", "peki", "tamam", "istemiyom", "istemiyorum", "ilgilenmiyorum", "gerek yok"}
        or is_closeout_message(message_text)
    )


def log_decline_memory_trace(stage: str, sender_id: str, trace_id: str | None, conversation: dict[str, Any], *, extra: dict[str, Any] | None = None) -> None:
    snapshot = {
        "state": conversation.get("state"),
        "service": conversation.get("service"),
        "booking_kind": conversation.get("booking_kind"),
        "appointment_status": conversation.get("appointment_status"),
        "last_customer_message": conversation.get("last_customer_message"),
        "memory": summarize_memory_trace(ensure_conversation_memory(conversation)),
    }
    if extra:
        snapshot["extra"] = extra
    logger.warning(
        "decline_memory_trace stage=%s trace_id=%s sender_id=%s snapshot=%s",
        stage,
        trace_id,
        sender_id,
        json.dumps(snapshot, ensure_ascii=False),
    )


def list_llm_models() -> list[str]:
    models = [LLM_MODEL]
    if LLM_FALLBACK_MODEL and LLM_FALLBACK_MODEL not in models:
        models.append(LLM_FALLBACK_MODEL)
    return [model for model in models if model]


def call_llm_content(
    messages: list[dict[str, str]],
    temperature: float = 0.1,
    max_tokens: int = 200,
    timeout: float = 20,
    models: list[str] | None = None,
) -> str | None:
    if not LLM_BASE_URL or not LLM_API_KEY:
        return None

    headers = {
        "Authorization": f"Bearer {LLM_API_KEY}",
        "Content-Type": "application/json",
    }

    payload_base = {
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }

    active_models = [model for model in (models or list_llm_models()) if model]
    started_at = time_module.perf_counter()
    minimum_per_attempt = 0.35
    reserve_for_fallback = 1.25
    for index, model in enumerate(active_models, start=1):
        elapsed = time_module.perf_counter() - started_at
        remaining = timeout - elapsed
        if remaining <= 0 or remaining < minimum_per_attempt:
            break
        request_timeout = remaining
        remaining_attempts = len(active_models) - index
        if remaining_attempts > 0:
            reserved_budget = max(minimum_per_attempt * remaining_attempts, min(remaining * 0.25, reserve_for_fallback * remaining_attempts))
            request_timeout = max(minimum_per_attempt, remaining - reserved_budget)
        request_started_at = time_module.perf_counter()
        try:
            response = requests.post(
                f"{LLM_BASE_URL}/chat/completions",
                headers=headers,
                json={**payload_base, "model": model},
                timeout=request_timeout,
            )
            if response.status_code >= 400:
                logger.warning(
                    "llm_request_failed model=%s status=%s body=%s base_url=%s",
                    model,
                    response.status_code,
                    sanitize_text(response.text)[:240],
                    LLM_BASE_URL,
                )
                continue
            payload = response.json()
            content = payload.get("choices", [{}])[0].get("message", {}).get("content", "")
            if content and content.strip():
                logger.info(
                    "llm_request_success model=%s duration_ms=%s base_url=%s",
                    model,
                    elapsed_ms(request_started_at),
                    LLM_BASE_URL,
                )
                return content.strip()
            logger.warning("llm_request_empty_response model=%s", model)
        except Exception as exc:  # noqa: BLE001
            logger.warning("llm_request_exception model=%s error=%s base_url=%s", model, exc, LLM_BASE_URL)
            continue
    return None


def normalize_llm_reply_text(content: str | None) -> str | None:
    if not content:
        return None
    text = content.strip()
    if text.startswith("```"):
        text = re.sub(r"^```(?:json|text)?", "", text).strip()
        text = re.sub(r"```$", "", text).strip()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = [line.strip() for line in text.split("\n")]
    cleaned = "\n".join(line for line in lines if line)
    cleaned = re.sub(
        r"^(?:güvenli\s+taslak\s+cevap|guvenli\s+taslak\s+cevap|taslak\s+cevap|cevap)\s*:\s*",
        "",
        cleaned,
        flags=re.IGNORECASE,
    ).strip()
    cleaned = restore_common_turkish_reply_words(cleaned)
    return cleaned[:2000] if cleaned else None


def _preserve_case_replacement(replacement: str, original: str) -> str:
    if original.isupper():
        return replacement.upper()
    if original[:1].isupper():
        return replacement[:1].upper() + replacement[1:]
    return replacement


def restore_common_turkish_reply_words(text: str) -> str:
    replacements = {
        "yarin": "yarın",
        "tasarim": "tasarım",
        "fiyati": "fiyatı",
        "detayli": "detaylı",
        "yardimci": "yardımcı",
        "yardim": "yardım",
        "icin": "için",
        "kisa": "kısa",
        "musteri": "müşteri",
        "musteriler": "müşteriler",
        "musterilere": "müşterilere",
        "mesajlarıza": "mesajlarınıza",
        "mesajlariniza": "mesajlarınıza",
        "mesajlarini": "mesajlarını",
        "calisiyor": "çalışıyor",
        "calisir": "çalışır",
        "gorusme": "görüşme",
        "gorusmeyi": "görüşmeyi",
        "gorusmek": "görüşmek",
        "goruselim": "görüşelim",
        "gorusebiliriz": "görüşebiliriz",
        "gorusebilir": "görüşebilir",
        "gorus": "görüş",
    }
    result = text
    for source, replacement in replacements.items():
        result = re.sub(
            rf"\b{re.escape(source)}\b",
            lambda match: _preserve_case_replacement(replacement, match.group(0)),
            result,
            flags=re.IGNORECASE,
        )
    return result


def extract_price_number_tokens(text: str | None) -> set[str]:
    if not text:
        return set()
    tokens: set[str] = set()
    for match in re.finditer(r"\b\d{1,3}(?:[.,]\d{3})+\b", text):
        tokens.add(re.sub(r"\D", "", match.group(0)))
    for match in re.finditer(r"\b\d+(?:[.,]\d+)?\s*(?:tl|₺)\b", text, flags=re.IGNORECASE):
        tokens.add(re.sub(r"\D", "", match.group(0)))
    return {token for token in tokens if token}


def reply_has_truncated_price_number(text: str | None) -> bool:
    if not text:
        return False
    return bool(re.search(r"\b\d{1,3}\.(?!\d)", text))


def reply_mentions_price(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    if any(token in lowered for token in ["fiyat", "ücret", "ucret", "tl", "₺", "aylık", "aylik", "tek sefer", "ilk 3 ay"]):
        return True
    return bool(re.search(r"(?:₺\s*\d|\d[\d\.,]*\s*tl)", lowered))


def reply_requests_booking_details(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    patterns = [
        "ad soyad", "telefon numaran", "telefonunuzu", "uygun gün", "uygun gun", "hangi gün", "hangi gun", "hangi g?n", "hangi tarih",
        "hangi saat", "gün ve saat", "gun ve saat", "g?n ve saat", "size uygun gün", "size uygun gun", "randevu kaydı", "ön görüşme kaydı", "on gorusme kaydi",
        "telefon numarası", "telefon numarasi"
    ]
    return any(pattern in lowered for pattern in patterns)


def can_collect_booking_details_from_message(text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> bool:
    return bool(
        explicitly_starts_consultation_collection(text)
        or accepts_pending_consultation_offer(text, conversation, history, {})
        or extract_phone(text)
        or extract_date(text)
        or extract_time_for_state(text, conversation.get("state", "new"))
    )


def is_service_info_dump_reply(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    hints = ["7/24", "dm", "randevu", "fatura", "yorum", "excel", "crm", "otomasyon"]
    score = sum(1 for hint in hints if hint in lowered)
    return score >= 4


def reply_claims_booking_creation(text: str) -> bool:
    lowered = sanitize_text(text).lower()
    claims = [
        "oluşturuyorum", "olusturuyorum", "oluşturdum", "olusturdum", "kaydınızı açıyorum", "kaydinizi aciyorum",
        "kaydınızı oluşturuyorum", "kaydinizi olusturuyorum", "hemen oluşturuyorum", "hemen olusturuyorum",
        "hemen kaydediyorum", "planladım", "planladik", "kaydınızı açtım", "kaydinizi actim"
    ]
    return any(claim in lowered for claim in claims)


def normalize_similarity_text(text: str) -> str:
    lowered = sanitize_text(text).lower()
    lowered = re.sub(r"[^a-z0-9çğıöşü\s]", " ", lowered)
    lowered = re.sub(r"\s+", " ", lowered).strip()
    return lowered


def looks_like_repeated_prompt(candidate_reply: str, previous_reply: str) -> bool:
    candidate = normalize_similarity_text(candidate_reply)
    previous = normalize_similarity_text(previous_reply)
    if not candidate or not previous:
        return False
    if candidate == previous:
        return True
    candidate_words = {word for word in candidate.split() if len(word) > 2}
    previous_words = {word for word in previous.split() if len(word) > 2}
    if not candidate_words or not previous_words:
        return False
    overlap = len(candidate_words & previous_words) / max(1, len(candidate_words | previous_words))
    return overlap >= 0.72 and candidate.endswith("?") and previous.endswith("?")


def infer_customer_emotion(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    if is_technical_issue_message(message_text):
        return "frustration"
    if is_reaction_message(message_text):
        return "frustration"
    if is_fatigue_painpoint_message(message_text):
        return "fatigue"
    if match_objection_type(message_text) or is_price_negotiation_message(message_text, {}):
        return "hesitation"
    if is_confirmation_acceptance_message(message_text) or is_all_choice_message(message_text) or detect_priority_choice(message_text):
        return "approval"
    if is_clarification_request(message_text):
        return "confusion"
    if recent_outbound_requested_priority(history) and sanitize_text(message_text).lower() in {"dm", "randevu", "crm", "teklif", "fatura"}:
        return "decisive"
    return "neutral"


def infer_message_role(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    state = sanitize_text(conversation.get("state") or "")
    followup_role = infer_contextual_followup_role(message_text, conversation, history, None)
    if followup_role == "price_clarification":
        return "scope_confirmation"
    if followup_role in {"offer_followup", "answer_to_previous_question"}:
        return "answer_to_previous_question"
    if state == "collect_name" and extract_name(message_text, "collect_name"):
        return "booking_progress"
    if state == "collect_phone" and extract_phone(message_text):
        return "booking_progress"
    if state in {"collect_date", "collect_period", "collect_time"} and (extract_date(message_text) or extract_time_for_state(message_text, state) or extract_preferred_period(message_text)):
        return "booking_progress"
    if recent_outbound_offered_consultation(history) and is_confirmation_acceptance_message(message_text):
        return "answer_to_offer"
    if recent_outbound_requested_priority(history) and (detect_priority_choice(message_text) or is_all_choice_message(message_text)):
        return "answer_to_previous_question"
    if recent_outbound_requested_dm_issue(history) and detect_dm_issue_choice(message_text):
        return "answer_to_previous_question"
    if recent_outbound_requested_message_volume(history) and is_message_volume_answer(message_text):
        return "answer_to_previous_question"
    if is_price_question(message_text) or is_working_schedule_question(message_text) or is_company_background_question(message_text) or is_assistant_identity_question(message_text):
        return "new_question"
    if match_objection_type(message_text) or is_price_negotiation_message(message_text, {}):
        return "objection"
    if is_fatigue_painpoint_message(message_text):
        return "pain_point"
    if "?" in sanitize_text(message_text):
        return "new_question"
    return "new_message"


def infer_user_need(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> str:
    state = sanitize_text(conversation.get("state") or "")
    followup_role = infer_contextual_followup_role(message_text, conversation, history, None)
    if followup_role == "price_clarification":
        return "az önce verilen fiyatın kapsamını netleştirmek"
    priority_choice = detect_priority_choice(message_text)
    if recent_outbound_requested_priority(history) and priority_choice == "dm":
        return "DM tarafındaki ana sorunun anlaşılması ve mantıklı çözüm önerisi"
    if recent_outbound_requested_priority(history) and priority_choice == "appointment":
        return "randevu tarafındaki ana sorunun anlaşılması ve çözüm önerisi"
    if recent_outbound_requested_priority(history) and priority_choice == "crm":
        return "müşteri takibindeki dağınıklığa çözüm"
    if recent_outbound_requested_priority(history) and priority_choice == "invoice":
        return "teklif/fatura tarafındaki yükü azaltacak çözüm"
    if recent_outbound_requested_priority(history) and is_all_choice_message(message_text):
        return "birden fazla sürecin birlikte ele alınması"
    if recent_outbound_requested_dm_issue(history) and detect_dm_issue_choice(message_text) == "delay":
        return "DM tarafındaki gecikmeyi azaltacak mantıklı bir sonraki adımı duymak"
    if recent_outbound_requested_dm_issue(history) and detect_dm_issue_choice(message_text) == "repetition":
        return "tekrar eden soruları azaltacak mantıklı çözümü duymak"
    if recent_outbound_requested_message_volume(history) and is_message_volume_answer(message_text):
        return "mesaj yoğunluğuna uygun net bir otomasyon önerisi duymak"
    if recent_outbound_offered_consultation(history) and is_confirmation_acceptance_message(message_text):
        return "görüşmeyi başlatmak ve booking akışına geçmek"
    if state == "collect_name" and extract_name(message_text, "collect_name"):
        return "booking akışında bir sonraki adıma geçmek"
    if state == "collect_phone" and extract_phone(message_text):
        return "iletişim bilgisini tamamlayıp devam etmek"
    if is_price_question(message_text):
        return "net fiyat bilgisini almak"
    if is_business_need_analysis_message(message_text):
        return "işletmesine en uygun çözümün mantıklı şekilde önerilmesi"
    if is_service_advice_request(message_text, {}):
        return "hangi hizmetin daha mantıklı olduğunu duymak"
    if is_technical_issue_message(message_text):
        return "teknik aksaklığın hangi mesajda tetiklendiğini netleştirmek"
    if is_reaction_message(message_text):
        return "önce anlaşılıp sonra sakince neye takıldığını netleştirmek"
    if is_fatigue_painpoint_message(message_text):
        return "önce anlaşılıp sonra pratik bir çözüm önerisi duymak"
    if is_clarification_request(message_text):
        return "önceki sorunun ne anlama geldiğini netleştirmek"
    if match_objection_type(message_text):
        return "çekincesine mantıklı bir karşılık almak"
    return "doğrudan ve mantıklı bir yanıt almak"


def build_reply_understanding_snapshot(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    memory = ensure_conversation_memory(conversation)
    message_role = infer_message_role(message_text, conversation, history)
    smalltalk_signal = is_smalltalk_message(message_text) or is_presence_check_message(message_text)
    decline_context = bool(memory.get("offer_status") == "declined")
    recent_outbound_act = infer_recent_outbound_act(history) or memory.get("last_outbound_act")
    followup_role = infer_contextual_followup_role(message_text, conversation, history, None)
    return {
        "emotion": infer_customer_emotion(message_text, conversation, history),
        "message_role": message_role,
        "what_user_needs": infer_user_need(message_text, conversation, history),
        "last_assistant_message": get_last_outbound_text(history) or None,
        "last_outbound_act": recent_outbound_act,
        "likely_followup_role": followup_role,
        "should_continue_same_topic": bool(followup_role or recent_outbound_act in {"answered_price", "asked_priority", "asked_dm_issue", "asked_message_volume", "offered_consultation"}),
        "open_loop": memory.get("open_loop"),
        "pending_offer": memory.get("pending_offer"),
        "offer_status": memory.get("offer_status"),
        "last_bot_question_type": memory.get("last_bot_question_type"),
        "answered_question_types": memory.get("answered_question_types"),
        "conversation_summary": memory.get("conversation_summary"),
        "smalltalk_signal": smalltalk_signal,
        "decline_context": decline_context,
        "sales_cooldown": decline_context and message_role in {"new_message", "objection", "answer_to_previous_question"},
    }


def apply_reply_guardrails(
    draft_reply: str | None,
    candidate_reply: str | None,
    customer_message: str,
    conversation: dict[str, Any],
    decision_label: str | None = None,
    history: list[dict[str, Any]] | None = None,
) -> str | None:
    if not candidate_reply:
        return draft_reply
    if not draft_reply:
        return candidate_reply

    label = sanitize_text(decision_label or "").lower()
    message = sanitize_text(customer_message)
    lowered = message.lower()
    has_booking_signal = bool(
        message_shows_booking_intent(message, {})
        or wants_availability_information(message, {})
        or extract_phone(message)
        or extract_date(message)
        or extract_time_for_state(message, conversation.get("state", "new"))
    )
    message_role = infer_message_role(message, conversation, history)
    last_outbound = get_last_outbound_text(history)
    candidate_lower = candidate_reply.lower()
    memory = ensure_conversation_memory(conversation)
    candidate_question_type = infer_reply_question_type(candidate_reply, decision_label, conversation)

    if is_price_question(message):
        if not reply_mentions_price(candidate_reply):
            return draft_reply
        draft_prices = extract_price_number_tokens(draft_reply)
        candidate_prices = extract_price_number_tokens(candidate_reply)
        if reply_has_truncated_price_number(candidate_reply):
            return draft_reply
        if draft_prices and not draft_prices.intersection(candidate_prices):
            return draft_reply
        if any(token in lowered for token in ["aylık", "aylik", "tek sefer", "tek seferlik"]) and not any(token in candidate_lower for token in ["aylık", "aylik", "tek sefer", "tek seferlik", "ilk 3 ay"]):
            return draft_reply

    if is_price_negotiation_message(message, {}) and is_service_info_dump_reply(candidate_reply):
        negotiation_signals = ["bütçe", "butce", "kapsam", "daralt", "yaklaş", "yaklas", "uygun", "kritik", "öncelik", "oncelik", "başlangıç", "baslangic"]
        if not any(signal in candidate_lower for signal in negotiation_signals):
            return draft_reply

    if label != "collect_service" and not has_booking_signal and reply_requests_booking_details(candidate_reply):
        return draft_reply

    explicit_collection_signal = can_collect_booking_details_from_message(message, conversation, history)
    if label == "collect_service" and reply_requests_booking_details(candidate_reply) and not explicit_collection_signal:
        if is_quality_model_question(message) or is_business_need_analysis_message(message) or is_service_advice_request(message, {}):
            return draft_reply

    if label != "collect_service" and match_service_candidates(message, conversation.get("service")) and len(message.split()) <= 3 and reply_requests_booking_details(candidate_reply):
        return draft_reply

    if label.startswith("info:") and reply_requests_booking_details(candidate_reply) and not has_booking_signal:
        return draft_reply

    if message_role in {"answer_to_previous_question", "booking_progress", "answer_to_offer"} and last_outbound and looks_like_repeated_prompt(candidate_reply, last_outbound):
        return draft_reply

    if recent_outbound_requested_priority(history) and (detect_priority_choice(message) or is_all_choice_message(message)) and any(cue in candidate_lower for cue in ["hangi süreç", "hangi surec", "öncelik", "oncelik", "en çok", "en cok"]):
        return draft_reply

    if recent_outbound_requested_dm_issue(history) and detect_dm_issue_choice(message) and any(cue in candidate_lower for cue in ["gecikme mi", "geç cevap vermek mi", "gec cevap vermek mi", "tekrar eden mesajlar", "aynı sorular", "ayni sorular"]):
        return draft_reply

    if recent_outbound_requested_message_volume(history) and is_message_volume_answer(message) and any(cue in candidate_lower for cue in ["dm trafiği mi", "dm trafigi mi", "randevu takibi mi", "hangi taraf", "öncelik", "oncelik", "en çok", "en cok"]):
        return draft_reply

    if (is_fatigue_painpoint_message(message) or is_reaction_message(message)) and not any(token in candidate_lower for token in ["haklı", "haklisiniz", "anlıyorum", "anliyorum", "yorucu", "zorlayıcı", "zorlayici", "can", "takıld", "sıkmış", "sikmis"]):
        return draft_reply
    if (is_fatigue_painpoint_message(message) or is_reaction_message(message)) and any(token in candidate_lower for token in ["hoş geldiniz", "hos geldiniz", "selamlar", "merhaba"]):
        return draft_reply

    if detect_business_sector(message, history) == "beauty" and label in {"info:business_owner_need_analysis", "info:service_advice", "info:priority_choice"}:
        if not any(token in candidate_lower for token in ["salon", "randevu", "dm", "müşteri", "musteri"]):
            return draft_reply

    if label in {"info:service_advice", "info:business_owner_need_analysis", "info:fatigue_painpoint", "info:priority_choice"} and is_service_info_dump_reply(candidate_reply):
        return draft_reply

    if label == "info:technical_issue":
        if any(token in candidate_lower for token in ["hoş geldiniz", "hos geldiniz", "merhaba", "hangi konuda yardımcı olabilirim", "hangi hizmet", "size nasıl yardımcı"]):
            return draft_reply
        if reply_requests_booking_details(candidate_reply) or is_service_info_dump_reply(candidate_reply):
            return draft_reply

    if reply_claims_booking_creation(candidate_reply):
        required_ready = bool(conversation.get("full_name") and conversation.get("phone") and conversation.get("service"))
        if not required_ready:
            return draft_reply

    if not conversation.get("full_name") and any(token in candidate_lower for token in ["ad soyadınızı aldım", "adınızı aldım", "isminizle", "adınızla", "ad soyadınızı öğrendim"]):
        return draft_reply

    if label == "info:offer_hesitation" and "?" in sanitize_text(candidate_reply):
        return draft_reply

    if label == "collect_service":
        banned_collect_phrases = [
            "web tasarım, otomasyon, reklam yönetimi veya sosyal medya",
            "dijitaldeki büyüme süreçleri",
            "pazarlama stratejilerini uçtan uca",
            "uçtan uca yönetiyoruz",
        ]
        if any(phrase in candidate_lower for phrase in banned_collect_phrases):
            return draft_reply

    if candidate_question_type and candidate_question_type in set(memory.get("answered_question_types") or []) and message_role in {"answer_to_previous_question", "booking_progress", "answer_to_offer"}:
        return draft_reply

    if memory.get("offer_status") == "accepted" and candidate_question_type == "priority":
        return draft_reply

    return candidate_reply


def polish_reply_text(
    draft_reply,
    conversation,
    history=None,
    decision_label=None,
):
    if not draft_reply and not FULL_AI_CONVERSATIONAL_MODE:
        return draft_reply

    label = sanitize_text(decision_label or "").lower()
    profile = get_ai_compose_profile(decision_label, conversation)
    compact_history = build_recent_history_lines(history, profile["history_limit"])
    reply_goal = build_ai_reply_goal(decision_label, conversation)
    known_facts = build_compact_known_facts(conversation, include_contact=profile["include_contact"])
    customer_message = sanitize_text(conversation.get("last_customer_message") or "")

    history_text = " | ".join(compact_history) if compact_history else "yok"
    facts_text = json.dumps(known_facts, ensure_ascii=False) if known_facts else "{}"

    is_guarded = label in GUARDED_COMPOSE_LABELS

    catalog = (
        "HİZMETLER VE FİYATLAR: "
        "1) Web Tasarım KOBİ: 12.900 TL (tek seferlik), teslim 7-14 iş günü. "
        "2) Otomasyon ve Yapay Zeka: 5.000 TL/ay, teslim standart kurulumlarda 3-7 iş günü, özel entegrasyonlarda 1-3 hafta. "
        "3) Performans Pazarlama: 7.500 TL/ay (reklam bütçesi hariç). "
        "4) Sosyal Medya Yönetimi: Özel teklif. "
        "5) Marka Stratejisi: Özel teklif. "
        "6) Kreatif Prodüksiyon: Özel teklif."
    )

    sys_prompt = " ".join([
        f"Sen {BUSINESS_NAME} adına Instagram DM yanıtı veren doğal bir satış destek asistanısın.",
        "Doel Digital, markalara web tasarım, otomasyon, performans pazarlama ve sosyal medya alanlarında destek veren bir dijital ajanstır.",
        catalog,
        "KESİN KURALLAR:",
        "1. Türkçe karakter kullan: ç, ğ, ı, ö, ş, ü. 'tasarim', 'icin', 'gorusme', 'yarin' gibi Latinleştirilmiş kelimeler yasak.",
        "2. Sadece düz metin yaz; emoji, markdown, madde işareti ve tırnak kullanma.",
        "3. Maksimum 2 kısa cümle yaz; selamlaşma veya küçük sohbet ise 1 kısa cümle yeterli.",
        "4. Önce soruyu cevapla, sonra sadece bir net sonraki adım öner.",
        "5. Önceki soruyu tekrarlama ve müşteri istemedikçe hizmet listesi dökme.",
        "6. Selamlaşma ise satış yapma; kısa ve insani cevap ver.",
        "7. Fiyat sorarsa fiyatı tam yaz: 12.900 TL, 5.000 TL/ay, 7.500 TL/ay. Asla '12.' gibi yarım fiyat yazma.",
        "8. Müşteri kararsızsa baskı yapma; 'ödemelisiniz' gibi sert ifadeler kullanma.",
        "9. Sadece sorulan hizmet hakkında yaz.",
        "10. Telefon gerekiyorsa kısa sor: 'Telefon numaranızı paylaşır mısınız?'",
        "11. Randevu onayında context'ten gelen bilgileri birebir kullan, uydurma.",
    ])

    user_parts = [
        "Müşteri mesajı: " + (customer_message or "-"),
        "Konuşma geçmişi: " + history_text,
        "Bilinen bilgiler: " + facts_text,
        "Güvenli taslak cevap: " + (draft_reply or "-"),
        "Cevap hedefi: " + reply_goal,
    ]
    if is_guarded and draft_reply:
        user_parts.append("Güvence bilgileri (tarihleri, saatleri, adları AYNEN kullan): " + draft_reply)
    user_msg = chr(10).join(user_parts)

    messages = [
        {"role": "system", "content": sys_prompt},
        {"role": "user", "content": user_msg},
    ]

    rewritten = call_llm_content(
        messages,
        temperature=profile["temperature"],
        max_tokens=profile["max_tokens"],
        timeout=profile["timeout"],
        models=profile["models"],
    )
    normalized = normalize_llm_reply_text(rewritten)
    if not normalized and profile.get("allow_retry", True):
        retry_messages = [
            {
                "role": "system",
                "content": (
                    f"You write exactly one very short Turkish Instagram DM reply for {BUSINESS_NAME}. "
                    "Use Turkish characters: ç, ğ, ı, ö, ş, ü. Plain text only. No markdown. No emojis. "
                    "Keep it to one short natural sentence. Reply like a real human."
                ),
            },
            {
                "role": "user",
                "content": chr(10).join([
                    "Customer message: " + customer_message,
                    "Goal: " + reply_goal,
                    "History: " + history_text,
                ]),
            },
        ]
        rewritten = call_llm_content(
            retry_messages,
            temperature=0.1,
            max_tokens=min(48, profile["max_tokens"]),
            timeout=max(1.8, min(3.2, profile["timeout"])),
            models=profile["models"],
        )
        normalized = normalize_llm_reply_text(rewritten)
    if not normalized:
        final_messages = [
            {
                "role": "system",
                "content": (
                    "Aşağıdaki Türkçe mesajı müşteriye gönderilecek çok kısa, doğal bir Instagram DM cevabı olarak yeniden yaz. "
                    "Türkçe karakter kullan. Düz metin kullan. Emojisiz ol. Tek cümle tercih et."
                ),
            },
            {
                "role": "user",
                "content": draft_reply or customer_message or "Merhaba, nasil yardimci olabilirim?",
            },
        ]
        rewritten = call_llm_content(
            final_messages,
            temperature=0.0,
            max_tokens=max(18, min(32, profile["max_tokens"])),
            timeout=max(5.5, profile["timeout"]),
            models=profile["models"],
        )
        normalized = normalize_llm_reply_text(rewritten)
    if not normalized:
        logger.warning("ai_reply_empty_after_compose decision_label=%s", decision_label)
        return None
    return normalized


def call_llm_extractor(message_text: str, conversation: dict[str, Any], history: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    messages = [
        {
            "role": "system",
            "content": (
                "You extract structured data from Turkish Instagram DM messages. "
                "Return ONLY minified JSON with keys: intent, sub_intent, confidence, booking_confidence, risk_level, name, service, requested_date, requested_time, wants_human, notes, recommended_service, secondary_service, needs_clarification, clarifying_question_focus, wants_booking, booking_change, price_question, objection_type, emotion, message_role, what_user_needs, did_user_accept_previous_offer, did_user_answer_previous_question, should_continue_same_topic, reply_strategy. "
                "intent must be one of appointment, info, human, other, service_advice, comparison, availability, booking_change. sub_intent should be a short snake_case label or null. requested_date must be ISO YYYY-MM-DD or null. "
                "requested_time must be HH:MM or null. confidence and booking_confidence must be 0..1 numbers. risk_level must be low, medium, high, or null. wants_human, needs_clarification, wants_booking, booking_change, price_question, did_user_accept_previous_offer, did_user_answer_previous_question, should_continue_same_topic must be boolean. objection_type must be price, hesitation, or null. emotion should be one of fatigue, frustration, hesitation, approval, confusion, neutral, or null. message_role should be one of answer_to_previous_question, new_question, objection, scope_confirmation, booking_progress, pain_point, new_message, or null. what_user_needs should be a short Turkish phrase or null. reply_strategy should be one of answer_then_ask_one_question, continue_same_topic, offer_consultation, start_booking, clarify, reassure, or null. Do not include markdown. "
                "If the message is a short acknowledgement, confirmation, follow-up question, thanks, service name only, price follow-up, or asks about known context, use intent='info' unless the user explicitly asks to book. "
                "If recent history shows the assistant proposed a meeting and the user says they do not want it now, are not interested, says gerek yok, istemiyorum, or cools the conversation, classify it as objection_type='hesitation', keep intent='info', and avoid interpreting the next short acknowledgement as renewed sales interest. "
                "Use recent history and known_state.memory to decide whether the user is answering the assistant's last question or accepting a pending offer. Never invent a new service/date/time/name from filler words, vague confirmations, pricing questions, or profession/sector statements like 'ben dövmeciyim'. Service name alone does not mean booking intent."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(
                {
                    "today": datetime.now(TZ).date().isoformat(),
                    "timezone": TIMEZONE,
                    "business_name": BUSINESS_NAME,
                    "business_tagline": BUSINESS_TAGLINE,
                    "service_catalog": [{"display": s["display"], "keywords": s["keywords"]} for s in DOEL_SERVICE_CATALOG],
                    "known_state": build_normalized(conversation),
                    "recent_history": history or [],
                    "message": message_text,
                },
                ensure_ascii=False,
            ),
        },
    ]

    extractor_models = list_llm_models()

    content = call_llm_content(
        messages,
        temperature=0.1,
        max_tokens=260,
        timeout=LLM_EXTRACT_TIMEOUT_SECONDS,
        models=extractor_models or None,
    )
    parsed = parse_json_like(content or "")
    return parsed or {}


def parse_json_like(content: str) -> dict[str, Any]:
    if not content:
        return {}
    content = content.strip()
    if content.startswith("```"):
        content = re.sub(r"^```(?:json)?", "", content).strip()
        content = re.sub(r"```$", "", content).strip()
    try:
        decoded = json.loads(content)
        if isinstance(decoded, dict):
            return decoded
        if isinstance(decoded, str) and decoded != content:
            nested = parse_json_like(decoded)
            if nested:
                return nested
    except json.JSONDecodeError:
        pass
    start = content.find("{")
    end = content.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    try:
        return json.loads(content[start : end + 1])
    except json.JSONDecodeError:
        return {}


AI_FIRST_DECISION_KEYS = {
    "reply_text",
    "intent",
    "should_reply",
    "booking_intent",
    "extracted_service",
    "extracted_name",
    "extracted_phone",
    "requested_date",
    "requested_time",
    "missing_fields",
    "crm_action",
    "handoff_needed",
}


def select_ai_first_models(message_text: str, conversation: dict[str, Any]) -> list[str]:
    cleaned = sanitize_text(message_text).lower()
    quality_markers = [
        "mantikli",
        "mantikli mi",
        "strateji",
        "hangisi",
        "hepsini",
        "dolandirici",
        "guven",
        "neden",
        "nasil",
        "fiyat",
        "teslim",
    ]
    simple_markers = ["merhaba", "selam", "aleykum", "nasilsiniz", "tesekkur", "sagol", "kolay gelsin"]
    active_state = sanitize_text(conversation.get("state") or "")
    if len(cleaned.split()) <= 4 and any(marker in cleaned for marker in simple_markers):
        ordered = [LLM_REPLY_MICRO_MODEL, LLM_REPLY_ADVISORY_MODEL, LLM_FALLBACK_MODEL]
    elif active_state in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"} or any(marker in cleaned for marker in quality_markers):
        ordered = [LLM_REPLY_QUALITY_MODEL, LLM_REPLY_ADVISORY_MODEL, LLM_FALLBACK_MODEL]
    else:
        ordered = [LLM_REPLY_ADVISORY_MODEL, LLM_REPLY_QUALITY_MODEL, LLM_FALLBACK_MODEL]
    result: list[str] = []
    for model in ordered:
        if model and model not in result:
            result.append(model)
    return result or list_llm_models()


def build_ai_first_service_context() -> list[dict[str, Any]]:
    services: list[dict[str, Any]] = []
    for service in DOEL_SERVICE_CATALOG:
        services.append(
            {
                "display": display_service_name(str(service.get("display") or "")),
                "price": str(service.get("price") or ""),
                "price_note": str(service.get("price_note") or ""),
                "delivery_time": str(service.get("delivery_time") or ""),
                "summary": str(service.get("summary") or ""),
                "keywords": service.get("keywords") or [],
            }
        )
    return services


def build_ai_first_prompt_payload(
    message_text: str,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None,
    llm_data: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "today": datetime.now(TZ).date().isoformat(),
        "timezone": TIMEZONE,
        "business_name": BUSINESS_NAME or "DOEL Digital",
        "business_phone": BUSINESS_PHONE,
        "business_email": BUSINESS_EMAIL,
        "business_website": BUSINESS_WEBSITE,
        "service_catalog": build_ai_first_service_context(),
        "known_state": build_normalized(conversation),
        "recent_history": history or [],
        "extractor_hint": llm_data or {},
        "message": message_text,
    }


def normalize_ai_first_decision(
    parsed: dict[str, Any],
    message_text: str,
    conversation: dict[str, Any],
    *,
    fallback_used: bool,
    ai_model_used: str | None,
) -> dict[str, Any]:
    if not isinstance(parsed, dict):
        parsed = {}
    reply_text = normalize_llm_reply_text(str(parsed.get("reply_text") or ""))
    if not reply_text:
        reply_text = build_ai_first_emergency_reply(message_text, conversation)
        fallback_used = True

    extracted_service = sanitize_text(str(parsed.get("extracted_service") or ""))
    service_match = match_service_catalog(extracted_service, extracted_service) if extracted_service else None
    if service_match:
        extracted_service = str(service_match.get("display") or extracted_service)
    else:
        extracted_service = extracted_service or None

    extracted_name = titlecase_name(parsed.get("extracted_name"))
    extracted_phone = extract_phone(str(parsed.get("extracted_phone") or "")) if parsed.get("extracted_phone") else None
    requested_date = normalize_date_string(parsed.get("requested_date"))
    requested_time = normalize_time_string(parsed.get("requested_time"))
    missing_fields = parsed.get("missing_fields")
    if not isinstance(missing_fields, list):
        missing_fields = []
    missing_fields = [sanitize_text(str(field)).lower() for field in missing_fields if sanitize_text(str(field))]

    should_reply = parsed.get("should_reply")
    should_reply = True if should_reply is None else llm_bool(should_reply)
    if REPLY_GUARANTEE_ENABLED and sanitize_text(message_text):
        should_reply = True

    decision = {
        "reply_text": reply_text,
        "intent": sanitize_text(str(parsed.get("intent") or "general_reply")) or "general_reply",
        "should_reply": should_reply,
        "booking_intent": llm_bool(parsed.get("booking_intent")),
        "extracted_service": extracted_service,
        "extracted_name": extracted_name,
        "extracted_phone": extracted_phone,
        "requested_date": requested_date,
        "requested_time": requested_time,
        "missing_fields": missing_fields,
        "crm_action": sanitize_text(str(parsed.get("crm_action") or "update_customer")) or "update_customer",
        "handoff_needed": llm_bool(parsed.get("handoff_needed")),
        "fallback_used": fallback_used,
        "ai_model_used": ai_model_used,
    }
    return decision


def enforce_ai_first_booking_order(
    decision: dict[str, Any],
    conversation: dict[str, Any],
    message_text: str,
) -> dict[str, Any]:
    if not llm_bool(decision.get("booking_intent")):
        return decision

    service = sanitize_text(str(decision.get("extracted_service") or conversation.get("service") or ""))
    service_meta = match_service_catalog(service, service) if service else None
    service_display = display_service_name(str((service_meta or {}).get("display") or service))
    booking_label = "ön görüşme"
    extracted_name = titlecase_name(decision.get("extracted_name"))
    extracted_phone = canonical_phone(decision.get("extracted_phone"))
    requested_date = normalize_date_string(decision.get("requested_date"))
    requested_time = normalize_time_string(decision.get("requested_time"))
    current_name = titlecase_name(conversation.get("full_name"))
    current_phone = canonical_phone(conversation.get("phone"))
    current_date = normalize_date_string(conversation.get("requested_date"))
    current_time = normalize_time_string(conversation.get("requested_time"))

    if not service_display:
        decision["reply_text"] = "Tabii, görüşme planlayabiliriz. Hangi hizmet için görüşmek istediğinizi yazar mısınız?"
        decision["missing_fields"] = ["service"]
        return decision
    if not (extracted_name or current_name):
        decision["reply_text"] = f"Tabii, {service_display} için {booking_label} planlayabiliriz. Önce adınızı ve soyadınızı yazar mısınız?"
        decision["missing_fields"] = ["full_name", "phone", "requested_date", "requested_time"]
        return decision
    if not (extracted_phone or current_phone):
        decision["reply_text"] = f"Teşekkürler. {booking_label.capitalize()} kaydını tamamlamak için telefon numaranızı paylaşır mısınız?"
        decision["missing_fields"] = ["phone", "requested_date", "requested_time"]
        return decision
    if not (requested_date or current_date):
        decision["reply_text"] = f"Not aldım. {service_display} için hangi gün görüşmek istersiniz?"
        decision["missing_fields"] = ["requested_date", "requested_time"]
        return decision
    if not (requested_time or current_time):
        decision["reply_text"] = "Uygun günü aldım. Hangi saat sizin için uygun?"
        decision["missing_fields"] = ["requested_time"]
        return decision
    return decision


def should_suppress_ai_booking_collection(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    llm_data: dict[str, Any] | None = None,
) -> bool:
    if not llm_bool(decision.get("booking_intent")):
        return False
    if message_shows_booking_intent(message_text, llm_data or {}):
        return False
    state = sanitize_text(conversation.get("state") or "")
    if state in {"collect_name", "collect_phone", "collect_date", "collect_time", "collect_period"}:
        if state == "collect_name" and titlecase_name(decision.get("extracted_name")):
            return False
        if state == "collect_phone" and canonical_phone(decision.get("extracted_phone")):
            return False
        if state == "collect_date" and normalize_date_string(decision.get("requested_date")):
            return False
        if state == "collect_time" and normalize_time_string(decision.get("requested_time")):
            return False
    cleaned = sanitize_text(message_text)
    if any(
        [
            "?" in cleaned,
            is_price_question(cleaned),
            is_price_followup_message(cleaned, llm_data or {}),
            is_delivery_time_question(cleaned),
            is_delivery_duration_followup(cleaned),
            is_trust_or_scam_question(cleaned),
            is_request_reason_question(cleaned),
            is_clarification_request(cleaned),
            is_assistant_identity_question(cleaned),
            is_service_overview_question(cleaned),
            is_working_schedule_question(cleaned),
            is_company_background_question(cleaned),
        ]
    ):
        return True
    return False


def should_replace_collection_reply_with_clarification(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
) -> bool:
    reply = sanitize_text(str(decision.get("reply_text") or ""))
    if not reply_requests_booking_details(reply):
        return False
    cleaned = sanitize_text(message_text)
    return any(
        [
            is_meeting_method_question(cleaned),
            is_phone_reason_question(cleaned),
            is_meeting_clarification_question(cleaned),
            is_request_reason_question(cleaned),
            is_trust_or_scam_question(cleaned),
        ]
    )


def cleanup_ai_first_reply_text(reply_text: str | None) -> str | None:
    reply = normalize_llm_reply_text(reply_text or "")
    if not reply:
        return None
    replacements = {
        "Transparent": "şeffaf",
        "transparent": "şeffaf",
        "Hangi konuda bilgi almak isteriz?": "Hangi konuda bilgi almak istersiniz?",
        "hangi konuda bilgi almak isteriz?": "hangi konuda bilgi almak istersiniz?",
        "DOEL DIGITAL": "DOEL Digital",
    }
    for source, target in replacements.items():
        reply = reply.replace(source, target)
    return reply.strip()


def apply_ai_first_quality_overrides(
    message_text: str,
    decision: dict[str, Any],
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    decision["reply_text"] = cleanup_ai_first_reply_text(decision.get("reply_text"))
    lowered = sanitize_text(message_text).lower()
    if recent_outbound_can_start_service_consultation(history, conversation) and is_positive_more_details_acceptance(message_text):
        inferred_service = infer_recent_service_for_consultation(history, conversation)
        decision["reply_text"] = build_service_consultation_acceptance_reply(conversation)
        decision["intent"] = "service_consultation_acceptance"
        decision["booking_intent"] = True
        if inferred_service:
            decision["extracted_service"] = inferred_service
        decision["missing_fields"] = ["name"]
        return decision
    if recent_outbound_can_accept_automation_details(history, conversation) and is_positive_more_details_acceptance(message_text):
        decision["reply_text"] = build_more_details_acceptance_reply(conversation)
        decision["intent"] = "more_details_acceptance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
    if is_trust_or_scam_question(message_text):
        decision["reply_text"] = build_trust_or_scam_reply()
        decision["intent"] = "reassurance"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
    if is_angry_complaint_message(message_text):
        decision["reply_text"] = build_angry_complaint_reply()
        decision["intent"] = "complaint"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
    if ("aleykum" in lowered or "aleyküm" in lowered) and len(sanitize_text(message_text).split()) <= 4:
        decision["reply_text"] = "Aleyküm selam, hoş geldiniz. Size nasıl yardımcı olabilirim?"
        decision["intent"] = "greeting"
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        return decision
    return decision


def build_ai_first_emergency_reply(message_text: str, conversation: dict[str, Any]) -> str:
    lowered = sanitize_text(message_text).lower()
    if is_simple_greeting(message_text) or "aleykum" in lowered:
        return "Merhaba, buradayım. Size nasıl yardımcı olabilirim?"
    if any(token in lowered for token in ["dolandirici", "guven", "guvenilir", "sahte"]):
        return "Endişenizi anlıyorum. Süreci şeffaf şekilde anlatabilirim; isterseniz önce hizmet, fiyat ve çalışma şeklini netleştireyim."
    if any(token in lowered for token in ["fiyat", "ucret", "ne kadar", "kac para"]):
        return "Fiyat hizmete ve kapsama göre değişir. Web tasarım, otomasyon, reklam veya sosyal medya tarafında hangisini merak ettiğinizi yazarsanız net bilgi vereyim."
    if any(token in lowered for token in ["teslim", "sure", "kac gun", "hafta"]):
        service = display_service_name(conversation.get("service"))
        if service:
            meta = match_service_catalog(service, service)
            delivery = str((meta or {}).get("delivery_time") or "kapsama göre netleşir")
            return f"{service} için tahmini teslim süresi {delivery}. Kapsam büyüdükçe süre değişebilir."
        return "Teslim süresi hizmetin kapsamına göre değişir. Hangi hizmet için süre öğrenmek istediğinizi yazarsanız net cevap vereyim."
    if "?" in lowered:
        return build_generic_ai_draft_reply(message_text, conversation, [])
    return "Anladım. Size yardımcı olabilmem için mesajınızı dikkate alıyorum; neye ihtiyacınız olduğunu yazarsanız doğrudan cevap vereyim."


def build_ai_first_decision(
    message_text: str,
    conversation: dict[str, Any],
    history: list[dict[str, Any]] | None = None,
    llm_data: dict[str, Any] | None = None,
) -> dict[str, Any]:
    selected_models = select_ai_first_models(message_text, conversation)
    payload = build_ai_first_prompt_payload(message_text, conversation, history, llm_data)
    messages = [
        {
            "role": "system",
            "content": (
                "You are the AI-first Instagram DM reply engine for DOEL Digital. "
                "For every real inbound user message, return ONLY minified JSON. "
                "Never leave the user on seen. Rules and state are context only; the final reply is decided by you. "
                "Schema keys: reply_text, intent, should_reply, booking_intent, extracted_service, extracted_name, extracted_phone, requested_date, requested_time, missing_fields, crm_action, handoff_needed. "
                "should_reply must be true for every non-empty user message. reply_text must be natural, useful Turkish. "
                "Answer the user's actual question first. Do not force name, phone, date, or appointment collection unless the user clearly asks for meeting, appointment, pre-consultation, offer call, or continues a booking. "
                "If the user asks a question during booking, answer the question first and set booking_intent false unless they also clearly continue booking. "
                "If the user changes service, extracted_service must use the new service. "
                "If the user is angry or insults the bot, apologize briefly, avoid repeating old collection prompts, and explain the next useful step. "
                "Use only the provided service catalog for prices, delivery times, and services. If unsure, say that the team should confirm instead of inventing. "
                "If booking_intent is true, include one clear next missing field in the reply, not several at once. Do not include markdown."
            ),
        },
        {"role": "user", "content": json.dumps(payload, ensure_ascii=False)},
    ]
    content = call_llm_content(
        messages,
        temperature=0.35,
        max_tokens=420,
        timeout=max(LLM_REPLY_ADVISORY_TIMEOUT_SECONDS, 25),
        models=selected_models,
    )
    parsed = parse_json_like(content or "")
    fallback_used = not bool(parsed)
    unstructured_reply = normalize_llm_reply_text(content) if content and not parsed else None
    if not parsed and unstructured_reply:
        parsed = {
            "reply_text": unstructured_reply,
            "intent": "ai_unstructured_reply",
            "should_reply": True,
            "booking_intent": False,
            "extracted_service": None,
            "extracted_name": None,
            "extracted_phone": None,
            "requested_date": None,
            "requested_time": None,
            "missing_fields": [],
            "crm_action": "update_customer",
            "handoff_needed": False,
        }
        fallback_used = False
    if not parsed:
        parsed = {
            "reply_text": build_ai_first_emergency_reply(message_text, conversation),
            "intent": "fallback_reply",
            "should_reply": True,
            "booking_intent": False,
            "extracted_service": None,
            "extracted_name": None,
            "extracted_phone": None,
            "requested_date": None,
            "requested_time": None,
            "missing_fields": [],
            "crm_action": "update_customer",
            "handoff_needed": False,
        }
    decision = normalize_ai_first_decision(
        parsed,
        message_text,
        conversation,
        fallback_used=fallback_used,
        ai_model_used=selected_models[0] if selected_models else None,
    )
    if not set(decision).issuperset(AI_FIRST_DECISION_KEYS):
        decision = normalize_ai_first_decision(
            {},
            message_text,
            conversation,
            fallback_used=True,
            ai_model_used=selected_models[0] if selected_models else None,
        )
    decision = apply_ai_first_quality_overrides(message_text, decision, conversation, history)
    if should_suppress_ai_booking_collection(message_text, decision, conversation, llm_data):
        decision["booking_intent"] = False
        decision["missing_fields"] = []
        if should_replace_collection_reply_with_clarification(message_text, decision, conversation):
            decision["reply_text"] = build_contextual_clarification_reply(conversation, message_text)
            decision["intent"] = "clarification"
    decision = enforce_ai_first_booking_order(decision, conversation, message_text)
    return decision


def apply_ai_first_decision_to_conversation(
    conversation: dict[str, Any],
    decision: dict[str, Any],
    message_text: str,
) -> None:
    conversation["last_customer_message"] = message_text
    service = sanitize_text(str(decision.get("extracted_service") or ""))
    service_meta = match_service_catalog(service, service) if service else None
    if service_meta:
        conversation["service"] = service_meta.get("display")
    name = titlecase_name(decision.get("extracted_name"))
    if name and not is_invalid_name_attempt(name, "collect_name"):
        conversation["full_name"] = name
    phone = canonical_phone(decision.get("extracted_phone"))
    if phone:
        conversation["phone"] = phone
    requested_date = normalize_date_string(decision.get("requested_date"))
    if requested_date:
        conversation["requested_date"] = requested_date
    requested_time = normalize_time_string(decision.get("requested_time"))
    if requested_time:
        conversation["requested_time"] = requested_time
        conversation["preferred_period"] = conversation.get("preferred_period") or infer_period_from_time(requested_time)

    if llm_bool(decision.get("handoff_needed")):
        conversation["state"] = "human_handoff"
        conversation["assigned_human"] = True
        conversation["appointment_status"] = "handoff"
        return

    booking_intent = llm_bool(decision.get("booking_intent"))
    if not booking_intent:
        if sanitize_text(conversation.get("state") or "") in {"collect_name", "collect_phone", "collect_date", "collect_period", "collect_time"}:
            conversation["state"] = "collect_service"
        return

    if not conversation.get("booking_kind"):
        conversation["booking_kind"] = "preconsultation"

    missing = set(decision.get("missing_fields") or [])
    if not conversation.get("service"):
        conversation["state"] = "collect_service"
    elif "full_name" in missing or "name" in missing or not conversation.get("full_name"):
        conversation["state"] = "collect_name"
    elif "phone" in missing or not conversation.get("phone"):
        conversation["state"] = "collect_phone"
    elif "requested_date" in missing or "date" in missing or not conversation.get("requested_date"):
        conversation["state"] = "collect_date"
    elif "requested_time" in missing or "time" in missing or not conversation.get("requested_time"):
        conversation["state"] = "collect_time"
    else:
        conversation["state"] = "collect_time"


def get_or_create_conversation(conn: psycopg.Connection, sender_id: str, username: str | None) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO conversations (instagram_user_id, instagram_username)
            VALUES (%s, %s)
            ON CONFLICT (instagram_user_id) DO UPDATE
            SET instagram_username = COALESCE(conversations.instagram_username, EXCLUDED.instagram_username)
            RETURNING *
            """,
            (sender_id, username),
        )
        row = cur.fetchone()
        conn.commit()
        data = serialize_row(row)
        if username and not data.get("instagram_username"):
            data["instagram_username"] = username
        return data


def upsert_conversation(conn: psycopg.Connection, conversation: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO conversations (
                instagram_user_id,
                instagram_username,
                full_name,
                phone,
                service,
                requested_date,
                requested_time,
                appointment_status,
                state,
                booking_kind,
                preferred_period,
                assigned_human,
                last_customer_message,
                llm_notes,
                memory_state,
                updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s::date, %s::time, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, NOW())
            ON CONFLICT (instagram_user_id) DO UPDATE SET
                instagram_username = EXCLUDED.instagram_username,
                full_name = EXCLUDED.full_name,
                phone = EXCLUDED.phone,
                service = EXCLUDED.service,
                requested_date = EXCLUDED.requested_date,
                requested_time = EXCLUDED.requested_time,
                appointment_status = EXCLUDED.appointment_status,
                state = EXCLUDED.state,
                booking_kind = EXCLUDED.booking_kind,
                preferred_period = EXCLUDED.preferred_period,
                assigned_human = EXCLUDED.assigned_human,
                last_customer_message = EXCLUDED.last_customer_message,
                llm_notes = EXCLUDED.llm_notes,
                memory_state = EXCLUDED.memory_state,
                updated_at = NOW()
            """,
            (
                conversation.get("instagram_user_id"),
                conversation.get("instagram_username"),
                conversation.get("full_name"),
                conversation.get("phone"),
                conversation.get("service"),
                conversation.get("requested_date"),
                conversation.get("requested_time"),
                conversation.get("appointment_status", "collecting"),
                conversation.get("state", "new"),
                conversation.get("booking_kind"),
                conversation.get("preferred_period"),
                bool(conversation.get("assigned_human")),
                conversation.get("last_customer_message"),
                conversation.get("llm_notes"),
                json.dumps(ensure_conversation_memory(conversation), ensure_ascii=False),
            ),
        )
    conn.commit()


def save_message_log(conn: psycopg.Connection, sender_id: str, direction: str, message_text: str | None, raw_payload: dict[str, Any] | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO message_logs (instagram_user_id, direction, message_text, raw_payload) VALUES (%s, %s, %s, %s::jsonb)",
            (sender_id, direction, message_text, json.dumps(raw_payload or {}, ensure_ascii=False)),
        )
    conn.commit()


def get_recent_message_history(conn: psycopg.Connection, sender_id: str, limit: int = HISTORY_MESSAGE_LIMIT) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT direction, message_text, created_at FROM message_logs WHERE instagram_user_id = %s ORDER BY created_at DESC LIMIT %s",
            (sender_id, limit),
        )
        rows = cur.fetchall()

    history: list[dict[str, Any]] = []
    for row in reversed(rows):
        history.append(
            {
                "direction": row["direction"],
                "message_text": row["message_text"],
                "created_at": row["created_at"].isoformat() if row.get("created_at") else None,
            }
        )
    return history


def wants_availability_information(text: str, llm_data: dict[str, Any]) -> bool:
    lowered = text.lower()
    if any(keyword in lowered for keyword in AVAILABILITY_KEYWORDS):
        return True
    return llm_data.get("intent") in {"info", "availability", "appointment"} and has_date_cue(text)


def format_human_date(date_value: str) -> str:
    return date.fromisoformat(date_value).strftime("%d.%m.%Y")


def get_local_taken_slots(conn: psycopg.Connection, date_value: str) -> set[str]:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT appointment_time FROM appointments WHERE appointment_date = %s::date AND status IN ('confirmed', 'preconsultation') ORDER BY appointment_time ASC",
            (date_value,),
        )
        return {row["appointment_time"].strftime("%H:%M") for row in cur.fetchall()}


def get_taken_slots_for_date(conn: psycopg.Connection, date_value: str) -> set[str]:
    taken = get_local_taken_slots(conn, date_value)
    if is_live_crm_configured():
        taken |= live_crm_list_taken_slots(date_value)
    return taken


ACTIVE_SLOT_STATUSES = {"confirmed", "preconsultation", "scheduled"}
INACTIVE_ATTENDANCE_STATUSES = {"completed", "no_show", "canceled", "cancelled"}


def resolve_service_capacity_slug(service_name: str | None) -> str:
    if not service_name:
        return sanitize_service_slug(LIVE_CRM_PRECONSULTATION_SERVICE)
    catalog_match = match_service_catalog(service_name, service_name)
    if catalog_match and catalog_match.get("slug"):
        return sanitize_service_slug(str(catalog_match["slug"]))
    return sanitize_service_slug(service_name)


def get_service_capacity(conn: psycopg.Connection, service_name: str | None) -> int:
    slug = resolve_service_capacity_slug(service_name)
    fallback = get_default_service_capacity(service_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT capacity
            FROM service_capacity_rules
            WHERE service_slug = %s AND active = TRUE
            LIMIT 1
            """,
            (slug,),
        )
        row = cur.fetchone()
    if not row:
        return fallback
    return max(1, int(row.get("capacity") or fallback or 1))


def get_slot_service_usage(conn: psycopg.Connection, date_value: str, time_value: str, service_name: str | None) -> int:
    requested_slug = resolve_service_capacity_slug(service_name)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT service, capacity_units
            FROM appointments
            WHERE appointment_date = %s::date
              AND appointment_time = %s::time
              AND status IN ('confirmed', 'preconsultation', 'scheduled')
              AND COALESCE(attendance_status, 'scheduled') NOT IN ('completed', 'no_show', 'canceled', 'cancelled')
            """,
            (date_value, time_value),
        )
        rows = cur.fetchall()
    total = 0
    for row in rows:
        if resolve_service_capacity_slug(row.get("service")) == requested_slug:
            total += max(1, int(row.get("capacity_units") or 1))
    return total


def is_slot_capacity_available(conn: psycopg.Connection, date_value: str, time_value: str, service_name: str | None) -> bool:
    capacity = get_service_capacity(conn, service_name)
    current_count = get_slot_service_usage(conn, date_value, time_value, service_name)
    return is_slot_capacity_available_from_counts(current_count, capacity)


def lock_capacity_slot(cur: Any, date_value: str, time_value: str, service_name: str | None) -> None:
    slug = resolve_service_capacity_slug(service_name)
    cur.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"{date_value}|{time_value}|{slug}",))


def build_calendar_slots(conn: psycopg.Connection, date_value: str) -> dict[str, Any]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, instagram_user_id, instagram_username, full_name, phone, service,
                   appointment_date, appointment_time, status, attendance_status,
                   approval_status, notes, capacity_units
            FROM appointments
            WHERE appointment_date = %s::date
              AND status IN ('confirmed', 'preconsultation', 'scheduled')
              AND COALESCE(attendance_status, 'scheduled') NOT IN ('completed', 'no_show', 'canceled', 'cancelled')
            ORDER BY appointment_time ASC, id ASC
            """,
            (date_value,),
        )
        appointments = filter_business_records([serialize_row(row) for row in cur.fetchall()])
    by_time: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for appointment in appointments:
        slot_time = str(appointment.get("appointment_time") or "")[:5]
        service_name = appointment.get("service") or LIVE_CRM_PRECONSULTATION_SERVICE
        slug = resolve_service_capacity_slug(service_name)
        by_time.setdefault(slot_time, {}).setdefault(slug, []).append(appointment)

    slot_step = SLOT_DURATION_MINUTES + SLOT_BUFFER_MINUTES
    slots: list[dict[str, Any]] = []
    current = datetime.combine(date.fromisoformat(date_value), WORK_START)
    end = datetime.combine(date.fromisoformat(date_value), WORK_END)
    default_service = LIVE_CRM_PRECONSULTATION_SERVICE
    default_capacity = get_service_capacity(conn, default_service)
    while current < end:
        slot = current.time().strftime("%H:%M")
        services: list[dict[str, Any]] = []
        total_used = 0
        total_capacity = default_capacity
        for service_appointments in by_time.get(slot, {}).values():
            service_name = service_appointments[0].get("service") or default_service
            used = sum(max(1, int(item.get("capacity_units") or 1)) for item in service_appointments)
            capacity = get_service_capacity(conn, service_name)
            total_used += used
            total_capacity = max(total_capacity, capacity)
            services.append({
                "service": service_name,
                "used": used,
                "capacity": capacity,
                "status": "full" if used >= capacity else "available",
                "appointments": service_appointments,
            })
        slots.append({
            "time": slot,
            "used": total_used,
            "capacity": total_capacity,
            "label": f"{total_used}/{total_capacity}",
            "status": "full" if total_used >= total_capacity else "available",
            "services": services,
        })
        current += timedelta(minutes=slot_step)
    return {"date": date_value, "slots": slots}


def _expand_taken_with_buffer(taken: set[str]) -> set[str]:
    """Expand taken slots to include buffer zones before and after each appointment."""
    if not taken or SLOT_BUFFER_MINUTES <= 0:
        return taken
    expanded = set(taken)
    for slot in taken:
        try:
            slot_minutes = to_minutes(slot)
        except ValueError:
            continue
        for offset in range(1, SLOT_BUFFER_MINUTES + 1):
            before = slot_minutes - offset
            after = slot_minutes + SLOT_DURATION_MINUTES - 1 + offset
            if before >= 0:
                expanded.add(f"{before // 60:02d}:{before % 60:02d}")
            if after < 24 * 60:
                expanded.add(f"{after // 60:02d}:{after % 60:02d}")
    return expanded


def get_available_slots_for_date(conn: psycopg.Connection, date_value: str, service_name: str | None = None) -> list[str]:
    slot_step = SLOT_DURATION_MINUTES + SLOT_BUFFER_MINUTES
    slots: list[str] = []
    current = datetime.combine(date.fromisoformat(date_value), WORK_START)
    end = datetime.combine(date.fromisoformat(date_value), WORK_END)
    while current < end:
        slot = current.time().strftime("%H:%M")
        if is_slot_capacity_available(conn, date_value, slot, service_name or LIVE_CRM_PRECONSULTATION_SERVICE):
            slots.append(slot)
        current += timedelta(minutes=slot_step)
    return slots


def get_available_booking_slots_for_date(conn: psycopg.Connection, date_value: str, service_name: str | None = None) -> list[str]:
    slots: list[str] = []
    current = datetime.combine(date.fromisoformat(date_value), WORK_START)
    end = datetime.combine(date.fromisoformat(date_value), WORK_END)
    while current < end:
        slot = current.time().strftime("%H:%M")
        if is_slot_capacity_available(conn, date_value, slot, service_name or LIVE_CRM_PRECONSULTATION_SERVICE):
            slots.append(slot)
        current += timedelta(minutes=max(30, SLOT_DURATION_MINUTES))
    return slots


def build_availability_reply(date_value: str, open_slots: list[str], ask_service: bool = False, period: str | None = None) -> str:
    human_date = format_human_date(date_value)
    visible_slots = open_slots[:4]
    slot_text = ", ".join(visible_slots)
    period_text = f" {get_period_label(period)} için" if period in {"morning", "afternoon"} else ""
    if ask_service:
        return f"{human_date}{period_text} şu saatlerimiz uygun görünüyor: {slot_text}. İlgilendiğiniz hizmeti de yazarsanız devam edebilirim."
    return f"{human_date}{period_text} şu saatlerimiz uygun görünüyor: {slot_text}. Size uyan net saati yazarsanız devam edebilirim."


def find_next_available_days(conn: psycopg.Connection, start_date_value: str, limit: int = 3, service_name: str | None = None) -> list[dict[str, Any]]:
    start_date = date.fromisoformat(start_date_value)
    results: list[dict[str, Any]] = []
    max_days = min(APPOINTMENT_LOOKAHEAD_DAYS, 14)
    for offset in range(1, max_days + 1):
        current_date = (start_date + timedelta(days=offset)).isoformat()
        slots = get_available_slots_for_date(conn, current_date, service_name)
        if slots:
            results.append({"date": current_date, "slots": slots[:3]})
        if len(results) >= limit:
            break
    return results


def build_no_availability_reply(date_value: str, next_days: list[dict[str, Any]], ask_service: bool = False) -> str:
    human_date = format_human_date(date_value)
    if next_days:
        suggestion_text = "; ".join(
            f"{format_human_date(item['date'])} için {', '.join(item['slots'])}" for item in next_days
        )
        tail = " İlgilendiğiniz hizmeti de yazarsanız devam edebilirim." if ask_service else " Size uygun olan gün ve saati yazarsanız devam edebilirim."
        return f"{human_date} için maalesef boş saat kalmadı. En yakın uygun ön görüşme seçenekleri: {suggestion_text}.{tail}"

    tail = " İlgilendiğiniz hizmeti de yazarsanız devam edebilirim." if ask_service else ""
    return f"{human_date} için maalesef boş saat kalmadı.{tail}"


def normalize_booking_slot_option(item: Any) -> dict[str, str] | None:
    if not isinstance(item, dict):
        return None
    slot_date = normalize_date_string(item.get("date"))
    slot_time = normalize_time_string(item.get("time"))
    if not slot_date or not slot_time:
        return None
    return {"date": slot_date, "time": slot_time}


def format_booking_slot_option(slot: dict[str, str]) -> str:
    return f"{format_human_date(slot['date'])} {slot['time']}"


def remember_booking_slot_options(conversation: dict[str, Any], slots: list[dict[str, str]]) -> None:
    memory = ensure_conversation_memory(conversation)
    cleaned: list[dict[str, str]] = []
    for slot in slots:
        normalized = normalize_booking_slot_option(slot)
        if normalized and normalized not in cleaned:
            cleaned.append(normalized)
        if len(cleaned) >= AI_FIRST_BOOKING_SLOT_LIMIT:
            break
    memory["suggested_booking_slots"] = cleaned
    conversation["memory_state"] = memory


def build_ai_first_booking_slot_reply(slots: list[dict[str, str]]) -> str:
    option_text = ", ".join(format_booking_slot_option(slot) for slot in slots[:AI_FIRST_BOOKING_SLOT_LIMIT])
    return f"Uygun ilk seçenekler: {option_text}. Size uyan gün ve saati yazarsanız kaydı oluşturayım."


def collect_next_booking_slot_options(
    conn: psycopg.Connection,
    conversation: dict[str, Any],
    *,
    start_date_value: str | None = None,
    preferred_time: str | None = None,
    limit: int | None = None,
) -> list[dict[str, str]]:
    service_name = conversation.get("service") or LIVE_CRM_PRECONSULTATION_SERVICE
    limit = max(1, int(limit or AI_FIRST_BOOKING_SLOT_LIMIT))
    normalized_preferred_time = normalize_time_string(preferred_time)
    start_date = date.fromisoformat(normalize_date_string(start_date_value) or datetime.now(TZ).date().isoformat())
    max_days = min(APPOINTMENT_LOOKAHEAD_DAYS, 14)
    exact_options: list[dict[str, str]] = []
    fallback_options: list[dict[str, str]] = []

    for offset in range(1, max_days + 1):
        current_date = (start_date + timedelta(days=offset)).isoformat()
        open_slots = get_available_booking_slots_for_date(conn, current_date, service_name)
        if normalized_preferred_time:
            for slot in open_slots:
                option = {"date": current_date, "time": slot}
                if slot == normalized_preferred_time:
                    exact_options.append(option)
                else:
                    fallback_options.append(option)
        else:
            for slot in open_slots:
                exact_options.append({"date": current_date, "time": slot})
        if len(exact_options) >= limit:
            break

    if normalized_preferred_time and not exact_options:
        try:
            preferred_minutes = to_minutes(normalized_preferred_time)
            fallback_options.sort(key=lambda slot: abs(to_minutes(slot["time"]) - preferred_minutes))
        except ValueError:
            pass
        return fallback_options[:limit]
    return exact_options[:limit]


def build_ambiguous_time_choice_reply(time_value: str, slots: list[dict[str, str]]) -> str:
    dates = ", ".join(format_human_date(slot["date"]) for slot in slots[:AI_FIRST_BOOKING_SLOT_LIMIT])
    return f"{time_value} için birden fazla uygun gün var: {dates}. Hangi günü seçmek istersiniz?"


def prepare_ai_first_booking_availability(
    conn: psycopg.Connection | None,
    conversation: dict[str, Any],
    *,
    detected_date: str | None = None,
    detected_time: str | None = None,
    start_date_value: str | None = None,
) -> dict[str, Any]:
    result: dict[str, Any] = {"reply_text": None, "ready_to_book": False}
    if not (conversation.get("service") and conversation.get("full_name") and conversation.get("phone")):
        return result

    memory = ensure_conversation_memory(conversation)
    normalized_date = normalize_date_string(detected_date) or normalize_date_string(conversation.get("requested_date"))
    normalized_time = normalize_time_string(detected_time)
    pending_time = normalize_time_string(memory.get("pending_requested_time"))

    if normalized_date and pending_time and not normalized_time:
        conversation["requested_date"] = normalized_date
        conversation["requested_time"] = pending_time
        memory["pending_requested_time"] = None
        conversation["memory_state"] = memory
        result["ready_to_book"] = True
        return result

    if normalized_time and not normalized_date:
        conversation["requested_time"] = None
        suggested_slots = [
            slot
            for slot in (normalize_booking_slot_option(item) for item in memory.get("suggested_booking_slots") or [])
            if slot
        ]
        matching_slots = [slot for slot in suggested_slots if slot["time"] == normalized_time]
        if len(matching_slots) == 1:
            conversation["requested_date"] = matching_slots[0]["date"]
            conversation["requested_time"] = normalized_time
            memory["pending_requested_time"] = None
            conversation["memory_state"] = memory
            result["ready_to_book"] = True
            return result
        if len(matching_slots) > 1:
            memory["pending_requested_time"] = normalized_time
            conversation["memory_state"] = memory
            conversation["state"] = "collect_date"
            result["reply_text"] = build_ambiguous_time_choice_reply(normalized_time, matching_slots)
            return result

        if conn is not None:
            next_slots = collect_next_booking_slot_options(
                conn,
                conversation,
                start_date_value=start_date_value,
                preferred_time=normalized_time,
            )
            if next_slots:
                exact_slots = [slot for slot in next_slots if slot["time"] == normalized_time]
                if len(exact_slots) == 1:
                    conversation["requested_date"] = exact_slots[0]["date"]
                    conversation["requested_time"] = normalized_time
                    result["ready_to_book"] = True
                    return result
                if len(exact_slots) > 1:
                    memory["pending_requested_time"] = normalized_time
                    conversation["memory_state"] = memory
                    conversation["state"] = "collect_date"
                    result["reply_text"] = build_ambiguous_time_choice_reply(normalized_time, exact_slots)
                    return result
                remember_booking_slot_options(conversation, next_slots)
                conversation["state"] = "collect_time"
                result["reply_text"] = (
                    f"{normalized_time} için uygunluk bulamadım. Yakın uygun seçenekler: "
                    f"{', '.join(format_booking_slot_option(slot) for slot in next_slots)}. Size uyanı yazarsanız kaydı oluşturayım."
                )
                return result

    if normalized_date and not (normalize_time_string(conversation.get("requested_time")) or normalized_time):
        if conn is None:
            return result
        open_slots = get_available_booking_slots_for_date(conn, normalized_date, conversation.get("service"))
        if open_slots:
            slots = [{"date": normalized_date, "time": slot} for slot in open_slots[:AI_FIRST_BOOKING_SLOT_LIMIT]]
            remember_booking_slot_options(conversation, slots)
            conversation["requested_date"] = normalized_date
            conversation["state"] = "collect_time"
            result["reply_text"] = build_availability_reply(normalized_date, open_slots)
            return result
        next_days = find_next_available_days(conn, normalized_date, service_name=conversation.get("service"))
        conversation["state"] = "collect_date"
        result["reply_text"] = build_no_availability_reply(normalized_date, next_days)
        return result

    if not normalized_date and not normalize_time_string(conversation.get("requested_time")):
        if conn is None:
            return result
        next_slots = collect_next_booking_slot_options(
            conn,
            conversation,
            start_date_value=start_date_value,
        )
        if next_slots:
            remember_booking_slot_options(conversation, next_slots)
            conversation["state"] = "collect_time"
            result["reply_text"] = build_ai_first_booking_slot_reply(next_slots)
            return result
        conversation["state"] = "human_handoff"
        conversation["assigned_human"] = True
        result["reply_text"] = "Şu an uygun saat bulamadım. Kaydınızı manuel kontrol için ekibe iletiyorum."
        return result

    return result


def validate_slot(date_value: Any, time_value: Any) -> str | None:
    normalized_date = normalize_date_string(date_value)
    normalized_time = normalize_time_string(time_value)
    if not normalized_date or not normalized_time:
        return "Tarih veya saat formatını anlayamadım. Lütfen örnek olarak 12.04 ve 14:00 şeklinde yazın."
    try:
        requested_date = date.fromisoformat(normalized_date)
        requested_time = time.fromisoformat(normalized_time)
    except ValueError:
        return "Tarih veya saat formatını anlayamadım. Lütfen örnek olarak 12.04 ve 14:00 şeklinde yazın."

    today = datetime.now(TZ).date()
    if requested_date < today:
        return "Geçmiş bir tarih seçilemez. Lütfen bugün veya ileri bir tarih yazın."
    if requested_date > today + timedelta(days=APPOINTMENT_LOOKAHEAD_DAYS):
        return f"Şu an en fazla {APPOINTMENT_LOOKAHEAD_DAYS} gün sonrası için randevu açabiliyorum."
    if requested_time < WORK_START or requested_time >= WORK_END:
        return f"Çalışma saatlerimiz {WORKING_HOURS_START} - {WORKING_HOURS_END} arası. Bu aralıktan bir saat yazar mısınız?"
    return None


def find_existing_appointment(conn: psycopg.Connection, date_value: str, time_value: str, service_name: str | None = None) -> dict[str, Any] | None:
    """Return a conflict only when the requested service capacity is full."""
    service = service_name or LIVE_CRM_PRECONSULTATION_SERVICE
    if is_slot_capacity_available(conn, date_value, time_value, service):
        return None
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, full_name, appointment_date, appointment_time, service
            FROM appointments
            WHERE appointment_date = %s::date
              AND appointment_time = %s::time
              AND status IN ('confirmed', 'preconsultation', 'scheduled')
              AND COALESCE(attendance_status, 'scheduled') NOT IN ('completed', 'no_show', 'canceled', 'cancelled')
            ORDER BY id ASC
            LIMIT 1
            """,
            (date_value, time_value),
        )
        row = cur.fetchone()
        if row:
            return serialize_row(row)
    return {"id": None, "full_name": "capacity_full", "appointment_date": date_value, "appointment_time": time_value, "service": service}


def suggest_alternatives(conn: psycopg.Connection, date_value: str, requested_time_value: str, service_name: str | None = None) -> list[str]:
    slot_step = SLOT_DURATION_MINUTES + SLOT_BUFFER_MINUTES
    slots: list[str] = []
    current = datetime.combine(date.fromisoformat(date_value), WORK_START)
    end = datetime.combine(date.fromisoformat(date_value), WORK_END)
    while current < end:
        slot = current.time().strftime("%H:%M")
        if is_slot_capacity_available(conn, date_value, slot, service_name or LIVE_CRM_PRECONSULTATION_SERVICE):
            slots.append(slot)
        current += timedelta(minutes=slot_step)

    if not slots:
        return []

    try:
        requested_minutes = to_minutes(requested_time_value)
        slots.sort(key=lambda slot: abs(to_minutes(slot) - requested_minutes))
    except ValueError:
        pass
    return slots[:3]


def to_minutes(value: str) -> int:
    parsed = time.fromisoformat(value)
    return parsed.hour * 60 + parsed.minute


def create_appointment(conn: psycopg.Connection, conversation: dict[str, Any], username: str | None) -> tuple[int, int]:
    live_crm_ms = 0
    booking_kind = get_booking_kind(conversation)
    if not conversation.get("service"):
        conversation["service"] = LIVE_CRM_PRECONSULTATION_SERVICE if booking_kind == "preconsultation" else DEFAULT_SERVICE_NAME
    local_status = "confirmed" if booking_kind == "appointment" else "preconsultation"

    try:
        with conn.cursor() as cur:
            requested_date = normalize_date_string(conversation.get("requested_date"))
            requested_time = normalize_time_string(conversation.get("requested_time"))
            requested_service = conversation.get("service")
            lock_capacity_slot(cur, requested_date, requested_time, requested_service)
            cur.execute(
                """
                SELECT id, instagram_user_id, status
                FROM appointments
                WHERE instagram_user_id = %s
                  AND appointment_date = %s::date
                  AND appointment_time = %s::time
                  AND status IN ('confirmed', 'preconsultation', 'scheduled')
                LIMIT 1
                """,
                (conversation.get("instagram_user_id"), requested_date, requested_time),
            )
            existing_slot = cur.fetchone()

            if existing_slot:
                if False and str(existing_slot.get("instagram_user_id") or "") != str(conversation.get("instagram_user_id") or ""):
                    raise HTTPException(status_code=409, detail="Bu slot artık dolu.")
                resolved_status = "confirmed" if existing_slot.get("status") == "confirmed" else local_status
                cur.execute(
                    """
                    UPDATE appointments
                    SET instagram_user_id = %s,
                        instagram_username = %s,
                        full_name = %s,
                        phone = %s,
                        service = %s,
                        status = %s,
                        source = 'instagram_dm',
                        notes = %s,
                        updated_at = NOW()
                    WHERE id = %s
                    RETURNING id
                    """,
                    (
                        conversation.get("instagram_user_id"),
                        username or conversation.get("instagram_username"),
                        conversation.get("full_name"),
                        conversation.get("phone"),
                        conversation.get("service"),
                        resolved_status,
                        conversation.get("llm_notes"),
                        existing_slot["id"],
                    ),
                )
            else:
                cur.execute(
                    """
                    INSERT INTO appointments (
                        instagram_user_id,
                        instagram_username,
                        full_name,
                        phone,
                        service,
                        appointment_date,
                        appointment_time,
                        status,
                        notes
                    ) VALUES (%s, %s, %s, %s, %s, %s::date, %s::time, %s, %s)
                    RETURNING id
                    """,
                    (
                        conversation.get("instagram_user_id"),
                        username or conversation.get("instagram_username"),
                        conversation.get("full_name"),
                        conversation.get("phone"),
                        conversation.get("service"),
                        requested_date,
                        requested_time,
                        local_status,
                        conversation.get("llm_notes"),
                    ),
                )
            row = cur.fetchone()

        if is_live_crm_configured():
            live_crm_started_at = time_module.perf_counter()
            try:
                if booking_kind == "appointment":
                    live_crm_upsert_appointment(conversation)
                else:
                    live_crm_upsert_preconsultation(conversation)
                    live_crm_ensure_task_for_conversation(dict(conversation))
            except HTTPException:
                conn.rollback()
                raise
            except Exception as exc:  # noqa: BLE001
                conn.rollback()
                raise HTTPException(status_code=503, detail="Canlı CRM eşitlemesi tamamlanamadı.") from exc
            live_crm_ms = elapsed_ms(live_crm_started_at)

        appointment_id = int(row["id"])
        customer = upsert_customer_from_conversation(conn, conversation)
        if customer:
            record_customer_history(conn, int(customer["id"]), conversation, appointment_id)
            scheduled_base = None
            requested_date = normalize_date_string(conversation.get("requested_date"))
            requested_time = normalize_time_string(conversation.get("requested_time"))
            if requested_date and requested_time:
                scheduled_base = datetime.fromisoformat(f"{requested_date}T{requested_time}:00").replace(tzinfo=TZ)
            schedule_customer_automation_events(conn, int(customer["id"]), customer.get("sector"), base_time=scheduled_base)

        conn.commit()
        return appointment_id, live_crm_ms
    except psycopg.errors.UniqueViolation as exc:
        conn.rollback()
        raise HTTPException(status_code=409, detail={"type": "slot_conflict", "message": "Bu slot artık dolu."}) from exc


def is_crm_sync_configured() -> bool:
    return all([
        CRM_SYNC_ENABLED,
        CRM_SUPABASE_URL,
        CRM_SUPABASE_ANON_KEY,
        CRM_SUPABASE_EMAIL,
        CRM_SUPABASE_PASSWORD,
        CRM_WORKSPACE_ID,
    ])


def crm_generate_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex[:12]}"


def normalize_phone_digits(value: str | None) -> str:
    return re.sub(r"\D", "", value or "")


def parse_price_hint(value: str | None) -> float:
    if not value:
        return 0.0
    digits = re.sub(r"[^0-9]", "", value)
    return float(digits) if digits else 0.0


def is_live_crm_configured() -> bool:
    return all([
        LIVE_CRM_ENABLED,
        LIVE_CRM_SUPABASE_URL,
        LIVE_CRM_SUPABASE_ANON_KEY,
        LIVE_CRM_EMAIL,
        LIVE_CRM_PASSWORD,
    ])


def build_live_crm_headers(access_token: str) -> dict[str, str]:
    return {
        "apikey": LIVE_CRM_SUPABASE_ANON_KEY,
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }


def live_crm_auth_session(force_refresh: bool = False) -> tuple[dict[str, str], str] | tuple[None, None]:
    if not is_live_crm_configured():
        return None, None

    now = time_module.time()
    with LIVE_CRM_AUTH_CACHE_LOCK:
        cached_headers = LIVE_CRM_AUTH_CACHE.get("headers")
        cached_user_id = LIVE_CRM_AUTH_CACHE.get("user_id")
        cached_expires_at = float(LIVE_CRM_AUTH_CACHE.get("expires_at") or 0)
        if not force_refresh and cached_headers and cached_user_id and cached_expires_at > now:
            return dict(cached_headers), str(cached_user_id)

    try:
        response = requests.post(
            f"{LIVE_CRM_SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers={
                "apikey": LIVE_CRM_SUPABASE_ANON_KEY,
                "Content-Type": "application/json",
            },
            json={
                "email": LIVE_CRM_EMAIL,
                "password": LIVE_CRM_PASSWORD,
            },
            timeout=CRM_SYNC_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        access_token = payload.get("access_token")
        user_id = payload.get("user", {}).get("id")
        if not access_token or not user_id:
            return None, None

        expires_in = int(payload.get("expires_in") or LIVE_CRM_AUTH_CACHE_SECONDS)
        cache_ttl = max(30, min(expires_in, LIVE_CRM_AUTH_CACHE_SECONDS) - 30)
        headers = build_live_crm_headers(access_token)
        with LIVE_CRM_AUTH_CACHE_LOCK:
            LIVE_CRM_AUTH_CACHE.clear()
            LIVE_CRM_AUTH_CACHE.update({
                "headers": headers,
                "user_id": str(user_id),
                "expires_at": time_module.time() + cache_ttl,
            })
        return dict(headers), str(user_id)
    except Exception:
        logger.exception("live_crm_auth_failed")
        return None, None


def live_crm_request(method: str, path: str, headers: dict[str, str], *, params: dict[str, Any] | None = None, json_body: Any | None = None, retry_auth: bool = True) -> requests.Response:
    response = requests.request(
        method,
        f"{LIVE_CRM_SUPABASE_URL}/rest/v1/{path}",
        headers=headers,
        params=params,
        json=json_body,
        timeout=CRM_SYNC_TIMEOUT_SECONDS,
    )
    if response.status_code in {401, 403} and retry_auth:
        fresh_headers, _ = live_crm_auth_session(force_refresh=True)
        if fresh_headers:
            response = requests.request(
                method,
                f"{LIVE_CRM_SUPABASE_URL}/rest/v1/{path}",
                headers=fresh_headers,
                params=params,
                json=json_body,
                timeout=CRM_SYNC_TIMEOUT_SECONDS,
            )
    response.raise_for_status()
    return response


def live_crm_service_category(service_name: str) -> str:
    matched = match_service_catalog(service_name, service_name)
    if not matched:
        return "Genel"
    if matched["slug"] == "web-tasarim":
        return "Web"
    if matched["slug"] == "otomasyon-ai":
        return "Otomasyon"
    if matched["slug"] == "performans-pazarlama":
        return "Pazarlama"
    if matched["slug"] == "sosyal-medya-yonetimi":
        return "Sosyal Medya"
    if matched["slug"] == "marka-stratejisi":
        return "Danışmanlık"
    return "Genel"


def has_live_crm_services_cache(user_id: str) -> bool:
    with LIVE_CRM_SERVICES_CACHE_LOCK:
        expires_at = float(LIVE_CRM_SERVICES_CACHE.get(str(user_id)) or 0)
    return expires_at > time_module.time()


def mark_live_crm_services_cache(user_id: str) -> None:
    with LIVE_CRM_SERVICES_CACHE_LOCK:
        LIVE_CRM_SERVICES_CACHE[str(user_id)] = time_module.time() + LIVE_CRM_SERVICES_CACHE_SECONDS


def live_crm_slot_cache_key(user_id: str, date_value: str) -> str:
    return f"{user_id}:{date_value}"


def get_live_crm_cached_slots(user_id: str, date_value: str) -> set[str] | None:
    cache_key = live_crm_slot_cache_key(user_id, date_value)
    with LIVE_CRM_SLOT_CACHE_LOCK:
        cached = LIVE_CRM_SLOT_CACHE.get(cache_key)
        if not cached:
            return None
        if float(cached.get("expires_at") or 0) <= time_module.time():
            LIVE_CRM_SLOT_CACHE.pop(cache_key, None)
            return None
        return set(cached.get("slots") or [])


def set_live_crm_cached_slots(user_id: str, date_value: str, slots: set[str]) -> None:
    cache_key = live_crm_slot_cache_key(user_id, date_value)
    with LIVE_CRM_SLOT_CACHE_LOCK:
        LIVE_CRM_SLOT_CACHE[cache_key] = {
            "slots": sorted(slots),
            "expires_at": time_module.time() + LIVE_CRM_SLOT_CACHE_SECONDS,
        }


def invalidate_live_crm_slot_cache(user_id: str, date_value: str | None) -> None:
    if not date_value:
        return
    with LIVE_CRM_SLOT_CACHE_LOCK:
        LIVE_CRM_SLOT_CACHE.pop(live_crm_slot_cache_key(user_id, date_value), None)


def ensure_live_crm_services(headers: dict[str, str], user_id: str) -> None:
    if has_live_crm_services_cache(user_id):
        return

    response = live_crm_request(
        "GET",
        "services",
        headers,
        params={
            "select": "id,name",
            "user_id": f"eq.{user_id}",
            "limit": "500",
        },
    )
    existing = {sanitize_text(item.get("name") or "").lower() for item in response.json() or []}
    payloads = []
    for service in DOEL_SERVICE_CATALOG:
        name = sanitize_text(service.get("display") or "")
        if not name or name.lower() in existing:
            continue
        payloads.append({
            "user_id": user_id,
            "name": name,
            "category": live_crm_service_category(name),
            "price": parse_price_hint(service.get("price")),
            "duration": 60,
        })
    if payloads:
        live_crm_request("POST", "services", headers, json_body=payloads)
    mark_live_crm_services_cache(user_id)


def live_crm_find_customer(headers: dict[str, str], user_id: str, conversation: dict[str, Any]) -> dict[str, Any] | None:
    phone = sanitize_text(conversation.get("phone") or "")
    if phone:
        response = live_crm_request(
            "GET",
            "customers",
            headers,
            params={
                "select": "id,name,phone,created_at",
                "user_id": f"eq.{user_id}",
                "phone": f"eq.{phone}",
                "limit": "1",
            },
        )
        rows = response.json() or []
        if rows:
            return rows[0]

    full_name = sanitize_text(conversation.get("full_name") or "")
    if full_name:
        response = live_crm_request(
            "GET",
            "customers",
            headers,
            params={
                "select": "id,name,phone,created_at",
                "user_id": f"eq.{user_id}",
                "name": f"eq.{full_name}",
                "order": "created_at.desc",
                "limit": "1",
            },
        )
        rows = response.json() or []
        if rows:
            return rows[0]

    instagram_username = sanitize_text(conversation.get("instagram_username") or "")
    if instagram_username:
        response = live_crm_request(
            "GET",
            "customers",
            headers,
            params={
                "select": "id,name,phone,created_at",
                "user_id": f"eq.{user_id}",
                "name": f"eq.{instagram_username}",
                "order": "created_at.desc",
                "limit": "1",
            },
        )
        rows = response.json() or []
        if rows:
            return rows[0]

    return None


def live_crm_upsert_customer(headers: dict[str, str], user_id: str, conversation: dict[str, Any], existing: dict[str, Any] | None = None) -> dict[str, Any]:
    existing = existing or live_crm_find_customer(headers, user_id, conversation)
    payload = {
        "user_id": user_id,
        "name": sanitize_text(conversation.get("full_name") or conversation.get("instagram_username") or "Instagram Lead"),
        "phone": sanitize_text(conversation.get("phone") or ""),
    }
    if existing:
        current_name = sanitize_text(existing.get("name") or "")
        current_phone = sanitize_text(existing.get("phone") or "")
        if current_name != payload["name"] or current_phone != payload["phone"]:
            live_crm_request(
                "PATCH",
                "customers",
                headers,
                params={
                    "id": f"eq.{existing['id']}",
                    "user_id": f"eq.{user_id}",
                },
                json_body={
                    "name": payload["name"],
                    "phone": payload["phone"],
                },
            )
        return {**existing, **payload, "id": existing["id"]}
    response = live_crm_request("POST", "customers", headers, json_body=[payload])
    rows = response.json() or []
    if not rows:
        raise HTTPException(status_code=503, detail="Canlı CRM müşteri kaydı oluşturulamadı.")
    return rows[0]


def live_crm_list_taken_slots(date_value: str, headers: dict[str, str] | None = None, user_id: str | None = None, force_refresh: bool = False) -> set[str]:
    headers = dict(headers or {})
    if not headers or not user_id:
        headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        return set()

    if not force_refresh:
        cached = get_live_crm_cached_slots(user_id, date_value)
        if cached is not None:
            return cached

    response = live_crm_request(
        "GET",
        "appointments",
        headers,
        params={
            "select": "id,time,status",
            "user_id": f"eq.{user_id}",
            "date": f"eq.{date_value}",
            "limit": "500",
        },
    )
    slots = {
        str(item.get("time") or "")[:5]
        for item in (response.json() or [])
        if item.get("time") and str(item.get("status") or "") in {"scheduled", LIVE_CRM_PRECONSULTATION_STATUS}
    }
    set_live_crm_cached_slots(user_id, date_value, slots)
    return slots


def live_crm_find_slot_appointment(date_value: str, time_value: str, headers: dict[str, str] | None = None, user_id: str | None = None) -> dict[str, Any] | None:
    headers = dict(headers or {})
    if not headers or not user_id:
        headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        return None

    normalized_time = str(time_value)[:5]
    taken_slots = live_crm_list_taken_slots(date_value, headers, user_id)
    if normalized_time not in taken_slots:
        return None

    response = live_crm_request(
        "GET",
        "appointments",
        headers,
        params={
            "select": "id,customer_id,customer_name,service,date,time,status",
            "user_id": f"eq.{user_id}",
            "date": f"eq.{date_value}",
            "time": f"eq.{normalized_time}",
            "limit": "5",
        },
    )
    rows = response.json() or []
    for row in rows:
        if str(row.get("status") or "") in {"scheduled", LIVE_CRM_PRECONSULTATION_STATUS}:
            return row
    return None


def live_crm_find_customer_active_appointment(conversation: dict[str, Any], headers: dict[str, str] | None = None, user_id: str | None = None, customer: dict[str, Any] | None = None) -> dict[str, Any] | None:
    headers = dict(headers or {})
    if not headers or not user_id:
        headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        return None
    params = {
        "select": "id,customer_id,customer_name,service,date,time,status",
        "user_id": f"eq.{user_id}",
        "status": "eq.scheduled",
        "date": f"gte.{datetime.now(TZ).date().isoformat()}",
        "order": "date.asc,time.asc",
        "limit": "1",
    }
    if customer:
        params["customer_id"] = f"eq.{customer['id']}"
    elif conversation.get("full_name"):
        params["customer_name"] = f"eq.{sanitize_text(conversation['full_name'])}"
    else:
        return None
    response = live_crm_request("GET", "appointments", headers, params=params)
    rows = response.json() or []
    return rows[0] if rows else None


def live_crm_find_customer_preconsultation(conversation: dict[str, Any], headers: dict[str, str] | None = None, user_id: str | None = None, customer: dict[str, Any] | None = None) -> dict[str, Any] | None:
    headers = dict(headers or {})
    if not headers or not user_id:
        headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        return None
    params = {
        "select": "id,customer_id,customer_name,service,date,time,status",
        "user_id": f"eq.{user_id}",
        "status": f"eq.{LIVE_CRM_PRECONSULTATION_STATUS}",
        "order": "date.desc,time.desc",
        "limit": "1",
    }
    if customer:
        params["customer_id"] = f"eq.{customer['id']}"
    elif conversation.get("full_name"):
        params["customer_name"] = f"eq.{sanitize_text(conversation['full_name'])}"
    else:
        return None
    response = live_crm_request("GET", "appointments", headers, params=params)
    rows = response.json() or []
    return rows[0] if rows else None


def live_crm_upsert_preconsultation(conversation: dict[str, Any], headers: dict[str, str] | None = None, user_id: str | None = None, customer: dict[str, Any] | None = None) -> dict[str, Any] | None:
    headers = dict(headers or {})
    if not headers or not user_id:
        headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        return None

    ensure_live_crm_services(headers, user_id)
    existing_customer = customer or live_crm_find_customer(headers, user_id, conversation)
    customer = customer or live_crm_upsert_customer(headers, user_id, conversation, existing=existing_customer)

    requested_date = sanitize_text(conversation.get("requested_date") or "")
    requested_time = str(conversation.get("requested_time") or "")[:5]
    if requested_date and requested_time:
        existing_slot = live_crm_find_slot_appointment(requested_date, requested_time, headers, user_id)
        if existing_slot and str(existing_slot.get("customer_id") or "") != str(customer.get("id") or ""):
            raise HTTPException(status_code=409, detail="Bu slot artık dolu.")

    active_appointment = live_crm_find_customer_active_appointment(conversation, headers, user_id, customer)
    if active_appointment:
        return active_appointment

    requested_date = sanitize_text(conversation.get("requested_date") or "")
    requested_time = str(conversation.get("requested_time") or "")[:5]
    requested_service = sanitize_text(conversation.get("service") or "") or LIVE_CRM_PRECONSULTATION_SERVICE

    if not requested_date and not requested_time and requested_service == LIVE_CRM_PRECONSULTATION_SERVICE:
        return None

    payload = {
        "user_id": user_id,
        "customer_id": customer["id"],
        "customer_name": customer.get("name") or sanitize_text(conversation.get("full_name") or ""),
        "service": requested_service,
        "date": requested_date,
        "time": requested_time,
        "status": LIVE_CRM_PRECONSULTATION_STATUS,
    }

    existing = live_crm_find_customer_preconsultation(conversation, headers, user_id, customer)
    if existing:
        changed = any(str(existing.get(field) or "") != str(payload.get(field) or "") for field in ["customer_name", "service", "date", "time", "status"])
        if changed:
            live_crm_request(
                "PATCH",
                "appointments",
                headers,
                params={
                    "id": f"eq.{existing['id']}",
                    "user_id": f"eq.{user_id}",
                },
                json_body={
                    "customer_name": payload["customer_name"],
                    "service": payload["service"],
                    "date": payload["date"],
                    "time": payload["time"],
                    "status": payload["status"],
                },
            )
        return {**existing, **payload, "id": existing["id"]}

    created = live_crm_request("POST", "appointments", headers, json_body=[payload]).json() or []
    if not created:
        return None
    return created[0]


def live_crm_upsert_appointment(conversation: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], str]:
    headers, user_id = live_crm_auth_session()
    if not headers or not user_id:
        raise HTTPException(status_code=503, detail="Canlı CRM bağlantısı kurulamadı.")

    ensure_live_crm_services(headers, user_id)
    requested_date = conversation.get("requested_date")
    requested_time = str(conversation.get("requested_time") or "")[:5]
    existing_customer = live_crm_find_customer(headers, user_id, conversation)
    customer = live_crm_upsert_customer(headers, user_id, conversation, existing=existing_customer)

    if requested_date and requested_time:
        existing_slot = live_crm_find_slot_appointment(requested_date, requested_time, headers, user_id)
        if existing_slot and str(existing_slot.get("customer_id") or "") != str(customer.get("id") or ""):
            raise HTTPException(status_code=409, detail="Bu slot artık dolu.")

    active_appointment = live_crm_find_customer_active_appointment(conversation, headers, user_id, customer)
    if active_appointment:
        same_slot = active_appointment.get("date") == requested_date and str(active_appointment.get("time") or "")[:5] == requested_time
        if not same_slot:
            raise HTTPException(
                status_code=409,
                detail={
                    "type": "existing_customer_appointment",
                    "date": active_appointment.get("date"),
                    "time": str(active_appointment.get("time") or "")[:5],
                    "service": active_appointment.get("service"),
                },
            )
        invalidate_live_crm_slot_cache(user_id, requested_date)
        return customer, active_appointment, user_id

    payload = {
        "user_id": user_id,
        "customer_id": customer["id"],
        "customer_name": customer.get("name") or sanitize_text(conversation.get("full_name") or ""),
        "service": sanitize_text(conversation.get("service") or DEFAULT_SERVICE_NAME),
        "date": requested_date,
        "time": requested_time,
        "status": "scheduled",
    }

    existing_preconsultation = live_crm_find_customer_preconsultation(conversation, headers, user_id, customer)
    if existing_preconsultation:
        live_crm_request(
            "PATCH",
            "appointments",
            headers,
            params={
                "id": f"eq.{existing_preconsultation['id']}",
                "user_id": f"eq.{user_id}",
            },
            json_body=payload,
        )
        refreshed = live_crm_request(
            "GET",
            "appointments",
            headers,
            params={
                "select": "id,customer_id,customer_name,service,date,time,status",
                "id": f"eq.{existing_preconsultation['id']}",
                "user_id": f"eq.{user_id}",
                "limit": "1",
            },
        ).json() or []
        appointment_row = refreshed[0] if refreshed else {**existing_preconsultation, **payload, "id": existing_preconsultation["id"]}
    else:
        created = live_crm_request("POST", "appointments", headers, json_body=[payload]).json() or []
        if not created:
            raise HTTPException(status_code=503, detail="Canlı CRM randevu kaydı oluşturulamadı.")
        appointment_row = created[0]

    task_name_candidates = {
        sanitize_text(customer.get('name') or ''),
        sanitize_text(conversation.get('full_name') or ''),
        sanitize_text(conversation.get('instagram_username') or ''),
    }
    task_titles = {f"Instagram ön görüşme: {name}" for name in task_name_candidates if name}
    if task_titles and requested_date:
        existing_tasks = live_crm_request(
            "GET",
            "tasks",
            headers,
            params={
                "select": "id,title,due_date,completed",
                "user_id": f"eq.{user_id}",
                "due_date": f"eq.{requested_date}",
                "completed": "eq.false",
                "limit": "50",
            },
        ).json() or []
        matching_ids = [task.get("id") for task in existing_tasks if sanitize_text(task.get("title") or "") in task_titles]
        if matching_ids:
            joined_ids = ",".join(str(task_id) for task_id in matching_ids if task_id)
            if joined_ids:
                live_crm_request(
                    "PATCH",
                    "tasks",
                    headers,
                    params={
                        "user_id": f"eq.{user_id}",
                        "id": f"in.({joined_ids})",
                    },
                    json_body={"completed": True},
                )

    invalidate_live_crm_slot_cache(user_id, requested_date)
    return customer, appointment_row, user_id


def live_crm_ensure_task_for_conversation(conversation: dict[str, Any]) -> None:
    try:
        sender_id = str(conversation.get("instagram_user_id") or "")
        observed_at = str(conversation.get("_live_crm_task_observed_at") or "")
        if sender_id and observed_at:
            with LIVE_CRM_TASK_GUARD_LOCK:
                latest_observed_at = LIVE_CRM_TASK_LATEST_OBSERVED.get(sender_id)
            if latest_observed_at and observed_at < latest_observed_at:
                logger.info(
                    "live_crm_task_sync_skipped_stale sender_id=%s observed_at=%s latest_observed_at=%s",
                    sender_id,
                    observed_at,
                    latest_observed_at,
                )
                return
        headers, user_id = live_crm_auth_session()
        if not headers or not user_id:
            return
        requested_date = sanitize_text(conversation.get("requested_date") or "")
        customer_name = sanitize_text(conversation.get("full_name") or conversation.get("instagram_username") or "Instagram Lead")
        if not requested_date or not customer_name:
            return
        existing_customer = live_crm_find_customer(headers, user_id, conversation)
        customer = live_crm_upsert_customer(headers, user_id, conversation, existing=existing_customer)
        live_crm_upsert_preconsultation(conversation, headers, user_id, customer)
        task_title = f"Instagram ön görüşme: {customer_name}"
        existing_tasks = live_crm_request(
            "GET",
            "tasks",
            headers,
            params={
                "select": "id,title,due_date,completed",
                "user_id": f"eq.{user_id}",
                "title": f"eq.{task_title}",
                "due_date": f"eq.{requested_date}",
                "limit": "1",
            },
        ).json() or []
        if existing_tasks:
            return
        live_crm_request(
            "POST",
            "tasks",
            headers,
            json_body=[{
                "user_id": user_id,
                "title": task_title,
                "due_date": requested_date,
                "completed": False,
            }],
        )
    except Exception:
        logger.exception("live_crm_task_sync_failed")


def normalize_crm_payload(payload: dict[str, Any] | None) -> dict[str, Any]:
    base = payload if isinstance(payload, dict) else {}
    integrations = base.get("integrations") if isinstance(base.get("integrations"), dict) else {}
    ui = base.get("ui") if isinstance(base.get("ui"), dict) else {}
    inventory = base.get("inventory") if isinstance(base.get("inventory"), list) else (base.get("products") if isinstance(base.get("products"), list) else [])
    return {
        **base,
        "customers": list(base.get("customers") or []),
        "partners": list(base.get("partners") or []),
        "services": list(base.get("services") or []),
        "finance": list(base.get("finance") or []),
        "tasks": list(base.get("tasks") or []),
        "logs": list(base.get("logs") or []),
        "appointments": list(base.get("appointments") or []),
        "preConsultations": list(base.get("preConsultations") or []),
        "inventory": list(inventory or []),
        "integrations": {
            **integrations,
            "bindings": list(integrations.get("bindings") or []),
        },
        "syncEvents": list(base.get("syncEvents") or []),
        "ui": {
            **ui,
            "businessModel": "ecommerce" if ui.get("businessModel") == "ecommerce" else "service",
        },
    }


def crm_auth_session() -> tuple[dict[str, str], str] | tuple[None, None]:
    if not is_crm_sync_configured():
        return None, None
    try:
        response = requests.post(
            f"{CRM_SUPABASE_URL}/auth/v1/token?grant_type=password",
            headers={
                "apikey": CRM_SUPABASE_ANON_KEY,
                "Content-Type": "application/json",
            },
            json={
                "email": CRM_SUPABASE_EMAIL,
                "password": CRM_SUPABASE_PASSWORD,
            },
            timeout=CRM_SYNC_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        payload = response.json()
        access_token = payload.get("access_token")
        user_id = payload.get("user", {}).get("id")
        if not access_token or not user_id:
            return None, None
        return {
            "apikey": CRM_SUPABASE_ANON_KEY,
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
            "Prefer": "return=representation",
        }, user_id
    except Exception:
        return None, None


def build_crm_customer_note(conversation: dict[str, Any]) -> str:
    details = [
        f"Kaynak: {CRM_SYNC_SOURCE}",
        f"Instagram Hesabı: {IG_LOGIN_USERNAME or '-'}",
        f"Instagram Business User ID: {IG_BUSINESS_USER_ID or '-'}",
        f"AI Modeli: {LLM_MODEL}",
        f"Instagram User ID: {conversation.get('instagram_user_id')}",
        f"Akış durumu: {conversation.get('state')}",
    ]
    if conversation.get("requested_date") or conversation.get("requested_time"):
        parts = []
        if conversation.get("requested_date"):
            parts.append(format_human_date(conversation["requested_date"]))
        if conversation.get("requested_time"):
            parts.append(str(conversation["requested_time"])[:5])
        details.append(f"Ön görüşme: {' '.join(parts)}")
    if conversation.get("last_customer_message"):
        details.append(f"Son mesaj: {conversation['last_customer_message']}")
    return "\n".join(details)


def merge_note_text(existing: str | None, new_text: str) -> str:
    base_lines = [line.strip() for line in (existing or "").splitlines() if line.strip()]
    filtered_lines = [
        line
        for line in base_lines
        if not any(line.startswith(prefix) for prefix in CRM_AUTO_NOTE_PREFIXES)
    ]
    base = "\n".join(filtered_lines).strip()
    if not new_text:
        return base
    return f"{base}\n{new_text}".strip() if base else new_text


def derive_preconsultation_status(conversation: dict[str, Any]) -> str:
    if conversation.get("appointment_status") == "confirmed":
        return "converted"
    if conversation.get("requested_date") and conversation.get("requested_time"):
        return "scheduled"
    if any(conversation.get(field) for field in ["service", "phone", "full_name", "requested_date", "requested_time"]):
        return "waiting"
    return "new"


def map_crm_service_metadata(service_name: str | None, payload: dict[str, Any]) -> dict[str, Any]:
    cleaned_service = sanitize_text(service_name or DEFAULT_SERVICE_NAME)
    for service in payload.get("services", []):
        if sanitize_text(service.get("name") or "").lower() == cleaned_service.lower():
            return {
                "name": service.get("name") or cleaned_service,
                "price": float(service.get("price") or 0),
                "type": service.get("type") or "Tek Seferlik",
            }

    matched = match_service_catalog(cleaned_service, cleaned_service)
    if matched:
        recurring_slugs = {"otomasyon-ai", "performans-pazarlama", "sosyal-medya-yonetimi"}
        return {
            "name": matched.get("display") or cleaned_service,
            "price": parse_price_hint(matched.get("price")),
            "type": "Aylık" if matched.get("slug") in recurring_slugs else "Tek Seferlik",
        }

    return {
        "name": cleaned_service,
        "price": 0.0,
        "type": "Tek Seferlik",
    }


def find_crm_customer(payload: dict[str, Any], conversation: dict[str, Any]) -> dict[str, Any] | None:
    phone_digits = normalize_phone_digits(conversation.get("phone"))
    instagram_user_id = str(conversation.get("instagram_user_id") or "")
    full_name = sanitize_text(conversation.get("full_name") or "").lower()

    for customer in payload.get("customers", []):
        customer_phone = normalize_phone_digits(customer.get("phone"))
        if phone_digits and customer_phone and customer_phone == phone_digits:
            return customer
        if instagram_user_id and str(customer.get("instagramUserId") or "") == instagram_user_id:
            return customer
        if full_name and sanitize_text(customer.get("authorizedPerson") or customer.get("name") or "").lower() == full_name:
            return customer
    return None


def should_sync_crm_conversation(conversation: dict[str, Any]) -> bool:
    return any(
        conversation.get(field)
        for field in ["full_name", "phone", "service", "requested_date", "requested_time"]
    ) or conversation.get("appointment_status") == "confirmed"


def queue_crm_sync(
    background_tasks: BackgroundTasks,
    conversation: dict[str, Any],
    appointment_id: int | None = None,
    request_metrics: dict[str, Any] | None = None,
) -> bool:
    if not CRM_SYNC_ENABLED or not should_sync_crm_conversation(conversation):
        return False

    sync_payload = dict(conversation)
    observed_at = datetime.now(TZ).isoformat()
    sync_payload["_crm_observed_at"] = observed_at
    sender_id = str(conversation.get("instagram_user_id") or "")
    if sender_id:
        with CRM_SYNC_GUARD_LOCK:
            CRM_SYNC_LATEST_OBSERVED[sender_id] = observed_at
    background_tasks.add_task(sync_conversation_to_crm_safe, sync_payload, appointment_id, dict(request_metrics or {}))
    return True


def sync_conversation_to_crm_safe(
    conversation: dict[str, Any],
    appointment_id: int | None = None,
    request_metrics: dict[str, Any] | None = None,
) -> None:
    started_at = time_module.perf_counter()
    sender_id = str(conversation.get("instagram_user_id") or "")
    observed_at = str(conversation.get("_crm_observed_at") or "")
    if sender_id and observed_at:
        with CRM_SYNC_GUARD_LOCK:
            latest_observed_at = CRM_SYNC_LATEST_OBSERVED.get(sender_id)
        if latest_observed_at and observed_at < latest_observed_at:
            logger.info(
                "crm_sync_skipped_stale sender_id=%s observed_at=%s latest_observed_at=%s appointment_id=%s",
                sender_id,
                observed_at,
                latest_observed_at,
                appointment_id,
            )
            return
    try:
        success = sync_conversation_to_crm(conversation, appointment_id, request_metrics)
        logger.info(
            "crm_sync_finished sender_id=%s success=%s crm_sync_ms=%s appointment_id=%s",
            conversation.get("instagram_user_id"),
            success,
            elapsed_ms(started_at),
            appointment_id,
        )
    except Exception:  # noqa: BLE001
        logger.exception(
            "crm_sync_background_exception sender_id=%s appointment_id=%s",
            conversation.get("instagram_user_id"),
            appointment_id,
        )


def sync_conversation_to_crm(
    conversation: dict[str, Any],
    appointment_id: int | None = None,
    request_metrics: dict[str, Any] | None = None,
) -> bool:
    if not should_sync_crm_conversation(conversation):
        return False

    request_metrics = request_metrics or {}
    sync_started_at = time_module.perf_counter()
    headers, crm_user_id = crm_auth_session()
    if not headers or not crm_user_id:
        logger.warning("crm_auth_failed sender_id=%s", conversation.get("instagram_user_id"))
        return False

    try:
        state_response = requests.get(
            f"{CRM_SUPABASE_URL}/rest/v1/workspace_state?select=workspace_id,payload&workspace_id=eq.{CRM_WORKSPACE_ID}",
            headers=headers,
            timeout=CRM_SYNC_TIMEOUT_SECONDS,
        )
        state_response.raise_for_status()
        rows = state_response.json()
        payload = normalize_crm_payload(rows[0].get("payload") if rows else {})

        service_meta = map_crm_service_metadata(conversation.get("service"), payload)
        source_account = IG_LOGIN_USERNAME or "instagram"
        display_name = sanitize_text(
            conversation.get("full_name")
            or conversation.get("instagram_username")
            or f"Instagram Lead {str(conversation.get('instagram_user_id') or '')[-4:]}"
        )
        customer = find_crm_customer(payload, conversation)
        customer_id = customer.get("id") if customer else crm_generate_id("cust")
        customer_index = next((index for index, item in enumerate(payload["customers"]) if item.get("id") == customer_id), -1)
        observed_at = conversation.get("_crm_observed_at") or datetime.now(TZ).isoformat()
        now_iso = datetime.now(TZ).isoformat()
        appointment_record = None
        preconsultation_record = None
        preconsultation_index = -1
        task_record = None
        log_key = ""
        entity_type = "customer"
        entity_id = customer_id
        event_type = "lead_sync"

        customer_record = {
            **(customer or {}),
            "id": customer_id,
            "name": display_name if conversation.get("full_name") else (customer.get("name") if customer and customer.get("name") else display_name),
            "authorizedPerson": conversation.get("full_name") or (customer.get("authorizedPerson") if customer else display_name) or display_name,
            "phone": conversation.get("phone") or (customer.get("phone") if customer else ""),
            "email": customer.get("email") if customer else "",
            "address": customer.get("address") if customer else "",
            "service": service_meta["name"],
            "price": float(customer.get("price") or 0) if customer and customer.get("price") not in [None, ""] else service_meta["price"],
            "type": customer.get("type") if customer and customer.get("type") else service_meta["type"],
            "status": customer.get("status") if customer and customer.get("status") else "Lead",
            "affiliateId": customer.get("affiliateId") if customer else "",
            "contractDate": conversation.get("requested_date") or (customer.get("contractDate") if customer and customer.get("contractDate") else datetime.now(TZ).date().isoformat()),
            "lastRenewalMonth": customer.get("lastRenewalMonth") if customer else "",
            "notes": merge_note_text(customer.get("notes") if customer else "", build_crm_customer_note(conversation)),
            "instagramUserId": conversation.get("instagram_user_id"),
            "source": CRM_SYNC_SOURCE,
            "sourceAccount": source_account,
            "instagramAccountUsername": source_account,
            "instagramBusinessUserId": IG_BUSINESS_USER_ID,
            "aiModel": LLM_MODEL,
            "aiFallbackModel": LLM_FALLBACK_MODEL,
            "lastConversationAt": observed_at,
        }

        if customer_index >= 0:
            payload["customers"][customer_index] = customer_record
        else:
            payload["customers"].insert(0, customer_record)

        preconsultation_key = f"lead-{conversation.get('instagram_user_id') or customer_id}"
        preconsultation_index = next((index for index, item in enumerate(payload["preConsultations"]) if item.get("sourceLeadId") == preconsultation_key), -1)
        if preconsultation_index < 0:
            preconsultation_index = next(
                (
                    index
                    for index, item in enumerate(payload["preConsultations"])
                    if str(item.get("instagramUserId") or "") == str(conversation.get("instagram_user_id") or "")
                ),
                -1,
            )
        existing_preconsultation = payload["preConsultations"][preconsultation_index] if preconsultation_index >= 0 else {}
        preconsultation_record = {
            **existing_preconsultation,
            "id": existing_preconsultation.get("id") or crm_generate_id("pre"),
            "sourceLeadId": existing_preconsultation.get("sourceLeadId") or preconsultation_key,
            "customerId": customer_id,
            "customerName": display_name,
            "authorizedPerson": conversation.get("full_name") or display_name,
            "instagramUserId": conversation.get("instagram_user_id"),
            "phone": conversation.get("phone") or existing_preconsultation.get("phone") or "",
            "service": service_meta["name"],
            "requestedDate": conversation.get("requested_date") or existing_preconsultation.get("requestedDate") or "",
            "requestedTime": str(conversation.get("requested_time") or existing_preconsultation.get("requestedTime") or "")[:5],
            "status": derive_preconsultation_status(conversation),
            "source": CRM_SYNC_SOURCE,
            "sourceAccount": source_account,
            "instagramAccountUsername": source_account,
            "instagramBusinessUserId": IG_BUSINESS_USER_ID,
            "channel": "instagram",
            "notes": conversation.get("llm_notes") or existing_preconsultation.get("notes") or "",
            "aiModel": LLM_MODEL,
            "aiFallbackModel": LLM_FALLBACK_MODEL,
            "appointmentId": existing_preconsultation.get("appointmentId") or "",
            "appointmentSourceId": existing_preconsultation.get("appointmentSourceId") or "",
            "createdAt": existing_preconsultation.get("createdAt") or now_iso,
            "updatedAt": now_iso,
            "lastConversationAt": observed_at,
        }
        if preconsultation_index >= 0:
            payload["preConsultations"][preconsultation_index] = preconsultation_record
        else:
            payload["preConsultations"].insert(0, preconsultation_record)
            preconsultation_index = 0

        entity_type = "pre_consultation"
        entity_id = preconsultation_record["id"]
        event_type = "pre_consultation_sync"

        if conversation.get("appointment_status") == "confirmed" and conversation.get("requested_date") and conversation.get("requested_time"):
            requested_time = str(conversation.get("requested_time"))[:5]
            appointment_key = f"booking-{appointment_id}" if appointment_id is not None else f"{conversation.get('instagram_user_id')}-{conversation.get('requested_date')}-{requested_time}"
            appointment_index = next((index for index, item in enumerate(payload["appointments"]) if item.get("sourceAppointmentId") == appointment_key), -1)
            if appointment_index < 0:
                appointment_index = next(
                    (
                        index
                        for index, item in enumerate(payload["appointments"])
                        if str(item.get("instagramUserId") or "") == str(conversation.get("instagram_user_id") or "")
                        and item.get("appointmentDate") == conversation.get("requested_date")
                        and str(item.get("appointmentTime") or "")[:5] == requested_time
                    ),
                    -1,
                )
            was_existing_appointment = appointment_index >= 0
            existing_appointment = payload["appointments"][appointment_index] if appointment_index >= 0 else {}
            appointment_source_id = existing_appointment.get("sourceAppointmentId") or appointment_key
            appointment_record = {
                **existing_appointment,
                "id": existing_appointment.get("id") or crm_generate_id("apt"),
                "sourceAppointmentId": appointment_source_id,
                "customerId": customer_id,
                "customerName": display_name,
                "authorizedPerson": conversation.get("full_name") or display_name,
                "instagramUserId": conversation.get("instagram_user_id"),
                "phone": conversation.get("phone"),
                "service": service_meta["name"],
                "appointmentDate": conversation.get("requested_date"),
                "appointmentTime": requested_time,
                "status": "confirmed",
                "source": CRM_SYNC_SOURCE,
                "sourceAccount": source_account,
                "instagramAccountUsername": source_account,
                "instagramBusinessUserId": IG_BUSINESS_USER_ID,
                "channel": "instagram",
                "notes": conversation.get("llm_notes") or existing_appointment.get("notes") or "",
                "aiModel": LLM_MODEL,
                "aiFallbackModel": LLM_FALLBACK_MODEL,
                "createdAt": existing_appointment.get("createdAt") or now_iso,
                "updatedAt": now_iso,
                "lastConversationAt": observed_at,
            }
            if appointment_index >= 0:
                payload["appointments"][appointment_index] = appointment_record
            else:
                payload["appointments"].insert(0, appointment_record)

            if preconsultation_record:
                preconsultation_record = {
                    **preconsultation_record,
                    "service": service_meta["name"],
                    "requestedDate": conversation.get("requested_date") or preconsultation_record.get("requestedDate") or "",
                    "requestedTime": requested_time,
                    "status": "converted",
                    "appointmentId": appointment_record["id"],
                    "appointmentSourceId": appointment_source_id,
                    "updatedAt": now_iso,
                    "lastConversationAt": observed_at,
                }
                if preconsultation_index >= 0:
                    payload["preConsultations"][preconsultation_index] = preconsultation_record
                else:
                    payload["preConsultations"].insert(0, preconsultation_record)
                    preconsultation_index = 0

            task_key = f"task-{appointment_source_id}"
            task_index = next((index for index, item in enumerate(payload["tasks"]) if item.get("sourceAppointmentId") == appointment_source_id or item.get("id") == task_key), -1)
            existing_task = payload["tasks"][task_index] if task_index >= 0 else {}
            task_record = {
                **existing_task,
                "id": existing_task.get("id") or task_key,
                "sourceAppointmentId": appointment_source_id,
                "title": f"Instagram ön görüşme: {display_name}",
                "desc": f"{service_meta['name']} • {format_human_date(conversation['requested_date'])} {requested_time} • {conversation.get('phone') or '-'}",
                "status": existing_task.get("status") or "pending",
                "assignee": existing_task.get("assignee") or "DOEL",
                "deadline": conversation.get("requested_date"),
                "priority": existing_task.get("priority") or "Yüksek",
                "sourceAccount": source_account,
                "channel": "instagram",
                "aiModel": LLM_MODEL,
            }
            if task_index >= 0:
                payload["tasks"][task_index] = task_record
            else:
                payload["tasks"].insert(0, task_record)

            log_key = f"log-{appointment_source_id}"
            if not any(item.get("id") == log_key for item in payload["logs"]):
                payload["logs"].insert(0, {
                    "id": log_key,
                    "date": datetime.now(TZ).strftime("%d.%m.%Y %H:%M:%S"),
                    "action": "Instagram Randevu",
                    "desc": f"{display_name} için {service_meta['name']} görüşmesi onaylandı: {format_human_date(conversation['requested_date'])} {requested_time}",
                    "sourceAccount": source_account,
                    "aiModel": LLM_MODEL,
                })

            entity_type = "appointment"
            entity_id = appointment_record["id"]
            event_type = "appointment_followup_sync" if was_existing_appointment and appointment_id is None else "appointment_confirmed_sync"

        binding_key = IG_BUSINESS_USER_ID or source_account or CRM_SYNC_SOURCE
        bindings = payload["integrations"]["bindings"]
        binding_index = next(
            (
                index
                for index, item in enumerate(bindings)
                if item.get("bindingKey") == binding_key
                or (IG_BUSINESS_USER_ID and item.get("instagramBusinessUserId") == IG_BUSINESS_USER_ID)
                or (source_account and item.get("instagramUsername") == source_account)
            ),
            -1,
        )
        sync_duration_ms = elapsed_ms(sync_started_at)
        existing_binding = bindings[binding_index] if binding_index >= 0 else {}
        binding_record = {
            **existing_binding,
            "id": existing_binding.get("id") or crm_generate_id("binding"),
            "bindingKey": binding_key,
            "channel": "instagram",
            "instagramUsername": source_account,
            "instagramBusinessUserId": IG_BUSINESS_USER_ID,
            "aiModel": LLM_MODEL,
            "aiFallbackModel": LLM_FALLBACK_MODEL,
            "aiEndpoint": LLM_BASE_URL,
            "syncSource": CRM_SYNC_SOURCE,
            "workspaceId": CRM_WORKSPACE_ID,
            "syncEnabled": True,
            "lastSyncAt": now_iso,
            "lastSyncStatus": "success",
            "lastEntityType": entity_type,
            "lastEntityId": entity_id,
            "lastCustomerId": customer_id,
            "lastPreConsultationId": preconsultation_record.get("id") if preconsultation_record else existing_binding.get("lastPreConsultationId", ""),
            "lastAppointmentId": appointment_record.get("id") if appointment_record else existing_binding.get("lastAppointmentId", ""),
            "lastRequestTotalMs": int(request_metrics.get("total_ms") or 0),
            "lastExtractMs": int(request_metrics.get("extract_ms") or 0),
            "lastPolishMs": int(request_metrics.get("polish_ms") or 0),
            "lastSyncDurationMs": sync_duration_ms,
            "updatedAt": now_iso,
        }
        if binding_index >= 0:
            bindings[binding_index] = binding_record
        else:
            bindings.insert(0, binding_record)

        if event_type == "appointment_followup_sync":
            sync_summary = f"{source_account} hesabındaki {display_name} randevusu follow-up sonrası tekrar doğrulandı."
        elif event_type == "appointment_confirmed_sync":
            sync_summary = f"{source_account} hesabından {display_name} ön görüşmesi randevuya dönüştürülüp CRM'e işlendi."
        elif event_type == "pre_consultation_sync":
            sync_summary = f"{source_account} hesabından {display_name} ön görüşme kaydı CRM'e işlendi."
        else:
            sync_summary = f"{source_account} hesabından {display_name} kaydı {entity_type} olarak CRM'e işlendi."

        sync_event = {
            "id": f"sync-{uuid4().hex[:12]}",
            "createdAt": now_iso,
            "status": "success",
            "channel": "instagram",
            "sourceAccount": source_account,
            "instagramBusinessUserId": IG_BUSINESS_USER_ID,
            "aiModel": LLM_MODEL,
            "aiFallbackModel": LLM_FALLBACK_MODEL,
            "syncSource": CRM_SYNC_SOURCE,
            "workspaceId": CRM_WORKSPACE_ID,
            "eventType": event_type,
            "entityType": entity_type,
            "entityId": entity_id,
            "customerId": customer_id,
            "preConsultationId": preconsultation_record.get("id") if preconsultation_record else "",
            "appointmentId": appointment_record.get("id") if appointment_record else "",
            "taskId": task_record.get("id") if task_record else "",
            "logId": log_key,
            "conversationState": conversation.get("state"),
            "appointmentStatus": conversation.get("appointment_status"),
            "requestTotalMs": int(request_metrics.get("total_ms") or 0),
            "llmExtractMs": int(request_metrics.get("extract_ms") or 0),
            "llmPolishMs": int(request_metrics.get("polish_ms") or 0),
            "syncDurationMs": sync_duration_ms,
            "summary": sync_summary,
        }
        payload["syncEvents"] = [sync_event, *payload.get("syncEvents", [])][:CRM_SYNC_EVENT_LIMIT]

        patch_response = requests.patch(
            f"{CRM_SUPABASE_URL}/rest/v1/workspace_state?workspace_id=eq.{CRM_WORKSPACE_ID}",
            headers=headers,
            json={
                "payload": payload,
                "updated_by": crm_user_id,
                "client_updated_at": now_iso,
            },
            timeout=CRM_SYNC_TIMEOUT_SECONDS,
        )
        patch_response.raise_for_status()
        return True
    except Exception:  # noqa: BLE001
        logger.exception(
            "crm_sync_failed sender_id=%s appointment_id=%s",
            conversation.get("instagram_user_id"),
            appointment_id,
        )
        return False


def build_confirmation_message(conversation: dict[str, Any]) -> str:
    requested_date = date.fromisoformat(conversation["requested_date"]).strftime("%d.%m.%Y")
    requested_time = conversation["requested_time"][:5]
    contact_text = build_contact_text()
    booking_label = get_booking_label(conversation)
    contact_suffix = f" İhtiyacınız olursa {contact_text} üzerinden bize ulaşabilirsiniz." if contact_text else ""
    return (
        f"{booking_label.capitalize()} kaydınız oluşturuldu.\n\n"
        f"Ad Soyad: {conversation['full_name']}\n"
        f"Hizmet: {conversation['service']}\n"
        f"Tarih: {requested_date}\n"
        f"Saat: {requested_time}\n"
        f"Telefon: {conversation['phone']}\n\n"
        f"Lütfen görüşme günü ve saati için müsaitliğinizi ayarlayın."
        f" Ekibimiz görüşme öncesinde gerekli olursa sizinle iletişime geçecektir.{contact_suffix}"
    )


def build_normalized(conversation: dict[str, Any]) -> dict[str, Any]:
    memory = ensure_conversation_memory(conversation)
    if not memory.get("conversation_summary"):
        memory["conversation_summary"] = build_conversation_memory_summary(conversation)
    return {
        "instagram_user_id": conversation.get("instagram_user_id"),
        "full_name": conversation.get("full_name"),
        "phone": conversation.get("phone"),
        "service": conversation.get("service"),
        "requested_date": conversation.get("requested_date"),
        "requested_time": conversation.get("requested_time"),
        "state": conversation.get("state"),
        "appointment_status": conversation.get("appointment_status"),
        "booking_kind": conversation.get("booking_kind"),
        "preferred_period": conversation.get("preferred_period"),
        "memory_state": memory,
    }
