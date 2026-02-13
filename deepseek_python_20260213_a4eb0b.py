import asyncio
import logging
import threading
import signal
import re
import uuid
import json
import base64
import hashlib
import secrets
import string
import os
import time
import shutil
import zipfile
import sqlite3
import html as html_escape
from datetime import datetime, timedelta
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlparse, urlencode
from typing import Callable, Dict, Any, Awaitable, Optional, List, Tuple
from functools import wraps
from pathlib import Path
from hmac import compare_digest
from io import BytesIO

import aiohttp
import paramiko
import qrcode
from yookassa import Configuration, Payment
from aiogram import Bot, Dispatcher, Router, F, types, html, BaseMiddleware
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode, ChatMemberStatus
from aiogram.filters import Command, CommandObject, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import BufferedInputFile, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiosend import CryptoPay
from pytonconnect import TonConnect
from pytonconnect.exceptions import UserRejectsError

try:
    import colorama
    colorama_available = True
except Exception:
    colorama_available = False

logger = logging.getLogger(__name__)

# ==================== DATABASE MANAGER ====================
PROJECT_ROOT = Path("/app/project")
DB_FILE = PROJECT_ROOT / "users.db"

def normalize_host_name(name: str | None) -> str:
    s = (name or "").strip()
    for ch in ("\u00A0", "\u200B", "\u200C", "\u200D", "\uFEFF"):
        s = s.replace(ch, "")
    return s

def initialize_db():
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    telegram_id INTEGER PRIMARY KEY, username TEXT, total_spent REAL DEFAULT 0,
                    total_months INTEGER DEFAULT 0, trial_used BOOLEAN DEFAULT 0,
                    agreed_to_terms BOOLEAN DEFAULT 0,
                    registration_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_banned BOOLEAN DEFAULT 0,
                    balance REAL DEFAULT 0,
                    referred_by INTEGER,
                    referral_balance REAL DEFAULT 0,
                    referral_balance_all REAL DEFAULT 0,
                    referral_start_bonus_received BOOLEAN DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS vpn_keys (
                    key_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    host_name TEXT NOT NULL,
                    xui_client_uuid TEXT NOT NULL,
                    key_email TEXT NOT NULL UNIQUE,
                    expiry_date TIMESTAMP,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    comment TEXT,
                    is_gift BOOLEAN DEFAULT 0
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS promo_codes (
                    promo_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    discount_percent REAL,
                    discount_amount REAL,
                    months_bonus INTEGER,
                    max_uses INTEGER,
                    used_count INTEGER DEFAULT 0,
                    active INTEGER NOT NULL DEFAULT 1,
                    valid_from TIMESTAMP,
                    valid_to TIMESTAMP,
                    comment TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usage_limit_total INTEGER,
                    usage_limit_per_user INTEGER,
                    used_total INTEGER DEFAULT 0,
                    is_active INTEGER DEFAULT 1,
                    description TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS bot_settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS xui_hosts(
                    host_name TEXT NOT NULL,
                    host_url TEXT NOT NULL,
                    host_username TEXT NOT NULL,
                    host_pass TEXT NOT NULL,
                    host_inbound_id INTEGER NOT NULL,
                    subscription_url TEXT,
                    ssh_host TEXT,
                    ssh_port INTEGER,
                    ssh_user TEXT,
                    ssh_password TEXT,
                    ssh_key_path TEXT
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS plans (
                    plan_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    plan_name TEXT NOT NULL,
                    months INTEGER NOT NULL,
                    price REAL NOT NULL,
                    FOREIGN KEY (host_name) REFERENCES xui_hosts (host_name)
                )
            ''')            
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS transactions (
                    username TEXT,
                    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    payment_id TEXT UNIQUE NOT NULL,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    amount_rub REAL NOT NULL,
                    amount_currency REAL,
                    currency_name TEXT,
                    payment_method TEXT,
                    metadata TEXT,
                    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_tickets (
                    ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'open',
                    subject TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    forum_chat_id TEXT,
                    message_thread_id INTEGER
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS support_messages (
                    message_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticket_id INTEGER NOT NULL,
                    sender TEXT NOT NULL,
                    content TEXT NOT NULL,
                    media TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (ticket_id) REFERENCES support_tickets (ticket_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS host_speedtests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    method TEXT NOT NULL,
                    ping_ms REAL,
                    jitter_ms REAL,
                    download_mbps REAL,
                    upload_mbps REAL,
                    server_name TEXT,
                    server_id TEXT,
                    ok INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS host_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    host_name TEXT NOT NULL,
                    cpu_percent REAL,
                    mem_percent REAL,
                    mem_used INTEGER,
                    mem_total INTEGER,
                    disk_percent REAL,
                    disk_used INTEGER,
                    disk_total INTEGER,
                    load1 REAL,
                    load5 REAL,
                    load15 REAL,
                    uptime_seconds REAL,
                    ok INTEGER NOT NULL DEFAULT 1,
                    error TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS resource_metrics (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    scope TEXT NOT NULL,
                    object_name TEXT NOT NULL,
                    cpu_percent REAL,
                    mem_percent REAL,
                    disk_percent REAL,
                    load1 REAL,
                    net_bytes_sent INTEGER,
                    net_bytes_recv INTEGER,
                    raw_json TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS button_configs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    menu_type TEXT NOT NULL DEFAULT 'main_menu',
                    button_id TEXT NOT NULL,
                    text TEXT NOT NULL,
                    callback_data TEXT,
                    url TEXT,
                    row_position INTEGER DEFAULT 0,
                    column_position INTEGER DEFAULT 0,
                    button_width INTEGER DEFAULT 1,
                    sort_order INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(menu_type, button_id)
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS promo_code_usages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL,
                    user_id INTEGER NOT NULL,
                    applied_amount REAL NOT NULL,
                    order_id TEXT,
                    used_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_reg_date ON users(registration_date)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_host_speedtests_host_time ON host_speedtests(host_name, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_host_metrics_host_time ON host_metrics(host_name, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_resource_metrics_scope_time ON resource_metrics(scope, object_name, created_at DESC)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_button_configs_menu_type ON button_configs(menu_type, sort_order)")

            default_settings = {
                "panel_login": "admin", "panel_password": "admin", "about_text": None,
                "terms_url": None, "privacy_url": None, "support_user": None,
                "support_text": None, "main_menu_text": None,
                "howto_android_text": None, "howto_ios_text": None,
                "howto_windows_text": None, "howto_linux_text": None,
                "btn_try": "🎁 Попробовать бесплатно", "btn_profile": "👤 Мой профиль",
                "btn_my_keys": "🔑 Мои ключи ({count})", "btn_buy_key": "💳 Купить ключ",
                "btn_top_up": "➕ Пополнить баланс", "btn_referral": "🤝 Реферальная программа",
                "btn_support": "🆘 Поддержка", "btn_about": "ℹ️ О проекте",
                "btn_howto": "❓ Как использовать", "btn_speed": "⚡ Тест скорости",
                "btn_admin": "⚙️ Админка", "btn_back_to_menu": "⬅️ Назад в меню",
                "btn_back": "⬅️ Назад", "btn_back_to_plans": "⬅️ Назад к тарифам",
                "btn_back_to_key": "⬅️ Назад к ключу", "btn_back_to_keys": "⬅️ Назад к списку ключей",
                "btn_extend_key": "➕ Продлить этот ключ", "btn_show_qr": "📱 Показать QR-код",
                "btn_instruction": "📖 Инструкция", "btn_switch_server": "🌍 Сменить сервер",
                "btn_skip_email": "➡️ Продолжить без почты", "btn_go_to_payment": "Перейти к оплате",
                "btn_check_payment": "✅ Проверить оплату", "btn_pay_with_balance": "💼 Оплатить с баланса",
                "btn_channel": "📰 Наш канал", "btn_terms": "📄 Условия использования",
                "btn_privacy": "🔒 Политика конфиденциальности", "btn_howto_android": "📱 Android",
                "btn_howto_ios": "📱 iOS", "btn_howto_windows": "💻 Windows",
                "btn_howto_linux": "🐧 Linux", "btn_support_open": "🆘 Открыть поддержку",
                "btn_support_new_ticket": "✍️ Новое обращение", "btn_support_my_tickets": "📨 Мои обращения",
                "btn_support_external": "🆘 Внешняя поддержка", "channel_url": None,
                "force_subscription": "true", "receipt_email": "example@example.com",
                "telegram_bot_token": None, "telegram_bot_username": None,
                "trial_enabled": "true", "trial_duration_days": "3",
                "enable_referrals": "true", "referral_percentage": "10",
                "referral_discount": "5", "minimum_withdrawal": "100",
                "admin_telegram_id": None, "admin_telegram_ids": None,
                "yookassa_shop_id": None, "yookassa_secret_key": None,
                "sbp_enabled": "false", "cryptobot_token": None,
                "heleket_merchant_id": None, "heleket_api_key": None,
                "domain": None, "ton_wallet_address": None, "tonapi_key": None,
                "support_forum_chat_id": None, "enable_fixed_referral_bonus": "false",
                "fixed_referral_bonus_amount": "50", "referral_reward_type": "percent_purchase",
                "referral_on_start_referrer_amount": "20", "backup_interval_days": "1",
                "stars_enabled": "false", "stars_per_rub": "1", "stars_title": "VPN подписка",
                "stars_description": "Оплата в Telegram Stars", "yoomoney_enabled": "false",
                "yoomoney_wallet": None, "yoomoney_api_token": None, "yoomoney_client_id": None,
                "yoomoney_client_secret": None, "yoomoney_redirect_uri": None,
                "panel_brand_title": "T‑Shift VPN",
            }
            for key, value in default_settings.items():
                cursor.execute("INSERT OR IGNORE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
            
            cursor.execute("SELECT COUNT(*) FROM button_configs")
            if cursor.fetchone()[0] == 0:
                migrate_existing_buttons()
                cleanup_duplicate_buttons()
            
            cursor.execute("PRAGMA table_info(vpn_keys)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'created_date' not in columns:
                cursor.execute("ALTER TABLE vpn_keys ADD COLUMN created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
            if 'xui_client_uuid' not in columns:
                cursor.execute("ALTER TABLE vpn_keys ADD COLUMN xui_client_uuid TEXT")
            if 'comment' not in columns:
                cursor.execute("ALTER TABLE vpn_keys ADD COLUMN comment TEXT")
            if 'is_gift' not in columns:
                cursor.execute("ALTER TABLE vpn_keys ADD COLUMN is_gift BOOLEAN DEFAULT 0")
            conn.commit()
            
            cursor.execute("PRAGMA table_info(users)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'referred_by' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN referred_by INTEGER")
            if 'balance' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN balance REAL DEFAULT 0")
            if 'referral_balance' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN referral_balance REAL DEFAULT 0")
            if 'referral_balance_all' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN referral_balance_all REAL DEFAULT 0")
            if 'referral_start_bonus_received' not in columns:
                cursor.execute("ALTER TABLE users ADD COLUMN referral_start_bonus_received BOOLEAN DEFAULT 0")
            conn.commit()
            
            cursor.execute("PRAGMA table_info(support_tickets)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'forum_chat_id' not in columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN forum_chat_id TEXT")
            if 'message_thread_id' not in columns:
                cursor.execute("ALTER TABLE support_tickets ADD COLUMN message_thread_id INTEGER")
            conn.commit()
            
            cursor.execute("PRAGMA table_info(xui_hosts)")
            columns = [row[1] for row in cursor.fetchall()]
            for col in ['subscription_url', 'ssh_host', 'ssh_port', 'ssh_user', 'ssh_password', 'ssh_key_path']:
                if col not in columns:
                    cursor.execute(f"ALTER TABLE xui_hosts ADD COLUMN {col} TEXT")
            conn.commit()
            
            cursor.execute("PRAGMA table_info(promo_codes)")
            columns = [row[1] for row in cursor.fetchall()]
            for col in ['usage_limit_total', 'usage_limit_per_user', 'used_total', 'is_active', 'description']:
                if col not in columns:
                    cursor.execute(f"ALTER TABLE promo_codes ADD COLUMN {col} INTEGER")
            conn.commit()
            
            logging.info("База данных успешно инициализирована.")
    except sqlite3.Error as e:
        logging.error(f"Ошибка базы данных при инициализации: {e}")

def get_all_settings() -> dict:
    settings = {}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT key, value FROM bot_settings")
            for row in cursor.fetchall():
                settings[row['key']] = row['value']
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить все настройки: {e}")
    return settings

def get_setting(key: str) -> str | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT value FROM bot_settings WHERE key = ?", (key,))
            result = cursor.fetchone()
            return result[0] if result else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить настройку '{key}': {e}")
        return None

def update_setting(key: str, value: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT OR REPLACE INTO bot_settings (key, value) VALUES (?, ?)", (key, value))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить настройку '{key}': {e}")

def get_admin_ids() -> set[int]:
    ids = set()
    try:
        single = get_setting("admin_telegram_id")
        if single:
            try: ids.add(int(single))
            except: pass
        multi_raw = get_setting("admin_telegram_ids")
        if multi_raw:
            s = multi_raw.strip()
            try:
                arr = json.loads(s)
                if isinstance(arr, list):
                    for v in arr:
                        try: ids.add(int(v))
                        except: pass
                    return ids
            except: pass
            parts = [p for p in re.split(r"[\s,]+", s) if p]
            for p in parts:
                try: ids.add(int(p))
                except: pass
    except Exception as e:
        logging.warning(f"Не удалось получить ID администраторов: {e}")
    return ids

def is_admin(user_id: int) -> bool:
    try:
        return int(user_id) in get_admin_ids()
    except Exception:
        return False

def get_all_hosts() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts")
            hosts = cursor.fetchall()
            result = []
            for row in hosts:
                d = dict(row)
                d['host_name'] = normalize_host_name(d.get('host_name'))
                result.append(d)
            return result
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения списка всех хостов: {e}")
        return []

def get_host(host_name: str) -> dict | None:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            result = cursor.fetchone()
            return dict(result) if result else None
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения хоста '{host_name}': {e}")
        return None

def create_host(name: str, url: str, user: str, passwd: str, inbound: int, subscription_url: str | None = None):
    try:
        name = normalize_host_name(name)
        url = url.strip()
        user = user.strip()
        passwd = passwd or ""
        try: inbound = int(inbound)
        except: pass
        subscription_url = subscription_url or None
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO xui_hosts (host_name, host_url, host_username, host_pass, host_inbound_id, subscription_url) VALUES (?, ?, ?, ?, ?, ?)",
                (name, url, user, passwd, inbound, subscription_url)
            )
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Ошибка при создании хоста '{name}': {e}")

def delete_host(host_name: str):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            cursor.execute("DELETE FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Ошибка удаления хоста '{host_name}': {e}")

def update_host_subscription_url(host_name: str, subscription_url: str | None) -> bool:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            if not cursor.fetchone(): return False
            cursor.execute("UPDATE xui_hosts SET subscription_url = ? WHERE TRIM(host_name) = TRIM(?)", (subscription_url, host_name))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить subscription_url для хоста '{host_name}': {e}")
        return False

def update_host_url(host_name: str, new_url: str) -> bool:
    try:
        host_name = normalize_host_name(host_name)
        new_url = new_url.strip()
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            if not cursor.fetchone(): return False
            cursor.execute("UPDATE xui_hosts SET host_url = ? WHERE TRIM(host_name) = TRIM(?)", (new_url, host_name))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить host_url для хоста '{host_name}': {e}")
        return False

def update_host_name(old_name: str, new_name: str) -> bool:
    try:
        old_name_n = normalize_host_name(old_name)
        new_name_n = normalize_host_name(new_name)
        if not new_name_n: return False
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (old_name_n,))
            if not cursor.fetchone(): return False
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (new_name_n,))
            if cursor.fetchone() and old_name_n.lower() != new_name_n.lower(): return False
            cursor.execute("UPDATE xui_hosts SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)", (new_name_n, old_name_n))
            cursor.execute("UPDATE plans SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)", (new_name_n, old_name_n))
            cursor.execute("UPDATE vpn_keys SET host_name = TRIM(?) WHERE TRIM(host_name) = TRIM(?)", (new_name_n, old_name_n))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось переименовать хост с '{old_name}' на '{new_name}': {e}")
        return False

def update_host_ssh_settings(host_name: str, ssh_host: str | None = None, ssh_port: int | None = None,
                           ssh_user: str | None = None, ssh_password: str | None = None,
                           ssh_key_path: str | None = None) -> bool:
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1 FROM xui_hosts WHERE TRIM(host_name) = TRIM(?)", (host_name_n,))
            if not cursor.fetchone(): return False
            cursor.execute(
                "UPDATE xui_hosts SET ssh_host = ?, ssh_port = ?, ssh_user = ?, ssh_password = ?, ssh_key_path = ? WHERE TRIM(host_name) = TRIM(?)",
                (ssh_host or None, ssh_port or None, ssh_user or None, ssh_password if ssh_password is not None else None, ssh_key_path or None, host_name_n)
            )
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить SSH-настройки для хоста '{host_name}': {e}")
        return False

def get_plans_for_host(host_name: str) -> list[dict]:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE TRIM(host_name) = TRIM(?) ORDER BY months", (host_name,))
            return [dict(plan) for plan in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить планы для хоста '{host_name}': {e}")
        return []

def get_plan_by_id(plan_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM plans WHERE plan_id = ?", (plan_id,))
            plan = cursor.fetchone()
            return dict(plan) if plan else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить план по id '{plan_id}': {e}")
        return None

def create_plan(host_name: str, plan_name: str, months: int, price: float):
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO plans (host_name, plan_name, months, price) VALUES (?, ?, ?, ?)",
                          (host_name, plan_name, months, price))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось создать план для хоста '{host_name}': {e}")

def delete_plan(plan_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM plans WHERE plan_id = ?", (plan_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось удалить план с id {plan_id}: {e}")

def update_plan(plan_id: int, plan_name: str, months: int, price: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE plans SET plan_name = ?, months = ?, price = ? WHERE plan_id = ?",
                          (plan_name, months, price, plan_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить план {plan_id}: {e}")
        return False

def get_user(telegram_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users WHERE telegram_id = ?", (telegram_id,))
            user_data = cursor.fetchone()
            return dict(user_data) if user_data else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось get user {telegram_id}: {e}")
        return None

def get_all_users() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM users ORDER BY registration_date DESC")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get all users: {e}")
        return []

def get_users_paginated(page: int = 1, per_page: int = 20, q: str | None = None) -> tuple[list[dict], int]:
    try:
        page = max(1, int(page or 1))
        per_page = max(1, min(100, int(per_page or 20)))
    except Exception:
        page, per_page = 1, 20
    offset = (page - 1) * per_page
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if q:
                q = q.strip()
                like = f"%{q}%"
                cursor.execute("SELECT COUNT(*) FROM users WHERE CAST(telegram_id AS TEXT) LIKE ? OR username LIKE ? COLLATE NOCASE", (like, like))
                total = cursor.fetchone()[0] or 0
                cursor.execute("SELECT * FROM users WHERE CAST(telegram_id AS TEXT) LIKE ? OR username LIKE ? COLLATE NOCASE ORDER BY datetime(registration_date) DESC LIMIT ? OFFSET ?",
                              (like, like, per_page, offset))
            else:
                cursor.execute("SELECT COUNT(*) FROM users")
                total = cursor.fetchone()[0] or 0
                cursor.execute("SELECT * FROM users ORDER BY datetime(registration_date) DESC LIMIT ? OFFSET ?", (per_page, offset))
            return [dict(r) for r in cursor.fetchall()], total
    except sqlite3.Error as e:
        logging.error(f"Не удалось get paginated users: {e}")
        return [], 0

def register_user_if_not_exists(telegram_id: int, username: str, referrer_id: int | None = None):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referred_by FROM users WHERE telegram_id = ?", (telegram_id,))
            row = cursor.fetchone()
            if not row:
                cursor.execute("INSERT INTO users (telegram_id, username, registration_date, referred_by) VALUES (?, ?, ?, ?)",
                              (telegram_id, username, datetime.now(), referrer_id))
            else:
                cursor.execute("UPDATE users SET username = ? WHERE telegram_id = ?", (username, telegram_id))
                current_ref = row[0]
                if referrer_id and (current_ref is None or str(current_ref).strip() == "") and int(referrer_id) != int(telegram_id):
                    cursor.execute("UPDATE users SET referred_by = ? WHERE telegram_id = ?", (int(referrer_id), telegram_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось зарегистрировать пользователя {telegram_id}: {e}")

def set_terms_agreed(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET agreed_to_terms = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось set terms agreed for user {telegram_id}: {e}")

def get_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Не удалось get balance for user {user_id}: {e}")
        return 0.0

def add_to_balance(user_id: int, amount: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = balance + ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось add to balance for user {user_id}: {e}")
        return False

def deduct_from_balance(user_id: int, amount: float) -> bool:
    if amount <= 0: return True
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            cursor.execute("SELECT balance FROM users WHERE telegram_id = ?", (user_id,))
            row = cursor.fetchone()
            current = row[0] if row else 0.0
            if current < amount:
                conn.rollback()
                return False
            cursor.execute("UPDATE users SET balance = balance - ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось deduct from balance for user {user_id}: {e}")
        return False

def adjust_user_balance(user_id: int, delta: float) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET balance = COALESCE(balance, 0) + ? WHERE telegram_id = ?", (float(delta), user_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось adjust balance for user {user_id}: {e}")
        return False

def get_referral_balance(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить реферальный баланс для пользователя {user_id}: {e}")
        return 0.0

def get_referral_balance_all(user_id: int) -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT referral_balance_all FROM users WHERE telegram_id = ?", (user_id,))
            result = cursor.fetchone()
            return result[0] if result else 0.0
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить общий реферальный баланс для пользователя {user_id}: {e}")
        return 0.0

def add_to_referral_balance_all(user_id: int, amount: float):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_balance_all = referral_balance_all + ? WHERE telegram_id = ?", (amount, user_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось добавить к общему реферальному балансу для пользователя {user_id}: {e}")

def set_referral_start_bonus_received(user_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET referral_start_bonus_received = 1 WHERE telegram_id = ?", (user_id,))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось пометить получение стартового реферального бонуса для пользователя {user_id}: {e}")
        return False

def get_referral_count(user_id: int) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users WHERE referred_by = ?", (user_id,))
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось get referral count for user {user_id}: {e}")
        return 0

def get_referrals_for_user(user_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT telegram_id, username, registration_date, total_spent FROM users WHERE referred_by = ? ORDER BY registration_date DESC", (user_id,))
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить рефералов для пользователя {user_id}: {e}")
        return []

def ban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось ban user {telegram_id}: {e}")

def unban_user(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось unban user {telegram_id}: {e}")

def update_user_stats(telegram_id: int, amount_spent: float, months_purchased: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET total_spent = total_spent + ?, total_months = total_months + ? WHERE telegram_id = ?",
                          (amount_spent, months_purchased, telegram_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось update user stats for {telegram_id}: {e}")

def set_trial_used(telegram_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE users SET trial_used = 1 WHERE telegram_id = ?", (telegram_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось отметить пробный период как использованный для пользователя {telegram_id}: {e}")

def get_user_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось get user count: {e}")
        return 0

def get_total_keys_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось get total keys count: {e}")
        return 0

def get_total_spent_sum() -> float:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COALESCE(SUM(amount_rub), 0.0) FROM transactions WHERE LOWER(COALESCE(status, '')) IN ('paid', 'completed', 'success') AND LOWER(COALESCE(payment_method, '')) <> 'balance'")
            val = cursor.fetchone()
            return val[0] if val else 0.0
    except sqlite3.Error as e:
        logging.error(f"Не удалось get total spent sum: {e}")
        return 0.0

def get_all_keys() -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить все ключи: {e}")
        return []

def get_user_keys(user_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE user_id = ? ORDER BY created_date DESC", (user_id,))
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get keys for user {user_id}: {e}")
        return []

def get_keys_for_host(host_name: str) -> list[dict]:
    try:
        host_name = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE TRIM(host_name) = TRIM(?)", (host_name,))
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get keys for host '{host_name}': {e}")
        return []

def get_key_by_id(key_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_id = ?", (key_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить ключ по id {key_id}: {e}")
        return None

def get_key_by_email(key_email: str) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM vpn_keys WHERE key_email = ?", (key_email,))
            key_data = cursor.fetchone()
            return dict(key_data) if key_data else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось get key by email {key_email}: {e}")
        return None

def add_new_key(user_id: int, host_name: str, xui_client_uuid: str, key_email: str, expiry_timestamp_ms: int) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(expiry_timestamp_ms / 1000)
            cursor.execute("INSERT INTO vpn_keys (user_id, host_name, xui_client_uuid, key_email, expiry_date) VALUES (?, ?, ?, ?, ?)",
                          (user_id, host_name, xui_client_uuid, key_email, expiry_date))
            new_key_id = cursor.lastrowid
            conn.commit()
            return new_key_id
    except sqlite3.Error as e:
        logging.error(f"Не удалось add new key for user {user_id}: {e}")
        return None

def create_gift_key(user_id: int, host_name: str, key_email: str, months: int, xui_client_uuid: str | None = None) -> int | None:
    try:
        host_name = normalize_host_name(host_name)
        from datetime import timedelta
        expiry = datetime.now() + timedelta(days=30 * int(months or 1))
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO vpn_keys (user_id, host_name, xui_client_uuid, key_email, expiry_date, is_gift) VALUES (?, ?, ?, ?, ?, 1)",
                          (user_id, host_name, xui_client_uuid or f"GIFT-{user_id}-{int(datetime.now().timestamp())}", key_email, expiry.isoformat()))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.IntegrityError as e:
        logging.error(f"Не удалось создать подарочный ключ для пользователя {user_id}: дублирующийся email {key_email}: {e}")
        return None
    except sqlite3.Error as e:
        logging.error(f"Не удалось создать подарочный ключ для пользователя {user_id}: {e}")
        return None

def delete_key_by_id(key_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_id = ?", (key_id,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось удалить ключ по id {key_id}: {e}")
        return False

def delete_key_by_email(email: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE key_email = ?", (email,))
            affected = cursor.rowcount
            conn.commit()
            return affected > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось delete key '{email}': {e}")
        return False

def delete_user_keys(user_id: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM vpn_keys WHERE user_id = ?", (user_id,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось delete keys for user {user_id}: {e}")

def update_key_info(key_id: int, new_xui_uuid: str, new_expiry_ms: int):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(new_expiry_ms / 1000)
            cursor.execute("UPDATE vpn_keys SET xui_client_uuid = ?, expiry_date = ? WHERE key_id = ?", (new_xui_uuid, expiry_date, key_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось update key {key_id}: {e}")

def update_key_host_and_info(key_id: int, new_host_name: str, new_xui_uuid: str, new_expiry_ms: int):
    try:
        new_host_name = normalize_host_name(new_host_name)
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            expiry_date = datetime.fromtimestamp(new_expiry_ms / 1000)
            cursor.execute("UPDATE vpn_keys SET host_name = ?, xui_client_uuid = ?, expiry_date = ? WHERE key_id = ?",
                          (new_host_name, new_xui_uuid, expiry_date, key_id))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось update key {key_id} host and info: {e}")

def update_key_comment(key_id: int, comment: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET comment = ? WHERE key_id = ?", (comment, key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить комментарий ключа для {key_id}: {e}")
        return False

def update_key_email(key_id: int, new_email: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET key_email = ? WHERE key_id = ?", (new_email, key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.IntegrityError as e:
        logging.error(f"Нарушение уникальности email для ключа {key_id}: {e}")
        return False
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить email ключа для {key_id}: {e}")
        return False

def update_key_host(key_id: int, new_host_name: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE vpn_keys SET host_name = ? WHERE key_id = ?", (normalize_host_name(new_host_name), key_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось обновить хост ключа для {key_id}: {e}")
        return False

def update_key_status_from_server(key_email: str, xui_client_data):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            if xui_client_data:
                expiry_date = datetime.fromtimestamp(xui_client_data.expiry_time / 1000)
                cursor.execute("UPDATE vpn_keys SET xui_client_uuid = ?, expiry_date = ? WHERE key_email = ?", (xui_client_data.id, expiry_date, key_email))
            else:
                cursor.execute("DELETE FROM vpn_keys WHERE key_email = ?", (key_email,))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось update key status for {key_email}: {e}")

def get_next_key_number(user_id: int) -> int:
    keys = get_user_keys(user_id)
    return len(keys) + 1

def create_pending_transaction(payment_id: str, user_id: int, amount_rub: float, metadata: dict) -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO transactions (payment_id, user_id, status, amount_rub, metadata) VALUES (?, ?, ?, ?, ?)",
                          (payment_id, user_id, 'pending', amount_rub, json.dumps(metadata)))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Не удалось create pending transaction: {e}")
        return 0

def find_and_complete_pending_transaction(payment_id: str, amount_rub: float | None = None, payment_method: str | None = None,
                                         currency_name: str | None = None, amount_currency: float | None = None) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,))
            transaction = cursor.fetchone()
            if not transaction:
                logger.warning(f"Ожидающая транзакция не найдена для payment_id={payment_id}")
                return None
            cursor.execute("UPDATE transactions SET status = 'paid', amount_rub = COALESCE(?, amount_rub), amount_currency = COALESCE(?, amount_currency), currency_name = COALESCE(?, currency_name), payment_method = COALESCE(?, payment_method) WHERE payment_id = ?",
                          (amount_rub, amount_currency, currency_name, payment_method, payment_id))
            conn.commit()
            try:
                raw_md = transaction['metadata']
                md = json.loads(raw_md) if raw_md else {}
            except Exception:
                md = {}
            return md
    except sqlite3.Error as e:
        logging.error(f"Не удалось завершить ожидающую транзакцию {payment_id}: {e}")
        return None

def find_and_complete_ton_transaction(payment_id: str, amount_ton: float) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM transactions WHERE payment_id = ? AND status = 'pending'", (payment_id,))
            transaction = cursor.fetchone()
            if not transaction:
                logger.warning(f"TON Webhook: Получен платеж для неизвестного или завершенного payment_id: {payment_id}")
                return None
            cursor.execute("UPDATE transactions SET status = 'paid', amount_currency = ?, currency_name = 'TON', payment_method = 'TON' WHERE payment_id = ?", (amount_ton, payment_id))
            conn.commit()
            return json.loads(transaction['metadata'])
    except sqlite3.Error as e:
        logging.error(f"Не удалось complete TON transaction {payment_id}: {e}")
        return None

def log_transaction(username: str, transaction_id: str | None, payment_id: str | None, user_id: int, status: str,
                   amount_rub: float, amount_currency: float | None, currency_name: str | None, payment_method: str, metadata: str):
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO transactions (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, created_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (username, transaction_id, payment_id, user_id, status, amount_rub, amount_currency, currency_name, payment_method, metadata, datetime.now()))
            conn.commit()
    except sqlite3.Error as e:
        logging.error(f"Не удалось log transaction for user {user_id}: {e}")

def get_paginated_transactions(page: int = 1, per_page: int = 15) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    transactions = []
    total = 0
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM transactions")
            total = cursor.fetchone()[0]
            cursor.execute("SELECT * FROM transactions ORDER BY created_date DESC LIMIT ? OFFSET ?", (per_page, offset))
            for row in cursor.fetchall():
                transaction_dict = dict(row)
                metadata_str = transaction_dict.get('metadata')
                if metadata_str:
                    try:
                        metadata = json.loads(metadata_str)
                        transaction_dict['host_name'] = metadata.get('host_name', 'N/A')
                        transaction_dict['plan_name'] = metadata.get('plan_name', 'N/A')
                    except json.JSONDecodeError:
                        transaction_dict['host_name'] = 'Ошибка'
                        transaction_dict['plan_name'] = 'Ошибка'
                else:
                    transaction_dict['host_name'] = 'N/A'
                    transaction_dict['plan_name'] = 'N/A'
                transactions.append(transaction_dict)
    except sqlite3.Error as e:
        logging.error(f"Не удалось get paginated transactions: {e}")
    return transactions, total

def get_daily_stats_for_charts(days: int = 30) -> dict:
    stats = {'users': {}, 'keys': {}}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT date(registration_date) as day, COUNT(*) FROM users WHERE registration_date >= date('now', ?) GROUP BY day ORDER BY day", (f'-{days} days',))
            for row in cursor.fetchall():
                stats['users'][row[0]] = row[1]
            cursor.execute("SELECT date(created_date) as day, COUNT(*) FROM vpn_keys WHERE created_date >= date('now', ?) GROUP BY day ORDER BY day", (f'-{days} days',))
            for row in cursor.fetchall():
                stats['keys'][row[0]] = row[1]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get daily stats for charts: {e}")
    return stats

def get_admin_stats() -> dict:
    stats = {"total_users": 0, "total_keys": 0, "active_keys": 0, "total_income": 0.0,
             "today_new_users": 0, "today_income": 0.0, "today_issued_keys": 0}
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM users")
            row = cursor.fetchone()
            stats["total_users"] = (row[0] or 0) if row else 0
            cursor.execute("SELECT COUNT(*) FROM vpn_keys")
            row = cursor.fetchone()
            stats["total_keys"] = (row[0] or 0) if row else 0
            cursor.execute("SELECT COUNT(*) FROM vpn_keys WHERE expiry_date > CURRENT_TIMESTAMP")
            row = cursor.fetchone()
            stats["active_keys"] = (row[0] or 0) if row else 0
            cursor.execute("SELECT COALESCE(SUM(amount_rub), 0) FROM transactions WHERE status IN ('paid','success','succeeded') AND LOWER(COALESCE(payment_method, '')) <> 'balance'")
            row = cursor.fetchone()
            stats["total_income"] = float(row[0] or 0.0) if row else 0.0
            cursor.execute("SELECT COUNT(*) FROM users WHERE date(registration_date) = date('now')")
            row = cursor.fetchone()
            stats["today_new_users"] = (row[0] or 0) if row else 0
            cursor.execute("SELECT COALESCE(SUM(amount_rub), 0) FROM transactions WHERE status IN ('paid','success','succeeded') AND LOWER(COALESCE(payment_method, '')) <> 'balance' AND date(created_date) = date('now')")
            row = cursor.fetchone()
            stats["today_income"] = float(row[0] or 0.0) if row else 0.0
            cursor.execute("SELECT COUNT(*) FROM vpn_keys WHERE date(created_date) = date('now')")
            row = cursor.fetchone()
            stats["today_issued_keys"] = (row[0] or 0) if row else 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить статистику администратора: {e}")
    return stats

def get_latest_speedtest(host_name: str) -> dict | None:
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error, created_at FROM host_speedtests WHERE TRIM(host_name) = TRIM(?) ORDER BY datetime(created_at) DESC LIMIT 1", (host_name_n,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить последний speedtest для хоста '{host_name}': {e}")
        return None

def get_speedtests(host_name: str, limit: int = 20) -> list[dict]:
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            try: limit_int = int(limit)
            except: limit_int = 20
            cursor.execute("SELECT id, host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error, created_at FROM host_speedtests WHERE TRIM(host_name) = TRIM(?) ORDER BY datetime(created_at) DESC LIMIT ?", (host_name_n, limit_int))
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось получить speedtest-данные для хоста '{host_name}': {e}")
        return []

def insert_host_speedtest(host_name: str, method: str, ping_ms: float | None = None, jitter_ms: float | None = None,
                         download_mbps: float | None = None, upload_mbps: float | None = None,
                         server_name: str | None = None, server_id: str | None = None,
                         ok: bool = True, error: str | None = None) -> bool:
    try:
        host_name_n = normalize_host_name(host_name)
        method_s = (method or '').strip().lower()
        if method_s not in ('ssh', 'net'): method_s = 'ssh'
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO host_speedtests (host_name, method, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, ok, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (host_name_n, method_s, ping_ms, jitter_ms, download_mbps, upload_mbps, server_name, server_id, 1 if ok else 0, error or None))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось сохранить запись speedtest для '{host_name}': {e}")
        return False

def insert_host_metrics(host_name: str, metrics: dict) -> bool:
    try:
        host_name_n = normalize_host_name(host_name)
        m = metrics or {}
        load = m.get('loadavg') or {}
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO host_metrics (host_name, cpu_percent, mem_percent, mem_used, mem_total, disk_percent, disk_used, disk_total, load1, load5, load15, uptime_seconds, ok, error) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (host_name_n, float(m.get('cpu_percent')) if m.get('cpu_percent') is not None else None,
                           float(m.get('mem_percent')) if m.get('mem_percent') is not None else None,
                           int(m.get('mem_used')) if m.get('mem_used') is not None else None,
                           int(m.get('mem_total')) if m.get('mem_total') is not None else None,
                           float(m.get('disk_percent')) if m.get('disk_percent') is not None else None,
                           int(m.get('disk_used')) if m.get('disk_used') is not None else None,
                           int(m.get('disk_total')) if m.get('disk_total') is not None else None,
                           float(load.get('1m')) if load.get('1m') is not None else None,
                           float(load.get('5m')) if load.get('5m') is not None else None,
                           float(load.get('15m')) if load.get('15m') is not None else None,
                           float(m.get('uptime_seconds')) if m.get('uptime_seconds') is not None else None,
                           1 if (m.get('ok') in (True, 1, '1')) else 0,
                           str(m.get('error')) if m.get('error') else None))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"insert_host_metrics failed for '{host_name}': {e}")
        return False

def get_host_metrics_recent(host_name: str, limit: int = 60) -> list[dict]:
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT host_name, cpu_percent, mem_percent, mem_used, mem_total, disk_percent, disk_used, disk_total, load1, load5, load15, uptime_seconds, ok, error, created_at FROM host_metrics WHERE TRIM(host_name) = TRIM(?) ORDER BY datetime(created_at) DESC LIMIT ?", (host_name_n, int(limit)))
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"get_host_metrics_recent failed for '{host_name}': {e}")
        return []

def get_latest_host_metrics(host_name: str) -> dict | None:
    try:
        host_name_n = normalize_host_name(host_name)
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM host_metrics WHERE TRIM(host_name) = TRIM(?) ORDER BY datetime(created_at) DESC LIMIT 1", (host_name_n,))
            r = cursor.fetchone()
            return dict(r) if r else None
    except sqlite3.Error as e:
        logging.error(f"get_latest_host_metrics failed for '{host_name}': {e}")
        return None

def insert_resource_metric(scope: str, object_name: str, *, cpu_percent: float | None = None, mem_percent: float | None = None,
                          disk_percent: float | None = None, load1: float | None = None,
                          net_bytes_sent: int | None = None, net_bytes_recv: int | None = None,
                          raw_json: str | None = None) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO resource_metrics (scope, object_name, cpu_percent, mem_percent, disk_percent, load1, net_bytes_sent, net_bytes_recv, raw_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          ((scope or '').strip(), (object_name or '').strip(), cpu_percent, mem_percent, disk_percent, load1, net_bytes_sent, net_bytes_recv, raw_json))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error("Не удалось insert resource metric for %s/%s: %s", scope, object_name, e)
        return None

def get_metrics_series(scope: str, object_name: str, *, since_hours: int = 24, limit: int = 500) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            hours_filter = 2 if since_hours == 1 else max(1, int(since_hours))
            cursor.execute("SELECT created_at, cpu_percent, mem_percent, disk_percent, load1 FROM resource_metrics WHERE scope = ? AND object_name = ? AND created_at >= datetime('now', ?) ORDER BY created_at ASC LIMIT ?",
                          ((scope or '').strip(), (object_name or '').strip(), f'-{hours_filter} hours', max(10, int(limit))))
            rows = cursor.fetchall() or []
            return [dict(r) for r in rows]
    except sqlite3.Error as e:
        logging.error("Не удалось get metrics series for %s/%s: %s", scope, object_name, e)
        return []

def create_support_ticket(user_id: int, subject: str | None = None) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO support_tickets (user_id, subject) VALUES (?, ?)", (user_id, subject))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Не удалось create support ticket for user {user_id}: {e}")
        return None

def add_support_message(ticket_id: int, sender: str, content: str) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO support_messages (ticket_id, sender, content) VALUES (?, ?, ?)", (ticket_id, sender, content))
            cursor.execute("UPDATE support_tickets SET updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?", (ticket_id,))
            conn.commit()
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Не удалось add support message to ticket {ticket_id}: {e}")
        return None

def get_ticket(ticket_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось get ticket {ticket_id}: {e}")
        return None

def get_ticket_by_thread(forum_chat_id: str, message_thread_id: int) -> dict | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM support_tickets WHERE forum_chat_id = ? AND message_thread_id = ?", (str(forum_chat_id), int(message_thread_id)))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Не удалось get ticket by thread {forum_chat_id}/{message_thread_id}: {e}")
        return None

def get_user_tickets(user_id: int, status: str | None = None) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT * FROM support_tickets WHERE user_id = ? AND status = ? ORDER BY updated_at DESC", (user_id, status))
            else:
                cursor.execute("SELECT * FROM support_tickets WHERE user_id = ? ORDER BY updated_at DESC", (user_id,))
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get tickets for user {user_id}: {e}")
        return []

def get_ticket_messages(ticket_id: int) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM support_messages WHERE ticket_id = ? ORDER BY created_at ASC", (ticket_id,))
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get messages for ticket {ticket_id}: {e}")
        return []

def set_ticket_status(ticket_id: int, status: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE support_tickets SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?", (status, ticket_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось set status '{status}' for ticket {ticket_id}: {e}")
        return False

def update_ticket_subject(ticket_id: int, subject: str) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE support_tickets SET subject = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?", (subject, ticket_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось update subject for ticket {ticket_id}: {e}")
        return False

def update_ticket_thread_info(ticket_id: int, forum_chat_id: str | None, message_thread_id: int | None) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE support_tickets SET forum_chat_id = ?, message_thread_id = ?, updated_at = CURRENT_TIMESTAMP WHERE ticket_id = ?",
                          (forum_chat_id, message_thread_id, ticket_id))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось update thread info for ticket {ticket_id}: {e}")
        return False

def delete_ticket(ticket_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM support_messages WHERE ticket_id = ?", (ticket_id,))
            cursor.execute("DELETE FROM support_tickets WHERE ticket_id = ?", (ticket_id,))
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось delete ticket {ticket_id}: {e}")
        return False

def get_tickets_paginated(page: int = 1, per_page: int = 20, status: str | None = None) -> tuple[list[dict], int]:
    offset = (page - 1) * per_page
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if status:
                cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = ?", (status,))
                total = cursor.fetchone()[0] or 0
                cursor.execute("SELECT * FROM support_tickets WHERE status = ? ORDER BY updated_at DESC LIMIT ? OFFSET ?", (status, per_page, offset))
            else:
                cursor.execute("SELECT COUNT(*) FROM support_tickets")
                total = cursor.fetchone()[0] or 0
                cursor.execute("SELECT * FROM support_tickets ORDER BY updated_at DESC LIMIT ? OFFSET ?", (per_page, offset))
            return [dict(r) for r in cursor.fetchall()], total
    except sqlite3.Error as e:
        logging.error("Не удалось get paginated support tickets: %s", e)
        return [], 0

def get_open_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'open'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Не удалось get open tickets count: %s", e)
        return 0

def get_closed_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets WHERE status = 'closed'")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Не удалось get closed tickets count: %s", e)
        return 0

def get_all_tickets_count() -> int:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM support_tickets")
            return cursor.fetchone()[0] or 0
    except sqlite3.Error as e:
        logging.error("Не удалось get all tickets count: %s", e)
        return 0

def create_promo_code(code: str, *, discount_percent: float | None = None, discount_amount: float | None = None,
                     usage_limit_total: int | None = None, usage_limit_per_user: int | None = None,
                     valid_from: datetime | None = None, valid_until: datetime | None = None,
                     created_by: int | None = None, description: str | None = None) -> bool:
    code_s = (code or "").strip().upper()
    if not code_s:
        raise ValueError("code is required")
    if (discount_percent or 0) <= 0 and (discount_amount or 0) <= 0:
        raise ValueError("discount must be positive")
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            vf = valid_from.isoformat() if valid_from else None
            vu = valid_until.isoformat() if valid_until else None
            fields = [("code", code_s), ("discount_percent", float(discount_percent) if discount_percent is not None else None),
                     ("discount_amount", float(discount_amount) if discount_amount is not None else None),
                     ("usage_limit_total", usage_limit_total), ("usage_limit_per_user", usage_limit_per_user),
                     ("valid_from", vf), ("description", description), ("valid_to", vu), ("is_active", 1), ("active", 1)]
            columns = ", ".join([f for f, _ in fields])
            placeholders = ", ".join(["?" for _ in fields])
            values = [v for _, v in fields]
            cursor.execute(f"INSERT INTO promo_codes ({columns}) VALUES ({placeholders})", values)
            conn.commit()
            return True
    except sqlite3.IntegrityError:
        return False
    except sqlite3.Error as e:
        logging.error(f"Ошибка создания промокода: {e}")
        return False

def get_promo_code(code: str) -> dict | None:
    code_s = (code or "").strip().upper()
    if not code_s: return None
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM promo_codes WHERE code = ?", (code_s,))
            row = cursor.fetchone()
            return dict(row) if row else None
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения промокода: {e}")
        return None

def list_promo_codes(include_inactive: bool = True) -> list[dict]:
    query = "SELECT * FROM promo_codes"
    if not include_inactive:
        query += " WHERE COALESCE(is_active, active, 1) = 1"
    query += " ORDER BY created_at DESC"
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(r) for r in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Ошибка получения списка промокодов: {e}")
        return []

def check_promo_code_available(code: str, user_id: int) -> tuple[dict | None, str | None]:
    code_s = (code or "").strip().upper()
    if not code_s: return None, "empty_code"
    user_id_i = int(user_id)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            used_expr = "COALESCE(used_total, used_count, 0)"
            vu_expr = "valid_until" if "valid_until" in [c[1] for c in cursor.execute("PRAGMA table_info(promo_codes)").fetchall()] else "valid_to"
            active_expr = "is_active" if "is_active" in [c[1] for c in cursor.execute("PRAGMA table_info(promo_codes)").fetchall()] else "active"
            cursor.execute(f"SELECT code, discount_percent, discount_amount, usage_limit_total, usage_limit_per_user, {used_expr} AS used_total, valid_from, {vu_expr} AS valid_until, {active_expr} AS is_active FROM promo_codes WHERE code = ?", (code_s,))
            row = cursor.fetchone()
            if not row: return None, "not_found"
            promo = dict(row)
            if not promo.get("is_active"): return None, "inactive"
            now = datetime.utcnow()
            vf = promo.get("valid_from")
            if vf:
                try:
                    if datetime.fromisoformat(str(vf)) > now: return None, "not_started"
                except: pass
            vu = promo.get("valid_until")
            if vu:
                try:
                    if datetime.fromisoformat(str(vu)) < now: return None, "expired"
                except: pass
            limit_total = promo.get("usage_limit_total")
            used_total = promo.get("used_total") or 0
            if limit_total and used_total >= limit_total: return None, "total_limit_reached"
            per_user = promo.get("usage_limit_per_user")
            if per_user:
                cursor.execute("SELECT COUNT(1) FROM promo_code_usages WHERE code = ? AND user_id = ?", (code_s, user_id_i))
                count = cursor.fetchone()[0]
                if count >= per_user: return None, "user_limit_reached"
            return promo, None
    except sqlite3.Error as e:
        logging.error(f"Ошибка проверки доступности промокода: {e}")
        return None, "db_error"

def redeem_promo_code(code: str, user_id: int, *, applied_amount: float, order_id: str | None = None) -> dict | None:
    code_s = (code or "").strip().upper()
    if not code_s: return None
    user_id_i = int(user_id)
    applied_amount_f = float(applied_amount)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            used_expr = "COALESCE(used_total, used_count, 0)"
            vu_expr = "valid_until" if "valid_until" in [c[1] for c in cursor.execute("PRAGMA table_info(promo_codes)").fetchall()] else "valid_to"
            active_expr = "is_active" if "is_active" in [c[1] for c in cursor.execute("PRAGMA table_info(promo_codes)").fetchall()] else "active"
            cursor.execute(f"SELECT code, discount_percent, discount_amount, usage_limit_total, usage_limit_per_user, {used_expr} AS used_total, valid_from, {vu_expr} AS valid_until, {active_expr} AS is_active FROM promo_codes WHERE code = ?", (code_s,))
            row = cursor.fetchone()
            if not row: return None
            promo = dict(row)
            if not promo.get("is_active"): return None
            now = datetime.utcnow()
            vf = promo.get("valid_from")
            if vf:
                try:
                    if datetime.fromisoformat(str(vf)) > now: return None
                except: pass
            vu = promo.get("valid_until")
            if vu:
                try:
                    if datetime.fromisoformat(str(vu)) < now: return None
                except: pass
            limit_total = promo.get("usage_limit_total")
            used_total = promo.get("used_total") or 0
            if limit_total and used_total >= limit_total: return None
            per_user = promo.get("usage_limit_per_user")
            if per_user:
                cursor.execute("SELECT COUNT(1) FROM promo_code_usages WHERE code = ? AND user_id = ?", (code_s, user_id_i))
                count = cursor.fetchone()[0]
                if count >= per_user: return None
            cursor.execute("INSERT INTO promo_code_usages (code, user_id, applied_amount, order_id) VALUES (?, ?, ?, ?)", (code_s, user_id_i, applied_amount_f, order_id))
            cursor.execute("UPDATE promo_codes SET used_total = COALESCE(used_total, 0) + 1, used_count = COALESCE(used_count, 0) + 1 WHERE code = ?", (code_s,))
            conn.commit()
            promo["used_total"] = (used_total or 0) + 1
            promo["redeemed_by"] = user_id_i
            promo["applied_amount"] = applied_amount_f
            promo["order_id"] = order_id
            if per_user: promo["user_usage_count"] = (count or 0) + 1
            else: promo["user_usage_count"] = None
            return promo
    except sqlite3.Error as e:
        logging.error(f"Ошибка использования промокода: {e}")
        return None

def update_promo_code_status(code: str, *, is_active: bool | None = None) -> bool:
    code_s = (code or "").strip().upper()
    if not code_s: return False
    sets = []
    params = []
    if is_active is not None:
        sets.append("is_active = ?")
        params.append(1 if is_active else 0)
        sets.append("active = ?")
        params.append(1 if is_active else 0)
    if not sets: return False
    params.append(code_s)
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute(f"UPDATE promo_codes SET {', '.join(sets)} WHERE code = ?", params)
            conn.commit()
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Ошибка обновления статуса промокода: {e}")
        return False

def get_button_configs(menu_type: str = None) -> list[dict]:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            if menu_type:
                cursor.execute("SELECT * FROM button_configs WHERE menu_type = ? ORDER BY sort_order, id", (menu_type,))
            else:
                cursor.execute("SELECT * FROM button_configs ORDER BY menu_type, sort_order, id")
            return [dict(row) for row in cursor.fetchall()]
    except sqlite3.Error as e:
        logging.error(f"Не удалось get button configs: {e}")
        return []

def create_button_config(config: dict) -> int | None:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("INSERT INTO button_configs (menu_type, button_id, text, callback_data, url, row_position, column_position, button_width, sort_order, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                          (config.get('menu_type', 'main_menu'), config.get('button_id', ''), config.get('text', ''),
                           config.get('callback_data'), config.get('url'), config.get('row_position', 0),
                           config.get('column_position', 0), config.get('button_width', 1),
                           config.get('sort_order', 0), config.get('is_active', True)))
            return cursor.lastrowid
    except sqlite3.Error as e:
        logging.error(f"Не удалось create button config: {e}")
        return None

def update_button_config(button_id: int, config: dict) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("UPDATE button_configs SET text = ?, callback_data = ?, url = ?, row_position = ?, column_position = ?, button_width = ?, sort_order = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
                          (config.get('text', ''), config.get('callback_data'), config.get('url'),
                           config.get('row_position', 0), config.get('column_position', 0),
                           config.get('button_width', 1), config.get('sort_order', 0),
                           config.get('is_active', True), button_id))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось update button config {button_id}: {e}")
        return False

def delete_button_config(button_id: int) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM button_configs WHERE id = ?", (button_id,))
            return cursor.rowcount > 0
    except sqlite3.Error as e:
        logging.error(f"Не удалось delete button config {button_id}: {e}")
        return False

def reorder_button_configs(menu_type: str, button_orders: list[dict]) -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            for order_data in button_orders:
                sort_order = int(order_data.get('sort_order', 0) or 0)
                row_pos = int(order_data.get('row_position', 0) or 0)
                col_pos = int(order_data.get('column_position', 0) or 0)
                btn_width = int(order_data.get('button_width', 1) or 1)
                btn_id = order_data.get('id')
                if not btn_id:
                    btn_key = order_data.get('button_id')
                    if not btn_key: continue
                    cursor.execute("SELECT id FROM button_configs WHERE menu_type = ? AND button_id = ?", (menu_type, btn_key))
                    row = cursor.fetchone()
                    if not row: continue
                    btn_id = row[0]
                cursor.execute("UPDATE button_configs SET sort_order = ?, row_position = ?, column_position = ?, button_width = ? WHERE id = ? AND menu_type = ?",
                              (sort_order, row_pos, col_pos, btn_width, btn_id, menu_type))
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось reorder button configs for {menu_type}: {e}")
        return False

def migrate_existing_buttons() -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            menu_configs = {
                'main_menu': [
                    {'button_id': 'btn_try', 'callback_data': 'get_trial', 'text': '🎁 Попробовать бесплатно', 'row_position': 0, 'column_position': 0, 'button_width': 2},
                    {'button_id': 'btn_profile', 'callback_data': 'show_profile', 'text': '👤 Мой профиль', 'row_position': 1, 'column_position': 0, 'button_width': 2},
                    {'button_id': 'btn_my_keys', 'callback_data': 'manage_keys', 'text': '🔑 Мои ключи ({count})', 'row_position': 2, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'btn_buy_key', 'callback_data': 'buy_new_key', 'text': '💳 Купить ключ', 'row_position': 2, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'btn_top_up', 'callback_data': 'top_up_start', 'text': '➕ Пополнить баланс', 'row_position': 3, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'btn_referral', 'callback_data': 'show_referral_program', 'text': '🤝 Реферальная программа', 'row_position': 3, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'btn_support', 'callback_data': 'show_help', 'text': '🆘 Поддержка', 'row_position': 4, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'btn_about', 'callback_data': 'show_about', 'text': 'ℹ️ О проекте', 'row_position': 4, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'btn_howto', 'callback_data': 'howto_vless', 'text': '❓ Как использовать', 'row_position': 5, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'btn_speed', 'callback_data': 'user_speedtest', 'text': '⚡ Тест скорости', 'row_position': 5, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'btn_admin', 'callback_data': 'admin_menu', 'text': '⚙️ Админка', 'row_position': 6, 'column_position': 0, 'button_width': 2},
                ],
                'admin_menu': [
                    {'button_id': 'admin_users', 'callback_data': 'admin_users', 'text': '👥 Пользователи', 'row_position': 0, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'admin_keys', 'callback_data': 'admin_host_keys', 'text': '🔑 Ключи', 'row_position': 0, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'admin_issue_key', 'callback_data': 'admin_gift_key', 'text': '🎁 Выдать ключ', 'row_position': 1, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'admin_speed_test', 'callback_data': 'admin_speedtest', 'text': '⚡ Тест скорости', 'row_position': 1, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'admin_monitoring', 'callback_data': 'admin_monitor', 'text': '📊 Мониторинг', 'row_position': 2, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'admin_db_backup', 'callback_data': 'admin_backup_db', 'text': '💾 Бэкап БД', 'row_position': 2, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'admin_restore_db', 'callback_data': 'admin_restore_db', 'text': '🔄 Восстановить БД', 'row_position': 3, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'admin_administrators', 'callback_data': 'admin_administrators', 'text': '👮 Администраторы', 'row_position': 3, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'admin_promo_codes', 'callback_data': 'admin_promo_menu', 'text': '🏷️ Промокоды', 'row_position': 4, 'column_position': 0, 'button_width': 2},
                    {'button_id': 'admin_mailing', 'callback_data': 'start_broadcast', 'text': '📢 Рассылка', 'row_position': 5, 'column_position': 0, 'button_width': 2},
                    {'button_id': 'back_to_main', 'callback_data': 'main_menu', 'text': '⬅️ Назад в меню', 'row_position': 6, 'column_position': 0, 'button_width': 2},
                ],
                'profile_menu': [
                    {'button_id': 'profile_info', 'callback_data': 'profile_info', 'text': 'ℹ️ Информация', 'row_position': 0, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'profile_balance', 'callback_data': 'profile_balance', 'text': '💰 Баланс', 'row_position': 0, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'profile_keys', 'callback_data': 'manage_keys', 'text': '🔑 Мои ключи', 'row_position': 1, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'profile_referrals', 'callback_data': 'show_referral_program', 'text': '🤝 Рефералы', 'row_position': 1, 'column_position': 1, 'button_width': 1},
                    {'button_id': 'back_to_main', 'callback_data': 'main_menu', 'text': '🏠 Главное меню', 'row_position': 2, 'column_position': 0, 'button_width': 2},
                ],
                'support_menu': [
                    {'button_id': 'support_new', 'callback_data': 'support_new_ticket', 'text': '📝 Новое обращение', 'row_position': 0, 'column_position': 0, 'button_width': 1},
                    {'button_id': 'support_my', 'callback_data': 'support_my_tickets', 'text': '📋 Мои обращения', 'row_position': 0, 'column_position': 1, 'button_width': 1},
                ]
            }
            cursor.execute("SELECT COUNT(*) FROM button_configs")
            if cursor.fetchone()[0] > 0:
                return True
            for menu_type, button_settings in menu_configs.items():
                sort_order = 0
                for button_data in button_settings:
                    text = get_setting(button_data['button_id']) or button_data['text']
                    cursor.execute("INSERT INTO button_configs (menu_type, button_id, text, callback_data, row_position, column_position, button_width, sort_order, is_active) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                                  (menu_type, button_data['button_id'], text, button_data['callback_data'],
                                   button_data['row_position'], button_data['column_position'], button_data['button_width'], sort_order, True))
                    sort_order += 1
            cursor.execute("DELETE FROM button_configs WHERE id NOT IN (SELECT MIN(id) FROM button_configs GROUP BY menu_type, button_id)")
            conn.commit()
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось migrate existing buttons: {e}")
        return False

def cleanup_duplicate_buttons() -> bool:
    try:
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM button_configs WHERE id NOT IN (SELECT MIN(id) FROM button_configs WHERE menu_type = 'main_menu' GROUP BY button_id)")
            logging.info(f"Удалено {cursor.rowcount} дублирующихся конфигураций кнопок")
            return True
    except sqlite3.Error as e:
        logging.error(f"Не удалось очистить дублирующиеся кнопки: {e}")
        return False

def force_button_migration() -> bool:
    try:
        logging.info("Начинаю принудительную миграцию кнопок...")
        with sqlite3.connect(DB_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM button_configs")
            logging.info(f"Принудительно удалено {cursor.rowcount} существующих конфигураций кнопок")
            conn.commit()
        migrate_existing_buttons()
        logging.info("Принудительная миграция кнопок успешно завершена")
        return True
    except Exception as e:
        logging.error(f"Ошибка при принудительной миграции кнопок: {e}")
        return False

# ==================== BACKUP MANAGER ====================
BACKUPS_DIR = Path("/app/project/backups")
BACKUPS_DIR.mkdir(parents=True, exist_ok=True)

def _timestamp() -> str:
    return datetime.now().strftime("%Y%m%d-%H%M%S")

def create_backup_file() -> Path | None:
    try:
        if not DB_FILE.exists():
            logger.error(f"Бэкап: файл БД не найден: {DB_FILE}")
            return None
        ts = _timestamp()
        tmp_db_copy = BACKUPS_DIR / f"users-{ts}.db"
        zip_path = BACKUPS_DIR / f"db-backup-{ts}.zip"
        with sqlite3.connect(DB_FILE) as src:
            with sqlite3.connect(tmp_db_copy) as dst:
                src.backup(dst)
        with zipfile.ZipFile(zip_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(tmp_db_copy, arcname=tmp_db_copy.name)
        try:
            tmp_db_copy.unlink(missing_ok=True)
        except: pass
        logger.info(f"Бэкап: создан файл {zip_path}")
        return zip_path
    except Exception as e:
        logger.error(f"Бэкап: не удалось создать архив: {e}", exc_info=True)
        return None

def cleanup_old_backups(keep: int = 7):
    try:
        files = sorted(BACKUPS_DIR.glob("db-backup-*.zip"), key=lambda p: p.stat().st_mtime, reverse=True)
        for f in files[keep:]:
            try: f.unlink(missing_ok=True)
            except: pass
    except Exception as e:
        logger.warning(f"Бэкап: не удалось очистить старые архивы: {e}")

async def send_backup_to_admins(bot: Bot, zip_path: Path) -> int:
    cnt = 0
    try:
        admin_ids = list(get_admin_ids() or [])
        if not admin_ids:
            logger.warning("Бэкап: нет администраторов для отправки архива")
            return 0
        caption = f"🗄 Бэкап БД: {zip_path.name}"
        file = FSInputFile(str(zip_path))
        for uid in admin_ids:
            try:
                await bot.send_document(chat_id=int(uid), document=file, caption=caption)
                cnt += 1
            except Exception as e:
                logger.error(f"Бэкап: не удалось отправить администратору {uid}: {e}")
        return cnt
    except Exception as e:
        logger.error(f"Бэкап: ошибка при рассылке архива: {e}", exc_info=True)
        return cnt

def validate_db_file(db_path: Path) -> bool:
    try:
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
            present = {row[0] for row in cur.fetchall()}
            missing = {'users', 'bot_settings'} - present
            return not missing
    except Exception as e:
        logger.error(f"Восстановление: ошибка валидации файла БД: {e}")
        return False

def restore_from_file(uploaded_path: Path) -> bool:
    try:
        if not uploaded_path.exists():
            logger.error(f"Восстановление: файл не найден: {uploaded_path}")
            return False
        tmp_dir = BACKUPS_DIR / f"restore-{_timestamp()}"
        tmp_dir.mkdir(parents=True, exist_ok=True)
        candidate_db = None
        if uploaded_path.suffix.lower() == '.zip':
            try:
                with zipfile.ZipFile(uploaded_path, 'r') as zf:
                    for n in zf.namelist():
                        if n.lower().endswith('.db'):
                            zf.extract(n, path=tmp_dir)
                            candidate_db = tmp_dir / n
                            break
            except Exception as e:
                logger.error(f"Восстановление: не удалось распаковать архив: {e}")
                return False
        else:
            candidate_db = uploaded_path
        if not candidate_db or not candidate_db.exists():
            logger.error("Восстановление: в переданном файле не найдено .db")
            return False
        if not validate_db_file(candidate_db):
            logger.error("Восстановление: файл БД не прошёл проверку")
            return False
        cur_backup = create_backup_file()
        if cur_backup and cur_backup.exists():
            try:
                backup_before = BACKUPS_DIR / f"before-restore-{_timestamp()}.zip"
                shutil.copy(cur_backup, backup_before)
            except: pass
        with sqlite3.connect(candidate_db) as src:
            with sqlite3.connect(DB_FILE) as dst:
                src.backup(dst)
        try:
            run_migration()
        except: pass
        logger.info("Восстановление: база данных успешно заменена")
        return True
    except Exception as e:
        logger.error(f"Восстановление: ошибка: {e}", exc_info=True)
        return False

def run_migration():
    try:
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()
        cursor.execute("PRAGMA table_info(users)")
        columns = [row[1] for row in cursor.fetchall()]
        for col in ['referred_by', 'balance', 'referral_balance', 'referral_balance_all', 'referral_start_bonus_received']:
            if col not in columns:
                cursor.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 0" if col != 'balance' else f"ALTER TABLE users ADD COLUMN {col} REAL DEFAULT 0")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_users_reg_date ON users(registration_date)")
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error(f"Ошибка миграции: {e}")

# ==================== BAN MIDDLEWARE ====================
class BanMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]], event: TelegramObject, data: Dict[str, Any]) -> Any:
        user = data.get('event_from_user')
        if not user:
            return await handler(event, data)
        user_data = get_user(user.id)
        if user_data and user_data.get('is_banned'):
            ban_message_text = "🚫 Вы заблокированы и не можете использовать этого бота."
            support = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
            kb_builder = InlineKeyboardBuilder()
            url = None
            if support:
                if support.startswith("@"):
                    url = f"tg://resolve?domain={support[1:]}"
                elif support.startswith("tg://"):
                    url = support
                elif support.startswith("http://") or support.startswith("https://"):
                    try:
                        part = support.split("/")[-1].split("?")[0]
                        if part: url = f"tg://resolve?domain={part}"
                    except: url = support
                else: url = f"tg://resolve?domain={support}"
            if url: kb_builder.button(text="🆘 Написать в поддержку", url=url)
            else: kb_builder.button(text="🆘 Поддержка", callback_data="show_help")
            ban_kb = kb_builder.as_markup()
            if isinstance(event, CallbackQuery):
                await event.answer(ban_message_text, show_alert=True)
                try: await event.bot.send_message(chat_id=event.from_user.id, text=ban_message_text, reply_markup=ban_kb)
                except: pass
            elif isinstance(event, Message):
                try: await event.answer(ban_message_text, reply_markup=ban_kb)
                except: await event.answer(ban_message_text)
            return
        return await handler(event, data)

# ==================== XUI API ====================
from py3xui import Api, Client, Inbound

def login_to_host(host_url: str, username: str, password: str, inbound_id: int) -> tuple[Api | None, Inbound | None]:
    try:
        api = Api(host=host_url, username=username, password=password)
        api.login()
        inbounds = api.inbound.get_list()
        target_inbound = next((inbound for inbound in inbounds if inbound.id == inbound_id), None)
        if target_inbound is None:
            logger.error(f"Входящий трафик с ID '{inbound_id}' не найден на хосте '{host_url}'")
            return None, None
        return api, target_inbound
    except Exception as e:
        logger.error(f"Не удалось выполнить вход для хоста '{host_url}': {e}", exc_info=True)
        return None, None

def get_connection_string(inbound: Inbound, user_uuid: str, host_url: str, remark: str) -> str | None:
    if not inbound: return None
    settings = inbound.stream_settings.reality_settings.get("settings")
    if not settings: return None
    public_key = settings.get("publicKey")
    fp = settings.get("fingerprint")
    server_names = inbound.stream_settings.reality_settings.get("serverNames")
    short_ids = inbound.stream_settings.reality_settings.get("shortIds")
    port = inbound.port
    if not all([public_key, server_names, short_ids]): return None
    parsed_url = urlparse(host_url)
    short_id = short_ids[0]
    return (f"vless://{user_uuid}@{parsed_url.hostname}:{port}?type=tcp&security=reality&pbk={public_key}&fp={fp}&sni={server_names[0]}&sid={short_id}&spx=%2F&flow=xtls-rprx-vision#{remark}")

def get_subscription_link(user_uuid: str, host_url: str, host_name: str | None = None, sub_token: str | None = None) -> str:
    host_base = None
    try:
        if host_name:
            host = get_host(host_name)
            if host: host_base = (host.get("subscription_url") or "").strip()
    except: pass
    base = (host_base or "").strip()
    if sub_token:
        if base: return base.replace("{token}", sub_token) if "{token}" in base else f"{base.rstrip('/')}/{sub_token}"
        domain = (get_setting("domain") or "").strip()
        parsed = urlparse(host_url)
        hostname = domain if domain else (parsed.hostname or "")
        scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
        return f"{scheme}://{hostname}/sub/{sub_token}"
    if base: return base
    domain = (get_setting("domain") or "").strip()
    parsed = urlparse(host_url)
    hostname = domain if domain else (parsed.hostname or "")
    scheme = parsed.scheme if parsed.scheme in ("http", "https") else "https"
    return f"{scheme}://{hostname}/sub/{user_uuid}?format=v2ray"

def update_or_create_client_on_panel(api: Api, inbound_id: int, email: str, days_to_add: int | None = None, target_expiry_ms: int | None = None) -> tuple[str | None, int | None, str | None]:
    try:
        inbound_to_modify = api.inbound.get_by_id(inbound_id)
        if not inbound_to_modify: raise ValueError(f"Could not find inbound with ID {inbound_id}")
        if inbound_to_modify.settings.clients is None: inbound_to_modify.settings.clients = []
        client_index = -1
        for i, client in enumerate(inbound_to_modify.settings.clients):
            if client.email == email:
                client_index = i
                break
        if target_expiry_ms is not None:
            new_expiry_ms = int(target_expiry_ms)
        else:
            if days_to_add is None: raise ValueError("Either days_to_add or target_expiry_ms must be provided")
            if client_index != -1:
                existing_client = inbound_to_modify.settings.clients[client_index]
                if existing_client.expiry_time > int(datetime.now().timestamp() * 1000):
                    current_expiry_dt = datetime.fromtimestamp(existing_client.expiry_time / 1000)
                    new_expiry_dt = current_expiry_dt + timedelta(days=days_to_add)
                else: new_expiry_dt = datetime.now() + timedelta(days=days_to_add)
            else: new_expiry_dt = datetime.now() + timedelta(days=days_to_add)
            new_expiry_ms = int(new_expiry_dt.timestamp() * 1000)
        client_sub_token = None
        if client_index != -1:
            try: inbound_to_modify.settings.clients[client_index].reset = 0
            except: pass
            inbound_to_modify.settings.clients[client_index].enable = True
            inbound_to_modify.settings.clients[client_index].expiry_time = new_expiry_ms
            existing_client = inbound_to_modify.settings.clients[client_index]
            client_uuid = existing_client.id
            try:
                for attr in ("subId", "subscription", "sub_id"):
                    if hasattr(existing_client, attr):
                        val = getattr(existing_client, attr)
                        if val: client_sub_token = val; break
                if not client_sub_token:
                    client_sub_token = secrets.token_hex(12)
                    for attr in ("subId", "subscription", "sub_id"):
                        try: setattr(existing_client, attr, client_sub_token)
                        except: pass
            except: pass
        else:
            client_uuid = str(uuid.uuid4())
            new_client = Client(id=client_uuid, email=email, enable=True, flow="xtls-rprx-vision", expiry_time=new_expiry_ms)
            try: setattr(new_client, "reset", 0)
            except: pass
            try:
                client_sub_token = secrets.token_hex(12)
                for attr in ("subId", "subscription", "sub_id"):
                    try: setattr(new_client, attr, client_sub_token)
                    except: pass
            except: pass
            inbound_to_modify.settings.clients.append(new_client)
        api.inbound.update(inbound_id, inbound_to_modify)
        return client_uuid, new_expiry_ms, client_sub_token
    except Exception as e:
        logger.error(f"Ошибка в update_or_create_client_on_panel: {e}", exc_info=True)
        return None, None, None

async def create_or_update_key_on_host(host_name: str, email: str, days_to_add: int | None = None, expiry_timestamp_ms: int | None = None) -> Dict | None:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Хост '{host_name}' не найден в базе данных.")
        return None
    api, inbound = login_to_host(host_url=host_data['host_url'], username=host_data['host_username'], password=host_data['host_pass'], inbound_id=host_data['host_inbound_id'])
    if not api or not inbound:
        logger.error(f"Не удалось войти или найти inbound на хосте '{host_name}'.")
        return None
    client_uuid, new_expiry_ms, client_sub_token = update_or_create_client_on_panel(api, inbound.id, email, days_to_add=days_to_add, target_expiry_ms=expiry_timestamp_ms)
    if not client_uuid:
        logger.error(f"Не удалось создать/обновить клиента '{email}' на хосте '{host_name}'.")
        return None
    connection_string = get_subscription_link(client_uuid, host_data['host_url'], host_name, sub_token=client_sub_token)
    logger.info(f"Успешно обработан ключ для '{email}' на хосте '{host_name}'.")
    return {"client_uuid": client_uuid, "email": email, "expiry_timestamp_ms": new_expiry_ms, "connection_string": connection_string, "host_name": host_name}

async def get_key_details_from_host(key_data: dict) -> dict | None:
    host_name = key_data.get('host_name')
    if not host_name:
        logger.error(f"Не удалось получить данные ключа: отсутствует host_name для key_id {key_data.get('key_id')}")
        return None
    host_db_data = get_host(host_name)
    if not host_db_data:
        logger.error(f"Не удалось получить данные ключа: хост '{host_name}' не найден в базе данных.")
        return None
    api, inbound = login_to_host(host_url=host_db_data['host_url'], username=host_db_data['host_username'], password=host_db_data['host_pass'], inbound_id=host_db_data['host_inbound_id'])
    if not api or not inbound: return None
    client_sub_token = None
    try:
        if inbound.settings and inbound.settings.clients:
            for client in inbound.settings.clients:
                if getattr(client, "id", None) == key_data['xui_client_uuid'] or getattr(client, "email", None) == key_data.get('email'):
                    for attr in ("subId", "subscription", "sub_id", "subscriptionId", "subscription_token"):
                        val = None
                        if hasattr(client, attr): val = getattr(client, attr)
                        else:
                            try: val = client.get(attr)
                            except: pass
                        if val: client_sub_token = val; break
                    break
    except: pass
    connection_string = get_subscription_link(key_data['xui_client_uuid'], host_db_data['host_url'], host_name, sub_token=client_sub_token)
    return {"connection_string": connection_string}

async def delete_client_on_host(host_name: str, client_email: str) -> bool:
    host_data = get_host(host_name)
    if not host_data:
        logger.error(f"Не удалось удалить клиента: хост '{host_name}' не найден.")
        return False
    api, inbound = login_to_host(host_url=host_data['host_url'], username=host_data['host_username'], password=host_data['host_pass'], inbound_id=host_data['host_inbound_id'])
    if not api or not inbound:
        logger.error(f"Не удалось удалить клиента: ошибка входа для хоста '{host_name}'.")
        return False
    try:
        client_to_delete = get_key_by_email(client_email)
        if client_to_delete:
            api.client.delete(inbound.id, client_to_delete['xui_client_uuid'])
            logger.info(f"Клиент '{client_email}' успешно удалён с хоста '{host_name}'.")
            return True
        else:
            logger.warning(f"Клиент с email '{client_email}' не найден на хосте '{host_name}' для удаления.")
            return True
    except Exception as e:
        logger.error(f"Не удалось удалить клиента '{client_email}' с хоста '{host_name}': {e}", exc_info=True)
        return False

# ==================== SPEEDTEST RUNNER ====================
def _parse_host_port_from_url(url: str) -> tuple[str | None, int | None, bool]:
    try:
        u = urlparse(url)
        host = u.hostname
        port = u.port
        is_https = (u.scheme == 'https')
        if port is None: port = 443 if is_https else 80
        return host, port, is_https
    except: return None, None, False

async def net_probe_for_host(host_row: dict) -> dict:
    url = (host_row.get('host_url') or '').strip()
    target_host, target_port, _ = _parse_host_port_from_url(url)
    result = {'ok': False, 'method': 'net', 'ping_ms': None, 'jitter_ms': None, 'download_mbps': None,
              'upload_mbps': None, 'server_name': None, 'server_id': None, 'http_ms': None, 'error': None}
    if not target_host or not target_port:
        result['error'] = f'Invalid host_url: {url}'; return result
    try:
        loop = asyncio.get_event_loop()
        start = loop.time()
        reader, writer = await asyncio.wait_for(asyncio.open_connection(target_host, target_port), timeout=10.0)
        tcp_ms = (loop.time() - start) * 1000.0
        result['ping_ms'] = round(tcp_ms, 2)
        try: writer.close(); await writer.wait_closed()
        except: pass
    except Exception as e:
        result['error'] = f'TCP connect failed: {e}'; return result
    try:
        async with aiohttp.ClientSession() as session:
            start = asyncio.get_event_loop().time()
            async with session.head(url, timeout=aiohttp.ClientTimeout(total=10)) as resp: _ = resp.status
            http_ms = (asyncio.get_event_loop().time() - start) * 1000.0
            result['http_ms'] = round(http_ms, 2)
        result['ok'] = True
    except:
        try:
            async with aiohttp.ClientSession() as session:
                start = asyncio.get_event_loop().time()
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp: _ = await resp.text()
                http_ms = (asyncio.get_event_loop().time() - start) * 1000.0
                result['http_ms'] = round(http_ms, 2)
            result['ok'] = True
        except Exception as e: result['error'] = f'HTTP failed: {e}'
    return result

def _ssh_exec_json(ssh: paramiko.SSHClient, commands: list[str]) -> tuple[dict | None, str | None]:
    for cmd in commands:
        try:
            stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
            out = stdout.read().decode('utf-8', errors='ignore')
            err = stderr.read().decode('utf-8', errors='ignore')
            if out:
                out = out.strip()
                m = re.search(r"\{.*\}$", out, re.S)
                if m: out = m.group(0)
                try: data = json.loads(out); return data, None
                except: pass
            if err: logger.debug(f"SSH cmd error ({cmd}): {err}")
        except Exception as e: logger.debug(f"SSH exec failed for '{cmd}': {e}"); continue
    return None, 'No JSON output from speedtest commands'

def _parse_ookla_json(data: dict) -> dict:
    try:
        ping_ms = float(data.get('ping', {}).get('latency')) if data.get('ping') else None
        jitter = float(data.get('ping', {}).get('jitter')) if data.get('ping') else None
        down_bps = float(data.get('download', {}).get('bandwidth', 0)) * 8.0
        up_bps = float(data.get('upload', {}).get('bandwidth', 0)) * 8.0
        server = data.get('server', {})
        return {'ping_ms': round(ping_ms, 2) if ping_ms is not None else None,
                'jitter_ms': round(jitter, 2) if jitter is not None else None,
                'download_mbps': round(down_bps / 1_000_000.0, 2) if down_bps else None,
                'upload_mbps': round(up_bps / 1_000_000.0, 2) if up_bps else None,
                'server_name': server.get('name'), 'server_id': str(server.get('id')) if server.get('id') is not None else None}
    except: return {}

def _parse_speedtest_cli_json(data: dict) -> dict:
    try:
        ping_ms = float(data.get('ping')) if data.get('ping') is not None else None
        down_bps = float(data.get('download', 0))
        up_bps = float(data.get('upload', 0))
        srv = data.get('server', {})
        return {'ping_ms': round(ping_ms, 2) if ping_ms is not None else None, 'jitter_ms': None,
                'download_mbps': round(down_bps / 1_000_000.0, 2) if down_bps else None,
                'upload_mbps': round(up_bps / 1_000_000.0, 2) if up_bps else None,
                'server_name': srv.get('name'), 'server_id': str(srv.get('id')) if srv.get('id') is not None else None}
    except: return {}

async def ssh_speedtest_for_host(host_row: dict) -> dict:
    result = {'ok': False, 'method': 'ssh', 'ping_ms': None, 'jitter_ms': None, 'download_mbps': None,
              'upload_mbps': None, 'server_name': None, 'server_id': None, 'error': None}
    ssh_host = (host_row.get('ssh_host') or '').strip()
    ssh_port = int(host_row.get('ssh_port') or 22)
    ssh_user = (host_row.get('ssh_user') or '').strip()
    ssh_password = host_row.get('ssh_password')
    ssh_key_path = (host_row.get('ssh_key_path') or '').strip() or None
    if not ssh_host or not ssh_user:
        result['error'] = 'SSH settings are not configured for host'; return result
    def _run_ssh():
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        pkey = None
        if ssh_key_path:
            try: pkey = paramiko.RSAKey.from_private_key_file(ssh_key_path)
            except:
                try: pkey = paramiko.Ed25519Key.from_private_key_file(ssh_key_path)
                except: pkey = None
        ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, pkey=pkey, timeout=20)
        data, err = _ssh_exec_json(ssh, [
            'speedtest --accept-license --accept-gdpr -f json',
            'speedtest --accept-license --accept-gdpr --format=json',
            'speedtest -f json',
            'speedtest --format=json',
            'speedtest-cli --json'
        ])
        ssh.close()
        if data:
            parsed = _parse_ookla_json(data)
            if not parsed.get('download_mbps') and 'download' in data: parsed = _parse_speedtest_cli_json(data)
            return {'ok': True, **parsed}
        return {'ok': False, 'error': err or 'unknown'}
    try:
        loop = asyncio.get_event_loop()
        out = await loop.run_in_executor(None, _run_ssh)
        result.update(out)
    except Exception as e:
        result['error'] = str(e); result['ok'] = False
    return result

async def run_and_store_net_probe(host_name: str) -> dict:
    host = get_host(host_name)
    if not host: return {'ok': False, 'error': 'host not found'}
    res = await net_probe_for_host(host)
    insert_host_speedtest(host_name=host_name, method='net', ping_ms=res.get('ping_ms'), jitter_ms=res.get('jitter_ms'),
                         download_mbps=res.get('download_mbps'), upload_mbps=res.get('upload_mbps'),
                         server_name=res.get('server_name'), server_id=res.get('server_id'),
                         ok=bool(res.get('ok')), error=res.get('error'))
    return res

async def run_and_store_ssh_speedtest(host_name: str) -> dict:
    host = get_host(host_name)
    if not host: return {'ok': False, 'error': 'host not found'}
    res = await ssh_speedtest_for_host(host)
    insert_host_speedtest(host_name=host_name, method='ssh', ping_ms=res.get('ping_ms'), jitter_ms=res.get('jitter_ms'),
                         download_mbps=res.get('download_mbps'), upload_mbps=res.get('upload_mbps'),
                         server_name=res.get('server_name'), server_id=res.get('server_id'),
                         ok=bool(res.get('ok')), error=res.get('error'))
    return res

async def run_both_for_host(host_name: str) -> dict:
    ok = True; errors = []; out = {'ssh': None, 'net': None}
    try:
        out['ssh'] = await run_and_store_ssh_speedtest(host_name)
        if not out['ssh'].get('ok'): ok = False; errors.append(f"ssh: {out['ssh'].get('error')}")
    except Exception as e: ok = False; errors.append(f'ssh exception: {e}')
    try:
        out['net'] = await run_and_store_net_probe(host_name)
        if not out['net'].get('ok'): ok = False; errors.append(f"net: {out['net'].get('error')}")
    except Exception as e: ok = False; errors.append(f'net exception: {e}')
    return {'ok': ok, 'details': out, 'error': '; '.join(errors) if errors else None}

def _ssh_connect(host_row: dict) -> paramiko.SSHClient:
    ssh_host = (host_row.get('ssh_host') or '').strip()
    ssh_port = int(host_row.get('ssh_port') or 22)
    ssh_user = (host_row.get('ssh_user') or '').strip()
    ssh_password = host_row.get('ssh_password')
    ssh_key_path = (host_row.get('ssh_key_path') or '').strip() or None
    if not ssh_host or not ssh_user: raise RuntimeError('SSH settings are not configured for host')
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    pkey = None
    if ssh_key_path:
        for KeyClass in (paramiko.RSAKey, paramiko.Ed25519Key):
            try: pkey = KeyClass.from_private_key_file(ssh_key_path); break
            except: pkey = None
    ssh.connect(ssh_host, port=ssh_port, username=ssh_user, password=ssh_password, pkey=pkey, timeout=20)
    return ssh

def _ssh_exec(ssh: paramiko.SSHClient, cmd: str, timeout: int = 20) -> tuple[int, str, str]:
    stdin, stdout, stderr = ssh.exec_command(cmd, timeout=timeout)
    out = stdout.read().decode('utf-8', errors='ignore')
    err = stderr.read().decode('utf-8', errors='ignore')
    rc = stdout.channel.recv_exit_status() if hasattr(stdout, 'channel') else 0
    return rc, out, err

async def auto_install_speedtest_on_host(host_name: str) -> dict:
    host = get_host(host_name)
    if not host: return {'ok': False, 'log': 'host not found'}
    def _install():
        log_lines = []
        try:
            ssh = _ssh_connect(host)
        except Exception as e: return {'ok': False, 'log': f'SSH connect failed: {e}'}
        try:
            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Found existing speedtest binary: ' + out.strip())
                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                ver_text = (o2 + e2).strip()
                if ver_text: log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + ver_text).strip())
                need_reinstall = True
                try:
                    if '1.2.0' in ver_text: need_reinstall = False
                except: need_reinstall = True
                if not need_reinstall: return {'ok': True, 'log': '\n'.join(log_lines)}
                else: log_lines.append('Different Ookla speedtest version detected; reinstalling 1.2.0 via tarball.')
            rc, out, _ = _ssh_exec(ssh, 'cat /etc/os-release || uname -a')
            os_release = out.lower()
            log_lines.append('OS detection: ' + out.strip())
            rc, arch_out, _ = _ssh_exec(ssh, 'uname -m || echo unknown')
            arch = (arch_out or '').strip()
            arch_tag = 'linux-x86_64'
            if arch in ('x86_64', 'amd64'): arch_tag = 'linux-x86_64'
            elif arch in ('aarch64', 'arm64'): arch_tag = 'linux-aarch64'
            elif arch in ('armv7l',): arch_tag = 'linux-armhf'
            tar_url = f'https://install.speedtest.net/app/cli/ookla-speedtest-1.2.0-{arch_tag}.tgz'
            cmds_tar = [
                f'curl -fsSL {tar_url} -o /tmp/ookla-speedtest.tgz || wget -O /tmp/ookla-speedtest.tgz {tar_url}',
                'mkdir -p /tmp/ookla-speedtest && tar -xf /tmp/ookla-speedtest.tgz -C /tmp/ookla-speedtest',
                'install -m 0755 /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest || (cp /tmp/ookla-speedtest/speedtest /usr/local/bin/speedtest && chmod +x /usr/local/bin/speedtest)',
                'speedtest --accept-license --accept-gdpr --version || true',
                'rm -rf /tmp/ookla-speedtest /tmp/ookla-speedtest.tgz'
            ]
            for c in cmds_tar:
                rc, o, e = _ssh_exec(ssh, c)
                log_lines.append(f'$ {c}\n{o}{e}'.strip())
            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || echo "NO"')
            if 'NO' not in out:
                rcv, ov, ev = _ssh_exec(ssh, 'speedtest --version 2>&1 || true')
                ver_info = (ov + ev).strip()
                if '1.2.0' in ver_info:
                    log_lines.append('Installed Ookla speedtest via tarball (1.2.0): ' + out.strip())
                    return {'ok': True, 'log': '\n'.join(log_lines)}
                else: log_lines.append('Tarball install finished but version check did not return 1.2.0; continuing fallbacks.')
            cmds_deb = [
                'which sudo || true', 'export DEBIAN_FRONTEND=noninteractive',
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.deb.sh | bash',
                'apt-get update -y || true', 'apt-get install -y speedtest || true'
            ]
            cmds_rpm = [
                'curl -fsSL https://packagecloud.io/install/repositories/ookla/speedtest-cli/script.rpm.sh | bash',
                'yum -y install speedtest || dnf -y install speedtest || true'
            ]
            if 'debian' in os_release or 'ubuntu' in os_release:
                for c in cmds_deb: rc, o, e = _ssh_exec(ssh, c); log_lines.append(f'$ {c}\n{o}{e}'.strip())
            elif any(x in os_release for x in ['centos', 'rhel', 'fedora', 'almalinux', 'rocky']):
                for c in cmds_rpm: rc, o, e = _ssh_exec(ssh, c); log_lines.append(f'$ {c}\n{o}{e}'.strip())
            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'speedtest' in out or 'speedtest-cli' in out:
                log_lines.append('Installed speedtest successfully: ' + out.strip())
                rc2, o2, e2 = _ssh_exec(ssh, 'speedtest --accept-license --accept-gdpr --version || true')
                if o2 or e2: log_lines.append(('$ speedtest --accept-license --accept-gdpr --version\n' + (o2 + e2)).strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}
            pip_try = [
                'command -v python3 || command -v python || echo NO',
                'command -v pip3 || command -v pip || (apt-get update -y && apt-get install -y python3-pip) || (yum -y install python3-pip || dnf -y install python3-pip) || true',
                'pip3 install --upgrade pip || true',
                'pip3 install speedtest-cli || pip install speedtest-cli || true',
                'command -v speedtest-cli || (which python3 && python3 -m pip show speedtest-cli && ln -sf $(python3 -c "import shutil,sys; import os; print(shutil.which(\"speedtest-cli\") or \"/usr/local/bin/speedtest-cli\")") /usr/local/bin/speedtest-cli) || true'
            ]
            for c in pip_try: rc, o, e = _ssh_exec(ssh, c); log_lines.append(f'$ {c}\n{o}{e}'.strip())
            rc, out, err = _ssh_exec(ssh, 'command -v speedtest || command -v speedtest-cli || echo "NO"')
            if 'NO' not in out:
                log_lines.append('Installed speedtest-cli via pip: ' + out.strip())
                return {'ok': True, 'log': '\n'.join(log_lines)}
            return {'ok': False, 'log': 'Failed to install speedtest using available methods.\n' + '\n'.join(log_lines)}
        finally:
            try: ssh.close()
            except: pass
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _install)

# ==================== RESOURCE MONITOR ====================
def _read_proc_meminfo() -> tuple[int | None, int | None]:
    total_kb, avail_kb = None, None
    try:
        with open('/proc/meminfo', 'r') as f:
            for line in f:
                if line.startswith('MemTotal:'): total_kb = int(line.split()[1])
                elif line.startswith('MemAvailable:'): avail_kb = int(line.split()[1])
                if total_kb is not None and avail_kb is not None: break
    except: pass
    return total_kb, avail_kb

def _get_uptime_seconds_fallback() -> float | None:
    try:
        with open('/proc/uptime', 'r') as f: return float(f.read().strip().split()[0])
    except: return None

def get_local_metrics() -> Dict[str, Any]:
    out = {'ok': True, 'source': 'local', 'cpu_percent': None, 'cpu_count': os.cpu_count() or 1,
           'loadavg': None, 'mem_total': None, 'mem_used': None, 'mem_available': None,
           'mem_percent': None, 'disk_total': None, 'disk_used': None, 'disk_free': None,
           'disk_percent': None, 'uptime_seconds': None, 'network_sent': None, 'network_recv': None,
           'network_packets_sent': None, 'network_packets_recv': None, 'error': None}
    try:
        if hasattr(os, 'getloadavg'):
            la = os.getloadavg(); out['loadavg'] = {'1m': la[0], '5m': la[1], '15m': la[2]}
    except: pass
    try:
        disk_path = '/'
        if os.name == 'nt': disk_path = os.environ.get('SystemDrive', 'C:') + '\\'
        du = shutil.disk_usage(disk_path)
        out['disk_total'] = int(du.total); out['disk_used'] = int(du.used); out['disk_free'] = int(du.free)
        out['disk_percent'] = round((du.used / du.total) * 100.0, 2) if du.total else None
    except: pass
    try:
        import psutil
        out['cpu_percent'] = float(psutil.cpu_percent(interval=0.1))
        vm = psutil.virtual_memory()
        out['mem_total'] = int(vm.total); out['mem_used'] = int(vm.used); out['mem_available'] = int(vm.available); out['mem_percent'] = float(vm.percent)
        try:
            net_io = psutil.net_io_counters()
            out['network_sent'] = int(net_io.bytes_sent); out['network_recv'] = int(net_io.bytes_recv)
            out['network_packets_sent'] = int(net_io.packets_sent); out['network_packets_recv'] = int(net_io.packets_recv)
        except: pass
        try: out['uptime_seconds'] = float(time.time() - psutil.boot_time())
        except: out['uptime_seconds'] = _get_uptime_seconds_fallback()
    except:
        total_kb, avail_kb = _read_proc_meminfo()
        if total_kb is not None and avail_kb is not None:
            total = total_kb * 1024; avail = avail_kb * 1024; used = total - avail
            out['mem_total'] = total; out['mem_available'] = avail; out['mem_used'] = used
            out['mem_percent'] = round((used / total) * 100.0, 2) if total else None
        out['cpu_percent'] = None; out['uptime_seconds'] = _get_uptime_seconds_fallback()
        if out['mem_total'] is None: out['ok'] = False; out['error'] = 'psutil not installed and /proc parsing failed'
    return out

def get_host_metrics_via_ssh(host_row: dict) -> Dict[str, Any]:
    res = {'ok': False, 'host_name': host_row.get('host_name'), 'cpu_percent': None, 'cpu_count': None,
           'loadavg': None, 'mem_total': None, 'mem_used': None, 'mem_available': None,
           'mem_percent': None, 'disk_total': None, 'disk_used': None, 'disk_free': None,
           'disk_percent': None, 'uptime_seconds': None, 'error': None}
    try:
        ssh = _ssh_connect(host_row)
    except Exception as e: res['error'] = f'SSH connect failed: {e}'; return res
    try:
        rc, out, _ = _ssh_exec(ssh, 'nproc || getconf _NPROCESSORS_ONLN || echo 1')
        try: res['cpu_count'] = int((out or '1').strip().splitlines()[0])
        except: res['cpu_count'] = 1
        rc, out, _ = _ssh_exec(ssh, 'cat /proc/loadavg || uptime')
        la = None
        try:
            parts = (out or '').strip().split()
            la = {'1m': float(parts[0]), '5m': float(parts[1]), '15m': float(parts[2])}
        except: la = None
        res['loadavg'] = la
        if res['cpu_count']:
            try: cpu_pct = (la.get('1m') / float(res['cpu_count'])) * 100.0 if la and la.get('1m') is not None else None
            except: cpu_pct = None
            if cpu_pct is not None:
                if cpu_pct < 0: cpu_pct = 0.0
                res['cpu_percent'] = round(min(cpu_pct, 100.0), 2)
        rc, out, _ = _ssh_exec(ssh, "grep -E 'MemTotal:|MemAvailable:' /proc/meminfo || cat /proc/meminfo")
        total_kb, avail_kb = None, None
        try:
            for line in out.splitlines():
                if line.startswith('MemTotal:'): total_kb = int(line.split()[1])
                elif line.startswith('MemAvailable:'): avail_kb = int(line.split()[1])
        except: pass
        if total_kb is not None and avail_kb is not None:
            total = total_kb * 1024; avail = avail_kb * 1024; used = total - avail
            res['mem_total'] = total; res['mem_available'] = avail; res['mem_used'] = used
            res['mem_percent'] = round((used / total) * 100.0, 2) if total else None
        rc, out, _ = _ssh_exec(ssh, "LC_ALL=C df -P -B1 / | tail -n 1")
        try:
            parts = out.strip().split()
            if len(parts) >= 5:
                total = int(parts[1]); used = int(parts[2]); avail = int(parts[3])
                res['disk_total'] = total; res['disk_used'] = used; res['disk_free'] = avail
                res['disk_percent'] = round((used / total) * 100.0, 2) if total else None
        except: pass
        rc, out, _ = _ssh_exec(ssh, 'cat /proc/uptime || uptime -s')
        up = None
        try: up = float(out.strip().split()[0])
        except:
            try:
                rc, out2, _ = _ssh_exec(ssh, 'uptime -s')
                boot_str = (out2 or '').strip()
                if boot_str:
                    try: boot_dt = datetime.fromisoformat(boot_str); up = (datetime.now() - boot_dt).total_seconds()
                    except: up = None
            except: up = None
        res['uptime_seconds'] = up
        res['ok'] = True
    except Exception as e: res['ok'] = False; res['error'] = str(e)
    finally:
        try: ssh.close()
        except: pass
    return res

def collect_hosts_metrics() -> Dict[str, Any]:
    items = []
    try: hosts = get_all_hosts()
    except Exception as e: return {'ok': False, 'items': [], 'error': f'get_all_hosts failed: {e}'}
    for h in hosts:
        if h.get('ssh_host') and h.get('ssh_user'):
            try: m = get_host_metrics_via_ssh(h)
            except Exception as e: m = {'ok': False, 'host_name': h.get('host_name'), 'error': str(e)}
        else: m = {'ok': False, 'host_name': h.get('host_name'), 'host_url': h.get('host_url'), 'error': 'SSH не настроен', 'cpu_percent': None, 'mem_percent': None, 'disk_percent': None, 'uptime_seconds': None}
        items.append(m)
    return {'ok': True, 'items': items}

# ==================== SCHEDULER ====================
CHECK_INTERVAL_SECONDS = 300
NOTIFY_BEFORE_HOURS = {72, 48, 24, 1}
notified_users = {}
SPEEDTEST_INTERVAL_SECONDS = 8 * 3600
_last_speedtests_run_at = None
_last_backup_run_at = None
METRICS_INTERVAL_SECONDS = 5 * 60
_last_metrics_run_at = None

def format_time_left(hours: int) -> str:
    if hours >= 24:
        days = hours // 24
        if days % 10 == 1 and days % 100 != 11: return f"{days} день"
        elif 2 <= days % 10 <= 4 and (days % 100 < 10 or days % 100 >= 20): return f"{days} дня"
        else: return f"{days} дней"
    else:
        if hours % 10 == 1 and hours % 100 != 11: return f"{hours} час"
        elif 2 <= hours % 10 <= 4 and (hours % 100 < 10 or hours % 100 >= 20): return f"{hours} часа"
        else: return f"{hours} часов"

async def send_subscription_notification(bot: Bot, user_id: int, key_id: int, time_left_hours: int, expiry_date: datetime):
    try:
        time_text = format_time_left(time_left_hours)
        expiry_str = expiry_date.strftime('%d.%m.%Y в %H:%M')
        message = f"⚠️ **Внимание!** ⚠️\n\nСрок действия вашей подписки истекает через **{time_text}**.\nДата окончания: **{expiry_str}**\n\nПродлите подписку, чтобы не остаться без доступа к VPN!"
        builder = InlineKeyboardBuilder()
        builder.button(text="🔑 Мои ключи", callback_data="manage_keys")
        builder.button(text="➕ Продлить ключ", callback_data=f"extend_key_{key_id}")
        builder.adjust(2)
        await bot.send_message(chat_id=user_id, text=message, reply_markup=builder.as_markup(), parse_mode='Markdown')
        logger.debug(f"Scheduler: Отправлено уведомление пользователю {user_id} по ключу {key_id} (осталось {time_left_hours} ч).")
    except Exception as e: logger.error(f"Scheduler: Ошибка отправки уведомления пользователю {user_id}: {e}")

def _cleanup_notified_users(all_db_keys: list[dict]):
    if not notified_users: return
    active_key_ids = {key['key_id'] for key in all_db_keys}
    for user_id in list(notified_users.keys()):
        for key_id in list(notified_users[user_id].keys()):
            if key_id not in active_key_ids: del notified_users[user_id][key_id]
        if not notified_users[user_id]: del notified_users[user_id]

async def check_expiring_subscriptions(bot: Bot):
    current_time = datetime.now()
    all_keys = get_all_keys()
    _cleanup_notified_users(all_keys)
    for key in all_keys:
        try:
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            time_left = expiry_date - current_time
            if time_left.total_seconds() < 0: continue
            total_hours_left = int(time_left.total_seconds() / 3600)
            user_id = key['user_id']; key_id = key['key_id']
            for hours_mark in NOTIFY_BEFORE_HOURS:
                if hours_mark - 1 < total_hours_left <= hours_mark:
                    notified_users.setdefault(user_id, {}).setdefault(key_id, set())
                    if hours_mark not in notified_users[user_id][key_id]:
                        await send_subscription_notification(bot, user_id, key_id, hours_mark, expiry_date)
                        notified_users[user_id][key_id].add(hours_mark)
                    break
        except Exception as e: logger.error(f"Scheduler: Ошибка обработки истечения для ключа {key.get('key_id')}: {e}")

async def sync_keys_with_panels():
    total_affected_records = 0
    all_hosts = get_all_hosts()
    if not all_hosts: return
    for host in all_hosts:
        host_name = host['host_name']
        try:
            api, inbound = login_to_host(host_url=host['host_url'], username=host['host_username'], password=host['host_pass'], inbound_id=host['host_inbound_id'])
            if not api or not inbound:
                logger.error(f"Scheduler: Не удалось авторизоваться на хосте '{host_name}'.")
                continue
            full_inbound_details = api.inbound.get_by_id(inbound.id)
            clients_on_server = {client.email: client for client in (full_inbound_details.settings.clients or [])}
            keys_in_db = get_keys_for_host(host_name)
            for db_key in keys_in_db:
                key_email = db_key['key_email']
                expiry_date = datetime.fromisoformat(db_key['expiry_date'])
                now = datetime.now()
                if expiry_date < now - timedelta(days=5):
                    try: await delete_client_on_host(host_name, key_email)
                    except Exception as e: logger.error(f"Scheduler: Не удалось удалить клиента '{key_email}' с панели: {e}")
                    deleted = delete_key_by_email(key_email)
                    if deleted: total_affected_records += 1
                    continue
                server_client = clients_on_server.pop(key_email, None)
                if server_client:
                    reset_days = server_client.reset if server_client.reset is not None else 0
                    server_expiry_ms = server_client.expiry_time + reset_days * 24 * 3600 * 1000
                    local_expiry_dt = expiry_date
                    local_expiry_ms = int(local_expiry_dt.timestamp() * 1000)
                    if abs(server_expiry_ms - local_expiry_ms) > 1000:
                        update_key_status_from_server(key_email, server_client)
                        total_affected_records += 1
                else:
                    update_key_status_from_server(key_email, None)
                    total_affected_records += 1
            if clients_on_server:
                for orphan_email, orphan_client in clients_on_server.items():
                    try:
                        m = re.search(r"user(\d+)", orphan_email)
                        user_id = int(m.group(1)) if m else None
                        if not user_id: continue
                        usr = get_user(user_id)
                        if not usr: continue
                        existing = get_key_by_email(orphan_email)
                        if existing: continue
                        reset_days = getattr(orphan_client, 'reset', 0) or 0
                        expiry_ms = int(getattr(orphan_client, 'expiry_time', 0)) + int(reset_days) * 24 * 3600 * 1000
                        client_uuid = getattr(orphan_client, 'id', None) or getattr(orphan_client, 'email', None) or ''
                        if not client_uuid: continue
                        new_id = add_new_key(user_id=user_id, host_name=host_name, xui_client_uuid=str(client_uuid), key_email=orphan_email, expiry_timestamp_ms=expiry_ms)
                        if new_id: total_affected_records += 1
                    except Exception as e: logger.error(f"Scheduler: Ошибка при попытке привязать осиротевшего клиента '{orphan_email}': {e}", exc_info=True)
        except Exception as e: logger.error(f"Scheduler: Непредвиденная ошибка при обработке хоста '{host_name}': {e}", exc_info=True)

async def _maybe_run_periodic_speedtests():
    global _last_speedtests_run_at
    now = datetime.now()
    if _last_speedtests_run_at and (now - _last_speedtests_run_at).total_seconds() < SPEEDTEST_INTERVAL_SECONDS: return
    try:
        hosts = get_all_hosts()
        if hosts:
            for h in hosts:
                host_name = h.get('host_name')
                if host_name:
                    try:
                        async with asyncio.timeout(180): res = await run_both_for_host(host_name)
                    except AttributeError: res = await asyncio.wait_for(run_both_for_host(host_name), timeout=180)
                    if res.get('ok'): logger.info(f"Scheduler: Speedtest для '{host_name}' завершён успешно")
                    else: logger.warning(f"Scheduler: Speedtest для '{host_name}' завершён с ошибками: {res.get('error')}")
        _last_speedtests_run_at = now
    except Exception as e: logger.error(f"Scheduler: Ошибка запуска speedtests: {e}", exc_info=True)

async def _maybe_run_daily_backup(bot: Bot):
    global _last_backup_run_at
    now = datetime.now()
    try: s = get_setting("backup_interval_days") or "1"; days = int(str(s).strip() or "1")
    except: days = 1
    if days <= 0: return
    interval_seconds = max(1, days) * 24 * 3600
    if _last_backup_run_at and (now - _last_backup_run_at).total_seconds() < interval_seconds: return
    try:
        zip_path = create_backup_file()
        if zip_path and zip_path.exists():
            try: sent = await send_backup_to_admins(bot, zip_path); logger.info(f"Scheduler: Создан бэкап {zip_path.name}, отправлен {sent} адм.")
            except Exception as e: logger.error(f"Scheduler: Не удалось отправить бэкап: {e}")
            try: cleanup_old_backups(keep=7)
            except: pass
        _last_backup_run_at = now
    except Exception as e: logger.error(f"Scheduler: Критическая ошибка при создании и отправке бэкапа: {e}", exc_info=True)

async def _maybe_collect_host_metrics():
    global _last_metrics_run_at
    now = datetime.now()
    if _last_metrics_run_at and (now - _last_metrics_run_at).total_seconds() < METRICS_INTERVAL_SECONDS: return
    try:
        local_metrics = await asyncio.wait_for(asyncio.to_thread(get_local_metrics), timeout=10)
        if local_metrics and local_metrics.get('ok'):
            insert_resource_metric('local', 'panel', cpu_percent=local_metrics.get('cpu_percent'), mem_percent=local_metrics.get('mem_percent'), disk_percent=local_metrics.get('disk_percent'), load1=local_metrics.get('loadavg', {}).get('1m') if local_metrics.get('loadavg') else None, net_bytes_sent=local_metrics.get('network_sent'), net_bytes_recv=local_metrics.get('network_recv'), raw_json=json.dumps(local_metrics, ensure_ascii=False))
    except Exception as e: logger.error(f"Scheduler: Ошибка сбора локальных метрик: {e}")
    hosts = get_all_hosts()
    if hosts:
        for h in hosts:
            host_name = h.get('host_name')
            if not host_name or not (h.get('ssh_host') and h.get('ssh_user')): continue
            try:
                m = await asyncio.wait_for(asyncio.to_thread(get_host_metrics_via_ssh, h), timeout=30)
                insert_host_metrics(host_name, m)
                if m and m.get('ok'):
                    insert_resource_metric('host', host_name, cpu_percent=m.get('cpu_percent'), mem_percent=m.get('mem_percent'), disk_percent=m.get('disk_percent'), load1=m.get('loadavg', {}).get('1m') if m.get('loadavg') else None, raw_json=json.dumps(m, ensure_ascii=False))
            except asyncio.TimeoutError: logger.warning(f"Scheduler: Таймаут сбора метрик для хоста '{host_name}'")
            except Exception as e: logger.error(f"Scheduler: Ошибка сбора метрик для '{host_name}': {e}")
    _last_metrics_run_at = now

async def periodic_subscription_check(bot_controller):
    logger.info("Scheduler: Планировщик фоновых задач запущен.")
    await asyncio.sleep(10)
    while True:
        try:
            await sync_keys_with_panels()
            await _maybe_run_periodic_speedtests()
            await _maybe_collect_host_metrics()
            bot = bot_controller.get_bot_instance() if bot_controller.get_status().get("is_running") else None
            if bot:
                await _maybe_run_daily_backup(bot)
                if bot_controller.get_status().get("is_running"): await check_expiring_subscriptions(bot)
                else: logger.debug("Scheduler: Бот остановлен, уведомления пользователям пропущены.")
        except Exception as e: logger.error(f"Scheduler: Необработанная ошибка в основном цикле: {e}", exc_info=True)
        logger.info(f"Scheduler: Цикл завершён. Следующая проверка через {CHECK_INTERVAL_SECONDS} сек.")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

# ==================== BOT CONTROLLER ====================
class BotController:
    def __init__(self):
        self._dp = None; self._bot = None; self._task = None; self._is_running = False; self._loop = None
    def set_loop(self, loop: asyncio.AbstractEventLoop):
        self._loop = loop; logger.info("Цикл событий установлен.")
    def get_bot_instance(self) -> Bot | None: return self._bot
    async def _start_polling(self):
        self._is_running = True
        logger.info("Запущен опрос Telegram (Основной-бот).")
        try: await self._dp.start_polling(self._bot)
        except asyncio.CancelledError: logger.info("Опрос остановлен (задача отменена).")
        except Exception as e: logger.error(f"Ошибка во время опроса: {e}", exc_info=True)
        finally:
            logger.info("Опрос корректно остановлен.")
            self._is_running = False; self._task = None
            if self._bot: await self._bot.close()
            self._bot = None; self._dp = None
    def start(self):
        if self._is_running: return {"status": "error", "message": "Бот уже запущен."}
        if not self._loop or not self._loop.is_running(): return {"status": "error", "message": "Критическая ошибка: цикл событий не установлен."}
        token = get_setting("telegram_bot_token")
        bot_username = get_setting("telegram_bot_username")
        admin_id = get_setting("admin_telegram_id")
        if not all([token, bot_username, admin_id]): return {"status": "error", "message": "Невозможно запустить: не все обязательные настройки Telegram заполнены."}
        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()
            self._dp.message.middleware(BanMiddleware())
            self._dp.callback_query.middleware(BanMiddleware())
            user_router = get_user_router()
            admin_router = get_admin_router()
            self._dp.include_router(user_router)
            self._dp.include_router(admin_router)
            try: asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e: logger.warning(f"Не удалось удалить вебхук перед запуском опроса: {e}")
            yookassa_shop_id = get_setting("yookassa_shop_id")
            yookassa_secret_key = get_setting("yookassa_secret_key")
            yookassa_enabled = bool(yookassa_shop_id and yookassa_secret_key)
            cryptobot_token = get_setting("cryptobot_token")
            cryptobot_enabled = bool(cryptobot_token)
            heleket_shop_id = get_setting("heleket_merchant_id")
            heleket_api_key = get_setting("heleket_api_key")
            heleket_enabled = bool(heleket_api_key and heleket_shop_id)
            ton_wallet_address = get_setting("ton_wallet_address")
            tonapi_key = get_setting("tonapi_key")
            tonconnect_enabled = bool(ton_wallet_address and tonapi_key)
            stars_flag = get_setting("stars_enabled")
            stars_enabled = str(stars_flag).lower() in ("true", "1", "yes", "on")
            ym_flag = get_setting("yoomoney_enabled")
            ym_wallet = get_setting("yoomoney_wallet")
            yoomoney_enabled = (str(ym_flag).lower() in ("true", "1", "yes", "on")) and bool(ym_wallet)
            if yookassa_enabled:
                Configuration.account_id = yookassa_shop_id
                Configuration.secret_key = yookassa_secret_key
            global PAYMENT_METHODS, TELEGRAM_BOT_USERNAME, ADMIN_ID
            PAYMENT_METHODS = {"yookassa": yookassa_enabled, "heleket": heleket_enabled, "cryptobot": cryptobot_enabled, "tonconnect": tonconnect_enabled, "stars": stars_enabled, "yoomoney": yoomoney_enabled}
            TELEGRAM_BOT_USERNAME = bot_username
            ADMIN_ID = int(admin_id) if admin_id else None
            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("Команда на запуск передана в цикл событий.")
            return {"status": "success", "message": "Команда на запуск бота отправлена."}
        except Exception as e: logger.error(f"Не удалось запустить бота: {e}", exc_info=True); self._bot = None; self._dp = None; return {"status": "error", "message": f"Ошибка при запуске: {e}"}
    def stop(self):
        if not self._is_running: return {"status": "error", "message": "Бот не запущен."}
        if not self._loop or not self._dp: return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}
        logger.info("Отправляю сигнал на корректную остановку...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        return {"status": "success", "message": "Команда на остановку бота отправлена."}
    def get_status(self): return {"is_running": self._is_running}

# ==================== SUPPORT BOT CONTROLLER ====================
class SupportBotController:
    def __init__(self):
        self._dp: Dispatcher | None = None; self._bot: Bot | None = None; self._task = None; self._is_running = False; self._loop = None
    def set_loop(self, loop: asyncio.AbstractEventLoop): self._loop = loop; logger.info("Цикл событий установлен.")
    def get_bot_instance(self) -> Bot | None: return self._bot
    async def _start_polling(self):
        self._is_running = True; logger.info("Запущен опрос Telegram (Support-бот)...")
        try: await self._dp.start_polling(self._bot)
        except asyncio.CancelledError: logger.info("Опрос остановлен (задача отменена).")
        except Exception as e: logger.error(f"Ошибка во время опроса: {e}", exc_info=True)
        finally:
            logger.info("Опрос корректно остановлен.")
            self._is_running = False; self._task = None
            if self._bot: await self._bot.close()
            self._bot = None; self._dp = None
    def start(self):
        if self._is_running: return {"status": "error", "message": "Support-бот уже запущен."}
        if not self._loop or not self._loop.is_running(): return {"status": "error", "message": "Критическая ошибка: цикл событий не установлен."}
        token = get_setting("support_bot_token")
        bot_username = get_setting("support_bot_username")
        admin_id = get_setting("admin_telegram_id")
        admin_ids = get_admin_ids()
        if not all([token, bot_username]) or (not admin_id and not admin_ids): return {"status": "error", "message": "Невозможно запустить support-бот: заполните support_bot_token, support_bot_username и хотя бы одного администратора."}
        try:
            self._bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
            self._dp = Dispatcher()
            self._dp.message.middleware(BanMiddleware())
            self._dp.callback_query.middleware(BanMiddleware())
            router = get_support_router()
            self._dp.include_router(router)
            try: asyncio.run_coroutine_threadsafe(self._bot.delete_webhook(drop_pending_updates=True), self._loop)
            except Exception as e: logger.warning(f"Не удалось удалить вебхук перед запуском опроса: {e}")
            self._task = asyncio.run_coroutine_threadsafe(self._start_polling(), self._loop)
            logger.info("Команда на запуск передана в цикл событий.")
            return {"status": "success", "message": "Команда на запуск support-бота отправлена."}
        except Exception as e: logger.error(f"Ошибка запуска support-бота: {e}", exc_info=True); self._bot = None; self._dp = None; return {"status": "error", "message": f"Ошибка при запуске support-бота: {e}"}
    def stop(self):
        if not self._is_running: return {"status": "error", "message": "Support-бот не запущен."}
        if not self._loop or not self._dp: return {"status": "error", "message": "Критическая ошибка: компоненты бота недоступны."}
        logger.info("Отправляю сигнал на корректную остановку...")
        asyncio.run_coroutine_threadsafe(self._dp.stop_polling(), self._loop)
        return {"status": "success", "message": "Команда на остановку support-бота отправлена."}
    def get_status(self): return {"is_running": self._is_running}

# ==================== SUPPORT BOT ROUTER ====================
class SupportDialog(StatesGroup):
    waiting_for_subject = State()
    waiting_for_message = State()
    waiting_for_reply = State()

class AdminDialog(StatesGroup):
    waiting_for_note = State()

def get_support_router() -> Router:
    router = Router()
    def _user_main_reply_kb() -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✍️ Новое обращение")], [KeyboardButton(text="📨 Мои обращения")]], resize_keyboard=True)
    def _get_latest_open_ticket(user_id: int) -> dict | None:
        try:
            tickets = get_user_tickets(user_id) or []
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets: return None
            return max(open_tickets, key=lambda t: int(t['ticket_id']))
        except: return None
    def _admin_actions_kb(ticket_id: int) -> InlineKeyboardMarkup:
        try: t = get_ticket(ticket_id); status = (t and t.get('status')) or 'open'
        except: status = 'open'
        user_id = None; is_banned = False
        if t and t.get('user_id') is not None:
            try:
                user_id = int(t.get('user_id'))
                user_data = get_user(user_id) or {}
                is_banned = bool(user_data.get('is_banned'))
            except: pass
        first_row = []
        if status == 'open': first_row.append(InlineKeyboardButton(text="✅ Закрыть", callback_data=f"admin_close_{ticket_id}"))
        else: first_row.append(InlineKeyboardButton(text="🔓 Переоткрыть", callback_data=f"admin_reopen_{ticket_id}"))
        inline_kb = [first_row, [InlineKeyboardButton(text="🗑 Удалить", callback_data=f"admin_delete_{ticket_id}")],
                    [InlineKeyboardButton(text="⭐ Важно", callback_data=f"admin_star_{ticket_id}"),
                     InlineKeyboardButton(text="👤 Пользователь", callback_data=f"admin_user_{ticket_id}"),
                     InlineKeyboardButton(text="📝 Заметка", callback_data=f"admin_note_{ticket_id}")],
                    [InlineKeyboardButton(text="🗒 Заметки", callback_data=f"admin_notes_{ticket_id}")]]
        if user_id is not None:
            toggle_label = "✅ Разбанить пользователя" if is_banned else "🚫 Забанить пользователя"
            inline_kb.append([InlineKeyboardButton(text=toggle_label, callback_data=f"admin_toggle_ban_{ticket_id}")])
        return InlineKeyboardMarkup(inline_keyboard=inline_kb)
    async def _is_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
        is_admin_by_setting = is_admin(user_id); is_admin_in_chat = False
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
        except: pass
        return bool(is_admin_by_setting or is_admin_in_chat)
    @router.message(CommandStart(), F.chat.type == "private")
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot):
        args = (message.text or "").split(maxsplit=1)
        arg = None
        if len(args) > 1: arg = args[1].strip()
        if arg == "new":
            existing = _get_latest_open_ticket(message.from_user.id)
            if existing: await message.answer(f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём. Новый тикет можно создать после его закрытия.")
            else: await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')"); await state.set_state(SupportDialog.waiting_for_subject)
            return
        support_text = get_setting("support_text") or "Раздел поддержки. Вы можете создать обращение или открыть существующее."
        await message.answer(support_text, reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="✍️ Новое обращение")], [KeyboardButton(text="📨 Мои обращения")]], resize_keyboard=True))
    @router.callback_query(F.data == "support_new_ticket")
    async def support_new_ticket_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        existing = _get_latest_open_ticket(callback.from_user.id)
        if existing: await callback.message.edit_text(f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём. Новый тикет можно создать после закрытия текущего.")
        else: await callback.message.edit_text("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')"); await state.set_state(SupportDialog.waiting_for_subject)
    @router.message(SupportDialog.waiting_for_subject, F.chat.type == "private")
    async def support_subject_received(message: types.Message, state: FSMContext):
        subject = (message.text or "").strip()
        await state.update_data(subject=subject)
        await message.answer("✉️ Опишите проблему максимально подробно одним сообщением.")
        await state.set_state(SupportDialog.waiting_for_message)
    @router.message(SupportDialog.waiting_for_message, F.chat.type == "private")
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        user_id = message.from_user.id
        data = await state.get_data()
        raw_subject = (data.get("subject") or "").strip()
        subject = raw_subject if raw_subject else "Обращение без темы"
        existing = _get_latest_open_ticket(user_id)
        created_new = False
        if existing:
            ticket_id = int(existing['ticket_id'])
            add_support_message(ticket_id, sender="user", content=(message.text or message.caption or ""))
            ticket = get_ticket(ticket_id)
        else:
            ticket_id = create_support_ticket(user_id, subject)
            if not ticket_id: await message.answer("❌ Не удалось создать обращение. Попробуйте позже."); await state.clear(); return
            add_support_message(ticket_id, sender="user", content=(message.text or message.caption or ""))
            ticket = get_ticket(ticket_id)
            created_new = True
        support_forum_chat_id = get_setting("support_forum_chat_id")
        thread_id = None
        if support_forum_chat_id and not (ticket and ticket.get('message_thread_id')):
            try:
                chat_id = int(support_forum_chat_id)
                author_tag = (message.from_user.username and f"@{message.from_user.username}") or (message.from_user.full_name if message.from_user else None) or str(user_id)
                subj_full = (subject or 'Обращение без темы')
                is_star = subj_full.strip().startswith('⭐')
                display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                trimmed_subject = display_subj[:40]
                important_prefix = '🔴 Важно: ' if is_star else ''
                topic_name = f"#{ticket_id} {important_prefix}{trimmed_subject} • от {author_tag}"
                forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                thread_id = forum_topic.message_thread_id
                update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                subj_display = (subject or '—')
                header = f"🆘 Новое обращение\nТикет: #{ticket_id}\nПользователь: @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\nТема: {subj_display} — от @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\n\nСообщение:\n{message.text or ''}"
                await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
            except Exception as e: logger.warning(f"Не удалось создать форумную тему или отправить сообщение для тикета {ticket_id}: {e}")
        try:
            ticket = get_ticket(ticket_id)
            forum_chat_id = ticket and ticket.get('forum_chat_id')
            thread_id = ticket and ticket.get('message_thread_id')
            if forum_chat_id and thread_id:
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(chat_id=int(forum_chat_id), text=f"🆕 Новое обращение от {username} (ID: {message.from_user.id}) по тикету #{ticket_id}:" if created_new else f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):", message_thread_id=int(thread_id))
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e: logger.warning(f"Не удалось отзеркалить сообщение пользователя в форум: {e}")
        await state.clear()
        if created_new: await message.answer(f"✅ Обращение создано: #{ticket_id}. Мы ответим вам как можно скорее.", reply_markup=_user_main_reply_kb())
        else: await message.answer(f"✉️ Сообщение добавлено в ваш открытый тикет #{ticket_id}.", reply_markup=_user_main_reply_kb())
        try:
            for aid in get_admin_ids():
                try: await bot.send_message(int(aid), f"🆘 Новое обращение в поддержку\nID тикета: #{ticket_id}\nОт пользователя: @{message.from_user.username or message.from_user.full_name} (ID: {user_id})\nТема: {subject or '—'}\n\nСообщение:\n{message.text or ''}")
                except: pass
        except Exception as e: logger.warning(f"Не удалось уведомить администраторов о тикете {ticket_id}: {e}")
    @router.callback_query(F.data == "support_my_tickets")
    async def support_my_tickets_handler(callback: types.CallbackQuery):
        await callback.answer()
        tickets = get_user_tickets(callback.from_user.id)
        text = "Ваши обращения:" if tickets else "У вас пока нет обращений."
        rows = []
        if tickets:
            for t in tickets:
                status_text = "🟢 Открыт" if t.get('status') == 'open' else "🔒 Закрыт"
                is_star = (t.get('subject') or '').startswith('⭐ ')
                star = '⭐ ' if is_star else ''
                title = f"{star}#{t['ticket_id']} • {status_text}"
                if t.get('subject'): title += f" • {t['subject'][:20]}"
                rows.append([InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        await callback.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    @router.callback_query(F.data.startswith("support_view_"))
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id: await callback.message.edit_text("Тикет не найден или доступ запрещён."); return
        messages = get_ticket_messages(ticket_id)
        human_status = "🟢 Открыт" if ticket.get('status') == 'open' else "🔒 Закрыт"
        is_star = (ticket.get('subject') or '').startswith('⭐ ')
        star_line = "⭐ Важно" if is_star else "—"
        parts = [f"🧾 Тикет #{ticket_id} — статус: {human_status}", f"Тема: {ticket.get('subject') or '—'}", f"Важность: {star_line}", ""]
        for m in messages:
            if m.get('sender') == 'note': continue
            who = "Вы" if m.get('sender') == 'user' else 'Поддержка'
            created = m.get('created_at')
            parts.append(f"{who} ({created}):\n{m.get('content','')}\n")
        final_text = "\n".join(parts)
        is_open = (ticket.get('status') == 'open')
        buttons = []
        if is_open:
            buttons.append([InlineKeyboardButton(text="💬 Ответить", callback_data=f"support_reply_{ticket_id}")])
            buttons.append([InlineKeyboardButton(text="✅ Закрыть", callback_data=f"support_close_{ticket_id}")])
        buttons.append([InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")])
        await callback.message.edit_text(final_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    @router.callback_query(F.data.startswith("support_reply_"))
    async def support_reply_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id or ticket.get('status') != 'open': await callback.message.edit_text("Нельзя ответить на этот тикет."); return
        await state.update_data(reply_ticket_id=ticket_id)
        await callback.message.edit_text("Напишите ваш ответ одним сообщением.")
        await state.set_state(SupportDialog.waiting_for_reply)
    @router.message(SupportDialog.waiting_for_reply, F.chat.type == "private")
    async def support_reply_received(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        ticket_id = data.get('reply_ticket_id')
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != message.from_user.id or ticket.get('status') != 'open': await message.answer("Нельзя ответить на этот тикет."); await state.clear(); return
        add_support_message(ticket_id, sender='user', content=(message.text or message.caption or ''))
        await state.clear()
        await message.answer("Сообщение отправлено.")
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        subj_full = (ticket.get('subject') or 'Обращение без темы')
                        is_star = subj_full.strip().startswith('⭐')
                        display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                        trimmed_subject = display_subj[:40]
                        author_tag = (message.from_user.username and f"@{message.from_user.username}") or (message.from_user.full_name if message.from_user else None) or str(message.from_user.id)
                        important_prefix = '🔴 Важно: ' if is_star else ''
                        topic_name = f"#{ticket_id} {important_prefix}{trimmed_subject} • от {author_tag}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        subj_display = (ticket.get('subject') or '—')
                        header = f"📌 Тред создан автоматически\nТикет: #{ticket_id}\nПользователь: ID {ticket.get('user_id')}\nТема: {subj_display} — от ID {ticket.get('user_id')}"
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                    except Exception as e: logger.warning(f"Не удалось автоматически создать форумную тему для тикета {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                try:
                    subj_full = (ticket.get('subject') or 'Обращение без темы')
                    is_star = subj_full.strip().startswith('⭐')
                    display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                    trimmed = display_subj[:40]
                    author_tag = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                    important_prefix = '🔴 Важно: ' if is_star else ''
                    topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                    await bot.edit_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), name=topic_name)
                except Exception as e: logger.warning(f"Не удалось переименовать существующую тему для тикета {ticket_id}: {e}")
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(chat_id=int(forum_chat_id), text=f"✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):", message_thread_id=int(thread_id))
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e: logger.warning(f"Не удалось отзеркалить ответ пользователя в форум: {e}")
        admin_id = get_setting("admin_telegram_id")
        if admin_id:
            try: await bot.send_message(int(admin_id), f"📩 Новое сообщение в тикете\nID тикета: #{ticket_id}\nОт пользователя: @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})\n\nСообщение:\n{message.text or ''}")
            except Exception as e: logger.warning(f"Не удалось уведомить администратора о сообщении тикета #{ticket_id}: {e}")
    @router.message(F.is_topic_message == True)
    async def forum_thread_message_handler(message: types.Message, bot: Bot, state: FSMContext):
        try:
            if not message.message_thread_id: return
            forum_chat_id = message.chat.id
            thread_id = message.message_thread_id
            ticket = get_ticket_by_thread(str(forum_chat_id), int(thread_id))
            if not ticket: return
            user_id = int(ticket.get('user_id'))
            try:
                current_state = await state.get_state()
                if current_state == AdminDialog.waiting_for_note.state:
                    note_body = (message.text or message.caption or '').strip()
                    author_id = message.from_user.id if message.from_user else None
                    if author_id:
                        username = None
                        if message.from_user.username: username = f"@{message.from_user.username}"
                        else: username = message.from_user.full_name or str(author_id)
                        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}"
                    else: note_text = note_body
                    add_support_message(int(ticket['ticket_id']), sender='note', content=note_text)
                    await message.answer("📝 Внутренняя заметка сохранена.")
                    await state.clear()
                    return
            except: pass
            me = await bot.get_me()
            if message.from_user and message.from_user.id == me.id: return
            is_admin_by_setting = is_admin(message.from_user.id); is_admin_in_chat = False
            try:
                member = await bot.get_chat_member(chat_id=forum_chat_id, user_id=message.from_user.id)
                is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
            except: pass
            if not (is_admin_by_setting or is_admin_in_chat): return
            content = (message.text or message.caption or "").strip()
            if content: add_support_message(ticket_id=int(ticket['ticket_id']), sender='admin', content=content)
            header = await bot.send_message(chat_id=user_id, text=f"💬 Ответ поддержки по тикету #{ticket['ticket_id']}")
            try: await bot.copy_message(chat_id=user_id, from_chat_id=message.chat.id, message_id=message.message_id, reply_to_message_id=header.message_id)
            except:
                if content: await bot.send_message(chat_id=user_id, text=content)
        except Exception as e: logger.warning(f"Не удалось переслать сообщение из форумной темы: {e}")
    @router.callback_query(F.data.startswith("support_close_"))
    async def support_close_ticket_handler(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        ticket_id = int(callback.data.split("_")[-1])
        ticket = get_ticket(ticket_id)
        if not ticket or ticket.get('user_id') != callback.from_user.id: await callback.message.edit_text("Тикет не найден или доступ запрещён."); return
        if ticket.get('status') == 'closed': await callback.message.edit_text("Тикет уже закрыт."); return
        ok = set_ticket_status(ticket_id, 'closed')
        if ok:
            try:
                forum_chat_id = ticket.get('forum_chat_id')
                thread_id = ticket.get('message_thread_id')
                if forum_chat_id and thread_id:
                    try:
                        username = (callback.from_user.username and f"@{callback.from_user.username}") or callback.from_user.full_name or str(callback.from_user.id)
                        await bot.send_message(chat_id=int(forum_chat_id), text=f"✅ Пользователь {username} закрыл тикет #{ticket_id}.", message_thread_id=int(thread_id))
                        await bot.send_message(chat_id=int(forum_chat_id), text="Панель управления тикетом:", message_thread_id=int(thread_id), reply_markup=_admin_actions_kb(ticket_id))
                    except: pass
                await bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id))
            except Exception as e: logger.warning(f"Не удалось закрыть форумную тему для тикета {ticket_id} из бота: {e}")
            await callback.message.edit_text("✅ Тикет закрыт.", reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="⬅️ К списку", callback_data="support_my_tickets")]]))
            try: await callback.message.answer("Меню поддержки:", reply_markup=_user_main_reply_kb())
            except: pass
        else: await callback.message.edit_text("❌ Не удалось закрыть тикет.")
    @router.callback_query(F.data.startswith("admin_close_"))
    async def admin_close_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: await callback.message.edit_text("Тикет не найден."); return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        if set_ticket_status(ticket_id, 'closed'):
            try:
                thread_id = ticket.get('message_thread_id')
                if thread_id: await bot.close_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
            except: pass
            try: await callback.message.edit_text(f"✅ Тикет #{ticket_id} закрыт.", reply_markup=_admin_actions_kb(ticket_id))
            except TelegramBadRequest as e:
                if "message is not modified" in str(e): await callback.answer("Без изменений", show_alert=False)
                else: raise
            try:
                user_id = int(ticket.get('user_id'))
                await bot.send_message(chat_id=user_id, text=f"✅ Ваш тикет #{ticket_id} был закрыт администратором. Спасибо за обращение!")
            except: pass
        else: await callback.message.answer("❌ Не удалось закрыть тикет.")
    @router.callback_query(F.data.startswith("admin_reopen_"))
    async def admin_reopen_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: await callback.message.edit_text("Тикет не найден."); return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        if set_ticket_status(ticket_id, 'open'):
            try:
                thread_id = ticket.get('message_thread_id')
                if thread_id: await bot.reopen_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
            except: pass
            try: await callback.message.edit_text(f"🔓 Тикет #{ticket_id} переоткрыт.", reply_markup=_admin_actions_kb(ticket_id))
            except TelegramBadRequest as e:
                if "message is not modified" in str(e): await callback.answer("Без изменений", show_alert=False)
                else: raise
            try:
                user_id = int(ticket.get('user_id'))
                await bot.send_message(chat_id=user_id, text=f"🔓 Ваш тикет #{ticket_id} был переоткрыт администратором. Вы можете продолжить переписку.")
            except: pass
        else: await callback.message.answer("❌ Не удалось переоткрыть тикет.")
    @router.callback_query(F.data.startswith("admin_delete_"))
    async def admin_delete_ticket(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: await callback.message.edit_text("Тикет уже удалён или не найден."); return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        try:
            thread_id = ticket.get('message_thread_id')
            if thread_id: await bot.delete_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
        except:
            try:
                if thread_id: await bot.close_forum_topic(chat_id=forum_chat_id, message_thread_id=int(thread_id))
            except: pass
        if delete_ticket(ticket_id):
            try: await callback.message.edit_text(f"🗑 Тикет #{ticket_id} удалён.")
            except TelegramBadRequest as e:
                if "message to edit not found" in str(e) or "message is not modified" in str(e): await callback.message.answer(f"🗑 Тикет #{ticket_id} удалён.")
                else: raise
        else: await callback.message.answer("❌ Не удалось удалить тикет.")
    @router.callback_query(F.data.startswith("admin_star_"))
    async def admin_toggle_star(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        subject = (ticket.get('subject') or '').strip()
        is_starred = subject.startswith("⭐ ")
        if is_starred:
            base_subject = subject[2:].strip()
            new_subject = base_subject if base_subject else "Обращение без темы"
        else:
            base_subject = subject if subject else "Обращение без темы"
            new_subject = f"⭐ {base_subject}"
        if update_ticket_subject(ticket_id, new_subject):
            try:
                thread_id = ticket.get('message_thread_id')
                if thread_id and ticket.get('forum_chat_id'):
                    user_id = int(ticket.get('user_id')) if ticket.get('user_id') else None
                    author_tag = None
                    if user_id:
                        try:
                            user = await bot.get_chat(user_id)
                            username = getattr(user, 'username', None)
                            author_tag = f"@{username}" if username else f"ID {user_id}"
                        except: author_tag = f"ID {user_id}"
                    else: author_tag = "пользователь"
                    subj_full = (new_subject or 'Обращение без темы')
                    is_star2 = subj_full.strip().startswith('⭐')
                    display_subj2 = (subj_full.lstrip('⭐️ ').strip() if is_star2 else subj_full)
                    trimmed = display_subj2[:40]
                    important_prefix2 = '🔴 Важно: ' if is_star2 else ''
                    topic_name = f"#{ticket_id} {important_prefix2}{trimmed} • от {author_tag}"
                    await bot.edit_forum_topic(chat_id=int(ticket['forum_chat_id']), message_thread_id=int(thread_id), name=topic_name)
            except: pass
            try:
                thread_id = ticket.get('message_thread_id')
                forum_chat_id = ticket.get('forum_chat_id')
                if thread_id and forum_chat_id:
                    state_text = "включена" if not is_starred else "снята"
                    msg = await bot.send_message(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), text=f"⭐ Важность {state_text} для тикета #{ticket_id}.")
                    if not is_starred:
                        try: await bot.pin_chat_message(chat_id=int(forum_chat_id), message_id=msg.message_id, disable_notification=True)
                        except: pass
                    else:
                        try: await bot.unpin_all_forum_topic_messages(chat_id=int(forum_chat_id), message_thread_id=int(thread_id))
                        except: pass
            except: pass
            state_text = "включена" if not is_starred else "снята"
            await callback.message.answer(f"⭐ Пометка важности {state_text}. Название темы обновлено.")
        else: await callback.message.answer("❌ Не удалось обновить тему тикета.")
    @router.callback_query(F.data.startswith("admin_user_"))
    async def admin_show_user(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        user_id = int(ticket.get('user_id'))
        mention_link = f"tg://user?id={user_id}"
        username = None
        try: user = await bot.get_chat(user_id); username = getattr(user, 'username', None)
        except: pass
        text = f"👤 Пользователь тикета\nID: `{user_id}`\nUsername: @{username}\n" if username else "" + f"Ссылка: {mention_link}"
        await callback.message.answer(text, parse_mode="Markdown")
    @router.callback_query(F.data.startswith("admin_toggle_ban_"))
    async def admin_toggle_ban(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: await callback.message.answer("Тикет не найден."); return
        forum_chat_id_raw = ticket.get('forum_chat_id')
        forum_chat_id = int(forum_chat_id_raw) if forum_chat_id_raw else callback.message.chat.id
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        user_id_raw = ticket.get('user_id')
        if not user_id_raw: await callback.message.answer("❌ Не удалось определить пользователя тикета."); return
        try: user_id = int(user_id_raw)
        except: await callback.message.answer("❌ Некорректный идентификатор пользователя."); return
        try: user_data = get_user(user_id) or {}; currently_banned = bool(user_data.get('is_banned'))
        except: currently_banned = False
        try:
            if currently_banned: unban_user(user_id)
            else: ban_user(user_id)
        except Exception as e: await callback.message.answer(f"❌ Не удалось обновить статус блокировки: {e}"); return
        if currently_banned:
            try: await bot.send_message(user_id, "✅ Ваш аккаунт разблокирован администратором. Вы снова можете пользоваться сервисом.")
            except: pass
        else:
            support_contact = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
            ban_message = "🚫 Ваш аккаунт заблокирован администратором."
            if support_contact: ban_message += f"\nЕсли это ошибка, свяжитесь с поддержкой: {support_contact}"
            try: await bot.send_message(user_id, ban_message)
            except: pass
        try: await callback.message.edit_reply_markup(reply_markup=_admin_actions_kb(ticket_id))
        except: pass
        await callback.message.answer(f"{'✅' if currently_banned else '🚫'} Пользователь {user_id} {'разбанен' if currently_banned else 'забанен'}.")
    @router.callback_query(F.data.startswith("admin_note_"))
    async def admin_note_prompt(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        await state.update_data(note_ticket_id=ticket_id)
        await callback.message.answer("📝 Отправьте внутреннюю заметку одним сообщением. Она не будет отправлена пользователю.")
        await state.set_state(AdminDialog.waiting_for_note)
    @router.callback_query(F.data.startswith("admin_notes_"))
    async def admin_list_notes(callback: types.CallbackQuery, bot: Bot):
        await callback.answer()
        try: ticket_id = int(callback.data.split("_")[-1])
        except: return
        ticket = get_ticket(ticket_id)
        if not ticket: return
        forum_chat_id = int(ticket.get('forum_chat_id') or callback.message.chat.id)
        if not await _is_admin(bot, forum_chat_id, callback.from_user.id): return
        notes = [m for m in get_ticket_messages(ticket_id) if m.get('sender') == 'note']
        if not notes: await callback.message.answer("🗒 Внутренних заметок пока нет."); return
        lines = [f"🗒 Заметки по тикету #{ticket_id}:"]
        for m in notes:
            created = m.get('created_at')
            content = (m.get('content') or '').strip()
            lines.append(f"— ({created})\n{content}")
        text = "\n\n".join(lines)
        await callback.message.answer(text)
    @router.message(AdminDialog.waiting_for_note, F.is_topic_message == True)
    async def admin_note_receive(message: types.Message, state: FSMContext):
        data = await state.get_data()
        ticket_id = data.get('note_ticket_id')
        if not ticket_id: await message.answer("❌ Не найден контекст тикета для заметки."); await state.clear(); return
        author_id = message.from_user.id if message.from_user else None
        username = None
        if message.from_user:
            if message.from_user.username: username = f"@{message.from_user.username}"
            else: username = message.from_user.full_name or str(author_id)
        note_body = (message.text or message.caption or '').strip()
        note_text = f"[Заметка от {username} (ID: {author_id})]\n{note_body}" if author_id else note_body
        add_support_message(int(ticket_id), sender='note', content=note_text)
        await message.answer("📝 Внутренняя заметка сохранена.")
        await state.clear()
    @router.message(F.text == "▶️ Начать", F.chat.type == "private")
    async def start_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing: await message.answer(f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём.")
        else: await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')"); await state.set_state(SupportDialog.waiting_for_subject)
    @router.message(F.text == "✍️ Новое обращение", F.chat.type == "private")
    async def new_ticket_text_button(message: types.Message, state: FSMContext):
        existing = _get_latest_open_ticket(message.from_user.id)
        if existing: await message.answer(f"У вас уже есть открытый тикет #{existing['ticket_id']}. Продолжайте переписку в нём.")
        else: await message.answer("📝 Кратко опишите тему обращения (например, 'Проблема с подключением')"); await state.set_state(SupportDialog.waiting_for_subject)
    @router.message(F.text == "📨 Мои обращения", F.chat.type == "private")
    async def my_tickets_text_button(message: types.Message):
        tickets = get_user_tickets(message.from_user.id)
        text = "Ваши обращения:" if tickets else "У вас пока нет обращений."
        rows = []
        if tickets:
            for t in tickets:
                status_text = "🟢 Открыт" if t.get('status') == 'open' else "🔒 Закрыт"
                title = f"#{t['ticket_id']} • {status_text}"
                if t.get('subject'): title += f" • {t['subject'][:20]}"
                rows.append([InlineKeyboardButton(text=title, callback_data=f"support_view_{t['ticket_id']}")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    @router.message(F.chat.type == "private")
    async def relay_user_message_to_forum(message: types.Message, bot: Bot, state: FSMContext):
        current_state = await state.get_state()
        if current_state is not None: return
        user_id = message.from_user.id if message.from_user else None
        if not user_id: return
        tickets = get_user_tickets(user_id)
        content = (message.text or message.caption or '')
        ticket = None
        if not tickets:
            ticket_id = create_support_ticket(user_id, None)
            add_support_message(ticket_id, sender='user', content=content)
            ticket = get_ticket(ticket_id)
            created_new = True
        else:
            open_tickets = [t for t in tickets if t.get('status') == 'open']
            if not open_tickets:
                ticket_id = create_support_ticket(user_id, None)
                add_support_message(ticket_id, sender='user', content=content)
                ticket = get_ticket(ticket_id)
                created_new = True
            else:
                ticket = max(open_tickets, key=lambda t: int(t['ticket_id']))
                ticket_id = int(ticket['ticket_id'])
                add_support_message(ticket_id, sender='user', content=content)
                created_new = False
        try:
            forum_chat_id = ticket.get('forum_chat_id')
            thread_id = ticket.get('message_thread_id')
            if not (forum_chat_id and thread_id):
                support_forum_chat_id = get_setting("support_forum_chat_id")
                if support_forum_chat_id:
                    try:
                        chat_id = int(support_forum_chat_id)
                        subj_full = (ticket.get('subject') or 'Обращение без темы')
                        is_star = subj_full.strip().startswith('⭐')
                        display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                        trimmed = display_subj[:40]
                        author_tag = (message.from_user.username and f"@{message.from_user.username}") or (message.from_user.full_name if message.from_user else None) or str(message.from_user.id)
                        important_prefix = '🔴 Важно: ' if is_star else ''
                        topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                        forum_topic = await bot.create_forum_topic(chat_id=chat_id, name=topic_name)
                        thread_id = forum_topic.message_thread_id
                        forum_chat_id = chat_id
                        update_ticket_thread_info(ticket_id, str(chat_id), int(thread_id))
                        subj_display = (ticket.get('subject') or '—')
                        header = f"{'🆘 Новое обращение\n' if created_new else '📌 Тред создан автоматически\n'}Тикет: #{ticket_id}\nПользователь: @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})\nТема: {subj_display} — от @{message.from_user.username or message.from_user.full_name} (ID: {message.from_user.id})"
                        await bot.send_message(chat_id=chat_id, text=header, message_thread_id=thread_id, reply_markup=_admin_actions_kb(ticket_id))
                    except Exception as e: logger.warning(f"Не удалось автоматически создать форумную тему для тикета {ticket_id}: {e}")
            if forum_chat_id and thread_id:
                try:
                    subj_full = (ticket.get('subject') or 'Обращение без темы')
                    is_star = subj_full.strip().startswith('⭐')
                    display_subj = (subj_full.lstrip('⭐️ ').strip() if is_star else subj_full)
                    trimmed = display_subj[:40]
                    author_tag = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                    important_prefix = '🔴 Важно: ' if is_star else ''
                    topic_name = f"#{ticket_id} {important_prefix}{trimmed} • от {author_tag}"
                    await bot.edit_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id), name=topic_name)
                except Exception as e: logger.warning(f"Не удалось переименовать тему для тикета со свободным сообщением {ticket_id}: {e}")
                username = (message.from_user.username and f"@{message.from_user.username}") or message.from_user.full_name or str(message.from_user.id)
                await bot.send_message(chat_id=int(forum_chat_id), text=f"{'🆘 Новое обращение от {username} (ID: {message.from_user.id}) по тикету #{ticket_id}:' if created_new else f'✉️ Новое сообщение по тикету #{ticket_id} от {username} (ID: {message.from_user.id}):'}", message_thread_id=int(thread_id))
                await bot.copy_message(chat_id=int(forum_chat_id), from_chat_id=message.chat.id, message_id=message.message_id, message_thread_id=int(thread_id))
        except Exception as e: logger.warning(f"Не удалось отзеркалить свободное сообщение пользователя в форум для тикета {ticket_id}: {e}")
        try:
            if created_new: await message.answer(f"✅ Обращение создано: #{ticket_id}. Мы ответим вам как можно скорее.")
            else: await message.answer("Сообщение принято. Поддержка скоро ответит.")
        except: pass
    return router

# ==================== KEYBOARDS ====================
def encode_host_callback_token(host_name: str) -> str:
    normalized = normalize_host_name(host_name)
    slug = re.sub(r"[^a-z0-9]+", "-", normalized.lower()).strip("-")
    slug = slug[:24]
    digest = hashlib.sha1(normalized.encode("utf-8")).hexdigest()[:8]
    if slug: return f"{slug}-{digest}"
    return digest

def parse_host_callback_data(data: str) -> tuple[str, str, str] | None:
    if not data or not data.startswith("select_host:"): return None
    parts = data.split(":", 3)
    if len(parts) != 4: return None
    _, action, extra, token = parts
    return action, extra or "-", token

def find_host_by_callback_token(hosts: list[dict], token: str) -> dict | None:
    if not token: return None
    for host in hosts or []:
        if encode_host_callback_token(host.get('host_name', '')) == token: return host
    return None

def _build_keyboard_from_db(menu_type: str, text_replacements: dict[str, str] | None = None, filter_func: Callable[[dict], bool] | None = None) -> InlineKeyboardMarkup | None:
    try: configs = get_button_configs(menu_type)
    except Exception as e: logger.warning(f"DB configs for {menu_type} not available: {e}"); return None
    if not configs: return None
    builder = InlineKeyboardBuilder()
    rows = {}
    added = set()
    for cfg in configs:
        if not cfg.get('is_active', True): continue
        if filter_func and not filter_func(cfg): continue
        text = cfg.get('text', '') or ''
        callback_data = cfg.get('callback_data')
        url = cfg.get('url')
        button_id = (cfg.get('button_id') or '').strip()
        if not callback_data and not url: continue
        if button_id:
            if button_id in added: continue
            added.add(button_id)
        if text_replacements:
            try:
                for k, v in text_replacements.items(): text = text.replace(k, str(v))
            except: pass
        row_pos = int(cfg.get('row_position', 0) or 0)
        col_pos = int(cfg.get('column_position', 0) or 0)
        sort_order = int(cfg.get('sort_order', 0) or 0)
        width = int(cfg.get('button_width', 1) or 1)
        rows.setdefault(row_pos, []).append({'text': text, 'callback_data': callback_data, 'url': url, 'width': max(1, min(width, 3)), 'col': col_pos, 'sort': sort_order})
    if not rows: return None
    for row_idx in sorted(rows.keys()):
        row_buttons = sorted(rows[row_idx], key=lambda b: (b['col'], b['sort']))
        i = 0
        while i < len(row_buttons):
            btn = row_buttons[i]
            button_width = btn['width']
            if button_width >= 2:
                if btn['callback_data']: builder.row(InlineKeyboardButton(text=btn['text'], callback_data=btn['callback_data']))
                elif btn['url']: builder.row(InlineKeyboardButton(text=btn['text'], url=btn['url']))
                i += 1
            else:
                if i + 1 < len(row_buttons) and row_buttons[i + 1]['width'] == 1:
                    btn2 = row_buttons[i + 1]
                    buttons = []
                    if btn['callback_data']: buttons.append(InlineKeyboardButton(text=btn['text'], callback_data=btn['callback_data']))
                    elif btn['url']: buttons.append(InlineKeyboardButton(text=btn['text'], url=btn['url']))
                    if btn2['callback_data']: buttons.append(InlineKeyboardButton(text=btn2['text'], callback_data=btn2['callback_data']))
                    elif btn2['url']: buttons.append(InlineKeyboardButton(text=btn2['text'], url=btn2['url']))
                    builder.row(*buttons)
                    i += 2
                else:
                    if btn['callback_data']: builder.row(InlineKeyboardButton(text=btn['text'], callback_data=btn['callback_data']))
                    elif btn['url']: builder.row(InlineKeyboardButton(text=btn['text'], url=btn['url']))
                    i += 1
    return builder.as_markup()

def create_main_menu_keyboard(user_keys: list, trial_available: bool, is_admin: bool) -> InlineKeyboardMarkup:
    def _filter(cfg: dict) -> bool:
        button_id = (cfg.get('button_id') or '').strip()
        if button_id == 'btn_try' and (not trial_available or get_setting("trial_enabled") != "true"): return False
        if button_id == 'btn_admin' and not is_admin: return False
        return True
    replacements = {'{count}': str(len(user_keys)), '((count))': f'({len(user_keys)})'}
    kb = _build_keyboard_from_db('main_menu', text_replacements=replacements, filter_func=_filter)
    if kb: return kb
    builder = InlineKeyboardBuilder()
    if trial_available and get_setting("trial_enabled") == "true": builder.button(text=(get_setting("btn_try") or "🎁 Попробовать бесплатно"), callback_data="get_trial")
    builder.button(text=(get_setting("btn_profile") or "👤 Мой профиль"), callback_data="show_profile")
    keys_label_tpl = (get_setting("btn_my_keys") or "🔑 Мои ключи ({count})")
    builder.button(text=keys_label_tpl.replace("{count}", str(len(user_keys))), callback_data="manage_keys")
    builder.button(text=(get_setting("btn_buy_key") or "💳 Купить ключ"), callback_data="buy_new_key")
    builder.button(text=(get_setting("btn_top_up") or "➕ Пополнить баланс"), callback_data="top_up_start")
    builder.button(text=(get_setting("btn_referral") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), callback_data="show_help")
    builder.button(text=(get_setting("btn_about") or "ℹ️ О проекте"), callback_data="show_about")
    builder.button(text=(get_setting("btn_howto") or "❓ Как использовать"), callback_data="howto_vless")
    builder.button(text=(get_setting("btn_speed") or "⚡ Тест скорости"), callback_data="user_speedtest")
    if is_admin: builder.button(text=(get_setting("btn_admin") or "⚙️ Админка"), callback_data="admin_menu")
    layout = [1 if trial_available and get_setting("trial_enabled") == "true" else 0, 2, 2, 1, 2, 2, 1 if is_admin else 0]
    actual_layout = [size for size in layout if size > 0]
    builder.adjust(*actual_layout)
    return builder.as_markup()

def create_admin_menu_keyboard() -> InlineKeyboardMarkup:
    kb = _build_keyboard_from_db('admin_menu')
    if kb: return kb
    builder = InlineKeyboardBuilder()
    builder.button(text="👥 Пользователи", callback_data="admin_users")
    builder.button(text="🌍 Ключи на хосте", callback_data="admin_host_keys")
    builder.button(text="🎁 Выдать ключ", callback_data="admin_gift_key")
    builder.button(text="⚡ Тест скорости", callback_data="admin_speedtest")
    builder.button(text="📊 Мониторинг", callback_data="admin_monitor")
    builder.button(text="🗄 Бэкап БД", callback_data="admin_backup_db")
    builder.button(text="♻️ Восстановить БД", callback_data="admin_restore_db")
    builder.button(text="👮 Администраторы", callback_data="admin_administrators")
    builder.button(text="🎟 Промокоды", callback_data="admin_promo_menu")
    builder.button(text="📢 Рассылка", callback_data="start_broadcast")
    builder.button(text="⬅️ Назад в меню", callback_data="back_to_main_menu")
    builder.adjust(2, 2, 2, 2, 1, 1, 1)
    return builder.as_markup()

def create_admin_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    return builder.as_markup()

def create_admin_promo_code_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🎲 Сгенерировать код", callback_data="admin_promo_gen_code")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_promos_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Создать промокод", callback_data="admin_promo_create")
    builder.button(text="📋 Список промокодов", callback_data="admin_promo_list")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_promo_discount_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Процент", callback_data="admin_promo_discount_type_percent")
    builder.button(text="Фикс (RUB)", callback_data="admin_promo_discount_type_amount")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 1)
    return builder.as_markup()

def create_admin_promo_discount_percent_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for p in (5, 10, 15, 20, 25, 30): builder.button(text=f"{p}%", callback_data=f"admin_promo_discount_percent_{p}")
    builder.button(text="🖊 Ввести процент", callback_data="admin_promo_discount_manual_percent")
    builder.button(text="🖊 Ввести фикс RUB", callback_data="admin_promo_discount_manual_amount")
    builder.button(text="↔️ Фикс-меню", callback_data="admin_promo_discount_show_amount_menu")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(3, 3, 1, 1, 1)
    return builder.as_markup()

def create_admin_promo_discount_amount_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for a in (50, 100, 150, 200, 300, 500): builder.button(text=f"{a} RUB", callback_data=f"admin_promo_discount_amount_{a}")
    builder.button(text="🖊 Ввести фикс RUB", callback_data="admin_promo_discount_manual_amount")
    builder.button(text="🖊 Ввести процент", callback_data="admin_promo_discount_manual_percent")
    builder.button(text="↔️ Процент-меню", callback_data="admin_promo_discount_show_percent_menu")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(3, 3, 1, 1, 1)
    return builder.as_markup()

def create_admin_promo_limits_type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Общий лимит", callback_data="admin_promo_limits_type_total")
    builder.button(text="Лимит на пользователя", callback_data="admin_promo_limits_type_per")
    builder.button(text="Оба лимита", callback_data="admin_promo_limits_type_both")
    builder.button(text="Пропустить", callback_data="admin_promo_limits_skip")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 1, 1, 1)
    return builder.as_markup()

def create_admin_promo_limits_total_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for n in (10, 50, 100, 200, 500, 1000): builder.button(text=str(n), callback_data=f"admin_promo_limits_total_preset_{n}")
    builder.button(text="🖊 Ввести значение", callback_data="admin_promo_limits_total_manual")
    builder.button(text="⬅️ Назад", callback_data="admin_promo_limits_back_to_type")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(3, 3, 1, 1)
    return builder.as_markup()

def create_admin_promo_limits_per_user_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for n in (1, 2, 3, 5, 10): builder.button(text=str(n), callback_data=f"admin_promo_limits_per_preset_{n}")
    builder.button(text="🖊 Ввести значение", callback_data="admin_promo_limits_per_manual")
    builder.button(text="⬅️ Назад", callback_data="admin_promo_limits_back_to_type")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(3, 2, 1, 1)
    return builder.as_markup()

def create_admin_promo_dates_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="3 дня", callback_data="admin_promo_dates_days_3")
    builder.button(text="7 дней", callback_data="admin_promo_dates_days_7")
    builder.button(text="14 дней", callback_data="admin_promo_dates_days_14")
    builder.button(text="30 дней", callback_data="admin_promo_dates_days_30")
    builder.button(text="90 дней", callback_data="admin_promo_dates_days_90")
    builder.button(text="Неделя", callback_data="admin_promo_dates_week")
    builder.button(text="Месяц", callback_data="admin_promo_dates_month")
    builder.button(text="🖊 Ввести число дней", callback_data="admin_promo_dates_custom_days")
    builder.button(text="Пропустить", callback_data="admin_promo_dates_skip")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2, 2, 1, 2, 1)
    return builder.as_markup()

def create_admin_promo_description_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="Пропустить", callback_data="admin_promo_desc_skip")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_promo_confirm_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Создать", callback_data="admin_promo_confirm_create")
    builder.button(text="❌ Отмена", callback_data="admin_cancel")
    builder.adjust(2)
    return builder.as_markup()

def create_admin_users_pick_keyboard(users: list[dict], page: int = 0, page_size: int = 10, action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    start = page * page_size
    end = start + page_size
    for u in users[start:end]:
        user_id = u.get('telegram_id') or u.get('user_id') or u.get('id')
        username = u.get('username') or '—'
        title = f"{user_id} • @{username}" if username != '—' else f"{user_id}"
        builder.button(text=title, callback_data=f"admin_{action}_pick_user_{user_id}")
    total = len(users)
    have_prev = page > 0
    have_next = end < total
    if have_prev: builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_pick_user_page_{page-1}")
    if have_next: builder.button(text="Вперёд ➡️", callback_data=f"admin_{action}_pick_user_page_{page+1}")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    rows = [1] * len(users[start:end])
    tail = []
    if have_prev or have_next: tail.append(2 if (have_prev and have_next) else 1)
    tail.append(1)
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1]))
    return builder.as_markup()

def create_admin_hosts_pick_keyboard(hosts: list[dict], action: str = "gift") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if hosts:
        for h in hosts:
            name = h.get('host_name')
            title = name or "—"
            if action == "speedtest":
                token = encode_host_callback_token(name or "")
                builder.button(text=title, callback_data=f"admin_{action}_pick_host_{token}")
                builder.button(text="🛠 Автоустановка", callback_data=f"admin_speedtest_autoinstall_{token}")
            else: builder.button(text=title, callback_data=f"admin_{action}_pick_host_{title}")
    else: builder.button(text="Хостов нет", callback_data="noop")
    if action == "speedtest":
        builder.button(text="🚀 Запустить для всех", callback_data="admin_speedtest_run_all")
    builder.button(text="⬅️ Назад", callback_data=f"admin_{action}_back_to_users")
    if action == "speedtest":
        rows = [2] * (len(hosts) if hosts else 1)
        tail = [1, 1]
    else:
        rows = [1] * (len(hosts) if hosts else 1)
        tail = [1]
    builder.adjust(*(rows + tail))
    return builder.as_markup()

def create_admin_keys_for_host_keyboard(host_name: str, keys: list[dict], page: int = 0, page_size: int = 20) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if not keys:
        builder.button(text="Ключей на хосте нет", callback_data="noop")
        builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
        builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        builder.adjust(1)
        return builder.as_markup()
    start = page * page_size
    end = start + page_size
    for k in keys[start:end]:
        kid = k.get('key_id')
        email = k.get('key_email') or '—'
        expiry = k.get('expiry_date') or '—'
        title = f"#{kid} • {email[:24]} • до {expiry}"
        builder.button(text=title, callback_data=f"admin_edit_key_{kid}")
    total = len(keys)
    have_prev = page > 0
    have_next = end < total
    if have_prev: builder.button(text="⬅️ Назад", callback_data=f"admin_hostkeys_page_{page-1}")
    if have_next: builder.button(text="Вперёд ➡️", callback_data=f"admin_hostkeys_page_{page+1}")
    builder.button(text="⬅️ К выбору хоста", callback_data="admin_hostkeys_back_to_hosts")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    rows = [1] * len(keys[start:end])
    tail = []
    if have_prev or have_next: tail.append(2 if (have_prev and have_next) else 1)
    tail.extend([1, 1])
    builder.adjust(*(rows + tail if rows else ([2] if (have_prev or have_next) else []) + [1, 1]))
    return builder.as_markup()

def create_admin_user_actions_keyboard(user_id: int, is_banned: bool | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Начислить баланс", callback_data=f"admin_add_balance_{user_id}")
    builder.button(text="➖ Списать баланс", callback_data=f"admin_deduct_balance_{user_id}")
    builder.button(text="🎁 Выдать ключ", callback_data=f"admin_gift_key_{user_id}")
    builder.button(text="🤝 Рефералы пользователя", callback_data=f"admin_user_referrals_{user_id}")
    if is_banned is True: builder.button(text="✅ Разбанить", callback_data=f"admin_unban_user_{user_id}")
    else: builder.button(text="🚫 Забанить", callback_data=f"admin_ban_user_{user_id}")
    builder.button(text="✏️ Ключи пользователя", callback_data=f"admin_user_keys_{user_id}")
    builder.button(text="⬅️ К списку", callback_data="admin_users")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(2, 2, 2, 2, 1)
    return builder.as_markup()

def create_admin_user_keys_keyboard(user_id: int, keys: list[dict]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if keys:
        for k in keys:
            kid = k.get('key_id')
            host = k.get('host_name') or '—'
            email = k.get('key_email') or '—'
            title = f"#{kid} • {host} • {email[:20]}"
            builder.button(text=title, callback_data=f"admin_edit_key_{kid}")
    else: builder.button(text="Ключей нет", callback_data="noop")
    builder.button(text="⬅️ Назад", callback_data=f"admin_view_user_{user_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_admin_key_actions_keyboard(key_id: int, user_id: int | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🌍 Изменить сервер", callback_data=f"admin_key_edit_host_{key_id}")
    builder.button(text="➕ Добавить дни", callback_data=f"admin_key_extend_{key_id}")
    builder.button(text="🗑 Удалить ключ", callback_data=f"admin_key_delete_{key_id}")
    builder.button(text="⬅️ Назад к ключам", callback_data=f"admin_key_back_{key_id}")
    if user_id is not None:
        builder.button(text="👤 Перейти к пользователю", callback_data=f"admin_view_user_{user_id}")
        builder.adjust(2, 2, 1)
    else: builder.adjust(2, 2)
    return builder.as_markup()

def create_admin_delete_key_confirm_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить удаление", callback_data=f"admin_key_delete_confirm_{key_id}")
    builder.button(text="❌ Отмена", callback_data=f"admin_key_delete_cancel_{key_id}")
    builder.adjust(1)
    return builder.as_markup()

def create_admins_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить админа", callback_data="admin_add_admin")
    builder.button(text="➖ Снять админа", callback_data="admin_remove_admin")
    builder.button(text="📋 Список админов", callback_data="admin_view_admins")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(2, 2)
    return builder.as_markup()

def create_admin_monitor_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔄 Обновить", callback_data="admin_monitor_refresh")
    builder.button(text="⬅️ В админ-меню", callback_data="admin_menu")
    builder.adjust(1, 1)
    return builder.as_markup()

def create_howto_vless_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_howto_android") or "📱 Android"), callback_data="howto_android")
    builder.button(text=(get_setting("btn_howto_ios") or "📱 iOS"), callback_data="howto_ios")
    builder.button(text=(get_setting("btn_howto_windows") or "💻 Windows"), callback_data="howto_windows")
    builder.button(text=(get_setting("btn_howto_linux") or "🐧 Linux"), callback_data="howto_linux")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_back_to_menu_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    return builder.as_markup()

def create_broadcast_cancel_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    return builder.as_markup()

def create_broadcast_options_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить кнопку", callback_data="broadcast_add_button")
    builder.button(text="➡️ Пропустить", callback_data="broadcast_skip_button")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2, 1)
    return builder.as_markup()

def create_broadcast_confirmation_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Отправить всем", callback_data="confirm_broadcast")
    builder.button(text="❌ Отмена", callback_data="cancel_broadcast")
    builder.adjust(2)
    return builder.as_markup()

def create_profile_keyboard() -> InlineKeyboardMarkup:
    kb = _build_keyboard_from_db('profile_menu')
    if kb: return kb
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_top_up") or "➕ Пополнить баланс"), callback_data="top_up_start")
    builder.button(text=(get_setting("btn_referral") or "🤝 Реферальная программа"), callback_data="show_referral_program")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def get_main_menu_button() -> InlineKeyboardButton:
    return InlineKeyboardButton(text="🏠 В главное меню", callback_data="show_main_menu")

def create_support_keyboard(support_user: str | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    username = (support_user or "").strip()
    if not username: username = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
    url = None
    if username:
        if username.startswith("@"): url = f"tg://resolve?domain={username[1:]}"
        elif username.startswith("tg://"): url = username
        elif username.startswith("http://") or username.startswith("https://"):
            try: part = username.split("/")[-1].split("?")[0]; url = f"tg://resolve?domain={part}" if part else username
            except: url = username
        else: url = f"tg://resolve?domain={username}"
    if url:
        builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), url=url)
        builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    else:
        builder.button(text=(get_setting("btn_support") or "🆘 Поддержка"), callback_data="show_help")
        builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_support_bot_link_keyboard(support_bot_username: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    username = support_bot_username.lstrip("@")
    deep_link = f"tg://resolve?domain={username}&start=new"
    builder.button(text=(get_setting("btn_support_open") or "🆘 Открыть поддержку"), url=deep_link)
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_host_selection_keyboard(hosts: list, action: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    base_action = action
    extra = "-"
    if action.startswith("switch_"):
        base_action = "switch"
        extra = action[len("switch_"):] or "-"
    elif action in {"trial", "new"}: base_action = action
    else: base_action = action
    prefix = f"select_host:{base_action}:{extra}:"
    for host in hosts:
        token = encode_host_callback_token(host['host_name'])
        builder.button(text=host['host_name'], callback_data=f"{prefix}{token}")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="manage_keys" if action == 'new' else "back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_plans_keyboard(plans: list[dict], action: str, host_name: str, key_id: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for plan in plans: builder.button(text=f"{plan['plan_name']} - {plan['price']:.0f} RUB", callback_data=f"buy_{host_name}_{plan['plan_id']}_{action}_{key_id}")
    back_callback = "manage_keys" if action == "extend" else "buy_new_key"
    builder.button(text=(get_setting("btn_back") or "⬅️ Назад"), callback_data=back_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_skip_email_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_skip_email") or "➡️ Продолжить без почты"), callback_data="skip_email")
    builder.button(text=(get_setting("btn_back_to_plans") or "⬅️ Назад к тарифам"), callback_data="back_to_plans")
    builder.adjust(1)
    return builder.as_markup()

def create_payment_method_keyboard(payment_methods: dict, action: str, key_id: int, show_balance: bool | None = None,
                                 main_balance: float | None = None, price: float | None = None,
                                 has_promo_applied: bool | None = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if has_promo_applied: builder.button(text="❌ Убрать промокод", callback_data="remove_promo_code")
    else: builder.button(text="🎟️ Ввести промокод", callback_data="enter_promo_code")
    if show_balance:
        label = get_setting("btn_pay_with_balance") or "💼 Оплатить с баланса"
        if main_balance is not None:
            try: label += f" ({main_balance:.0f} RUB)"
            except: pass
        builder.button(text=label, callback_data="pay_balance")
    if payment_methods and payment_methods.get("yookassa"):
        if get_setting("sbp_enabled"): builder.button(text="🏦 СБП / Банковская карта", callback_data="pay_yookassa")
        else: builder.button(text="🏦 Банковская карта", callback_data="pay_yookassa")
    if payment_methods and payment_methods.get("heleket"): builder.button(text="💎 Криптовалюта", callback_data="pay_heleket")
    if payment_methods and payment_methods.get("cryptobot"): builder.button(text="🤖 CryptoBot", callback_data="pay_cryptobot")
    if payment_methods and payment_methods.get("yoomoney"): builder.button(text="💜 ЮMoney (кошелёк)", callback_data="pay_yoomoney")
    if payment_methods and payment_methods.get("stars"): builder.button(text="⭐ Telegram Stars", callback_data="pay_stars")
    if payment_methods and payment_methods.get("tonconnect"): builder.button(text="🪙 TON Connect", callback_data="pay_tonconnect")
    builder.button(text=(get_setting("btn_back") or "⬅️ Назад"), callback_data="back_to_email_prompt")
    builder.adjust(1)
    return builder.as_markup()

def create_payment_keyboard(payment_url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_go_to_payment") or "Перейти к оплате"), url=payment_url)
    return builder.as_markup()

def create_payment_with_check_keyboard(payment_url: str, check_callback: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_go_to_payment") or "Перейти к оплате"), url=payment_url)
    builder.button(text=(get_setting("btn_check_payment") or "✅ Проверить оплату"), callback_data=check_callback)
    builder.adjust(1)
    return builder.as_markup()

def create_topup_payment_method_keyboard(payment_methods: dict) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if payment_methods and payment_methods.get("yookassa"):
        if get_setting("sbp_enabled"): builder.button(text="🏦 СБП / Банковская карта", callback_data="topup_pay_yookassa")
        else: builder.button(text="🏦 Банковская карта", callback_data="topup_pay_yookassa")
    if payment_methods and payment_methods.get("heleket"): builder.button(text="💎 Криптовалюта", callback_data="topup_pay_heleket")
    if payment_methods and payment_methods.get("cryptobot"): builder.button(text="🤖 CryptoBot", callback_data="topup_pay_cryptobot")
    if payment_methods and payment_methods.get("yoomoney"): builder.button(text="💜 ЮMoney (кошелёк)", callback_data="topup_pay_yoomoney")
    if payment_methods and payment_methods.get("stars"): builder.button(text="⭐ Telegram Stars", callback_data="topup_pay_stars")
    if payment_methods and payment_methods.get("tonconnect"): builder.button(text="🪙 TON Connect", callback_data="topup_pay_tonconnect")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="show_profile")
    builder.adjust(1)
    return builder.as_markup()

def create_keys_management_keyboard(keys: list) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if keys:
        for i, key in enumerate(keys):
            expiry_date = datetime.fromisoformat(key['expiry_date'])
            status_icon = "✅" if expiry_date > datetime.now() else "❌"
            host_name = key.get('host_name', 'Неизвестный хост')
            button_text = f"{status_icon} Ключ #{i+1} ({host_name}) (до {expiry_date.strftime('%d.%m.%Y')})"
            builder.button(text=button_text, callback_data=f"show_key_{key['key_id']}")
    builder.button(text=(get_setting("btn_buy_key") or "➕ Купить новый ключ"), callback_data="buy_new_key")
    builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
    builder.adjust(1)
    return builder.as_markup()

def create_key_info_keyboard(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_extend_key") or "➕ Продлить этот ключ"), callback_data=f"extend_key_{key_id}")
    builder.button(text=(get_setting("btn_show_qr") or "📱 Показать QR-код"), callback_data=f"show_qr_{key_id}")
    builder.button(text=(get_setting("btn_instruction") or "📖 Инструкция"), callback_data=f"howto_vless_{key_id}")
    builder.button(text=(get_setting("btn_switch_server") or "🌍 Сменить сервер"), callback_data=f"switch_server_{key_id}")
    builder.button(text=(get_setting("btn_back_to_keys") or "⬅️ Назад к списку ключей"), callback_data="manage_keys")
    builder.adjust(1)
    return builder.as_markup()

def create_howto_vless_keyboard_key(key_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=(get_setting("btn_howto_android") or "📱 Android"), callback_data="howto_android")
    builder.button(text=(get_setting("btn_howto_ios") or "📱 iOS"), callback_data="howto_ios")
    builder.button(text=(get_setting("btn_howto_windows") or "💻 Windows"), callback_data="howto_windows")
    builder.button(text=(get_setting("btn_howto_linux") or "🐧 Linux"), callback_data="howto_linux")
    builder.button(text=(get_setting("btn_back_to_key") or "⬅️ Назад к ключу"), callback_data=f"show_key_{key_id}")
    builder.adjust(2, 2, 1)
    return builder.as_markup()

def create_welcome_keyboard(channel_url: str | None, is_subscription_forced: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if channel_url and is_subscription_forced:
        builder.button(text="📢 Перейти в канал", url=channel_url)
        builder.button(text="✅ Я подписался", callback_data="check_subscription_and_agree")
    elif channel_url:
        builder.button(text="📢 Наш канал (не обязательно)", url=channel_url)
        builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
    else: builder.button(text="✅ Принимаю условия", callback_data="check_subscription_and_agree")
    builder.adjust(1)
    return builder.as_markup()

def create_ton_connect_keyboard(connect_url: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🚀 Открыть кошелек", url=connect_url)
    return builder.as_markup()

main_reply_keyboard = ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="🏠 Главное меню")]], resize_keyboard=True)

# ==================== USER HANDLERS ====================
class KeyPurchase(StatesGroup):
    waiting_for_host_selection = State()
    waiting_for_plan_selection = State()

class Onboarding(StatesGroup):
    waiting_for_subscription_and_agreement = State()

class PaymentProcess(StatesGroup):
    waiting_for_email = State()
    waiting_for_payment_method = State()
    waiting_for_promo_code = State()
    waiting_for_cryptobot_payment = State()

class TopUpProcess(StatesGroup):
    waiting_for_amount = State()
    waiting_for_topup_method = State()
    waiting_for_cryptobot_topup_payment = State()

PAYMENT_METHODS = {}
TELEGRAM_BOT_USERNAME = get_setting("telegram_bot_username")
ADMIN_ID = None
CRYPTO_BOT_TOKEN = get_setting("cryptobot_token")

def is_valid_email(email: str) -> bool:
    pattern = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
    return re.match(pattern, email) is not None

async def show_main_menu(message: types.Message, edit_message: bool = False):
    user_id = message.chat.id
    user_db_data = get_user(user_id)
    user_keys = get_user_keys(user_id)
    trial_available = not (user_db_data and user_db_data.get('trial_used'))
    is_admin_flag = is_admin(user_id)
    custom_main_text = get_setting("main_menu_text")
    text = (custom_main_text or "🏠 <b>Главное меню</b>\n\nВыберите действие:")
    keyboard = create_main_menu_keyboard(user_keys, trial_available, is_admin_flag)
    if edit_message:
        try: await message.edit_text(text, reply_markup=keyboard)
        except TelegramBadRequest: pass
    else: await message.answer(text, reply_markup=keyboard)

async def process_successful_onboarding(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    try: set_terms_agreed(user_id)
    except Exception as e: logger.error(f"Не удалось установить согласие с условиями для пользователя {user_id}: {e}")
    try: await callback.answer()
    except: pass
    try: await show_main_menu(callback.message, edit_message=True)
    except:
        try: await callback.message.answer("✅ Требования выполнены. Открываю меню...")
        except: pass
    try: await state.clear()
    except: pass

def registration_required(f):
    @wraps(f)
    async def decorated_function(event, *args, **kwargs):
        user_id = event.from_user.id
        user_data = get_user(user_id)
        if user_data: return await f(event, *args, **kwargs)
        else:
            message_text = "Пожалуйста, для начала работы со мной, отправьте команду /start"
            if isinstance(event, CallbackQuery): await event.answer(message_text, show_alert=True)
            else: await event.answer(message_text)
    return decorated_function

def get_user_router() -> Router:
    user_router = Router()
    def _get_stars_rate() -> Decimal:
        try: rate_raw = get_setting("stars_per_rub") or "1"; rate = Decimal(str(rate_raw)); return rate if rate > 0 else Decimal("1")
        except: return Decimal("1")
    def _calc_stars_amount(amount_rub: Decimal) -> int:
        rate = _get_stars_rate()
        try: stars = (amount_rub * rate).quantize(Decimal("1"), rounding=ROUND_HALF_UP)
        except: stars = (amount_rub * rate)
        try: return int(stars)
        except: return int(float(stars))
    @user_router.message(CommandStart())
    async def start_handler(message: types.Message, state: FSMContext, bot: Bot, command: CommandObject):
        user_id = message.from_user.id
        username = message.from_user.username or message.from_user.full_name
        referrer_id = None
        if command.args and command.args.startswith('ref_'):
            try:
                potential_referrer_id = int(command.args.split('_')[1])
                if potential_referrer_id != user_id: referrer_id = potential_referrer_id
            except (IndexError, ValueError): logger.warning(f"Получен некорректный реферальный код: {command.args}")
        register_user_if_not_exists(user_id, username, referrer_id)
        user_data = get_user(user_id)
        try: reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
        except: reward_type = "percent_purchase"
        if reward_type == "fixed_start_referrer" and referrer_id and user_data and not user_data.get('referral_start_bonus_received'):
            try: amount_raw = get_setting("referral_on_start_referrer_amount") or "20"; start_bonus = Decimal(str(amount_raw)).quantize(Decimal("0.01"))
            except: start_bonus = Decimal("20.00")
            if start_bonus > 0:
                try: ok = add_to_balance(int(referrer_id), float(start_bonus))
                except Exception as e: logger.warning(f"Реферальный стартовый бонус: не удалось добавить к балансу для реферера {referrer_id}: {e}")
                try: add_to_referral_balance_all(int(referrer_id), float(start_bonus))
                except Exception as e: logger.warning(f"Реферальный стартовый бонус: не удалось увеличить общий реферальный баланс для {referrer_id}: {e}")
                try: set_referral_start_bonus_received(user_id)
                except: pass
                try: await bot.send_message(chat_id=int(referrer_id), text=f"🎁 Начисление за приглашение!\nНовый пользователь: {message.from_user.full_name} (ID: {user_id})\nБонус: {float(start_bonus):.2f} RUB")
                except: pass
        if user_data and user_data.get('agreed_to_terms'):
            await message.answer(f"👋 Снова здравствуйте, {html.bold(message.from_user.full_name)}!", reply_markup=main_reply_keyboard)
            await show_main_menu(message); return
        terms_url = get_setting("terms_url")
        privacy_url = get_setting("privacy_url")
        channel_url = get_setting("channel_url")
        if not channel_url and (not terms_url or not privacy_url):
            set_terms_agreed(user_id)
            await show_main_menu(message); return
        is_subscription_forced = get_setting("force_subscription") == "true"
        show_welcome_screen = (is_subscription_forced and channel_url) or (terms_url and privacy_url)
        if not show_welcome_screen:
            set_terms_agreed(user_id)
            await show_main_menu(message); return
        welcome_parts = ["<b>Добро пожаловать!</b>\n"]
        if is_subscription_forced and channel_url: welcome_parts.append("Для доступа ко всем функциям, пожалуйста, подпишитесь на наш канал.")
        if terms_url and privacy_url: welcome_parts.append(f"Также необходимо ознакомиться и принять наши <a href='{terms_url}'>Условия использования</a> и <a href='{privacy_url}'>Политику конфиденциальности</a>.")
        welcome_parts.append("\nПосле этого нажмите кнопку ниже.")
        final_text = "\n".join(welcome_parts)
        await message.answer(final_text, reply_markup=create_welcome_keyboard(channel_url=channel_url, is_subscription_forced=is_subscription_forced), disable_web_page_preview=True)
        await state.set_state(Onboarding.waiting_for_subscription_and_agreement)
    @user_router.callback_query(Onboarding.waiting_for_subscription_and_agreement, F.data == "check_subscription_and_agree")
    async def check_subscription_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        user_id = callback.from_user.id
        channel_url = get_setting("channel_url")
        is_subscription_forced = get_setting("force_subscription") == "true"
        if not is_subscription_forced or not channel_url:
            await process_successful_onboarding(callback, state); return
        try:
            if '@' not in channel_url and 't.me/' not in channel_url:
                logger.error(f"Неверный формат URL канала: {channel_url}. Пропускаем проверку подписки.")
                await process_successful_onboarding(callback, state); return
            channel_id = '@' + channel_url.split('/')[-1] if 't.me/' in channel_url else channel_url
            member = await bot.get_chat_member(chat_id=channel_id, user_id=user_id)
            if member.status in [ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]: await process_successful_onboarding(callback, state)
            else: await callback.answer("Вы еще не подписались на канал. Пожалуйста, подпишитесь и попробуйте снова.", show_alert=True)
        except Exception as e:
            logger.error(f"Ошибка при проверке подписки для user_id {user_id} на канал {channel_url}: {e}")
            await callback.answer("Не удалось проверить подписку. Убедитесь, что бот является администратором канала. Попробуйте позже.", show_alert=True)
    @user_router.message(Onboarding.waiting_for_subscription_and_agreement)
    async def onboarding_fallback_handler(message: types.Message): await message.answer("Пожалуйста, выполните требуемые действия и нажмите на кнопку в сообщении выше.")
    @user_router.message(F.text == "🏠 Главное меню")
    @registration_required
    async def main_menu_handler(message: types.Message): await show_main_menu(message)
    @user_router.callback_query(F.data == "back_to_main_menu")
    @registration_required
    async def back_to_main_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)
    @user_router.callback_query(F.data == "show_main_menu")
    @registration_required
    async def show_main_menu_cb(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)
    @user_router.callback_query(F.data == "show_profile")
    @registration_required
    async def profile_handler_callback(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        user_keys = get_user_keys(user_id)
        if not user_db_data: await callback.answer("Не удалось получить данные профиля.", show_alert=True); return
        username = html.bold(user_db_data.get('username', 'Пользователь'))
        total_spent, total_months = user_db_data.get('total_spent', 0), user_db_data.get('total_months', 0)
        now = datetime.now()
        active_keys = [key for key in user_keys if datetime.fromisoformat(key['expiry_date']) > now]
        if active_keys:
            latest_key = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']))
            latest_expiry_date = datetime.fromisoformat(latest_key['expiry_date'])
            time_left = latest_expiry_date - now
            vpn_status_text = f"✅ <b>Статус VPN:</b> Активен\n⏳ <b>Осталось:</b> {time_left.days} д. {time_left.seconds // 3600} ч."
        elif user_keys: vpn_status_text = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
        else: vpn_status_text = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."
        final_text = f"👤 <b>Профиль:</b> {username}\n\n💰 <b>Потрачено всего:</b> {total_spent:.0f} RUB\n📅 <b>Приобретено месяцев:</b> {total_months}\n\n{vpn_status_text}"
        try: main_balance = get_balance(user_id)
        except: main_balance = 0.0
        final_text += f"\n\n💼 <b>Основной баланс:</b> {main_balance:.0f} RUB"
        try: referral_count = get_referral_count(user_id)
        except: referral_count = 0
        try: total_ref_earned = float(get_referral_balance_all(user_id))
        except: total_ref_earned = 0.0
        final_text += f"\n🤝 <b>Рефералы:</b> {referral_count}\n💰 <b>Заработано по рефералке (всего):</b> {total_ref_earned:.2f} RUB"
        await callback.message.edit_text(final_text, reply_markup=create_profile_keyboard())
    @user_router.callback_query(F.data == "profile_info")
    @registration_required
    async def profile_info_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        user_keys = get_user_keys(user_id)
        if not user_db_data: await callback.answer("Не удалось получить данные профиля.", show_alert=True); return
        username = html.bold(user_db_data.get('username', 'Пользователь'))
        total_spent = user_db_data.get('total_spent', 0)
        total_months = user_db_data.get('total_months', 0)
        now = datetime.now()
        active_keys = [key for key in user_keys if datetime.fromisoformat(key['expiry_date']) > now]
        if active_keys:
            latest_key = max(active_keys, key=lambda k: datetime.fromisoformat(k['expiry_date']))
            latest_expiry_date = datetime.fromisoformat(latest_key['expiry_date'])
            time_left = latest_expiry_date - now
            vpn_status_text = f"✅ <b>Статус VPN:</b> Активен\n⏳ <b>Осталось:</b> {time_left.days} д. {time_left.seconds // 3600} ч."
        elif user_keys: vpn_status_text = "❌ <b>Статус VPN:</b> Неактивен (срок истек)"
        else: vpn_status_text = "ℹ️ <b>Статус VPN:</b> У вас пока нет активных ключей."
        final_text = f"👤 <b>Профиль:</b> {username}\n\n💰 <b>Потрачено всего:</b> {total_spent:.0f} RUB\n📅 <b>Приобретено месяцев:</b> {total_months}\n\n{vpn_status_text}"
        final_text += f"\n\n📊 <b>Статистика:</b>"
        final_text += f"\n🔑 <b>Всего ключей:</b> {len(user_keys)}"
        final_text += f"\n✅ <b>Активных ключей:</b> {len(active_keys)}"
        final_text += f"\n💸 <b>Потрачено всего:</b> {total_spent:.2f} RUB"
        final_text += f"\n📅 <b>Месяцев подписки:</b> {total_months}"
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data="show_profile")
        await callback.message.edit_text(final_text, reply_markup=builder.as_markup())
    @user_router.callback_query(F.data == "profile_balance")
    @registration_required
    async def profile_balance_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        try: main_balance = get_balance(user_id)
        except: main_balance = 0.0
        try: referral_count = get_referral_count(user_id)
        except: referral_count = 0
        try: total_ref_earned = float(get_referral_balance_all(user_id))
        except: total_ref_earned = 0.0
        try: ref_balance = float(get_referral_balance(user_id))
        except: ref_balance = 0.0
        text = f"💰 <b>Информация о балансе</b>\n\n💼 <b>Основной баланс:</b> {main_balance:.2f} RUB\n🤝 <b>Реферальный баланс:</b> {ref_balance:.2f} RUB\n📊 <b>Всего заработано по рефералке:</b> {total_ref_earned:.2f} RUB\n👥 <b>Приглашено пользователей:</b> {referral_count}\n\n💡 <b>Совет:</b> Используйте реферальный баланс для покупки ключей!"
        builder = InlineKeyboardBuilder()
        builder.button(text="💳 Пополнить", callback_data="top_up_start")
        builder.button(text="⬅️ Назад", callback_data="show_profile")
        builder.adjust(1)
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    @user_router.callback_query(F.data == "main_menu")
    @registration_required
    async def profile_main_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        await show_main_menu(callback.message, edit_message=True)
    @user_router.callback_query(F.data == "top_up_start")
    @registration_required
    async def topup_start_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text("Введите сумму пополнения в рублях (например, 300):\nМинимум: 10 RUB, максимум: 100000 RUB")
        await state.set_state(TopUpProcess.waiting_for_amount)
    @user_router.message(TopUpProcess.waiting_for_amount)
    async def topup_amount_input(message: types.Message, state: FSMContext):
        text = (message.text or "").replace(",", ".").strip()
        try: amount = Decimal(text)
        except: await message.answer("❌ Введите корректную сумму, например: 300"); return
        if amount <= 0: await message.answer("❌ Сумма должна быть положительной"); return
        if amount < Decimal("10"): await message.answer("❌ Минимальная сумма пополнения: 10 RUB"); return
        if amount > Decimal("100000"): await message.answer("❌ Максимальная сумма пополнения: 100000 RUB"); return
        final_amount = amount.quantize(Decimal("0.01"))
        await state.update_data(topup_amount=float(final_amount))
        await message.answer(f"К пополнению: {final_amount:.2f} RUB\nВыберите способ оплаты:", reply_markup=create_topup_payment_method_keyboard(PAYMENT_METHODS))
        await state.set_state(TopUpProcess.waiting_for_topup_method)
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_yookassa")
    async def topup_pay_yookassa(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Создаю ссылку на оплату...")
        data = await state.get_data()
        amount = Decimal(str(data.get('topup_amount', 0)))
        if amount <= 0: await callback.message.edit_text("❌ Некорректная сумма пополнения. Повторите ввод."); await state.clear(); return
        user_id = callback.from_user.id
        price_str_for_api = f"{amount:.2f}"
        price_float_for_metadata = float(amount)
        try:
            customer_email = get_setting("receipt_email")
            receipt = None
            if customer_email and is_valid_email(customer_email):
                receipt = {"customer": {"email": customer_email}, "items": [{"description": f"Пополнение баланса", "quantity": "1.00", "amount": {"value": price_str_for_api, "currency": "RUB"}, "vat_code": 1, "payment_subject": "service", "payment_mode": "full_payment"}]}
            payment_payload = {"amount": {"value": price_str_for_api, "currency": "RUB"}, "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"}, "capture": True, "description": f"Пополнение баланса на {price_str_for_api} RUB", "metadata": {"user_id": str(user_id), "price": f"{price_float_for_metadata:.2f}", "action": "top_up", "payment_method": "YooKassa"}}
            if receipt: payment_payload['receipt'] = receipt
            payment = Payment.create(payment_payload, uuid.uuid4())
            await state.clear()
            await callback.message.edit_text("Нажмите на кнопку ниже для оплаты:", reply_markup=create_payment_keyboard(payment.confirmation.confirmation_url))
        except Exception as e: logger.error(f"Не удалось создать платеж YooKassa для пополнения: {e}", exc_info=True); await callback.message.answer("Не удалось создать ссылку на оплату."); await state.clear()
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_yoomoney")
    async def topup_pay_yoomoney(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Готовлю ЮMoney…")
        data = await state.get_data()
        amount = Decimal(str(data.get('topup_amount', 0)))
        if amount <= 0: await callback.message.edit_text("❌ Некорректная сумма пополнения. Повторите ввод."); await state.clear(); return
        ym_wallet = (get_setting("yoomoney_wallet") or "").strip()
        if not ym_wallet: await callback.message.edit_text("❌ Оплата через ЮMoney временно недоступна."); await state.clear(); return
        user_id = callback.from_user.id
        payment_id = str(uuid.uuid4())
        metadata = {"payment_id": payment_id, "user_id": user_id, "price": float(amount), "action": "top_up", "payment_method": "YooMoney"}
        try: create_pending_transaction(payment_id, user_id, float(amount), metadata)
        except Exception as e: logger.warning(f"YooMoney пополнение: не удалось создать ожидающую транзакцию: {e}")
        try: success_url = f"https://t.me/{TELEGRAM_BOT_USERNAME}" if TELEGRAM_BOT_USERNAME else None
        except: success_url = None
        def _build_yoomoney_quickpay_url(wallet: str, amount: float, label: str, success_url: Optional[str] = None, targets: Optional[str] = None) -> str:
            try:
                params = {"receiver": wallet, "quickpay-form": "shop", "sum": f"{float(amount):.2f}", "label": label}
                if success_url: params["successURL"] = success_url
                if targets: params["targets"] = targets
                return f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(params)}"
            except: return "https://yoomoney.ru/"
        pay_url = _build_yoomoney_quickpay_url(wallet=ym_wallet, amount=float(amount), label=payment_id, success_url=success_url, targets=f"Пополнение на {amount:.2f} RUB")
        await state.clear()
        await callback.message.edit_text("Нажмите на кнопку ниже для оплаты. После оплаты нажмите 'Проверить оплату':", reply_markup=create_payment_with_check_keyboard(pay_url, f"check_yoomoney_{payment_id}"))
    @user_router.callback_query(F.data.startswith("check_yoomoney_"))
    async def check_yoomoney_status(callback: types.CallbackQuery, bot: Bot):
        await callback.answer("Проверяю оплату…")
        payment_id = callback.data[len("check_yoomoney_"):]
        if not payment_id: await callback.answer("❌ Некорректные данные для проверки.", show_alert=True); return
        async def _yoomoney_find_payment(label: str) -> Optional[dict]:
            token = (get_setting("yoomoney_api_token") or "").strip()
            if not token: logger.warning("YooMoney: API токен не задан."); return None
            url = "https://yoomoney.ru/api/operation-history"
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/x-www-form-urlencoded"}
            data = {"label": label, "records": "5"}
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, data=data, headers=headers, timeout=15) as resp:
                        text = await resp.text()
                        if resp.status != 200: logger.warning(f"YooMoney: operation-history HTTP {resp.status}: {text}"); return None
                        try: payload = await resp.json()
                        except:
                            try: payload = json.loads(text)
                            except: logger.warning("YooMoney: не удалось распарсить JSON"); return None
                        ops = payload.get("operations") or []
                        for op in ops:
                            if str(op.get("label")) == str(label) and str(op.get("direction")) == "in":
                                status = str(op.get("status") or "").lower()
                                if status == "success":
                                    try: amount = float(op.get("amount"))
                                    except: amount = None
                                    return {"operation_id": op.get("operation_id"), "amount": amount, "datetime": op.get("datetime")}
                        return None
            except Exception as e: logger.error(f"YooMoney: ошибка запроса operation-history: {e}", exc_info=True); return None
        op = await _yoomoney_find_payment(payment_id)
        if not op: await callback.answer("Платёж не найден или не завершён. Подождите и попробуйте ещё раз.", show_alert=True); return
        try: amount_rub = float(op.get('amount', 0)) if isinstance(op.get('amount', 0), (int, float)) else None
        except: amount_rub = None
        md = find_and_complete_pending_transaction(payment_id=payment_id, amount_rub=amount_rub, payment_method="YooMoney", currency_name="RUB", amount_currency=None)
        if not md: await callback.message.edit_text("❌ Не удалось завершить транзакцию. Обратитесь в поддержку, если средства списаны."); return
        try: await process_successful_payment(bot, md)
        except Exception as e: logger.error(f"YooMoney: не удалось обработать успешный платеж: {e}", exc_info=True); await callback.message.edit_text("❌ Ошибка при выдаче после оплаты. Напишите в поддержку."); return
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_stars")
    async def topup_pay_stars(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer("Готовлю счёт в Stars…")
        data = await state.get_data()
        amount_rub = Decimal(str(data.get('topup_amount', 0)))
        if amount_rub <= 0: await callback.message.edit_text("❌ Некорректная сумма пополнения. Повторите ввод."); await state.clear(); return
        stars_count = _calc_stars_amount(amount_rub.quantize(Decimal("0.01")))
        payment_id = str(uuid.uuid4())
        metadata = {"user_id": callback.from_user.id, "price": float(amount_rub), "action": "top_up", "payment_method": "Stars"}
        try: create_pending_transaction(payment_id, callback.from_user.id, float(amount_rub), metadata)
        except Exception as e: logger.warning(f"Stars пополнение: не удалось создать ожидающую транзакцию: {e}")
        payload = payment_id
        title = (get_setting("stars_title") or "Пополнение баланса")
        description = (get_setting("stars_description") or f"Пополнение на {amount_rub} RUB")
        try:
            await bot.send_invoice(chat_id=callback.message.chat.id, title=title, description=description, payload=payload, currency="XTR", prices=[types.LabeledPrice(label="Пополнение", amount=stars_count)])
            await state.clear()
        except Exception as e: logger.error(f"Не удалось отправить счет Stars для пополнения: {e}"); await callback.message.edit_text("❌ Не удалось создать счёт Stars. Попробуйте другой способ оплаты."); await state.clear()
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, (F.data == "topup_pay_cryptobot") | (F.data == "topup_pay_heleket"))
    async def topup_pay_heleket_like(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Создаю счёт...")
        data = await state.get_data()
        user_id = callback.from_user.id
        amount = float(data.get('topup_amount', 0))
        if amount <= 0: await callback.message.edit_text("❌ Некорректная сумма пополнения. Повторите ввод."); await state.clear(); return
        state_data = {"action": "top_up", "customer_email": None, "plan_id": None, "host_name": None, "key_id": None}
        try:
            if callback.data == "topup_pay_cryptobot":
                async def _create_cryptobot_invoice(user_id: int, price_rub: float, months: int, host_name: str, state_data: dict) -> Optional[tuple[str, int]]:
                    try:
                        token = get_setting("cryptobot_token")
                        if not token: logger.error("CryptoBot: не задан cryptobot_token"); return None
                        async def get_usdt_rub_rate() -> Optional[Decimal]:
                            try:
                                url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub"
                                async with aiohttp.ClientSession() as session:
                                    async with session.get(url, timeout=10) as resp:
                                        if resp.status != 200: logger.warning(f"USDT/RUB: HTTP {resp.status}"); return None
                                        data = await resp.json()
                                        val = data.get("tether", {}).get("rub")
                                        return Decimal(str(val)) if val is not None else None
                            except Exception as e: logger.warning(f"USDT/RUB: ошибка получения курса: {e}"); return None
                        rate = await get_usdt_rub_rate()
                        if not rate or rate <= 0: logger.error("CryptoBot: не удалось получить курс USDT/RUB"); return None
                        amount_usdt = (Decimal(str(price_rub)) / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                        payload_parts = [str(user_id), str(months), str(float(price_rub)), str(state_data.get("action")), str(state_data.get("key_id")), str(host_name or ""), str(state_data.get("plan_id")), str(state_data.get("customer_email")), "CryptoBot", str(state_data.get("promo_code") or "")]
                        payload = ":".join(payload_parts)
                        cp = CryptoPay(token)
                        invoice = await cp.create_invoice(asset="USDT", amount=float(amount_usdt), description="VPN оплата", payload=payload)
                        pay_url = getattr(invoice, "pay_url", None) or getattr(invoice, "bot_invoice_url", None)
                        invoice_id = getattr(invoice, "invoice_id", None)
                        if not pay_url and isinstance(invoice, dict): pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url") or invoice.get("url")
                        if not invoice_id and isinstance(invoice, dict): invoice_id = invoice.get("invoice_id")
                        if not pay_url: logger.error(f"CryptoBot: не удалось получить ссылку на оплату: {invoice}"); return None
                        if not invoice_id: logger.error(f"CryptoBot: не удалось получить invoice_id: {invoice}"); return None
                        return (str(pay_url), int(invoice_id))
                    except Exception as e: logger.error(f"CryptoBot: ошибка при создании счёта: {e}", exc_info=True); return None
                result = await _create_cryptobot_invoice(user_id=user_id, price_rub=float(amount), months=0, host_name="", state_data=state_data)
                if result:
                    pay_url, invoice_id = result
                    await state.update_data(cryptobot_invoice_id=invoice_id)
                    await state.set_state(TopUpProcess.waiting_for_cryptobot_topup_payment)
                    await callback.message.edit_text("Нажмите на кнопку ниже для оплаты:\n\n💡 После оплаты нажмите «Проверить платёж» для подтверждения.", reply_markup=create_payment_with_check_keyboard(pay_url, "check_cryptobot_topup_payment"))
                else: await callback.message.edit_text("❌ Не удалось создать счёт CryptoBot. Попробуйте другой способ оплаты."); await state.clear()
            else:
                async def _create_heleket_payment_request(user_id: int, price: float, months: int, host_name: str, state_data: dict) -> Optional[str]:
                    try:
                        merchant_id = get_setting("heleket_merchant_id")
                        api_key = get_setting("heleket_api_key")
                        if not merchant_id or not api_key: logger.error("Heleket: отсутствуют merchant_id/api_key."); return None
                        metadata = {"payment_id": str(uuid.uuid4()), "user_id": user_id, "months": months, "price": float(price), "action": state_data.get("action"), "key_id": state_data.get("key_id"), "host_name": host_name, "plan_id": state_data.get("plan_id"), "customer_email": state_data.get("customer_email"), "payment_method": "Crypto", "promo_code": state_data.get("promo_code"), "promo_discount_percent": state_data.get('promo_discount_percent'), "promo_discount_amount": state_data.get('promo_discount_amount')}
                        dom_val = get_setting("domain")
                        domain = (dom_val or "").strip() if isinstance(dom_val, str) else dom_val
                        callback_url = None
                        try:
                            if domain: callback_url = f"{str(domain).rstrip('/')}/heleket-webhook"
                        except: callback_url = None
                        success_url = None
                        try:
                            if TELEGRAM_BOT_USERNAME: success_url = f"https://t.me/{TELEGRAM_BOT_USERNAME}"
                        except: success_url = None
                        data = {"merchant_id": merchant_id, "order_id": str(uuid.uuid4()), "amount": float(price), "currency": "RUB", "description": json.dumps(metadata, ensure_ascii=False, separators=(",", ":"))}
                        if callback_url: data["callback_url"] = callback_url
                        if success_url: data["success_url"] = success_url
                        sorted_data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
                        base64_encoded = base64.b64encode(sorted_data_str.encode()).decode()
                        raw_string = f"{base64_encoded}{api_key}"
                        sign = hashlib.md5(raw_string.encode()).hexdigest()
                        payload = dict(data)
                        payload["sign"] = sign
                        api_base_val = get_setting("heleket_api_base")
                        api_base = (api_base_val or "https://api.heleket.com").rstrip("/")
                        endpoint = f"{api_base}/invoice/create"
                        async with aiohttp.ClientSession() as session:
                            try:
                                async with session.post(endpoint, json=payload, timeout=15) as resp:
                                    text = await resp.text()
                                    if resp.status not in (200, 201): logger.error(f"Heleket: не удалось создать счёт (HTTP {resp.status}): {text}"); return None
                                    try: data_json = await resp.json()
                                    except: logger.warning(f"Heleket: неожиданный ответ (не JSON): {text}"); return None
                                    pay_url = data_json.get("payment_url") or data_json.get("pay_url") or data_json.get("url")
                                    if not pay_url: logger.error(f"Heleket: не найдено поле URL в ответе: {data_json}"); return None
                                    return str(pay_url)
                            except Exception as e: logger.error(f"Heleket: ошибка HTTP при создании счёта: {e}", exc_info=True); return None
                    except Exception as e: logger.error(f"Heleket: общая ошибка при создании счёта: {e}", exc_info=True); return None
                pay_url = await _create_heleket_payment_request(user_id=user_id, price=float(amount), months=0, host_name="", state_data=state_data)
                if pay_url:
                    await callback.message.edit_text("Нажмите на кнопку ниже для оплаты:", reply_markup=create_payment_keyboard(pay_url))
                    await state.clear()
                else: await callback.message.edit_text("❌ Не удалось создать счёт. Попробуйте другой способ оплаты."); await state.clear()
        except Exception as e: logger.error(f"Не удалось создать счет для пополнения: {e}", exc_info=True); await callback.message.edit_text("❌ Не удалось создать счёт. Попробуйте другой способ оплаты."); await state.clear()
    @user_router.callback_query(TopUpProcess.waiting_for_cryptobot_topup_payment, F.data == "check_cryptobot_topup_payment")
    async def check_cryptobot_topup_payment_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer("Проверяю статус платежа...")
        data = await state.get_data()
        invoice_id = data.get('cryptobot_invoice_id')
        if not invoice_id: await callback.message.edit_text("❌ Не найден ID счета для проверки."); await state.clear(); return
        cryptobot_token = get_setting('cryptobot_token')
        if not cryptobot_token: logger.error("CryptoBot: не задан cryptobot_token для проверки платежа пополнения"); await callback.message.edit_text("❌ Ошибка конфигурации. Обратитесь к администратору."); await state.clear(); return
        try:
            cp = CryptoPay(cryptobot_token)
            invoices = await cp.get_invoices(invoice_ids=[invoice_id])
            if not invoices or len(invoices) == 0: await callback.answer("❌ Счет не найден", show_alert=True); return
            invoice = invoices[0]
            status = None
            try: status = getattr(invoice, "status", None)
            except: pass
            if not status and isinstance(invoice, dict): status = invoice.get("status")
            logger.info(f"CryptoBot проверка платежа пополнения: invoice_id={invoice_id}, status={status}")
            if status == "paid":
                await callback.answer("✅ Платеж найден! Обрабатываю...", show_alert=True)
                payload_string = None
                try: payload_string = getattr(invoice, "payload", None)
                except: pass
                if not payload_string and isinstance(invoice, dict): payload_string = invoice.get("payload")
                if not payload_string: logger.error(f"CryptoBot проверка пополнения: не найден payload для счета {invoice_id}"); await callback.message.edit_text("❌ Ошибка обработки платежа. Обратитесь в поддержку."); await state.clear(); return
                parts = payload_string.split(':')
                if len(parts) < 9: logger.error(f"CryptoBot проверка пополнения: некорректный формат payload: {payload_string}"); await callback.message.edit_text("❌ Ошибка обработки платежа. Обратитесь в поддержку."); await state.clear(); return
                metadata = {"user_id": parts[0], "months": parts[1], "price": parts[2], "action": parts[3], "key_id": parts[4], "host_name": parts[5], "plan_id": parts[6], "customer_email": parts[7] if parts[7] != 'None' else None, "payment_method": parts[8], "promo_code": (parts[9] if len(parts) > 9 and parts[9] else None)}
                await process_successful_payment(bot, metadata)
                await callback.message.edit_text("✅ Платеж успешно обработан! Баланс пополнен.")
                await state.clear()
            elif status == "active": await callback.answer("⏳ Платеж еще не получен. Пожалуйста, завершите оплату и нажмите кнопку снова.", show_alert=True)
            else: await callback.answer(f"❌ Статус платежа: {status}. Пожалуйста, создайте новый счет.", show_alert=True); await state.clear()
        except Exception as e: logger.error(f"CryptoBot: ошибка при проверке счёта пополнения {invoice_id}: {e}", exc_info=True); await callback.answer("❌ Ошибка при проверке платежа. Попробуйте позже.", show_alert=True)
    @user_router.callback_query(TopUpProcess.waiting_for_topup_method, F.data == "topup_pay_tonconnect")
    async def topup_pay_tonconnect(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Готовлю TON Connect...")
        data = await state.get_data()
        user_id = callback.from_user.id
        amount_rub = Decimal(str(data.get('topup_amount', 0)))
        if amount_rub <= 0: await callback.message.edit_text("❌ Некорректная сумма пополнения. Повторите ввод."); await state.clear(); return
        wallet_address = get_setting("ton_wallet_address")
        if not wallet_address: await callback.message.edit_text("❌ Оплата через TON временно недоступна."); await state.clear(); return
        async def get_usdt_rub_rate() -> Optional[Decimal]:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status != 200: logger.warning(f"USDT/RUB: HTTP {resp.status}"); return None
                        data = await resp.json()
                        val = data.get("tether", {}).get("rub")
                        return Decimal(str(val)) if val is not None else None
            except Exception as e: logger.warning(f"USDT/RUB: ошибка получения курса: {e}"); return None
        async def get_ton_usdt_rate() -> Optional[Decimal]:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=usd"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status != 200: logger.warning(f"TON/USD: HTTP {resp.status}"); return None
                        data = await resp.json()
                        usd = data.get("toncoin", {}).get("usd")
                        return Decimal(str(usd)) if usd is not None else None
            except Exception as e: logger.warning(f"TON/USD: ошибка получения курса: {e}"); return None
        usdt_rub_rate = await get_usdt_rub_rate()
        ton_usdt_rate = await get_ton_usdt_rate()
        if not usdt_rub_rate or not ton_usdt_rate: await callback.message.edit_text("❌ Не удалось получить курс TON. Попробуйте позже."); await state.clear(); return
        price_ton = (amount_rub / usdt_rub_rate / ton_usdt_rate).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        amount_nanoton = int(price_ton * 1_000_000_000)
        payment_id = str(uuid.uuid4())
        metadata = {"user_id": user_id, "price": float(amount_rub), "action": "top_up", "payment_method": "TON Connect"}
        create_pending_transaction(payment_id, user_id, float(amount_rub), metadata)
        transaction_payload = {'messages': [{'address': wallet_address, 'amount': str(amount_nanoton), 'payload': payment_id}], 'valid_until': int(datetime.now().timestamp()) + 600}
        try:
            def _start_ton_connect_process(user_id: int, transaction_payload: Dict) -> str:
                try:
                    messages = transaction_payload.get("messages") or []
                    if not messages: raise ValueError("transaction_payload.messages is empty")
                    msg = messages[0]
                    address = msg.get("address")
                    amount = msg.get("amount")
                    payload_text = msg.get("payload") or ""
                    if not address or not amount: raise ValueError("address/amount are required")
                    params = {"amount": amount}
                    if payload_text: params["text"] = str(payload_text)
                    query = urlencode(params)
                    return f"ton://transfer/{address}?{query}"
                except Exception as e: logger.error(f"TON генерация deep link не удалась: {e}"); return "ton://transfer"
            connect_url = _start_ton_connect_process(user_id, transaction_payload)
            qr_img = qrcode.make(connect_url)
            bio = BytesIO()
            qr_img.save(bio, "PNG")
            qr_file = BufferedInputFile(bio.getvalue(), "ton_qr.png")
            try: await callback.message.delete()
            except: pass
            await callback.message.answer_photo(photo=qr_file, caption=f"💎 **Оплата через TON Connect**\n\nСумма к оплате: `{price_ton}` **TON**\n\n✅ **Способ 1 (на телефоне):** Нажмите кнопку **'Открыть кошелек'** ниже.\n✅ **Способ 2 (на компьютере):** Отсканируйте QR-код кошельком.\n\nПосле подключения кошелька подтвердите транзакцию.", parse_mode="Markdown", reply_markup=create_ton_connect_keyboard(connect_url))
            await state.clear()
        except Exception as e: logger.error(f"Не удалось запустить TON Connect для пополнения: {e}", exc_info=True); await callback.message.answer("❌ Не удалось подготовить оплату TON Connect."); await state.clear()
    @user_router.callback_query(F.data == "show_referral_program")
    @registration_required
    async def referral_program_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_data = get_user(user_id)
        bot_username = (await callback.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start=ref_{user_id}"
        referral_count = get_referral_count(user_id)
        try: total_ref_earned = float(get_referral_balance_all(user_id))
        except: total_ref_earned = 0.0
        text = f"🤝 <b>Реферальная программа</b>\n\n<b>Ваша реферальная ссылка:</b>\n<code>{referral_link}</code>\n\n<b>Приглашено пользователей:</b> {referral_count}\n<b>Заработано по рефералке:</b> {total_ref_earned:.2f} RUB"
        builder = InlineKeyboardBuilder()
        builder.button(text="⬅️ Назад", callback_data="back_to_main_menu")
        await callback.message.edit_text(text, reply_markup=builder.as_markup())
    @user_router.callback_query(F.data == "show_about")
    @registration_required
    async def about_handler(callback: types.CallbackQuery):
        await callback.answer()
        about_text = get_setting("about_text")
        terms_url = get_setting("terms_url")
        privacy_url = get_setting("privacy_url")
        channel_url = get_setting("channel_url")
        final_text = about_text if about_text else "Информация о проекте не добавлена."
        builder = InlineKeyboardBuilder()
        if channel_url: builder.button(text=(get_setting("btn_channel") or "📰 Наш канал"), url=channel_url)
        if terms_url: builder.button(text=(get_setting("btn_terms") or "📄 Условия использования"), url=terms_url)
        if privacy_url: builder.button(text=(get_setting("btn_privacy") or "🔒 Политика конфиденциальности"), url=privacy_url)
        builder.button(text=(get_setting("btn_back_to_menu") or "⬅️ Назад в меню"), callback_data="back_to_main_menu")
        builder.adjust(1)
        await callback.message.edit_text(final_text, reply_markup=builder.as_markup(), disable_web_page_preview=True)
    @user_router.callback_query(F.data == "show_help")
    @registration_required
    async def about_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        support_text = get_setting("support_text") or "Раздел поддержки. Нажмите кнопку ниже, чтобы открыть чат с поддержкой."
        if support_bot_username: await callback.message.edit_text(support_text, reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else:
            support_user = get_setting("support_user")
            if support_user: await callback.message.edit_text("Для связи с поддержкой используйте кнопку ниже.", reply_markup=create_support_keyboard(support_user))
            else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.callback_query(F.data == "support_menu")
    @registration_required
    async def support_menu_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        support_text = get_setting("support_text") or "Раздел поддержки. Нажмите кнопку ниже, чтобы открыть чат с поддержкой."
        if support_bot_username: await callback.message.edit_text(support_text, reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else:
            support_user = get_setting("support_user")
            if support_user: await callback.message.edit_text("Для связи с поддержкой используйте кнопку ниже.", reply_markup=create_support_keyboard(support_user))
            else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.callback_query(F.data == "support_external")
    @registration_required
    async def support_external_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text(get_setting("support_text") or "Раздел поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username)); return
        support_user = get_setting("support_user")
        if not support_user: await callback.message.edit_text("Внешний контакт поддержки не настроен.", reply_markup=create_back_to_menu_keyboard()); return
        await callback.message.edit_text("Для связи с поддержкой используйте кнопку ниже.", reply_markup=create_support_keyboard(support_user))
    @user_router.callback_query(F.data == "support_new_ticket")
    @registration_required
    async def support_new_ticket_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text("Раздел поддержки вынесен в отдельного бота.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.message(SupportDialog.waiting_for_subject)
    @registration_required
    async def support_subject_received(message: types.Message, state: FSMContext):
        await state.clear()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await message.answer("Создание тикетов доступно в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await message.answer("Контакты поддержки не настроены.")
    @user_router.message(SupportDialog.waiting_for_message)
    @registration_required
    async def support_message_received(message: types.Message, state: FSMContext, bot: Bot):
        await state.clear()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await message.answer("Создание тикетов доступно в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await message.answer("Контакты поддержки не настроены.")
    @user_router.callback_query(F.data == "support_my_tickets")
    @registration_required
    async def support_my_tickets_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text("Список обращений доступен в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.callback_query(F.data.startswith("support_view_"))
    @registration_required
    async def support_view_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text("Просмотр тикетов доступен в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.callback_query(F.data.startswith("support_reply_"))
    @registration_required
    async def support_reply_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.clear()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text("Отправка ответов доступна в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.message(SupportDialog.waiting_for_reply)
    @registration_required
    async def support_reply_received(message: types.Message, state: FSMContext, bot: Bot):
        await state.clear()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await message.answer("Отправка ответов доступна в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username))
        else: await message.answer("Контакты поддержки не настроены.")
    @user_router.message(F.is_topic_message == True)
    async def forum_thread_message_handler(message: types.Message, bot: Bot):
        try:
            support_bot_username = get_setting("support_bot_username")
            me = await bot.get_me()
            if support_bot_username and (me.username or "").lower() != support_bot_username.lower(): return
            if not message.message_thread_id: return
            forum_chat_id = message.chat.id
            thread_id = message.message_thread_id
            ticket = get_ticket_by_thread(str(forum_chat_id), int(thread_id))
            if not ticket: return
            user_id = int(ticket.get('user_id'))
            if message.from_user and message.from_user.id == me.id: return
            is_admin_by_setting = is_admin(message.from_user.id); is_admin_in_chat = False
            try:
                member = await bot.get_chat_member(chat_id=forum_chat_id, user_id=message.from_user.id)
                is_admin_in_chat = member.status in [ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR]
            except: pass
            if not (is_admin_by_setting or is_admin_in_chat): return
            content = (message.text or message.caption or "").strip()
            if content: add_support_message(ticket_id=int(ticket['ticket_id']), sender='admin', content=content)
            header = await bot.send_message(chat_id=user_id, text=f"💬 Ответ поддержки по тикету #{ticket['ticket_id']}")
            try: await bot.copy_message(chat_id=user_id, from_chat_id=message.chat.id, message_id=message.message_id, reply_to_message_id=header.message_id)
            except:
                if content: await bot.send_message(chat_id=user_id, text=content)
        except Exception as e: logger.warning(f"Не удалось переслать сообщение из форумной темы: {e}")
    @user_router.callback_query(F.data.startswith("support_close_"))
    @registration_required
    async def support_close_ticket_handler(callback: types.CallbackQuery):
        await callback.answer()
        support_bot_username = get_setting("support_bot_username")
        if support_bot_username: await callback.message.edit_text("Управление тикетами доступно в отдельном боте поддержки.", reply_markup=create_support_bot_link_keyboard(support_bot_username)); return
        await callback.message.edit_text("Контакты поддержки не настроены.", reply_markup=create_back_to_menu_keyboard())
    @user_router.callback_query(F.data == "manage_keys")
    @registration_required
    async def manage_keys_handler(callback: types.CallbackQuery):
        await callback.answer()
        user_id = callback.from_user.id
        user_keys = get_user_keys(user_id)
        await callback.message.edit_text("Ваши ключи:" if user_keys else "У вас пока нет ключей.", reply_markup=create_keys_management_keyboard(user_keys))
    @user_router.callback_query(F.data == "get_trial")
    @registration_required
    async def trial_period_handler(callback: types.CallbackQuery, state: FSMContext):
        user_id = callback.from_user.id
        user_db_data = get_user(user_id)
        if user_db_data and user_db_data.get('trial_used'): await callback.answer("Вы уже использовали бесплатный пробный период.", show_alert=True); return
        hosts = get_all_hosts()
        if not hosts: await callback.message.edit_text("❌ В данный момент нет доступных серверов для создания пробного ключа."); return
        if len(hosts) == 1:
            await callback.answer()
            async def process_trial_key_creation(message: types.Message, host_name: str):
                user_id = message.chat.id
                await message.edit_text(f"Отлично! Создаю для вас бесплатный ключ на {get_setting('trial_duration_days')} дня на сервере \"{host_name}\"...")
                try:
                    user_data = get_user(user_id) or {}
                    raw_username = (user_data.get('username') or f'user{user_id}').lower()
                    username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
                    base_local = f"trial_{username_slug}"
                    candidate_local = base_local
                    attempt = 1
                    while True:
                        candidate_email = f"{candidate_local}@bot.local"
                        if not get_key_by_email(candidate_email): break
                        attempt += 1
                        candidate_local = f"{base_local}-{attempt}"
                        if attempt > 100:
                            candidate_local = f"{base_local}-{int(datetime.now().timestamp())}"
                            candidate_email = f"{candidate_local}@bot.local"
                            break
                    result = await create_or_update_key_on_host(host_name=host_name, email=candidate_email, days_to_add=int(get_setting("trial_duration_days")))
                    if not result: await message.edit_text("❌ Не удалось создать пробный ключ. Ошибка на сервере."); return
                    set_trial_used(user_id)
                    new_key_id = add_new_key(user_id=user_id, host_name=host_name, xui_client_uuid=result['client_uuid'], key_email=result['email'], expiry_timestamp_ms=result['expiry_timestamp_ms'])
                    new_expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
                    final_text = f"🎉 <b>Ваш ключ #{get_next_key_number(user_id) - 1} готов!</b>\n\n⏳ <b>Он будет действовать до:</b> {new_expiry_date.strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(result['connection_string'])}"
                    try: await message.edit_text(text=final_text, reply_markup=create_key_info_keyboard(new_key_id), disable_web_page_preview=True)
                    except TelegramBadRequest:
                        try: await message.delete()
                        except: pass
                        await message.answer(text=final_text, reply_markup=create_key_info_keyboard(new_key_id))
                except Exception as e: logger.error(f"Ошибка создания пробного ключа для пользователя {user_id} на хосте {host_name}: {e}", exc_info=True); await message.edit_text("❌ Произошла ошибка при создании пробного ключа.")
            await process_trial_key_creation(callback.message, hosts[0]['host_name'])
        else:
            await callback.answer()
            await callback.message.edit_text("Выберите сервер, на котором хотите получить пробный ключ:", reply_markup=create_host_selection_keyboard(hosts, action="trial"))
    @user_router.callback_query(F.data.startswith("select_host:"))
    @registration_required
    async def select_host_callback_handler(callback: types.CallbackQuery):
        parsed = parse_host_callback_data(callback.data)
        if not parsed: await callback.answer("Некорректные данные выбора сервера.", show_alert=True); return
        action, extra, token = parsed
        hosts = get_all_hosts()
        host_entry = find_host_by_callback_token(hosts, token)
        if not host_entry: await callback.answer("Сервер не найден.", show_alert=True); return
        host_name = host_entry.get('host_name')
        if action == "trial":
            await callback.answer()
            async def process_trial_key_creation(message: types.Message, host_name: str):
                user_id = message.chat.id
                await message.edit_text(f"Отлично! Создаю для вас бесплатный ключ на {get_setting('trial_duration_days')} дня на сервере \"{host_name}\"...")
                try:
                    user_data = get_user(user_id) or {}
                    raw_username = (user_data.get('username') or f'user{user_id}').lower()
                    username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
                    base_local = f"trial_{username_slug}"
                    candidate_local = base_local
                    attempt = 1
                    while True:
                        candidate_email = f"{candidate_local}@bot.local"
                        if not get_key_by_email(candidate_email): break
                        attempt += 1
                        candidate_local = f"{base_local}-{attempt}"
                        if attempt > 100:
                            candidate_local = f"{base_local}-{int(datetime.now().timestamp())}"
                            candidate_email = f"{candidate_local}@bot.local"
                            break
                    result = await create_or_update_key_on_host(host_name=host_name, email=candidate_email, days_to_add=int(get_setting("trial_duration_days")))
                    if not result: await message.edit_text("❌ Не удалось создать пробный ключ. Ошибка на сервере."); return
                    set_trial_used(user_id)
                    new_key_id = add_new_key(user_id=user_id, host_name=host_name, xui_client_uuid=result['client_uuid'], key_email=result['email'], expiry_timestamp_ms=result['expiry_timestamp_ms'])
                    new_expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000)
                    final_text = f"🎉 <b>Ваш ключ #{get_next_key_number(user_id) - 1} готов!</b>\n\n⏳ <b>Он будет действовать до:</b> {new_expiry_date.strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(result['connection_string'])}"
                    try: await message.edit_text(text=final_text, reply_markup=create_key_info_keyboard(new_key_id), disable_web_page_preview=True)
                    except TelegramBadRequest:
                        try: await message.delete()
                        except: pass
                        await message.answer(text=final_text, reply_markup=create_key_info_keyboard(new_key_id))
                except Exception as e: logger.error(f"Ошибка создания пробного ключа для пользователя {user_id} на хосте {host_name}: {e}", exc_info=True); await message.edit_text("❌ Произошла ошибка при создании пробного ключа.")
            await process_trial_key_creation(callback.message, host_name); return
        if action == "new":
            await callback.answer()
            plans = get_plans_for_host(host_name)
            if not plans: await callback.message.edit_text(f"❌ Для сервера \"{host_name}\" не настроены тарифы."); return
            await callback.message.edit_text("Выберите тариф:", reply_markup=create_plans_keyboard(plans, action="new", host_name=host_name))
            return
        if action == "switch":
            try: key_id = int(extra)
            except: await callback.answer("Некорректные данные выбора сервера.", show_alert=True); return
            async def handle_switch_host(callback: types.CallbackQuery, key_id: int, new_host_name: str):
                key_data = get_key_by_id(key_id)
                if not key_data or key_data.get('user_id') != callback.from_user.id: await callback.answer("Ключ не найден.", show_alert=True); return
                old_host = key_data.get('host_name')
                if not old_host: await callback.answer("Для ключа не указан текущий сервер.", show_alert=True); return
                if new_host_name == old_host: await callback.answer("Это уже текущий сервер.", show_alert=True); return
                try: expiry_dt = datetime.fromisoformat(key_data['expiry_date']); expiry_timestamp_ms_exact = int(expiry_dt.timestamp() * 1000)
                except: now_dt = datetime.now(); expiry_timestamp_ms_exact = int((now_dt + timedelta(days=1)).timestamp() * 1000)
                email = key_data.get('key_email')
                if not email: await callback.answer("Не удалось определить email ключа. Обратитесь в поддержку.", show_alert=True); return
                await callback.answer()
                await callback.message.edit_text(f"⏳ Переношу ключ на сервер \"{new_host_name}\"...")
                try:
                    result = await create_or_update_key_on_host(new_host_name, email, days_to_add=None, expiry_timestamp_ms=expiry_timestamp_ms_exact)
                    if not result: await callback.message.edit_text(f"❌ Не удалось перенести ключ на сервер \"{new_host_name}\". Попробуйте позже."); return
                    try: await delete_client_on_host(old_host, email)
                    except: pass
                    update_key_host_and_info(key_id=key_id, new_host_name=new_host_name, new_xui_uuid=result['client_uuid'], new_expiry_ms=result['expiry_timestamp_ms'])
                    try:
                        updated_key = get_key_by_id(key_id)
                        details = await get_key_details_from_host(updated_key)
                        if details and details.get('connection_string'):
                            connection_string = details['connection_string']
                            expiry_date = datetime.fromisoformat(updated_key['expiry_date'])
                            created_date = datetime.fromisoformat(updated_key['created_date'])
                            all_user_keys = get_user_keys(callback.from_user.id)
                            key_number = next((i + 1 for i, k in enumerate(all_user_keys) if k['key_id'] == key_id), 0)
                            final_text = f"<b>🔑 Информация о ключе #{key_number}</b>\n\n<b>➕ Приобретён:</b> {created_date.strftime('%d.%m.%Y в %H:%M')}\n<b>⏳ Действителен до:</b> {expiry_date.strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(connection_string)}"
                            await callback.message.edit_text(text=final_text, reply_markup=create_key_info_keyboard(key_id))
                        else: await callback.message.edit_text(f"✅ Готово! Ключ перенесён на сервер \"{new_host_name}\".\nОбновите подписку/конфиг в клиенте, если требуется.", reply_markup=create_back_to_menu_keyboard())
                    except: await callback.message.edit_text(f"✅ Готово! Ключ перенесён на сервер \"{new_host_name}\".\nОбновите подписку/конфиг в клиенте, если требуется.", reply_markup=create_back_to_menu_keyboard())
                except Exception as e: logger.error(f"Ошибка переключения ключа {key_id} на хост {new_host_name}: {e}", exc_info=True); await callback.message.edit_text("❌ Произошла ошибка при переносе ключа. Попробуйте позже.")
            await handle_switch_host(callback, key_id, host_name); return
        await callback.answer("Неизвестное действие.", show_alert=True)
    @user_router.callback_query(F.data.startswith("show_key_"))
    @registration_required
    async def show_key_handler(callback: types.CallbackQuery):
        key_id_to_show = int(callback.data.split("_")[2])
        await callback.message.edit_text("Загружаю информацию о ключе...")
        user_id = callback.from_user.id
        key_data = get_key_by_id(key_id_to_show)
        if not key_data or key_data['user_id'] != user_id: await callback.message.edit_text("❌ Ошибка: ключ не найден."); return
        try:
            details = await get_key_details_from_host(key_data)
            if not details or not details['connection_string']: await callback.message.edit_text("❌ Ошибка на сервере. Не удалось получить данные ключа."); return
            connection_string = details['connection_string']
            expiry_date = datetime.fromisoformat(key_data['expiry_date'])
            created_date = datetime.fromisoformat(key_data['created_date'])
            all_user_keys = get_user_keys(user_id)
            key_number = next((i + 1 for i, key in enumerate(all_user_keys) if key['key_id'] == key_id_to_show), 0)
            final_text = f"<b>🔑 Информация о ключе #{key_number}</b>\n\n<b>➕ Приобретён:</b> {created_date.strftime('%d.%m.%Y в %H:%M')}\n<b>⏳ Действителен до:</b> {expiry_date.strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(connection_string)}"
            await callback.message.edit_text(text=final_text, reply_markup=create_key_info_keyboard(key_id_to_show))
        except Exception as e: logger.error(f"Ошибка показа ключа {key_id_to_show}: {e}"); await callback.message.edit_text("❌ Произошла ошибка при получении данных ключа.")
    @user_router.callback_query(F.data.startswith("switch_server_"))
    @registration_required
    async def switch_server_start(callback: types.CallbackQuery):
        await callback.answer()
        try: key_id = int(callback.data[len("switch_server_"):])
        except ValueError: await callback.answer("Некорректный идентификатор ключа.", show_alert=True); return
        key_data = get_key_by_id(key_id)
        if not key_data or key_data.get('user_id') != callback.from_user.id: await callback.answer("Ключ не найден.", show_alert=True); return
        hosts = get_all_hosts()
        if not hosts: await callback.answer("Нет доступных серверов.", show_alert=True); return
        current_host = key_data.get('host_name')
        hosts = [h for h in hosts if h.get('host_name') != current_host]
        if not hosts: await callback.answer("Другие серверы отсутствуют.", show_alert=True); return
        await callback.message.edit_text("Выберите новый сервер (локацию) для этого ключа:", reply_markup=create_host_selection_keyboard(hosts, action=f"switch_{key_id}"))
    @user_router.callback_query(F.data.startswith("select_host_switch_"))
    @registration_required
    async def select_host_for_switch(callback: types.CallbackQuery):
        payload = callback.data[len("select_host_switch_"):]
        parts = payload.split("_", 1)
        if len(parts) != 2: await callback.answer("Некорректные данные выбора сервера.", show_alert=True); return
        try: key_id = int(parts[0])
        except ValueError: await callback.answer("Некорректный идентификатор ключа.", show_alert=True); return
        new_host_name = parts[1]
        async def handle_switch_host(callback: types.CallbackQuery, key_id: int, new_host_name: str):
            key_data = get_key_by_id(key_id)
            if not key_data or key_data.get('user_id') != callback.from_user.id: await callback.answer("Ключ не найден.", show_alert=True); return
            old_host = key_data.get('host_name')
            if not old_host: await callback.answer("Для ключа не указан текущий сервер.", show_alert=True); return
            if new_host_name == old_host: await callback.answer("Это уже текущий сервер.", show_alert=True); return
            try: expiry_dt = datetime.fromisoformat(key_data['expiry_date']); expiry_timestamp_ms_exact = int(expiry_dt.timestamp() * 1000)
            except: now_dt = datetime.now(); expiry_timestamp_ms_exact = int((now_dt + timedelta(days=1)).timestamp() * 1000)
            email = key_data.get('key_email')
            if not email: await callback.answer("Не удалось определить email ключа. Обратитесь в поддержку.", show_alert=True); return
            await callback.answer()
            await callback.message.edit_text(f"⏳ Переношу ключ на сервер \"{new_host_name}\"...")
            try:
                result = await create_or_update_key_on_host(new_host_name, email, days_to_add=None, expiry_timestamp_ms=expiry_timestamp_ms_exact)
                if not result: await callback.message.edit_text(f"❌ Не удалось перенести ключ на сервер \"{new_host_name}\". Попробуйте позже."); return
                try: await delete_client_on_host(old_host, email)
                except: pass
                update_key_host_and_info(key_id=key_id, new_host_name=new_host_name, new_xui_uuid=result['client_uuid'], new_expiry_ms=result['expiry_timestamp_ms'])
                try:
                    updated_key = get_key_by_id(key_id)
                    details = await get_key_details_from_host(updated_key)
                    if details and details.get('connection_string'):
                        connection_string = details['connection_string']
                        expiry_date = datetime.fromisoformat(updated_key['expiry_date'])
                        created_date = datetime.fromisoformat(updated_key['created_date'])
                        all_user_keys = get_user_keys(callback.from_user.id)
                        key_number = next((i + 1 for i, k in enumerate(all_user_keys) if k['key_id'] == key_id), 0)
                        final_text = f"<b>🔑 Информация о ключе #{key_number}</b>\n\n<b>➕ Приобретён:</b> {created_date.strftime('%d.%m.%Y в %H:%M')}\n<b>⏳ Действителен до:</b> {expiry_date.strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(connection_string)}"
                        await callback.message.edit_text(text=final_text, reply_markup=create_key_info_keyboard(key_id))
                    else: await callback.message.edit_text(f"✅ Готово! Ключ перенесён на сервер \"{new_host_name}\".\nОбновите подписку/конфиг в клиенте, если требуется.", reply_markup=create_back_to_menu_keyboard())
                except: await callback.message.edit_text(f"✅ Готово! Ключ перенесён на сервер \"{new_host_name}\".\nОбновите подписку/конфиг в клиенте, если требуется.", reply_markup=create_back_to_menu_keyboard())
            except Exception as e: logger.error(f"Ошибка переключения ключа {key_id} на хост {new_host_name}: {e}", exc_info=True); await callback.message.edit_text("❌ Произошла ошибка при переносе ключа. Попробуйте позже.")
        await handle_switch_host(callback, key_id, new_host_name)
    @user_router.callback_query(F.data.startswith("show_qr_"))
    @registration_required
    async def show_qr_handler(callback: types.CallbackQuery):
        await callback.answer("Генерирую QR-код...")
        key_id = int(callback.data.split("_")[2])
        key_data = get_key_by_id(key_id)
        if not key_data or key_data['user_id'] != callback.from_user.id: return
        try:
            details = await get_key_details_from_host(key_data)
            if not details or not details['connection_string']: await callback.answer("Ошибка: Не удалось сгенерировать QR-код.", show_alert=True); return
            connection_string = details['connection_string']
            qr_img = qrcode.make(connection_string)
            bio = BytesIO(); qr_img.save(bio, "PNG"); bio.seek(0)
            qr_code_file = BufferedInputFile(bio.read(), filename="vpn_qr.png")
            await callback.message.answer_photo(photo=qr_code_file)
        except Exception as e: logger.error(f"Ошибка показа QR-кода для ключа {key_id}: {e}")
    @user_router.callback_query(F.data.startswith("howto_vless_"))
    @registration_required
    async def show_instruction_handler(callback: types.CallbackQuery):
        await callback.answer()
        key_id = int(callback.data.split("_")[2])
        try: await callback.message.edit_text("Выберите вашу платформу для инструкции по подключению VLESS:", reply_markup=create_howto_vless_keyboard_key(key_id), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data.startswith("howto_vless"))
    @registration_required
    async def show_instruction_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: await callback.message.edit_text("Выберите вашу платформу для инструкции по подключению VLESS:", reply_markup=create_howto_vless_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "user_speedtest")
    @registration_required
    async def user_speedtest_handler(callback: types.CallbackQuery):
        await callback.answer()
        try:
            hosts = get_all_hosts() or []
            if not hosts: await callback.message.edit_text("⚠️ Хосты не найдены в настройках. Обратитесь к администратору.", reply_markup=create_back_to_menu_keyboard()); return
            text = "⚡️ <b>Последние результаты Speedtest</b>\n\n"
            for host in hosts:
                host_name = host.get('host_name', 'Неизвестный хост')
                latest_test = get_latest_speedtest(host_name)
                if latest_test:
                    ping = latest_test.get('ping_ms')
                    download = latest_test.get('download_mbps')
                    upload = latest_test.get('upload_mbps')
                    server = latest_test.get('server_name', '—')
                    method = latest_test.get('method', 'unknown').upper()
                    created_at = latest_test.get('created_at', '—')
                    try:
                        from datetime import datetime
                        if created_at and created_at != '—':
                            dt = datetime.fromisoformat(created_at.replace('Z', '+00:00'))
                            time_str = dt.strftime('%d.%m %H:%M')
                        else: time_str = created_at
                    except: time_str = created_at
                    ping_str = f"{ping:.2f}" if ping is not None else "—"
                    download_str = f"{download:.0f}" if download is not None else "—"
                    upload_str = f"{upload:.0f}" if upload is not None else "—"
                    text += f"• 🌏{host_name} — {method}: ✅ · ⏱️ {ping_str} ms · ↓ {download_str} Mbps · ↑ {upload_str} Mbps · 🕒 {time_str}\n"
                else: text += f"• 🌏{host_name} — Нет данных о тестах скорости\n"
            await callback.message.edit_text(text, reply_markup=create_back_to_menu_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "howto_android")
    @registration_required
    async def howto_android_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: await callback.message.edit_text((get_setting("howto_android_text") or ("<b>Подключение на Android</b>\n\n1. <b>Установите приложение V2RayTun:</b> Загрузите и установите приложение V2RayTun из Google Play Store.\n2. <b>Скопируйте свой ключ (vless://)</b> Перейдите в раздел «Моя подписка» в нашем боте и скопируйте свой ключ.\n3. <b>Импортируйте конфигурацию:</b>\n   • Откройте V2RayTun.\n   • Нажмите на значок + в правом нижнем углу.\n   • Выберите «Импортировать конфигурацию из буфера обмена» (или аналогичный пункт).\n4. <b>Выберите сервер:</b> Выберите появившийся сервер в списке.\n5. <b>Подключитесь к VPN:</b> Нажмите на кнопку подключения (значок «V» или воспроизведения). Возможно, потребуется разрешение на создание VPN-подключения.\n6. <b>Проверьте подключение:</b> После подключения проверьте свой IP-адрес, например, на https://whatismyipaddress.com/. Он должен отличаться от вашего реального IP.")), reply_markup=create_howto_vless_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "howto_ios")
    @registration_required
    async def howto_ios_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: await callback.message.edit_text((get_setting("howto_ios_text") or ("<b>Подключение на iOS (iPhone/iPad)</b>\n\n1. <b>Установите приложение V2RayTun:</b> Загрузите и установите приложение V2RayTun из App Store.\n2. <b>Скопируйте свой ключ (vless://):</b> Перейдите в раздел «Моя подписка» в нашем боте и скопируйте свой ключ.\n3. <b>Импортируйте конфигурацию:</b>\n   • Откройте V2RayTun.\n   • Нажмите на значок +.\n   • Выберите «Импортировать конфигурацию из буфера обмена» (или аналогичный пункт).\n4. <b>Выберите сервер:</b> Выберите появившийся сервер в списке.\n5. <b>Подключитесь к VPN:</b> Включите главный переключатель в V2RayTun. Возможно, потребуется разрешить создание VPN-подключения.\n6. <b>Проверьте подключение:</b> После подключения проверьте свой IP-адрес, например, на https://whatismyipaddress.com/. Он должен отличаться от вашего реального IP.")), reply_markup=create_howto_vless_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "howto_windows")
    @registration_required
    async def howto_windows_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: await callback.message.edit_text((get_setting("howto_windows_text") or ("<b>Подключение на Windows</b>\n\n1. <b>Установите приложение Nekoray:</b> Загрузите Nekoray с https://github.com/MatsuriDayo/Nekoray/releases. Выберите подходящую версию (например, Nekoray-x64.exe).\n2. <b>Распакуйте архив:</b> Распакуйте скачанный архив в удобное место.\n3. <b>Запустите Nekoray.exe:</b> Откройте исполняемый файл.\n4. <b>Скопируйте свой ключ (vless://)</b> Перейдите в раздел «Моя подписка» в нашем боте и скопируйте свой ключ.\n5. <b>Импортируйте конфигурацию:</b>\n   • В Nekoray нажмите «Сервер» (Server).\n   • Выберите «Импортировать из буфера обмена».\n   • Nekoray автоматически импортирует конфигурацию.\n6. <b>Обновите серверы (если нужно):</b> Если серверы не появились, нажмите «Серверы» → «Обновить все серверы».\n7. Сверху включите пункт 'Режим TUN' ('Tun Mode')\n8. <b>Выберите сервер:</b> В главном окне выберите появившийся сервер.\n9. <b>Подключитесь к VPN:</b> Нажмите «Подключить» (Connect).\n10. <b>Проверьте подключение:</b> Откройте браузер и проверьте IP на https://whatismyipaddress.com/. Он должен отличаться от вашего реального IP.")), reply_markup=create_howto_vless_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "howto_linux")
    @registration_required
    async def howto_linux_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: await callback.message.edit_text((get_setting("howto_linux_text") or ("<b>Подключение на Linux</b>\n\n1. <b>Скачайте и распакуйте Nekoray:</b> Перейдите на https://github.com/MatsuriDayo/Nekoray/releases и скачайте архив для Linux. Распакуйте его в удобную папку.\n2. <b>Запустите Nekoray:</b> Откройте терминал, перейдите в папку с Nekoray и выполните <code>./nekoray</code> (или используйте графический запуск, если доступен).\n3. <b>Скопируйте свой ключ (vless://)</b> Перейдите в раздел «Моя подписка» в нашем боте и скопируйте свой ключ.\n4. <b>Импортируйте конфигурацию:</b>\n   • В Nekoray нажмите «Сервер» (Server).\n   • Выберите «Импортировать из буфера обмена».\n   • Nekoray автоматически импортирует конфигурацию.\n5. <b>Обновите серверы (если нужно):</b> Если серверы не появились, нажмите «Серверы» → «Обновить все серверы».\n6. Сверху включите пункт 'Режим TUN' ('Tun Mode')\n7. <b>Выберите сервер:</b> В главном окне выберите появившийся сервер.\n8. <b>Подключитесь к VPN:</b> Нажмите «Подключить» (Connect).\n9. <b>Проверьте подключение:</b> Откройте браузер и проверьте IP на https://whatismyipaddress.com/. Он должен отличаться от вашего реального IP.")), reply_markup=create_howto_vless_keyboard(), disable_web_page_preview=True)
        except TelegramBadRequest: pass
    @user_router.callback_query(F.data == "buy_new_key")
    @registration_required
    async def buy_new_key_handler(callback: types.CallbackQuery):
        await callback.answer()
        hosts = get_all_hosts()
        if not hosts: await callback.message.edit_text("❌ В данный момент нет доступных серверов для покупки."); return
        await callback.message.edit_text("Выберите сервер, на котором хотите приобрести ключ:", reply_markup=create_host_selection_keyboard(hosts, action="new"))
    @user_router.callback_query(F.data.startswith("extend_key_"))
    @registration_required
    async def extend_key_handler(callback: types.CallbackQuery):
        await callback.answer()
        try: key_id = int(callback.data.split("_")[2])
        except (IndexError, ValueError): await callback.message.edit_text("❌ Произошла ошибка. Неверный формат ключа."); return
        key_data = get_key_by_id(key_id)
        if not key_data or key_data['user_id'] != callback.from_user.id: await callback.message.edit_text("❌ Ошибка: Ключ не найден или не принадлежит вам."); return
        host_name = key_data.get('host_name')
        if not host_name: await callback.message.edit_text("❌ Ошибка: У этого ключа не указан сервер. Обратитесь в поддержку."); return
        plans = get_plans_for_host(host_name)
        if not plans: await callback.message.edit_text(f"❌ Извините, для сервера \"{host_name}\" в данный момент не настроены тарифы для продления."); return
        await callback.message.edit_text(f"Выберите тариф для продления ключа на сервере \"{host_name}\":", reply_markup=create_plans_keyboard(plans=plans, action="extend", host_name=host_name, key_id=key_id))
    @user_router.callback_query(F.data.startswith("buy_"))
    @registration_required
    async def plan_selection_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        parts = callback.data.split("_")[1:]
        action = parts[-2]
        key_id = int(parts[-1])
        plan_id = int(parts[-3])
        host_name = "_".join(parts[:-3])
        await state.update_data(action=action, key_id=key_id, plan_id=plan_id, host_name=host_name)
        await callback.message.edit_text("📧 Пожалуйста, введите ваш email для отправки чека об оплате.\n\nЕсли вы не хотите указывать почту, нажмите кнопку ниже.", reply_markup=create_skip_email_keyboard())
        await state.set_state(PaymentProcess.waiting_for_email)
    @user_router.callback_query(PaymentProcess.waiting_for_email, F.data == "back_to_plans")
    async def back_to_plans_handler(callback: types.CallbackQuery, state: FSMContext):
        data = await state.get_data()
        action = data.get('action')
        host_name = data.get('host_name')
        key_id = data.get('key_id')
        try: await callback.answer()
        except: pass
        try:
            if action == 'extend' and host_name and (key_id is not None):
                plans = get_plans_for_host(host_name)
                if plans: await callback.message.edit_text(f"Выберите тариф для продления ключа на сервере \"{host_name}\":", reply_markup=create_plans_keyboard(plans=plans, action="extend", host_name=host_name, key_id=int(key_id)))
                else: await callback.message.edit_text(f"❌ Для сервера \"{host_name}\" не настроены тарифы.")
            elif action == 'new' and host_name:
                plans = get_plans_for_host(host_name)
                if plans: await callback.message.edit_text("Выберите тариф для нового ключа:", reply_markup=create_plans_keyboard(plans=plans, action="new", host_name=host_name))
                else: await callback.message.edit_text(f"❌ Для сервера \"{host_name}\" не настроены тарифы.")
            elif action == 'new':
                hosts = get_all_hosts()
                if not hosts: await callback.message.edit_text("❌ В данный момент нет доступных серверов для покупки.")
                else: await callback.message.edit_text("Выберите сервер, на котором хотите приобрести ключ:", reply_markup=create_host_selection_keyboard(hosts, action="new"))
            else: await show_main_menu(callback.message, edit_message=True)
        finally:
            try: await state.clear()
            except: pass
    @user_router.message(PaymentProcess.waiting_for_email)
    async def process_email_handler(message: types.Message, state: FSMContext):
        if is_valid_email(message.text):
            await state.update_data(customer_email=message.text)
            await message.answer(f"✅ Email принят: {message.text}")
            await show_payment_options(message, state)
            logger.info(f"Пользователь {message.chat.id}: Состояние установлено в waiting_for_payment_method через show_payment_options")
        else: await message.answer("❌ Неверный формат email. Попробуйте еще раз.")
    @user_router.callback_query(PaymentProcess.waiting_for_email, F.data == "skip_email")
    async def skip_email_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.update_data(customer_email=None)
        await show_payment_options(callback.message, state)
        logger.info(f"Пользователь {callback.from_user.id}: Состояние установлено в waiting_for_payment_method через show_payment_options")
    async def show_payment_options(message: types.Message, state: FSMContext):
        data = await state.get_data()
        user_data = get_user(message.chat.id)
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan:
            try: await message.edit_text("❌ Ошибка: Тариф не найден.")
            except TelegramBadRequest: await message.answer("❌ Ошибка: Тариф не найден.")
            await state.clear(); return
        price = Decimal(str(plan['price']))
        final_price = price
        message_text = "Выберите способ оплаты:"
        if user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            discount_percentage_str = get_setting("referral_discount") or "0"
            discount_percentage = Decimal(discount_percentage_str)
            if discount_percentage > 0:
                discount_amount = (price * discount_percentage / 100).quantize(Decimal("0.01"))
                final_price = price - discount_amount
                message_text = f"🎉 Как приглашенному пользователю, на вашу первую покупку предоставляется скидка {discount_percentage_str}%!\nСтарая цена: <s>{price:.2f} RUB</s>\n<b>Новая цена: {final_price:.2f} RUB</b>\n\n" + message_text
        promo_percent = data.get('promo_discount_percent')
        promo_amount = data.get('promo_discount_amount')
        promo_code = (data.get('promo_code') or '').strip()
        if promo_code:
            try:
                if promo_percent:
                    perc = Decimal(str(promo_percent))
                    if perc > 0:
                        discount_amount = (final_price * perc / 100).quantize(Decimal("0.01"))
                        final_price = (final_price - discount_amount).quantize(Decimal("0.01"))
                elif promo_amount:
                    amt = Decimal(str(promo_amount))
                    if amt > 0:
                        final_price = (final_price - amt).quantize(Decimal("0.01"))
                if final_price < Decimal('0'): final_price = Decimal('0.00')
                promo_line = f"Промокод {promo_code}: "
                if promo_percent: promo_line += f"скидка {Decimal(str(promo_percent)):.0f}%\n"
                elif promo_amount: promo_line += f"скидка {Decimal(str(promo_amount)):.2f} RUB\n"
                else: promo_line += "применён\n"
                message_text = f"{promo_line}Старая цена: <s>{price:.2f} RUB</s>\n<b>Новая цена: {final_price:.2f} RUB</b>\n\n" + message_text
            except: pass
        await state.update_data(final_price=float(final_price))
        try: main_balance = get_balance(message.chat.id)
        except: main_balance = 0.0
        show_balance_btn = main_balance >= float(final_price)
        try: await message.edit_text(message_text, reply_markup=create_payment_method_keyboard(payment_methods=PAYMENT_METHODS, action=data.get('action'), key_id=data.get('key_id'), show_balance=show_balance_btn, main_balance=main_balance, price=float(final_price), has_promo_applied=bool(promo_code)))
        except TelegramBadRequest: await message.answer(message_text, reply_markup=create_payment_method_keyboard(payment_methods=PAYMENT_METHODS, action=data.get('action'), key_id=data.get('key_id'), show_balance=show_balance_btn, main_balance=main_balance, price=float(final_price), has_promo_applied=bool(promo_code)))
        await state.set_state(PaymentProcess.waiting_for_payment_method)
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "back_to_email_prompt")
    async def back_to_email_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_text("📧 Пожалуйста, введите ваш email для отправки чека об оплате.\n\nЕсли вы не хотите указывать почту, нажмите кнопку ниже.", reply_markup=create_skip_email_keyboard())
        await state.set_state(PaymentProcess.waiting_for_email)
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "enter_promo_code")
    async def prompt_enter_promo(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await state.set_state(PaymentProcess.waiting_for_promo_code)
        await callback.message.edit_text("🎟️ Введите промокод текстом:")
    @user_router.message(PaymentProcess.waiting_for_promo_code)
    async def handle_promo_input(message: types.Message, state: FSMContext):
        code = (message.text or "").strip()
        if not code: await message.answer("❌ Пустой промокод. Введите код ещё раз."); return
        promo, reason = check_promo_code_available(code, message.from_user.id)
        if not promo:
            reasons = {"not_found": "❌ Промокод не найден.", "inactive": "❌ Промокод деактивирован.", "not_started": "❌ Промокод ещё не начал действовать.", "expired": "❌ Срок действия промокода истёк.", "total_limit_reached": "❌ Достигнут общий лимит использования промокода.", "user_limit_reached": "❌ Вы исчерпали лимит использования промокода.", "db_error": "❌ Ошибка базы данных. Попробуйте позже.", "empty_code": "❌ Пустой промокод."}
            await message.answer(reasons.get(reason or "not_found", "❌ Промокод недоступен."))
            await show_payment_options(message, state); return
        await state.update_data(promo_code=promo.get("code"), promo_discount_percent=promo.get("discount_percent"), promo_discount_amount=promo.get("discount_amount"))
        await message.answer("✅ Промокод применён.")
        await show_payment_options(message, state)
        await state.set_state(PaymentProcess.waiting_for_payment_method)
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "remove_promo_code")
    async def remove_promo(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        data = await state.get_data()
        data.pop('promo_code', None); data.pop('promo_discount_percent', None); data.pop('promo_discount_amount', None)
        await state.set_data(data)
        await callback.message.answer("Промокод удалён.")
        await show_payment_options(callback.message, state)
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_yookassa")
    async def create_yookassa_payment_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Создаю ссылку на оплату...")
        data = await state.get_data()
        user_data = get_user(callback.from_user.id)
        plan_id = data.get('plan_id')
        plan = get_plan_by_id(plan_id)
        if not plan: await callback.message.answer("Произошла ошибка при выборе тарифа."); await state.clear(); return
        base_price = Decimal(str(plan['price']))
        price_rub = base_price
        if user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            discount_percentage_str = get_setting("referral_discount") or "0"
            discount_percentage = Decimal(discount_percentage_str)
            if discount_percentage > 0:
                discount_amount = (base_price * discount_percentage / 100).quantize(Decimal("0.01"))
                price_rub = base_price - discount_amount
        final_price_decimal = price_rub
        try:
            final_price_from_state = data.get('final_price')
            if final_price_from_state is not None: final_price_decimal = Decimal(str(final_price_from_state)).quantize(Decimal("0.01"))
        except: pass
        if final_price_decimal < Decimal('0'): final_price_decimal = Decimal('0.00')
        customer_email = data.get('customer_email')
        host_name = data.get('host_name')
        action = data.get('action')
        key_id = data.get('key_id')
        if not customer_email: customer_email = get_setting("receipt_email")
        months = plan['months']
        user_id = callback.from_user.id
        try:
            price_str_for_api = f"{final_price_decimal:.2f}"
            price_float_for_metadata = float(final_price_decimal)
            receipt = None
            if customer_email and is_valid_email(customer_email):
                receipt = {"customer": {"email": customer_email}, "items": [{"description": f"Подписка на {months} мес.", "quantity": "1.00", "amount": {"value": price_str_for_api, "currency": "RUB"}, "vat_code": 1, "payment_subject": "service", "payment_mode": "full_payment"}]}
            payment_payload = {"amount": {"value": price_str_for_api, "currency": "RUB"}, "confirmation": {"type": "redirect", "return_url": f"https://t.me/{TELEGRAM_BOT_USERNAME}"}, "capture": True, "description": f"Подписка на {months} мес.", "metadata": {"user_id": str(user_id), "months": str(months), "price": f"{price_float_for_metadata:.2f}", "action": str(action) if action is not None else "", "key_id": (str(key_id) if key_id is not None else ""), "host_name": str(host_name) if host_name is not None else "", "plan_id": (str(plan_id) if plan_id is not None else ""), "customer_email": customer_email or "", "payment_method": "YooKassa", "promo_code": (data.get('promo_code') or ""), "promo_discount_percent": (str(data.get('promo_discount_percent')) if data.get('promo_discount_percent') is not None else ""), "promo_discount_amount": (str(data.get('promo_discount_amount')) if data.get('promo_discount_amount') is not None else "")}}
            if receipt: payment_payload['receipt'] = receipt
            payment = Payment.create(payment_payload, uuid.uuid4())
            await state.clear()
            await callback.message.edit_text("Нажмите на кнопку ниже для оплаты:", reply_markup=create_payment_keyboard(payment.confirmation.confirmation_url))
        except Exception as e: logger.error(f"Не удалось создать платеж YooKassa: {e}", exc_info=True); await callback.message.answer("Не удалось создать ссылку на оплату."); await state.clear()
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_yoomoney")
    async def create_yoomoney_payment_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Готовлю ссылку ЮMoney…")
        data = await state.get_data()
        user_data = get_user(callback.from_user.id)
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: await callback.message.edit_text("❌ Произошла ошибка при выборе тарифа."); await state.clear(); return
        base_price = Decimal(str(plan['price']))
        price_rub = base_price
        if user_data and user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            try: discount_percentage = Decimal(get_setting("referral_discount") or "0")
            except: discount_percentage = Decimal("0")
            if discount_percentage > 0:
                price_rub = base_price - (base_price * discount_percentage / 100).quantize(Decimal("0.01"))
        final_price_decimal = price_rub
        try:
            final_price_from_state = data.get('final_price')
            if final_price_from_state is not None: final_price_decimal = Decimal(str(final_price_from_state)).quantize(Decimal("0.01"))
        except: pass
        if final_price_decimal < Decimal('0'): final_price_decimal = Decimal('0.00')
        final_price_float = float(final_price_decimal)
        ym_wallet = (get_setting("yoomoney_wallet") or "").strip()
        if not ym_wallet: await callback.message.edit_text("❌ Оплата через ЮMoney временно недоступна."); await state.clear(); return
        months = int(plan['months'])
        user_id = callback.from_user.id
        payment_id = str(uuid.uuid4())
        metadata = {"payment_id": payment_id, "user_id": user_id, "months": months, "price": final_price_float, "action": data.get('action'), "key_id": data.get('key_id'), "host_name": data.get('host_name'), "plan_id": data.get('plan_id'), "customer_email": data.get('customer_email'), "payment_method": "YooMoney", "promo_code": data.get('promo_code'), "promo_discount_percent": data.get('promo_discount_percent'), "promo_discount_amount": data.get('promo_discount_amount')}
        try: create_pending_transaction(payment_id, user_id, final_price_float, metadata)
        except Exception as e: logger.warning(f"YooMoney: не удалось создать ожидающую транзакцию: {e}")
        try: success_url = f"https://t.me/{TELEGRAM_BOT_USERNAME}" if TELEGRAM_BOT_USERNAME else None
        except: success_url = None
        def _build_yoomoney_quickpay_url(wallet: str, amount: float, label: str, success_url: Optional[str] = None, targets: Optional[str] = None) -> str:
            try:
                params = {"receiver": wallet, "quickpay-form": "shop", "sum": f"{float(amount):.2f}", "label": label}
                if success_url: params["successURL"] = success_url
                if targets: params["targets"] = targets
                return f"https://yoomoney.ru/quickpay/confirm.xml?{urlencode(params)}"
            except: return "https://yoomoney.ru/"
        pay_url = _build_yoomoney_quickpay_url(wallet=ym_wallet, amount=final_price_float, label=payment_id, success_url=success_url, targets=f"Оплата {months} мес.")
        await state.clear()
        try: await callback.message.edit_text("Нажмите на кнопку ниже для оплаты. После оплаты нажмите 'Проверить оплату':", reply_markup=create_payment_with_check_keyboard(pay_url, f"check_yoomoney_{payment_id}"))
        except TelegramBadRequest: await callback.message.answer("Нажмите на кнопку ниже для оплаты. После оплаты нажмите 'Проверить оплату':", reply_markup=create_payment_with_check_keyboard(pay_url, f"check_yoomoney_{payment_id}"))
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_stars")
    async def create_stars_invoice_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer("Готовлю счёт в Stars…")
        data = await state.get_data()
        user_data = get_user(callback.from_user.id)
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: await callback.message.edit_text("❌ Произошла ошибка при выборе тарифа."); await state.clear(); return
        base_price = Decimal(str(plan['price']))
        price_rub = base_price
        if user_data and user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            try: discount_percentage = Decimal(get_setting("referral_discount") or "0")
            except: discount_percentage = Decimal("0")
            if discount_percentage > 0:
                price_rub = base_price - (base_price * discount_percentage / 100).quantize(Decimal("0.01"))
        months = int(plan['months'])
        price_decimal = Decimal(str(price_rub)).quantize(Decimal("0.01"))
        stars_count = _calc_stars_amount(price_decimal)
        payment_id = str(uuid.uuid4())
        metadata = {"user_id": callback.from_user.id, "months": months, "price": float(price_decimal), "action": data.get('action'), "key_id": data.get('key_id'), "host_name": data.get('host_name'), "plan_id": data.get('plan_id'), "customer_email": data.get('customer_email'), "payment_method": "Stars", "promo_code": data.get('promo_code'), "promo_discount_percent": data.get('promo_discount_percent'), "promo_discount_amount": data.get('promo_discount_amount')}
        try: create_pending_transaction(payment_id, callback.from_user.id, float(price_decimal), metadata)
        except Exception as e: logger.warning(f"Stars покупка: не удалось создать ожидающую транзакцию: {e}")
        payload = payment_id
        title = (get_setting("stars_title") or "Покупка VPN")
        description = (get_setting("stars_description") or f"Оплата {months} мес.")
        try:
            await bot.send_invoice(chat_id=callback.message.chat.id, title=title, description=description, payload=payload, currency="XTR", prices=[types.LabeledPrice(label=f"{months} мес.", amount=stars_count)])
            await state.clear()
        except Exception as e: logger.error(f"Не удалось отправить счет Stars: {e}"); await callback.message.edit_text("❌ Не удалось создать счёт Stars. Попробуйте другой способ оплаты."); await state.clear()
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_cryptobot")
    async def create_cryptobot_invoice_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Создаю счет в Crypto Pay...")
        data = await state.get_data()
        user_data = get_user(callback.from_user.id)
        plan_id = data.get('plan_id')
        user_id = data.get('user_id', callback.from_user.id)
        customer_email = data.get('customer_email')
        host_name = data.get('host_name')
        action = data.get('action')
        key_id = data.get('key_id')
        cryptobot_token = get_setting('cryptobot_token')
        if not cryptobot_token: logger.error(f"Попытка создания счета Crypto Pay не удалась для пользователя {user_id}: cryptobot_token не установлен."); await callback.message.edit_text("❌ Оплата криптовалютой временно недоступна. (Администратор не указал токен)."); await state.clear(); return
        plan = get_plan_by_id(plan_id)
        if not plan: logger.error(f"Попытка создания счета Crypto Pay не удалась для пользователя {user_id}: План с id {plan_id} не найден."); await callback.message.edit_text("❌ Произошла ошибка при выборе тарифа."); await state.clear(); return
        base_price = Decimal(str(plan['price']))
        price_rub_decimal = base_price
        if user_data.get('referred_by') and user_data.get('total_spent', 0) == 0:
            discount_percentage_str = get_setting("referral_discount") or "0"
            discount_percentage = Decimal(discount_percentage_str)
            if discount_percentage > 0:
                discount_amount = (base_price * discount_percentage / 100).quantize(Decimal("0.01"))
                price_rub_decimal = base_price - discount_amount
        months = plan['months']
        final_price_float = float(price_rub_decimal)
        async def _create_cryptobot_invoice(user_id: int, price_rub: float, months: int, host_name: str, state_data: dict) -> Optional[tuple[str, int]]:
            try:
                token = get_setting("cryptobot_token")
                if not token: logger.error("CryptoBot: не задан cryptobot_token"); return None
                async def get_usdt_rub_rate() -> Optional[Decimal]:
                    try:
                        url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub"
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url, timeout=10) as resp:
                                if resp.status != 200: logger.warning(f"USDT/RUB: HTTP {resp.status}"); return None
                                data = await resp.json()
                                val = data.get("tether", {}).get("rub")
                                return Decimal(str(val)) if val is not None else None
                    except Exception as e: logger.warning(f"USDT/RUB: ошибка получения курса: {e}"); return None
                rate = await get_usdt_rub_rate()
                if not rate or rate <= 0: logger.error("CryptoBot: не удалось получить курс USDT/RUB"); return None
                amount_usdt = (Decimal(str(price_rub)) / rate).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
                payload_parts = [str(user_id), str(months), str(float(price_rub)), str(state_data.get("action")), str(state_data.get("key_id")), str(host_name or ""), str(state_data.get("plan_id")), str(state_data.get("customer_email")), "CryptoBot", str(state_data.get("promo_code") or "")]
                payload = ":".join(payload_parts)
                cp = CryptoPay(token)
                invoice = await cp.create_invoice(asset="USDT", amount=float(amount_usdt), description="VPN оплата", payload=payload)
                pay_url = getattr(invoice, "pay_url", None) or getattr(invoice, "bot_invoice_url", None)
                invoice_id = getattr(invoice, "invoice_id", None)
                if not pay_url and isinstance(invoice, dict): pay_url = invoice.get("pay_url") or invoice.get("bot_invoice_url") or invoice.get("url")
                if not invoice_id and isinstance(invoice, dict): invoice_id = invoice.get("invoice_id")
                if not pay_url: logger.error(f"CryptoBot: не удалось получить ссылку на оплату из ответа: {invoice}"); return None
                if not invoice_id: logger.error(f"CryptoBot: не удалось получить invoice_id из ответа: {invoice}"); return None
                return (str(pay_url), int(invoice_id))
            except Exception as e: logger.error(f"CryptoBot: ошибка при создании счёта: {e}", exc_info=True); return None
        result = await _create_cryptobot_invoice(user_id=callback.from_user.id, price_rub=final_price_float, months=plan['months'], host_name=data.get('host_name'), state_data=data)
        if result:
            pay_url, invoice_id = result
            await state.update_data(cryptobot_invoice_id=invoice_id)
            await state.set_state(PaymentProcess.waiting_for_cryptobot_payment)
            await callback.message.edit_text("Нажмите на кнопку ниже для оплаты:\n\n💡 После оплаты нажмите «Проверить платёж» для подтверждения.", reply_markup=create_payment_with_check_keyboard(pay_url, "check_cryptobot_payment"))
        else: await callback.message.edit_text("❌ Не удалось создать счет CryptoBot. Попробуйте другой способ оплаты."); await state.clear()
    @user_router.callback_query(PaymentProcess.waiting_for_cryptobot_payment, F.data == "check_cryptobot_payment")
    async def check_cryptobot_payment_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer("Проверяю статус платежа...")
        data = await state.get_data()
        invoice_id = data.get('cryptobot_invoice_id')
        if not invoice_id: await callback.message.edit_text("❌ Не найден ID счета для проверки."); await state.clear(); return
        cryptobot_token = get_setting('cryptobot_token')
        if not cryptobot_token: logger.error("CryptoBot: не задан cryptobot_token для проверки платежа"); await callback.message.edit_text("❌ Ошибка конфигурации. Обратитесь к администратору."); await state.clear(); return
        try:
            cp = CryptoPay(cryptobot_token)
            invoices = await cp.get_invoices(invoice_ids=[invoice_id])
            if not invoices or len(invoices) == 0: await callback.answer("❌ Счет не найден", show_alert=True); return
            invoice = invoices[0]
            status = None
            try: status = getattr(invoice, "status", None)
            except: pass
            if not status and isinstance(invoice, dict): status = invoice.get("status")
            logger.info(f"CryptoBot проверка платежа: invoice_id={invoice_id}, status={status}")
            if status == "paid":
                await callback.answer("✅ Платеж найден! Обрабатываю...", show_alert=True)
                payload_string = None
                try: payload_string = getattr(invoice, "payload", None)
                except: pass
                if not payload_string and isinstance(invoice, dict): payload_string = invoice.get("payload")
                if not payload_string: logger.error(f"CryptoBot проверка: не найден payload для счета {invoice_id}"); await callback.message.edit_text("❌ Ошибка обработки платежа. Обратитесь в поддержку."); await state.clear(); return
                parts = payload_string.split(':')
                if len(parts) < 9: logger.error(f"CryptoBot проверка: некорректный формат payload: {payload_string}"); await callback.message.edit_text("❌ Ошибка обработки платежа. Обратитесь в поддержку."); await state.clear(); return
                metadata = {"user_id": parts[0], "months": parts[1], "price": parts[2], "action": parts[3], "key_id": parts[4], "host_name": parts[5], "plan_id": parts[6], "customer_email": parts[7] if parts[7] != 'None' else None, "payment_method": parts[8], "promo_code": (parts[9] if len(parts) > 9 and parts[9] else None)}
                await process_successful_payment(bot, metadata)
                await callback.message.edit_text("✅ Платеж успешно обработан!")
                await state.clear()
            elif status == "active": await callback.answer("⏳ Платеж еще не получен. Пожалуйста, завершите оплату и нажмите кнопку снова.", show_alert=True)
            else: await callback.answer(f"❌ Статус платежа: {status}. Пожалуйста, создайте новый счет.", show_alert=True); await state.clear()
        except Exception as e: logger.error(f"CryptoBot: ошибка при проверке счёта {invoice_id}: {e}", exc_info=True); await callback.answer("❌ Ошибка при проверке платежа. Попробуйте позже.", show_alert=True)
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_tonconnect")
    async def create_ton_invoice_handler(callback: types.CallbackQuery, state: FSMContext):
        logger.info(f"Пользователь {callback.from_user.id}: Вход в create_ton_invoice_handler.")
        data = await state.get_data()
        user_id = callback.from_user.id
        wallet_address = get_setting("ton_wallet_address")
        plan = get_plan_by_id(data.get('plan_id'))
        if not wallet_address or not plan: await callback.message.edit_text("❌ Оплата через TON временно недоступна."); await state.clear(); return
        await callback.answer("Создаю ссылку и QR-код для TON Connect...")
        price_rub = Decimal(str(data.get('final_price', plan['price'])))
        async def get_usdt_rub_rate() -> Optional[Decimal]:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=tether&vs_currencies=rub"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status != 200: logger.warning(f"USDT/RUB: HTTP {resp.status}"); return None
                        data = await resp.json()
                        val = data.get("tether", {}).get("rub")
                        return Decimal(str(val)) if val is not None else None
            except Exception as e: logger.warning(f"USDT/RUB: ошибка получения курса: {e}"); return None
        async def get_ton_usdt_rate() -> Optional[Decimal]:
            try:
                url = "https://api.coingecko.com/api/v3/simple/price?ids=toncoin&vs_currencies=usd"
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as resp:
                        if resp.status != 200: logger.warning(f"TON/USD: HTTP {resp.status}"); return None
                        data = await resp.json()
                        usd = data.get("toncoin", {}).get("usd")
                        return Decimal(str(usd)) if usd is not None else None
            except Exception as e: logger.warning(f"TON/USD: ошибка получения курса: {e}"); return None
        usdt_rub_rate = await get_usdt_rub_rate()
        ton_usdt_rate = await get_ton_usdt_rate()
        if not usdt_rub_rate or not ton_usdt_rate: await callback.message.edit_text("❌ Не удалось получить курс TON. Попробуйте позже."); await state.clear(); return
        price_ton = (price_rub / usdt_rub_rate / ton_usdt_rate).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
        amount_nanoton = int(price_ton * 1_000_000_000)
        payment_id = str(uuid.uuid4())
        metadata = {"user_id": user_id, "months": plan['months'], "price": float(price_rub), "action": data.get('action'), "key_id": data.get('key_id'), "host_name": data.get('host_name'), "plan_id": data.get('plan_id'), "customer_email": data.get('customer_email'), "payment_method": "TON Connect", "promo_code": data.get('promo_code'), "promo_discount_percent": data.get('promo_discount_percent'), "promo_discount_amount": data.get('promo_discount_amount')}
        create_pending_transaction(payment_id, user_id, float(price_rub), metadata)
        transaction_payload = {'messages': [{'address': wallet_address, 'amount': str(amount_nanoton), 'payload': payment_id}], 'valid_until': int(datetime.now().timestamp()) + 600}
        try:
            def _start_ton_connect_process(user_id: int, transaction_payload: Dict) -> str:
                try:
                    messages = transaction_payload.get("messages") or []
                    if not messages: raise ValueError("transaction_payload.messages is empty")
                    msg = messages[0]
                    address = msg.get("address")
                    amount = msg.get("amount")
                    payload_text = msg.get("payload") or ""
                    if not address or not amount: raise ValueError("address/amount are required")
                    params = {"amount": amount}
                    if payload_text: params["text"] = str(payload_text)
                    query = urlencode(params)
                    return f"ton://transfer/{address}?{query}"
                except Exception as e: logger.error(f"TON генерация deep link не удалась: {e}"); return "ton://transfer"
            connect_url = _start_ton_connect_process(user_id, transaction_payload)
            qr_img = qrcode.make(connect_url)
            bio = BytesIO()
            qr_img.save(bio, "PNG")
            qr_file = BufferedInputFile(bio.getvalue(), "ton_qr.png")
            try: await callback.message.delete()
            except: pass
            await callback.message.answer_photo(photo=qr_file, caption=f"💎 **Оплата через TON Connect**\n\nСумма к оплате: `{price_ton}` **TON**\n\n✅ **Способ 1 (на телефоне):** Нажмите кнопку **'Открыть кошелек'** ниже.\n✅ **Способ 2 (на компьютере):** Отсканируйте QR-код кошельком.\n\nПосле подключения кошелька подтвердите транзакцию.", parse_mode="Markdown", reply_markup=create_ton_connect_keyboard(connect_url))
            await state.clear()
        except Exception as e: logger.error(f"Не удалось сгенерировать ссылку TON Connect для пользователя {user_id}: {e}", exc_info=True); await callback.message.answer("❌ Не удалось создать ссылку для TON Connect. Попробуйте позже."); await state.clear()
    @user_router.callback_query(PaymentProcess.waiting_for_payment_method, F.data == "pay_balance")
    async def pay_with_main_balance_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        data = await state.get_data()
        user_id = callback.from_user.id
        plan = get_plan_by_id(data.get('plan_id'))
        if not plan: await callback.message.edit_text("❌ Ошибка: Тариф не найден."); await state.clear(); return
        months = int(plan['months'])
        price = float(data.get('final_price', plan['price']))
        if not deduct_from_balance(user_id, price): await callback.answer("Недостаточно средств на основном балансе.", show_alert=True); return
        metadata = {"user_id": user_id, "months": months, "price": price, "action": data.get('action'), "key_id": data.get('key_id'), "host_name": data.get('host_name'), "plan_id": data.get('plan_id'), "customer_email": data.get('customer_email'), "payment_method": "Balance", "promo_code": data.get('promo_code'), "promo_discount_percent": data.get('promo_discount_percent'), "promo_discount_amount": data.get('promo_discount_amount'), "chat_id": callback.message.chat.id, "message_id": callback.message.message_id}
        await state.clear()
        await process_successful_payment(bot, metadata)
    @user_router.pre_checkout_query()
    async def pre_checkout_handler(pre_checkout_query: types.PreCheckoutQuery, bot: Bot):
        try: await bot.answer_pre_checkout_query(pre_checkout_query.id, ok=True)
        except Exception as e: logger.warning(f"pre_checkout_handler не удался: {e}")
    @user_router.message(F.successful_payment)
    async def successful_payment_handler(message: types.Message, bot: Bot):
        try:
            sp = message.successful_payment
            payload = sp.invoice_payload or ""
            metadata = {}
            if payload:
                try: parsed = json.loads(payload); metadata = parsed if isinstance(parsed, dict) else {}
                except: metadata = {}
            if not metadata and payload:
                try:
                    currency = getattr(sp, 'currency', None)
                    total_amount = getattr(sp, 'total_amount', None)
                    payment_method = "Stars" if str(currency).upper() == "XTR" else "Card"
                    md = find_and_complete_pending_transaction(payment_id=payload, amount_rub=None, payment_method=payment_method, currency_name=currency, amount_currency=(float(total_amount) if total_amount is not None else None))
                    if md: metadata = md
                except Exception as e: logger.error(f"Не удалось разрешить ожидающую транзакцию по payload '{payload}': {e}")
        except Exception as e: logger.error(f"Не удалось разобрать payload успешного платежа: {e}"); metadata = {}
        if not metadata:
            try: await message.answer("✅ Оплата получена, но нет данных заказа. Обратитесь в поддержку, если ключ не выдан.")
            except: pass
            return
        await process_successful_payment(bot, metadata)
    return user_router

# ==================== ADMIN HANDLERS ====================
class Broadcast(StatesGroup):
    waiting_for_message = State()
    waiting_for_button_option = State()
    waiting_for_button_text = State()
    waiting_for_button_url = State()
    waiting_for_confirmation = State()

class AdminExtendSingleKey(StatesGroup):
    waiting_days = State()

class AdminAddAdmin(StatesGroup):
    waiting_for_input = State()

class AdminRemoveAdmin(StatesGroup):
    waiting_for_input = State()

class AdminEditKeyEmail(StatesGroup):
    waiting_for_email = State()

class AdminEditKeyHost(StatesGroup):
    waiting_for_host = State()

class AdminGiftKey(StatesGroup):
    picking_user = State()
    picking_host = State()
    picking_days = State()

class AdminMainRefill(StatesGroup):
    waiting_for_amount = State()

class AdminMainDeduct(StatesGroup):
    waiting_for_amount = State()

class AdminHostKeys(StatesGroup):
    picking_host = State()

class AdminQuickDeleteKey(StatesGroup):
    waiting_for_identifier = State()

class AdminExtendKey(StatesGroup):
    waiting_for_pair = State()

class PromoCreate(StatesGroup):
    waiting_code = State()
    waiting_discount = State()
    waiting_limits = State()
    waiting_dates = State()
    waiting_custom_days = State()
    waiting_description = State()
    waiting_confirmation = State()

class AdminRestoreDB(StatesGroup):
    waiting_file = State()

def get_admin_router() -> Router:
    admin_router = Router()
    def _format_user_mention(u: types.User) -> str:
        try:
            if u.username: return f"@{u.username.lstrip('@')}"
            full_name = (u.full_name or u.first_name or "Администратор").strip()
            try: safe_name = html_escape.escape(full_name)
            except: safe_name = full_name
            return f"<a href='tg://user?id={u.id}'>{safe_name}</a>"
        except: return str(getattr(u, 'id', '—'))
    async def show_admin_menu(message: types.Message, edit_message: bool = False):
        stats = get_admin_stats() or {}
        today_new = stats.get('today_new_users', 0); today_income = float(stats.get('today_income', 0) or 0); today_keys = stats.get('today_issued_keys', 0)
        total_users = stats.get('total_users', 0); total_income = float(stats.get('total_income', 0) or 0); total_keys = stats.get('total_keys', 0); active_keys = stats.get('active_keys', 0)
        text = (f"📊 <b>Панель Администратора</b>\n\n<b>За сегодня:</b>\n👥 Новых пользователей: {today_new}\n💰 Доход: {today_income:.2f} RUB\n🔑 Выдано ключей: {today_keys}\n\n<b>За все время:</b>\n👥 Всего пользователей: {total_users}\n💰 Общий доход: {total_income:.2f} RUB\n🔑 Всего ключей: {total_keys}\n\n<b>Состояние ключей:</b>\n✅ Активных: {active_keys}")
        keyboard = create_admin_menu_keyboard()
        if edit_message:
            try: await message.edit_text(text, reply_markup=keyboard)
            except: pass
        else: await message.answer(text, reply_markup=keyboard)
    def _format_monitor_metrics() -> tuple[str, dict[str, float]]:
        local = get_local_metrics()
        hosts = []
        try: hosts = get_all_hosts() or []
        except: hosts = []
        pieces = []
        worst = {'cpu_percent': 0.0, 'mem_percent': 0.0, 'disk_percent': 0.0}
        def _add_line(title: str, ok: bool, cpu: float | None, mem: float | None, disk: float | None, load: dict | None, uptime: float | None, extra: str | None = None) -> str:
            cpu_txt = f"CPU {cpu:.0f}%" if cpu is not None else "CPU —"
            mem_txt = f"RAM {mem:.0f}%" if mem is not None else "RAM —"
            disk_txt = f"Disk {disk:.0f}%" if disk is not None else "Disk —"
            load_txt = f" | load {load.get('1m'):.2f}/{load.get('5m'):.2f}/{load.get('15m'):.2f}" if load and load.get('1m') is not None else ""
            uptime_txt = f" | uptime {int(uptime // 86400)}д {int((uptime % 86400) // 3600)}ч" if uptime is not None else ""
            status = "✅" if ok else "❌"
            line = f"{status} <b>{title}</b>: {cpu_txt} · {mem_txt} · {disk_txt}{load_txt}{uptime_txt}"
            if extra: line += f"\n    {extra}"
            return line
        cpu_local = local.get('cpu_percent') if isinstance(local, dict) else None
        mem_local = local.get('mem_percent') if isinstance(local, dict) else None
        disk_local = local.get('disk_percent') if isinstance(local, dict) else None
        pieces.append(_add_line("Панель", bool(local.get('ok')), cpu_local, mem_local, disk_local, local.get('loadavg'), local.get('uptime_seconds'), extra=(local.get('error') if not local.get('ok') else None)))
        for name in [h.get('host_name') for h in hosts if h.get('ssh_host') and h.get('ssh_user')]:
            metrics = get_latest_host_metrics(name) or {}
            ok = bool(metrics.get('ok'))
            cpu = metrics.get('cpu_percent')
            mem = metrics.get('mem_percent')
            disk = metrics.get('disk_percent')
            pieces.append(_add_line(f"Хост {name}", ok, cpu, mem, disk, {'1m': metrics.get('load1'), '5m': metrics.get('load5'), '15m': metrics.get('load15')}, metrics.get('uptime_seconds'), extra=(metrics.get('error') if not ok else None)))
            if isinstance(cpu, (int, float)) and cpu > worst['cpu_percent']: worst['cpu_percent'] = float(cpu)
            if isinstance(mem, (int, float)) and mem > worst['mem_percent']: worst['mem_percent'] = float(mem)
            if isinstance(disk, (int, float)) and disk > worst['disk_percent']: worst['disk_percent'] = float(disk)
        text = "📈 <b>Мониторинг системных ресурсов</b>\n" + "\n".join(pieces)
        return text, worst
    async def _send_monitor_view(message: types.Message, edit_message: bool = False):
        text, worst = _format_monitor_metrics()
        suffix = ""
        warn_parts = []
        if worst['cpu_percent'] >= 85: warn_parts.append(f"CPU {worst['cpu_percent']:.0f}%")
        if worst['mem_percent'] >= 85: warn_parts.append(f"RAM {worst['mem_percent']:.0f}%")
        if worst['disk_percent'] >= 90: warn_parts.append(f"Disk {worst['disk_percent']:.0f}%")
        if warn_parts: suffix = "\n\n⚠️ <b>Внимание:</b> " + ", ".join(warn_parts) + ""
        keyboard = create_admin_monitor_keyboard()
        full_text = text + suffix
        if edit_message:
            try: await message.edit_text(full_text, reply_markup=keyboard)
            except: pass
        else: await message.answer(full_text, reply_markup=keyboard)
    @admin_router.callback_query(F.data == "admin_menu")
    async def open_admin_menu_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await show_admin_menu(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_speed_test")
    async def admin_speed_test_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await admin_speedtest_entry(callback)
    @admin_router.callback_query(F.data == "admin_monitoring")
    async def admin_monitoring_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await admin_monitor_open(callback)
    @admin_router.callback_query(F.data == "admin_administrators")
    async def admin_administrators_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await admin_admins_menu_entry(callback)
    @admin_router.callback_query(F.data == "admin_promo_codes")
    async def admin_promo_codes_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await admin_promo_menu(callback)
    @admin_router.callback_query(F.data == "admin_mailing")
    async def admin_mailing_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await start_broadcast_handler(callback, state)
    @admin_router.callback_query(F.data == "admin_monitor")
    async def admin_monitor_open(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await _send_monitor_view(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_monitor_refresh")
    async def admin_monitor_refresh(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await _send_monitor_view(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_speedtest")
    async def admin_speedtest_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        hosts = get_all_hosts() or []
        if not hosts: await callback.message.answer("⚠️ Хосты не найдены в настройках."); return
        await callback.message.edit_text("⚡ Выберите хост для теста скорости:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="speedtest"))
    @admin_router.callback_query(F.data.startswith("admin_speedtest_pick_host_"))
    async def admin_speedtest_run(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        token = callback.data.replace("admin_speedtest_pick_host_", "", 1)
        hosts = get_all_hosts() or []
        host = find_host_by_callback_token(hosts, token)
        if not host: await callback.message.answer("❌ Хост не найден или устарел список."); return
        host_name = host.get('host_name') or token
        try: admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except: admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для хоста: <b>{host_name}</b>\n(инициатор: {initiator})"
        for aid in admin_ids:
            try: await callback.bot.send_message(aid, start_text)
            except: pass
        try: wait_msg = await callback.message.answer(f"⏳ Выполняю тест скорости для <b>{host_name}</b>…")
        except: wait_msg = None
        try: result = await run_both_for_host(host_name)
        except Exception as e: result = {"ok": False, "error": str(e), "details": {}}
        def fmt_part(title: str, d: dict | None) -> str:
            if not d: return f"<b>{title}:</b> —"
            if not d.get("ok"): return f"<b>{title}:</b> ❌ {d.get('error') or 'ошибка'}"
            ping = d.get('ping_ms'); down = d.get('download_mbps'); up = d.get('upload_mbps'); srv = d.get('server_name') or '—'
            return (f"<b>{title}:</b> ✅\n• ping: {ping if ping is not None else '—'} ms\n• ↓ {down if down is not None else '—'} Mbps\n• ↑ {up if up is not None else '—'} Mbps\n• сервер: {srv}")
        details = result.get('details') or {}
        text_res = f"🏁 Тест скорости завершён для <b>{host_name}</b>\n\n" + fmt_part("SSH", details.get('ssh')) + "\n\n" + fmt_part("NET", details.get('net'))
        if wait_msg:
            try: await wait_msg.edit_text(text_res)
            except: await callback.message.answer(text_res)
        else: await callback.message.answer(text_res)
        for aid in admin_ids:
            if wait_msg and aid == callback.from_user.id: continue
            try: await callback.bot.send_message(aid, text_res)
            except: pass
    @admin_router.callback_query(F.data == "admin_speedtest_back_to_users")
    async def admin_speedtest_back(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer(); await show_admin_menu(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_speedtest_run_all")
    async def admin_speedtest_run_all(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: admin_ids = list({*(get_admin_ids() or []), int(callback.from_user.id)})
        except: admin_ids = [int(callback.from_user.id)]
        initiator = _format_user_mention(callback.from_user)
        start_text = f"🚀 Запущен тест скорости для всех хостов\n(инициатор: {initiator})"
        for aid in admin_ids:
            try: await callback.bot.send_message(aid, start_text)
            except: pass
        hosts = get_all_hosts() or []
        summary_lines = []
        for h in hosts:
            name = h.get('host_name')
            try:
                res = await run_both_for_host(name)
                ok = res.get('ok')
                det = res.get('details') or {}
                dm = det.get('ssh', {}).get('download_mbps') or det.get('net', {}).get('download_mbps')
                um = det.get('ssh', {}).get('upload_mbps') or det.get('net', {}).get('upload_mbps')
                summary_lines.append(f"• {name}: {'✅' if ok else '❌'} ↓ {dm or '—'} ↑ {um or '—'}")
            except Exception as e: summary_lines.append(f"• {name}: ❌ {e}")
        text = "🏁 Тест для всех завершён:\n" + "\n".join(summary_lines)
        await callback.message.answer(text)
        for aid in admin_ids:
            if aid == callback.from_user.id or aid == callback.message.chat.id: continue
            try: await callback.bot.send_message(aid, text)
            except: pass
    @admin_router.callback_query(F.data.startswith("admin_speedtest_autoinstall_"))
    async def admin_speedtest_autoinstall(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        token = callback.data.replace("admin_speedtest_autoinstall_", "", 1)
        hosts = get_all_hosts() or []
        host = find_host_by_callback_token(hosts, token)
        if not host: await callback.message.answer("❌ Хост не найден или устарел список."); return
        host_name = host.get('host_name') or token
        try: wait = await callback.message.answer(f"🛠 Пытаюсь установить speedtest на <b>{host_name}</b>…")
        except: wait = None
        try: res = await auto_install_speedtest_on_host(host_name)
        except Exception as e: res = {"ok": False, "log": f"Ошибка: {e}"}
        text = ("✅ Автоустановка завершена успешно" if res.get("ok") else "❌ Автоустановка завершилась с ошибкой") + f"\n<pre>{(res.get('log') or '')[:3500]}</pre>"
        if wait:
            try: await wait.edit_text(text)
            except: await callback.message.answer(text)
        else: await callback.message.answer(text)
    @admin_router.callback_query(F.data == "admin_backup_db")
    async def admin_backup_db(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: wait = await callback.message.answer("⏳ Создаю бэкап базы данных…")
        except: wait = None
        zip_path = create_backup_file()
        if not zip_path:
            if wait: await wait.edit_text("❌ Не удалось создать бэкап БД")
            else: await callback.message.answer("❌ Не удалось создать бэкап БД")
            return
        try: sent = await send_backup_to_admins(callback.bot, zip_path)
        except: sent = 0
        txt = f"✅ Бэкап создан: <b>{zip_path.name}</b>\nОтправлено администраторам: {sent}"
        if wait:
            try: await wait.edit_text(txt)
            except: await callback.message.answer(txt)
        else: await callback.message.answer(txt)
    @admin_router.callback_query(F.data == "admin_restore_db")
    async def admin_restore_db_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(AdminRestoreDB.waiting_file)
        kb = InlineKeyboardBuilder()
        kb.button(text="❌ Отмена", callback_data="admin_cancel")
        kb.adjust(1)
        text = "⚠️ <b>Восстановление базы данных</b>\n\nОтправьте файл <code>.zip</code> с бэкапом или файл <code>.db</code> в ответ на это сообщение.\nТекущая БД предварительно будет сохранена."
        try: await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except: await callback.message.answer(text, reply_markup=kb.as_markup())
    @admin_router.message(AdminRestoreDB.waiting_file)
    async def admin_restore_db_receive(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        doc = message.document
        if not doc: await message.answer("❌ Пришлите файл .zip или .db"); return
        filename = (doc.file_name or "uploaded.db").lower()
        if not (filename.endswith('.zip') or filename.endswith('.db')): await message.answer("❌ Поддерживаются только файлы .zip или .db"); return
        try:
            ts = datetime.now().strftime('%Y%m%d-%H%M%S')
            dest = BACKUPS_DIR / f"uploaded-{ts}-{filename}"
            dest.parent.mkdir(parents=True, exist_ok=True)
            await message.bot.download(doc, destination=dest)
        except Exception as e: await message.answer(f"❌ Не удалось скачать файл: {e}"); return
        ok = restore_from_file(dest)
        await state.clear()
        if ok: await message.answer("✅ Восстановление выполнено успешно.\nБот и панель продолжают работу с новой БД.")
        else: await message.answer("❌ Восстановление не удалось. Проверьте файл и повторите.")
    @admin_router.callback_query(F.data == "admin_promo_menu")
    async def admin_promo_menu(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await callback.message.edit_text("🎟 <b>Управление промокодами</b>", reply_markup=create_admin_promos_menu_keyboard())
    @admin_router.callback_query(F.data == "admin_promo_list")
    async def admin_promo_list(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        promos = list_promo_codes(include_inactive=True) or []
        if not promos: text = "📋 Промокоды отсутствуют."
        else:
            lines = []
            for p in promos:
                code = p.get('code')
                active = p.get('is_active') if 'is_active' in p else p.get('active', 1)
                used_total = p.get('used_total') if p.get('used_total') is not None else p.get('used_count', 0)
                limit_total = p.get('usage_limit_total')
                vf = p.get('valid_from') or '—'
                vu = p.get('valid_until') or p.get('valid_to') or '—'
                disc = None
                if p.get('discount_percent'): disc = f"{float(p.get('discount_percent')):.0f}%"
                elif p.get('discount_amount'): disc = f"{float(p.get('discount_amount')):.2f} RUB"
                disc = disc or '—'
                limit_str = f"{used_total}/{limit_total}" if limit_total else f"{used_total}"
                lines.append(f"• <b>{code}</b> — {'✅' if active else '❌'} | скидка: {disc} | исх./лим.: {limit_str} | {vf} → {vu}")
            text = "\n".join(lines)
        kb = InlineKeyboardBuilder()
        for p in (promos[:10] if promos else []):
            code = p.get('code')
            is_act = p.get('is_active') if 'is_active' in p else p.get('active', 1)
            label = f"{'🧯 Выкл' if is_act else '✅ Вкл'} {code}"
            kb.button(text=label, callback_data=f"admin_promo_toggle_{code}")
        kb.button(text="⬅️ В меню промокодов", callback_data="admin_promo_menu")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        rows = [1] * (len(promos[:10]) if promos else 0) + [1, 1]
        kb.adjust(*rows if rows else [1])
        try: await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except: await callback.message.answer(text, reply_markup=kb.as_markup())
    @admin_router.callback_query(F.data.startswith("admin_promo_toggle_"))
    async def admin_promo_toggle(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        code = callback.data.replace("admin_promo_toggle_", "", 1)
        try:
            p = get_promo_code(code)
            if not p: await callback.message.answer("❌ Промокод не найден."); return
            current = p.get('is_active') if 'is_active' in p else p.get('active', 1)
            ok = update_promo_code_status(code, is_active=(0 if current else 1))
            if ok: await callback.message.answer(f"Готово: {'деактивирован' if current else 'активирован'} {code}")
            else: await callback.message.answer("❌ Не удалось изменить статус.")
        except Exception as e: await callback.message.answer(f"❌ Ошибка: {e}")
        await admin_promo_list(callback)
    @admin_router.callback_query(F.data == "admin_promo_create")
    async def admin_promo_create_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(PromoCreate.waiting_code)
        await callback.message.edit_text("Введите код промокода (латиница/цифры) или нажмите \"Сгенерировать\":", reply_markup=create_admin_promo_code_keyboard())
    @admin_router.callback_query(PromoCreate.waiting_code, F.data == "admin_promo_gen_code")
    async def admin_promo_generate_code(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer("Сгенерировано")
        alphabet = string.ascii_uppercase + string.digits
        code = "".join(secrets.choice(alphabet) for _ in range(8))
        await state.update_data(code=code)
        await state.set_state(PromoCreate.waiting_discount)
        await callback.message.edit_text(f"Код: <b>{code}</b>\n\nУкажите скидку", reply_markup=create_admin_promo_discount_keyboard())
    @admin_router.message(PromoCreate.waiting_code)
    async def promo_create_code(message: types.Message, state: FSMContext):
        code = (message.text or '').strip().upper()
        if not code or len(code) < 2: await message.answer("❌ Код слишком короткий. Повторите ввод."); return
        await state.update_data(code=code)
        await state.set_state(PromoCreate.waiting_discount)
        await message.answer("Укажите скидку", reply_markup=create_admin_promo_discount_keyboard())
    @admin_router.callback_query(PromoCreate.waiting_discount, F.data.startswith("admin_promo_discount_"))
    async def promo_create_discount_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        data = callback.data
        perc = None; amt = None
        if data == "admin_promo_discount_type_percent":
            await callback.message.edit_text("Выберите процент скидки или введите вручную:", reply_markup=create_admin_promo_discount_percent_menu_keyboard()); return
        if data == "admin_promo_discount_type_amount":
            await callback.message.edit_text("Выберите фиксированную сумму скидки (RUB) или введите вручную:", reply_markup=create_admin_promo_discount_amount_menu_keyboard()); return
        if data == "admin_promo_discount_show_amount_menu":
            await callback.message.edit_text("Выберите фиксированную сумму скидки (RUB) или введите вручную:", reply_markup=create_admin_promo_discount_amount_menu_keyboard()); return
        if data == "admin_promo_discount_show_percent_menu":
            await callback.message.edit_text("Выберите процент скидки или введите вручную:", reply_markup=create_admin_promo_discount_percent_menu_keyboard()); return
        if data == "admin_promo_discount_manual_percent":
            await state.update_data(manual_discount_mode="percent")
            await callback.message.edit_text("Введите процент скидки (например, 10). Можно также в формате percent:10", reply_markup=create_admin_cancel_keyboard()); return
        if data == "admin_promo_discount_manual_amount":
            await state.update_data(manual_discount_mode="amount")
            await callback.message.edit_text("Введите фиксированную сумму скидки в RUB (например, 100). Можно также amount:100", reply_markup=create_admin_cancel_keyboard()); return
        if data.startswith("admin_promo_discount_percent_"):
            try: perc = float(data.rsplit("_", 1)[-1])
            except: perc = 10.0
        elif data.startswith("admin_promo_discount_amount_"):
            try: amt = float(data.rsplit("_", 1)[-1])
            except: amt = 50.0
        await state.update_data(discount_percent=perc, discount_amount=amt, manual_discount_mode=None, usage_limit_total=None, usage_limit_per_user=None, limits_manual_input=None, limits_both=False)
        await state.set_state(PromoCreate.waiting_limits)
        await callback.message.edit_text("Лимиты (опционально)", reply_markup=create_admin_promo_limits_type_keyboard())
    @admin_router.message(PromoCreate.waiting_discount)
    async def promo_create_discount(message: types.Message, state: FSMContext):
        text = (message.text or '').strip().lower()
        perc = None; amt = None
        data = await state.get_data()
        manual_mode = (data.get('manual_discount_mode') or '').strip()
        try:
            if text.startswith('percent:'): perc = float(text.split(':', 1)[1].strip())
            elif text.startswith('amount:'): amt = float(text.split(':', 1)[1].strip())
            elif manual_mode == 'percent' and re.match(r'^\d+(\.\d+)?$', text): perc = float(text)
            elif manual_mode == 'amount' and re.match(r'^\d+(\.\d+)?$', text): amt = float(text)
            else: await message.answer("❌ Формат не распознан. Введите число или percent:10 / amount:100"); return
        except: await message.answer("❌ Не удалось прочитать число. Повторите ввод."); return
        await state.update_data(discount_percent=perc, discount_amount=amt, usage_limit_total=None, usage_limit_per_user=None, limits_manual_input=None, limits_both=False)
        await state.set_state(PromoCreate.waiting_limits)
        await message.answer("Лимиты (опционально)", reply_markup=create_admin_promo_limits_type_keyboard())
    @admin_router.callback_query(PromoCreate.waiting_limits, F.data.startswith("admin_promo_limits_"))
    async def promo_create_limits_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        data = await state.get_data()
        if callback.data == "admin_promo_limits_type_total":
            await state.update_data(limits_both=False)
            await callback.message.edit_text("Общий лимит — выберите значение:", reply_markup=create_admin_promo_limits_total_keyboard()); return
        if callback.data == "admin_promo_limits_type_per":
            await state.update_data(limits_both=False)
            await callback.message.edit_text("Лимит на пользователя — выберите значение:", reply_markup=create_admin_promo_limits_per_user_keyboard()); return
        if callback.data == "admin_promo_limits_type_both":
            await state.update_data(limits_both=True, usage_limit_total=None, usage_limit_per_user=None)
            await callback.message.edit_text("Сначала укажите общий лимит:", reply_markup=create_admin_promo_limits_total_keyboard()); return
        if callback.data == "admin_promo_limits_back_to_type":
            await callback.message.edit_text("Лимиты (опционально)", reply_markup=create_admin_promo_limits_type_keyboard()); return
        if callback.data == "admin_promo_limits_skip":
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text("Даты (опционально)", reply_markup=create_admin_promo_dates_keyboard()); return
        if callback.data.startswith("admin_promo_limits_total_preset_"):
            try: total = int(callback.data.rsplit("_", 1)[-1])
            except: total = None
            await state.update_data(usage_limit_total=total)
            if data.get('limits_both'):
                await callback.message.edit_text("Теперь укажите лимит на пользователя:", reply_markup=create_admin_promo_limits_per_user_keyboard()); return
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text("Даты (опционально)", reply_markup=create_admin_promo_dates_keyboard()); return
        if callback.data.startswith("admin_promo_limits_per_preset_"):
            try: per_user = int(callback.data.rsplit("_", 1)[-1])
            except: per_user = None
            await state.update_data(usage_limit_per_user=per_user)
            if data.get('limits_both') and data.get('usage_limit_total') is None:
                await callback.message.edit_text("Сначала укажите общий лимит:", reply_markup=create_admin_promo_limits_total_keyboard()); return
            await state.set_state(PromoCreate.waiting_dates)
            await callback.message.edit_text("Даты (опционально)", reply_markup=create_admin_promo_dates_keyboard()); return
        if callback.data == "admin_promo_limits_total_manual":
            await state.update_data(limits_manual_input="total")
            await callback.message.edit_text("Введите общий лимит (целое число):", reply_markup=create_admin_cancel_keyboard()); return
        if callback.data == "admin_promo_limits_per_manual":
            await state.update_data(limits_manual_input="per")
            await callback.message.edit_text("Введите лимит на пользователя (целое число):", reply_markup=create_admin_cancel_keyboard()); return
    @admin_router.message(PromoCreate.waiting_limits)
    async def promo_create_limits(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        data = await state.get_data()
        manual = (data.get('limits_manual_input') or '').strip()
        if not manual: await message.answer("Пожалуйста, выберите вариант на клавиатуре.", reply_markup=create_admin_promo_limits_type_keyboard()); return
        try: val = int(text); assert val > 0
        except: await message.answer("❌ Введите положительное целое число."); return
        if manual == 'total':
            await state.update_data(usage_limit_total=val, limits_manual_input=None)
            if data.get('limits_both'):
                await message.answer("Теперь укажите лимит на пользователя:", reply_markup=create_admin_promo_limits_per_user_keyboard()); return
        elif manual == 'per': await state.update_data(usage_limit_per_user=val, limits_manual_input=None)
        await state.set_state(PromoCreate.waiting_dates)
        await message.answer("Даты (опционально)", reply_markup=create_admin_promo_dates_keyboard())
    @admin_router.message(PromoCreate.waiting_dates)
    async def promo_create_dates(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        vf = None; vu = None
        if text:
            parts = [p.strip() for p in text.split(';') if p.strip()]
            for p in parts:
                if p.startswith('from='): vf = p.split('=', 1)[1].strip()
                elif p.startswith('until='): vu = p.split('=', 1)[1].strip()
        def _to_iso(d: str | None) -> str | None:
            if not d: return None
            try:
                if len(d) == 10 and d.count('-') == 2: return datetime.fromisoformat(d).isoformat()
                datetime.fromisoformat(d); return d
            except: return None
        await state.update_data(valid_from=_to_iso(vf), valid_until=_to_iso(vu))
        await state.set_state(PromoCreate.waiting_description)
        await message.answer("Описание (опционально). Введите текст или оставьте пустым.", reply_markup=create_admin_promo_description_keyboard())
    @admin_router.callback_query(PromoCreate.waiting_dates, F.data.startswith("admin_promo_dates_"))
    async def promo_create_dates_buttons(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        now = datetime.now()
        vf_iso = None; vu_iso = None
        if callback.data == "admin_promo_dates_skip": pass
        elif callback.data == "admin_promo_dates_week": vf_iso = now.isoformat(); vu_iso = (now + timedelta(days=7)).isoformat()
        elif callback.data == "admin_promo_dates_month": vf_iso = now.isoformat(); vu_iso = (now + timedelta(days=30)).isoformat()
        elif callback.data.startswith("admin_promo_dates_days_"):
            try: days = int(callback.data.rsplit("_", 1)[-1]); assert days > 0
            except: days = 7
            vf_iso = now.isoformat(); vu_iso = (now + timedelta(days=days)).isoformat()
        elif callback.data == "admin_promo_dates_custom_days":
            await state.set_state(PromoCreate.waiting_custom_days)
            await callback.message.edit_text("Введите число дней действия промокода (например, 14):", reply_markup=create_admin_cancel_keyboard()); return
        await state.update_data(valid_from=vf_iso, valid_until=vu_iso)
        await state.set_state(PromoCreate.waiting_description)
        await callback.message.edit_text("Описание (опционально). Введите текст или оставьте пустым.", reply_markup=create_admin_promo_description_keyboard())
    @admin_router.message(PromoCreate.waiting_custom_days)
    async def promo_create_dates_custom_days(message: types.Message, state: FSMContext):
        text = (message.text or '').strip()
        try: days = int(text); assert 0 < days <= 3650
        except: await message.answer("❌ Введите целое число дней (1–3650)"); return
        now = datetime.now()
        vf_iso = now.isoformat(); vu_iso = (now + timedelta(days=days)).isoformat()
        await state.update_data(valid_from=vf_iso, valid_until=vu_iso)
        await state.set_state(PromoCreate.waiting_description)
        await message.answer("Описание (опционально). Введите текст или оставьте пустым.", reply_markup=create_admin_promo_description_keyboard())
    @admin_router.message(PromoCreate.waiting_description)
    async def promo_create_finish(message: types.Message, state: FSMContext):
        desc = (message.text or '').strip() or None
        await state.update_data(description=desc)
        await state.set_state(PromoCreate.waiting_confirmation)
        await _send_promo_summary(message, state, edit=False)
    @admin_router.callback_query(PromoCreate.waiting_description, F.data == "admin_promo_desc_skip")
    async def promo_create_finish_skip(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.update_data(description=None)
        await state.set_state(PromoCreate.waiting_confirmation)
        await _send_promo_summary(callback.message, state, edit=True)
    async def _send_promo_summary(message_or_msg, state: FSMContext, edit: bool = False):
        data = await state.get_data()
        code = data.get('code') or '—'
        if data.get('discount_percent'): disc_txt = f"{float(data['discount_percent']):.0f}%"
        elif data.get('discount_amount'): disc_txt = f"{float(data['discount_amount']):.2f} RUB"
        else: disc_txt = '—'
        lim_total = data.get('usage_limit_total')
        lim_per = data.get('usage_limit_per_user')
        limits_txt = []
        if lim_total: limits_txt.append(f"total={lim_total}")
        if lim_per: limits_txt.append(f"per_user={lim_per}")
        limits_txt = ";".join(limits_txt) if limits_txt else '—'
        def _fmt_date(s): return datetime.fromisoformat(s).strftime('%Y-%m-%d') if s else '—'
        dates_txt = '—'
        if data.get('valid_from') or data.get('valid_until'): dates_txt = f"{_fmt_date(data.get('valid_from'))} → {_fmt_date(data.get('valid_until'))}"
        desc = data.get('description') or '—'
        text = (f"🎟 <b>Сводка промокода</b>\n\n<b>Код:</b> {code}\n<b>Скидка:</b> {disc_txt}\n<b>Лимиты:</b> {limits_txt}\n<b>Даты:</b> {dates_txt}\n<b>Описание:</b> {html_escape.escape(desc) if desc != '—' else '—'}\n\nПодтвердите создание.")
        kb = create_admin_promo_confirm_keyboard()
        if edit:
            try: await message_or_msg.edit_text(text, reply_markup=kb)
            except: await message_or_msg.answer(text, reply_markup=kb)
        else: await message_or_msg.answer(text, reply_markup=kb)
    @admin_router.callback_query(PromoCreate.waiting_confirmation, F.data == "admin_promo_confirm_create")
    async def promo_confirm_create(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer("Создаю…")
        data = await state.get_data()
        try:
            ok = create_promo_code(data['code'], discount_percent=data.get('discount_percent'), discount_amount=data.get('discount_amount'), usage_limit_total=data.get('usage_limit_total'), usage_limit_per_user=data.get('usage_limit_per_user'), valid_from=(datetime.fromisoformat(data['valid_from']) if data.get('valid_from') else None), valid_until=(datetime.fromisoformat(data['valid_until']) if data.get('valid_until') else None), description=data.get('description'))
        except: ok = False
        await state.clear()
        await callback.message.edit_text(("✅ Промокод создан." if ok else "❌ Не удалось создать промокод."), reply_markup=create_admin_promos_menu_keyboard())
    @admin_router.callback_query(F.data.startswith("admin_users"))
    async def admin_users_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        users = get_all_users()
        page = 0
        if callback.data.startswith("admin_users_page_"):
            try: page = int(callback.data.split("_")[-1])
            except: page = 0
        await callback.message.edit_text("👥 <b>Пользователи</b>", reply_markup=create_admin_users_pick_keyboard(users, page=page, action="view"))
    @admin_router.callback_query(F.data.startswith("admin_view_user_"))
    async def admin_view_user_handler(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        user = get_user(user_id)
        if not user: await callback.message.answer("❌ Пользователь не найден"); return
        if user.get('username'): uname = user.get('username').lstrip('@'); user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else: user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        is_banned = user.get('is_banned', False)
        total_spent = user.get('total_spent', 0)
        balance = user.get('balance', 0)
        referred_by = user.get('referred_by')
        keys = get_user_keys(user_id); keys_count = len(keys)
        text = f"👤 <b>Пользователь {user_id}</b>\n\nИмя пользователя: {user_tag}\nВсего потратил: {float(total_spent):.2f} RUB\nБаланс: {float(balance):.2f} RUB\nЗабанен: {'да' if is_banned else 'нет'}\nПриглашён: {referred_by if referred_by else '—'}\nКлючей: {keys_count}"
        await callback.message.edit_text(text, reply_markup=create_admin_user_actions_keyboard(user_id, is_banned=is_banned))
    @admin_router.callback_query(F.data.startswith("admin_ban_user_"))
    async def admin_ban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        try:
            ban_user(user_id)
            await callback.message.answer(f"🚫 Пользователь {user_id} забанен")
            try:
                support = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
                kb = InlineKeyboardBuilder()
                url = None
                if support:
                    if support.startswith("@"): url = f"tg://resolve?domain={support[1:]}"
                    elif support.startswith("tg://"): url = support
                    elif support.startswith("http://") or support.startswith("https://"):
                        try: part = support.split("/")[-1].split("?")[0]; url = f"tg://resolve?domain={part}" if part else support
                        except: url = support
                    else: url = f"tg://resolve?domain={support}"
                if url: kb.button(text="🆘 Написать в поддержку", url=url)
                else: kb.button(text="🆘 Поддержка", callback_data="show_help")
                await callback.bot.send_message(user_id, "🚫 Ваш аккаунт заблокирован администратором. Если это ошибка — напишите в поддержку.", reply_markup=kb.as_markup())
            except: pass
        except Exception as e: await callback.message.answer(f"❌ Не удалось забанить пользователя: {e}"); return
        user = get_user(user_id) or {}
        if user.get('username'): uname = user.get('username').lstrip('@'); user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else: user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0); balance = user.get('balance', 0); referred_by = user.get('referred_by'); keys = get_user_keys(user_id); keys_count = len(keys)
        text = f"👤 <b>Пользователь {user_id}</b>\n\nИмя пользователя: {user_tag}\nВсего потратил: {float(total_spent):.2f} RUB\nБаланс: {float(balance):.2f} RUB\nЗабанен: да\nПриглашён: {referred_by if referred_by else '—'}\nКлючей: {keys_count}"
        try: await callback.message.edit_text(text, reply_markup=create_admin_user_actions_keyboard(user_id, is_banned=True))
        except: pass
    @admin_router.callback_query(F.data == "admin_admins_menu")
    async def admin_admins_menu_entry(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await callback.message.edit_text("👮 <b>Управление администраторами</b>", reply_markup=create_admins_menu_keyboard())
    @admin_router.callback_query(F.data == "admin_view_admins")
    async def admin_view_admins(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: ids = list(get_admin_ids() or [])
        except: ids = []
        if not ids: text = "📋 Список администраторов пуст."
        else:
            lines = []
            for aid in ids:
                try: u = get_user(int(aid)) or {}
                except: u = {}
                uname = (u.get('username') or '').strip()
                if uname: uname_clean = uname.lstrip('@'); tag = f"<a href='https://t.me/{uname_clean}'>@{uname_clean}</a>"
                else: tag = f"<a href='tg://user?id={aid}'>Профиль</a>"
                lines.append(f"• ID: {aid} — {tag}")
            text = "📋 <b>Администраторы</b>:\n" + "\n".join(lines)
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ Назад", callback_data="admin_admins_menu")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try: await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except: await callback.message.answer(text, reply_markup=kb.as_markup())
    @admin_router.callback_query(F.data.startswith("admin_unban_user_"))
    async def admin_unban_user(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        try:
            unban_user(user_id)
            await callback.message.answer(f"✅ Пользователь {user_id} разбанен")
            try:
                kb = InlineKeyboardBuilder()
                kb.row(get_main_menu_button())
                await callback.bot.send_message(user_id, "✅ Доступ к аккаунту восстановлен администратором.", reply_markup=kb.as_markup())
            except: pass
        except Exception as e: await callback.message.answer(f"❌ Не удалось разбанить пользователя: {e}"); return
        user = get_user(user_id) or {}
        if user.get('username'): uname = user.get('username').lstrip('@'); user_tag = f"<a href='https://t.me/{uname}'>@{uname}</a>"
        else: user_tag = f"<a href='tg://user?id={user_id}'>Профиль</a>"
        total_spent = user.get('total_spent', 0); balance = user.get('balance', 0); referred_by = user.get('referred_by'); keys = get_user_keys(user_id); keys_count = len(keys)
        text = f"👤 <b>Пользователь {user_id}</b>\n\nИмя пользователя: {user_tag}\nВсего потратил: {float(total_spent):.2f} RUB\nБаланс: {float(balance):.2f} RUB\nЗабанен: нет\nПриглашён: {referred_by if referred_by else '—'}\nКлючей: {keys_count}"
        try: await callback.message.edit_text(text, reply_markup=create_admin_user_actions_keyboard(user_id, is_banned=False))
        except: pass
    @admin_router.callback_query(F.data.startswith("admin_user_keys_"))
    async def admin_user_keys(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        keys = get_user_keys(user_id)
        await callback.message.edit_text(f"🔑 Ключи пользователя {user_id}:", reply_markup=create_admin_user_keys_keyboard(user_id, keys))
    @admin_router.callback_query(F.data.startswith("admin_user_referrals_"))
    async def admin_user_referrals(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        inviter = get_user(user_id)
        if not inviter: await callback.message.answer("❌ Пользователь не найден"); return
        refs = get_referrals_for_user(user_id) or []
        ref_count = len(refs)
        try: total_ref_earned = float(get_referral_balance_all(user_id) or 0)
        except: total_ref_earned = 0.0
        max_items = 30
        lines = []
        for r in refs[:max_items]:
            rid = r.get('telegram_id'); uname = r.get('username') or '—'; rdate = r.get('registration_date') or '—'; spent = float(r.get('total_spent') or 0)
            lines.append(f"• @{uname} (ID: {rid}) — рег: {rdate}, потратил: {spent:.2f} RUB")
        more_suffix = "\n… и ещё {}".format(ref_count - max_items) if ref_count > max_items else ""
        text = f"🤝 <b>Рефералы пользователя {user_id}</b>\n\nВсего приглашено: {ref_count}\nЗаработано по рефералке (всего): {total_ref_earned:.2f} RUB\n\n" + ("\n".join(lines) if lines else "Пока нет рефералов") + more_suffix
        kb = InlineKeyboardBuilder()
        kb.button(text="⬅️ К пользователю", callback_data=f"admin_view_user_{user_id}")
        kb.button(text="⬅️ В админ-меню", callback_data="admin_menu")
        kb.adjust(1, 1)
        try: await callback.message.edit_text(text, reply_markup=kb.as_markup())
        except: await callback.message.answer(text, reply_markup=kb.as_markup())
    @admin_router.callback_query(F.data.startswith("admin_edit_key_"))
    async def admin_edit_key(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: key_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        key = get_key_by_id(key_id)
        if not key: await callback.message.answer("❌ Ключ не найден"); return
        text = f"🔑 <b>Ключ #{key_id}</b>\nХост: {key.get('host_name') or '—'}\nEmail: {key.get('key_email') or '—'}\nИстекает: {key.get('expiry_date') or '—'}"
        try: await callback.message.edit_text(text, reply_markup=create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None))
        except Exception as e: logger.debug(f"edit_text failed in delete cancel for key #{key_id}: {e}"); await callback.message.answer(text, reply_markup=create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None))
    @admin_router.callback_query(F.data.regexp(r"^admin_key_delete_\d+$"))
    async def admin_key_delete_prompt(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        logger.info(f"admin_key_delete_prompt received: data='{callback.data}' from {callback.from_user.id}")
        try: key_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        key = get_key_by_id(key_id)
        if not key: await callback.message.answer("❌ Ключ не найден"); return
        email = key.get('key_email') or '—'; host = key.get('host_name') or '—'
        try: await callback.message.edit_text(f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}", reply_markup=create_admin_delete_key_confirm_keyboard(key_id))
        except Exception as e: logger.debug(f"edit_text failed in delete prompt for key #{key_id}: {e}"); await callback.message.answer(f"Вы уверены, что хотите удалить ключ #{key_id}?\nEmail: {email}\nСервер: {host}", reply_markup=create_admin_delete_key_confirm_keyboard(key_id))
    @admin_router.callback_query(F.data.startswith("admin_key_extend_"))
    async def admin_key_extend_prompt(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: key_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        await state.update_data(extend_key_id=key_id)
        await state.set_state(AdminExtendSingleKey.waiting_days)
        await callback.message.edit_text(f"Укажите, на сколько дней продлить ключ #{key_id} (число):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminExtendSingleKey.waiting_days)
    async def admin_key_extend_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        data = await state.get_data()
        key_id = int(data.get("extend_key_id", 0))
        if not key_id: await state.clear(); await message.answer("❌ Не удалось определить ключ."); return
        try: days = int((message.text or '').strip())
        except: await message.answer("❌ Введите число дней"); return
        if days <= 0: await message.answer("❌ Дней должно быть положительное число"); return
        key = get_key_by_id(key_id)
        if not key: await message.answer("❌ Ключ не найден"); await state.clear(); return
        host = key.get('host_name'); email = key.get('key_email')
        if not host or not email: await message.answer("❌ У ключа отсутствует сервер или email"); await state.clear(); return
        try: resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e: logger.error(f"Admin key extend: host update failed for key #{key_id}: {e}"); resp = None
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'): await message.answer("❌ Не удалось продлить ключ на сервере"); return
        try: update_key_info(key_id, resp['client_uuid'], int(resp['expiry_timestamp_ms']))
        except Exception as e: logger.error(f"Admin key extend: DB update failed for key #{key_id}: {e}")
        await state.clear()
        new_key = get_key_by_id(key_id)
        text = f"🔑 <b>Ключ #{key_id}</b>\nХост: {new_key.get('host_name') or '—'}\nEmail: {new_key.get('key_email') or '—'}\nИстекает: {new_key.get('expiry_date') or '—'}"
        await message.answer(f"✅ Ключ продлён на {days} дн.")
        await message.answer(text, reply_markup=create_admin_key_actions_keyboard(key_id, int(new_key.get('user_id')) if new_key and new_key.get('user_id') else None))
    @admin_router.callback_query(F.data == "admin_add_admin")
    async def admin_add_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(AdminAddAdmin.waiting_for_input)
        await callback.message.edit_text("Введите ID пользователя или его @username, которого нужно сделать администратором:\n\nПримеры: 123456789 или @username", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminAddAdmin.waiting_for_input)
    async def admin_add_admin_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        raw = (message.text or '').strip()
        target_id = None
        if raw.isdigit():
            try: target_id = int(raw)
            except: target_id = None
        if target_id is None and raw.startswith('@'):
            uname = raw.lstrip('@')
            try: chat = await message.bot.get_chat(raw); target_id = int(chat.id)
            except: target_id = None
            if target_id is None:
                try: chat = await message.bot.get_chat(uname); target_id = int(chat.id)
                except: target_id = None
            if target_id is None:
                try:
                    users = get_all_users() or []
                    uname_low = uname.lower()
                    for u in users:
                        u_un = (u.get('username') or '').lstrip('@').lower()
                        if u_un and u_un == uname_low: target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id')); break
                except: target_id = None
        if target_id is None: await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена."); return
        try:
            ids = set(get_admin_ids())
            ids.add(int(target_id))
            ids_str = ",".join(str(i) for i in sorted(ids))
            update_setting("admin_telegram_ids", ids_str)
            await message.answer(f"✅ Пользователь {target_id} добавлен в администраторы.")
        except Exception as e: await message.answer(f"❌ Ошибка при сохранении: {e}")
        await state.clear()
        try: await show_admin_menu(message)
        except: pass
    @admin_router.callback_query(F.data == "admin_remove_admin")
    async def admin_remove_admin_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(AdminRemoveAdmin.waiting_for_input)
        await callback.message.edit_text("Введите ID пользователя или его @username, которого нужно снять из админов:\n\nПримеры: 123456789 или @username", reply_markup=create_admin_cancel_keyboard())
    # Замените строку 6364 и несколько строк вокруг нее на этот код:

@admin_router.message(AdminRemoveAdmin.waiting_for_input)
async def admin_remove_admin_process(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        return
    raw = (message.text or '').strip()
    target_id = None
    if raw.isdigit():
        try:
            target_id = int(raw)
        except:
            target_id = None
    if target_id is None:
        uname = raw.lstrip('@')
        try:
            chat = await message.bot.get_chat(raw)
            target_id = int(chat.id)
        except:
            target_id = None
        if target_id is None and uname:
            try:
                chat = await message.bot.get_chat(uname)
                target_id = int(chat.id)
            except:
                target_id = None
        if target_id is None and uname:
            try:
                users = get_all_users() or []
                uname_low = uname.lower()
                for u in users:
                    u_un = (u.get('username') or '').lstrip('@').lower()
                    if u_un and u_un == uname_low:
                        target_id = int(u.get('telegram_id') or u.get('user_id') or u.get('id'))
                        break
            except:
                target_id = None
    if target_id is None:
        await message.answer("❌ Не удалось распознать ID/username. Отправьте корректное значение или нажмите Отмена.")
        return
    try:
        ids = set(get_admin_ids())
        if target_id not in ids:
            await message.answer(f"ℹ️ Пользователь {target_id} не является администратором.")
            await state.clear()
            try:
                await show_admin_menu(message)
            except:
                pass
            return
        if len(ids) <= 1:
            await message.answer("❌ Нельзя снять последнего администратора.")
            return
        ids.discard(int(target_id))
        ids_str = ",".join(str(i) for i in sorted(ids))
        update_setting("admin_telegram_ids", ids_str)
        await message.answer(f"✅ Пользователь {target_id} снят с администраторов.")
    except Exception as e:
        await message.answer(f"❌ Ошибка при сохранении: {e}")
    await state.clear()
    try:
        await show_admin_menu(message)
    except:
        pass
       
    @admin_router.callback_query(F.data.startswith("admin_key_delete_cancel_"))
    async def admin_key_delete_cancel(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        try: await callback.answer("Отменено")
        except: pass
        logger.info(f"admin_key_delete_cancel received: data='{callback.data}' from {callback.from_user.id}")
        try: key_id = int(callback.data.split("_")[-1])
        except: return
        key = get_key_by_id(key_id)
        if not key: return
        text = f"🔑 <b>Ключ #{key_id}</b>\nХост: {key.get('host_name') or '—'}\nEmail: {key.get('key_email') or '—'}\nИстекает: {key.get('expiry_date') or '—'}"
        try: await callback.message.edit_text(text, reply_markup=create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None))
        except Exception as e: logger.debug(f"edit_text failed in delete cancel for key #{key_id}: {e}"); await callback.message.answer(text, reply_markup=create_admin_key_actions_keyboard(key_id, int(key.get('user_id')) if key and key.get('user_id') else None))
    @admin_router.callback_query(F.data.startswith("admin_key_delete_confirm_"))
    async def admin_key_delete_confirm(callback: types.CallbackQuery):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        try: await callback.answer("Удаляю…")
        except: pass
        logger.info(f"admin_key_delete_confirm received: data='{callback.data}' from {callback.from_user.id}")
        try: key_id = int(callback.data.split('_')[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        try: key = get_key_by_id(key_id)
        except Exception as e: logger.error(f"DB get_key_by_id failed for #{key_id}: {e}"); key = None
        if not key: await callback.message.answer("❌ Ключ не найден"); return
        try: user_id = int(key.get('user_id'))
        except Exception as e: logger.error(f"Invalid user_id for key #{key_id}: {key.get('user_id')}, err={e}"); await callback.message.answer("❌ Ошибка данных ключа: некорректный пользователь"); return
        host = key.get('host_name'); email = key.get('key_email'); ok_host = True
        if host and email:
            try: ok_host = await delete_client_on_host(host, email)
            except Exception as e: ok_host = False; logger.error(f"Failed to delete client on host '{host}' for key #{key_id}: {e}")
        ok_db = False
        try: ok_db = delete_key_by_email(email)
        except Exception as e: logger.error(f"Failed to delete key in DB for email '{email}': {e}")
        if ok_db:
            await callback.message.answer("✅ Ключ удалён" + (" (с хоста тоже)" if ok_host else " (но удалить на хосте не удалось)"))
            keys = get_user_keys(user_id)
            try: await callback.message.edit_text(f"🔑 Ключи пользователя {user_id}:", reply_markup=create_admin_user_keys_keyboard(user_id, keys))
            except Exception as e: logger.debug(f"edit_text failed in delete confirm list refresh for user {user_id}: {e}"); await callback.message.answer(f"🔑 Ключи пользователя {user_id}:", reply_markup=create_admin_user_keys_keyboard(user_id, keys))
            try: await callback.bot.send_message(user_id, "ℹ️ Администратор удалил один из ваших ключей. Если это ошибка — напишите в поддержку.", reply_markup=create_support_keyboard())
            except: pass
        else: await callback.message.answer("❌ Не удалось удалить ключ из базы данных")
    @admin_router.callback_query(F.data.startswith("admin_key_edit_host_"))
    async def admin_key_edit_host_start(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: key_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        await state.update_data(edit_key_id=key_id)
        await state.set_state(AdminEditKeyHost.waiting_for_host)
        await callback.message.edit_text(f"Введите новое имя сервера (host) для ключа #{key_id}", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminEditKeyHost.waiting_for_host)
    async def admin_key_edit_host_commit(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        data = await state.get_data()
        key_id = int(data.get('edit_key_id'))
        new_host = (message.text or '').strip()
        if not new_host: await message.answer("❌ Введите корректное имя сервера"); return
        ok = update_key_host(key_id, new_host)
        if ok: await message.answer("✅ Сервер обновлён")
        else: await message.answer("❌ Не удалось обновить сервер")
        await state.clear()
    @admin_router.callback_query(F.data == "admin_gift_key")
    async def admin_gift_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        users = get_all_users()
        await state.clear()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text("🎁 Выдача подарочного ключа\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=0, action="gift"))
    @admin_router.callback_query(F.data.startswith("admin_gift_key_"))
    async def admin_gift_key_for_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.clear()
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(f"👤 Пользователь {user_id}. Выберите сервер:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="gift"))
    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_page_"))
    async def admin_gift_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: page = int(callback.data.split("_")[-1])
        except: page = 0
        users = get_all_users()
        await callback.message.edit_text("🎁 Выдача подарочного ключа\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=page, action="gift"))
    @admin_router.callback_query(AdminGiftKey.picking_user, F.data.startswith("admin_gift_pick_user_"))
    async def admin_gift_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.update_data(target_user_id=user_id)
        hosts = get_all_hosts()
        await state.set_state(AdminGiftKey.picking_host)
        await callback.message.edit_text(f"👤 Пользователь {user_id}. Выберите сервер:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="gift"))
    @admin_router.callback_query(AdminGiftKey.picking_host, F.data == "admin_gift_back_to_users")
    async def admin_gift_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        users = get_all_users()
        await state.set_state(AdminGiftKey.picking_user)
        await callback.message.edit_text("🎁 Выдача подарочного ключа\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=0, action="gift"))
    @admin_router.callback_query(AdminGiftKey.picking_host, F.data.startswith("admin_gift_pick_host_"))
    async def admin_gift_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        host_name = callback.data.split("admin_gift_pick_host_")[-1]
        await state.update_data(host_name=host_name)
        await state.set_state(AdminGiftKey.picking_days)
        await callback.message.edit_text(f"🌍 Сервер: {host_name}. Введите срок действия ключа в днях (целое число):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminGiftKey.picking_days)
    async def admin_gift_pick_days(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        host_name = data.get('host_name')
        try: days = int(message.text.strip())
        except: await message.answer("❌ Введите целое число дней"); return
        if days <= 0: await message.answer("❌ Срок должен быть положительным"); return
        user = get_user(user_id) or {}
        username = (user.get('username') or f'user{user_id}').lower()
        username_slug = re.sub(r"[^a-z0-9._-]", "_", username).strip("_")[:16] or f"user{user_id}"
        base_local = f"gift_{username_slug}"
        candidate_local = base_local
        attempt = 1
        while True:
            candidate_email = f"{candidate_local}@bot.local"
            existing = get_key_by_email(candidate_email)
            if not existing: break
            attempt += 1
            candidate_local = f"{base_local}-{attempt}"
            if attempt > 100: candidate_local = f"{base_local}-{int(time.time())}"; candidate_email = f"{candidate_local}@bot.local"; break
        generated_email = candidate_email
        try: host_resp = await create_or_update_key_on_host(host_name, generated_email, days_to_add=days)
        except Exception as e: host_resp = None; logging.error(f"Подарочный поток: не удалось создать клиента на хосте '{host_name}' для пользователя {user_id}: {e}")
        if not host_resp or not host_resp.get("client_uuid") or not host_resp.get("expiry_timestamp_ms"): await message.answer("❌ Не удалось выдать ключ на сервере. Проверьте настройки хоста и доступность панели XUI."); await state.clear(); await show_admin_menu(message); return
        client_uuid = host_resp["client_uuid"]; expiry_ms = int(host_resp["expiry_timestamp_ms"]); connection_link = host_resp.get("connection_string")
        key_id = add_new_key(user_id, host_name, client_uuid, generated_email, expiry_ms)
        if key_id:
            username_readable = (user.get('username') or '').strip()
            user_part = f"{user_id} (@{username_readable})" if username_readable else f"{user_id}"
            text_admin = f"✅ 🎁 Подарочный ключ #{key_id} выдан пользователю {user_part} (сервер: {host_name}, {days} дн.)\nEmail: {generated_email}"
            await message.answer(text_admin)
            try:
                notify_text = f"🎁 Администратор выдал вам подарочный ключ #{key_id}\nСервер: {host_name}\nСрок: {days} дн.\n"
                if connection_link: cs = html_escape.escape(connection_link); notify_text += f"\n🔗 Подписка:\n<pre><code>{cs}</code></pre>"
                await message.bot.send_message(user_id, notify_text, parse_mode='HTML', disable_web_page_preview=True)
            except: pass
        else: await message.answer("❌ Не удалось сохранить ключ в базе данных.")
        await state.clear()
        await show_admin_menu(message)
    @admin_router.callback_query(F.data == "admin_add_balance")
    async def admin_add_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text("➕ Начисление баланса\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=0, action="add_balance"))
    @admin_router.callback_query(F.data.startswith("admin_add_balance_"))
    async def admin_add_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(f"Пользователь {user_id}. Введите сумму начисления (в рублях):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_page_"))
    async def admin_add_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: page = int(callback.data.split("_")[-1])
        except: page = 0
        users = get_all_users()
        await callback.message.edit_text("➕ Начисление баланса\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=page, action="add_balance"))
    @admin_router.callback_query(F.data.startswith("admin_add_balance_pick_user_"))
    async def admin_add_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainRefill.waiting_for_amount)
        await callback.message.edit_text(f"Пользователь {user_id}. Введите сумму начисления (в рублях):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminMainRefill.waiting_for_amount)
    async def handle_main_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try: amount = float(message.text.strip().replace(',', '.'))
        except: await message.answer("❌ Введите число — сумму в рублях"); return
        if amount <= 0: await message.answer("❌ Сумма должна быть положительной"); return
        try:
            ok = add_to_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Начислено {amount:.2f} RUB на баланс пользователю {user_id}")
                try: await message.bot.send_message(user_id, f"💰 Вам начислено {amount:.2f} RUB на баланс администратором.")
                except: pass
            else: await message.answer("❌ Пользователь не найден или ошибка БД")
        except Exception as e: await message.answer(f"❌ Ошибка начисления: {e}")
        await state.clear()
        await show_admin_menu(message)
    @admin_router.callback_query(F.data.startswith("admin_key_back_"))
    async def admin_key_back(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: key_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат key_id"); return
        key = get_key_by_id(key_id)
        if not key: await callback.message.answer("❌ Ключ не найден"); return
        host_from_state = None
        try:
            data = await state.get_data()
            host_from_state = (data or {}).get('hostkeys_host')
        except: host_from_state = None
        if host_from_state:
            host_name = host_from_state
            keys = get_keys_for_host(host_name)
            await callback.message.edit_text(f"🔑 Ключи на хосте {host_name}:", reply_markup=create_admin_keys_for_host_keyboard(host_name, keys))
        else:
            user_id = int(key.get('user_id'))
            keys = get_user_keys(user_id)
            await callback.message.edit_text(f"🔑 Ключи пользователя {user_id}:", reply_markup=create_admin_user_keys_keyboard(user_id, keys))
    @admin_router.callback_query(F.data == "noop")
    async def admin_noop(callback: types.CallbackQuery): await callback.answer()
    @admin_router.callback_query(F.data == "admin_cancel")
    async def admin_cancel_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Отменено")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_deduct_balance")
    async def admin_deduct_balance_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        users = get_all_users()
        await callback.message.edit_text("➖ Списание баланса\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=0, action="deduct_balance"))
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_"))
    async def admin_deduct_balance_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(f"Пользователь {user_id}. Введите сумму списания (в рублях):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_page_"))
    async def admin_deduct_balance_pick_user_page(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: page = int(callback.data.split("_")[-1])
        except: page = 0
        users = get_all_users()
        await callback.message.edit_text("➖ Списание баланса\n\nВыберите пользователя:", reply_markup=create_admin_users_pick_keyboard(users, page=page, action="deduct_balance"))
    @admin_router.callback_query(F.data.startswith("admin_deduct_balance_pick_user_"))
    async def admin_deduct_balance_pick_user(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: user_id = int(callback.data.split("_")[-1])
        except: await callback.message.answer("❌ Неверный формат user_id"); return
        await state.update_data(target_user_id=user_id)
        await state.set_state(AdminMainDeduct.waiting_for_amount)
        await callback.message.edit_text(f"Пользователь {user_id}. Введите сумму списания (в рублях):", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminMainDeduct.waiting_for_amount)
    async def handle_deduct_amount(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        data = await state.get_data()
        user_id = int(data.get('target_user_id'))
        try: amount = float(message.text.strip().replace(',', '.'))
        except: await message.answer("❌ Введите число — сумму в рублях"); return
        if amount <= 0: await message.answer("❌ Сумма должна быть положительной"); return
        try:
            ok = deduct_from_balance(user_id, amount)
            if ok:
                await message.answer(f"✅ Списано {amount:.2f} RUB с баланса пользователя {user_id}")
                try: await message.bot.send_message(user_id, f"➖ С вашего баланса списано {amount:.2f} RUB администратором.\nЕсли это ошибка — напишите в поддержку.", reply_markup=create_support_keyboard())
                except: pass
            else: await message.answer("❌ Пользователь не найден или недостаточно средств")
        except Exception as e: await message.answer(f"❌ Ошибка списания: {e}")
        await state.clear()
        await show_admin_menu(message)
    @admin_router.callback_query(F.data == "admin_host_keys")
    async def admin_host_keys_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.clear()
        await state.set_state(AdminHostKeys.picking_host)
        hosts = get_all_hosts()
        await callback.message.edit_text("🌍 Выберите хост для просмотра ключей:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="hostkeys"))
    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_pick_host_"))
    async def admin_host_keys_pick_host(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        host_name = callback.data.split("admin_hostkeys_pick_host_")[-1]
        try: await state.update_data(hostkeys_host=host_name)
        except: pass
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(f"🔑 Ключи на хосте {host_name}:", reply_markup=create_admin_keys_for_host_keyboard(host_name, keys, page=0))
    @admin_router.callback_query(AdminHostKeys.picking_host, F.data.startswith("admin_hostkeys_page_"))
    async def admin_host_keys_page_nav(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: page = int(callback.data.split("_")[-1])
        except: page = 0
        data = await state.get_data()
        host_name = (data or {}).get("hostkeys_host")
        if not host_name:
            hosts = get_all_hosts()
            await callback.message.edit_text("🌍 Выберите хост для просмотра ключей:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="hostkeys"))
            return
        keys = get_keys_for_host(host_name)
        await callback.message.edit_text(f"🔑 Ключи на хосте {host_name}:", reply_markup=create_admin_keys_for_host_keyboard(host_name, keys, page=page))
    @admin_router.callback_query(AdminHostKeys.picking_host, F.data == "admin_hostkeys_back_to_hosts")
    async def admin_hostkeys_back_to_hosts(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        try: await state.update_data(hostkeys_host=None)
        except: pass
        hosts = get_all_hosts()
        await callback.message.edit_text("🌍 Выберите хост для просмотра ключей:", reply_markup=create_admin_hosts_pick_keyboard(hosts, action="hostkeys"))
    @admin_router.callback_query(F.data == "admin_hostkeys_back_to_users")
    async def admin_hostkeys_back_to_users(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await show_admin_menu(callback.message, edit_message=True)
    @admin_router.callback_query(F.data == "admin_delete_key")
    async def admin_delete_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(AdminQuickDeleteKey.waiting_for_identifier)
        await callback.message.edit_text("🗑 Введите <code>key_id</code> или <code>email</code> ключа для удаления:", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminQuickDeleteKey.waiting_for_identifier)
    async def admin_delete_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        text = (message.text or '').strip()
        key = None
        try: key_id = int(text); key = get_key_by_id(key_id)
        except: key = get_key_by_email(text)
        if not key: await message.answer("❌ Ключ не найден. Пришлите корректный key_id или email."); return
        key_id = int(key.get('key_id')); email = key.get('key_email') or '—'; host = key.get('host_name') or '—'
        await state.clear()
        await message.answer(f"Подтвердите удаление ключа #{key_id}\nEmail: {email}\nСервер: {host}", reply_markup=create_admin_delete_key_confirm_keyboard(key_id))
    @admin_router.callback_query(F.data == "admin_extend_key")
    async def admin_extend_key_entry(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await state.set_state(AdminExtendKey.waiting_for_pair)
        await callback.message.edit_text("➕ Введите: <code>key_id дни</code> (сколько дней добавить к ключу)", reply_markup=create_admin_cancel_keyboard())
    @admin_router.message(AdminExtendKey.waiting_for_pair)
    async def admin_extend_key_process(message: types.Message, state: FSMContext):
        if not is_admin(message.from_user.id): return
        parts = (message.text or '').strip().split()
        if len(parts) != 2: await message.answer("❌ Формат: <code>key_id дни</code>"); return
        try: key_id = int(parts[0]); days = int(parts[1])
        except: await message.answer("❌ Оба значения должны быть числами"); return
        if days <= 0: await message.answer("❌ Количество дней должно быть положительным"); return
        key = get_key_by_id(key_id)
        if not key: await message.answer("❌ Ключ не найден"); return
        host = key.get('host_name'); email = key.get('key_email')
        if not host or not email: await message.answer("❌ У ключа отсутствуют данные о хосте или email"); return
        resp = None
        try: resp = await create_or_update_key_on_host(host, email, days_to_add=days)
        except Exception as e: logger.error(f"Extend flow: failed to update client on host '{host}' for key #{key_id}: {e}")
        if not resp or not resp.get('client_uuid') or not resp.get('expiry_timestamp_ms'): await message.answer("❌ Не удалось продлить ключ на сервере"); return
        try: update_key_info(key_id, resp['client_uuid'], int(resp['expiry_timestamp_ms']))
        except Exception as e: logger.error(f"Extend flow: failed update DB for key #{key_id}: {e}")
        await state.clear()
        await message.answer(f"✅ Ключ #{key_id} продлён на {days} дн.")
        try: await message.bot.send_message(int(key.get('user_id')), f"ℹ️ Администратор продлил ваш ключ #{key_id} на {days} дн.")
        except: pass
    @admin_router.callback_query(F.data == "start_broadcast")
    async def start_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        if not is_admin(callback.from_user.id): await callback.answer("У вас нет прав.", show_alert=True); return
        await callback.answer()
        await callback.message.edit_text("Пришлите сообщение, которое вы хотите разослать всем пользователям.\nВы можете использовать форматирование (<b>жирный</b>, <i>курсив</i>).\nТакже поддерживаются фото, видео и документы.\n", reply_markup=create_broadcast_cancel_keyboard())
        await state.set_state(Broadcast.waiting_for_message)
    @admin_router.message(Broadcast.waiting_for_message)
    async def broadcast_message_received_handler(message: types.Message, state: FSMContext):
        await state.update_data(message_to_send=message.model_dump_json())
        await message.answer("Сообщение получено. Хотите добавить к нему кнопку со ссылкой?", reply_markup=create_broadcast_options_keyboard())
        await state.set_state(Broadcast.waiting_for_button_option)
    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_add_button")
    async def add_button_prompt_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer()
        await callback.message.edit_text("Хорошо. Теперь отправьте мне текст для кнопки.", reply_markup=create_broadcast_cancel_keyboard())
        await state.set_state(Broadcast.waiting_for_button_text)
    @admin_router.message(Broadcast.waiting_for_button_text)
    async def button_text_received_handler(message: types.Message, state: FSMContext):
        await state.update_data(button_text=message.text)
        await message.answer("Текст кнопки получен. Теперь отправьте ссылку (URL), куда она будет вести.", reply_markup=create_broadcast_cancel_keyboard())
        await state.set_state(Broadcast.waiting_for_button_url)
    @admin_router.message(Broadcast.waiting_for_button_url)
    async def button_url_received_handler(message: types.Message, state: FSMContext, bot: Bot):
        url_to_check = message.text
        if not (url_to_check.startswith("http://") or url_to_check.startswith("https://")):
            await message.answer("❌ Ссылка должна начинаться с http:// или https://. Попробуйте еще раз."); return
        await state.update_data(button_url=url_to_check)
        await show_broadcast_preview(message, state, bot)
    @admin_router.callback_query(Broadcast.waiting_for_button_option, F.data == "broadcast_skip_button")
    async def skip_button_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.answer()
        await state.update_data(button_text=None, button_url=None)
        await show_broadcast_preview(callback.message, state, bot)
    async def show_broadcast_preview(message: types.Message, state: FSMContext, bot: Bot):
        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)
        button_text = data.get('button_text'); button_url = data.get('button_url')
        preview_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            preview_keyboard = builder.as_markup()
        await message.answer("Вот так будет выглядеть ваше сообщение. Отправляем?", reply_markup=create_broadcast_confirmation_keyboard())
        await bot.copy_message(chat_id=message.chat.id, from_chat_id=original_message.chat.id, message_id=original_message.message_id, reply_markup=preview_keyboard)
        await state.set_state(Broadcast.waiting_for_confirmation)
    @admin_router.callback_query(Broadcast.waiting_for_confirmation, F.data == "confirm_broadcast")
    async def confirm_broadcast_handler(callback: types.CallbackQuery, state: FSMContext, bot: Bot):
        await callback.message.edit_text("⏳ Начинаю рассылку... Это может занять некоторое время.")
        data = await state.get_data()
        message_json = data.get('message_to_send')
        original_message = types.Message.model_validate_json(message_json)
        button_text = data.get('button_text'); button_url = data.get('button_url')
        final_keyboard = None
        if button_text and button_url:
            builder = InlineKeyboardBuilder()
            builder.button(text=button_text, url=button_url)
            final_keyboard = builder.as_markup()
        await state.clear()
        users = get_all_users()
        logger.info(f"Broadcast: Starting to iterate over {len(users)} users.")
        sent_count = 0; failed_count = 0; banned_count = 0
        for user in users:
            user_id = user['telegram_id']
            if user.get('is_banned'): banned_count += 1; continue
            try:
                await bot.copy_message(chat_id=user_id, from_chat_id=original_message.chat.id, message_id=original_message.message_id, reply_markup=final_keyboard)
                sent_count += 1; await asyncio.sleep(0.1)
            except Exception as e: failed_count += 1; logger.warning(f"Failed to send broadcast message to user {user_id}: {e}")
        await callback.message.answer(f"✅ Рассылка завершена!\n\n👍 Отправлено: {sent_count}\n👎 Не удалось отправить: {failed_count}\n🚫 Пропущено (забанены): {banned_count}")
        await show_admin_menu(callback.message)
    @admin_router.callback_query(StateFilter(Broadcast), F.data == "cancel_broadcast")
    async def cancel_broadcast_handler(callback: types.CallbackQuery, state: FSMContext):
        await callback.answer("Рассылка отменена.")
        await state.clear()
        await show_admin_menu(callback.message, edit_message=True)
    @admin_router.message(Command(commands=["approve_withdraw"]))
    async def approve_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id): return
        try:
            user_id = int(message.text.split("_")[-1])
            user = get_user(user_id)
            balance = user.get('referral_balance', 0)
            if balance < 100: await message.answer("Баланс пользователя менее 100 руб."); return
            set_referral_balance(user_id, 0)
            set_referral_balance_all(user_id, 0)
            await message.answer(f"✅ Выплата {balance:.2f} RUB пользователю {user_id} подтверждена.")
            await message.bot.send_message(user_id, f"✅ Ваша заявка на вывод {balance:.2f} RUB одобрена. Деньги будут переведены в ближайшее время.")
        except Exception as e: await message.answer(f"Ошибка: {e}")
    @admin_router.message(Command(commands=["decline_withdraw"]))
    async def decline_withdraw_handler(message: types.Message):
        if not is_admin(message.from_user.id): return
        try:
            user_id = int(message.text.split("_")[-1])
            await message.answer(f"❌ Заявка пользователя {user_id} отклонена.")
            await message.bot.send_message(user_id, "❌ Ваша заявка на вывод отклонена. Проверьте корректность реквизитов и попробуйте снова.")
        except Exception as e: await message.answer(f"Ошибка: {e}")
    return admin_router

# ==================== PAYMENT PROCESSING ====================
async def notify_admin_of_purchase(bot: Bot, metadata: dict):
    try:
        admin_id_raw = get_setting("admin_telegram_id")
        if not admin_id_raw: return
        admin_id = int(admin_id_raw)
        user_id = metadata.get('user_id'); host_name = metadata.get('host_name'); months = metadata.get('months'); price = metadata.get('price'); action = metadata.get('action'); payment_method = metadata.get('payment_method') or 'Unknown'
        payment_method_map = {'Balance': 'Баланс', 'Card': 'Карта', 'Crypto': 'Крипто', 'USDT': 'USDT', 'TON': 'TON'}
        payment_method_display = payment_method_map.get(payment_method, payment_method)
        plan_id = metadata.get('plan_id')
        plan = get_plan_by_id(plan_id)
        plan_name = plan.get('plan_name', 'Unknown') if plan else 'Unknown'
        text = (f"📥 Новая оплата\n👤 Пользователь: {user_id}\n🗺️ Хост: {host_name}\n📦 Тариф: {plan_name} ({months} мес.)\n💳 Метод: {payment_method_display}\n💰 Сумма: {float(price):.2f} RUB\n⚙️ Действие: {'Новый ключ' if action == 'new' else 'Продление'}")
        await bot.send_message(admin_id, text)
    except Exception as e: logger.warning(f"notify_admin_of_purchase не удался: {e}")

async def process_successful_payment(bot: Bot, metadata: dict):
    try:
        action = metadata.get('action')
        user_id = int(metadata.get('user_id'))
        price = float(metadata.get('price'))
        months = int(metadata.get('months', 0))
        key_id = int(metadata.get('key_id', 0)) if metadata.get('key_id') is not None else 0
        host_name = metadata.get('host_name', '')
        plan_id = int(metadata.get('plan_id', 0)) if metadata.get('plan_id') is not None else 0
        customer_email = metadata.get('customer_email')
        payment_method = metadata.get('payment_method')
        chat_id_to_delete = metadata.get('chat_id')
        message_id_to_delete = metadata.get('message_id')
    except (ValueError, TypeError) as e:
        logger.error(f"КРИТИЧЕСКАЯ ОШИБКА: Не удалось разобрать метаданные. Ошибка: {e}. Метаданные: {metadata}")
        return
    
    if chat_id_to_delete and message_id_to_delete:
        try:
            await bot.delete_message(chat_id=chat_id_to_delete, message_id=message_id_to_delete)
        except TelegramBadRequest as e:
            logger.warning(f"Не удалось удалить сообщение о платеже: {e}")
    
    if action == "top_up":
        try:
            ok = add_to_balance(user_id, float(price))
        except Exception as e:
            logger.error(f"Не удалось добавить к балансу для пользователя {user_id}: {e}", exc_info=True)
            ok = False
        try:
            user_info = get_user(user_id)
            log_username = user_info.get('username', 'N/A') if user_info else 'N/A'
            log_transaction(
                username=log_username,
                transaction_id=None,
                payment_id=str(uuid.uuid4()),
                user_id=user_id,
                status='paid',
                amount_rub=float(price),
                amount_currency=None,
                currency_name=None,
                payment_method=payment_method or 'Unknown',
                metadata=json.dumps({"action": "top_up"})
            )
        except:
            pass
        try:
            current_balance = 0.0
            try:
                current_balance = float(get_balance(user_id))
            except:
                pass
            if ok:
                await bot.send_message(
                    chat_id=user_id,
                    text=f"✅ Оплата получена!\n💼 Баланс пополнен на {float(price):.2f} RUB.\nТекущий баланс: {current_balance:.2f} RUB.",
                    reply_markup=create_profile_keyboard()
                )
            else:
                await bot.send_message(
                    chat_id=user_id,
                    text="⚠️ Оплата получена, но не удалось обновить баланс. Обратитесь в поддержку.",
                    reply_markup=create_support_keyboard()
                )
        except:
            pass
        try:
            admins = [u for u in (get_all_users() or []) if is_admin(u.get('telegram_id') or 0)]
            for a in admins:
                admin_id = a.get('telegram_id')
                if admin_id:
                    await bot.send_message(admin_id, f"📥 Пополнение: пользователь {user_id}, сумма {float(price):.2f} RUB")
        except:
            pass
        return
    
    processing_message = await bot.send_message(
        chat_id=user_id,
        text=f"✅ Оплата получена! Обрабатываю ваш запрос на сервере \"{host_name}\"..."
    )
    
    try:
        email = ""
        price = float(metadata.get('price'))
        result = None
        
        if action == "new":
            user_data = get_user(user_id) or {}
            raw_username = (user_data.get('username') or f'user{user_id}').lower()
            username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
            base_local = f"{username_slug}"
            candidate_local = base_local
            attempt = 1
            while True:
                candidate_email = f"{candidate_local}@bot.local"
                if not get_key_by_email(candidate_email):
                    break
                attempt += 1
                candidate_local = f"{base_local}-{attempt}"
                if attempt > 100:
                    candidate_local = f"{base_local}-{int(datetime.now().timestamp())}"
                    candidate_email = f"{candidate_local}@bot.local"
                    break
        else:
            existing_key = get_key_by_id(key_id)
            if not existing_key or not existing_key.get('key_email'):
                await processing_message.edit_text("❌ Не удалось найти ключ для продления.")
                return
            candidate_email = existing_key['key_email']
        
        result = await create_or_update_key_on_host(
            host_name=host_name,
            email=candidate_email,
            days_to_add=int(months * 30)
        )
        
        if not result:
            await processing_message.edit_text("❌ Не удалось создать/обновить ключ в панели.")
            return
        
        if action == "new":
            key_id = add_new_key(
                user_id=user_id,
                host_name=host_name,
                xui_client_uuid=result['client_uuid'],
                key_email=result['email'],
                expiry_timestamp_ms=result['expiry_timestamp_ms']
            )
        elif action == "extend":
            update_key_info(key_id, result['client_uuid'], result['expiry_timestamp_ms'])
        
        user_data = get_user(user_id)
        referrer_id = user_data.get('referred_by') if user_data else None
        if referrer_id:
            try:
                referrer_id = int(referrer_id)
            except Exception as e:
                logger.warning(f"Referral: invalid referrer_id={referrer_id} for user {user_id}")
                referrer_id = None
        
        if referrer_id:
            try:
                reward_type = (get_setting("referral_reward_type") or "percent_purchase").strip()
            except:
                reward_type = "percent_purchase"
            reward = Decimal("0")
            
            if reward_type == "fixed_start_referrer":
                reward = Decimal("0")
            elif reward_type == "fixed_purchase":
                try:
                    amount_raw = get_setting("fixed_referral_bonus_amount") or "50"
                    reward = Decimal(str(amount_raw)).quantize(Decimal("0.01"))
                except:
                    reward = Decimal("50.00")
            else:
                try:
                    percentage = Decimal(get_setting("referral_percentage") or "0")
                except:
                    percentage = Decimal("0")
                reward = (Decimal(str(price)) * percentage / 100).quantize(Decimal("0.01"))
            
            logger.info(f"Referral: user={user_id}, referrer={referrer_id}, type={reward_type}, reward={float(reward):.2f}")
            
            if float(reward) > 0:
                try:
                    ok = add_to_balance(referrer_id, float(reward))
                except Exception as e:
                    logger.warning(f"Referral: add_to_balance failed for referrer {referrer_id}: {e}")
                    ok = False
                try:
                    add_to_referral_balance_all(referrer_id, float(reward))
                except Exception as e:
                    logger.warning(f"Failed to increment referral_balance_all for {referrer_id}: {e}")
                
                referrer_username = user_data.get('username', 'пользователь') if user_data else 'пользователь'
                if ok:
                    try:
                        await bot.send_message(
                            chat_id=referrer_id,
                            text=f"💰 Вам начислено реферальное вознаграждение!\nПользователь: {referrer_username} (ID: {user_id})\nСумма: {float(reward):.2f} RUB"
                        )
                    except Exception as e:
                        logger.warning(f"Could not send referral reward notification to {referrer_id}: {e}")
        
        try:
            pm_lower = (payment_method or '').strip().lower()
        except:
            pm_lower = ''
        
        spent_for_stats = 0.0 if pm_lower == 'balance' else float(price)
        update_user_stats(user_id, spent_for_stats, months)
        
        user_info = get_user(user_id)
        log_username = user_info.get('username', 'N/A') if user_info else 'N/A'
        log_status = 'paid'
        log_amount_rub = float(price)
        log_method = metadata.get('payment_method', 'Unknown')
        
        log_metadata = json.dumps({
            "plan_id": metadata.get('plan_id'),
            "plan_name": get_plan_by_id(metadata.get('plan_id')).get('plan_name', 'Unknown') if get_plan_by_id(metadata.get('plan_id')) else 'Unknown',
            "host_name": metadata.get('host_name'),
            "customer_email": metadata.get('customer_email')
        })
        
        payment_id_for_log = metadata.get('payment_id') or str(uuid.uuid4())
        
        log_transaction(
            username=log_username,
            transaction_id=None,
            payment_id=payment_id_for_log,
            user_id=user_id,
            status=log_status,
            amount_rub=log_amount_rub,
            amount_currency=None,
            currency_name=None,
            payment_method=log_method,
            metadata=log_metadata
        )
        
        try:
            promo_code_used = (metadata.get('promo_code') or '').strip()
            if promo_code_used:
                try:
                    applied_amt = 0.0
                    try:
                        if metadata.get('promo_discount_amount') is not None:
                            applied_amt = float(metadata.get('promo_discount_amount') or 0.0)
                    except:
                        applied_amt = 0.0
                    redeemed = redeem_promo_code(
                        promo_code_used,
                        user_id,
                        applied_amount=float(applied_amt or 0.0),
                        order_id=payment_id_for_log,
                    )
                    if redeemed:
                        limit_total = redeemed.get('usage_limit_total')
                        per_user_limit = redeemed.get('usage_limit_per_user')
                        used_total_now = redeemed.get('used_total') or 0
                        user_usage_count = redeemed.get('user_usage_count')
                        should_deactivate = False
                        reason_lines = []
                        
                        if limit_total:
                            try:
                                if used_total_now >= int(limit_total):
                                    should_deactivate = True
                                    reason_lines.append("достигнут общий лимит использования")
                            except:
                                pass
                        
                        if per_user_limit:
                            try:
                                if (user_usage_count or 0) >= int(per_user_limit):
                                    should_deactivate = True
                                    reason_lines.append("исчерпан лимит на пользователя")
                            except:
                                pass
                        
                        if not should_deactivate and (limit_total or per_user_limit):
                            should_deactivate = True
                            reason_lines.append("лимит выставлен (код погашён)")
                        
                        if should_deactivate:
                            try:
                                update_promo_code_status(promo_code_used, is_active=False)
                            except:
                                pass
                        
                        try:
                            plan = get_plan_by_id(plan_id)
                            plan_name = plan.get('plan_name', 'Unknown') if plan else 'Unknown'
                            admins = list(get_admin_ids() or [])
                            if should_deactivate:
                                status_line = "Статус: деактивирован"
                                if reason_lines:
                                    status_line += " (" + ", ".join(reason_lines) + ")"
                            else:
                                status_line = "Статус: активен"
                                if limit_total:
                                    status_line += f" (использовано {used_total_now} из {limit_total})"
                                else:
                                    status_line += f" (использовано {used_total_now})"
                            text = f"🎟️ Промокод использован\nКод: {promo_code_used}\nПользователь: {user_id}\nТариф: {plan_name} ({months} мес.)\n{status_line}"
                            for aid in admins:
                                try:
                                    await bot.send_message(int(aid), text)
                                except:
                                    pass
                        except:
                            pass
                except Exception as e:
                    logger.warning(f"Promo redeem failed for user {user_id}, code {promo_code_used}: {e}")
        except:
            pass
        
        try:
            await processing_message.delete()
        except:
            pass
        
        connection_string = None
        new_expiry_date = None
        try:
            connection_string = result.get('connection_string') if isinstance(result, dict) else None
            new_expiry_date = datetime.fromtimestamp(result['expiry_timestamp_ms'] / 1000) if isinstance(result, dict) and 'expiry_timestamp_ms' in result else None
        except:
            connection_string = None
            new_expiry_date = None
        
        all_user_keys = get_user_keys(user_id)
        key_number = next((i + 1 for i, key in enumerate(all_user_keys) if key['key_id'] == key_id), len(all_user_keys))
        
        final_text = f"🎉 <b>Ваш ключ #{key_number} {'создан' if action == 'new' else 'продлен'}!</b>\n\n⏳ <b>Он будет действовать до:</b> {(new_expiry_date or datetime.now()).strftime('%d.%m.%Y в %H:%M')}\n\n{html.code(connection_string or '')}"
        
        await bot.send_message(
            chat_id=user_id,
            text=final_text,
            reply_markup=create_key_info_keyboard(key_id)
        )
        
        try:
            await notify_admin_of_purchase(bot, metadata)
        except Exception as e:
            logger.warning(f"Failed to notify admin of purchase: {e}")
            
    except Exception as e:
        logger.error(f"Error processing payment for user {user_id} on host {host_name}: {e}", exc_info=True)
        try:
            await processing_message.edit_text("❌ Ошибка при выдаче ключа.")
        except:
            try:
                await bot.send_message(chat_id=user_id, text="❌ Ошибка при выдаче ключа.")
            except:
                pass

# ==================== MAIN ====================
def main():
    if colorama_available:
        try: colorama.just_fix_windows_console()
        except: pass
    class ColoredFormatter(logging.Formatter):
        COLORS = {'DEBUG': '\x1b[36m', 'INFO': '\x1b[32m', 'WARNING': '\x1b[33m', 'ERROR': '\x1b[31m', 'CRITICAL': '\x1b[41m'}
        RESET = '\x1b[0m'
        def format(self, record: logging.LogRecord) -> str:
            level = record.levelname
            color = self.COLORS.get(level, '')
            reset = self.RESET if color else ''
            fmt = "%(asctime)s [%(levelname)s] %(message)s"
            datefmt = "%H:%M:%S"
            base = logging.Formatter(fmt=fmt, datefmt=datefmt)
            msg = base.format(record)
            if color: msg = msg.replace(f"[{level}]", f"{color}[{level}]{reset}")
            return msg
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    for h in list(root.handlers): root.removeHandler(h)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(ColoredFormatter())
    root.addHandler(ch)
    logging.getLogger('werkzeug').setLevel(logging.WARNING)
    aio_event_logger = logging.getLogger('aiogram.event')
    aio_event_logger.setLevel(logging.INFO)
    logging.getLogger('aiogram.dispatcher').setLevel(logging.WARNING)
    logging.getLogger('aiohttp').setLevel(logging.WARNING)
    logging.getLogger('paramiko').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    class RussianizeAiogramFilter(logging.Filter):
        def filter(self, record: logging.LogRecord) -> bool:
            try:
                msg = record.getMessage()
                if 'Update id=' in msg:
                    m = re.search(r"Update id=(\d+)\s+is\s+(not handled|handled)\.\s+Duration\s+(\d+)\s+ms\s+by bot id=(\d+)", msg)
                    if m:
                        upd_id, state, dur_ms, bot_id = m.groups()
                        state_ru = 'не обработано' if state == 'not handled' else 'обработано'
                        msg = f"Обновление {upd_id} {state_ru} за {dur_ms} мс (бот {bot_id})"
                        record.msg = msg
                        record.args = ()
                    else:
                        msg = msg.replace('Update id=', 'Обновление ')
                        msg = msg.replace(' is handled.', ' обработано.')
                        msg = msg.replace(' is not handled.', ' не обработано.')
                        msg = msg.replace('Duration', 'за')
                        msg = msg.replace('by bot id=', '(бот ')
                        if msg.endswith(')') is False and 'бот ' in msg: msg = msg + ')'
                        record.msg = msg
                        record.args = ()
            except: pass
            return True
    aio_event_logger.addFilter(RussianizeAiogramFilter())
    logger = logging.getLogger(__name__)
    initialize_db()
    logger.info("Проверка инициализации базы данных завершена.")
    bot_controller = BotController()
    support_bot_controller = SupportBotController()
    from flask import Flask, request, render_template, redirect, url_for, flash, session, jsonify, send_file
    from flask_wtf.csrf import CSRFProtect, generate_csrf
    ALL_SETTINGS_KEYS = [
        "panel_login", "panel_password", "about_text", "terms_url", "privacy_url",
        "support_user", "support_text", "main_menu_text", "howto_android_text",
        "howto_ios_text", "howto_windows_text", "howto_linux_text",
        "btn_try", "btn_profile", "btn_my_keys", "btn_buy_key", "btn_top_up",
        "btn_referral", "btn_support", "btn_about", "btn_howto", "btn_admin",
        "btn_back_to_menu", "btn_channel", "btn_terms", "btn_privacy",
        "btn_howto_android", "btn_howto_ios", "btn_howto_windows", "btn_howto_linux",
        "btn_back", "btn_back_to_plans", "btn_back_to_key", "btn_back_to_keys",
        "btn_extend_key", "btn_show_qr", "btn_instruction", "btn_switch_server",
        "btn_skip_email", "btn_go_to_payment", "btn_check_payment", "btn_pay_with_balance",
        "btn_support_open", "btn_support_new_ticket", "btn_support_my_tickets", "btn_support_external",
        "channel_url", "telegram_bot_token", "telegram_bot_username", "admin_telegram_id",
        "yookassa_shop_id", "yookassa_secret_key", "sbp_enabled", "receipt_email",
        "cryptobot_token", "heleket_merchant_id", "heleket_api_key", "domain",
        "referral_percentage", "referral_discount", "ton_wallet_address", "tonapi_key",
        "force_subscription", "trial_enabled", "trial_duration_days", "enable_referrals",
        "minimum_withdrawal", "enable_fixed_referral_bonus", "fixed_referral_bonus_amount",
        "referral_reward_type", "referral_on_start_referrer_amount",
        "support_forum_chat_id", "support_bot_token", "support_bot_username",
        "panel_brand_title", "backup_interval_days", "monitoring_enabled",
        "monitoring_interval_sec", "monitoring_cpu_threshold", "monitoring_mem_threshold",
        "monitoring_disk_threshold", "monitoring_alert_cooldown_sec",
        "stars_enabled", "stars_per_rub", "stars_title", "stars_description",
        "yoomoney_enabled", "yoomoney_wallet", "yoomoney_secret", "yoomoney_api_token",
        "yoomoney_client_id", "yoomoney_client_secret", "yoomoney_redirect_uri",
        "admin_telegram_ids"
    ]
    def create_webhook_app(bot_controller_instance):
        nonlocal support_bot_controller
        app_file_path = os.path.abspath(__file__)
        app_dir = os.path.dirname(app_file_path)
        template_dir = os.path.join(app_dir, 'templates')
        template_file = os.path.join(template_dir, 'login.html')
        logger.debug(f"--- ДИАГНОСТИКА ---\nТекущая директория: {os.getcwd()}\nПуть к app: {app_file_path}\nДиректория шаблонов: {template_dir}\nФайл login.html существует? -> {os.path.isfile(template_file)}\n--- КОНЕЦ ---")
        flask_app = Flask(__name__, template_folder='templates', static_folder='static')
        flask_app.config['SECRET_KEY'] = os.getenv('SHOPBOT_SECRET_KEY') or secrets.token_hex(32)
        flask_app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=30)
        csrf = CSRFProtect()
        csrf.init_app(flask_app)
        @flask_app.context_processor
        def inject_current_year(): return {'current_year': datetime.utcnow().year, 'csrf_token': generate_csrf}
        def login_required(f):
            @wraps(f)
            def decorated_function(*args, **kwargs):
                if 'logged_in' not in session: return redirect(url_for('login_page'))
                return f(*args, **kwargs)
            return decorated_function
        @flask_app.route('/login', methods=['GET', 'POST'])
        def login_page():
            settings = get_all_settings()
            if request.method == 'POST':
                if request.form.get('username') == settings.get("panel_login") and request.form.get('password') == settings.get("panel_password"):
                    session['logged_in'] = True
                    session.permanent = bool(request.form.get('remember_me'))
                    return redirect(url_for('dashboard_page'))
                else: flash('Неверный логин или пароль', 'danger')
            return render_template('login.html')
        @flask_app.route('/logout', methods=['POST'])
        @login_required
        def logout_page():
            session.pop('logged_in', None)
            flash('Вы успешно вышли.', 'success')
            return redirect(url_for('login_page'))
        def get_common_template_data():
            bot_status = bot_controller_instance.get_status()
            support_bot_status = support_bot_controller.get_status()
            settings = get_all_settings()
            required_for_start = ['telegram_bot_token', 'telegram_bot_username', 'admin_telegram_id']
            required_support_for_start = ['support_bot_token', 'support_bot_username', 'admin_telegram_id']
            all_settings_ok = all(settings.get(key) for key in required_for_start)
            support_settings_ok = all(settings.get(key) for key in required_support_for_start)
            try: open_tickets_count = get_open_tickets_count(); closed_tickets_count = get_closed_tickets_count(); all_tickets_count = get_all_tickets_count()
            except: open_tickets_count = 0; closed_tickets_count = 0; all_tickets_count = 0
            return {
                "bot_status": bot_status, "all_settings_ok": all_settings_ok,
                "support_bot_status": support_bot_status, "support_settings_ok": support_settings_ok,
                "open_tickets_count": open_tickets_count, "closed_tickets_count": closed_tickets_count,
                "all_tickets_count": all_tickets_count, "brand_title": settings.get('panel_brand_title') or 'T‑Shift VPN',
            }
        @flask_app.route('/brand-title', methods=['POST'])
        @login_required
        def update_brand_title_route():
            title = (request.form.get('title') or '').strip()
            if not title: return jsonify({"ok": False, "error": "empty"}), 400
            try: update_setting('panel_brand_title', title); return jsonify({"ok": True, "title": title})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/monitor/host/<host_name>/metrics.json')
        @login_required
        def monitor_host_metrics_json(host_name: str):
            try: limit = int(request.args.get('limit', '60'))
            except: limit = 60
            try: items = get_host_metrics_recent(host_name, limit=limit); return jsonify({"ok": True, "items": items})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/')
        @login_required
        def index(): return redirect(url_for('dashboard_page'))
        @flask_app.route('/dashboard')
        @login_required
        def dashboard_page():
            hosts = []
            try:
                hosts = get_all_hosts()
                for h in hosts:
                    try: h['latest_speedtest'] = get_latest_speedtest(h['host_name'])
                    except: h['latest_speedtest'] = None
            except: hosts = []
            stats = {"user_count": get_user_count(), "total_keys": get_total_keys_count(), "total_spent": get_total_spent_sum(), "host_count": len(hosts)}
            page = request.args.get('page', 1, type=int)
            per_page = 8
            transactions, total_transactions = get_paginated_transactions(page=page, per_page=per_page)
            total_pages = ceil(total_transactions / per_page)
            chart_data = get_daily_stats_for_charts(days=30)
            common_data = get_common_template_data()
            return render_template('dashboard.html', stats=stats, chart_data=chart_data, transactions=transactions, current_page=page, total_pages=total_pages, hosts=hosts, **common_data)
        @flask_app.route('/dashboard/run-speedtests', methods=['POST'])
        @login_required
        def run_speedtests_route():
            try: speedtest_runner.run_speedtests_for_all_hosts(); return jsonify({"ok": True})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/dashboard/stats.partial')
        @login_required
        def dashboard_stats_partial():
            stats = {"user_count": get_user_count(), "total_keys": get_total_keys_count(), "total_spent": get_total_spent_sum(), "host_count": len(get_all_hosts())}
            common_data = get_common_template_data()
            return render_template('partials/dashboard_stats.html', stats=stats, **common_data)
        @flask_app.route('/dashboard/transactions.partial')
        @login_required
        def dashboard_transactions_partial():
            page = request.args.get('page', 1, type=int); per_page = 8
            transactions, total_transactions = get_paginated_transactions(page=page, per_page=per_page)
            return render_template('partials/dashboard_transactions.html', transactions=transactions)
        @flask_app.route('/dashboard/charts.json')
        @login_required
        def dashboard_charts_json():
            data = get_daily_stats_for_charts(days=30); return jsonify(data)
        @flask_app.route('/monitor')
        @login_required
        def monitor_page():
            common_data = get_common_template_data()
            hosts = get_all_hosts()
            ssh_targets = []
            common_data.update({'hosts': hosts, 'ssh_targets': ssh_targets})
            return render_template('monitor.html', **common_data)
        @flask_app.route('/monitor/local.json')
        @login_required
        def monitor_local_json():
            try: data = get_local_metrics(); return jsonify(data)
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/monitor/hosts.json')
        @login_required
        def monitor_hosts_json():
            try: data = collect_hosts_metrics(); return jsonify(data)
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/monitor/host/<host_name>.json')
        @login_required
        def monitor_host_json(host_name: str):
            try:
                host = get_host(host_name)
                if not host: return jsonify({"ok": False, "error": "host not found"}), 404
                data = get_host_metrics_via_ssh(host); return jsonify(data)
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/monitor/metrics/<scope>/<object_name>.json')
        @login_required
        def monitor_metrics_json(scope: str, object_name: str):
            try: since_hours = int(request.args.get('since_hours', '24')); limit = int(request.args.get('limit', '500')); items = get_metrics_series(scope, object_name, since_hours=since_hours, limit=limit); return jsonify({"ok": True, "items": items})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/support/table.partial')
        @login_required
        def support_table_partial():
            status = request.args.get('status') or None
            page = request.args.get('page', 1, type=int); per_page = 12
            tickets, total = get_tickets_paginated(page=page, per_page=per_page, status=status)
            return render_template('partials/support_table.html', tickets=tickets)
        @flask_app.route('/support/open-count.partial')
        @login_required
        def support_open_count_partial():
            try: count = get_open_tickets_count() or 0
            except: count = 0
            if count and count > 0: html = f'<span class="badge bg-green-lt" title="Открытые тикеты"><span class="status-dot status-dot-animated bg-green"></span> {count}</span>'
            else: html = ''
            return html, 200, {"Content-Type": "text/html; charset=utf-8"}
        @flask_app.route('/users')
        @login_required
        def users_page():
            page = request.args.get('page', 1, type=int); per_page = request.args.get('per_page', 20, type=int); q = (request.args.get('q') or '').strip()
            users, total = get_users_paginated(page=page, per_page=per_page, q=q or None)
            for user in users:
                uid = user['telegram_id']
                user['user_keys'] = get_user_keys(uid)
                try: user['balance'] = get_balance(uid); user['referrals'] = get_referrals_for_user(uid)
                except: user['balance'] = 0.0; user['referrals'] = []
            total_pages = max(1, ceil(total / per_page)) if total else 1
            common_data = get_common_template_data()
            return render_template('users.html', users=users, current_page=page, total_pages=total_pages, total_users=total, per_page=per_page, q=q, **common_data)
        @flask_app.route('/users/table.partial')
        @login_required
        def users_table_partial():
            page = request.args.get('page', 1, type=int); per_page = request.args.get('per_page', 20, type=int); q = (request.args.get('q') or '').strip()
            users, total = get_users_paginated(page=page, per_page=per_page, q=q or None)
            for user in users:
                uid = user['telegram_id']
                user['user_keys'] = get_user_keys(uid)
                try: user['balance'] = get_balance(uid); user['referrals'] = get_referrals_for_user(uid)
                except: user['balance'] = 0.0; user['referrals'] = []
            return render_template('partials/users_table.html', users=users)
        @flask_app.route('/users/<int:user_id>/balance/adjust', methods=['POST'])
        @login_required
        def adjust_balance_route(user_id: int):
            try: delta = float(request.form.get('delta', '0') or '0')
            except ValueError:
                wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
                if wants_json: return jsonify({"ok": False, "error": "invalid_amount"}), 400
                flash('Некорректная сумма изменения баланса.', 'danger'); return redirect(url_for('users_page'))
            ok = adjust_user_balance(user_id, delta)
            message = 'Баланс изменён.' if ok else 'Не удалось изменить баланс.'
            category = 'success' if ok else 'danger'
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json: return jsonify({"ok": ok, "message": message})
            flash(message, category)
            try:
                if ok:
                    bot = bot_controller_instance.get_bot_instance()
                    if bot:
                        sign = '+' if delta >= 0 else ''
                        text = f"💳 Ваш баланс был изменён администратором: {sign}{delta:.2f} RUB\nТекущий баланс: {get_balance(user_id):.2f} RUB"
                        loop = current_app.config.get('EVENT_LOOP')
                        if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                        else: asyncio.run(bot.send_message(chat_id=user_id, text=text))
                    else: logger.warning("Экземпляр бота отсутствует; не могу отправить уведомление о балансе")
            except Exception as e: logger.warning(f"Не удалось отправить уведомление о балансе: {e}")
            return redirect(url_for('users_page'))
        @flask_app.route('/admin/keys')
        @login_required
        def admin_keys_page():
            try: keys = get_all_keys()
            except: keys = []
            try: hosts = get_all_hosts()
            except: hosts = []
            try: users = get_all_users()
            except: users = []
            common_data = get_common_template_data()
            return render_template('admin_keys.html', keys=keys, hosts=hosts, users=users, **common_data)
        @flask_app.route('/admin/keys/table.partial')
        @login_required
        def admin_keys_table_partial():
            try: keys = get_all_keys()
            except: keys = []
            return render_template('partials/admin_keys_table.html', keys=keys)
        @flask_app.route('/admin/hosts/<host_name>/plans')
        @login_required
        def admin_get_plans_for_host_json(host_name: str):
            try:
                plans = get_plans_for_host(host_name)
                data = [{"plan_id": p.get('plan_id'), "plan_name": p.get('plan_name'), "months": p.get('months'), "price": p.get('price')} for p in plans]
                return jsonify({"ok": True, "items": data})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/admin/keys/create', methods=['POST'])
        @login_required
        def create_key_route():
            try:
                user_id = int(request.form.get('user_id'))
                host_name = (request.form.get('host_name') or '').strip()
                xui_uuid = (request.form.get('xui_client_uuid') or '').strip()
                key_email = (request.form.get('key_email') or '').strip()
                expiry = request.form.get('expiry_date') or ''
                expiry_ms = int(datetime.fromisoformat(expiry).timestamp() * 1000) if expiry else 0
            except: flash('Проверьте поля ключа.', 'danger'); return redirect(request.referrer or url_for('admin_keys_page'))
            if not xui_uuid: xui_uuid = str(uuid.uuid4())
            result = None
            try: result = asyncio.run(create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
            except Exception as e: logger.error(f"Не удалось создать/обновить ключ на хосте: {e}"); result = None
            if not result: flash('Не удалось создать ключ на хосте. Проверьте доступность XUI.', 'danger'); return redirect(request.referrer or url_for('admin_keys_page'))
            try: xui_uuid = result.get('client_uuid') or xui_uuid; expiry_ms = result.get('expiry_timestamp_ms') or expiry_ms
            except: pass
            new_id = add_new_key(user_id, host_name, xui_uuid, key_email, expiry_ms or 0)
            flash(('Ключ добавлен.' if new_id else 'Ошибка при добавлении ключа.'), 'success' if new_id else 'danger')
            try:
                bot = bot_controller_instance.get_bot_instance()
                if bot and new_id:
                    text = f'🔐 Ваш ключ готов!\nСервер: {host_name}\nВыдан администратором через панель.\n'
                    if result and result.get('connection_string'): cs = html_escape.escape(result['connection_string']); text += f"\nПодключение:\n<code>{cs}</code>"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True), loop)
                    else: asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
            except Exception as e: logger.warning(f"Не удалось уведомить пользователя о новом ключе: {e}")
            return redirect(request.referrer or url_for('admin_keys_page'))
        @flask_app.route('/admin/keys/create-ajax', methods=['POST'])
        @login_required
        def create_key_ajax_route():
            try:
                user_id = int(request.form.get('user_id'))
                host_name = (request.form.get('host_name') or '').strip()
                xui_uuid = (request.form.get('xui_client_uuid') or '').strip()
                key_email = (request.form.get('key_email') or '').strip()
                expiry = request.form.get('expiry_date') or ''
                expiry_ms = int(datetime.fromisoformat(expiry).timestamp() * 1000) if expiry else 0
            except Exception as e: return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400
            if not xui_uuid: xui_uuid = str(uuid.uuid4())
            try: result = asyncio.run(create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
            except Exception as e: result = None; logger.error(f"create_key_ajax_route: ошибка панели/хоста: {e}")
            if not result: return jsonify({"ok": False, "error": "host_failed"}), 500
            new_id = add_new_key(user_id, host_name, result.get('client_uuid') or xui_uuid, key_email, result.get('expiry_timestamp_ms') or expiry_ms or 0)
            try:
                bot = bot_controller_instance.get_bot_instance()
                if bot and new_id:
                    text = f'🔐 Ваш ключ готов!\nСервер: {host_name}\nВыдан администратором через панель.\n'
                    if result and result.get('connection_string'): cs = html_escape.escape(result['connection_string']); text += f"\nПодключение:\n<pre><code>{cs}</code></pre>"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True), loop)
                    else: asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
            except Exception as e: logger.warning(f"Не удалось уведомить пользователя (ajax): {e}")
            return jsonify({"ok": True, "key_id": new_id, "uuid": result.get('client_uuid'), "expiry_ms": result.get('expiry_timestamp_ms'), "connection": result.get('connection_string')})
        @flask_app.route('/admin/keys/generate-gift-email')
        @login_required
        def generate_gift_email_route():
            try:
                for _ in range(12):
                    candidate_email = f"gift-{int(time.time())}-{secrets.token_hex(2)}@bot.local"
                    if not get_key_by_email(candidate_email): return jsonify({"ok": True, "email": candidate_email})
                return jsonify({"ok": False, "error": "no_unique_email"}), 500
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/admin/keys/create-standalone-ajax', methods=['POST'])
        @login_required
        def create_key_standalone_ajax_route():
            key_type = (request.form.get('key_type') or 'personal').strip()
            try:
                if key_type == 'gift': user_id = 0
                else: user_id = int(request.form.get('user_id'))
                host_name = (request.form.get('host_name') or '').strip()
                xui_uuid = (request.form.get('xui_client_uuid') or '').strip()
                key_email = (request.form.get('key_email') or '').strip()
                expiry = request.form.get('expiry_date') or ''
                comment = (request.form.get('comment') or '').strip()
                from datetime import datetime as _dt
                expiry_ms = int(_dt.fromisoformat(expiry).timestamp() * 1000) if expiry else 0
            except Exception as e: return jsonify({"ok": False, "error": f"invalid input: {e}"}), 400
            if key_type == 'gift' and not key_email:
                try:
                    for _ in range(12):
                        candidate_email = f"gift-{int(time.time())}-{secrets.token_hex(2)}@bot.local"
                        if not get_key_by_email(candidate_email): key_email = candidate_email; break
                except: pass
            if not xui_uuid: xui_uuid = str(uuid.uuid4())
            try: result = asyncio.run(create_or_update_key_on_host(host_name, key_email, expiry_timestamp_ms=expiry_ms or None))
            except Exception as e: result = None; logger.error(f"create_key_standalone_ajax_route: ошибка панели/хоста: {e}")
            if not result: return jsonify({"ok": False, "error": "host_failed"}), 500
            new_id = add_new_key(user_id, host_name, result.get('client_uuid') or xui_uuid, key_email, result.get('expiry_timestamp_ms') or expiry_ms or 0)
            if comment and new_id:
                try: update_key_comment(int(new_id), comment)
                except: pass
            if key_type != 'gift' and user_id:
                try:
                    bot = bot_controller_instance.get_bot_instance()
                    if bot and new_id:
                        text = f'🔐 Ваш ключ готов!\nСервер: {host_name}\nВыдан администратором через панель.\n'
                        if result and result.get('connection_string'): cs = html_escape.escape(result['connection_string']); text += f"\nПодключение:\n<pre><code>{cs}</code></pre>"
                        loop = current_app.config.get('EVENT_LOOP')
                        if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True), loop)
                        else: asyncio.run(bot.send_message(chat_id=user_id, text=text, parse_mode='HTML', disable_web_page_preview=True))
                except Exception as notify_err: logger.warning(f"Не удалось уведомить пользователя (standalone ajax): {notify_err}")
            return jsonify({"ok": True, "key_id": new_id, "uuid": result.get('client_uuid'), "expiry_ms": result.get('expiry_timestamp_ms'), "connection": result.get('connection_string')})
        @flask_app.route('/admin/keys/generate-email')
        @login_required
        def generate_key_email_route():
            try: user_id = int(request.args.get('user_id'))
            except: return jsonify({"ok": False, "error": "invalid user_id"}), 400
            try:
                user = get_user(user_id) or {}
                raw_username = (user.get('username') or f'user{user_id}').lower()
                username_slug = re.sub(r"[^a-z0-9._-]", "_", raw_username).strip("_")[:16] or f"user{user_id}"
                base_local = f"{username_slug}"; candidate_local = base_local; attempt = 1
                while True:
                    candidate_email = f"{candidate_local}@bot.local"
                    if not get_key_by_email(candidate_email): break
                    attempt += 1; candidate_local = f"{base_local}-{attempt}"
                return jsonify({"ok": True, "email": candidate_email})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/admin/keys/<int:key_id>/delete', methods=['POST'])
        @login_required
        def delete_key_route(key_id: int):
            try:
                key = get_key_by_id(key_id)
                if key:
                    try: asyncio.run(delete_client_on_host(key['host_name'], key['key_email']))
                    except: pass
            except: pass
            ok = delete_key_by_id(key_id)
            flash('Ключ удалён.' if ok else 'Не удалось удалить ключ.', 'success' if ok else 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))
        @flask_app.route('/admin/keys/<int:key_id>/adjust-expiry', methods=['POST'])
        @login_required
        def adjust_key_expiry_route(key_id: int):
            try: delta_days = int(request.form.get('delta_days', '0'))
            except: return jsonify({"ok": False, "error": "invalid_delta"}), 400
            key = get_key_by_id(key_id)
            if not key: return jsonify({"ok": False, "error": "not_found"}), 404
            try:
                cur_expiry = key.get('expiry_date')
                from datetime import datetime, timedelta
                if isinstance(cur_expiry, str):
                    try: exp_dt = datetime.fromisoformat(cur_expiry)
                    except:
                        try: exp_dt = datetime.strptime(cur_expiry, '%Y-%m-%d %H:%M:%S')
                        except: exp_dt = datetime.utcnow()
                else: exp_dt = cur_expiry or datetime.utcnow()
                new_dt = exp_dt + timedelta(days=delta_days)
                new_ms = int(new_dt.timestamp() * 1000)
                try: result = asyncio.run(create_or_update_key_on_host(host_name=key.get('host_name'), email=key.get('key_email'), expiry_timestamp_ms=new_ms))
                except Exception as e: result = None
                if not result or not result.get('expiry_timestamp_ms'): return jsonify({"ok": False, "error": "xui_update_failed"}), 500
                client_uuid = result.get('client_uuid') or key.get('xui_client_uuid') or ''
                update_key_info(key_id, client_uuid, int(result.get('expiry_timestamp_ms')))
                try:
                    user_id = key.get('user_id')
                    new_ms_final = int(result.get('expiry_timestamp_ms'))
                    from datetime import datetime as _dt
                    new_dt_local = _dt.fromtimestamp(new_ms_final/1000)
                    text = f"🗓️ Срок вашего VPN-ключа изменён администратором.\nХост: {key.get('host_name')}\nEmail ключа: {key.get('key_email')}\nНовая дата истечения: {new_dt_local.strftime('%Y-%m-%d %H:%M')}"
                    if user_id:
                        bot = bot_controller_instance.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        if bot and loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                        elif bot: asyncio.run(bot.send_message(chat_id=user_id, text=text))
                except: pass
                return jsonify({"ok": True, "new_expiry_ms": int(result.get('expiry_timestamp_ms'))})
            except Exception as e: return jsonify({"ok": False, "error": str(e)}), 500
        @flask_app.route('/admin/keys/sweep-expired', methods=['POST'])
        @login_required
        def sweep_expired_keys_route():
            from datetime import datetime
            removed = 0; failed = 0
            now = datetime.utcnow()
            keys = get_all_keys()
            for k in keys:
                exp = k.get('expiry_date'); exp_dt = None
                try:
                    if isinstance(exp, str):
                        try: exp_dt = datetime.fromisoformat(exp)
                        except:
                            try: exp_dt = datetime.strptime(exp, '%Y-%m-%d %H:%M:%S')
                            except: exp_dt = None
                    else: exp_dt = exp
                except: exp_dt = None
                if not exp_dt or exp_dt > now: continue
                try:
                    try: asyncio.run(delete_client_on_host(k.get('host_name'), k.get('key_email')))
                    except: pass
                    delete_key_by_id(k.get('key_id')); removed += 1
                    try:
                        bot = bot_controller_instance.get_bot_instance()
                        loop = current_app.config.get('EVENT_LOOP')
                        text = f"Ваш ключ был автоматически удалён по истечении срока.\nХост: {k.get('host_name')}\nEmail: {k.get('key_email')}\nПри необходимости вы можете оформить новый ключ."
                        if bot and loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=k.get('user_id'), text=text), loop)
                        else: asyncio.run(bot.send_message(chat_id=k.get('user_id'), text=text))
                    except: pass
                except: failed += 1
            flash(f"Удалено истёкших ключей: {removed}. Ошибок: {failed}.", 'success' if failed == 0 else 'warning')
            return redirect(request.referrer or url_for('admin_keys_page'))
        @flask_app.route('/admin/keys/<int:key_id>/comment', methods=['POST'])
        @login_required
        def update_key_comment_route(key_id: int):
            comment = (request.form.get('comment') or '').strip()
            ok = update_key_comment(key_id, comment)
            flash('Комментарий обновлён.' if ok else 'Не удалось обновить комментарий.', 'success' if ok else 'danger')
            return redirect(request.referrer or url_for('admin_keys_page'))
        @flask_app.route('/admin/hosts/ssh/update', methods=['POST'])
        @login_required
        def update_host_ssh_route():
            host_name = (request.form.get('host_name') or '').strip()
            ssh_host = (request.form.get('ssh_host') or '').strip() or None
            ssh_port_raw = (request.form.get('ssh_port') or '').strip()
            ssh_user = (request.form.get('ssh_user') or '').strip() or None
            ssh_password = request.form.get('ssh_password')
            ssh_key_path = (request.form.get('ssh_key_path') or '').strip() or None
            ssh_port = None
            try: ssh_port = int(ssh_port_raw) if ssh_port_raw else None
            except: ssh_port = None
            ok = update_host_ssh_settings(host_name, ssh_host=ssh_host, ssh_port=ssh_port, ssh_user=ssh_user, ssh_password=ssh_password, ssh_key_path=ssh_key_path)
            flash('SSH-параметры обновлены.' if ok else 'Не удалось обновить SSH-параметры.', 'success' if ok else 'danger')
            return redirect(request.referrer or url_for('settings_page'))
        @flask_app.route('/admin/hosts/<host_name>/speedtest/run', methods=['POST'])
        @login_required
        def run_host_speedtest_route(host_name: str):
            method = (request.form.get('method') or '').strip().lower()
            try:
                if method == 'ssh': res = asyncio.run(run_and_store_ssh_speedtest(host_name))
                elif method == 'net': res = asyncio.run(run_and_store_net_probe(host_name))
                else: res = asyncio.run(run_both_for_host(host_name))
            except Exception as e: res = {'ok': False, 'error': str(e)}
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json: return jsonify(res)
            flash(('Тест выполнен.' if res and res.get('ok') else f"Ошибка теста: {res.get('error') if res else 'unknown'}"), 'success' if res and res.get('ok') else 'danger')
            return redirect(request.referrer or url_for('settings_page'))
        @flask_app.route('/admin/hosts/<host_name>/speedtests.json')
        @login_required
        def host_speedtests_json(host_name: str):
            try: limit = int(request.args.get('limit') or 20)
            except: limit = 20
            try: items = get_speedtests(host_name, limit=limit) or []; return jsonify({'ok': True, 'items': items})
            except Exception as e: return jsonify({'ok': False, 'error': str(e)}), 500
        @flask_app.route('/admin/speedtests/run-all', methods=['POST'])
        @login_required
        def run_all_speedtests_route():
            try: hosts = get_all_hosts()
            except: hosts = []
            errors = []; ok_count = 0
            for h in hosts:
                name = h.get('host_name')
                if not name: continue
                try:
                    res = asyncio.run(run_both_for_host(name))
                    if res and res.get('ok'): ok_count += 1
                    else: errors.append(f"{name}: {res.get('error') if res else 'unknown'}")
                except Exception as e: errors.append(f"{name}: {e}")
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json: return jsonify({"ok": len(errors) == 0, "done": ok_count, "total": len(hosts), "errors": errors})
            if errors: flash(f"Выполнено для {ok_count}/{len(hosts)}. Ошибки: {'; '.join(errors[:3])}{'…' if len(errors) > 3 else ''}", 'warning')
            else: flash(f"Тесты скорости выполнены для всех хостов: {ok_count}/{len(hosts)}", 'success')
            return redirect(request.referrer or url_for('dashboard_page'))
        @flask_app.route('/admin/hosts/<host_name>/speedtest/install', methods=['POST'])
        @login_required
        def auto_install_speedtest_route(host_name: str):
            try: res = asyncio.run(auto_install_speedtest_on_host(host_name))
            except Exception as e: res = {'ok': False, 'log': str(e)}
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json: return jsonify({"ok": bool(res.get('ok')), "log": res.get('log')})
            flash(('Установка завершена успешно.' if res.get('ok') else 'Не удалось установить speedtest на хост.'), 'success' if res.get('ok') else 'danger')
            try:
                log = res.get('log') or ''
                short = '\n'.join((log.splitlines() or [])[-20:])
                if short: flash(short, 'secondary')
            except: pass
            return redirect(request.referrer or url_for('settings_page'))
        @flask_app.route('/admin/balance')
        @login_required
        def admin_balance_page():
            try: user_id = request.args.get('user_id', type=int)
            except: user_id = None
            user = None; balance = None; referrals = []
            if user_id:
                try: user = get_user(user_id); balance = get_balance(user_id); referrals = get_referrals_for_user(user_id)
                except: pass
            common_data = get_common_template_data()
            return render_template('admin_balance.html', user=user, balance=balance, referrals=referrals, **common_data)
        @flask_app.route('/support')
        @login_required
        def support_list_page():
            status = request.args.get('status')
            page = request.args.get('page', 1, type=int); per_page = 12
            tickets, total = get_tickets_paginated(page=page, per_page=per_page, status=status if status in ['open', 'closed'] else None)
            total_pages = ceil(total / per_page) if per_page else 1
            open_count = get_open_tickets_count(); closed_count = get_closed_tickets_count(); all_count = get_all_tickets_count()
            common_data = get_common_template_data()
            return render_template('support.html', tickets=tickets, current_page=page, total_pages=total_pages, filter_status=status, open_count=open_count, closed_count=closed_count, all_count=all_count, **common_data)
        @flask_app.route('/support/<int:ticket_id>', methods=['GET', 'POST'])
        @login_required
        def support_ticket_page(ticket_id):
            ticket = get_ticket(ticket_id)
            if not ticket: flash('Тикет не найден.', 'danger'); return redirect(url_for('support_list_page'))
            if request.method == 'POST':
                message = (request.form.get('message') or '').strip()
                action = request.form.get('action')
                if action == 'reply':
                    if not message: flash('Сообщение не может быть пустым.', 'warning')
                    else:
                        add_support_message(ticket_id, sender='admin', content=message)
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            user_chat_id = ticket.get('user_id')
                            if bot and loop and loop.is_running() and user_chat_id:
                                text = f"Ответ по тикету #{ticket_id}:\n\n{message}"
                                asyncio.run_coroutine_threadsafe(bot.send_message(user_chat_id, text), loop)
                            else: logger.error("Ответ поддержки: support-бот или цикл событий недоступны; сообщение пользователю не отправлено.")
                        except Exception as e: logger.error(f"Ответ поддержки: не удалось отправить сообщение пользователю {ticket.get('user_id')} через support-бота: {e}", exc_info=True)
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            forum_chat_id = ticket.get('forum_chat_id'); thread_id = ticket.get('message_thread_id')
                            if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                                text = f"💬 Ответ админа из панели по тикету #{ticket_id}:\n\n{message}"
                                asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=int(forum_chat_id), text=text, message_thread_id=int(thread_id)), loop)
                        except Exception as e: logger.warning(f"Ответ поддержки: не удалось отзеркалить сообщение в тему форума для тикета {ticket_id}: {e}")
                        flash('Ответ отправлен.', 'success')
                    return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
                elif action == 'close':
                    if ticket.get('status') != 'closed' and set_ticket_status(ticket_id, 'closed'):
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            forum_chat_id = ticket.get('forum_chat_id'); thread_id = ticket.get('message_thread_id')
                            if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                                asyncio.run_coroutine_threadsafe(bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)), loop)
                        except Exception as e: logger.warning(f"Закрытие тикета: не удалось закрыть тему форума для тикета {ticket_id}: {e}")
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            user_chat_id = ticket.get('user_id')
                            if bot and loop and loop.is_running() and user_chat_id:
                                text = f"✅ Ваш тикет #{ticket_id} был закрыт администратором. Вы можете создать новое обращение при необходимости."
                                asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                        except Exception as e: logger.warning(f"Закрытие тикета: не удалось уведомить пользователя {ticket.get('user_id')} о закрытии тикета #{ticket_id}: {e}")
                        flash('Тикет закрыт.', 'success')
                    else: flash('Не удалось закрыть тикет.', 'danger')
                    return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
                elif action == 'open':
                    if ticket.get('status') != 'open' and set_ticket_status(ticket_id, 'open'):
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            forum_chat_id = ticket.get('forum_chat_id'); thread_id = ticket.get('message_thread_id')
                            if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                                asyncio.run_coroutine_threadsafe(bot.reopen_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)), loop)
                        except Exception as e: logger.warning(f"Открытие тикета: не удалось переоткрыть тему форума для тикета {ticket_id}: {e}")
                        try:
                            bot = support_bot_controller.get_bot_instance()
                            loop = current_app.config.get('EVENT_LOOP')
                            user_chat_id = ticket.get('user_id')
                            if bot and loop and loop.is_running() and user_chat_id:
                                text = f"🔓 Ваш тикет #{ticket_id} снова открыт. Вы можете продолжить переписку."
                                asyncio.run_coroutine_threadsafe(bot.send_message(int(user_chat_id), text), loop)
                        except Exception as e: logger.warning(f"Открытие тикета: не удалось уведомить пользователя {ticket.get('user_id')} об открытии тикета #{ticket_id}: {e}")
                        flash('Тикет открыт.', 'success')
                    else: flash('Не удалось открыть тикет.', 'danger')
                    return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
            messages = get_ticket_messages(ticket_id)
            common_data = get_common_template_data()
            return render_template('ticket.html', ticket=ticket, messages=messages, **common_data)
        @flask_app.route('/support/<int:ticket_id>/messages.json')
        @login_required
        def support_ticket_messages_api(ticket_id):
            ticket = get_ticket(ticket_id)
            if not ticket: return jsonify({"error": "not_found"}), 404
            messages = get_ticket_messages(ticket_id) or []
            items = [{"sender": m.get('sender'), "content": m.get('content'), "created_at": m.get('created_at')} for m in messages]
            return jsonify({"ticket_id": ticket_id, "status": ticket.get('status'), "messages": items})
        @flask_app.route('/support/<int:ticket_id>/delete', methods=['POST'])
        @login_required
        def delete_support_ticket_route(ticket_id: int):
            ticket = get_ticket(ticket_id)
            if not ticket: flash('Тикет не найден.', 'danger'); return redirect(url_for('support_list_page'))
            try:
                bot = support_bot_controller.get_bot_instance()
                loop = current_app.config.get('EVENT_LOOP')
                forum_chat_id = ticket.get('forum_chat_id'); thread_id = ticket.get('message_thread_id')
                if bot and loop and loop.is_running() and forum_chat_id and thread_id:
                    try:
                        fut = asyncio.run_coroutine_threadsafe(bot.delete_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)), loop)
                        fut.result(timeout=5)
                    except Exception as e:
                        logger.warning(f"Удаление темы форума не удалось для тикета {ticket_id} (чат {forum_chat_id}, тема {thread_id}): {e}. Пытаюсь закрыть тему как фолбэк.")
                        try:
                            fut2 = asyncio.run_coroutine_threadsafe(bot.close_forum_topic(chat_id=int(forum_chat_id), message_thread_id=int(thread_id)), loop)
                            fut2.result(timeout=5)
                        except Exception as e2: logger.warning(f"Фолбэк-закрытие темы форума также не удалось для тикета {ticket_id}: {e2}")
                else: logger.error("Удаление тикета: support-бот или цикл событий недоступны, либо отсутствуют forum_chat_id/message_thread_id; тема не удалена.")
            except Exception as e: logger.warning(f"Не удалось обработать удаление темы форума для тикета {ticket_id} перед удалением: {e}")
            if delete_ticket(ticket_id): flash(f"Тикет #{ticket_id} удалён.", 'success')
            else: flash(f"Не удалось удалить тикет #{ticket_id}.", 'danger'); return redirect(url_for('support_ticket_page', ticket_id=ticket_id))
        @flask_app.route('/settings', methods=['GET', 'POST'])
        @login_required
        def settings_page():
            if request.method == 'POST':
                if 'panel_password' in request.form and request.form.get('panel_password'): update_setting('panel_password', request.form.get('panel_password'))
                checkbox_keys = ['force_subscription', 'sbp_enabled', 'trial_enabled', 'enable_referrals', 'enable_fixed_referral_bonus', 'stars_enabled', 'yoomoney_enabled', 'monitoring_enabled']
                for checkbox_key in checkbox_keys:
                    values = request.form.getlist(checkbox_key)
                    value = values[-1] if values else 'false'
                    update_setting(checkbox_key, value)
                for key in ALL_SETTINGS_KEYS:
                    if key in checkbox_keys or key == 'panel_password': continue
                    if key in request.form: update_setting(key, request.form.get(key))
                flash('Настройки сохранены.', 'success')
                next_hash = (request.form.get('next_hash') or '').strip() or '#panel'
                next_tab = (next_hash[1:] if next_hash.startswith('#') else next_hash) or 'panel'
                return redirect(url_for('settings_page', tab=next_tab))
            current_settings = get_all_settings()
            hosts = get_all_hosts()
            for host in hosts:
                host['plans'] = get_plans_for_host(host['host_name'])
                try: host['latest_speedtest'] = get_latest_speedtest(host['host_name'])
                except: host['latest_speedtest'] = None
            backups = []
            try:
                from pathlib import Path
                bdir = BACKUPS_DIR
                for p in sorted(bdir.glob('db-backup-*.zip'), key=lambda x: x.stat().st_mtime, reverse=True):
                    try:
                        st = p.stat()
                        backups.append({'name': p.name, 'mtime': datetime.fromtimestamp(st.st_mtime).strftime('%Y-%m-%d %H:%M'), 'size': st.st_size})
                    except: pass
            except: backups = []
            common_data = get_common_template_data()
            return render_template('settings.html', settings=current_settings, hosts=hosts, backups=backups, **common_data)
        @flask_app.route('/admin/db/backup', methods=['POST'])
        @login_required
        def backup_db_route():
            try:
                zip_path = create_backup_file()
                if not zip_path or not os.path.isfile(zip_path): flash('Не удалось создать бэкап БД.', 'danger'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
                return send_file(str(zip_path), as_attachment=True, download_name=os.path.basename(zip_path))
            except Exception as e: logger.error(f"Ошибка резервного копирования БД: {e}"); flash('Ошибка при создании бэкапа.', 'danger'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
        @flask_app.route('/admin/db/restore', methods=['POST'])
        @login_required
        def restore_db_route():
            try:
                existing = (request.form.get('existing_backup') or '').strip()
                ok = False
                if existing:
                    base = BACKUPS_DIR
                    candidate = (base / existing).resolve()
                    if str(candidate).startswith(str(base.resolve())) and os.path.isfile(candidate): ok = restore_from_file(candidate)
                    else: flash('Выбранный бэкап не найден.', 'danger'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
                else:
                    file = request.files.get('db_file')
                    if not file or file.filename == '': flash('Файл для восстановления не выбран.', 'warning'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
                    filename = file.filename.lower()
                    if not (filename.endswith('.zip') or filename.endswith('.db')): flash('Поддерживаются только файлы .zip или .db', 'warning'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
                    ts = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
                    dest_dir = BACKUPS_DIR
                    try: dest_dir.mkdir(parents=True, exist_ok=True)
                    except: pass
                    dest_path = dest_dir / f"uploaded-{ts}-{os.path.basename(filename)}"
                    file.save(dest_path)
                    ok = restore_from_file(dest_path)
                if ok: flash('Восстановление выполнено успешно.', 'success')
                else: flash('Восстановление не удалось. Проверьте файл и повторите.', 'danger')
                return redirect(request.referrer or url_for('settings_page', tab='panel'))
            except Exception as e: logger.error(f"Ошибка восстановления БД: {e}", exc_info=True); flash('Ошибка при восстановлении БД.', 'danger'); return redirect(request.referrer or url_for('settings_page', tab='panel'))
        @flask_app.route('/update-host-subscription', methods=['POST'])
        @login_required
        def update_host_subscription_route():
            host_name = (request.form.get('host_name') or '').strip()
            sub_url = (request.form.get('host_subscription_url') or '').strip()
            if not host_name: flash('Не указан хост для обновления ссылки подписки.', 'danger'); return redirect(url_for('settings_page', tab='hosts'))
            ok = update_host_subscription_url(host_name, sub_url or None)
            if ok: flash('Ссылка подписки для хоста обновлена.', 'success')
            else: flash('Не удалось обновить ссылку подписки для хоста (возможно, хост не найден).', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/update-host-url', methods=['POST'])
        @login_required
        def update_host_url_route():
            host_name = (request.form.get('host_name') or '').strip()
            new_url = (request.form.get('host_url') or '').strip()
            if not host_name or not new_url: flash('Укажите имя хоста и новый URL.', 'warning'); return redirect(url_for('settings_page', tab='hosts'))
            ok = update_host_url(host_name, new_url)
            flash('URL хоста обновлён.' if ok else 'Не удалось обновить URL хоста.', 'success' if ok else 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/rename-host', methods=['POST'])
        @login_required
        def rename_host_route():
            old_name = (request.form.get('old_host_name') or '').strip()
            new_name = (request.form.get('new_host_name') or '').strip()
            if not old_name or not new_name: flash('Введите старое и новое имя хоста.', 'warning'); return redirect(url_for('settings_page', tab='hosts'))
            ok = update_host_name(old_name, new_name)
            flash('Имя хоста обновлено.' if ok else 'Не удалось переименовать хост.', 'success' if ok else 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/start-support-bot', methods=['POST'])
        @login_required
        def start_support_bot_route():
            loop = current_app.config.get('EVENT_LOOP')
            if loop and loop.is_running(): support_bot_controller.set_loop(loop)
            result = support_bot_controller.start()
            flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
            return redirect(request.referrer or url_for('settings_page'))
        def _wait_for_stop(controller, timeout: float = 5.0) -> bool:
            start = time.time()
            while time.time() - start < timeout:
                status = controller.get_status() or {}
                if not status.get('is_running'): return True
                time.sleep(0.1)
            return False
        @flask_app.route('/stop-support-bot', methods=['POST'])
        @login_required
        def stop_support_bot_route():
            result = support_bot_controller.stop()
            _wait_for_stop(support_bot_controller)
            flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
            return redirect(request.referrer or url_for('settings_page'))
        @flask_app.route('/start-bot', methods=['POST'])
        @login_required
        def start_bot_route():
            result = bot_controller_instance.start()
            flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
            return redirect(request.referrer or url_for('dashboard_page'))
        @flask_app.route('/stop-bot', methods=['POST'])
        @login_required
        def stop_bot_route():
            result = bot_controller_instance.stop()
            _wait_for_stop(bot_controller_instance)
            flash(result['message'], 'success' if result['status'] == 'success' else 'danger')
            return redirect(request.referrer or url_for('dashboard_page'))
        @flask_app.route('/stop-both-bots', methods=['POST'])
        @login_required
        def stop_both_bots_route():
            main_result = bot_controller_instance.stop()
            support_result = support_bot_controller.stop()
            statuses = []; categories = []
            for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
                if res.get('status') == 'success': statuses.append(f"{name}: остановлен"); categories.append('success')
                else: statuses.append(f"{name}: ошибка — {res.get('message')}"); categories.append('danger')
            _wait_for_stop(bot_controller_instance); _wait_for_stop(support_bot_controller)
            category = 'danger' if 'danger' in categories else 'success'
            flash(' | '.join(statuses), category)
            return redirect(request.referrer or url_for('dashboard_page'))
        @flask_app.route('/start-both-bots', methods=['POST'])
        @login_required
        def start_both_bots_route():
            main_result = bot_controller_instance.start()
            loop = current_app.config.get('EVENT_LOOP')
            if loop and loop.is_running(): support_bot_controller.set_loop(loop)
            support_result = support_bot_controller.start()
            statuses = []; categories = []
            for name, res in [('Основной бот', main_result), ('Support-бот', support_result)]:
                if res.get('status') == 'success': statuses.append(f"{name}: запущен"); categories.append('success')
                else: statuses.append(f"{name}: ошибка — {res.get('message')}"); categories.append('danger')
            category = 'danger' if 'danger' in categories else 'success'
            flash(' | '.join(statuses), category)
            return redirect(request.referrer or url_for('settings_page'))
        @flask_app.route('/users/ban/<int:user_id>', methods=['POST'])
        @login_required
        def ban_user_route(user_id):
            ban_user(user_id)
            flash(f'Пользователь {user_id} был заблокирован.', 'success')
            try:
                bot = bot_controller_instance.get_bot_instance()
                if bot:
                    text = "🚫 Ваш аккаунт заблокирован администратором. Если это ошибка — напишите в поддержку."
                    support = (get_setting("support_bot_username") or get_setting("support_user") or "").strip()
                    kb = InlineKeyboardBuilder()
                    url = None
                    if support:
                        if support.startswith("@"): url = f"tg://resolve?domain={support[1:]}"
                        elif support.startswith("tg://"): url = support
                        elif support.startswith("http://") or support.startswith("https://"):
                            try: part = support.split("/")[-1].split("?")[0]; url = f"tg://resolve?domain={part}" if part else support
                            except: url = support
                        else: url = f"tg://resolve?domain={support}"
                    if url: kb.button(text="🆘 Написать в поддержку", url=url)
                    else: kb.button(text="🆘 Поддержка", callback_data="show_help")
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()), loop)
                    else: asyncio.run(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()))
            except Exception as e: logger.warning(f"Не удалось отправить уведомление о бане пользователю {user_id}: {e}")
            return redirect(url_for('users_page'))
        @flask_app.route('/users/unban/<int:user_id>', methods=['POST'])
        @login_required
        def unban_user_route(user_id):
            unban_user(user_id)
            flash(f'Пользователь {user_id} был разблокирован.', 'success')
            try:
                bot = bot_controller_instance.get_bot_instance()
                if bot:
                    kb = InlineKeyboardBuilder()
                    kb.row(get_main_menu_button())
                    text = "✅ Доступ к аккаунту восстановлен администратором."
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()), loop)
                    else: asyncio.run(bot.send_message(chat_id=user_id, text=text, reply_markup=kb.as_markup()))
            except Exception as e: logger.warning(f"Не удалось отправить уведомление о разбане пользователю {user_id}: {e}")
            return redirect(url_for('users_page'))
        @flask_app.route('/users/revoke/<int:user_id>', methods=['POST'])
        @login_required
        def revoke_keys_route(user_id):
            keys_to_revoke = get_user_keys(user_id)
            success_count = 0; total = len(keys_to_revoke)
            for key in keys_to_revoke:
                result = asyncio.run(delete_client_on_host(key['host_name'], key['key_email']))
                if result: success_count += 1
            delete_user_keys(user_id)
            try:
                bot = bot_controller_instance.get_bot_instance()
                if bot:
                    text = f"❌ Ваши VPN‑ключи были отозваны администратором.\nВсего ключей: {total}\nОтозвано: {success_count}"
                    loop = current_app.config.get('EVENT_LOOP')
                    if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(bot.send_message(chat_id=user_id, text=text), loop)
                    else: asyncio.run(bot.send_message(chat_id=user_id, text=text))
            except: pass
            message = f"Все {total} ключей для пользователя {user_id} были успешно отозваны." if success_count == total else f"Удалось отозвать {success_count} из {total} ключей для пользователя {user_id}. Проверьте логи."
            category = 'success' if success_count == total else 'warning'
            wants_json = 'application/json' in (request.headers.get('Accept') or '') or request.headers.get('X-Requested-With') == 'XMLHttpRequest'
            if wants_json: return jsonify({"ok": success_count == total, "message": message, "revoked": success_count, "total": total}), 200
            flash(message, category)
            return redirect(url_for('users_page'))
        @flask_app.route('/add-host', methods=['POST'])
        @login_required
        def add_host_route():
            create_host(name=request.form['host_name'], url=request.form['host_url'], user=request.form['host_username'], passwd=request.form['host_pass'], inbound=int(request.form['host_inbound_id']), subscription_url=(request.form.get('host_subscription_url') or '').strip() or None)
            flash(f"Хост '{request.form['host_name']}' успешно добавлен.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/delete-host/<host_name>', methods=['POST'])
        @login_required
        def delete_host_route(host_name):
            delete_host(host_name)
            flash(f"Хост '{host_name}' и все его тарифы были удалены.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/add-plan', methods=['POST'])
        @login_required
        def add_plan_route():
            create_plan(host_name=request.form['host_name'], plan_name=request.form['plan_name'], months=int(request.form['months']), price=float(request.form['price']))
            flash(f"Новый тариф для хоста '{request.form['host_name']}' добавлен.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/delete-plan/<int:plan_id>', methods=['POST'])
        @login_required
        def delete_plan_route(plan_id):
            delete_plan(plan_id)
            flash("Тариф успешно удален.", 'success')
            return redirect(url_for('settings_page', tab='hosts'))
        @flask_app.route('/update-plan/<int:plan_id>', methods=['POST'])
        @login_required
        def update_plan_route(plan_id):
            plan_name = (request.form.get('plan_name') or '').strip()
            months = request.form.get('months'); price = request.form.get('price')
            try: months_int = int(months); price_float = float(price)
            except (TypeError, ValueError): flash('Некорректные значения для месяцев или цены.', 'danger'); return redirect(url_for('settings_page', tab='hosts'))
            if not plan_name: flash('Название тарифа не может быть пустым.', 'danger'); return redirect(url_for('settings_page', tab='hosts'))
            ok = update_plan(plan_id, plan_name, months_int, price_float)
            if ok: flash('Тариф обновлён.', 'success')
            else: flash('Не удалось обновить тариф (возможно, он не найден).', 'danger')
            return redirect(url_for('settings_page', tab='hosts'))
        @csrf.exempt
        @flask_app.route('/yookassa-webhook', methods=['POST'])
        def yookassa_webhook_handler():
            try:
                event_json = request.json
                if event_json.get("event") == "payment.succeeded":
                    metadata = event_json.get("object", {}).get("metadata", {})
                    bot = bot_controller_instance.get_bot_instance()
                    payment_processor = process_successful_payment
                    if metadata and bot is not None and payment_processor is not None:
                        loop = current_app.config.get('EVENT_LOOP')
                        if loop and loop.is_running(): asyncio.run_coroutine_threadsafe(payment_processor(bot, metadata), loop)
                        else: logger.error("YooKassa вебхук: цикл событий недоступен!")
                return 'OK', 200
            except Exception as e: logger.error(f"Ошибка в обработчике вебхука YooKassa: {e}", exc_info=True); return 'Error', 500
        @csrf.exempt
        @flask_app.route('/cryptobot-webhook', methods=['POST'])
        def cryptobot_webhook_handler():
            try:
                request_data = request.json
                if request_data and request_data.get('update_type') == 'invoice_paid':
                    payload_data = request_data.get('payload', {})
                    payload_string = payload_data.get('payload')
                    if not payload_string: logger.warning("CryptoBot вебхук: Получен оплаченный invoice, но payload пустой."); return 'OK', 200
                    parts = payload_string.split(':')
                    if len(parts) < 9: logger.error(f"CryptoBot вебхук: некорректный формат payload: {payload_string}"); return 'Error', 400
                    metadata = {"user_id": parts[0], "months": parts[1], "price": parts[2], "action": parts[3], "key_id": parts[4], "host_name": parts[5], "plan_id": parts[6], "customer_email": parts[7] if parts[7] != 'None' else None, "payment_method": parts[8], "promo_code": (parts[9] if len(parts) > 9 and parts[9] else None)}
                    bot = bot_controller_instance.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    if bot and loop and loop.is_running(): asyncio.run_coroutine_threadsafe(process_successful_payment(bot, metadata), loop)
                    else: logger.error("CryptoBot вебхук: не удалось обработать платёж — бот или цикл событий не запущены.")
                return 'OK', 200
            except Exception as e: logger.error(f"Ошибка в обработчике вебхука CryptoBot: {e}", exc_info=True); return 'Error', 500
        @csrf.exempt
        @flask_app.route('/heleket-webhook', methods=['POST'])
        def heleket_webhook_handler():
            try:
                data = request.json
                logger.info(f"Получен вебхук Heleket: {data}")
                api_key = get_setting("heleket_api_key")
                if not api_key: return 'Error', 500
                sign = data.pop("sign", None)
                if not sign: return 'Error', 400
                sorted_data_str = json.dumps(data, sort_keys=True, separators=(",", ":"))
                base64_encoded = base64.b64encode(sorted_data_str.encode()).decode()
                raw_string = f"{base64_encoded}{api_key}"
                expected_sign = hashlib.md5(raw_string.encode()).hexdigest()
                if not compare_digest(expected_sign, sign): logger.warning("Heleket вебхук: недействительная подпись."); return 'Forbidden', 403
                if data.get('status') in ["paid", "paid_over"]:
                    metadata_str = data.get('description')
                    if not metadata_str: return 'Error', 400
                    metadata = json.loads(metadata_str)
                    bot = bot_controller_instance.get_bot_instance()
                    loop = current_app.config.get('EVENT_LOOP')
                    if bot and loop and loop.is_running(): asyncio.run_coroutine_threadsafe(process_successful_payment(bot, metadata), loop)
                return 'OK', 200
            except Exception as e: logger.error(f"Ошибка в обработчике вебхука Heleket: {e}", exc_info=True); return 'Error', 500
        @csrf.exempt
        @flask_app.route('/ton-webhook', methods=['POST'])
        def ton_webhook_handler():
            try:
                data = request.json
                logger.info(f"Получен вебхук TonAPI: {data}")
                if 'tx_id' in data:
                    account_id = data.get('account_id')
                    for tx in data.get('in_progress_txs', []) + data.get('txs', []):
                        in_msg = tx.get('in_msg')
                        if in_msg and in_msg.get('decoded_comment'):
                            payment_id = in_msg['decoded_comment']
                            amount_nano = int(in_msg.get('value', 0))
                            amount_ton = float(amount_nano / 1_000_000_000)
                            metadata = find_and_complete_ton_transaction(payment_id, amount_ton)
                            if metadata:
                                logger.info(f"TON Платеж успешен для payment_id: {payment_id}")
                                bot = bot_controller_instance.get_bot_instance()
                                loop = current_app.config.get('EVENT_LOOP')
                                if bot and loop and loop.is_running(): asyncio.run_coroutine_threadsafe(process_successful_payment(bot, metadata), loop)
                return 'OK', 200
            except Exception as e: logger.error(f"Ошибка в обработчике вебхука TonAPI: {e}", exc_info=True); return 'Error', 500
        def _ym_get_redirect_uri():
            try: saved = (get_setting("yoomoney_redirect_uri") or "").strip()
            except: saved = ""
            if saved: return saved
            root = request.url_root.rstrip('/')
            return f"{root}/yoomoney/callback"
        @flask_app.route('/yoomoney/connect')
        @login_required
        def yoomoney_connect_route():
            client_id = (get_setting('yoomoney_client_id') or '').strip()
            if not client_id: flash('Укажите YooMoney client_id в настройках.', 'warning'); return redirect(url_for('settings_page', tab='payments'))
            redirect_uri = _ym_get_redirect_uri()
            scope = 'operation-history operation-details account-info'
            qs = urllib.parse.urlencode({'client_id': client_id, 'response_type': 'code', 'scope': scope, 'redirect_uri': redirect_uri})
            url = f"https://yoomoney.ru/oauth/authorize?{qs}"
            return redirect(url)
        @csrf.exempt
        @flask_app.route('/yoomoney/callback')
        def yoomoney_callback_route():
            code = (request.args.get('code') or '').strip()
            if not code: flash('YooMoney: не получен code из OAuth.', 'danger'); return redirect(url_for('settings_page', tab='payments'))
            client_id = (get_setting('yoomoney_client_id') or '').strip()
            client_secret = (get_setting('yoomoney_client_secret') or '').strip()
            redirect_uri = _ym_get_redirect_uri()
            data = {'grant_type': 'authorization_code', 'code': code, 'client_id': client_id, 'redirect_uri': redirect_uri}
            if client_secret: data['client_secret'] = client_secret
            try:
                encoded = urllib.parse.urlencode(data).encode('utf-8')
                req = urllib.request.Request('https://yoomoney.ru/oauth/token', data=encoded, headers={'Content-Type': 'application/x-www-form-urlencoded'})
                with urllib.request.urlopen(req, timeout=15) as resp:
                    resp_text = resp.read().decode('utf-8', errors='ignore')
                try: payload = json.loads(resp_text)
                except: payload = {}
                token = (payload.get('access_token') or '').strip()
                if not token: flash(f"Не удалось получить access_token от YooMoney: {payload}", 'danger'); return redirect(url_for('settings_page', tab='payments'))
                update_setting('yoomoney_api_token', token)
                flash('YooMoney: токен успешно сохранён.', 'success')
            except Exception as e: logger.error(f"YooMoney OAuth callback ошибка: {e}", exc_info=True); flash(f'Ошибка при обмене кода на токен: {e}', 'danger')
            return redirect(url_for('settings_page', tab='payments'))
        @flask_app.route('/yoomoney/check', methods=['GET','POST'])
        @login_required
        def yoomoney_check_route():
            token = (get_setting('yoomoney_api_token') or '').strip()
            if not token: flash('YooMoney: токен не задан.', 'warning'); return redirect(url_for('settings_page', tab='payments'))
            try:
                req = urllib.request.Request('https://yoomoney.ru/api/account-info', headers={'Authorization': f'Bearer {token}'}, method='POST')
                with urllib.request.urlopen(req, timeout=15) as resp:
                    ai_text = resp.read().decode('utf-8', errors='ignore')
                    ai_status = resp.status
                    ai_headers = dict(resp.headers)
            except Exception as e: flash(f'YooMoney account-info: ошибка запроса: {e}', 'danger'); return redirect(url_for('settings_page', tab='payments'))
            try: ai = json.loads(ai_text)
            except: ai = {}
            if ai_status != 200:
                www = ai_headers.get('WWW-Authenticate', '')
                flash(f"YooMoney account-info HTTP {ai_status}. {www}", 'danger'); return redirect(url_for('settings_page', tab='payments'))
            account = ai.get('account') or ai.get('account_number') or '—'
            try:
                body = urllib.parse.urlencode({'records': '1'}).encode('utf-8')
                req2 = urllib.request.Request('https://yoomoney.ru/api/operation-history', data=body, headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/x-www-form-urlencoded'})
                with urllib.request.urlopen(req2, timeout=15) as resp2:
                    oh_text = resp2.read().decode('utf-8', errors='ignore')
                    oh_status = resp2.status
            except Exception as e: flash(f'YooMoney operation-history: ошибка запроса: {e}', 'warning'); oh_status = None
            if oh_status == 200: flash(f'YooMoney: токен валиден. Кошелёк: {account}', 'success')
            elif oh_status is not None: flash(f'YooMoney operation-history HTTP {oh_status}. Проверьте scope operation-history и соответствие кошелька.', 'danger')
            else: flash('YooMoney: не удалось проверить operation-history.', 'warning')
            return redirect(url_for('settings_page', tab='payments'))
        @flask_app.route('/button-constructor')
        @login_required
        def button_constructor_page():
            template_data = get_common_template_data()
            return render_template('button_constructor.html', **template_data)
        @flask_app.route('/api/button-configs', methods=['GET', 'POST'])
        @login_required
        def button_configs_api():
            if request.method == 'GET':
                menu_type = request.args.get('menu_type', 'main_menu')
                try:
                    configs = get_button_configs(menu_type)
                    return jsonify({"success": True, "data": configs})
                except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
            elif request.method == 'POST':
                try:
                    data = request.get_json()
                    button_id = create_button_config(data)
                    if button_id: return jsonify({"success": True, "id": button_id})
                    else: return jsonify({"success": False, "error": "Failed to create button config"}), 500
                except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
        @flask_app.route('/api/button-configs/<menu_type>', methods=['GET'])
        @login_required
        def button_configs_by_menu_api(menu_type):
            try:
                configs = get_button_configs(menu_type)
                return jsonify({"success": True, "data": configs})
            except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
        @flask_app.route('/api/button-configs/<int:button_id>', methods=['PUT', 'DELETE'])
        @login_required
        def button_config_api(button_id):
            if request.method == 'PUT':
                try:
                    data = request.get_json()
                    success = update_button_config(button_id, data)
                    if success: return jsonify({"success": True})
                    else: return jsonify({"success": False, "error": "Button config not found or update failed"}), 404
                except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
            elif request.method == 'DELETE':
                try:
                    success = delete_button_config(button_id)
                    if success: return jsonify({"success": True})
                    else: return jsonify({"success": False, "error": "Button config not found"}), 404
                except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
        @flask_app.route('/api/button-configs/<menu_type>/reorder', methods=['POST'])
        @login_required
        def button_configs_reorder_api(menu_type):
            try:
                data = request.get_json()
                button_orders = data.get('button_orders', [])
                success = reorder_button_configs(menu_type, button_orders)
                if success: return jsonify({"success": True})
                else: return jsonify({"success": False, "error": "Failed to reorder buttons"}), 500
            except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
        @flask_app.route('/api/button-configs/force-migration', methods=['POST'])
        @login_required
        def force_button_migration_api():
            try:
                success = force_button_migration()
                if success: return jsonify({"success": True, "message": "Миграция кнопок выполнена успешно"})
                else: return jsonify({"success": False, "error": "Миграция не удалась"}), 500
            except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
        @csrf.exempt
        @flask_app.route('/yoomoney-webhook', methods=['POST'])
        def yoomoney_webhook_handler():
            logger.info("🔔 Получен webhook от ЮMoney")
            try:
                form = request.form
                logger.info(f"YooMoney webhook data: {dict(form)}")
                if form.get('codepro') == 'true': logger.info("🧪 Игнорируем тестовый платеж (codepro=true)"); return 'OK', 200
                secret = get_setting('yoomoney_secret') or ''
                signature_str = "&".join([form.get('notification_type',''), form.get('operation_id',''), form.get('amount',''), form.get('currency',''), form.get('datetime',''), form.get('sender',''), form.get('codepro',''), secret, form.get('label','')])
                expected_signature = hashlib.sha1(signature_str.encode('utf-8')).hexdigest()
                received_signature = form.get('sha1_hash', '')
                if not compare_digest(expected_signature, received_signature): logger.warning("YooMoney webhook: неверная подпись"); return 'Forbidden', 403
                if form.get('notification_type') == 'p2p-incoming':
                    amount = float(form.get('amount', 0))
                    label = form.get('label', '')
                    logger.info(f"YooMoney payment: {amount} RUB, label: {label}")
                    try:
                        bot = bot_controller_instance.get_bot_instance()
                        if bot: pass
                    except Exception as e: logger.error(f"YooMoney webhook: ошибка уведомления бота: {e}")
                return 'OK', 200
            except Exception as e: logger.error(f"YooMoney webhook ошибка: {e}", exc_info=True); return 'Error', 500
        return flask_app
    flask_app = create_webhook_app(bot_controller)
    async def shutdown(sig: signal.Signals, loop: asyncio.AbstractEventLoop):
        logger.info(f"Получен сигнал: {sig.name}. Запускаю завершение работы...")
        if bot_controller.get_status()["is_running"]: bot_controller.stop(); await asyncio.sleep(2)
        tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
        if tasks:
            [task.cancel() for task in tasks]
            await asyncio.gather(*tasks, return_exceptions=True)
        loop.stop()
    async def start_services():
        loop = asyncio.get_running_loop()
        bot_controller.set_loop(loop)
        flask_app.config['EVENT_LOOP'] = loop
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda sig=sig: asyncio.create_task(shutdown(sig, loop)))
        flask_thread = threading.Thread(target=lambda: flask_app.run(host='0.0.0.0', port=1488, use_reloader=False, debug=False), daemon=True)
        flask_thread.start()
        logger.info("Flask-сервер запущен: http://0.0.0.0:1488")
        logger.info("Приложение запущено. Бота можно стартовать из веб-панели.")
        asyncio.create_task(periodic_subscription_check(bot_controller))
        try:
            while True: await asyncio.sleep(3600)
        except asyncio.CancelledError:
            logger.info("Главная задача отменена, выполняю корректное завершение...")
            return
    try: asyncio.run(start_services())
    except asyncio.CancelledError: logger.info("Получен сигнал остановки, сервисы остановлены.")
    finally: logger.info("Приложение завершается.")

if __name__ == "__main__":
    main()