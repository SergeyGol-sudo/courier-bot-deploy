#!/usr/bin/env python3
"""
Telegram-бот «Промокоды для курьеров» v3 — Додо Пицца Череповец.
Поиск через /staff/members, хранение SQLite, загрузка промокодов через Telegram/CLI.

python3 courier_promo_bot.py              — запуск бота
python3 courier_promo_bot.py nightly      — ночная проверка смен
python3 courier_promo_bot.py check_used   — проверка использования промокодов
python3 courier_promo_bot.py load_promos <file>  — загрузка промокодов из файла
python3 courier_promo_bot.py db_stats     — статистика БД
"""

import os, json, logging, re, sys, sqlite3, csv, io, fcntl, threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List
from pathlib import Path

import requests
from telegram import (
    Update, ReplyKeyboardMarkup, ReplyKeyboardRemove,
    KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup,
)
from telegram.ext import (
    Application, CommandHandler, MessageHandler, ConversationHandler,
    CallbackQueryHandler, ContextTypes, filters
)

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASE_DIR = Path(__file__).parent
BOT_TOKEN = os.getenv("BOT_TOKEN", "8724456604:AAE6VpyYfrADcSnwoBvZiXm8GCjT70S_Bh4")
ADMIN_IDS = [348836534]
DB_PATH = str(BASE_DIR / "promos.db")
DODO_CLIENT_ID = "cJADv"
DODO_CLIENT_SECRET = "2QgYshQfAUBcmYqmJYgvMlirvPnRaDiu"
DODO_TOKENS_FILE = str(BASE_DIR / "dodo_tokens.json")

MIN_HOURS = 4
MIN_ORDERS = 5
DEFAULT_PROMO_LEVEL = "70%"

MSK = timezone(timedelta(hours=3))
REG_FIO, REG_PHONE, REG_CONFIRM = range(3)

# Logging
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("courier_bot")
_fh = logging.FileHandler(str(BASE_DIR / "bot.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)
logging.getLogger("httpx").setLevel(logging.WARNING)

# ━━━ MENUS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BTN_PROMOS = "🏷 Мои промокоды"
BTN_SHIFTS = "📊 Мои смены"
BTN_RATING = "⭐ Мой рейтинг"
BTN_HELP   = "❓ Как это работает"
BTN_ADMIN  = "🔧 Управление"

def menu_kb(is_admin=False):
    rows = [[KeyboardButton(BTN_PROMOS)], [KeyboardButton(BTN_SHIFTS), KeyboardButton(BTN_RATING)], [KeyboardButton(BTN_HELP)]]
    if is_admin:
        rows.append([KeyboardButton(BTN_ADMIN)])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)

def reg_kb():
    return ReplyKeyboardMarkup([[KeyboardButton("📝 Зарегистрироваться")]], resize_keyboard=True)

# ━━━ SQLite ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_db():
    conn = sqlite3.connect(DB_PATH); conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL"); return conn

def init_db():
    conn = get_db()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS couriers (
            telegram_id INTEGER PRIMARY KEY,
            fio TEXT NOT NULL, staff_id TEXT, phone TEXT, inn TEXT,
            unit TEXT, position TEXT, employment_type TEXT,
            status TEXT DEFAULT 'Активен', welcome_issued INTEGER DEFAULT 0,
            registered_at TEXT
        );
        CREATE TABLE IF NOT EXISTS promo_pool (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT UNIQUE NOT NULL, level TEXT NOT NULL,
            status TEXT DEFAULT 'free', assigned_to INTEGER,
            assigned_at TEXT, shift_date TEXT, used_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_pp_status ON promo_pool(status, level);
        CREATE INDEX IF NOT EXISTS idx_pp_assigned ON promo_pool(assigned_to, status);
        CREATE TABLE IF NOT EXISTS promo_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT, telegram_id INTEGER, fio TEXT, staff_id TEXT,
            code TEXT, level TEXT, unit TEXT, type TEXT
        );
    """)
    conn.commit(); conn.close()

class DB:
    @staticmethod
    def find_courier(tg_id):
        c = get_db(); r = c.execute("SELECT * FROM couriers WHERE telegram_id=?", (tg_id,)).fetchone(); c.close()
        return dict(r) if r else None

    @staticmethod
    def find_courier_by_phone(phone):
        p10 = re.sub(r"\D","",phone)[-10:]
        c = get_db()
        for r in c.execute("SELECT * FROM couriers").fetchall():
            if re.sub(r"\D","",r["phone"] or "")[-10:] == p10: c.close(); return dict(r)
        c.close(); return None

    @staticmethod
    def find_courier_by_inn(inn):
        c = get_db(); r = c.execute("SELECT * FROM couriers WHERE inn=?", (inn.strip(),)).fetchone(); c.close()
        return dict(r) if r else None

    @staticmethod
    def register(data):
        c = get_db()
        c.execute("INSERT INTO couriers (telegram_id,fio,staff_id,phone,inn,unit,position,employment_type,registered_at) VALUES (?,?,?,?,?,?,?,?,?)",
            (data["telegram_id"], data["fio"], data.get("staffId",""), data.get("phone",""),
             str(data.get("inn","")), data.get("unit",""), data.get("position",""),
             data.get("employment_type",""), datetime.now(MSK).strftime("%Y-%m-%d %H:%M")))
        c.commit(); c.close()

    @staticmethod
    def mark_welcome(tg_id):
        c = get_db(); c.execute("UPDATE couriers SET welcome_issued=1 WHERE telegram_id=?", (tg_id,)); c.commit(); c.close()

    @staticmethod
    def get_all_couriers():
        c = get_db(); rows = c.execute("SELECT * FROM couriers WHERE status='Активен'").fetchall(); c.close()
        return [dict(r) for r in rows]

    @staticmethod
    def get_free_promo(level):
        c = get_db(); r = c.execute("SELECT id,code FROM promo_pool WHERE status='free' AND level=? LIMIT 1", (level,)).fetchone(); c.close()
        return dict(r) if r else None

    @staticmethod
    def assign_promo(pid, tg_id, shift_date=""):
        c = get_db()
        c.execute("UPDATE promo_pool SET status='assigned',assigned_to=?,assigned_at=?,shift_date=? WHERE id=?",
            (tg_id, datetime.now(MSK).strftime("%Y-%m-%d %H:%M"), shift_date, pid))
        c.commit(); c.close()

    @staticmethod
    def get_promos(tg_id):
        c = get_db()
        rows = c.execute("SELECT code,level,assigned_at,shift_date FROM promo_pool WHERE assigned_to=? AND status='assigned' ORDER BY assigned_at DESC", (tg_id,)).fetchall()
        c.close(); return [dict(r) for r in rows]

    @staticmethod
    def mark_used(code):
        """Mark promo as used. Returns (telegram_id, level) of the owner or (None, None)."""
        c = get_db()
        row = c.execute("SELECT assigned_to, level FROM promo_pool WHERE code=? AND status='assigned'", (code,)).fetchone()
        c.execute("UPDATE promo_pool SET status='used',used_at=? WHERE code=?", (datetime.now(MSK).strftime("%Y-%m-%d %H:%M"), code))
        c.commit(); c.close()
        return (row["assigned_to"], row["level"]) if row else (None, None)

    @staticmethod
    def get_assigned_codes():
        c = get_db(); rows = c.execute("SELECT code FROM promo_pool WHERE status='assigned'").fetchall(); c.close()
        return [r["code"] for r in rows]

    @staticmethod
    def load_promos(codes_levels):
        c = get_db(); added = skipped = 0
        for code, level in codes_levels:
            code = code.strip()
            if not code: continue
            try: c.execute("INSERT INTO promo_pool (code,level,status) VALUES (?,?,'free')", (code, level)); added += 1
            except sqlite3.IntegrityError: skipped += 1
        c.commit(); c.close(); return {"added": added, "skipped": skipped}

    @staticmethod
    def log_promo(d):
        c = get_db()
        c.execute("INSERT INTO promo_log (ts,telegram_id,fio,staff_id,code,level,unit,type) VALUES (?,?,?,?,?,?,?,?)",
            (datetime.now(MSK).strftime("%Y-%m-%d %H:%M"), d.get("telegram_id"), d.get("fio"), d.get("staffId"),
             d.get("code"), d.get("level"), d.get("unit"), d.get("type")))
        c.commit(); c.close()

    @staticmethod
    def stats():
        c = get_db()
        couriers = c.execute("SELECT COUNT(*) c FROM couriers WHERE status='Активен'").fetchone()["c"]
        total = c.execute("SELECT COUNT(*) c FROM promo_pool").fetchone()["c"]
        free = {r["level"]: r["c"] for r in c.execute("SELECT level,COUNT(*) c FROM promo_pool WHERE status='free' GROUP BY level").fetchall()}
        assigned = c.execute("SELECT COUNT(*) c FROM promo_pool WHERE status='assigned'").fetchone()["c"]
        used = c.execute("SELECT COUNT(*) c FROM promo_pool WHERE status='used'").fetchone()["c"]
        c.close()
        return {"couriers": couriers, "total": total, "free": free, "assigned": assigned, "used": used}


# ━━━ DODO IS API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNITS = {}

# ── Staff cache (TTL 1 hour) ──
_staff_cache = {"members": [], "ts": 0}
STAFF_CACHE_TTL = 3600  # seconds

class Dodo:
    BASE = "https://api.dodois.io/dodopizza/ru"

    def __init__(self):
        self.tokens = json.load(open(DODO_TOKENS_FILE))
        self._lock = threading.Lock()

    def _save(self, t):
        self.tokens = t
        # File lock to prevent concurrent writes from cron scripts
        fd = os.open(DODO_TOKENS_FILE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX)
            os.write(fd, json.dumps(t, indent=2).encode())
        finally:
            fcntl.flock(fd, fcntl.LOCK_UN)
            os.close(fd)

    def _load(self):
        """Reload tokens from file (in case cron updated them)."""
        try:
            with open(DODO_TOKENS_FILE) as f:
                self.tokens = json.load(f)
        except: pass

    def refresh(self):
        with self._lock:
            self._load()  # re-read in case another process refreshed
            r = requests.post("https://auth.dodois.io/connect/token", data={
                "grant_type": "refresh_token", "refresh_token": self.tokens["refresh_token"],
                "client_id": DODO_CLIENT_ID, "client_secret": DODO_CLIENT_SECRET}, timeout=30)
            if r.status_code != 200: raise Exception(f"Token refresh: {r.status_code}")
            self._save(r.json())

    def _h(self): return {"Authorization": f"Bearer {self.tokens['access_token']}"}

    def _get(self, ep, params, retry=True):
        r = requests.get(f"{self.BASE}{ep}", headers=self._h(), params=params, timeout=30)
        if r.status_code in (401,403) and retry:
            # Re-read tokens from file first (cron may have refreshed them)
            self._load()
            r2 = requests.get(f"{self.BASE}{ep}", headers=self._h(), params=params, timeout=30)
            if r2.status_code not in (401,403):
                return r2
            # Still 401 — do full refresh
            try:
                self.refresh(); self._update_units()
            except Exception as e:
                log.error(f"Token refresh failed: {e}")
                return r
            return self._get(ep, params, retry=False)
        return r

    def _update_units(self):
        global UNITS
        r = requests.get("https://api.dodois.io/auth/roles/units", headers=self._h(), timeout=30)
        if r.status_code == 200:
            nu = {u["id"]: u["name"] for u in r.json() if u.get("unitType") == 1}
            if nu: UNITS.clear(); UNITS.update(nu)

    def ensure_units(self):
        if not UNITS:
            self._load()  # re-read tokens (cron may have refreshed)
            self._update_units()
            if not UNITS:  # still empty — need full refresh
                self.refresh(); self._update_units()

    # ── Кэш сотрудников (TTL 1 час) ──
    def _refresh_staff_cache(self):
        """Load all staff members into cache."""
        import time as _time
        now = _time.time()
        if _staff_cache["members"] and (now - _staff_cache["ts"]) < STAFF_CACHE_TTL:
            return  # cache is fresh
        log.info("Refreshing staff cache...")
        self.ensure_units()
        all_uids = ",".join(UNITS.keys())
        all_members = []
        for status in ("Active", "Suspended"):
            skip = 0
            while True:
                r = self._get("/staff/members", {
                    "units": all_uids, "statuses": status, "skip": skip, "take": 100
                }, retry=(skip == 0))
                if r.status_code != 200: break
                members = r.json().get("members", [])
                all_members.extend(members)
                if r.json().get("isEndOfListReached", True) or len(members) < 100: break
                skip += len(members)
        _staff_cache["members"] = all_members
        _staff_cache["ts"] = now
        log.info(f"Staff cache loaded: {len(all_members)} members")

    # ── Поиск сотрудника по телефону через кэш ──
    def find_by_phone(self, phone: str) -> Optional[Dict]:
        """Ищет сотрудника по телефону. Использует кэш (TTL 1 час)."""
        phone10 = re.sub(r"\D", "", phone)[-10:]
        self._refresh_staff_cache()
        for m in _staff_cache["members"]:
            mp = re.sub(r"\D", "", m.get("phoneNumber", ""))[-10:]
            if mp == phone10:
                return {
                    "staffId": m.get("id"),
                    "firstName": m.get("firstName", ""),
                    "lastName": m.get("lastName", ""),
                    "patronymic": m.get("patronymicName", ""),
                    "phone": m.get("phoneNumber", ""),
                    "inn": m.get("taxpayerIdentificationNumber"),
                    "unit": m.get("unitName", ""),
                    "staffType": m.get("staffType", ""),
                    "position": m.get("positionName", ""),
                    "employmentType": m.get("employmentTypeName", ""),
                    "status": m.get("status", ""),
                    "hiredOn": m.get("hiredOn", ""),
                }
        return None

    def get_courier_shifts(self, date):
        self.ensure_units()
        s = date.replace(hour=0,minute=0,second=0); e = date.replace(hour=23,minute=59,second=59)
        result = []
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get("/staff/shifts", {"units": uid, "clockInFrom": s.strftime("%Y-%m-%dT00:00:00"),
                    "clockInTo": e.strftime("%Y-%m-%dT23:59:59"), "skip": skip, "take": 500}, retry=(skip==0))
                if r.status_code != 200: break
                shifts = r.json().get("shifts", [])
                result.extend(sh for sh in shifts if sh.get("staffTypeName") == "Courier")
                if len(shifts) < 500: break
                skip += 500
        return result

    def get_staff_shifts(self, staff_id, days=14):
        self.ensure_units()
        end = datetime.now(MSK); start = end - timedelta(days=days)
        result = []
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get("/staff/shifts", {"units": uid, "clockInFrom": start.strftime("%Y-%m-%dT00:00:00"),
                    "clockInTo": end.strftime("%Y-%m-%dT23:59:59"), "skip": skip, "take": 500}, retry=(skip==0))
                if r.status_code != 200: break
                for s in r.json().get("shifts", []):
                    if s.get("staffId") == staff_id: result.append(s)
                if len(r.json().get("shifts", [])) < 500: break
                skip += 500
        return sorted(result, key=lambda x: x.get("clockInAtLocal",""), reverse=True)

    def find_used_codes(self, codes, days=30):
        if not codes: return set()
        self.ensure_units()
        end = datetime.now(MSK); start = end - timedelta(days=days)
        used = set(); cs = {c.upper() for c in codes}
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get("/accounting/sales", {"units": uid, "from": start.strftime("%Y-%m-%dT00:00:00"),
                    "to": end.strftime("%Y-%m-%dT23:59:59"), "skip": skip, "take": 500}, retry=(skip==0))
                if r.status_code != 200: break
                for s in r.json().get("sales", []):
                    for p in (s.get("products") or []):
                        pc = ((p.get("discount") or {}).get("promoCode") or "").upper()
                        if pc in cs: used.add(pc)
                if len(r.json().get("sales",[])) < 500: break
                skip += 500
        return used

dodo = Dodo()

# ━━━ REGISTRATION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def cmd_start(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    # Админ — сразу меню без регистрации
    if u.id in ADMIN_IDS:
        c = DB.find_courier(u.id)
        name = c["fio"].split()[0] if c and c.get("fio") else "Админ"
        await update.message.reply_text(
            f"👋 {name}!\nВыбирай 👇",
            reply_markup=menu_kb(True))
        return ConversationHandler.END
    c = DB.find_courier(u.id)
    if c and c["status"] == "Активен":
        name = c["fio"].split()[0] if c["fio"] else "курьер"
        await update.message.reply_text(
            f"👋 Привет, {name}!\n📍 {c['unit']}\n\nВыбирай 👇",
            reply_markup=menu_kb(False))
        return ConversationHandler.END
    await update.message.reply_text(
        "👋 Привет!\n\n"
        "Я — бот, который начисляет промокоды курьерам Додо Пицца за хорошую работу.\n\n"
        "Отработал смену — получил скидку на пиццу 🍕\n\n"
        "Чтобы начать получать промокоды, пройди быструю регистрацию 👇",
        reply_markup=reg_kb())
    return ConversationHandler.END

async def start_reg(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    # Админ не должен попадать в регистрацию
    if u.id in ADMIN_IDS:
        await update.message.reply_text("👋 Выбирай 👇", reply_markup=menu_kb(True))
        return ConversationHandler.END
    c = DB.find_courier(u.id)
    if c and c["status"] == "Активен":
        await update.message.reply_text("Ты уже в системе ✅", reply_markup=menu_kb(False))
        return ConversationHandler.END
    await update.message.reply_text(
        "📝 Давай знакомиться!\n\nНапиши свои ФИО (Фамилия Имя Отчество):",
        reply_markup=ReplyKeyboardRemove())
    return REG_FIO

async def on_fio(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    fio = update.message.text.strip()
    parts = fio.split()
    if len(parts) < 2:
        await update.message.reply_text(
            "Мне нужны полные ФИО — фамилия и имя как минимум.\n"
            "Например: Иванов Иван Иванович")
        return REG_FIO
    ctx.user_data["fio"] = fio
    await update.message.reply_text(
        f"Приятно познакомиться, {parts[1]} 👋\n\n"
        "Теперь нажми кнопку ниже, чтобы поделиться номером телефона.\n"
        "Мне нужен тот номер, который указан в твоём профиле Додо ИС.",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True))
    return REG_PHONE

async def on_phone_text_reject(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    """Отклоняем текстовый ввод телефона — только кнопка contact."""
    await update.message.reply_text(
        "Номер нужно отправить именно кнопкой — так Telegram подтверждает, что это твой номер.\n"
        "Нажми «📱 Отправить номер» 👇",
        reply_markup=ReplyKeyboardMarkup(
            [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
            resize_keyboard=True, one_time_keyboard=True))
    return REG_PHONE

async def on_phone(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    # Принимаем ТОЛЬКО contact (верифицированный Telegram), не текст
    if not update.message.contact:
        await update.message.reply_text(
            "Номер нужно отправить именно кнопкой — это защита от ошибок.\n"
            "Нажми «📱 Отправить номер» 👇",
            reply_markup=ReplyKeyboardMarkup(
                [[KeyboardButton("📱 Отправить номер", request_contact=True)]],
                resize_keyboard=True, one_time_keyboard=True))
        return REG_PHONE
    phone = update.message.contact.phone_number

    pc = re.sub(r"\D","",phone)
    if len(pc)==11 and pc.startswith("8"): pc = "7"+pc[1:]
    if len(pc)==10: pc = "7"+pc
    if len(pc)!=11:
        await update.message.reply_text("Хм, номер выглядит неправильно. Попробуй ещё раз через кнопку 👇")
        return REG_PHONE

    ctx.user_data["phone"] = pc
    await update.message.reply_text("🔍 Секунду, ищу тебя в системе...", reply_markup=ReplyKeyboardRemove())

    try:
        cd = dodo.find_by_phone(pc)
    except Exception as e:
        log.error(f"Dodo search: {e}")
        await update.message.reply_text(
            "Что-то пошло не так при связи с системой 😕\n"
            "Попробуй через пару минут. Если повторится — скажи управляющему.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    if not cd:
        await update.message.reply_text(
            "Не нашёл этот номер в системе Додо ИС 😔\n\n"
            "Возможные причины:\n"
            "• В твоём профиле указан другой номер\n"
            "• Тебя ещё не добавили в систему\n\n"
            "Обратись к управляющему пиццерии — он проверит и поправит.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    if cd["staffType"] != "Courier":
        type_names = {"KitchenMember": "кухни", "Cashier": "кассы", "Operator": "оператор", "PersonalManager": "менеджер"}
        friendly = type_names.get(cd["staffType"], cd["staffType"])
        await update.message.reply_text(
            f"Я нашёл тебя — ты сотрудник {friendly} 👍\n\n"
            "Но этот бот работает только для курьеров — "
            "здесь начисляются промокоды именно за доставки.\n\n"
            "Если ты на самом деле курьер — попроси управляющего "
            "проверить твою должность в Додо ИС.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    if cd["status"] == "Dismissed":
        await update.message.reply_text(
            "Этот профиль в системе помечен как уволенный.\n"
            "Если это ошибка — обратись к управляющему.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    if not cd.get("inn"):
        await update.message.reply_text(
            "Нашёл тебя в системе ✅\n\n"
            "Но есть проблема — в профиле не заполнен ИНН.\n"
            "Без ИНН выплаты не приходят, и промокод выдать не получится.\n\n"
            "👉 Обратись к управляющему — он заполнит ИНН за минуту.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    # Дубликат?
    dup = DB.find_courier_by_phone(pc) or DB.find_courier_by_inn(str(cd["inn"]))
    if dup:
        await update.message.reply_text(
            "Курьер с такими данными уже зарегистрирован в боте.\n\n"
            "Если это ты — просто напиши /start и всё заработает.\n"
            "Если кто-то другой — обратись к управляющему.",
            reply_markup=reg_kb())
        return ConversationHandler.END

    ctx.user_data["cd"] = cd
    fio = ctx.user_data["fio"]
    await update.message.reply_text(
        f"Отлично, нашёл тебя! ✅\n\n"
        f"👤 {fio}\n"
        f"📍 {cd['unit']}\n"
        f"💼 {cd['position']}\n"
        f"📄 ИНН заполнен ✅\n\n"
        f"Всё верно?",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("✅ Да, всё верно", callback_data="reg_yes")],
            [InlineKeyboardButton("❌ Нет, отмена", callback_data="reg_no")]]))
    return REG_CONFIRM

async def on_confirm(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.data == "reg_no":
        await q.edit_message_text("Ладно, отменил. Если передумаешь — жми «📝 Зарегистрироваться» 👇")
        await q.message.reply_text("👇", reply_markup=reg_kb())
        return ConversationHandler.END

    u = q.from_user; cd = ctx.user_data.get("cd",{}); fio = ctx.user_data.get("fio",""); phone = ctx.user_data.get("phone","")
    try:
        DB.register({"telegram_id": u.id, "fio": fio, "staffId": cd.get("staffId",""),
            "unit": cd.get("unit",""), "phone": phone, "inn": str(cd.get("inn","")),
            "employment_type": cd.get("employmentType",""), "position": cd.get("position","")})
    except Exception as e:
        log.error(f"Register: {e}")
        await q.edit_message_text("Ой, произошла ошибка при регистрации 😕\nПопробуй позже.")
        return ConversationHandler.END

    promo = DB.get_free_promo("70%")
    pmsg = ""
    if promo:
        DB.assign_promo(promo["id"], u.id, "welcome")
        DB.mark_welcome(u.id)
        DB.log_promo({"telegram_id": u.id, "fio": fio, "staffId": cd.get("staffId",""),
            "code": promo["code"], "level": "70%", "unit": cd.get("unit",""), "type": "Приветственный"})
        pmsg = (f"\n\n🎁 Держи приветственный промокод:\n\n"
                f"    🏷  `{promo['code']}`\n"
                f"    💰  Скидка 70%\n\n"
                f"Используй его при следующем заказе на киоске!")
    else:
        pmsg = "\n\n⚠️ Приветственные промокоды пока закончились — скоро пополним."

    first_name = fio.split()[1] if len(fio.split()) > 1 else fio.split()[0]
    await q.edit_message_text(
        f"Добро пожаловать, {first_name}! 🎉\n\n"
        f"📍 {cd.get('unit','')}\n"
        f"💼 {cd.get('position','')}"
        f"{pmsg}\n\n"
        f"Теперь после каждой смены (от {MIN_HOURS} часов и {MIN_ORDERS} заказов) "
        f"тебе будет автоматически начисляться промокод на скидку.", parse_mode="Markdown")
    await q.message.reply_text("Вот твоё меню 👇", reply_markup=menu_kb(u.id in ADMIN_IDS))
    return ConversationHandler.END

async def cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; c = DB.find_courier(u.id)
    await update.message.reply_text("Отменил.", reply_markup=menu_kb(u.id in ADMIN_IDS) if c else reg_kb())
    return ConversationHandler.END

# ━━━ MENU HANDLERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def h_promos(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; c = DB.find_courier(u.id)
    if not c: await update.message.reply_text("Сначала зарегистрируйся 👇", reply_markup=reg_kb()); return
    promos = DB.get_promos(u.id)
    if not promos:
        await update.message.reply_text(
            "У тебя пока нет промокодов 📭\n\n"
            f"Они появляются после смены — нужно отработать минимум {MIN_HOURS} часа "
            f"и доставить {MIN_ORDERS}+ заказов.\n\n"
            "Дерзай! 💪",
            reply_markup=menu_kb(u.id in ADMIN_IDS)); return
    msg = f"🏷 Твои промокоды ({len(promos)}):\n"
    for p in promos:
        sd = p["shift_date"] or ""
        tag = "🎁 приветственный" if sd=="welcome" else f"📅 за смену {sd}" if sd else ""
        msg += f"\n  🏷  `{p['code']}`  —  скидка {p['level']}"
        if tag: msg += f"\n       {tag}"
        msg += f"\n       выдан {p['assigned_at']}\n"
    await update.message.reply_text(msg, reply_markup=menu_kb(u.id in ADMIN_IDS), parse_mode="Markdown")

async def h_shifts(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; c = DB.find_courier(u.id)
    if not c: await update.message.reply_text("Сначала зарегистрируйся 👇", reply_markup=reg_kb()); return
    if not c.get("staff_id"):
        await update.message.reply_text("Не могу найти твой staffId. Напиши управляющему.", reply_markup=menu_kb(u.id in ADMIN_IDS)); return
    await update.message.reply_text("Загружаю твои смены ⏳")
    try: shifts = dodo.get_staff_shifts(c["staff_id"], 14)
    except Exception as e:
        log.error(f"Shifts: {e}")
        await update.message.reply_text("Не удалось загрузить. Попробуй позже.", reply_markup=menu_kb(u.id in ADMIN_IDS)); return
    if not shifts:
        await update.message.reply_text("За последние 14 дней смен не было 📭", reply_markup=menu_kb(u.id in ADMIN_IDS)); return
    msg = "📊 Твои смены:\n\n"; th=to=0
    for s in shifts[:10]:
        mins = (s.get("dayShiftMinutes") or 0)+(s.get("nightShiftMinutes") or 0); hrs=mins/60
        ords = s.get("deliveredOrdersCount") or 0; th+=hrs; to+=ords
        ok = "✅" if hrs>=MIN_HOURS and ords>=MIN_ORDERS else "❌"
        msg += f"  {s.get('clockInAtLocal','')[:10]}   {hrs:.1f}ч   {ords} зак.  {ok}\n"
    msg += f"\n📈 Итого: {th:.1f} ч, {to} заказов\n\n✅ промокод начислен\n❌ мало часов или заказов"
    await update.message.reply_text(msg, reply_markup=menu_kb(u.id in ADMIN_IDS), parse_mode="Markdown")

async def h_rating(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; c = DB.find_courier(u.id)
    if not c: await update.message.reply_text("Сначала зарегистрируйся 👇", reply_markup=reg_kb()); return
    if not c.get("staff_id"):
        await update.message.reply_text("Не могу найти твой staffId. Напиши управляющему.", reply_markup=menu_kb(u.id in ADMIN_IDS)); return
    await update.message.reply_text("Загружаю оценки гостей ⏳")
    try:
        from courier_core import get_courier_guest_rating, format_guest_rating
        shifts = dodo.get_staff_shifts(c["staff_id"], 60)
        rating_data = get_courier_guest_rating(c["staff_id"], shifts)
        text = "⭐ Оценки гостей\n\n" + format_guest_rating(rating_data)
    except Exception as e:
        log.error(f"Rating: {e}")
        text = "Не удалось загрузить оценки. Попробуй позже."
    await update.message.reply_text(text, reply_markup=menu_kb(u.id in ADMIN_IDS), parse_mode="Markdown")

async def h_help(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; c = DB.find_courier(u.id)
    if c:
        await update.message.reply_text(
            "❓ Как это работает\n\n"
            "🎁 При регистрации ты получаешь промокод на скидку 70%.\n\n"
            f"📦 После каждой смены, где ты отработал {MIN_HOURS}+ часов и доставил "
            f"{MIN_ORDERS}+ заказов, тебе автоматически начисляется промокод.\n\n"
            "🕐 Проверка проходит каждую ночь. Если за день было несколько коротких смен — "
            "часы и заказы суммируются.\n\n"
            "🏷 Промокоды одноразовые, использовать на киоске самообслуживания.\n\n"
            "📊 В меню «Мои смены» можно посмотреть, за какие смены начислены промокоды.\n\n"
            "Есть вопросы? Обратись к управляющему пиццерии.",
            reply_markup=menu_kb(u.id in ADMIN_IDS))
    else:
        await update.message.reply_text(
            "Этот бот начисляет промокоды курьерам Додо Пицца за доставки 🍕\n\n"
            "Нажми «📝 Зарегистрироваться» чтобы начать.",
            reply_markup=reg_kb())

# ━━━ ADMIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def h_admin(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id not in ADMIN_IDS: return
    await update.message.reply_text("🔧 Управление:", reply_markup=InlineKeyboardMarkup([
        [InlineKeyboardButton("📊 Статистика", callback_data="adm_stats")],
        [InlineKeyboardButton("⭐ Рейтинг курьеров", callback_data="adm_rating")],
        [InlineKeyboardButton("🔍 Найти курьера", callback_data="adm_lookup")],
        [InlineKeyboardButton("📥 Загрузить промокоды", callback_data="adm_upload")],
        [InlineKeyboardButton("🔄 Проверить использование", callback_data="adm_check")]]))

async def admin_cb(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query; await q.answer()
    if q.from_user.id not in ADMIN_IDS: return
    if q.data == "adm_stats":
        st = DB.stats()
        msg = f"📊 Статистика\n\n👥 Курьеров: {st['couriers']}\n🏷 Выдано: {st['assigned']}\n✅ Использовано: {st['used']}\n\n📦 Свободных:\n"
        for l,c in sorted(st["free"].items()): msg += f"  {l}: {c}\n"
        if not st["free"]: msg += "  пусто — нужно загрузить!\n"
        msg += f"\n📋 Всего в пуле: {st['total']}"
        await q.edit_message_text(msg)
    elif q.data == "adm_rating":
        await q.edit_message_text("⏳ Собираю рейтинги...")
        try:
            all_couriers = DB.get_all_couriers()
            active = [c for c in all_couriers if c.get("staff_id") and c.get("status") == "Активен"]
            if not active:
                await q.message.reply_text("Нет активных курьеров.", reply_markup=menu_kb(True)); return
            # Get feedbacks from SQLite
            conn = get_db()
            rows = conn.execute("""
                SELECT courier_staff_id, 
                       COUNT(*) as cnt, 
                       ROUND(AVG(rating), 1) as avg_rating,
                       MIN(rating) as min_r,
                       MAX(rating) as max_r
                FROM feedbacks 
                WHERE courier_staff_id != '' AND rating IS NOT NULL
                AND order_date >= date('now', '-30 days')
                GROUP BY courier_staff_id
                ORDER BY avg_rating DESC
            """).fetchall()
            conn.close()
            staff_ratings = {r["courier_staff_id"]: dict(r) for r in rows}
            
            lines = ["⭐ Рейтинг курьеров (30 дней)\n"]
            lines.append(f"{'№':>2}  {'Курьер':<25} {'⭐':>4} {'Оц':>3} {'Пицц':>5}")
            lines.append("─" * 45)
            
            rated = []
            unrated = []
            for c in active:
                sid = c["staff_id"]
                r = staff_ratings.get(sid)
                if r and r["cnt"] > 0:
                    rated.append((c["fio"], r["avg_rating"], r["cnt"], c.get("unit", "")[:5]))
                else:
                    unrated.append(c["fio"])
            
            rated.sort(key=lambda x: (-x[1], -x[2]))
            for i, (fio, avg, cnt, unit) in enumerate(rated, 1):
                stars = "⭐" * round(avg)
                name = fio[:24]
                lines.append(f"{i:>2}. {name:<25} {avg:>3.1f} {cnt:>3}  {unit}")
            
            if unrated:
                lines.append(f"\n📭 Без оценок: {len(unrated)} курьеров")
            
            lines.append(f"\nВсего: {len(rated)} с оценками, {len(unrated)} без")
            msg = "\n".join(lines)
            
            # Telegram limit 4096 chars
            if len(msg) > 4000:
                msg = msg[:3990] + "\n..."
            await q.message.reply_text(msg, reply_markup=menu_kb(True), parse_mode=None)
        except Exception as e:
            log.error(f"Admin rating: {e}")
            await q.message.reply_text(f"Ошибка: {e}", reply_markup=menu_kb(True))
    elif q.data == "adm_lookup":
        ctx.user_data["adm_lookup"] = True
        await q.edit_message_text("Введи телефон или staffId курьера:")
    elif q.data == "adm_upload":
        ctx.user_data["adm_upload"] = True
        await q.edit_message_text(
            "📥 Загрузка промокодов\n\n"
            "Отправь файл .txt или .csv\n\n"
            "TXT — один код на строку (все 70%):\n"
            "ABC123\nDEF456\n\n"
            "CSV — код,уровень:\n"
            "ABC123,70%\nDEF456,50%")
    elif q.data == "adm_check":
        await q.edit_message_text("⏳ Проверяю...")
        codes = DB.get_assigned_codes()
        if not codes: await q.message.reply_text("Нет выданных промокодов.", reply_markup=menu_kb(True)); return
        used = dodo.find_used_codes(codes, 30)
        if used:
            for code in used:
                tg_id, level = DB.mark_used(code)
                if tg_id:
                    try:
                        await q.get_bot().send_message(
                            chat_id=tg_id,
                            text=f"✅ Промокод `{code}` использован!\n\n"
                                 f"Спасибо, что пользуешься промокодами 🍕\n"
                                 f"Продолжай работать — новые промокоды начисляются после каждой смены!",
                            parse_mode="Markdown")
                    except: pass
            await q.message.reply_text(f"✅ {len(used)} использованных (курьеры уведомлены):\n"+"\n".join(f"  • {c}" for c in used), reply_markup=menu_kb(True))
        else:
            await q.message.reply_text("Ни один ещё не использован.", reply_markup=menu_kb(True))

async def h_document(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id not in ADMIN_IDS or not ctx.user_data.get("adm_upload"):
        c = DB.find_courier(u.id)
        await update.message.reply_text("Я принимаю только кнопки меню 👇", reply_markup=menu_kb(u.id in ADMIN_IDS) if c else reg_kb()); return
    ctx.user_data["adm_upload"] = False
    doc = update.message.document
    if not doc: await update.message.reply_text("Отправь файл .txt или .csv"); return
    await update.message.reply_text("⏳ Загружаю...")
    file = await doc.get_file(); data = (await file.download_as_bytearray()).decode("utf-8", errors="ignore")
    codes = []
    fname = (doc.file_name or "").lower()
    if fname.endswith(".csv"):
        for row in csv.reader(io.StringIO(data)):
            if row and row[0].strip():
                codes.append((row[0].strip(), row[1].strip() if len(row)>1 and row[1].strip() else DEFAULT_PROMO_LEVEL))
    else:
        for line in data.strip().splitlines():
            parts = line.strip().split(",")
            code = parts[0].strip()
            level = parts[1].strip() if len(parts)>1 and parts[1].strip() else DEFAULT_PROMO_LEVEL
            if code: codes.append((code, level))
    if not codes: await update.message.reply_text("Файл пуст или неверный формат.", reply_markup=menu_kb(True)); return
    result = DB.load_promos(codes)
    await update.message.reply_text(
        f"✅ Готово!\n\n📥 Добавлено: {result['added']}\n⏭ Дубликатов: {result['skipped']}\n📋 В файле: {len(codes)}",
        reply_markup=menu_kb(True))

# ━━━ FALLBACKS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
async def h_text(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user; text = (update.message.text or "").strip().lower()

    # Админ-поиск
    if ctx.user_data.get("adm_lookup") and u.id in ADMIN_IDS:
        ctx.user_data["adm_lookup"] = False
        raw = update.message.text.strip()
        c = DB.find_courier_by_phone(raw)
        if not c:
            for cr in DB.get_all_couriers():
                if cr.get("staff_id") == raw: c = cr; break
        if not c:
            await update.message.reply_text(f"Не нашёл «{raw}».", reply_markup=menu_kb(True)); return
        promos = DB.get_promos(c["telegram_id"])
        msg = f"👤 {c['fio']}\n📍 {c['unit']}\n📱 {c['phone']}\n🆔 {c['staff_id']}\n"
        if promos:
            msg += f"\n🏷 Промокоды ({len(promos)}):\n"
            for p in promos: msg += f"  {p['code']} — {p['level']} ({p['shift_date'] or ''})\n"
        else: msg += "\n📭 Промокодов нет."
        await update.message.reply_text(msg, reply_markup=menu_kb(True)); return

    # Админ без регистрации — сразу меню
    if u.id in ADMIN_IDS:
        await update.message.reply_text("Выбери действие 👇", reply_markup=menu_kb(True))
        return

    # Незарегистрированный
    c = DB.find_courier(u.id)
    if not c or c["status"] != "Активен":
        # Распознавание намерений
        if any(w in text for w in ["промокод", "скидк", "код", "promo"]):
            await update.message.reply_text(
                "Чтобы получать промокоды, нужно зарегистрироваться 👇\n"
                "Это займёт меньше минуты!",
                reply_markup=reg_kb())
        elif any(w in text for w in ["привет", "здравств", "добр", "хай", "hello", "hi"]):
            await update.message.reply_text(
                "Привет! 👋\nЯ — бот промокодов для курьеров Додо Пицца.\n"
                "Нажми кнопку ниже, чтобы зарегистрироваться.",
                reply_markup=reg_kb())
        elif any(w in text for w in ["помощь", "помоги", "help", "как", "что"]):
            await update.message.reply_text(
                "Это бот для курьеров Додо Пицца 🍕\n"
                "После регистрации ты будешь получать промокоды на скидку за каждую полноценную смену.\n\n"
                "Нажми «📝 Зарегистрироваться» 👇",
                reply_markup=reg_kb())
        else:
            await update.message.reply_text(
                "Я бот промокодов для курьеров 🍕\nНажми кнопку ниже, чтобы начать 👇",
                reply_markup=reg_kb())
        return

    # Зарегистрированный — распознавание
    if any(w in text for w in ["промокод", "скидк", "код", "promo", "мои"]):
        await h_promos(update, ctx)
    elif any(w in text for w in ["смен", "работ", "статистик", "shift"]):
        await h_shifts(update, ctx)
    elif any(w in text for w in ["рейтинг", "оценк", "звезд", "гост", "rating"]):
        await h_rating(update, ctx)
    elif any(w in text for w in ["помощь", "помоги", "как", "что", "help"]):
        await h_help(update, ctx)
    elif any(w in text for w in ["привет", "здравств", "хай", "hello"]):
        name = c["fio"].split()[1] if len(c["fio"].split()) > 1 else c["fio"].split()[0]
        await update.message.reply_text(f"Привет, {name}! 👋\nВыбирай действие 👇", reply_markup=menu_kb(u.id in ADMIN_IDS))
    elif any(w in text for w in ["спасибо", "благодар", "thanks"]):
        await update.message.reply_text("Пожалуйста! 😊 Хорошей смены!", reply_markup=menu_kb(u.id in ADMIN_IDS))
    else:
        await update.message.reply_text(
            "Не совсем понял 🤔\nВыбери действие в меню 👇",
            reply_markup=menu_kb(u.id in ADMIN_IDS))

async def h_nontext(update: Update, ctx: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    if u.id in ADMIN_IDS and ctx.user_data.get("adm_upload") and update.message.document:
        await h_document(update, ctx); return
    c = DB.find_courier(u.id)
    await update.message.reply_text("Я понимаю только текст и кнопки 👇", reply_markup=menu_kb(u.id in ADMIN_IDS) if c else reg_kb())

# ━━━ NIGHTLY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def nightly():
    log.info("=== Nightly ===")
    yesterday = datetime.now(MSK) - timedelta(days=1); ds = yesterday.strftime("%Y-%m-%d")
    shifts = dodo.get_courier_shifts(yesterday)
    log.info(f"{ds}: {len(shifts)} shifts")
    agg = {}
    for s in shifts:
        sid = s["staffId"]
        if sid not in agg: agg[sid] = {"mins":0,"orders":0,"unit":s.get("unitName","")}
        agg[sid]["mins"] += (s.get("dayShiftMinutes") or 0)+(s.get("nightShiftMinutes") or 0)
        agg[sid]["orders"] += s.get("deliveredOrdersCount") or 0
    couriers = {c["staff_id"]: c for c in DB.get_all_couriers() if c.get("staff_id")}
    # Dedup: check who already got a promo for this shift date
    already = set()
    try:
        conn = get_db()
        rows = conn.execute("SELECT fio FROM promo_log WHERE type=?", (f"Смена {ds}",)).fetchall()
        already = {r["fio"] for r in rows}
        conn.close()
    except: pass
    assigned=skipped=duped=0
    for sid,d in agg.items():
        hrs=d["mins"]/60; ords=d["orders"]
        if hrs<MIN_HOURS or ords<MIN_ORDERS: skipped+=1; continue
        c = couriers.get(sid)
        if not c: continue
        if c["fio"] in already:
            duped+=1; continue
        promo = DB.get_free_promo(DEFAULT_PROMO_LEVEL)
        if not promo: log.warning("No free promos"); break
        DB.assign_promo(promo["id"], c["telegram_id"], ds)
        DB.log_promo({"telegram_id":c["telegram_id"],"fio":c["fio"],"staffId":sid,
            "code":promo["code"],"level":DEFAULT_PROMO_LEVEL,"unit":d["unit"],"type":f"Смена {ds}"})
        try:
            requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={
                "chat_id":c["telegram_id"],
                "text":f"🎉 Промокод за смену {ds}!\n\n⏱ {hrs:.1f} ч   📦 {ords} заказов\n\n"
                       f"    🏷  `{promo['code']}`\n    💰  Скидка {DEFAULT_PROMO_LEVEL}\n\n"
                       f"Посмотреть все промокоды → «🏷 Мои промокоды»",
                "parse_mode": "Markdown"})
        except: pass
        assigned+=1
    log.info(f"Done: {assigned} assigned, {skipped} skipped, {duped} already had")
    for aid in ADMIN_IDS:
        dup_line = f"\n♻️ {duped} уже получали" if duped else ""
        try: requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",json={"chat_id":aid,"text":f"📊 Ночная проверка {ds}\n👥 {len(agg)} курьеров\n✅ {assigned} промокодов\n⏭ {skipped} пропущено{dup_line}"})
        except: pass

def check_used():
    """Check which assigned promo codes were used in sales. Notify couriers."""
    log.info("=== Check used ===")
    codes = DB.get_assigned_codes()
    if not codes:
        log.info("No assigned codes to check")
        return
    log.info(f"Checking {len(codes)} codes...")
    used = dodo.find_used_codes(codes, 30)
    for code in used:
        tg_id, level = DB.mark_used(code)
        log.info(f"  Used: {code} (tg={tg_id}, {level})")
        if tg_id:
            try:
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={
                    "chat_id": tg_id,
                    "text": f"\u2705 \u041f\u0440\u043e\u043c\u043e\u043a\u043e\u0434 `{code}` \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d!\n\n"
                            f"\u0421\u043f\u0430\u0441\u0438\u0431\u043e, \u0447\u0442\u043e \u043f\u043e\u043b\u044c\u0437\u0443\u0435\u0448\u044c\u0441\u044f \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u0430\u043c\u0438 \ud83c\udf55\n"
                            f"\u041f\u0440\u043e\u0434\u043e\u043b\u0436\u0430\u0439 \u0440\u0430\u0431\u043e\u0442\u0430\u0442\u044c \u2014 \u043d\u043e\u0432\u044b\u0435 \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u044b \u043d\u0430\u0447\u0438\u0441\u043b\u044f\u044e\u0442\u0441\u044f \u043f\u043e\u0441\u043b\u0435 \u043a\u0430\u0436\u0434\u043e\u0439 \u0441\u043c\u0435\u043d\u044b!",
                    "parse_mode": "Markdown"})
            except Exception as e:
                log.error(f"Notify {tg_id}: {e}")
    log.info(f"Total used: {len(used)}")
    # Notify admin
    if used:
        for aid in ADMIN_IDS:
            try:
                requests.post(f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage", json={
                    "chat_id": aid,
                    "text": f"\ud83d\udcca \u041f\u0440\u043e\u0432\u0435\u0440\u043a\u0430 \u043f\u0440\u043e\u043c\u043e\u043a\u043e\u0434\u043e\u0432: {len(used)} \u0438\u0441\u043f\u043e\u043b\u044c\u0437\u043e\u0432\u0430\u043d\u043e\n" + "\n".join(f"  \u2022 {c}" for c in used)})
            except: pass

def load_cli(path):
    codes = []
    with open(path, encoding="utf-8") as f:
        if path.endswith(".csv"):
            for row in csv.reader(f):
                if row and row[0].strip(): codes.append((row[0].strip(), row[1].strip() if len(row)>1 and row[1].strip() else DEFAULT_PROMO_LEVEL))
        else:
            for line in f:
                parts = line.strip().split(","); code = parts[0].strip()
                if code: codes.append((code, parts[1].strip() if len(parts)>1 and parts[1].strip() else DEFAULT_PROMO_LEVEL))
    r = DB.load_promos(codes)
    print(f"Добавлено: {r['added']}, дубликатов: {r['skipped']}, в файле: {len(codes)}")

# ━━━ MAIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    init_db()
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "nightly": nightly()
        elif cmd == "check_used": check_used()
        elif cmd == "load_promos": load_cli(sys.argv[2]) if len(sys.argv)>2 else print("Usage: load_promos <file>")
        elif cmd == "db_stats":
            st = DB.stats()
            print(f"Курьеров: {st['couriers']}\nВсего промокодов: {st['total']}\nВыдано: {st['assigned']}\nИспользовано: {st['used']}")
            for l,c in sorted(st["free"].items()): print(f"  {l}: {c} свободных")
        return

    app = Application.builder().token(BOT_TOKEN).build()
    conv = ConversationHandler(
        entry_points=[CommandHandler("start", cmd_start), MessageHandler(filters.Regex("^📝 Зарегистрироваться$"), start_reg)],
        states={
            REG_FIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, on_fio)],
            REG_PHONE: [MessageHandler(filters.CONTACT, on_phone), MessageHandler(filters.TEXT & ~filters.COMMAND, on_phone_text_reject)],
            REG_CONFIRM: [CallbackQueryHandler(on_confirm, pattern="^reg_")]},
        fallbacks=[CommandHandler("start", cmd_start), CommandHandler("cancel", cancel)])
    app.add_handler(conv)
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_PROMOS)}$"), h_promos))
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_SHIFTS)}$"), h_shifts))
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_RATING)}$"), h_rating))
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_HELP)}$"), h_help))
    app.add_handler(MessageHandler(filters.Regex(f"^{re.escape(BTN_ADMIN)}$"), h_admin))
    app.add_handler(CallbackQueryHandler(admin_cb, pattern="^adm_"))
    app.add_handler(MessageHandler(filters.Document.ALL, h_document))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, h_text))
    app.add_handler(MessageHandler(~filters.TEXT & ~filters.COMMAND, h_nontext))
    log.info("🤖 Bot v3 starting...")
    app.run_polling(allowed_updates=Update.ALL_TYPES, drop_pending_updates=True)

if __name__ == "__main__": main()
