#!/usr/bin/env python3
"""
courier_max.py — MAX messenger frontend for the courier promo bot system.
Functionally equivalent to courier_promo_bot.py (Telegram), but using the MAX API.

Base URL: https://platform-api.max.ru
Auth: Authorization: <token> header

Run modes:
    python3 courier_max.py              — run MAX bot (long polling)
    python3 courier_max.py nightly      — run nightly with MAX notifications
    python3 courier_max.py check_used   — check used promos, notify via MAX
"""

import os, logging, re, sys, json, time, signal, threading
from typing import Optional, Dict, Any

import requests

# Import all shared business logic from core
from courier_core import (
    init_db, DB, dodo,
    ADMIN_IDS, MIN_HOURS, MIN_ORDERS, DEFAULT_PROMO_LEVEL, MSK,
    nightly, check_used, load_cli,
    log as core_log,
    BASE_DIR,
)

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
MAX_TOKEN = "f9LHodD0cOJbBMClbrLrQ4o2rIEuRFNPKZnJfHlEkijHMvyLUqv0Xq3wpR7xBgmwc8pxemvVaP8V3bdMGjjQ"
MAX_BASE = "https://platform-api.max.ru"

# MAX admin user IDs (MAX user IDs differ from Telegram user IDs — configure separately)
MAX_ADMIN_IDS: list = [116338333]  # Сергей Головин

# ━━━ LOGGING ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("courier_max")
_fh = logging.FileHandler(str(BASE_DIR / "max_bot.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)

# ━━━ BUTTON LABELS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BTN_PROMOS   = "🏷 Мои промокоды"
BTN_SHIFTS   = "📊 Мои смены"
BTN_RATING   = "⭐ Мой рейтинг"
BTN_HELP     = "❓ Как это работает"
BTN_ADMIN    = "🔧 Управление"
BTN_REGISTER = "📝 Зарегистрироваться"

# ━━━ STATE MACHINE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# States for conversation
STATE_IDLE      = "idle"
STATE_REG_FIO   = "reg_fio"
STATE_REG_PHONE = "reg_phone"   # waiting for contact share
STATE_REG_CONFIRM = "reg_confirm"
STATE_ADM_LOOKUP  = "adm_lookup"
STATE_ADM_UPLOAD  = "adm_upload"

# user_id -> {"state": STATE_*, "data": {}}
_user_states: Dict[int, Dict] = {}
_states_lock = threading.Lock()


def get_state(user_id: int) -> Dict:
    with _states_lock:
        if user_id not in _user_states:
            _user_states[user_id] = {"state": STATE_IDLE, "data": {}}
        return dict(_user_states[user_id])


def set_state(user_id: int, state: str, data: Optional[Dict] = None):
    with _states_lock:
        _user_states[user_id] = {"state": state, "data": data or {}}


def update_state_data(user_id: int, key: str, value: Any):
    with _states_lock:
        if user_id not in _user_states:
            _user_states[user_id] = {"state": STATE_IDLE, "data": {}}
        _user_states[user_id]["data"][key] = value


def clear_state(user_id: int):
    with _states_lock:
        _user_states[user_id] = {"state": STATE_IDLE, "data": {}}


# ━━━ MAX API CLIENT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
class MaxAPI:
    def __init__(self, token: str):
        self.token = token
        self.session = requests.Session()
        self.session.headers.update({"Authorization": token, "Content-Type": "application/json"})

    def _url(self, path: str) -> str:
        return f"{MAX_BASE}{path}"

    def send_message(self, user_id: int, text: str, attachments: Optional[list] = None) -> Optional[Dict]:
        """Send a message to a user. Returns response JSON or None on error."""
        # MAX supports max 4000 chars per message
        if len(text) > 4000:
            text = text[:3997] + "..."
        body: Dict[str, Any] = {"text": text, "format": "markdown"}
        if attachments:
            body["attachments"] = attachments
        try:
            r = self.session.post(self._url("/messages"), params={"user_id": user_id}, json=body, timeout=30)
            if r.status_code == 200:
                return r.json()
            else:
                log.error(f"send_message({user_id}): {r.status_code} {r.text[:200]}")
                return None
        except Exception as e:
            log.error(f"send_message({user_id}): {e}")
            return None

    def answer_callback(self, callback_id: str, notification: str = "", new_message: Optional[Dict] = None) -> bool:
        """Answer a callback query."""
        body: Dict[str, Any] = {}
        if notification:
            body["notification"] = notification
        if new_message:
            body["message"] = new_message
        try:
            r = self.session.post(self._url("/answers"), params={"callback_id": callback_id}, json=body, timeout=15)
            return r.status_code == 200
        except Exception as e:
            log.error(f"answer_callback({callback_id}): {e}")
            return False

    def get_updates(self, marker: Optional[int] = None, timeout: int = 5) -> Optional[Dict]:
        """Long-poll for updates. Short timeout for responsive shutdown."""
        params: Dict[str, Any] = {"timeout": timeout, "limit": 100}
        if marker is not None:
            params["marker"] = marker
        try:
            r = self.session.get(self._url("/updates"), params=params, timeout=timeout + 5)
            if r.status_code == 200:
                return r.json()
            else:
                log.error(f"get_updates: {r.status_code} {r.text[:200]}")
                return None
        except requests.exceptions.Timeout:
            return {"updates": [], "marker": marker}  # empty, not error
        except Exception as e:
            log.error(f"get_updates: {e}")
            return None

    def get_me(self) -> Optional[Dict]:
        """Get bot info."""
        try:
            r = self.session.get(self._url("/me"), timeout=15)
            return r.json() if r.status_code == 200 else None
        except Exception as e:
            log.error(f"get_me: {e}")
            return None

    def upload_file(self, file_bytes: bytes, filename: str) -> Optional[str]:
        """Upload a file and return download URL."""
        try:
            r = self.session.post(
                self._url("/uploads"),
                params={"type": "file"},
                files={"file": (filename, file_bytes)},
                headers={"Authorization": self.token},
                timeout=60,
            )
            if r.status_code == 200:
                return r.json().get("url")
            else:
                log.error(f"upload_file: {r.status_code} {r.text[:200]}")
                return None
        except Exception as e:
            log.error(f"upload_file: {e}")
            return None

    def download_url(self, url: str) -> Optional[bytes]:
        """Download content from a URL."""
        try:
            r = self.session.get(url, timeout=30)
            return r.content if r.status_code == 200 else None
        except Exception as e:
            log.error(f"download_url: {e}")
            return None


# Singleton MAX API instance
api = MaxAPI(MAX_TOKEN)


# ━━━ KEYBOARD BUILDERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def make_keyboard(rows: list) -> list:
    """
    Build MAX inline keyboard attachment.
    rows is a list of lists of button dicts.
    Returns the full attachments list.
    """
    return [{"type": "inline_keyboard", "payload": {"buttons": rows}}]


def menu_keyboard(is_admin: bool = False) -> list:
    """Main menu keyboard."""
    rows = [
        [{"type": "callback", "text": BTN_PROMOS, "payload": "menu_promos"}],
        [
            {"type": "callback", "text": BTN_SHIFTS, "payload": "menu_shifts"},
            {"type": "callback", "text": BTN_RATING, "payload": "menu_rating"},
        ],
        [{"type": "callback", "text": BTN_HELP, "payload": "menu_help"}],
    ]
    if is_admin:
        rows.append([{"type": "callback", "text": BTN_ADMIN, "payload": "menu_admin"}])
    return make_keyboard(rows)


def reg_keyboard() -> list:
    """Registration start keyboard."""
    return make_keyboard([[{"type": "callback", "text": BTN_REGISTER, "payload": "start_reg"}]])


def contact_keyboard() -> list:
    """Phone sharing keyboard (request_contact button)."""
    return make_keyboard([[{"type": "request_contact", "text": "📱 Отправить номер"}]])


def confirm_keyboard() -> list:
    """Yes/No confirmation keyboard for registration."""
    return make_keyboard([
        [{"type": "callback", "text": "✅ Да, всё верно", "payload": "reg_yes"}],
        [{"type": "callback", "text": "❌ Нет, отмена", "payload": "reg_no"}],
    ])


def admin_keyboard() -> list:
    """Admin panel keyboard."""
    return make_keyboard([
        [{"type": "callback", "text": "📊 Статистика", "payload": "adm_stats"}],
        [{"type": "callback", "text": "🔍 Найти курьера", "payload": "adm_lookup"}],
        [{"type": "callback", "text": "📥 Загрузить промокоды", "payload": "adm_upload"}],
        [{"type": "callback", "text": "🔄 Проверить использование", "payload": "adm_check"}],
    ])


# ━━━ HELPERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def is_max_admin(user_id: int) -> bool:
    return user_id in MAX_ADMIN_IDS


def find_courier_by_max_id(max_user_id: int):
    """Find courier by MAX user ID."""
    return DB.find_courier_by_max_id(max_user_id)


def send_menu(user_id: int, text: str, is_admin: bool):
    api.send_message(user_id, text, menu_keyboard(is_admin))


def send_reg(user_id: int, text: str):
    api.send_message(user_id, text, reg_keyboard())


def notify_func(chat_id: int, text: str):
    """
    Notification callback for nightly() and check_used().
    chat_id is telegram_id in core, but for MAX we need to map to max_user_id.
    We try to find the courier by telegram_id and use max_user_id if available.
    Falls back to sending to MAX_ADMIN_IDS for admin notifications.
    """
    # Try to find the courier's MAX user ID
    courier = DB.find_courier(chat_id)
    if courier and courier.get("max_user_id"):
        api.send_message(courier["max_user_id"], text)
    elif chat_id in ADMIN_IDS:
        # Admin notification — send to all MAX admins
        for mid in MAX_ADMIN_IDS:
            api.send_message(mid, text)
    # If we can't map, silently skip (the other platform's bot will handle it)


# ━━━ HANDLERS: START / REGISTRATION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def handle_start(user_id: int):
    """Handle /start command or bot_started event."""
    courier = find_courier_by_max_id(user_id)
    if is_max_admin(user_id):
        name = courier["fio"].split()[0] if courier and courier.get("fio") else "Админ"
        send_menu(user_id, f"👋 {name}!\nВыбирай 👇", True)
        return
    if courier and courier.get("status") == "Активен":
        name = courier["fio"].split()[0] if courier.get("fio") else "курьер"
        send_menu(user_id, f"👋 Привет, {name}!\n📍 {courier['unit']}\n\nВыбирай 👇", False)
        return
    send_reg(
        user_id,
        "👋 Привет!\n\n"
        "Я — бот, который начисляет промокоды курьерам Додо Пицца за хорошую работу.\n\n"
        "Отработал смену — получил скидку на пиццу 🍕\n\n"
        "Чтобы начать получать промокоды, пройди быструю регистрацию 👇",
    )


def handle_start_reg(user_id: int):
    """User clicked 'Register' button."""
    if is_max_admin(user_id):
        send_menu(user_id, "👋 Выбирай 👇", True)
        return
    courier = find_courier_by_max_id(user_id)
    if courier and courier.get("status") == "Активен":
        send_menu(user_id, "Ты уже в системе ✅", False)
        return
    set_state(user_id, STATE_REG_FIO)
    api.send_message(user_id, "📝 Давай знакомиться!\n\nНапиши свои ФИО (Фамилия Имя Отчество):")


def handle_reg_fio(user_id: int, text: str):
    """Received FIO text during registration."""
    fio = text.strip()
    parts = fio.split()
    if len(parts) < 2:
        api.send_message(
            user_id,
            "Мне нужны полные ФИО — фамилия и имя как минимум.\n"
            "Например: Иванов Иван Иванович",
        )
        return  # stay in STATE_REG_FIO
    update_state_data(user_id, "fio", fio)
    set_state(user_id, STATE_REG_PHONE, {"fio": fio})
    api.send_message(
        user_id,
        f"Приятно познакомиться, {parts[1]} 👋\n\n"
        "Теперь нажми кнопку ниже, чтобы поделиться номером телефона.\n"
        "Мне нужен тот номер, который указан в твоём профиле Додо ИС.",
        contact_keyboard(),
    )


def handle_reg_phone_text(user_id: int):
    """User typed text instead of using contact button during phone step."""
    api.send_message(
        user_id,
        "Номер нужно отправить именно кнопкой — так MAX подтверждает, что это твой номер.\n"
        "Нажми «📱 Отправить номер» 👇",
        contact_keyboard(),
    )


def handle_reg_contact(user_id: int, phone: str):
    """Received contact (phone number) during registration."""
    state = get_state(user_id)
    fio = state["data"].get("fio", "")
    if not fio:
        # Something went wrong, restart
        set_state(user_id, STATE_IDLE)
        send_reg(user_id, "Что-то пошло не так. Нажми «📝 Зарегистрироваться» ещё раз.")
        return

    pc = re.sub(r"\D", "", phone)
    if len(pc) == 11 and pc.startswith("8"):
        pc = "7" + pc[1:]
    if len(pc) == 10:
        pc = "7" + pc
    if len(pc) != 11:
        api.send_message(user_id, "Хм, номер выглядит неправильно. Попробуй ещё раз через кнопку 👇", contact_keyboard())
        return

    api.send_message(user_id, "🔍 Секунду, ищу тебя в системе...")

    try:
        cd = dodo.find_by_phone(pc)
    except Exception as e:
        log.error(f"Dodo search: {e}")
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            "Что-то пошло не так при связи с системой 😕\n"
            "Попробуй через пару минут. Если повторится — скажи управляющему.",
        )
        return

    if not cd:
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            "Не нашёл этот номер в системе Додо ИС 😔\n\n"
            "Возможные причины:\n"
            "• В твоём профиле указан другой номер\n"
            "• Тебя ещё не добавили в систему\n\n"
            "Обратись к управляющему пиццерии — он проверит и поправит.",
        )
        return

    if cd["staffType"] != "Courier":
        type_names = {
            "KitchenMember": "кухни",
            "Cashier": "кассы",
            "Operator": "оператор",
            "PersonalManager": "менеджер",
        }
        friendly = type_names.get(cd["staffType"], cd["staffType"])
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            f"Я нашёл тебя — ты сотрудник {friendly} 👍\n\n"
            "Но этот бот работает только для курьеров — "
            "здесь начисляются промокоды именно за доставки.\n\n"
            "Если ты на самом деле курьер — попроси управляющего "
            "проверить твою должность в Додо ИС.",
        )
        return

    if cd["status"] == "Dismissed":
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            "Этот профиль в системе помечен как уволенный.\n"
            "Если это ошибка — обратись к управляющему.",
        )
        return

    if not cd.get("inn"):
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            "Нашёл тебя в системе ✅\n\n"
            "Но есть проблема — в профиле не заполнен ИНН.\n"
            "Без ИНН выплаты не приходят, и промокод выдать не получится.\n\n"
            "👉 Обратись к управляющему — он заполнит ИНН за минуту.",
        )
        return

    # Check for duplicates
    dup = DB.find_courier_by_phone(pc) or DB.find_courier_by_inn(str(cd["inn"]))
    if dup:
        set_state(user_id, STATE_IDLE)
        send_reg(
            user_id,
            "Курьер с такими данными уже зарегистрирован в боте.\n\n"
            "Если это ты — просто нажми «Старт» и всё заработает.\n"
            "Если кто-то другой — обратись к управляющему.",
        )
        return

    # Store data for confirmation
    set_state(user_id, STATE_REG_CONFIRM, {"fio": fio, "phone": pc, "cd": cd})
    api.send_message(
        user_id,
        f"Отлично, нашёл тебя! ✅\n\n"
        f"👤 {fio}\n"
        f"📍 {cd['unit']}\n"
        f"💼 {cd['position']}\n"
        f"📄 ИНН заполнен ✅\n\n"
        f"Всё верно?",
        confirm_keyboard(),
    )


def handle_reg_confirm(user_id: int, answer: str, callback_id: str):
    """User confirmed or cancelled registration."""
    api.answer_callback(callback_id, "")
    state = get_state(user_id)
    if answer == "reg_no":
        clear_state(user_id)
        send_reg(user_id, "Ладно, отменил. Если передумаешь — нажми «📝 Зарегистрироваться» 👇")
        return

    fio = state["data"].get("fio", "")
    phone = state["data"].get("phone", "")
    cd = state["data"].get("cd", {})

    try:
        DB.register({
            "telegram_id": user_id,  # Use MAX user_id as primary key
            "fio": fio,
            "staffId": cd.get("staffId", ""),
            "unit": cd.get("unit", ""),
            "phone": phone,
            "inn": str(cd.get("inn", "")),
            "employment_type": cd.get("employmentType", ""),
            "position": cd.get("position", ""),
            "max_user_id": user_id,
        })
    except Exception as e:
        log.error(f"Register: {e}")
        clear_state(user_id)
        api.send_message(user_id, "Ой, произошла ошибка при регистрации 😕\nПопробуй позже.")
        return

    clear_state(user_id)

    # Issue welcome promo
    promo = DB.get_free_promo("70%")
    pmsg = ""
    if promo:
        DB.assign_promo(promo["id"], user_id, "welcome")
        DB.mark_welcome(user_id)
        DB.log_promo({
            "telegram_id": user_id,
            "fio": fio,
            "staffId": cd.get("staffId", ""),
            "code": promo["code"],
            "level": "70%",
            "unit": cd.get("unit", ""),
            "type": "Приветственный",
        })
        pmsg = (
            f"\n\n🎁 Держи приветственный промокод:\n\n"
            f"    🏷  {promo['code']}\n"
            f"    💰  Скидка 70%\n\n"
            f"Используй его при следующем заказе на киоске!"
        )
    else:
        pmsg = "\n\n⚠️ Приветственные промокоды пока закончились — скоро пополним."

    first_name = fio.split()[1] if len(fio.split()) > 1 else fio.split()[0]
    api.send_message(
        user_id,
        f"Добро пожаловать, {first_name}! 🎉\n\n"
        f"📍 {cd.get('unit', '')}\n"
        f"💼 {cd.get('position', '')}"
        f"{pmsg}\n\n"
        f"Теперь после каждой смены (от {MIN_HOURS} часов и {MIN_ORDERS} заказов) "
        f"тебе будет автоматически начисляться промокод на скидку.",
    )
    send_menu(user_id, "Вот твоё меню 👇", is_max_admin(user_id))


# ━━━ HANDLERS: MENU ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def handle_promos(user_id: int):
    courier = find_courier_by_max_id(user_id)
    if not courier:
        send_reg(user_id, "Сначала зарегистрируйся 👇")
        return
    promos = DB.get_promos(user_id)
    if not promos:
        send_menu(
            user_id,
            f"У тебя пока нет промокодов 📭\n\n"
            f"Они появляются после смены — нужно отработать минимум {MIN_HOURS} часа "
            f"и доставить {MIN_ORDERS}+ заказов.\n\n"
            f"Дерзай! 💪",
            is_max_admin(user_id),
        )
        return
    msg = f"🏷 Твои промокоды ({len(promos)}):\n"
    for p in promos:
        sd = p["shift_date"] or ""
        tag = "🎁 приветственный" if sd == "welcome" else f"📅 за смену {sd}" if sd else ""
        msg += f"\n  🏷  {p['code']}  —  скидка {p['level']}"
        if tag:
            msg += f"\n       {tag}"
        msg += f"\n       выдан {p['assigned_at']}\n"
    send_menu(user_id, msg, is_max_admin(user_id))


def handle_shifts(user_id: int):
    courier = find_courier_by_max_id(user_id)
    if not courier:
        send_reg(user_id, "Сначала зарегистрируйся 👇")
        return
    if not courier.get("staff_id"):
        send_menu(user_id, "Не могу найти твой staffId. Напиши управляющему.", is_max_admin(user_id))
        return
    api.send_message(user_id, "Загружаю твои смены ⏳")
    try:
        shifts = dodo.get_staff_shifts(courier["staff_id"], 14)
    except Exception as e:
        log.error(f"Shifts: {e}")
        send_menu(user_id, "Не удалось загрузить. Попробуй позже.", is_max_admin(user_id))
        return
    if not shifts:
        send_menu(user_id, "За последние 14 дней смен не было 📭", is_max_admin(user_id))
        return
    msg = "📊 Твои смены:\n\n"
    th = to = 0
    for s in shifts[:10]:
        mins = (s.get("dayShiftMinutes") or 0) + (s.get("nightShiftMinutes") or 0)
        hrs = mins / 60
        ords = s.get("deliveredOrdersCount") or 0
        th += hrs
        to += ords
        ok = "✅" if hrs >= MIN_HOURS and ords >= MIN_ORDERS else "❌"
        msg += f"  {s.get('clockInAtLocal', '')[:10]}   {hrs:.1f}ч   {ords} зак.  {ok}\n"
    msg += f"\n📈 Итого: {th:.1f} ч, {to} заказов\n\n✅ промокод начислен\n❌ мало часов или заказов"
    send_menu(user_id, msg, is_max_admin(user_id))


def handle_rating(user_id: int):
    """Show guest rating for the courier."""
    courier = find_courier_by_max_id(user_id)
    if not courier:
        api.send_message(user_id, "Сначала зарегистрируйся 👇", reg_keyboard())
        return
    if not courier.get("staff_id"):
        send_menu(user_id, "Не могу найти твой staffId. Напиши управляющему.", is_max_admin(user_id))
        return
    api.send_message(user_id, "Загружаю оценки гостей ⏳")
    try:
        from courier_core import get_courier_guest_rating, format_guest_rating
        shifts = dodo.get_staff_shifts(courier["staff_id"], 60)
        rating_data = get_courier_guest_rating(courier["staff_id"], shifts)
        text = "⭐ Оценки гостей\n\n" + format_guest_rating(rating_data)
    except Exception as e:
        log.error(f"Rating error: {e}")
        text = "Не удалось загрузить оценки. Попробуй позже."
    send_menu(user_id, text, is_max_admin(user_id))


def handle_help(user_id: int):
    courier = find_courier_by_max_id(user_id)
    if courier:
        send_menu(
            user_id,
            "❓ Как это работает\n\n"
            "🎁 При регистрации ты получаешь промокод на скидку 70%.\n\n"
            f"📦 После каждой смены, где ты отработал {MIN_HOURS}+ часов и доставил "
            f"{MIN_ORDERS}+ заказов, тебе автоматически начисляется промокод.\n\n"
            "🕐 Проверка проходит каждую ночь. Если за день было несколько коротких смен — "
            "часы и заказы суммируются.\n\n"
            "🏷 Промокоды одноразовые, использовать на киоске самообслуживания.\n\n"
            "📊 В меню «Мои смены» можно посмотреть, за какие смены начислены промокоды.\n\n"
            "Есть вопросы? Обратись к управляющему пиццерии.",
            is_max_admin(user_id),
        )
    else:
        send_reg(
            user_id,
            "Этот бот начисляет промокоды курьерам Додо Пицца за доставки 🍕\n\n"
            "Нажми «📝 Зарегистрироваться» чтобы начать.",
        )


# ━━━ HANDLERS: ADMIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def handle_admin_menu(user_id: int):
    if not is_max_admin(user_id):
        return
    api.send_message(user_id, "🔧 Управление:", admin_keyboard())


def handle_admin_stats(user_id: int, callback_id: str):
    api.answer_callback(callback_id, "")
    st = DB.stats()
    msg = (
        f"📊 Статистика\n\n"
        f"👥 Курьеров: {st['couriers']}\n"
        f"🏷 Выдано: {st['assigned']}\n"
        f"✅ Использовано: {st['used']}\n\n"
        f"📦 Свободных:\n"
    )
    for l, c in sorted(st["free"].items()):
        msg += f"  {l}: {c}\n"
    if not st["free"]:
        msg += "  пусто — нужно загрузить!\n"
    msg += f"\n📋 Всего в пуле: {st['total']}"
    api.send_message(user_id, msg)


def handle_admin_lookup_start(user_id: int, callback_id: str):
    api.answer_callback(callback_id, "")
    set_state(user_id, STATE_ADM_LOOKUP)
    api.send_message(user_id, "Введи телефон или staffId курьера:")


def handle_admin_lookup_query(user_id: int, query: str):
    clear_state(user_id)
    c = DB.find_courier_by_phone(query)
    if not c:
        for cr in DB.get_all_couriers():
            if cr.get("staff_id") == query:
                c = cr
                break
    if not c:
        send_menu(user_id, f"Не нашёл «{query}».", True)
        return
    promos = DB.get_promos(c["telegram_id"])
    msg = (
        f"👤 {c['fio']}\n"
        f"📍 {c['unit']}\n"
        f"📱 {c['phone']}\n"
        f"🆔 {c['staff_id']}\n"
    )
    if promos:
        msg += f"\n🏷 Промокоды ({len(promos)}):\n"
        for p in promos:
            msg += f"  {p['code']} — {p['level']} ({p['shift_date'] or ''})\n"
    else:
        msg += "\n📭 Промокодов нет."
    send_menu(user_id, msg, True)


def handle_admin_upload_start(user_id: int, callback_id: str):
    api.answer_callback(callback_id, "")
    set_state(user_id, STATE_ADM_UPLOAD)
    api.send_message(
        user_id,
        "📥 Загрузка промокодов\n\n"
        "Отправь файл .txt или .csv\n\n"
        "TXT — один код на строку (все 70%):\n"
        "ABC123\nDEF456\n\n"
        "CSV — код,уровень:\n"
        "ABC123,70%\nDEF456,50%",
    )


def handle_admin_check(user_id: int, callback_id: str):
    api.answer_callback(callback_id, "")
    api.send_message(user_id, "⏳ Проверяю использованные промокоды...")
    codes = DB.get_assigned_codes()
    if not codes:
        send_menu(user_id, "Нет выданных промокодов.", True)
        return
    used = dodo.find_used_codes(codes, 30)
    if used:
        for code in used:
            tg_id, level = DB.mark_used(code)
            if tg_id:
                courier = DB.find_courier(tg_id)
                target_id = courier.get("max_user_id") if courier else None
                if target_id:
                    try:
                        api.send_message(
                            target_id,
                            f"✅ Промокод {code} использован!\n\n"
                            f"Спасибо, что пользуешься промокодами 🍕\n"
                            f"Продолжай работать — новые промокоды начисляются после каждой смены!",
                        )
                    except Exception as e:
                        log.error(f"Notify courier MAX {target_id}: {e}")
        send_menu(
            user_id,
            f"✅ {len(used)} использованных (курьеры уведомлены):\n" + "\n".join(f"  • {c}" for c in used),
            True,
        )
    else:
        send_menu(user_id, "Ни один ещё не использован.", True)


def handle_admin_file(user_id: int, file_url: str, filename: str):
    """Handle file upload from admin."""
    clear_state(user_id)
    api.send_message(user_id, "⏳ Загружаю...")
    raw = api.download_url(file_url)
    if raw is None:
        send_menu(user_id, "Не удалось скачать файл. Попробуй ещё раз.", True)
        return
    data = raw.decode("utf-8", errors="ignore")
    from courier_core import load_promos_from_text
    result = load_promos_from_text(data, filename)
    if result.get("empty"):
        send_menu(user_id, "Файл пуст или неверный формат.", True)
        return
    send_menu(
        user_id,
        f"✅ Готово!\n\n📥 Добавлено: {result['added']}\n⏭ Дубликатов: {result['skipped']}\n📋 В файле: {result['total']}",
        True,
    )


# ━━━ INTENT RECOGNITION ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def handle_text_fallback(user_id: int, text: str):
    """Handle free text input for registered and unregistered users."""
    tl = text.lower()

    # Admin in lookup state
    state = get_state(user_id)
    if state["state"] == STATE_ADM_LOOKUP and is_max_admin(user_id):
        handle_admin_lookup_query(user_id, text.strip())
        return

    # Admin without registration — show menu
    if is_max_admin(user_id):
        send_menu(user_id, "Выбери действие 👇", True)
        return

    courier = find_courier_by_max_id(user_id)
    if not courier or courier.get("status") != "Активен":
        # Unregistered user — intent matching
        if any(w in tl for w in ["промокод", "скидк", "код", "promo"]):
            send_reg(
                user_id,
                "Чтобы получать промокоды, нужно зарегистрироваться 👇\n"
                "Это займёт меньше минуты!",
            )
        elif any(w in tl for w in ["привет", "здравств", "добр", "хай", "hello", "hi"]):
            send_reg(
                user_id,
                "Привет! 👋\nЯ — бот промокодов для курьеров Додо Пицца.\n"
                "Нажми кнопку ниже, чтобы зарегистрироваться.",
            )
        elif any(w in tl for w in ["помощь", "помоги", "help", "как", "что"]):
            send_reg(
                user_id,
                "Это бот для курьеров Додо Пицца 🍕\n"
                "После регистрации ты будешь получать промокоды на скидку за каждую полноценную смену.\n\n"
                "Нажми «📝 Зарегистрироваться» 👇",
            )
        else:
            send_reg(user_id, "Я бот промокодов для курьеров 🍕\nНажми кнопку ниже, чтобы начать 👇")
        return

    # Registered user — intent matching
    if any(w in tl for w in ["промокод", "скидк", "код", "promo", "мои"]):
        handle_promos(user_id)
    elif any(w in tl for w in ["смен", "работ", "статистик", "shift"]):
        handle_shifts(user_id)
    elif any(w in tl for w in ["помощь", "помоги", "как", "что", "help"]):
        handle_help(user_id)
    elif any(w in tl for w in ["привет", "здравств", "хай", "hello"]):
        name = courier["fio"].split()[1] if len(courier["fio"].split()) > 1 else courier["fio"].split()[0]
        send_menu(user_id, f"Привет, {name}! 👋\nВыбирай действие 👇", is_max_admin(user_id))
    elif any(w in tl for w in ["спасибо", "благодар", "thanks"]):
        send_menu(user_id, "Пожалуйста! 😊 Хорошей смены!", is_max_admin(user_id))
    else:
        send_menu(user_id, "Не совсем понял 🤔\nВыбери действие в меню 👇", is_max_admin(user_id))


# ━━━ UPDATE DISPATCHER ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def dispatch_update(update: Dict):
    """Route an update to the appropriate handler."""
    update_type = update.get("update_type")

    try:
        if update_type == "bot_started":
            user = update.get("user") or {}
            user_id = user.get("user_id")
            if user_id:
                handle_start(user_id)

        elif update_type == "bot_stopped":
            pass  # No action needed

        elif update_type == "message_created":
            message = update.get("message") or {}
            sender = message.get("sender") or {}
            user_id = sender.get("user_id")
            if not user_id:
                return

            body = message.get("body") or {}
            text = body.get("text") or ""
            attachments = body.get("attachments") or []

            state = get_state(user_id)

            # Check for contact sharing
            contact_phone = None
            for att in attachments:
                if att.get("type") == "contact":
                    payload = att.get("payload") or {}
                    contact_phone = payload.get("phone_number") or payload.get("contact_id")
                    break

            if contact_phone and state["state"] == STATE_REG_PHONE:
                handle_reg_contact(user_id, contact_phone)
                return

            # Check for file upload (admin)
            file_att = None
            for att in attachments:
                if att.get("type") in ("file", "document"):
                    file_att = att
                    break

            if file_att and is_max_admin(user_id) and state["state"] == STATE_ADM_UPLOAD:
                payload = file_att.get("payload") or {}
                file_url = payload.get("url") or payload.get("download_url") or ""
                filename = payload.get("filename") or payload.get("name") or "upload.txt"
                if file_url:
                    handle_admin_file(user_id, file_url, filename)
                    return

            # Text messages
            if text:
                # Check /start command
                if text.strip().lower() in ("/start", "start"):
                    handle_start(user_id)
                    return

                # Handle states
                if state["state"] == STATE_REG_FIO:
                    handle_reg_fio(user_id, text)
                elif state["state"] == STATE_REG_PHONE:
                    handle_reg_phone_text(user_id)
                elif state["state"] == STATE_ADM_LOOKUP and is_max_admin(user_id):
                    handle_admin_lookup_query(user_id, text.strip())
                else:
                    handle_text_fallback(user_id, text)

        elif update_type == "message_callback":
            callback = update.get("callback") or {}
            callback_id = callback.get("callback_id", "")
            payload = callback.get("payload") or ""
            user = callback.get("user") or {}
            user_id = user.get("user_id")
            if not user_id:
                return

            # Route callback payloads
            if payload == "start_reg":
                api.answer_callback(callback_id, "")
                handle_start_reg(user_id)

            elif payload == "menu_promos":
                api.answer_callback(callback_id, "")
                handle_promos(user_id)

            elif payload == "menu_shifts":
                api.answer_callback(callback_id, "")
                handle_shifts(user_id)

            elif payload == "menu_rating":
                api.answer_callback(callback_id, "")
                handle_rating(user_id)

            elif payload == "menu_help":
                api.answer_callback(callback_id, "")
                handle_help(user_id)

            elif payload == "menu_admin":
                api.answer_callback(callback_id, "")
                handle_admin_menu(user_id)

            elif payload in ("reg_yes", "reg_no"):
                state = get_state(user_id)
                if state["state"] == STATE_REG_CONFIRM:
                    handle_reg_confirm(user_id, payload, callback_id)
                else:
                    api.answer_callback(callback_id, "")

            elif payload == "adm_stats" and is_max_admin(user_id):
                handle_admin_stats(user_id, callback_id)

            elif payload == "adm_lookup" and is_max_admin(user_id):
                handle_admin_lookup_start(user_id, callback_id)

            elif payload == "adm_upload" and is_max_admin(user_id):
                handle_admin_upload_start(user_id, callback_id)

            elif payload == "adm_check" and is_max_admin(user_id):
                handle_admin_check(user_id, callback_id)

            else:
                api.answer_callback(callback_id, "")

    except Exception as e:
        log.exception(f"Error dispatching update {update_type}: {e}")


# ━━━ LONG POLLING LOOP ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_running = True


def signal_handler(sig, frame):
    global _running
    if not _running:
        # Second signal — force exit
        log.info("Force exit")
        os._exit(0)
    log.info(f"Signal {sig} received, shutting down...")
    _running = False


def run_polling():
    """Main long polling loop."""
    global _running
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Check bot connectivity
    me = api.get_me()
    if me:
        log.info(f"MAX Bot started: @{me.get('username', '?')} (id={me.get('user_id', '?')})")
    else:
        log.warning("Could not fetch bot info — check token and connectivity")

    marker: Optional[int] = None
    consecutive_errors = 0

    log.info("Starting long polling loop...")
    while _running:
        try:
            result = api.get_updates(marker=marker, timeout=5)
            if result is None:
                consecutive_errors += 1
                if consecutive_errors > 5:
                    log.warning(f"Multiple consecutive errors ({consecutive_errors}), sleeping 10s...")
                    time.sleep(10)
                continue

            consecutive_errors = 0
            updates = result.get("updates") or []
            new_marker = result.get("marker")

            if updates:
                log.info(f"Received {len(updates)} updates")
            for upd in updates:
                log.info(f"  -> {upd.get('update_type')} from user {(upd.get('message') or upd.get('callback') or upd.get('user') or {}).get('sender', {}).get('user_id') or (upd.get('user') or {}).get('user_id', '?')}")
                dispatch_update(upd)

            if new_marker is not None:
                marker = new_marker

        except Exception as e:
            log.exception(f"Polling loop error: {e}")
            consecutive_errors += 1
            time.sleep(min(consecutive_errors * 2, 30))

    log.info("MAX bot stopped.")


# ━━━ MAIN ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    init_db()

    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "nightly":
            nightly(notify_func=notify_func)
        elif cmd == "check_used":
            check_used(notify_func=notify_func)
        elif cmd == "load_promos":
            if len(sys.argv) > 2:
                load_cli(sys.argv[2])
            else:
                print("Usage: load_promos <file>")
        elif cmd == "db_stats":
            from courier_core import DB
            st = DB.stats()
            print(f"Курьеров: {st['couriers']}")
            print(f"Всего промокодов: {st['total']}")
            print(f"Выдано: {st['assigned']}")
            print(f"Использовано: {st['used']}")
            for l, c in sorted(st["free"].items()):
                print(f"  {l}: {c} свободных")
        else:
            print(f"Unknown command: {cmd}")
            sys.exit(1)
        return

    # Run the bot
    run_polling()


if __name__ == "__main__":
    main()
