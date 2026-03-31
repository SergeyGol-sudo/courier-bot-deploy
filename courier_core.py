#!/usr/bin/env python3
"""
courier_core.py — shared business logic for the courier promo bot system.
No messenger-specific code here. Pure Python + requests + sqlite3.

CLI usage:
    python3 courier_core.py nightly             — run nightly promo assignment (prints only)
    python3 courier_core.py check_used          — check used promos (prints only)
    python3 courier_core.py load_promos <file>  — load promos from file
    python3 courier_core.py db_stats            — print DB stats
"""

import os, json, logging, re, sys, sqlite3, csv, io, fcntl, threading
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, List, Callable
from pathlib import Path

import requests

# ━━━ CONFIG ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
BASE_DIR = Path(__file__).parent
# BOT tokens are intentionally NOT here — they live in frontend files.
ADMIN_IDS = [348836534]
DB_PATH = str(BASE_DIR / "promos.db")
DODO_CLIENT_ID = "cJADv"
DODO_CLIENT_SECRET = "2QgYshQfAUBcmYqmJYgvMlirvPnRaDiu"
DODO_TOKENS_FILE = str(BASE_DIR / "dodo_tokens.json")

MIN_HOURS = 4
MIN_ORDERS = 5
DEFAULT_PROMO_LEVEL = "70%"

MSK = timezone(timedelta(hours=3))

# ━━━ LOGGING ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("courier_core")
_fh = logging.FileHandler(str(BASE_DIR / "bot.log"), encoding="utf-8")
_fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(_fh)


# ━━━ SQLite ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


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
    conn.commit()
    # Add max_user_id column if it doesn't exist yet (idempotent migration)
    try:
        conn.execute("ALTER TABLE couriers ADD COLUMN max_user_id INTEGER")
        conn.commit()
    except sqlite3.OperationalError:
        pass  # Column already exists
    conn.close()


class DB:
    @staticmethod
    def find_courier(tg_id):
        c = get_db()
        r = c.execute("SELECT * FROM couriers WHERE telegram_id=?", (tg_id,)).fetchone()
        c.close()
        return dict(r) if r else None

    @staticmethod
    def find_courier_by_max_id(max_user_id):
        c = get_db()
        r = c.execute("SELECT * FROM couriers WHERE max_user_id=?", (max_user_id,)).fetchone()
        c.close()
        return dict(r) if r else None

    @staticmethod
    def set_max_user_id(telegram_id, max_user_id):
        c = get_db()
        c.execute("UPDATE couriers SET max_user_id=? WHERE telegram_id=?", (max_user_id, telegram_id))
        c.commit()
        c.close()

    @staticmethod
    def find_courier_by_phone(phone):
        p10 = re.sub(r"\D", "", phone)[-10:]
        c = get_db()
        for r in c.execute("SELECT * FROM couriers").fetchall():
            if re.sub(r"\D", "", r["phone"] or "")[-10:] == p10:
                c.close()
                return dict(r)
        c.close()
        return None

    @staticmethod
    def find_courier_by_inn(inn):
        c = get_db()
        r = c.execute("SELECT * FROM couriers WHERE inn=?", (inn.strip(),)).fetchone()
        c.close()
        return dict(r) if r else None

    @staticmethod
    def register(data):
        c = get_db()
        c.execute(
            "INSERT INTO couriers (telegram_id,fio,staff_id,phone,inn,unit,position,employment_type,registered_at,max_user_id) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (
                data["telegram_id"],
                data["fio"],
                data.get("staffId", ""),
                data.get("phone", ""),
                str(data.get("inn", "")),
                data.get("unit", ""),
                data.get("position", ""),
                data.get("employment_type", ""),
                datetime.now(MSK).strftime("%Y-%m-%d %H:%M"),
                data.get("max_user_id", None),
            ),
        )
        c.commit()
        c.close()

    @staticmethod
    def mark_welcome(tg_id):
        c = get_db()
        c.execute("UPDATE couriers SET welcome_issued=1 WHERE telegram_id=?", (tg_id,))
        c.commit()
        c.close()

    @staticmethod
    def get_all_couriers():
        c = get_db()
        rows = c.execute("SELECT * FROM couriers WHERE status='Активен'").fetchall()
        c.close()
        return [dict(r) for r in rows]

    @staticmethod
    def get_free_promo(level):
        c = get_db()
        r = c.execute(
            "SELECT id,code FROM promo_pool WHERE status='free' AND level=? LIMIT 1", (level,)
        ).fetchone()
        c.close()
        return dict(r) if r else None

    @staticmethod
    def assign_promo(pid, tg_id, shift_date=""):
        c = get_db()
        c.execute(
            "UPDATE promo_pool SET status='assigned',assigned_to=?,assigned_at=?,shift_date=? WHERE id=?",
            (tg_id, datetime.now(MSK).strftime("%Y-%m-%d %H:%M"), shift_date, pid),
        )
        c.commit()
        c.close()

    @staticmethod
    def get_promos(tg_id):
        c = get_db()
        rows = c.execute(
            "SELECT code,level,assigned_at,shift_date FROM promo_pool "
            "WHERE assigned_to=? AND status='assigned' ORDER BY assigned_at DESC",
            (tg_id,),
        ).fetchall()
        c.close()
        return [dict(r) for r in rows]

    @staticmethod
    def mark_used(code):
        """Mark promo as used. Returns (telegram_id, level) tuple, or (None, None)."""
        c = get_db()
        row = c.execute(
            "SELECT assigned_to, level FROM promo_pool WHERE code=? AND status='assigned'", (code,)
        ).fetchone()
        c.execute(
            "UPDATE promo_pool SET status='used',used_at=? WHERE code=?",
            (datetime.now(MSK).strftime("%Y-%m-%d %H:%M"), code),
        )
        c.commit()
        c.close()
        return (row["assigned_to"], row["level"]) if row else (None, None)

    @staticmethod
    def get_assigned_codes():
        c = get_db()
        rows = c.execute("SELECT code FROM promo_pool WHERE status='assigned'").fetchall()
        c.close()
        return [r["code"] for r in rows]

    @staticmethod
    def load_promos(codes_levels):
        c = get_db()
        added = skipped = 0
        for code, level in codes_levels:
            code = code.strip()
            if not code:
                continue
            try:
                c.execute(
                    "INSERT INTO promo_pool (code,level,status) VALUES (?,?,'free')", (code, level)
                )
                added += 1
            except sqlite3.IntegrityError:
                skipped += 1
        c.commit()
        c.close()
        return {"added": added, "skipped": skipped}

    @staticmethod
    def log_promo(d):
        c = get_db()
        c.execute(
            "INSERT INTO promo_log (ts,telegram_id,fio,staff_id,code,level,unit,type) VALUES (?,?,?,?,?,?,?,?)",
            (
                datetime.now(MSK).strftime("%Y-%m-%d %H:%M"),
                d.get("telegram_id"),
                d.get("fio"),
                d.get("staffId"),
                d.get("code"),
                d.get("level"),
                d.get("unit"),
                d.get("type"),
            ),
        )
        c.commit()
        c.close()

    @staticmethod
    def stats():
        c = get_db()
        couriers = c.execute("SELECT COUNT(*) c FROM couriers WHERE status='Активен'").fetchone()["c"]
        total = c.execute("SELECT COUNT(*) c FROM promo_pool").fetchone()["c"]
        free = {
            r["level"]: r["c"]
            for r in c.execute(
                "SELECT level,COUNT(*) c FROM promo_pool WHERE status='free' GROUP BY level"
            ).fetchall()
        }
        assigned = c.execute("SELECT COUNT(*) c FROM promo_pool WHERE status='assigned'").fetchone()["c"]
        used = c.execute("SELECT COUNT(*) c FROM promo_pool WHERE status='used'").fetchone()["c"]
        c.close()
        return {"couriers": couriers, "total": total, "free": free, "assigned": assigned, "used": used}


# ━━━ DODO IS API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
UNITS = {}

# ── Staff cache (TTL 1 hour) ──
_staff_cache: Dict = {"members": [], "ts": 0}
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
        except Exception:
            pass

    def refresh(self):
        with self._lock:
            self._load()  # re-read in case another process refreshed
            r = requests.post(
                "https://auth.dodois.io/connect/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": self.tokens["refresh_token"],
                    "client_id": DODO_CLIENT_ID,
                    "client_secret": DODO_CLIENT_SECRET,
                },
                timeout=30,
            )
            if r.status_code != 200:
                raise Exception(f"Token refresh: {r.status_code}")
            self._save(r.json())

    def _h(self):
        return {"Authorization": f"Bearer {self.tokens['access_token']}"}

    def _get(self, ep, params, retry=True):
        r = requests.get(f"{self.BASE}{ep}", headers=self._h(), params=params, timeout=30)
        if r.status_code in (401, 403) and retry:
            # Re-read tokens from file first (cron may have refreshed them)
            self._load()
            r2 = requests.get(f"{self.BASE}{ep}", headers=self._h(), params=params, timeout=30)
            if r2.status_code not in (401, 403):
                return r2
            # Still 401 — do full refresh
            try:
                self.refresh()
                self._update_units()
            except Exception as e:
                log.error(f"Token refresh failed: {e}")
                return r
            return self._get(ep, params, retry=False)
        return r

    def _update_units(self):
        global UNITS
        r = requests.get(
            "https://api.dodois.io/auth/roles/units", headers=self._h(), timeout=30
        )
        if r.status_code == 200:
            nu = {u["id"]: u["name"] for u in r.json() if u.get("unitType") == 1}
            if nu:
                UNITS.clear()
                UNITS.update(nu)

    def ensure_units(self):
        if not UNITS:
            self._load()  # re-read tokens (cron may have refreshed)
            self._update_units()
            if not UNITS:  # still empty — need full refresh
                self.refresh()
                self._update_units()

    # ── Staff cache (TTL 1 hour) ──
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
                r = self._get(
                    "/staff/members",
                    {"units": all_uids, "statuses": status, "skip": skip, "take": 100},
                    retry=(skip == 0),
                )
                if r.status_code != 200:
                    break
                members = r.json().get("members", [])
                all_members.extend(members)
                if r.json().get("isEndOfListReached", True) or len(members) < 100:
                    break
                skip += len(members)
        _staff_cache["members"] = all_members
        _staff_cache["ts"] = now
        log.info(f"Staff cache loaded: {len(all_members)} members")

    # ── Find employee by phone using cache ──
    def find_by_phone(self, phone: str) -> Optional[Dict]:
        """Find employee by phone. Uses cache (TTL 1 hour)."""
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
        s = date.replace(hour=0, minute=0, second=0)
        e = date.replace(hour=23, minute=59, second=59)
        result = []
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get(
                    "/staff/shifts",
                    {
                        "units": uid,
                        "clockInFrom": s.strftime("%Y-%m-%dT00:00:00"),
                        "clockInTo": e.strftime("%Y-%m-%dT23:59:59"),
                        "skip": skip,
                        "take": 500,
                    },
                    retry=(skip == 0),
                )
                if r.status_code != 200:
                    break
                shifts = r.json().get("shifts", [])
                result.extend(sh for sh in shifts if sh.get("staffTypeName") == "Courier")
                if len(shifts) < 500:
                    break
                skip += 500
        return result

    def get_staff_shifts(self, staff_id, days=14):
        self.ensure_units()
        end = datetime.now(MSK)
        start = end - timedelta(days=days)
        result = []
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get(
                    "/staff/shifts",
                    {
                        "units": uid,
                        "clockInFrom": start.strftime("%Y-%m-%dT00:00:00"),
                        "clockInTo": end.strftime("%Y-%m-%dT23:59:59"),
                        "skip": skip,
                        "take": 500,
                    },
                    retry=(skip == 0),
                )
                if r.status_code != 200:
                    break
                for s in r.json().get("shifts", []):
                    if s.get("staffId") == staff_id:
                        result.append(s)
                if len(r.json().get("shifts", [])) < 500:
                    break
                skip += 500
        return sorted(result, key=lambda x: x.get("clockInAtLocal", ""), reverse=True)

    def find_used_codes(self, codes, days=30):
        if not codes:
            return set()
        self.ensure_units()
        end = datetime.now(MSK)
        start = end - timedelta(days=days)
        used = set()
        cs = {c.upper() for c in codes}
        for uid in UNITS:
            skip = 0
            while True:
                r = self._get(
                    "/accounting/sales",
                    {
                        "units": uid,
                        "from": start.strftime("%Y-%m-%dT00:00:00"),
                        "to": end.strftime("%Y-%m-%dT23:59:59"),
                        "skip": skip,
                        "take": 500,
                    },
                    retry=(skip == 0),
                )
                if r.status_code != 200:
                    break
                for s in r.json().get("sales", []):
                    for p in s.get("products") or []:
                        pc = ((p.get("discount") or {}).get("promoCode") or "").upper()
                        if pc in cs:
                            used.add(pc)
                if len(r.json().get("sales", [])) < 500:
                    break
                skip += 500
        return used


# Singleton instance — frontends import this
dodo = Dodo()


# ━━━ NIGHTLY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def nightly(notify_func: Optional[Callable[[int, str], None]] = None):
    """
    Run nightly promo assignment.

    notify_func(chat_id, text) — optional callback to send notifications.
    chat_id here is the platform-specific user id (telegram_id or max_user_id
    depending on the frontend that calls this).
    """
    log.info("=== Nightly ===")
    yesterday = datetime.now(MSK) - timedelta(days=1)
    ds = yesterday.strftime("%Y-%m-%d")
    shifts = dodo.get_courier_shifts(yesterday)
    log.info(f"{ds}: {len(shifts)} shifts")
    agg = {}
    for s in shifts:
        sid = s["staffId"]
        if sid not in agg:
            agg[sid] = {"mins": 0, "orders": 0, "unit": s.get("unitName", "")}
        agg[sid]["mins"] += (s.get("dayShiftMinutes") or 0) + (s.get("nightShiftMinutes") or 0)
        agg[sid]["orders"] += s.get("deliveredOrdersCount") or 0
    couriers = {c["staff_id"]: c for c in DB.get_all_couriers() if c.get("staff_id")}
    assigned = skipped = 0
    for sid, d in agg.items():
        hrs = d["mins"] / 60
        ords = d["orders"]
        if hrs < MIN_HOURS or ords < MIN_ORDERS:
            skipped += 1
            continue
        c = couriers.get(sid)
        if not c:
            continue
        promo = DB.get_free_promo(DEFAULT_PROMO_LEVEL)
        if not promo:
            log.warning("No free promos")
            break
        DB.assign_promo(promo["id"], c["telegram_id"], ds)
        DB.log_promo({
            "telegram_id": c["telegram_id"],
            "fio": c["fio"],
            "staffId": sid,
            "code": promo["code"],
            "level": DEFAULT_PROMO_LEVEL,
            "unit": d["unit"],
            "type": f"Смена {ds}",
        })
        if notify_func:
            try:
                notify_func(
                    c["telegram_id"],
                    f"🎉 Промокод за смену {ds}!\n\n"
                    f"⏱ {hrs:.1f} ч   📦 {ords} заказов\n\n"
                    f"    🏷  {promo['code']}\n    💰  Скидка {DEFAULT_PROMO_LEVEL}\n\n"
                    f"Посмотреть все промокоды → «🏷 Мои промокоды»",
                )
            except Exception as e:
                log.error(f"Notify courier {c['telegram_id']}: {e}")
        assigned += 1
    log.info(f"Done: {assigned} assigned, {skipped} skipped")
    summary = f"📊 Ночная проверка {ds}\n👥 {len(agg)} курьеров\n✅ {assigned} промокодов\n⏭ {skipped} пропущено"
    if notify_func:
        for aid in ADMIN_IDS:
            try:
                notify_func(aid, summary)
            except Exception as e:
                log.error(f"Notify admin {aid}: {e}")
    else:
        print(summary)


# ━━━ CHECK USED ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def check_used(notify_func: Optional[Callable[[int, str], None]] = None):
    """
    Check which assigned promo codes were used in sales. Notify couriers.

    notify_func(chat_id, text) — optional callback to send notifications.
    """
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
        if tg_id and notify_func:
            try:
                notify_func(
                    tg_id,
                    f"✅ Промокод {code} использован!\n\n"
                    f"Спасибо, что пользуешься промокодами 🍕\n"
                    f"Продолжай работать — новые промокоды начисляются после каждой смены!",
                )
            except Exception as e:
                log.error(f"Notify {tg_id}: {e}")
    log.info(f"Total used: {len(used)}")
    if used and notify_func:
        summary = f"📊 Проверка промокодов: {len(used)} использовано\n" + "\n".join(
            f"  • {c}" for c in used
        )
        for aid in ADMIN_IDS:
            try:
                notify_func(aid, summary)
            except Exception as e:
                log.error(f"Notify admin {aid}: {e}")
    elif used:
        print(f"Использовано: {len(used)}")
        for c in used:
            print(f"  • {c}")


# ━━━ CLI HELPERS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def load_cli(path):
    """Load promo codes from a file (CLI command)."""
    codes = []
    with open(path, encoding="utf-8") as f:
        if path.endswith(".csv"):
            for row in csv.reader(f):
                if row and row[0].strip():
                    codes.append((
                        row[0].strip(),
                        row[1].strip() if len(row) > 1 and row[1].strip() else DEFAULT_PROMO_LEVEL,
                    ))
        else:
            for line in f:
                parts = line.strip().split(",")
                code = parts[0].strip()
                if code:
                    codes.append((
                        code,
                        parts[1].strip() if len(parts) > 1 and parts[1].strip() else DEFAULT_PROMO_LEVEL,
                    ))
    r = DB.load_promos(codes)
    print(f"Добавлено: {r['added']}, дубликатов: {r['skipped']}, в файле: {len(codes)}")


def load_promos_from_text(data: str, filename: str = "") -> Dict:
    """Parse promo codes from text content (for in-bot file upload). Returns result dict."""
    codes = []
    fname = filename.lower()
    if fname.endswith(".csv"):
        for row in csv.reader(io.StringIO(data)):
            if row and row[0].strip():
                codes.append((
                    row[0].strip(),
                    row[1].strip() if len(row) > 1 and row[1].strip() else DEFAULT_PROMO_LEVEL,
                ))
    else:
        for line in data.strip().splitlines():
            parts = line.strip().split(",")
            code = parts[0].strip()
            level = parts[1].strip() if len(parts) > 1 and parts[1].strip() else DEFAULT_PROMO_LEVEL
            if code:
                codes.append((code, level))
    if not codes:
        return {"added": 0, "skipped": 0, "total": 0, "empty": True}
    result = DB.load_promos(codes)
    result["total"] = len(codes)
    result["empty"] = False
    return result


# ━━━ MAIN (CLI) ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
def main():
    init_db()
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        if cmd == "nightly":
            nightly()
        elif cmd == "check_used":
            check_used()
        elif cmd == "load_promos":
            if len(sys.argv) > 2:
                load_cli(sys.argv[2])
            else:
                print("Usage: load_promos <file>")
        elif cmd == "db_stats":
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
    else:
        print("courier_core.py — shared business logic library.")
        print("Commands: nightly, check_used, load_promos <file>, db_stats")


if __name__ == "__main__":
    main()
