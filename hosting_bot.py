import asyncio
import ast
import os
import re
import shutil
import sys
import time
import zipfile
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiosqlite
import psutil
from cryptography.fernet import Fernet, InvalidToken

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode, ChatMemberStatus
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================================================
# CONFIG (ENV driven)
# =========================================================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN env")

ADMIN_IDS = {int(x.strip()) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip().isdigit()}

DATA_DIR = Path(os.getenv("DATA_DIR", "data")).resolve()
DB_PATH = DATA_DIR / "hostingbot.sqlite3"

PUBLIC_MODE = os.getenv("PUBLIC_MODE", "1").strip() == "1"
FORCE_JOIN = os.getenv("FORCE_JOIN", "1").strip() == "1"

REQUIRED_JOIN_CHECKS = [c.strip() for c in os.getenv("REQUIRED_JOIN_CHECKS", "").split(",") if c.strip()]
REQUIRED_JOIN_URLS = [c.strip() for c in os.getenv("REQUIRED_JOIN_URLS", "").split(",") if c.strip()]
REQUIRED_JOIN_TITLES = [c.strip() for c in os.getenv("REQUIRED_JOIN_TITLES", "").split(",") if c.strip()]
JOIN_FAIL_OPEN = os.getenv("JOIN_FAIL_OPEN", "0").strip() == "1"  # for Public, keep 0 recommended

MAX_UPLOAD_BYTES = 50 * 1024 * 1024  # Telegram file limit (typical)

FREE_RUNNING_LIMIT = int(os.getenv("FREE_RUNNING_LIMIT", "2"))
PREMIUM_RUNNING_LIMIT = int(os.getenv("PREMIUM_RUNNING_LIMIT", "10"))

FREE_DISK_QUOTA_MB = int(os.getenv("FREE_DISK_QUOTA_MB", "300"))
PREMIUM_DISK_QUOTA_MB = int(os.getenv("PREMIUM_DISK_QUOTA_MB", "2000"))
FREE_DISK_QUOTA_BYTES = FREE_DISK_QUOTA_MB * 1024 * 1024
PREMIUM_DISK_QUOTA_BYTES = PREMIUM_DISK_QUOTA_MB * 1024 * 1024

FREE_PROJECT_LIMIT = int(os.getenv("FREE_PROJECT_LIMIT", "20"))
PREMIUM_PROJECT_LIMIT = int(os.getenv("PREMIUM_PROJECT_LIMIT", "200"))

FREE_MAX_RAM_MB = int(os.getenv("FREE_MAX_RAM_MB", "350"))
PREMIUM_MAX_RAM_MB = int(os.getenv("PREMIUM_MAX_RAM_MB", "1200"))
FREE_MAX_RAM_BYTES = FREE_MAX_RAM_MB * 1024 * 1024
PREMIUM_MAX_RAM_BYTES = PREMIUM_MAX_RAM_MB * 1024 * 1024

FREE_DAILY_UPLOADS = int(os.getenv("FREE_DAILY_UPLOADS", "5"))
PREMIUM_DAILY_UPLOADS = int(os.getenv("PREMIUM_DAILY_UPLOADS", "50"))
FREE_DAILY_INSTALLS = int(os.getenv("FREE_DAILY_INSTALLS", "10"))
PREMIUM_DAILY_INSTALLS = int(os.getenv("PREMIUM_DAILY_INSTALLS", "100"))

CRASH_RESTART_BASE_DELAY = 5
CRASH_RESTART_MAX_DELAY = 90

WATCHDOG_INTERVAL = 6
LOG_TAIL_LINES = 900
LOG_PAGE_SIZE = 50
MEM_LOG_RING_LINES = 80

VENV_CREATE_TIMEOUT = 120
PIP_TIMEOUT = 240

ENTRYPOINT_GUESSES = ["bot.py", "main.py", "app.py", "run.py", "start.py", "__main__.py"]

RATE_LIMIT_SECONDS = 0.7
_LAST_USER_ACTION: Dict[int, float] = {}

GLOBAL_APP: Optional[Application] = None

DEFAULT_TOS = (
    "⚠️ <b>Rules / TOS</b>\n"
    "• No spam, no abuse\n"
    "• No miners, no DDoS, no harmful code\n"
    "• Admin can stop/delete/ban any project\n"
    "• Use at your own risk\n"
)
TOS_TEXT = os.getenv("TOS_TEXT", DEFAULT_TOS)

# Webhook (for Choreo)
WEBHOOK_ENABLED = os.getenv("WEBHOOK_ENABLED", "1").strip() == "1"
PORT = int(os.getenv("PORT", "8080"))
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/tg-webhook").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").rstrip("/")


# =========================================================
# Encryption (ENV vars in DB)
# =========================================================
def _fernet() -> Fernet:
    key = os.getenv("HOSTINGBOT_SECRET_KEY", "").strip().encode()
    if not key:
        raise RuntimeError("Missing HOSTINGBOT_SECRET_KEY env (Fernet key).")
    return Fernet(key)

def enc(s: str) -> bytes:
    return _fernet().encrypt(s.encode("utf-8"))

def dec(b: bytes) -> str:
    return _fernet().decrypt(b).decode("utf-8")


# =========================================================
# DB
# =========================================================
async def db_init():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")

        await db.execute("""
        CREATE TABLE IF NOT EXISTS users(
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            is_premium INTEGER DEFAULT 0,
            created_at INTEGER
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS user_state(
            user_id INTEGER PRIMARY KEY,
            tos_accepted INTEGER DEFAULT 0,
            verified INTEGER DEFAULT 0,
            verified_at INTEGER,
            last_seen INTEGER
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS bans(
            user_id INTEGER PRIMARY KEY,
            banned_at INTEGER,
            banned_by INTEGER,
            reason TEXT
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS daily_usage(
            user_id INTEGER NOT NULL,
            day TEXT NOT NULL,
            uploads INTEGER DEFAULT 0,
            installs INTEGER DEFAULT 0,
            PRIMARY KEY(user_id, day)
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS projects(
            project_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            entrypoint TEXT NOT NULL,
            created_at INTEGER,
            updated_at INTEGER,
            autostart INTEGER DEFAULT 1
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS envvars(
            project_id INTEGER NOT NULL,
            k TEXT NOT NULL,
            v BLOB NOT NULL,
            PRIMARY KEY(project_id, k)
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS runs(
            run_id INTEGER PRIMARY KEY AUTOINCREMENT,
            project_id INTEGER NOT NULL,
            pid INTEGER,
            started_at INTEGER,
            stopped_at INTEGER,
            exit_code INTEGER,
            reason TEXT
        );
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS audit(
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts INTEGER,
            actor_id INTEGER,
            action TEXT,
            target TEXT,
            details TEXT
        );
        """)
        await db.commit()

async def audit_log(actor_id: int, action: str, target: str = "", details: str = ""):
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO audit(ts, actor_id, action, target, details) VALUES(?,?,?,?,?)",
                (int(time.time()), actor_id, action[:60], target[:120], details[:500]),
            )
            await db.commit()
    except Exception:
        pass

async def db_upsert_user(user_id: int, username: Optional[str]):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users(user_id, username, created_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET username=excluded.username
        """, (user_id, username or "", now))
        await db.execute("""
            INSERT INTO user_state(user_id, last_seen)
            VALUES(?,?)
            ON CONFLICT(user_id) DO UPDATE SET last_seen=excluded.last_seen
        """, (user_id, now))
        await db.commit()

async def db_is_premium(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT is_premium FROM users WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0] == 1)

async def db_set_premium(user_id: int, value: bool):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO users(user_id, is_premium, created_at)
            VALUES(?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET is_premium=excluded.is_premium
        """, (user_id, 1 if value else 0, now))
        await db.commit()

async def db_ban(user_id: int, banned_by: int, reason: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO bans(user_id, banned_at, banned_by, reason) VALUES(?,?,?,?)",
            (user_id, int(time.time()), banned_by, reason[:300]),
        )
        await db.commit()

async def db_unban(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM bans WHERE user_id=?", (user_id,))
        await db.commit()

async def db_is_banned(user_id: int) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT reason FROM bans WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return row[0] if row else None

async def db_tos_accepted(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT tos_accepted FROM user_state WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0] == 1)

async def db_set_tos(user_id: int, value: bool):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO user_state(user_id, tos_accepted, last_seen)
            VALUES(?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET tos_accepted=excluded.tos_accepted, last_seen=excluded.last_seen
        """, (user_id, 1 if value else 0, now))
        await db.commit()

async def db_set_verified(user_id: int, value: bool):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO user_state(user_id, verified, verified_at, last_seen)
            VALUES(?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET verified=excluded.verified, verified_at=excluded.verified_at, last_seen=excluded.last_seen
        """, (user_id, 1 if value else 0, now, now))
        await db.commit()

async def db_is_verified(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT verified FROM user_state WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return bool(row and row[0] == 1)

def today_key() -> str:
    return datetime.utcnow().strftime("%Y-%m-%d")

async def usage_get(user_id: int) -> Tuple[int, int]:
    d = today_key()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT uploads, installs FROM daily_usage WHERE user_id=? AND day=?", (user_id, d))
        row = await cur.fetchone()
        if not row:
            return 0, 0
        return int(row[0] or 0), int(row[1] or 0)

async def usage_inc(user_id: int, field: str, n: int = 1):
    assert field in {"uploads", "installs"}
    d = today_key()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO daily_usage(user_id, day, uploads, installs)
            VALUES(?,?,0,0)
            ON CONFLICT(user_id, day) DO NOTHING
        """, (user_id, d))
        await db.execute(f"UPDATE daily_usage SET {field} = {field} + ? WHERE user_id=? AND day=?",
                         (n, user_id, d))
        await db.commit()

async def db_count_projects(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM projects WHERE user_id=?", (user_id,))
        row = await cur.fetchone()
        return int(row[0] or 0)

async def db_create_project(user_id: int, name: str, entrypoint: str) -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO projects(user_id, name, entrypoint, created_at, updated_at, autostart)
            VALUES(?,?,?,?,?,1)
        """, (user_id, name, entrypoint, now, now))
        await db.commit()
        return int(cur.lastrowid)

async def db_update_project_entrypoint(project_id: int, entrypoint: str):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE projects SET entrypoint=?, updated_at=? WHERE project_id=?",
                         (entrypoint, now, project_id))
        await db.commit()

async def db_rename_project(project_id: int, new_name: str):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE projects SET name=?, updated_at=? WHERE project_id=?",
                         (new_name, now, project_id))
        await db.commit()

async def db_delete_project(project_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM envvars WHERE project_id=?", (project_id,))
        await db.execute("DELETE FROM runs WHERE project_id=?", (project_id,))
        await db.execute("DELETE FROM projects WHERE project_id=?", (project_id,))
        await db.commit()

async def db_set_autostart(project_id: int, value: bool):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE projects SET autostart=?, updated_at=? WHERE project_id=?",
                         (1 if value else 0, now, project_id))
        await db.commit()

async def db_get_project(project_id: int) -> Optional[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT project_id, user_id, name, entrypoint, autostart, created_at, updated_at
            FROM projects WHERE project_id=?
        """, (project_id,))
        row = await cur.fetchone()
        if not row:
            return None
        return {
            "project_id": int(row[0]),
            "user_id": int(row[1]),
            "name": row[2],
            "entrypoint": row[3],
            "autostart": bool(row[4]),
            "created_at": int(row[5] or 0),
            "updated_at": int(row[6] or 0),
        }

async def db_list_projects(user_id: int) -> List[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT project_id, name, entrypoint, autostart, updated_at
            FROM projects WHERE user_id=?
            ORDER BY updated_at DESC
        """, (user_id,))
        rows = await cur.fetchall()
        return [
            {"project_id": int(r[0]), "name": r[1], "entrypoint": r[2], "autostart": bool(r[3]), "updated_at": int(r[4] or 0)}
            for r in rows
        ]

async def db_list_autostart_projects() -> List[int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT project_id FROM projects WHERE autostart=1")
        rows = await cur.fetchall()
        return [int(r[0]) for r in rows]

async def db_env_list(project_id: int) -> List[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT k FROM envvars WHERE project_id=? ORDER BY k", (project_id,))
        rows = await cur.fetchall()
        return [r[0] for r in rows]

async def db_env_get_all(project_id: int) -> Dict[str, str]:
    out: Dict[str, str] = {}
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT k, v FROM envvars WHERE project_id=?", (project_id,))
        rows = await cur.fetchall()
    for k, v in rows:
        try:
            out[k] = dec(v)
        except InvalidToken:
            out[k] = ""
    return out

async def db_env_set(project_id: int, k: str, v_plain: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO envvars(project_id, k, v)
            VALUES(?,?,?)
            ON CONFLICT(project_id, k) DO UPDATE SET v=excluded.v
        """, (project_id, k, enc(v_plain)))
        await db.commit()

async def db_env_del(project_id: int, k: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM envvars WHERE project_id=? AND k=?", (project_id, k))
        await db.commit()

async def db_run_start(project_id: int, pid: int) -> int:
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO runs(project_id, pid, started_at) VALUES(?,?,?)",
                               (project_id, pid, now))
        await db.commit()
        return int(cur.lastrowid)

async def db_run_stop(run_id: int, exit_code: int, reason: str):
    now = int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE runs SET stopped_at=?, exit_code=?, reason=? WHERE run_id=?
        """, (now, exit_code, reason[:500], run_id))
        await db.commit()


# =========================================================
# Utils / Paths
# =========================================================
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

def now_ts() -> int:
    return int(time.time())

def fmt_dt(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")

def human_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024:
            return f"{x:.1f}{u}"
        x /= 1024
    return f"{x:.1f}PB"

def escape_html(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def header(title: str) -> str:
    return f"<b>⚡ {title}</b>\n"

def safe_project_name(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = re.sub(r"[^a-zA-Z0-9 _\-\.]", "", name)
    return name[:32] if name else "MyProject"

def safe_env_key(k: str) -> Optional[str]:
    k = k.strip()
    if not re.fullmatch(r"[A-Z_][A-Z0-9_]{0,50}", k):
        return None
    return k

def safe_pkg_spec(spec: str) -> Optional[str]:
    spec = spec.strip()
    if len(spec) > 80:
        return None
    if not re.fullmatch(r"[a-zA-Z0-9_\-\.]+(\[[a-zA-Z0-9_,\-]+\])?([<>=!~]{1,2}[a-zA-Z0-9_\-\.]+)?", spec):
        return None
    return spec

def kbd(rows: List[List[Tuple[str, str]]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(t, callback_data=d) for t, d in row] for row in rows])

def proj_dir(user_id: int, project_id: int) -> Path:
    return DATA_DIR / "projects" / str(user_id) / str(project_id)

def proj_src_dir(user_id: int, project_id: int) -> Path:
    return proj_dir(user_id, project_id) / "src"

def proj_venv_dir(user_id: int, project_id: int) -> Path:
    return proj_dir(user_id, project_id) / "venv"

def proj_logs_dir(user_id: int, project_id: int) -> Path:
    return proj_dir(user_id, project_id) / "logs"

def proj_log_file(user_id: int, project_id: int) -> Path:
    return proj_logs_dir(user_id, project_id) / "run.log"

def venv_python(venv: Path) -> Path:
    return venv / ("Scripts/python.exe" if os.name == "nt" else "bin/python")

def venv_pip(venv: Path) -> Path:
    return venv / ("Scripts/pip.exe" if os.name == "nt" else "bin/pip")

def tmp_dir_for(user_id: int) -> Path:
    p = DATA_DIR / "tmp" / f"{user_id}_{int(time.time())}"
    p.mkdir(parents=True, exist_ok=True)
    return p

def dir_size(path: Path) -> int:
    if not path.exists():
        return 0
    total = 0
    for root, _, files in os.walk(path):
        for fn in files:
            try:
                total += (Path(root) / fn).stat().st_size
            except Exception:
                pass
    return total

def user_root_dir(user_id: int) -> Path:
    return DATA_DIR / "projects" / str(user_id)

def user_used_bytes(user_id: int) -> int:
    return dir_size(user_root_dir(user_id))

async def user_quota_bytes(user_id: int) -> int:
    return (PREMIUM_DISK_QUOTA_BYTES if await db_is_premium(user_id) else FREE_DISK_QUOTA_BYTES)

async def user_project_limit(user_id: int) -> int:
    return (PREMIUM_PROJECT_LIMIT if await db_is_premium(user_id) else FREE_PROJECT_LIMIT)

async def user_ram_limit_bytes(user_id: int) -> int:
    return (PREMIUM_MAX_RAM_BYTES if await db_is_premium(user_id) else FREE_MAX_RAM_BYTES)

def safe_zip_extract(zip_path: Path, dest: Path) -> Tuple[bool, str]:
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for info in z.infolist():
                p = Path(info.filename)
                if p.is_absolute() or ".." in p.parts:
                    return False, f"Unsafe path in zip: {info.filename}"
            z.extractall(dest)
        return True, "OK"
    except Exception as e:
        return False, f"ZIP error: {e}"

def list_py_files(root: Path) -> List[str]:
    out = []
    for p in root.rglob("*.py"):
        if p.is_file():
            out.append(str(p.relative_to(root)).replace("\\", "/"))
    out.sort()
    return out

def detect_entrypoint(py_list: List[str]) -> Optional[str]:
    low_map = {p.lower(): p for p in py_list}
    for guess in ENTRYPOINT_GUESSES:
        if guess in low_map:
            return low_map[guess]
    return None

def syntax_check_all(src_root: Path) -> Optional[str]:
    for fp in src_root.rglob("*.py"):
        try:
            data = fp.read_text(encoding="utf-8", errors="replace")
            ast.parse(data, filename=str(fp))
        except SyntaxError as e:
            rel = fp.relative_to(src_root)
            return f"SyntaxError in {rel} (line {e.lineno}): {e.msg}"
        except Exception as e:
            rel = fp.relative_to(src_root)
            return f"Error parsing {rel}: {e}"
    return None

def tail_lines(path: Path, max_lines: int) -> List[str]:
    if not path.exists():
        return ["(no logs yet)"]
    try:
        lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        return lines[-max_lines:] if lines else ["(empty log)"]
    except Exception as e:
        return [f"(failed to read logs: {e})"]

def parse_requirements_text(txt: str) -> Tuple[bool, List[str], List[str]]:
    ok_lines: List[str] = []
    bad_lines: List[str] = []
    for raw in txt.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith(("-", "--")):
            bad_lines.append(raw)
            continue
        if "://" in line or "git+" in line:
            bad_lines.append(raw)
            continue
        if safe_pkg_spec(line) is None:
            bad_lines.append(raw)
            continue
        ok_lines.append(line)
    return (len(bad_lines) == 0), ok_lines, bad_lines

async def project_has_requirements(user_id: int, project_id: int) -> bool:
    req = proj_src_dir(user_id, project_id) / "requirements.txt"
    return req.exists() and req.is_file()

# =========================================================
# Public Security (Join + TOS + Ban + Rate)
# =========================================================
def join_gate_message() -> Tuple[str, InlineKeyboardMarkup]:
    rows = []
    n = max(len(REQUIRED_JOIN_URLS), len(REQUIRED_JOIN_TITLES), 0)
    for i in range(n):
        url = REQUIRED_JOIN_URLS[i] if i < len(REQUIRED_JOIN_URLS) else None
        title = REQUIRED_JOIN_TITLES[i] if i < len(REQUIRED_JOIN_TITLES) else f"Join #{i+1}"
        if url:
            rows.append([InlineKeyboardButton(f"📢 {title}", url=url)])
    rows.append([InlineKeyboardButton("✅ Verify", callback_data="verify")])
    text = (
        header("Join Required")
        + "To use this service, you must join our channel(s) first.\n\n"
        + "After joining, press <b>Verify</b>."
    )
    return text, InlineKeyboardMarkup(rows)

async def is_member_of_required_channels(user_id: int, context: ContextTypes.DEFAULT_TYPE) -> bool:
    if not REQUIRED_JOIN_CHECKS:
        return True
    for chat in REQUIRED_JOIN_CHECKS:
        try:
            chat_id: object = chat
            if re.fullmatch(r"-?\d+", chat):
                chat_id = int(chat)
            member = await context.bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status in {ChatMemberStatus.LEFT, ChatMemberStatus.KICKED}:
                return False
        except Exception:
            return True if JOIN_FAIL_OPEN else False
    return True

async def send_tos(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = header("Terms of Service") + TOS_TEXT + "\n\nPress <b>Accept</b> to continue."
    kb = InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Accept", callback_data="tos_accept"),
         InlineKeyboardButton("❌ Decline", callback_data="tos_decline")]
    ])
    if update.message:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    elif update.callback_query:
        await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)

async def guard(update: Update, context: ContextTypes.DEFAULT_TYPE, allow_callbacks: Optional[set] = None) -> bool:
    u = update.effective_user
    if not u:
        return False

    # rate limit (skip admins)
    if not is_admin(u.id):
        now = time.time()
        last = _LAST_USER_ACTION.get(u.id, 0)
        if now - last < RATE_LIMIT_SECONDS:
            return False
        _LAST_USER_ACTION[u.id] = now

    # ban check
    reason = await db_is_banned(u.id)
    if reason:
        if update.message:
            await update.message.reply_text(f"🚫 Access denied. You are banned.\nReason: {reason}")
        elif update.callback_query:
            await update.callback_query.answer("Banned", show_alert=True)
        return False

    # TOS
    if PUBLIC_MODE:
        if not await db_tos_accepted(u.id):
            if update.callback_query and allow_callbacks and update.callback_query.data in allow_callbacks:
                return True
            if update.callback_query:
                await update.callback_query.answer("Accept TOS first", show_alert=True)
            else:
                await send_tos(update, context)
            return False

    # Join gate
    if PUBLIC_MODE and FORCE_JOIN and REQUIRED_JOIN_CHECKS:
        verified = await db_is_verified(u.id)
        if not verified:
            if update.callback_query and allow_callbacks and update.callback_query.data in allow_callbacks:
                return True
            ok = await is_member_of_required_channels(u.id, context)
            if not ok:
                text, kb = join_gate_message()
                if update.callback_query:
                    await update.callback_query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                else:
                    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
                return False
            await db_set_verified(u.id, True)

    return True

# =========================================================
# Quotas
# =========================================================
async def check_daily_upload_limit(user_id: int) -> Tuple[bool, str]:
    prem = await db_is_premium(user_id)
    lim = PREMIUM_DAILY_UPLOADS if prem else FREE_DAILY_UPLOADS
    up, _ = await usage_get(user_id)
    if up >= lim:
        return False, f"Daily upload limit reached ({up}/{lim}). Try tomorrow or upgrade."
    return True, "OK"

async def check_daily_install_limit(user_id: int) -> Tuple[bool, str]:
    prem = await db_is_premium(user_id)
    lim = PREMIUM_DAILY_INSTALLS if prem else FREE_DAILY_INSTALLS
    _, ins = await usage_get(user_id)
    if ins >= lim:
        return False, f"Daily install limit reached ({ins}/{lim}). Try tomorrow or upgrade."
    return True, "OK"

async def quota_check_new_upload(user_id: int, new_src_bytes: int) -> Tuple[bool, str]:
    quota = await user_quota_bytes(user_id)
    used = user_used_bytes(user_id)
    if used + new_src_bytes > quota:
        return False, f"Disk quota exceeded. Used {human_bytes(used)} / {human_bytes(quota)}."
    return True, "OK"

async def quota_check_update(user_id: int, project_id: int, new_src_bytes: int) -> Tuple[bool, str]:
    quota = await user_quota_bytes(user_id)
    used = user_used_bytes(user_id)
    old_src = proj_src_dir(user_id, project_id)
    old_src_bytes = dir_size(old_src)
    new_used = used - old_src_bytes + new_src_bytes
    if new_used > quota:
        return False, f"Disk quota exceeded. After update would be {human_bytes(new_used)} / {human_bytes(quota)}."
    return True, "OK"

async def ensure_project_slot(user_id: int) -> Tuple[bool, str]:
    lim = await user_project_limit(user_id)
    cnt = await db_count_projects(user_id)
    if cnt >= lim:
        return False, f"Project limit reached ({cnt}/{lim}). Delete old projects or upgrade."
    return True, "OK"

# =========================================================
# UI Keyboards
# =========================================================
def main_menu_kb(user_id: int) -> InlineKeyboardMarkup:
    rows = [
        [("➕ New Project", "new"), ("⬆️ Import Project", "import")],
        [("📁 My Projects", "my_projects"), ("👤 Profile", "profile")],
        [("⭐ Premium", "premium_info"), ("🛟 Help", "help")],
    ]
    if is_admin(user_id):
        rows.append([("🛡 Admin Panel", "admin")])
    return kbd(rows)

def project_menu_kb(project_id: int, running: bool, autostart: bool, has_req: bool) -> InlineKeyboardMarkup:
    start_stop = ("⏹ Stop", f"p:{project_id}:stop") if running else ("▶️ Start", f"p:{project_id}:start")
    auto = ("🟢 Autostart ON", f"p:{project_id}:autostart_off") if autostart else ("⚪ Autostart OFF", f"p:{project_id}:autostart_on")
    req_btn = ("📦 Install requirements.txt", f"p:{project_id}:req") if has_req else ("📦 requirements.txt (missing)", f"p:{project_id}:req_missing")
    return kbd([
        [start_stop, ("🔁 Restart", f"p:{project_id}:restart")],
        [("📜 Logs", f"p:{project_id}:logs:0"), ("🔄 Refresh", f"p:{project_id}:refresh")],
        [("🔐 ENV Vars", f"p:{project_id}:env"), ("🧩 Install Module", f"p:{project_id}:install")],
        [req_btn, ("📤 Export ZIP", f"p:{project_id}:export")],
        [("♻️ Update Code", f"p:{project_id}:update"), ("✏️ Rename", f"p:{project_id}:rename")],
        [("🗑 Delete", f"p:{project_id}:delete"), auto],
        [("⬅️ Back", "my_projects")]
    ])

def logs_kb(project_id: int, page: int) -> InlineKeyboardMarkup:
    return kbd([
        [("🔄 Refresh", f"p:{project_id}:logs:{page}"), ("🧹 Clear Log", f"p:{project_id}:logclear")],
        [("◀️ Older", f"p:{project_id}:logs:{page+1}"), ("▶️ Newer", f"p:{project_id}:logs:{max(page-1, 0)}")],
        [("⬅️ Back", f"p:{project_id}:open")]
    ])

def env_kb(project_id: int) -> InlineKeyboardMarkup:
    return kbd([
        [("➕ Set KEY=VALUE", f"p:{project_id}:env_set"), ("➖ Delete KEY", f"p:{project_id}:env_del")],
        [("⬅️ Back", f"p:{project_id}:open")]
    ])

def admin_kb() -> InlineKeyboardMarkup:
    return kbd([
        [("📊 System", "a:stats"), ("🔥 Running", "a:running")],
        [("⭐ Set Premium", "a:premium"), ("🚫 Ban/Unban", "a:ban")],
        [("📣 Broadcast", "a:broadcast"), ("⛔ Stop Project ID", "a:stopid")],
        [("🧹 Cleanup logs", "a:cleanlogs"), ("⬅️ Back", "home")]
    ])

def project_card(p: dict, running: bool) -> str:
    status = "✅ <b>RUNNING</b>" if running else "⏸ <b>STOPPED</b>"
    auto = "🟢 ON" if p["autostart"] else "⚪ OFF"
    return (
        f"📦 <b>{escape_html(p['name'])}</b>\n"
        f"• ID: <code>{p['project_id']}</code>\n"
        f"• Entrypoint: <code>{escape_html(p['entrypoint'])}</code>\n"
        f"• Status: {status}\n"
        f"• Autostart: {auto}\n"
    )

async def render_logs(project: dict) -> List[str]:
    lf = proj_log_file(project["user_id"], project["project_id"])
    lines = tail_lines(lf, LOG_TAIL_LINES)
    running = project["project_id"] in RUNTIMES
    status = "RUNNING ✅" if running else "STOPPED ⏸"
    return [
        f"📜 <b>Logs</b> — <b>{escape_html(project['name'])}</b> <code>#{project['project_id']}</code>",
        f"Status: <b>{status}</b>",
        "",
    ] + lines

def paginate_logs(lines: List[str], page: int) -> str:
    header_lines = lines[:3]
    body = lines[3:]
    total = len(body)
    page = max(0, page)

    start_from_end = page * LOG_PAGE_SIZE
    start = max(0, total - start_from_end - LOG_PAGE_SIZE)
    end = max(0, total - start_from_end)
    chunk = body[start:end] if body else ["(no logs)"]
    if not chunk:
        chunk = ["(no more logs)"]

    info = f"\n\n<b>Page:</b> {page} | lines {start+1}-{end} of {total}"
    text = "\n".join(header_lines) + "\n<pre>" + escape_html("\n".join(chunk)) + "</pre>" + info
    return text[:3900]


# =========================================================
# Runtime manager (per-project venv + logs + restart + watchdog)
# =========================================================
@dataclass
class Runtime:
    project_id: int
    user_id: int
    name: str
    entrypoint: str
    proc: asyncio.subprocess.Process
    run_id: int
    started_at: int
    stopping: bool = False
    restart_delay: int = CRASH_RESTART_BASE_DELAY
    mem_log: List[str] = field(default_factory=list)
    pump_task: Optional[asyncio.Task] = None
    wait_task: Optional[asyncio.Task] = None

RUNTIMES: Dict[int, Runtime] = {}

def running_count_for_user(user_id: int) -> int:
    return sum(1 for r in RUNTIMES.values() if r.user_id == user_id)

async def ensure_venv(venv_dir: Path) -> Tuple[bool, str]:
    py = venv_python(venv_dir)
    if py.exists():
        return True, "OK"
    try:
        venv_dir.parent.mkdir(parents=True, exist_ok=True)
        proc = await asyncio.create_subprocess_exec(
            sys.executable, "-m", "venv", str(venv_dir),
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            out, _ = await asyncio.wait_for(proc.communicate(), timeout=VENV_CREATE_TIMEOUT)
        except asyncio.TimeoutError:
            proc.kill()
            return False, "venv create timeout"
        if proc.returncode != 0:
            return False, f"venv error:\n{out.decode(errors='replace')[:1500]}"
        return True, "OK"
    except Exception as e:
        return False, f"venv create failed: {e}"

async def kill_process_tree(pid: int, timeout: float = 8.0):
    try:
        p = psutil.Process(pid)
    except Exception:
        return
    children = []
    try:
        children = p.children(recursive=True)
    except Exception:
        children = []
    for c in children:
        try: c.terminate()
        except Exception: pass
    try: p.terminate()
    except Exception: pass
    _, alive = psutil.wait_procs(children + [p], timeout=timeout)
    for a in alive:
        try: a.kill()
        except Exception: pass

async def start_project_process(project: dict) -> Tuple[bool, str]:
    pid = project["project_id"]
    user_id = project["user_id"]

    if pid in RUNTIMES:
        return False, "Already running."

    prem = await db_is_premium(user_id)
    limit = PREMIUM_RUNNING_LIMIT if prem else FREE_RUNNING_LIMIT
    if running_count_for_user(user_id) >= limit:
        return False, f"Running limit reached ({limit}). Stop something first."

    src = proj_src_dir(user_id, pid)
    venv = proj_venv_dir(user_id, pid)
    logs_dir = proj_logs_dir(user_id, pid)
    logs_dir.mkdir(parents=True, exist_ok=True)

    ep_path = (src / project["entrypoint"]).resolve()
    if not ep_path.exists():
        return False, f"Entrypoint not found: {project['entrypoint']}"

    ok, msg = await ensure_venv(venv)
    if not ok:
        return False, msg

    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    env_vars = await db_env_get_all(pid)
    for k, v in env_vars.items():
        env[k] = v

    logf = proj_log_file(user_id, pid)
    with logf.open("a", encoding="utf-8", errors="replace") as f:
        f.write(f"\n===== START {fmt_dt(now_ts())} | project={pid} =====\n")

    proc = await asyncio.create_subprocess_exec(
        str(venv_python(venv)), "-u", str(ep_path),
        cwd=str(src),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        env=env,
        start_new_session=True if os.name != "nt" else False,
    )
    run_id = await db_run_start(pid, proc.pid or 0)

    rt = Runtime(
        project_id=pid,
        user_id=user_id,
        name=project["name"],
        entrypoint=project["entrypoint"],
        proc=proc,
        run_id=run_id,
        started_at=now_ts(),
    )
    RUNTIMES[pid] = rt
    rt.pump_task = asyncio.create_task(pump_logs(rt))
    rt.wait_task = asyncio.create_task(wait_and_maybe_restart(rt))
    await audit_log(user_id, "start_project", str(pid), project["name"])
    return True, "Started."

async def stop_project_process(project_id: int, reason: str = "Stopped by user") -> Tuple[bool, str]:
    rt = RUNTIMES.get(project_id)
    if not rt:
        return False, "Not running."
    rt.stopping = True

    pid = rt.proc.pid or 0
    try:
        if rt.proc.returncode is None:
            try: rt.proc.terminate()
            except Exception: pass
            try:
                await asyncio.wait_for(rt.proc.wait(), timeout=8)
            except asyncio.TimeoutError:
                if pid:
                    await kill_process_tree(pid)
    except Exception as e:
        return False, f"Stop error: {e}"

    exit_code = rt.proc.returncode if rt.proc.returncode is not None else -1
    await db_run_stop(rt.run_id, exit_code, reason)

    for t in [rt.pump_task, rt.wait_task]:
        if t and not t.done():
            t.cancel()

    RUNTIMES.pop(project_id, None)
    await audit_log(rt.user_id, "stop_project", str(project_id), reason)
    return True, "Stopped."

async def restart_project(project_id: int) -> Tuple[bool, str]:
    p = await db_get_project(project_id)
    if not p:
        return False, "Project not found."
    if project_id in RUNTIMES:
        await stop_project_process(project_id, "Restart requested")
        await asyncio.sleep(1)
    return await start_project_process(p)

async def pump_logs(rt: Runtime):
    logf = proj_log_file(rt.user_id, rt.project_id)
    try:
        assert rt.proc.stdout is not None
        while True:
            line = await rt.proc.stdout.readline()
            if not line:
                break
            s = line.decode("utf-8", errors="replace").rstrip("\n")
            rt.mem_log.append(s)
            if len(rt.mem_log) > MEM_LOG_RING_LINES:
                rt.mem_log = rt.mem_log[-MEM_LOG_RING_LINES:]
            with logf.open("a", encoding="utf-8", errors="replace") as f:
                f.write(s + "\n")
    except asyncio.CancelledError:
        return
    except Exception as e:
        with logf.open("a", encoding="utf-8", errors="replace") as f:
            f.write(f"[hostingbot] log pump error: {e}\n")

async def notify_crash(user_id: int, project_id: int, name: str, exit_code: int, last_lines: List[str], delay: int):
    if not GLOBAL_APP:
        return
    msg = (
        header("Crash Detected")
        + f"Project: <b>{escape_html(name)}</b> <code>#{project_id}</code>\n"
        + f"Exit code: <b>{exit_code}</b>\n"
        + f"Auto-restart in: <b>{delay}s</b>\n\n"
        + "<b>Last logs:</b>\n"
        + "<pre>" + escape_html("\n".join(last_lines[-25:])) + "</pre>"
    )
    try:
        await GLOBAL_APP.bot.send_message(
            chat_id=user_id,
            text=msg[:3900],
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([
                [("📜 Open Logs", f"p:{project_id}:logs:0"), ("🔁 Restart Now", f"p:{project_id}:restart")],
                [("⏹ Stop", f"p:{project_id}:stop"), ("📦 Open Project", f"p:{project_id}:open")]
            ])
        )
    except Exception:
        pass

async def wait_and_maybe_restart(rt: Runtime):
    project_id = rt.project_id
    user_id = rt.user_id
    logf = proj_log_file(user_id, project_id)

    try:
        rc = await rt.proc.wait()
    except asyncio.CancelledError:
        return
    except Exception:
        rc = -1

    try:
        await db_run_stop(rt.run_id, rc, "Exited")
    except Exception:
        pass

    if RUNTIMES.get(project_id) is rt:
        RUNTIMES.pop(project_id, None)

    with logf.open("a", encoding="utf-8", errors="replace") as f:
        f.write(f"===== EXIT {fmt_dt(now_ts())} | code={rc} =====\n")

    if rt.stopping:
        return

    project = await db_get_project(project_id)
    if not project or not project["autostart"]:
        return

    prem = await db_is_premium(user_id)
    limit = PREMIUM_RUNNING_LIMIT if prem else FREE_RUNNING_LIMIT
    if running_count_for_user(user_id) >= limit:
        return

    delay = min(CRASH_RESTART_MAX_DELAY, rt.restart_delay)
    rt.restart_delay = min(CRASH_RESTART_MAX_DELAY, rt.restart_delay * 2)

    last_lines = rt.mem_log if rt.mem_log else tail_lines(logf, 30)
    await notify_crash(user_id, project_id, project["name"], rc, last_lines, delay)

    await asyncio.sleep(delay)
    try:
        await start_project_process(project)
    except Exception:
        return

async def watchdog_loop():
    while True:
        await asyncio.sleep(WATCHDOG_INTERVAL)
        for _, rt in list(RUNTIMES.items()):
            if rt.proc.returncode is not None or not rt.proc.pid:
                continue
            try:
                p = psutil.Process(rt.proc.pid)
                rss = p.memory_info().rss
                limit = await user_ram_limit_bytes(rt.user_id)
                if rss > limit:
                    lf = proj_log_file(rt.user_id, rt.project_id)
                    with lf.open("a", encoding="utf-8", errors="replace") as f:
                        f.write(f"[watchdog] RAM limit exceeded: {human_bytes(rss)} > {human_bytes(limit)}. Killing...\n")
                    await kill_process_tree(rt.proc.pid)
            except Exception:
                continue


# =========================================================
# Install (pip + requirements)
# =========================================================
async def install_package(project_id: int, spec: str) -> str:
    project = await db_get_project(project_id)
    if not project:
        return header("Install") + "❌ Project not found."

    user_id = project["user_id"]
    ok, msg = await check_daily_install_limit(user_id)
    if not ok:
        return header("Install") + "❌ " + escape_html(msg)

    spec_ok = safe_pkg_spec(spec)
    if not spec_ok:
        return header("Install") + "❌ Invalid package spec."

    venv = proj_venv_dir(user_id, project_id)
    ok, msg = await ensure_venv(venv)
    if not ok:
        return header("Install") + "❌ " + escape_html(msg)

    pip = venv_pip(venv)
    cmd = [str(pip), "install", spec_ok, "--disable-pip-version-check"]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(proj_src_dir(user_id, project_id)),
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=PIP_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        return header("Install") + "❌ Install timed out."

    s = out.decode("utf-8", errors="replace")[-3000:]
    await usage_inc(user_id, "installs", 1)
    await audit_log(user_id, "pip_install", str(project_id), spec_ok)

    if proc.returncode == 0:
        return header("Install") + f"✅ Installed <code>{escape_html(spec_ok)}</code>\n\n<pre>{escape_html(s)}</pre>"
    return header("Install") + f"❌ Failed <code>{escape_html(spec_ok)}</code>\n\n<pre>{escape_html(s)}</pre>"

async def install_requirements(project_id: int) -> str:
    project = await db_get_project(project_id)
    if not project:
        return header("requirements.txt") + "❌ Project not found."
    user_id = project["user_id"]

    ok, msg = await check_daily_install_limit(user_id)
    if not ok:
        return header("requirements.txt") + "❌ " + escape_html(msg)

    req_path = proj_src_dir(user_id, project_id) / "requirements.txt"
    if not req_path.exists():
        return header("requirements.txt") + "❌ Not found."

    raw = req_path.read_text(encoding="utf-8", errors="replace")
    ok_parse, ok_lines, bad_lines = parse_requirements_text(raw)
    if not ok_parse:
        return (
            header("requirements.txt Blocked")
            + "❌ Unsafe lines detected. Only simple packages allowed.\n\n"
            + "<pre>" + escape_html("\n".join(bad_lines[:15])) + "</pre>"
        )
    if not ok_lines:
        return header("requirements.txt") + "⚠️ Nothing to install."

    tmp = tmp_dir_for(user_id)
    safe_req = tmp / "requirements.safe.txt"
    safe_req.write_text("\n".join(ok_lines) + "\n", encoding="utf-8")

    venv = proj_venv_dir(user_id, project_id)
    ok, msg = await ensure_venv(venv)
    if not ok:
        shutil.rmtree(tmp, ignore_errors=True)
        return header("requirements.txt") + "❌ " + escape_html(msg)

    pip = venv_pip(venv)
    cmd = [str(pip), "install", "-r", str(safe_req), "--disable-pip-version-check"]

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
        cwd=str(proj_src_dir(user_id, project_id)),
    )
    try:
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=PIP_TIMEOUT)
    except asyncio.TimeoutError:
        proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)
        return header("requirements.txt") + "❌ Install timed out."

    s = out.decode("utf-8", errors="replace")[-3000:]
    shutil.rmtree(tmp, ignore_errors=True)

    await usage_inc(user_id, "installs", 1)
    await audit_log(user_id, "pip_install_req", str(project_id), f"lines={len(ok_lines)}")

    if proc.returncode == 0:
        return header("requirements.txt") + "✅ Installed.\n\n<pre>" + escape_html(s) + "</pre>"
    return header("requirements.txt") + "❌ Failed.\n\n<pre>" + escape_html(s) + "</pre>"


# =========================================================
# Export / Import
# =========================================================
def build_export_zip(src_dir: Path, project: dict, out_zip: Path):
    import json
    meta = {
        "name": project["name"],
        "entrypoint": project["entrypoint"],
        "exported_at": fmt_dt(now_ts()),
        "format": "hostingbot-v1"
    }
    with zipfile.ZipFile(out_zip, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("hostingbot.json", json.dumps(meta, ensure_ascii=False, indent=2))
        for p in src_dir.rglob("*"):
            if p.is_file():
                arc = str(p.relative_to(src_dir)).replace("\\", "/")
                z.write(p, f"src/{arc}")

def load_import_zip(zip_path: Path, extract_to: Path) -> Tuple[bool, str, Optional[dict]]:
    meta_obj = None
    try:
        with zipfile.ZipFile(zip_path, "r") as z:
            for info in z.infolist():
                p = Path(info.filename)
                if p.is_absolute() or ".." in p.parts:
                    return False, f"Unsafe path in zip: {info.filename}", None
            z.extractall(extract_to)

        meta_path = extract_to / "hostingbot.json"
        if meta_path.exists():
            try:
                import json
                meta_obj = json.loads(meta_path.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                meta_obj = None

        if (extract_to / "src").exists() and (extract_to / "src").is_dir():
            tmp = extract_to / "__src__"
            if tmp.exists():
                shutil.rmtree(tmp, ignore_errors=True)
            (extract_to / "src").rename(tmp)
            shutil.rmtree(extract_to / "work", ignore_errors=True)
            tmp.rename(extract_to / "work")
        else:
            (extract_to / "work").mkdir(parents=True, exist_ok=True)
            for child in list(extract_to.iterdir()):
                if child.name in {"work", "hostingbot.json"}:
                    continue
                shutil.move(str(child), str(extract_to / "work" / child.name))

        return True, "OK", meta_obj
    except Exception as e:
        return False, f"Import zip error: {e}", None

async def export_project_zip(project_id: int) -> Optional[Path]:
    p = await db_get_project(project_id)
    if not p:
        return None
    user_id = p["user_id"]
    src = proj_src_dir(user_id, project_id)
    if not src.exists():
        return None
    tmp = tmp_dir_for(user_id)
    out_zip = tmp / f"project_{project_id}_export.zip"
    try:
        build_export_zip(src, p, out_zip)
        return out_zip
    except Exception:
        return None


# =========================================================
# Command Handlers
# =========================================================
async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    u = update.effective_user
    await db_upsert_user(u.id, u.username)

    if PUBLIC_MODE and not await db_tos_accepted(u.id):
        await send_tos(update, context)
        return

    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return

    prem = await db_is_premium(u.id)
    run_limit = PREMIUM_RUNNING_LIMIT if prem else FREE_RUNNING_LIMIT
    quota = await user_quota_bytes(u.id)
    used = user_used_bytes(u.id)
    proj_limit = await user_project_limit(u.id)
    proj_count = await db_count_projects(u.id)
    ram_limit = await user_ram_limit_bytes(u.id)
    up, ins = await usage_get(u.id)

    text = (
        header("Hosting Panel")
        + f"Welcome, <b>{escape_html(u.full_name or 'User')}</b>\n\n"
        + "✅ Python Hosting • ZIP/Single .py\n"
        + "🔍 Syntax check on upload\n"
        + "🔐 Secure ENV (encrypted)\n"
        + "🧩 Manual module install (pip)\n"
        + "♻️ Autostart + Crash auto-restart\n\n"
        + f"Plan: {'⭐ Premium' if prem else '🆓 Free'}\n"
        + f"Running: <b>{running_count_for_user(u.id)}</b> / {run_limit}\n"
        + f"Projects: <b>{proj_count}</b> / {proj_limit}\n"
        + f"Disk: <b>{human_bytes(used)}</b> / {human_bytes(quota)}\n"
        + f"RAM limit per bot: <b>{human_bytes(ram_limit)}</b>\n"
        + f"Today: uploads <b>{up}</b>, installs <b>{ins}</b>\n"
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(u.id))

async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    text = header("Help") + (
        "• <b>Project</b> = one hosted bot/script.\n"
        "• Upload .py or .zip\n"
        "• If syntax error exists, you will see it instantly.\n"
        "• Use ENV Vars for tokens/secrets.\n\n"
        "Public safety: Join gate + TOS + daily quotas enabled."
    )
    await update.message.reply_text(text, parse_mode=ParseMode.HTML)

async def cmd_projects(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    await show_projects(update, context)

async def cmd_admin(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Access denied.")
        return
    await update.message.reply_text(header("Admin Panel") + "Choose:", parse_mode=ParseMode.HTML, reply_markup=admin_kb())

async def cmd_chatid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin helper: get private channel chat_id by forwarding a post from that channel."""
    if update.effective_user.id not in ADMIN_IDS:
        return
    msg = update.message
    if not msg:
        return
    chat_id = None

    # Legacy
    if getattr(msg, "forward_from_chat", None):
        chat_id = msg.forward_from_chat.id

    # Newer API: forward_origin
    if not chat_id and getattr(msg, "forward_origin", None):
        fo = msg.forward_origin
        if getattr(fo, "chat", None):
            chat_id = fo.chat.id

    if not chat_id:
        await msg.reply_text("Forward a post from the private channel to me, then send /chatid on that forwarded message.")
        return

    await msg.reply_text(f"✅ Chat ID: `{chat_id}`", parse_mode="Markdown")

# =========================================================
# Menu rendering
# =========================================================
async def show_projects(update: Update, context: ContextTypes.DEFAULT_TYPE, edit: bool = False, query=None):
    user_id = update.effective_user.id
    projects = await db_list_projects(user_id)

    if not projects:
        text = header("My Projects") + "No projects yet.\n\nPress <b>New Project</b> to upload."
        if edit and query:
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(user_id))
        return

    lines = [header("My Projects")]
    rows = []
    for p in projects[:40]:
        pid = p["project_id"]
        running = pid in RUNTIMES
        status = "✅" if running else "⏸"
        auto = "🟢" if p["autostart"] else "⚪"
        lines.append(f"{status} {auto} <b>{escape_html(p['name'])}</b>  <code>#{pid}</code>")
        rows.append([(f"{status} {p['name']}", f"p:{pid}:open")])

    rows.append([("⬅️ Back", "home")])
    text = "\n".join(lines)
    if edit and query:
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kbd(rows))
    else:
        await update.message.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kbd(rows))


# =========================================================
# Callback router
# =========================================================
async def cb_router(update: Update, context: ContextTypes.DEFAULT_TYPE):
    allow = {"tos_accept", "tos_decline", "verify"}
    if not await guard(update, context, allow_callbacks=allow):
        return

    query = update.callback_query
    await query.answer()
    u = update.effective_user
    await db_upsert_user(u.id, u.username)
    data = query.data

    # TOS callbacks
    if data == "tos_accept":
        await db_set_tos(u.id, True)
        await audit_log(u.id, "tos_accept", "", "")
        await query.edit_message_text("✅ TOS accepted. Now press /start", parse_mode=ParseMode.HTML)
        return

    if data == "tos_decline":
        await db_set_tos(u.id, False)
        await audit_log(u.id, "tos_decline", "", "")
        await query.edit_message_text("❌ You declined the TOS. Access denied.", parse_mode=ParseMode.HTML)
        return

    if data == "verify":
        ok = await is_member_of_required_channels(u.id, context)
        if not ok:
            text, kb = join_gate_message()
            await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
            return
        await db_set_verified(u.id, True)
        await audit_log(u.id, "verified", "", "")
        await query.edit_message_text("✅ Verified! Now press /start", parse_mode=ParseMode.HTML)
        return

    if data == "home":
        context.user_data.clear()
        await query.edit_message_text(header("Main Menu") + "Choose:", parse_mode=ParseMode.HTML, reply_markup=main_menu_kb(u.id))
        return

    if data == "help":
        await query.edit_message_text(
            header("Help") +
            "• New Project → upload .py/.zip\n"
            "• Projects → start/stop/restart/logs/env/install\n"
            "• Autostart → always-on restart\n",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("⬅️ Back", "home")]])
        )
        return

    if data == "premium_info":
        await query.edit_message_text(
            header("Premium") +
            f"Free: {FREE_RUNNING_LIMIT} running, {FREE_PROJECT_LIMIT} projects, {FREE_DISK_QUOTA_MB}MB disk\n"
            f"Premium: {PREMIUM_RUNNING_LIMIT} running, {PREMIUM_PROJECT_LIMIT} projects, {PREMIUM_DISK_QUOTA_MB}MB disk\n\n"
            "Ask admin to activate premium for you.",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("⬅️ Back", "home")]])
        )
        return

    if data == "profile":
        prem = await db_is_premium(u.id)
        run_limit = PREMIUM_RUNNING_LIMIT if prem else FREE_RUNNING_LIMIT
        quota = await user_quota_bytes(u.id)
        used = user_used_bytes(u.id)
        proj_limit = await user_project_limit(u.id)
        proj_count = await db_count_projects(u.id)
        up, ins = await usage_get(u.id)

        await query.edit_message_text(
            header("Profile") +
            f"ID: <code>{u.id}</code>\n"
            f"Plan: {'⭐ Premium' if prem else '🆓 Free'}\n"
            f"Running: <b>{running_count_for_user(u.id)}</b> / {run_limit}\n"
            f"Projects: <b>{proj_count}</b> / {proj_limit}\n"
            f"Disk: <b>{human_bytes(used)}</b> / {human_bytes(quota)}\n"
            f"Today: uploads <b>{up}</b>, installs <b>{ins}</b>\n",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("⬅️ Back", "home")]])
        )
        return

    if data == "my_projects":
        context.user_data.clear()
        await show_projects(update, context, edit=True, query=query)
        return

    if data == "new":
        ok, msg = await ensure_project_slot(u.id)
        if not ok:
            await query.edit_message_text(header("New Project") + "❌ " + escape_html(msg), parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", "home")]]))
            return
        ok2, msg2 = await check_daily_upload_limit(u.id)
        if not ok2:
            await query.edit_message_text(header("New Project") + "❌ " + escape_html(msg2), parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", "home")]]))
            return

        context.user_data.clear()
        context.user_data["state"] = "NEW_NAME"
        await query.edit_message_text(
            header("New Project") + "Send a project name (example: <code>My Aiogram Bot</code>)",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("⬅️ Cancel", "home")]])
        )
        return

    if data == "import":
        ok, msg = await ensure_project_slot(u.id)
        if not ok:
            await query.edit_message_text(header("Import") + "❌ " + escape_html(msg), parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", "home")]]))
            return
        ok2, msg2 = await check_daily_upload_limit(u.id)
        if not ok2:
            await query.edit_message_text(header("Import") + "❌ " + escape_html(msg2), parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", "home")]]))
            return

        context.user_data.clear()
        context.user_data["state"] = "IMPORT_NAME"
        await query.edit_message_text(
            header("Import Project") +
            "Send a name for imported project (or send <code>auto</code>), then upload the ZIP.",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("⬅️ Cancel", "home")]])
        )
        return

    if data == "admin":
        if not is_admin(u.id):
            await query.edit_message_text("❌ Access denied.", reply_markup=kbd([[("⬅️ Back", "home")]]))
            return
        await query.edit_message_text(header("Admin Panel") + "Choose:", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if data.startswith("a:"):
        if not is_admin(u.id):
            await query.edit_message_text("❌ Access denied.")
            return
        await admin_action(update, context, query, data.split(":", 1)[1])
        return

    if data.startswith("p:"):
        parts = data.split(":")
        project_id = int(parts[1])
        action = parts[2]
        rest = parts[3:] if len(parts) > 3 else []
        await project_action(update, context, query, project_id, action, rest)
        return


# =========================================================
# Admin actions (compact but powerful)
# =========================================================
async def admin_action(update: Update, context: ContextTypes.DEFAULT_TYPE, query, action: str):
    admin_id = update.effective_user.id

    if action == "stats":
        vm = psutil.virtual_memory()
        cpu = psutil.cpu_percent(interval=0.2)
        disk = psutil.disk_usage(str(DATA_DIR))
        text = header("System") + (
            f"CPU: <b>{cpu}%</b>\n"
            f"RAM: <b>{human_bytes(vm.used)}</b> / {human_bytes(vm.total)}\n"
            f"DISK(data): <b>{human_bytes(disk.used)}</b> / {human_bytes(disk.total)}\n"
            f"Running projects: <b>{len(RUNTIMES)}</b>\n"
        )
        await query.edit_message_text(text, parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if action == "running":
        if not RUNTIMES:
            await query.edit_message_text(header("Running") + "No running projects.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
            return
        lines = [header("Running Projects")]
        rows = []
        for pid, rt in list(RUNTIMES.items())[:40]:
            uptime = now_ts() - rt.started_at
            lines.append(f"• <b>{escape_html(rt.name)}</b> <code>#{pid}</code> | user <code>{rt.user_id}</code> | {uptime}s")
            rows.append([("⛔ Stop #" + str(pid), f"a:stop:{pid}")])
        rows.append([("⬅️ Back", "admin")])
        await query.edit_message_text("\n".join(lines)[:3900], parse_mode=ParseMode.HTML, reply_markup=kbd(rows))
        return

    if action.startswith("stop:"):
        pid = int(action.split(":")[1])
        await stop_project_process(pid, "Stopped by admin")
        await audit_log(admin_id, "admin_stop_project", str(pid), "")
        await query.edit_message_text(header("Admin") + f"Stopped <code>#{pid}</code>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if action == "premium":
        context.user_data.clear()
        context.user_data["state"] = "ADMIN_PREMIUM"
        await query.edit_message_text(
            header("Set Premium") + "Send: <code>USER_ID on</code> or <code>USER_ID off</code>",
            parse_mode=ParseMode.HTML, reply_markup=admin_kb()
        )
        return

    if action == "ban":
        context.user_data.clear()
        context.user_data["state"] = "ADMIN_BAN"
        await query.edit_message_text(
            header("Ban / Unban") + "Send: <code>USER_ID ban reason...</code> or <code>USER_ID unban</code>",
            parse_mode=ParseMode.HTML, reply_markup=admin_kb()
        )
        return

    if action == "broadcast":
        context.user_data.clear()
        context.user_data["state"] = "ADMIN_BROADCAST"
        await query.edit_message_text(header("Broadcast") + "Send broadcast text now.", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if action == "stopid":
        context.user_data.clear()
        context.user_data["state"] = "ADMIN_STOPID"
        await query.edit_message_text(header("Stop Project") + "Send project id like <code>123</code>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return

    if action == "cleanlogs":
        cleaned = 0
        for p in (DATA_DIR / "projects").rglob("run.log"):
            try:
                if p.stat().st_size > 5 * 1024 * 1024:
                    lines = tail_lines(p, 2000)
                    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
                    cleaned += 1
            except Exception:
                pass
        await query.edit_message_text(header("Cleanup") + f"Done. Cleaned logs: <b>{cleaned}</b>", parse_mode=ParseMode.HTML, reply_markup=admin_kb())
        return


# =========================================================
# Project actions
# =========================================================
async def project_action(update: Update, context: ContextTypes.DEFAULT_TYPE, query, project_id: int, action: str, rest: List[str]):
    u = update.effective_user
    project = await db_get_project(project_id)
    if not project:
        await query.edit_message_text("❌ Project not found.", reply_markup=kbd([[("⬅️ Back", "my_projects")]]))
        return
    if project["user_id"] != u.id and not is_admin(u.id):
        await query.edit_message_text("❌ Not your project.", reply_markup=kbd([[("⬅️ Back", "my_projects")]]))
        return

    running = project_id in RUNTIMES
    has_req = await project_has_requirements(project["user_id"], project_id)

    if action in {"open", "refresh"}:
        await query.edit_message_text(header("Project") + project_card(project, running), parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    if action == "start":
        ok, msg = await start_project_process(project)
        project = await db_get_project(project_id)
        running = project_id in RUNTIMES
        text = header("Project") + project_card(project, running) + f"\n<b>Start:</b> {escape_html(msg)}"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    if action == "stop":
        ok, msg = await stop_project_process(project_id, "Stopped by user")
        project = await db_get_project(project_id)
        running = project_id in RUNTIMES
        text = header("Project") + project_card(project, running) + f"\n<b>Stop:</b> {escape_html(msg)}"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    if action == "restart":
        ok, msg = await restart_project(project_id)
        project = await db_get_project(project_id)
        running = project_id in RUNTIMES
        text = header("Project") + project_card(project, running) + f"\n<b>Restart:</b> {escape_html(msg)}"
        await query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    if action == "autostart_on":
        await db_set_autostart(project_id, True)
        project = await db_get_project(project_id)
        await query.edit_message_text(header("Project") + project_card(project, running), parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, True, has_req))
        return

    if action == "autostart_off":
        await db_set_autostart(project_id, False)
        project = await db_get_project(project_id)
        await query.edit_message_text(header("Project") + project_card(project, running), parse_mode=ParseMode.HTML,
                                     reply_markup=project_menu_kb(project_id, running, False, has_req))
        return

    if action == "logs":
        page = int(rest[0]) if rest else 0
        lines = await render_logs(project)
        await query.edit_message_text(paginate_logs(lines, page), parse_mode=ParseMode.HTML, reply_markup=logs_kb(project_id, page))
        return

    if action == "logclear":
        lf = proj_log_file(project["user_id"], project_id)
        lf.parent.mkdir(parents=True, exist_ok=True)
        lf.write_text("", encoding="utf-8")
        await query.edit_message_text(header("Logs") + "✅ Log cleared.", parse_mode=ParseMode.HTML, reply_markup=logs_kb(project_id, 0))
        return

    if action == "env":
        keys = await db_env_list(project_id)
        msg = header("ENV Vars") + "Values are hidden for security.\n\n"
        msg += "<b>Saved Keys:</b>\n" + ("\n".join([f"• <code>{k}</code>" for k in keys]) if keys else "<i>No keys.</i>")
        await query.edit_message_text(msg, parse_mode=ParseMode.HTML, reply_markup=env_kb(project_id))
        return

    if action == "env_set":
        context.user_data.clear()
        context.user_data["state"] = "ENV_SET"
        context.user_data["target_project_id"] = project_id
        await query.edit_message_text(header("Set ENV") + "Send: <code>KEY=VALUE</code> (KEY must be UPPERCASE)",
                                     parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Cancel", f"p:{project_id}:env")]]))
        return

    if action == "env_del":
        context.user_data.clear()
        context.user_data["state"] = "ENV_DEL"
        context.user_data["target_project_id"] = project_id
        await query.edit_message_text(header("Delete ENV") + "Send key: <code>BOT_TOKEN</code>",
                                     parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Cancel", f"p:{project_id}:env")]]))
        return

    if action == "install":
        context.user_data.clear()
        context.user_data["state"] = "INSTALL"
        context.user_data["target_project_id"] = project_id
        await query.edit_message_text(header("Install Module") + "Send package: <code>aiogram</code> / <code>requests==2.31.0</code>",
                                     parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Cancel", f"p:{project_id}:open")]]))
        return

    if action == "req_missing":
        await query.edit_message_text(header("requirements.txt") + "❌ requirements.txt not found in project root.",
                                     parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", f"p:{project_id}:open")]]))
        return

    if action == "req":
        await query.edit_message_text(header("requirements.txt") + "⏳ Installing...", parse_mode=ParseMode.HTML)
        out = await install_requirements(project_id)
        await query.edit_message_text(out[:3900], parse_mode=ParseMode.HTML, reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    if action == "export":
        await query.edit_message_text(header("Export") + "⏳ Preparing ZIP...", parse_mode=ParseMode.HTML)
        zpath = await export_project_zip(project_id)
        if not zpath:
            await query.edit_message_text(header("Export") + "❌ Export failed.", parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", f"p:{project_id}:open")]]))
            return
        await GLOBAL_APP.bot.send_document(
            chat_id=u.id,
            document=str(zpath),
            filename=f"{project['name']}_export.zip",
            caption=f"Exported: {project['name']} (#{project_id})"
        )
        try:
            zpath.unlink(missing_ok=True)
        except Exception:
            pass
        await query.edit_message_text(header("Export") + "✅ Sent ZIP in your chat.", parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Back", f"p:{project_id}:open")]]))
        return

    if action == "rename":
        context.user_data.clear()
        context.user_data["state"] = "RENAME"
        context.user_data["target_project_id"] = project_id
        await query.edit_message_text(header("Rename Project") + "Send new name:", parse_mode=ParseMode.HTML,
                                     reply_markup=kbd([[("⬅️ Cancel", f"p:{project_id}:open")]]))
        return

    if action == "update":
        context.user_data.clear()
        context.user_data["state"] = "UPDATE_WAIT_FILE"
        context.user_data["target_project_id"] = project_id
        await query.edit_message_text(header("Update Code") + "Upload new <b>.py</b> or <b>.zip</b> now.",
                                     parse_mode=ParseMode.HTML, reply_markup=kbd([[("⬅️ Cancel", f"p:{project_id}:open")]]))
        return

    if action == "delete":
        await query.edit_message_text(
            header("Delete Project") + f"Delete <b>{escape_html(project['name'])}</b>? This is permanent.",
            parse_mode=ParseMode.HTML,
            reply_markup=kbd([[("🗑 Yes Delete", f"p:{project_id}:delete_yes"), ("⬅️ Cancel", f"p:{project_id}:open")]])
        )
        return

    if action == "delete_yes":
        if project_id in RUNTIMES:
            await stop_project_process(project_id, "Deleted by user")
        base = proj_dir(project["user_id"], project_id)
        if base.exists():
            shutil.rmtree(base, ignore_errors=True)
        await db_delete_project(project_id)
        await query.edit_message_text(header("Deleted") + "✅ Project deleted.", parse_mode=ParseMode.HTML,
                                     reply_markup=kbd([[("⬅️ Back", "my_projects")]]))
        return


# =========================================================
# Text input states + Upload wizard
# =========================================================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    u = update.effective_user
    await db_upsert_user(u.id, u.username)

    state = context.user_data.get("state")
    if not state:
        return
    text = (update.message.text or "").strip()

    if state == "NEW_NAME":
        name = safe_project_name(text)
        context.user_data["tmp_name"] = name
        context.user_data["state"] = "NEW_WAIT_FILE"
        await update.message.reply_text(
            header("Upload Code") + f"Project name: <b>{escape_html(name)}</b>\n\nNow upload <b>.py</b> or <b>.zip</b>.",
            parse_mode=ParseMode.HTML
        )
        return

    if state == "IMPORT_NAME":
        context.user_data["tmp_name"] = "AUTO" if text.lower() == "auto" else safe_project_name(text)
        context.user_data["state"] = "IMPORT_WAIT_FILE"
        await update.message.reply_text(header("Import ZIP") + "Now upload the <b>.zip</b> file.", parse_mode=ParseMode.HTML)
        return

    if state == "RENAME":
        pid = int(context.user_data["target_project_id"])
        await db_rename_project(pid, safe_project_name(text))
        context.user_data.clear()
        await update.message.reply_text("✅ Renamed.")
        return

    if state == "ENV_SET":
        pid = int(context.user_data["target_project_id"])
        if "=" not in text:
            await update.message.reply_text("❌ Send KEY=VALUE.")
            return
        k, v = text.split("=", 1)
        k = safe_env_key(k)
        if not k:
            await update.message.reply_text("❌ Invalid key. Use UPPERCASE like BOT_TOKEN.")
            return
        await db_env_set(pid, k, v)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Saved ENV key: {k}")
        return

    if state == "ENV_DEL":
        pid = int(context.user_data["target_project_id"])
        k = safe_env_key(text)
        if not k:
            await update.message.reply_text("❌ Invalid key.")
            return
        await db_env_del(pid, k)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Deleted ENV key: {k}")
        return

    if state == "INSTALL":
        pid = int(context.user_data["target_project_id"])
        context.user_data.clear()
        await update.message.reply_text("⏳ Installing...")
        out = await install_package(pid, text)
        await update.message.reply_text(out[:3900], parse_mode=ParseMode.HTML)
        return

    # Admin text states
    if state == "ADMIN_PREMIUM" and is_admin(u.id):
        m = re.fullmatch(r"(\d+)\s+(on|off)", text.lower())
        if not m:
            await update.message.reply_text("❌ Format: USER_ID on/off")
            return
        uid = int(m.group(1))
        val = (m.group(2) == "on")
        await db_set_premium(uid, val)
        context.user_data.clear()
        await update.message.reply_text(f"✅ Premium for {uid}: {val}")
        return

    if state == "ADMIN_BAN" and is_admin(u.id):
        m = re.match(r"^(\d+)\s+(ban|unban)(.*)$", text.strip(), re.IGNORECASE)
        if not m:
            await update.message.reply_text("❌ Format: USER_ID ban reason... OR USER_ID unban")
            return
        uid = int(m.group(1))
        cmd = m.group(2).lower()
        rest = (m.group(3) or "").strip()
        if cmd == "ban":
            await db_ban(uid, u.id, rest or "No reason")
            # stop all running for that user
            for pid, rt in list(RUNTIMES.items()):
                if rt.user_id == uid:
                    await stop_project_process(pid, "Banned by admin")
            context.user_data.clear()
            await update.message.reply_text(f"✅ Banned {uid}")
            return
        if cmd == "unban":
            await db_unban(uid)
            context.user_data.clear()
            await update.message.reply_text(f"✅ Unbanned {uid}")
            return

    if state == "ADMIN_STOPID" and is_admin(u.id):
        if not text.isdigit():
            await update.message.reply_text("❌ Send numeric project id.")
            return
        pid = int(text)
        await stop_project_process(pid, "Stopped by admin")
        context.user_data.clear()
        await update.message.reply_text(f"✅ Stopped project #{pid}")
        return

    if state == "ADMIN_BROADCAST" and is_admin(u.id):
        context.user_data.clear()
        await update.message.reply_text("Broadcast is not included in this compact build. (Can be added if you want.)")
        return


async def on_document(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    u = update.effective_user
    await db_upsert_user(u.id, u.username)

    state = context.user_data.get("state")
    if state not in {"NEW_WAIT_FILE", "UPDATE_WAIT_FILE", "IMPORT_WAIT_FILE"}:
        return

    doc = update.message.document
    if not doc:
        return

    if doc.file_size and doc.file_size > MAX_UPLOAD_BYTES:
        await update.message.reply_text(f"❌ File too large. Max: {human_bytes(MAX_UPLOAD_BYTES)}")
        return

    filename = (doc.file_name or "upload.bin").lower()
    if state == "IMPORT_WAIT_FILE" and not filename.endswith(".zip"):
        await update.message.reply_text("❌ Import requires .zip")
        return
    if not (filename.endswith(".py") or filename.endswith(".zip")):
        await update.message.reply_text("❌ Only .py or .zip allowed.")
        return

    # daily upload quota
    ok_u, msg_u = await check_daily_upload_limit(u.id)
    if not ok_u:
        await update.message.reply_text("❌ " + msg_u)
        return

    tmp = tmp_dir_for(u.id)
    context.user_data["tmp_dir"] = str(tmp)

    await update.message.reply_text("⬇️ Downloading...")
    tg_file = await doc.get_file()
    dl_path = tmp / (doc.file_name or "upload.bin")
    await tg_file.download_to_drive(custom_path=str(dl_path))

    extract_root = tmp / "extract"
    extract_root.mkdir(parents=True, exist_ok=True)
    import_meta = None

    if filename.endswith(".py"):
        work = extract_root / "work"
        work.mkdir(parents=True, exist_ok=True)
        shutil.copy2(dl_path, work / (doc.file_name or "main.py"))
    else:
        if state == "IMPORT_WAIT_FILE":
            ok, msg, meta = load_import_zip(dl_path, extract_root)
            import_meta = meta
            if not ok:
                await update.message.reply_text(f"❌ {msg}")
                shutil.rmtree(tmp, ignore_errors=True)
                context.user_data.clear()
                return
        else:
            work = extract_root / "work"
            work.mkdir(parents=True, exist_ok=True)
            ok, msg = safe_zip_extract(dl_path, work)
            if not ok:
                await update.message.reply_text(f"❌ {msg}")
                shutil.rmtree(tmp, ignore_errors=True)
                context.user_data.clear()
                return

    work_root = extract_root / "work"
    if not work_root.exists():
        await update.message.reply_text("❌ Extract failed.")
        shutil.rmtree(tmp, ignore_errors=True)
        context.user_data.clear()
        return

    # disk quota check
    new_bytes = dir_size(work_root)
    if state in {"NEW_WAIT_FILE", "IMPORT_WAIT_FILE"}:
        ok, msg = await quota_check_new_upload(u.id, new_bytes)
        if not ok:
            await update.message.reply_text("❌ " + msg)
            shutil.rmtree(tmp, ignore_errors=True)
            context.user_data.clear()
            return
    elif state == "UPDATE_WAIT_FILE":
        pid = int(context.user_data["target_project_id"])
        pr = await db_get_project(pid)
        if not pr:
            await update.message.reply_text("❌ Project not found.")
            return
        ok, msg = await quota_check_update(pr["user_id"], pid, new_bytes)
        if not ok:
            await update.message.reply_text("❌ " + msg)
            shutil.rmtree(tmp, ignore_errors=True)
            context.user_data.clear()
            return

    # syntax check
    err = syntax_check_all(work_root)
    if err:
        await update.message.reply_text(header("Syntax Error") + f"<pre>{escape_html(err)}</pre>", parse_mode=ParseMode.HTML)
        shutil.rmtree(tmp, ignore_errors=True)
        context.user_data.clear()
        return

    py_list = list_py_files(work_root)
    if not py_list:
        await update.message.reply_text("❌ No .py found in upload.")
        shutil.rmtree(tmp, ignore_errors=True)
        context.user_data.clear()
        return

    context.user_data["tmp_file_root"] = str(work_root)
    context.user_data["tmp_py_list"] = py_list
    if import_meta:
        context.user_data["import_meta"] = import_meta

    auto_ep = detect_entrypoint(py_list)
    if auto_ep:
        await finalize_upload(update, context, auto_ep)
        return

    if len(py_list) == 1:
        await finalize_upload(update, context, py_list[0])
        return

    # pick entrypoint
    rows = []
    for i, p in enumerate(py_list[:35]):
        rows.append([(f"▶️ {p}", f"pick:{i}")])
    rows.append([("⬅️ Cancel", "home")])

    if state == "NEW_WAIT_FILE":
        context.user_data["state"] = "NEW_PICK_EP"
    elif state == "UPDATE_WAIT_FILE":
        context.user_data["state"] = "UPDATE_PICK_EP"
    else:
        context.user_data["state"] = "IMPORT_PICK_EP"

    await update.message.reply_text(
        header("Select Entrypoint") + "Multiple .py files found.\nChoose which file should run:",
        parse_mode=ParseMode.HTML,
        reply_markup=kbd(rows)
    )

async def cb_pick_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not await guard(update, context, allow_callbacks={"tos_accept", "tos_decline", "verify"}):
        return
    query = update.callback_query
    await query.answer()
    idx = int(query.data.split(":")[1])
    py_list = context.user_data.get("tmp_py_list") or []
    if idx < 0 or idx >= len(py_list):
        await query.edit_message_text("❌ Invalid selection.")
        return
    await finalize_upload(update, context, py_list[idx], via_query=query)

async def finalize_upload(update: Update, context: ContextTypes.DEFAULT_TYPE, entrypoint: str, via_query=None):
    u = update.effective_user
    state = context.user_data.get("state")
    tmp_root = Path(context.user_data.get("tmp_file_root", ""))
    tmp_dir = Path(context.user_data.get("tmp_dir", ""))

    if not tmp_root.exists():
        if via_query:
            await via_query.edit_message_text("❌ Upload session expired.")
        else:
            await update.message.reply_text("❌ Upload session expired.")
        context.user_data.clear()
        return

    # count upload when success
    await usage_inc(u.id, "uploads", 1)

    # NEW
    if state in {"NEW_WAIT_FILE", "NEW_PICK_EP"}:
        ok, msg = await ensure_project_slot(u.id)
        if not ok:
            await update.message.reply_text("❌ " + msg)
            return
        name = safe_project_name(context.user_data.get("tmp_name", "MyProject"))
        project_id = await db_create_project(u.id, name, entrypoint)

        src = proj_src_dir(u.id, project_id)
        logs = proj_logs_dir(u.id, project_id)
        src.mkdir(parents=True, exist_ok=True)
        logs.mkdir(parents=True, exist_ok=True)
        shutil.rmtree(src, ignore_errors=True)
        src.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_root, src, dirs_exist_ok=True)

        proj_log_file(u.id, project_id).write_text(
            f"===== CREATED {fmt_dt(now_ts())} | entrypoint={entrypoint} =====\n",
            encoding="utf-8"
        )

        shutil.rmtree(tmp_dir, ignore_errors=True)
        context.user_data.clear()

        p = await db_get_project(project_id)
        has_req = await project_has_requirements(u.id, project_id)
        text = header("Project Created") + project_card(p, False) + "\n✅ Upload complete."
        if via_query:
            await via_query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                             reply_markup=project_menu_kb(project_id, False, p["autostart"], has_req))
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                            reply_markup=project_menu_kb(project_id, False, p["autostart"], has_req))
        return

    # UPDATE
    if state in {"UPDATE_WAIT_FILE", "UPDATE_PICK_EP"}:
        project_id = int(context.user_data.get("target_project_id"))
        project = await db_get_project(project_id)
        if not project:
            await update.message.reply_text("❌ Project not found.")
            return

        src = proj_src_dir(project["user_id"], project_id)
        shutil.rmtree(src, ignore_errors=True)
        src.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_root, src, dirs_exist_ok=True)
        await db_update_project_entrypoint(project_id, entrypoint)

        shutil.rmtree(tmp_dir, ignore_errors=True)
        context.user_data.clear()

        project = await db_get_project(project_id)
        running = project_id in RUNTIMES
        has_req = await project_has_requirements(project["user_id"], project_id)
        text = header("Updated") + project_card(project, running) + "\n✅ Code updated."
        if via_query:
            await via_query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                             reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                            reply_markup=project_menu_kb(project_id, running, project["autostart"], has_req))
        return

    # IMPORT
    if state in {"IMPORT_WAIT_FILE", "IMPORT_PICK_EP"}:
        ok, msg = await ensure_project_slot(u.id)
        if not ok:
            await update.message.reply_text("❌ " + msg)
            return
        meta = context.user_data.get("import_meta") or {}
        raw_name = context.user_data.get("tmp_name", "Imported")
        name = safe_project_name(str(meta.get("name") or "ImportedProject")) if raw_name == "AUTO" else safe_project_name(raw_name)

        project_id = await db_create_project(u.id, name, entrypoint)
        src = proj_src_dir(u.id, project_id)
        logs = proj_logs_dir(u.id, project_id)
        src.mkdir(parents=True, exist_ok=True)
        logs.mkdir(parents=True, exist_ok=True)
        shutil.copytree(tmp_root, src, dirs_exist_ok=True)

        proj_log_file(u.id, project_id).write_text(
            f"===== IMPORTED {fmt_dt(now_ts())} | entrypoint={entrypoint} =====\n",
            encoding="utf-8"
        )

        shutil.rmtree(tmp_dir, ignore_errors=True)
        context.user_data.clear()

        p = await db_get_project(project_id)
        has_req = await project_has_requirements(u.id, project_id)
        text = header("Imported") + project_card(p, False) + "\n✅ Import complete."
        if via_query:
            await via_query.edit_message_text(text, parse_mode=ParseMode.HTML,
                                             reply_markup=project_menu_kb(project_id, False, p["autostart"], has_req))
        else:
            await update.message.reply_text(text, parse_mode=ParseMode.HTML,
                                            reply_markup=project_menu_kb(project_id, False, p["autostart"], has_req))
        return


# =========================================================
# Startup tasks
# =========================================================
async def autostart_all():
    pids = await db_list_autostart_projects()
    for pid in pids[:250]:
        p = await db_get_project(pid)
        if not p or pid in RUNTIMES:
            continue
        if await db_is_banned(p["user_id"]):
            continue
        try:
            await start_project_process(p)
            await asyncio.sleep(0.15)
        except Exception:
            continue

async def post_init(app: Application):
    global GLOBAL_APP
    GLOBAL_APP = app
    await db_init()
    asyncio.create_task(autostart_all())
    asyncio.create_task(watchdog_loop())


# =========================================================
# Build app + run (Webhook for Choreo)
# =========================================================
def build_app() -> Application:
    app = Application.builder().token(BOT_TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("projects", cmd_projects))
    app.add_handler(CommandHandler("admin", cmd_admin))
    app.add_handler(CommandHandler("chatid", cmd_chatid))

    app.add_handler(CallbackQueryHandler(cb_pick_entry, pattern=r"^pick:\d+$"))
    app.add_handler(CallbackQueryHandler(cb_router))

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_handler(MessageHandler(filters.Document.ALL, on_document))
    return app

if __name__ == "__main__":
    app = build_app()

    if WEBHOOK_ENABLED:
        kwargs = dict(
            listen="0.0.0.0",
            port=PORT,
            url_path=WEBHOOK_PATH.lstrip("/"),
            drop_pending_updates=True,
        )
        # PUBLIC_BASE_URL না থাকলেও server চালু থাকবে (readiness pass করবে)
        if PUBLIC_BASE_URL:
            kwargs["webhook_url"] = f"{PUBLIC_BASE_URL}{WEBHOOK_PATH}"

        app.run_webhook(**kwargs)
    else:
        app.run_polling(drop_pending_updates=True, close_loop=False)
