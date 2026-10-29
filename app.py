import os, secrets, sqlite3, mimetypes, json, threading, time, re, difflib
from urllib.parse import quote

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
from datetime import datetime, timezone
from urllib.parse import urlencode
from zoneinfo import ZoneInfo
import requests
from functools import wraps
from werkzeug.security import generate_password_hash, check_password_hash
from flask import abort, Flask, render_template, redirect, request, session, url_for, flash, send_from_directory, Response, jsonify

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))

@app.template_filter("fromjson")
def fromjson_filter(value):
    try:
        return json.loads(value or "[]")
    except Exception:
        return []

@app.template_filter("tiktok_date")
def tiktok_date(value):
    try:
        ts = int(value)
        return datetime.fromtimestamp(ts, ZoneInfo("Asia/Jakarta")).strftime("%d %b %Y %H:%M WIB")
    except Exception:
        return str(value or "—")

DB = os.environ.get("DATABASE_PATH", "DashboardFYEPE.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get(
    "TIKTOK_REDIRECT_URI",
    "https://app.dashboardfyepe.islammoderat.my.id/auth/tiktok/callback"
)
SCOPES = os.environ.get("TIKTOK_SCOPES", "user.info.basic,user.info.profile,user.info.stats,video.list,video.publish")

AUTH_URL = "https://www.tiktok.com/v2/auth/authorize/"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"
USER_URL = "https://open.tiktokapis.com/v2/user/info/"
CREATOR_URL = "https://open.tiktokapis.com/v2/post/publish/creator_info/query/"
DIRECT_POST_INIT_URL = "https://open.tiktokapis.com/v2/post/publish/video/init/"
POST_STATUS_URL = "https://open.tiktokapis.com/v2/post/publish/status/fetch/"
VIDEO_LIST_URL = "https://open.tiktokapis.com/v2/video/list/"
PHOTO_POST_URL = "https://open.tiktokapis.com/v2/post/publish/content/init/"
REVOKE_URL = "https://open.tiktokapis.com/v2/oauth/revoke/"

# One-chunk implementation. TikTok allows chunks up to 64 MB.
MAX_UPLOAD_BYTES = 64 * 1024 * 1024
app.config["MAX_CONTENT_LENGTH"] = MAX_UPLOAD_BYTES + (2 * 1024 * 1024)

VERIFY_FILENAME = "tiktok8X1qCm95yvCX8YUCrVKwJVg1gjLQxAqB.txt"
PHOTO_UPLOAD_DIR = os.environ.get("PHOTO_UPLOAD_DIR", "/tmp/DashboardFYEPE_photos")
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://app.dashboardfyepe.islammoderat.my.id").rstrip("/")
os.makedirs(PHOTO_UPLOAD_DIR, exist_ok=True)

# Background statistics refresh queue. One worker intentionally processes jobs
# sequentially so a large account set does not hammer TikTok APIs or the browser.
_stats_worker_lock = threading.Lock()
_stats_worker_started = False
STATS_PAGE_SIZE = 50
STATS_API_DELAY = float(os.environ.get("STATS_API_DELAY", "0.15"))
# A background worker is per Gunicorn process. Expire abandoned jobs so that
# restarts or worker termination never leave the browser polling forever.
STATS_JOB_MAX_SECONDS = max(60, int(os.environ.get("STATS_JOB_MAX_SECONDS", "600")))
STATS_QUEUE_MAX_SECONDS = max(60, int(os.environ.get("STATS_QUEUE_MAX_SECONDS", "900")))

def _stats_age_seconds(value):
    if not value:
        return 0
    try:
        return max(0, (datetime.utcnow() - datetime.fromisoformat(str(value).replace("Z", "+00:00").replace("+00:00", ""))).total_seconds())
    except (ValueError, TypeError):
        return 0

def _expire_stale_stats_jobs(c):
    """Recover orphaned jobs; preserve existing account statistics."""
    jobs = c.execute("SELECT id,batch_id,status,created_at,started_at FROM stats_refresh_jobs WHERE status IN ('QUEUED','PROCESSING')").fetchall()
    now = datetime.utcnow().isoformat()
    for job in jobs:
        age = _stats_age_seconds(job['started_at'] if job['status']=='PROCESSING' else job['created_at'])
        limit = STATS_JOB_MAX_SECONDS if job['status']=='PROCESSING' else STATS_QUEUE_MAX_SECONDS
        if age > limit:
            c.execute("UPDATE stats_refresh_jobs SET status='FAILED',last_error=?,finished_at=? WHERE id=? AND status=?",
                      ('Antrean kedaluwarsa atau proses terhenti. Silakan coba refresh kembali.',now,job['id'],job['status']))
    batches = c.execute("SELECT id FROM stats_refresh_batches WHERE status IN ('QUEUED','PROCESSING')").fetchall()
    for batch in batches:
        counts = c.execute("SELECT status,COUNT(*) AS n FROM stats_refresh_jobs WHERE batch_id=? GROUP BY status", (batch['id'],)).fetchall()
        tally = {r['status']:r['n'] for r in counts}
        completed = tally.get('DONE',0)+tally.get('FAILED',0)
        active = tally.get('QUEUED',0)+tally.get('PROCESSING',0)
        if not active:
            c.execute("UPDATE stats_refresh_batches SET status='DONE',completed_count=?,failed_count=?,finished_at=? WHERE id=?",
                      (completed,tally.get('FAILED',0),now,batch['id']))
        else:
            c.execute("UPDATE stats_refresh_batches SET completed_count=?,failed_count=? WHERE id=?",
                      (completed,tally.get('FAILED',0),batch['id']))
    c.commit()


class DBConn:
    """Small compatibility wrapper: PostgreSQL on Render, SQLite fallback locally."""
    def __init__(self):
        self.is_pg = bool(DATABASE_URL)
        if self.is_pg:
            if psycopg2 is None:
                raise RuntimeError("DATABASE_URL tersedia tetapi psycopg2 belum terpasang.")
            self.conn = psycopg2.connect(DATABASE_URL, sslmode="require")
        else:
            self.conn = sqlite3.connect(DB)
            self.conn.row_factory = sqlite3.Row

    def execute(self, sql, params=()):
        if self.is_pg:
            sql = sql.replace("?", "%s")
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, params)
            return cur
        return self.conn.execute(sql, params)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

def db():
    return DBConn()

def _columns(c):
    if c.is_pg:
        rows = c.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='accounts'
        """).fetchall()
        return {r["column_name"] for r in rows}
    rows = c.execute("PRAGMA table_info(accounts)").fetchall()
    return {r["name"] for r in rows}

def init_db():
    c = db()
    # Authentication / role management. Roles: superadmin, admin_medsos, customer.
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS users(
            id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, full_name TEXT,
            email TEXT, password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'customer',
            is_active INTEGER DEFAULT 1, created_at TEXT, last_login TEXT
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS users(
            id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL, full_name TEXT,
            email TEXT, password_hash TEXT NOT NULL, role TEXT NOT NULL DEFAULT 'customer',
            is_active INTEGER DEFAULT 1, created_at TEXT, last_login TEXT
        )""")
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS user_keywords(
            id SERIAL PRIMARY KEY, customer_id INTEGER NOT NULL, keyword TEXT NOT NULL, created_at TEXT
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS user_keywords(
            id INTEGER PRIMARY KEY AUTOINCREMENT, customer_id INTEGER NOT NULL, keyword TEXT NOT NULL, created_at TEXT
        )""")

    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS accounts(
            open_id TEXT PRIMARY KEY,
            display_name TEXT,
            avatar_url TEXT,
            access_token TEXT,
            refresh_token TEXT,
            scope TEXT,
            expires_in INTEGER,
            connected_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS publish_jobs(
            publish_id TEXT PRIMARY KEY,
            open_id TEXT NOT NULL,
            caption TEXT,
            privacy_level TEXT,
            status TEXT,
            created_at TEXT,
            last_response TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS scheduled_posts(
            id SERIAL PRIMARY KEY,
            open_id TEXT NOT NULL,
            post_type TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT DEFAULT 'SCHEDULED',
            created_at TEXT,
            last_error TEXT
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS accounts(
            open_id TEXT PRIMARY KEY,
            display_name TEXT,
            avatar_url TEXT,
            access_token TEXT,
            refresh_token TEXT,
            scope TEXT,
            expires_in INTEGER,
            connected_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS publish_jobs(
            publish_id TEXT PRIMARY KEY,
            open_id TEXT NOT NULL,
            caption TEXT,
            privacy_level TEXT,
            status TEXT,
            created_at TEXT,
            last_response TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS scheduled_posts(
            id SERIAL PRIMARY KEY,
            open_id TEXT NOT NULL,
            post_type TEXT NOT NULL,
            scheduled_at TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status TEXT DEFAULT 'SCHEDULED',
            created_at TEXT,
            last_error TEXT
        )""")

    # Bulk upload queue. PostgreSQL is recommended for persistent multi-device use.
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS bulk_batches(
            id SERIAL PRIMARY KEY, category TEXT NOT NULL, post_type TEXT NOT NULL,
            caption TEXT, privacy_level TEXT, file_path TEXT NOT NULL, original_name TEXT,
            total_accounts INTEGER DEFAULT 0, queued_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'QUEUED', created_at TEXT, started_at TEXT, finished_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS bulk_jobs(
            id SERIAL PRIMARY KEY, batch_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            status TEXT DEFAULT 'QUEUED', publish_id TEXT, attempts INTEGER DEFAULT 0,
            last_error TEXT, created_at TEXT, started_at TEXT, finished_at TEXT
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS bulk_batches(
            id INTEGER PRIMARY KEY AUTOINCREMENT, category TEXT NOT NULL, post_type TEXT NOT NULL,
            caption TEXT, privacy_level TEXT, file_path TEXT NOT NULL, original_name TEXT,
            total_accounts INTEGER DEFAULT 0, queued_count INTEGER DEFAULT 0,
            success_count INTEGER DEFAULT 0, failed_count INTEGER DEFAULT 0,
            status TEXT DEFAULT 'QUEUED', created_at TEXT, started_at TEXT, finished_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS bulk_jobs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            status TEXT DEFAULT 'QUEUED', publish_id TEXT, attempts INTEGER DEFAULT 0,
            last_error TEXT, created_at TEXT, started_at TEXT, finished_at TEXT
        )""")

    # Background statistics refresh queue. Existing accounts/tokens are untouched.
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS stats_refresh_batches(
            id SERIAL PRIMARY KEY, total_accounts INTEGER DEFAULT 0, completed_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0, status TEXT DEFAULT 'QUEUED', created_at TEXT,
            started_at TEXT, finished_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS stats_refresh_jobs(
            id SERIAL PRIMARY KEY, batch_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            status TEXT DEFAULT 'QUEUED', last_error TEXT, created_at TEXT, started_at TEXT, finished_at TEXT
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS stats_refresh_batches(
            id INTEGER PRIMARY KEY AUTOINCREMENT, total_accounts INTEGER DEFAULT 0, completed_count INTEGER DEFAULT 0,
            failed_count INTEGER DEFAULT 0, status TEXT DEFAULT 'QUEUED', created_at TEXT,
            started_at TEXT, finished_at TEXT
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS stats_refresh_jobs(
            id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            status TEXT DEFAULT 'QUEUED', last_error TEXT, created_at TEXT, started_at TEXT, finished_at TEXT
        )""")

    # Daily snapshots for historical analytics; never fabricate earlier dates.
    c.execute("""CREATE TABLE IF NOT EXISTS account_daily_stats (
        open_id TEXT NOT NULL, snapshot_date TEXT NOT NULL,
        admin_medsos TEXT, category TEXT,
        video_count BIGINT, repost_count BIGINT, view_count BIGINT,
        likes_count BIGINT, comment_count BIGINT, shares_count BIGINT,
        follower_count BIGINT, following_count BIGINT,
        is_verified INTEGER, is_business INTEGER,
        PRIMARY KEY(open_id,snapshot_date)
    )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_daily_stats_admin_date ON account_daily_stats(admin_medsos,snapshot_date)")

    # Safe migration: only ADD columns. Existing connected accounts/tokens stay intact.
    wanted = {
        "email": "TEXT",
        "username": "TEXT",
        "video_count": "BIGINT",
        "following_count": "BIGINT",
        "follower_count": "BIGINT",
        "likes_count": "BIGINT",
        "view_count": "BIGINT",
        "comment_count": "BIGINT",
        "shares_count": "BIGINT",
        "repost_count": "BIGINT",
        "repost_updated_at": "TEXT",
        "stats_updated_at": "TEXT",
        "category": "TEXT",
        "admin_medsos": "TEXT",
        "assigned_admin_id": "INTEGER",
        "is_verified": "INTEGER DEFAULT 0",
        "profile_deep_link": "TEXT"
    }
    existing = _columns(c)
    for name, typ in wanted.items():
        if name not in existing:
            c.execute(f"ALTER TABLE accounts ADD COLUMN {name} {typ}")

    # Bulk V2 migrations: adjustable pacing + pause/resume.
    if c.is_pg:
        rows = c.execute("""SELECT column_name FROM information_schema.columns
                            WHERE table_schema='public' AND table_name='bulk_batches'""").fetchall()
        bulk_cols = {r["column_name"] for r in rows}
    else:
        rows = c.execute("PRAGMA table_info(bulk_batches)").fetchall()
        bulk_cols = {r["name"] for r in rows}
    for name, typ in {"interval_seconds":"INTEGER DEFAULT 12", "paused":"INTEGER DEFAULT 0", "scheduled_at":"TEXT"}.items():
        if name not in bulk_cols:
            c.execute(f"ALTER TABLE bulk_batches ADD COLUMN {name} {typ}")
    # Customer issue/hashtag campaign fields. Existing keywords are preserved.
    if c.is_pg:
        kw_rows = c.execute("""SELECT column_name FROM information_schema.columns
                              WHERE table_schema='public' AND table_name='user_keywords'""").fetchall()
        kw_cols = {r["column_name"] for r in kw_rows}
    else:
        kw_rows = c.execute("PRAGMA table_info(user_keywords)").fetchall()
        kw_cols = {r["name"] for r in kw_rows}
    for name, typ in {"period_start":"TEXT", "period_end":"TEXT"}.items():
        if name not in kw_cols:
            c.execute(f"ALTER TABLE user_keywords ADD COLUMN {name} {typ}")

    # Customer campaign post snapshots. Only posts matching the customer's
    # requested issue/hashtag are stored here; account-wide statistics are
    # intentionally not used for campaign analytics.
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS customer_campaign_posts(
            id SERIAL PRIMARY KEY, keyword_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            video_id TEXT NOT NULL, title TEXT, create_time BIGINT, view_count BIGINT DEFAULT 0,
            like_count BIGINT DEFAULT 0, comment_count BIGINT DEFAULT 0, share_count BIGINT DEFAULT 0,
            cover_image_url TEXT, share_url TEXT, matched_hashtag TEXT, matched_hashtags TEXT, match_score DOUBLE PRECISION DEFAULT 0,
            fetched_at TEXT, UNIQUE(keyword_id, video_id)
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS customer_campaign_posts(
            id INTEGER PRIMARY KEY AUTOINCREMENT, keyword_id INTEGER NOT NULL, open_id TEXT NOT NULL,
            video_id TEXT NOT NULL, title TEXT, create_time INTEGER, view_count INTEGER DEFAULT 0,
            like_count INTEGER DEFAULT 0, comment_count INTEGER DEFAULT 0, share_count INTEGER DEFAULT 0,
            cover_image_url TEXT, share_url TEXT, matched_hashtag TEXT, matched_hashtags TEXT, match_score REAL DEFAULT 0,
            fetched_at TEXT, UNIQUE(keyword_id, video_id)
        )""")

    # Safe migration for multi-hashtag campaign matching.
    if c.is_pg:
        cp_rows = c.execute("""SELECT column_name FROM information_schema.columns
                              WHERE table_schema='public' AND table_name='customer_campaign_posts'""").fetchall()
        cp_cols = {r["column_name"] for r in cp_rows}
    else:
        cp_rows = c.execute("PRAGMA table_info(customer_campaign_posts)").fetchall()
        cp_cols = {r["name"] for r in cp_rows}
    if "matched_hashtags" not in cp_cols:
        c.execute("ALTER TABLE customer_campaign_posts ADD COLUMN matched_hashtags TEXT")

    # Activity log: separate from TikTok tokens and account data.
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS audit_sessions (
            id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, username TEXT NOT NULL,
            role TEXT, ip_address TEXT, user_agent TEXT, login_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL, logout_at TEXT, duration_seconds INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS audit_events (
            id BIGSERIAL PRIMARY KEY, session_id TEXT, user_id INTEGER, username TEXT,
            event_type TEXT NOT NULL, description TEXT, endpoint TEXT,
            method TEXT, ip_address TEXT, created_at TEXT NOT NULL
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS audit_sessions (
            id TEXT PRIMARY KEY, user_id INTEGER NOT NULL, username TEXT NOT NULL,
            role TEXT, ip_address TEXT, user_agent TEXT, login_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL, logout_at TEXT, duration_seconds INTEGER
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS audit_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT, user_id INTEGER,
            username TEXT, event_type TEXT NOT NULL, description TEXT, endpoint TEXT,
            method TEXT, ip_address TEXT, created_at TEXT NOT NULL
        )""")
    # Backward-compatible migration: device details on login and logout.
    if c.is_pg:
        existing = {r["column_name"] for r in c.execute("""SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='audit_sessions'""").fetchall()}
    else:
        existing = {r["name"] for r in c.execute("PRAGMA table_info(audit_sessions)").fetchall()}
    for field in ("login_device", "logout_device", "logout_user_agent", "logout_ip_address"):
        if field not in existing:
            c.execute(f"ALTER TABLE audit_sessions ADD COLUMN {field} TEXT")

    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_created ON audit_events(created_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_events_user ON audit_events(user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_audit_sessions_login ON audit_sessions(login_at)")

    # Bootstrap first Super Admin from Render environment variables.
    # Set INITIAL_ADMIN_USERNAME and INITIAL_ADMIN_PASSWORD once on Render.
    n_users = int(c.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"])
    if n_users == 0:
        initial_user = os.environ.get("INITIAL_ADMIN_USERNAME", "").strip()
        initial_password = os.environ.get("INITIAL_ADMIN_PASSWORD", "")
        if initial_user and initial_password:
            c.execute("""INSERT INTO users(username,full_name,email,password_hash,role,is_active,created_at)
                         VALUES(?,?,?,?,?,?,?)""", (initial_user, os.environ.get("INITIAL_ADMIN_NAME", "Super Admin"),
                         os.environ.get("INITIAL_ADMIN_EMAIL", ""), generate_password_hash(initial_password),
                         "superadmin", 1, datetime.now().isoformat()))
    # Role migration for the existing eboy login (does not change password).
    c.execute("UPDATE users SET role='supervisor_caprof' WHERE LOWER(TRIM(username))='eboy' AND role <> 'supervisor_caprof'")
    c.commit()
    # Ecommerce internal registry (no external TikTok Shop API or new environment variables).
    if c.is_pg:
        c.execute("""CREATE TABLE IF NOT EXISTS ecommerce_accounts (
            id SERIAL PRIMARY KEY, account_type TEXT NOT NULL, account_name TEXT NOT NULL,
            tiktok_username TEXT, shop_name TEXT, notes TEXT, status TEXT NOT NULL DEFAULT 'NOT_CONNECTED',
            owner_user_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ecommerce_products (
            id SERIAL PRIMARY KEY, account_id INTEGER NOT NULL, sku TEXT, product_name TEXT NOT NULL,
            price NUMERIC(16,2) DEFAULT 0, stock INTEGER DEFAULT 0, created_at TEXT NOT NULL
        )""")
    else:
        c.execute("""CREATE TABLE IF NOT EXISTS ecommerce_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT, account_type TEXT NOT NULL, account_name TEXT NOT NULL,
            tiktok_username TEXT, shop_name TEXT, notes TEXT, status TEXT NOT NULL DEFAULT 'NOT_CONNECTED',
            owner_user_id INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS ecommerce_products (
            id INTEGER PRIMARY KEY AUTOINCREMENT, account_id INTEGER NOT NULL, sku TEXT,
            product_name TEXT NOT NULL, price NUMERIC DEFAULT 0, stock INTEGER DEFAULT 0, created_at TEXT NOT NULL
        )""")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ecommerce_owner ON ecommerce_accounts(owner_user_id)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_ecommerce_products_account ON ecommerce_products(account_id)")
    # Internal Business Centre account selections (not TikTok Business API verification).
    c.execute("""CREATE TABLE IF NOT EXISTS business_centre_accounts (
        centre_number INTEGER NOT NULL, open_id TEXT NOT NULL,
        added_by INTEGER, added_at TEXT NOT NULL,
        PRIMARY KEY(centre_number,open_id)
    )""")
    # Phone Farm: gateway credentials are stored hashed in the existing database.
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_gateways (
        id TEXT PRIMARY KEY, name TEXT NOT NULL, token_hash TEXT NOT NULL,
        created_at TEXT NOT NULL, last_seen TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_devices (
        gateway_id TEXT NOT NULL, serial TEXT NOT NULL, model TEXT,
        battery INTEGER, last_seen TEXT NOT NULL,
        PRIMARY KEY(gateway_id,serial)
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_commands (
        id TEXT PRIMARY KEY, gateway_id TEXT NOT NULL, serial TEXT NOT NULL,
        action TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING',
        requested_by INTEGER, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
        result TEXT
    )""")
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_screens (
        gateway_id TEXT NOT NULL, serial TEXT NOT NULL, image_data BYTEA,
        updated_at TEXT NOT NULL, PRIMARY KEY(gateway_id,serial)
    )""" if c.is_pg else """CREATE TABLE IF NOT EXISTS phonefarm_screens (
        gateway_id TEXT NOT NULL, serial TEXT NOT NULL, image_data BLOB,
        updated_at TEXT NOT NULL, PRIMARY KEY(gateway_id,serial)
    )""")
    # Tahap 4A: persistent Panda slot number; never infer from ADB enumeration.
    if c.is_pg:
        pfcols = {r["column_name"] for r in c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='phonefarm_devices'").fetchall()}
    else:
        pfcols = {r["name"] for r in c.execute("PRAGMA table_info(phonefarm_devices)").fetchall()}
    if "panda_number" not in pfcols:
        c.execute("ALTER TABLE phonefarm_devices ADD COLUMN panda_number INTEGER")
    c.execute("CREATE INDEX IF NOT EXISTS idx_phonefarm_commands_gateway ON phonefarm_commands(gateway_id,status,created_at)")
    c.commit()
    c.close()

# Activity logging intentionally excludes request bodies, tokens, passwords and query strings.
# Set TRUST_PROXY_HEADERS=0 when running without a trusted reverse proxy.
from werkzeug.middleware.proxy_fix import ProxyFix
if os.environ.get("TRUST_PROXY_HEADERS", "1") == "1":
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1)

AUDIT_ACTIVITY_EXCLUDE = {"static", "account_avatar", "uploaded_photo", "publish_status", "stats_refresh_status", "audit_log"}
AUDIT_SESSION_IDLE_SECONDS = 60 * 60
# Background polling/static/media do not extend an interactive login.
IDLE_PASSIVE_ENDPOINTS = {"stats_refresh_status", "refresh_all_status", "publish_status", "account_avatar", "uploaded_photo", "static", "health", "phonefarm_heartbeat", "phonefarm_next_command", "phonefarm_ack_command", "phonefarm_screen", "phonefarm_screen_status", "phonefarm_screen_upload"}

def audit_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def audit_ip():
    return (request.remote_addr or "unknown")[:64]

def audit_device():
    """Best-effort device description from browser User-Agent (not a unique device ID)."""
    ua = request.headers.get("User-Agent", "")[:350]
    low = ua.lower()
    if "iphone" in low: platform = "iPhone / iOS"
    elif "ipad" in low: platform = "iPad / iPadOS"
    elif "android" in low: platform = "Android"
    elif "windows" in low: platform = "Windows"
    elif "macintosh" in low or "mac os" in low: platform = "macOS"
    elif "linux" in low: platform = "Linux"
    else: platform = "Perangkat tidak dikenali"
    if "edg/" in low: browser = "Edge"
    elif "opr/" in low or "opera" in low: browser = "Opera"
    elif "firefox/" in low: browser = "Firefox"
    elif "chrome/" in low or "crios/" in low: browser = "Chrome"
    elif "safari/" in low: browser = "Safari"
    else: browser = "Browser tidak dikenali"
    kind = "HP/Tablet" if any(x in low for x in ("mobile", "android", "iphone", "ipad")) else "Desktop/Laptop"
    return f"{kind} · {platform} · {browser}"[:150]

def audit_write(event_type, description="", user=None, session_id=None):
    try:
        uid = user["id"] if user else session.get("user_id")
        uname = user["username"] if user else session.get("audit_username", "")
        c = db()
        c.execute("""INSERT INTO audit_events(session_id,user_id,username,event_type,description,endpoint,method,ip_address,created_at)
                     VALUES(?,?,?,?,?,?,?,?,?)""", (session_id or session.get("audit_session_id"), uid,
                     uname, event_type[:32], description[:220], (request.endpoint or "")[:100],
                     request.method, audit_ip(), audit_now()))
        c.commit(); c.close()
    except Exception:
        app.logger.exception("Could not write activity audit event")

def audit_touch():
    sid = session.get("audit_session_id")
    if not sid or not session.get("user_id"):
        return
    now = int(time.time())
    last = int(session.get("audit_touch_ts", 0))
    endpoint = request.endpoint or ""
    if now - last < 30:
        return
    session["audit_touch_ts"] = now
    try:
        c = db()
        c.execute("UPDATE audit_sessions SET last_seen_at=? WHERE id=? AND logout_at IS NULL", (audit_now(), sid))
        c.commit(); c.close()
    except Exception:
        app.logger.exception("Could not update audit last seen")

def audit_finish():
    sid = session.get("audit_session_id")
    if not sid:
        return
    now = audit_now()
    try:
        c = db()
        record = c.execute("SELECT login_at FROM audit_sessions WHERE id=? AND logout_at IS NULL", (sid,)).fetchone()
        if record:
            start = datetime.fromisoformat(record["login_at"])
            duration = max(0, int((datetime.fromisoformat(now)-start).total_seconds()))
            c.execute("""UPDATE audit_sessions SET logout_at=?,last_seen_at=?,duration_seconds=?,
                       logout_device=?,logout_user_agent=?,logout_ip_address=? WHERE id=?""",
                       (now, now, duration, audit_device(), request.headers.get("User-Agent", "")[:350],
                        audit_ip(), sid))
            c.commit()
        c.close()
    except Exception:
        app.logger.exception("Could not finish audit session")

PUBLIC_ENDPOINTS = {"login", "logout", "terms_page", "privacy_page", "health", "verify_file", "tiktok_callback", "static", "uploaded_photo", "phonefarm_heartbeat", "phonefarm_next_command", "phonefarm_ack_command", "phonefarm_screen", "phonefarm_screen_status", "phonefarm_screen_upload"}

def current_user():
    uid = session.get("user_id")
    if not uid:
        return None
    c = db()
    u = c.execute("SELECT * FROM users WHERE id=? AND is_active=1", (uid,)).fetchone()
    c.close()
    return u

def role_required(*roles):
    def deco(fn):
        @wraps(fn)
        def wrapped(*args, **kwargs):
            u = current_user()
            if not u:
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok":False,"message":"Sesi login berakhir. Silakan login kembali."}), 401
                return redirect(url_for("login", next=request.path))
            if u["role"] not in roles:
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok":False,"message":"Tidak memiliki izin mengubah akun ini."}), 403
                flash("Anda tidak memiliki akses ke halaman tersebut.", "error")
                return redirect(url_for("customer_dashboard" if u["role"] == "customer" else "voucher_lht" if u["role"] == "harmoni" else "youtube_check_page" if u["role"] == "admin_pdn" else "dashboard"))
            return fn(*args, **kwargs)
        return wrapped
    return deco

@app.before_request
def _init():
    init_db()
    endpoint = request.endpoint or ""
    # Enforce a rolling inactivity limit for all roles except Super Admin.
    # This check also runs before /login and /logout, preventing stale sessions
    # from being considered authenticated after an idle hour.
    if session.get("user_id") and session.get("role") != "superadmin":
        now_ts = int(time.time())
        last_ts = int(session.get("user_active_ts") or session.get("audit_touch_ts") or now_ts)
        if now_ts - last_ts >= AUDIT_SESSION_IDLE_SECONDS:
            audit_write("TIMEOUT", "Logout otomatis: tidak ada aktivitas selama 60 menit")
            audit_finish()
            session.clear()
            if request.headers.get("X-Requested-With") == "XMLHttpRequest" or request.is_json:
                return jsonify({"ok": False, "message": "Sesi berakhir setelah 60 menit tanpa aktivitas.", "session_expired": True}), 401
            flash("Sesi berakhir setelah 60 menit tanpa aktivitas. Silakan login kembali.", "error")
            return redirect(url_for("login"))
        # Only genuine interactive navigation and actions refresh activity.
        if endpoint not in IDLE_PASSIVE_ENDPOINTS and endpoint not in {"logout", "login"}:
            session["user_active_ts"] = now_ts
    if endpoint in PUBLIC_ENDPOINTS:
        return
    u = current_user()
    if not u:
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok":False,"message":"Sesi login berakhir. Silakan login kembali."}), 401
        return redirect(url_for("login", next=request.path))
    audit_touch()
    if request.method == "POST" and endpoint not in {"logout"}:
        # Generic activity metadata only: never log form values or upload contents.
        audit_write("ACTION", "Aksi: " + endpoint, user=u)
    elif request.method == "GET" and endpoint in {"dashboard", "customer_dashboard", "users_page", "create_post", "create_photo", "bulk_upload", "schedule_page"}:
        # At most one page-view event per endpoint per minute in each browser session.
        stamp_key = "audit_view_" + endpoint
        if int(time.time()) - int(session.get(stamp_key, 0)) >= 60:
            session[stamp_key] = int(time.time())
            audit_write("PAGE_VIEW", "Membuka halaman: " + endpoint, user=u)
    # Harmoni is isolated from every non-voucher application route, including API endpoints.
    if u["role"] == "harmoni" and endpoint not in {"voucher_lht", "voucher_lht_create", "voucher_lht_upload", "voucher_lht_download", "voucher_lht_download_png_a4", "voucher_lht_attachment", "voucher_lht_attachment_pdf", "voucher_lht_edit", "voucher_lht_clear", "voucher_lht_replace", "voucher_lht_png", "voucher_lht_clear_attachment", "voucher_lht_replace_attachment", "logout"}:
        if request.is_json or request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify(ok=False, message="Akses khusus Voucher LHT"), 403
        return redirect(url_for("voucher_lht"))
    # Customers are intentionally confined to the read-only customer dashboard.
    if u["role"] == "customer" and endpoint not in {"customer_dashboard", "customer_account_detail", "customer_issue_refresh", "logout"}:
        return redirect(url_for("customer_dashboard"))

def get_account(open_id):
    c = db()
    a = c.execute("SELECT * FROM accounts WHERE open_id=?", (open_id,)).fetchone()
    c.close()
    return a

def api_headers(token):
    return {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json; charset=UTF-8"
    }

def get_creator_info(account):
    try:
        r = requests.post(
            CREATOR_URL,
            headers=api_headers(account["access_token"]),
            timeout=30
        )
        data = r.json()
        return r, data
    except Exception as e:
        return None, {"data": {}, "error": {"code": "network_error", "message": str(e)}}

@app.get("/account/<open_id>/avatar")
def account_avatar(open_id):
    account = get_account(open_id)
    if not account or not account["avatar_url"]:
        return ("", 404)
    try:
        r = requests.get(
            account["avatar_url"],
            headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/154 Safari/537.36",
                "Referer": "https://www.tiktok.com/",
                "Origin": "https://www.tiktok.com",
                "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
            },
            timeout=20,
        )
        if r.ok and r.content:
            ctype = r.headers.get("Content-Type", "image/jpeg").split(";")[0]
            return Response(r.content, mimetype=ctype)
    except Exception:
        pass
    # Fallback: let the browser request the current CDN URL directly.
    return redirect(account["avatar_url"])

@app.route("/login", methods=["GET", "POST"])
def login():
    if current_user():
        u = current_user()
        return redirect(url_for("customer_dashboard" if u["role"] == "customer" else "voucher_lht" if u["role"] == "harmoni" else "youtube_check_page" if u["role"] == "admin_pdn" else "dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        c = db()
        u = c.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?) AND is_active=1", (username,)).fetchone()
        if u and check_password_hash(u["password_hash"], password):
            session.clear(); session["user_id"] = u["id"]; session["role"] = u["role"]
            sid = secrets.token_urlsafe(24)
            session["audit_session_id"] = sid
            session["audit_username"] = u["username"]
            session["audit_touch_ts"] = int(time.time())
            session["user_active_ts"] = int(time.time())
            try:
                c.execute("""INSERT INTO audit_sessions(id,user_id,username,role,ip_address,user_agent,login_at,last_seen_at,login_device)
                             VALUES(?,?,?,?,?,?,?,?,?)""", (sid,u["id"],u["username"],u["role"],audit_ip(),
                             request.headers.get("User-Agent", "")[:350],audit_now(),audit_now(),audit_device()))
            except Exception:
                app.logger.exception("Could not create audit login session")
            c.execute("UPDATE users SET last_login=? WHERE id=?", (datetime.now().isoformat(), u["id"]))
            c.commit(); c.close()
            audit_write("LOGIN", "Login berhasil", user=u, session_id=sid)
            nxt = request.args.get("next") or request.form.get("next")
            if u["role"] == "customer": return redirect(url_for("customer_dashboard"))
            if u["role"] == "admin_pdn": return redirect(url_for("youtube_check_page"))
            if u["role"] == "harmoni": return redirect(url_for("voucher_lht"))
            return redirect(nxt if nxt and nxt.startswith("/") else url_for("dashboard"))
        c.close(); flash("Username atau password salah.", "error")
    return render_template("login.html")

@app.post("/logout")
def logout():
    if request.form.get("idle_timeout") == "1" and session.get("role") != "superadmin":
        audit_write("TIMEOUT", "Logout otomatis: tidak ada aktivitas selama 60 menit")
    else:
        audit_write("LOGOUT", "Logout manual")
    audit_finish()
    session.clear()
    return redirect(url_for("login"))

def _campaign_date_bounds(keyword):
    """Return UTC epoch bounds for the customer's requested WIB date range."""
    tz = ZoneInfo("Asia/Jakarta")
    start_ts = None
    end_ts = None
    if keyword["period_start"]:
        dt = datetime.strptime(str(keyword["period_start"]), "%Y-%m-%d").replace(tzinfo=tz)
        start_ts = int(dt.timestamp())
    if keyword["period_end"]:
        dt = datetime.strptime(str(keyword["period_end"]), "%Y-%m-%d").replace(tzinfo=tz)
        dt = dt.replace(hour=23, minute=59, second=59)
        end_ts = int(dt.timestamp())
    return start_ts, end_ts

def _normalise_hashtag(value):
    return re.sub(r"[^a-z0-9]", "", (value or "").lower().lstrip("#"))

def _extract_hashtags(value):
    """Split '#Gibran #PolitikIndonesia #MK' into independent normalized tags."""
    tags = re.findall(r"#([\w\d_]+)", value or "", flags=re.UNICODE)
    result = []
    seen = set()
    for tag in tags:
        norm = _normalise_hashtag(tag)
        if norm and norm not in seen:
            seen.add(norm)
            result.append({"label": tag, "norm": norm})
    return result

def _best_hashtag_match(tag_norm, requested_norm):
    if tag_norm == requested_norm:
        return 1.0
    if requested_norm in tag_norm or tag_norm in requested_norm:
        score = min(len(requested_norm), len(tag_norm)) / max(len(requested_norm), len(tag_norm))
        return score if score >= 0.72 else 0.0
    score = difflib.SequenceMatcher(None, requested_norm, tag_norm).ratio()
    return score if score >= 0.78 else 0.0

def _match_requested_hashtags(title, keyword_text):
    """Return every requested hashtag that appears or closely matches a post tag."""
    requested = _extract_hashtags(keyword_text)
    if not requested:
        return [], 0.0
    actual_tags = re.findall(r"#([\w\d_]+)", title or "", flags=re.UNICODE)
    matches = []
    best_score = 0.0
    for req in requested:
        req_best = 0.0
        req_tag = None
        for actual in actual_tags:
            score = _best_hashtag_match(_normalise_hashtag(actual), req["norm"])
            if score > req_best:
                req_best = score
                req_tag = actual
        if req_best:
            matches.append({"requested": req["label"], "matched": req_tag, "score": req_best})
            best_score = max(best_score, req_best)
    return matches, best_score

def _video_create_ts(value):
    try:
        return int(value)
    except Exception:
        try:
            return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0

def _fetch_campaign_posts(keyword):
    """Fetch connected accounts' posts and keep ONLY posts matching any requested hashtag."""
    start_ts, end_ts = _campaign_date_bounds(keyword)
    requested = _extract_hashtags(keyword["keyword"])
    if not requested:
        return []
    c = db()
    accounts = c.execute("SELECT * FROM accounts ORDER BY connected_at DESC").fetchall()
    c.close()
    matched = []
    fields = "id,title,create_time,view_count,like_count,comment_count,share_count,cover_image_url,share_url"
    for account in accounts:
        granted = {x.strip() for x in (account["scope"] or "").split(",") if x.strip()}
        if "video.list" not in granted:
            continue
        try:
            access, _ = _get_valid_access_token(account)
            if not access:
                continue
            cursor = None
            for _ in range(100):
                body = {"max_count": 20}
                if cursor:
                    body["cursor"] = cursor
                r = requests.post(VIDEO_LIST_URL, params={"fields": fields},
                                   headers=api_headers(access), json=body, timeout=30)
                payload = r.json()
                err = payload.get("error", {})
                if not r.ok or err.get("code") not in (None, "", "ok"):
                    break
                data = payload.get("data", {})
                videos = data.get("videos", [])
                if not videos:
                    break
                oldest_ts = None
                for v in videos:
                    ts = _video_create_ts(v.get("create_time"))
                    oldest_ts = ts if oldest_ts is None else min(oldest_ts, ts)
                    if start_ts and ts < start_ts:
                        continue
                    if end_ts and ts > end_ts:
                        continue
                    matches, score = _match_requested_hashtags(v.get("title", ""), keyword["keyword"])
                    if not matches:
                        continue
                    matched.append({
                        "keyword_id": keyword["id"], "open_id": account["open_id"],
                        "display_name": account["display_name"], "username": account["username"],
                        "avatar_url": account["avatar_url"], "is_verified": account["is_verified"],
                        "video_id": v.get("id"), "title": v.get("title") or "(Tanpa judul)", "create_time": ts,
                        "view_count": int(v.get("view_count") or 0), "like_count": int(v.get("like_count") or 0),
                        "comment_count": int(v.get("comment_count") or 0), "share_count": int(v.get("share_count") or 0),
                        "cover_image_url": v.get("cover_image_url"), "share_url": v.get("share_url"),
                        "matched_hashtag": matches[0]["matched"],
                        "matched_hashtags": json.dumps([m["requested"] for m in matches], ensure_ascii=False),
                        "match_score": score
                    })
                if start_ts and oldest_ts and oldest_ts < start_ts:
                    break
                if not data.get("has_more"):
                    break
                cursor = data.get("cursor")
                time.sleep(STATS_API_DELAY)
        except Exception:
            continue
    unique = {}
    for item in matched:
        unique[(item["open_id"], item["video_id"])] = item
    return sorted(unique.values(), key=lambda x: (x["view_count"], x["like_count"], x["comment_count"]), reverse=True)

def _store_campaign_posts(keyword_id, posts):
    c = db()
    c.execute("DELETE FROM customer_campaign_posts WHERE keyword_id=?", (keyword_id,))
    now = datetime.utcnow().isoformat()
    for p in posts:
        c.execute("""INSERT INTO customer_campaign_posts
            (keyword_id,open_id,video_id,title,create_time,view_count,like_count,comment_count,share_count,
             cover_image_url,share_url,matched_hashtag,matched_hashtags,match_score,fetched_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (keyword_id,p["open_id"],p["video_id"],p["title"],p["create_time"],p["view_count"],
             p["like_count"],p["comment_count"],p["share_count"],p.get("cover_image_url"),p.get("share_url"),
             p.get("matched_hashtag"),p.get("matched_hashtags"),p.get("match_score",0),now))
    c.commit(); c.close()

def _load_campaign_posts(keyword_id):
    c = db()
    rows = c.execute("""SELECT p.*,a.display_name,a.username,a.avatar_url,a.is_verified
                       FROM customer_campaign_posts p JOIN accounts a ON a.open_id=p.open_id
                       WHERE p.keyword_id=? ORDER BY p.view_count DESC,p.like_count DESC,p.comment_count DESC""", (keyword_id,)).fetchall()
    c.close()
    return rows

@app.get("/customer")
@role_required("customer")
def customer_dashboard():
    u = current_user(); c = db()
    keywords = c.execute("""SELECT id,keyword,period_start,period_end FROM user_keywords
        WHERE customer_id=? ORDER BY id DESC""", (u["id"],)).fetchall()
    c.close()
    selected_id = request.args.get("keyword_id", "").strip()
    keyword = None
    if selected_id:
        try:
            wanted_id = int(selected_id)
            keyword = next((k for k in keywords if int(k["id"]) == wanted_id), None)
        except ValueError:
            pass
    if keyword is None and keywords:
        keyword = keywords[0]

    posts = _load_campaign_posts(keyword["id"]) if keyword else []
    # Dashboard is read-only and must not block on TikTok API calls.
    # An empty snapshot is valid; customers can explicitly refresh their issue.

    requested_tags = _extract_hashtags(keyword["keyword"]) if keyword else []
    hashtag_summaries = []
    for tag in requested_tags:
        tag_norm = tag["norm"]
        tag_posts = []
        account_ids = set()
        for p in posts:
            raw = p["matched_hashtags"] or "[]"
            try:
                matched_norms = {_normalise_hashtag(x) for x in json.loads(raw)}
            except Exception:
                matched_norms = {_normalise_hashtag(p["matched_hashtag"])}
            if tag_norm in matched_norms:
                tag_posts.append(p); account_ids.add(p["open_id"])
        hashtag_summaries.append({
            "label": tag["label"],
            "accounts_network": len(account_ids),
            "posts_network": len(tag_posts),
            "views_network": sum(int(p["view_count"] or 0) for p in tag_posts),
            "likes_network": sum(int(p["like_count"] or 0) for p in tag_posts),
            "comments_network": sum(int(p["comment_count"] or 0) for p in tag_posts),
            "shares_network": sum(int(p["share_count"] or 0) for p in tag_posts),
        })

    totals = {
        "accounts": len({p["open_id"] for p in posts}),
        "posts": len(posts),
        "views": sum(int(p["view_count"] or 0) for p in posts),
        "likes": sum(int(p["like_count"] or 0) for p in posts),
        "comments": sum(int(p["comment_count"] or 0) for p in posts),
        "shares": sum(int(p["share_count"] or 0) for p in posts),
    }
    # Optional comment module must never prevent the customer dashboard loading.
    sentiment_data = {"count": 0, "counts": {"positif": 0, "negatif": 0, "netral": 0, "campuran": 0}, "items": []}
    try:
        from comment_service import summary
        result = summary(db, [p["video_id"] for p in posts])
        if isinstance(result, dict):
            sentiment_data["count"] = int(result.get("count") or 0)
            sentiment_data["items"] = result.get("items") or []
            counts = result.get("counts") or {}
            if isinstance(counts, dict):
                for label in sentiment_data["counts"]:
                    sentiment_data["counts"][label] = int(counts.get(label) or 0)
    except Exception:
        app.logger.exception("Optional comment sentiment unavailable; customer dashboard remains operational")
    return render_template("customer_dashboard.html", user=u, keywords=keywords, keyword=keyword,
                           requested_tags=requested_tags, posts=posts, totals=totals,
                           hashtag_summaries=hashtag_summaries, external_available=False, sentiment_data=sentiment_data)

@app.post("/customer/issue/<int:keyword_id>/refresh")
@role_required("customer")
def customer_issue_refresh(keyword_id):
    u=current_user(); c=db()
    keyword=c.execute("SELECT id,keyword,period_start,period_end FROM user_keywords WHERE id=? AND customer_id=?",(keyword_id,u["id"])).fetchone()
    c.close()
    if not keyword:
        flash("Issue/Hashtag tidak ditemukan.","error")
        return redirect(url_for("customer_dashboard"))
    posts=_fetch_campaign_posts(keyword)
    _store_campaign_posts(keyword_id,posts)
    flash(f"Monitoring {keyword['keyword']} diperbarui: {len(posts)} postingan cocok.","success")
    return redirect(url_for("customer_dashboard"))

@app.get("/customer/account/<open_id>")
@role_required("customer")
def customer_account_detail(open_id):
    c = db()
    account = c.execute("""SELECT open_id,display_name,username,avatar_url,category,view_count,likes_count,comment_count,
        follower_count,following_count,is_verified,stats_updated_at,scope FROM accounts WHERE open_id=?""", (open_id,)).fetchone()
    c.close()
    if not account:
        flash("Akun tidak ditemukan.", "error")
        return redirect(url_for("customer_dashboard"))
    videos=[]; video_error=None
    granted={x.strip() for x in (account["scope"] or "").split(",") if x.strip()}
    if "video.list" in granted:
        try:
            access, token_error = _get_valid_access_token(account)
            if access:
                cursor=None
                for _ in range(50):
                    body={"max_count":20}
                    if cursor: body["cursor"]=cursor
                    r=requests.post(VIDEO_LIST_URL, params={"fields":"id,title,create_time,view_count,like_count,comment_count,share_count,cover_image_url,share_url"}, headers=api_headers(access), json=body, timeout=30)
                    payload=r.json(); err=payload.get("error",{})
                    if not r.ok or err.get("code") not in (None,"","ok"):
                        video_error=err.get("message") or err.get("code") or "Gagal membaca video"; break
                    data=payload.get("data",{}); videos.extend(data.get("videos",[]))
                    if not data.get("has_more"): break
                    cursor=data.get("cursor")
        except Exception as e:
            video_error=str(e)
    return render_template("customer_account_detail.html", user=current_user(), account=account, videos=videos, video_error=video_error)

@app.post("/customer/keywords")
@role_required("superadmin", "admin_medsos")
def customer_keyword_add():
    customer_id=request.form.get("customer_id")
    keyword=request.form.get("keyword","").strip()
    period_start=request.form.get("period_start","").strip() or None
    period_end=request.form.get("period_end","").strip() or None
    if not customer_id or not keyword:
        flash("Customer dan Issue/Hashtag wajib diisi.","error")
        return redirect(url_for("users_page"))
    c=db()
    user=c.execute("SELECT id,role FROM users WHERE id=? AND is_active=1",(customer_id,)).fetchone()
    if not user or user["role"]!="customer":
        c.close(); flash("Customer tidak valid.","error"); return redirect(url_for("users_page"))
    c.execute("INSERT INTO user_keywords(customer_id,keyword,period_start,period_end,created_at) VALUES(?,?,?,?,?)",(customer_id,keyword,period_start,period_end,datetime.now().isoformat()))
    c.commit(); c.close(); flash("Issue/Hashtag customer berhasil ditambahkan.","success")
    return redirect(url_for("users_page"))

@app.post("/customer/keywords/<int:keyword_id>/delete")
@role_required("superadmin", "admin_medsos")
def customer_keyword_delete(keyword_id):
    c=db(); c.execute("DELETE FROM user_keywords WHERE id=?",(keyword_id,)); c.commit(); c.close()
    flash("Issue/Hashtag dihapus.","success"); return redirect(url_for("users_page"))

@app.get("/audit-log")
@role_required("superadmin")
def audit_log():
    from datetime import timedelta
    try:
        page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        page = 1
    try:
        days = min(90, max(1, int(request.args.get("days", "7"))))
    except ValueError:
        days = 7
    username = request.args.get("username", "").strip()[:80]
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat(timespec="seconds")
    c = db()
    clause = " WHERE created_at>=?"
    args = [cutoff]
    if username:
        clause += " AND LOWER(username) LIKE LOWER(?)"
        args.append("%" + username + "%")
    total = c.execute("SELECT COUNT(*) AS n FROM audit_events" + clause, args).fetchone()["n"]
    events = c.execute("SELECT * FROM audit_events" + clause + " ORDER BY created_at DESC,id DESC LIMIT 100 OFFSET ?", args + [(page-1)*100]).fetchall()
    sessions = c.execute("SELECT * FROM audit_sessions WHERE login_at>=? ORDER BY login_at DESC LIMIT 100", (cutoff,)).fetchall()
    c.close()
    return render_template("audit_log.html", events=events, sessions=sessions, total=total,
                           page=page, days=days, username=username, idle_seconds=AUDIT_SESSION_IDLE_SECONDS)

# Roles created here are stored, but do not gain route permissions automatically.
BUILTIN_USER_ROLES = ("superadmin", "supervisor_caprof", "admin_medsos", "supervisor_pdn", "admin_pdn", "customer", "harmoni")
ROLE_NAMES = {"superadmin":"Super Admin", "supervisor_caprof":"Supervisor Caprof", "admin_medsos":"Admin Medsos", "supervisor_pdn":"Supervisor PDN", "admin_pdn":"Admin PDN", "customer":"Customer", "harmoni":"Harmoni"}

def available_user_roles(c):
    c.execute("CREATE TABLE IF NOT EXISTS custom_user_roles (slug TEXT PRIMARY KEY, label TEXT NOT NULL)")
    custom = c.execute("SELECT slug,label FROM custom_user_roles ORDER BY label").fetchall()
    return [(slug, ROLE_NAMES[slug]) for slug in BUILTIN_USER_ROLES] + [(r["slug"],r["label"]) for r in custom if r["slug"] not in BUILTIN_USER_ROLES]

@app.post("/users/roles/add")
@role_required("superadmin")
def add_user_role():
    import re
    label = request.form.get("role_label", "").strip()
    slug = re.sub(r"[^a-z0-9_]+", "_", label.lower()).strip("_")
    if not label or len(label)>60 or not slug or slug in BUILTIN_USER_ROLES:
        flash("Nama role tidak valid atau sudah ada.", "error")
        return redirect(url_for("users_page"))
    c=db()
    try:
        c.execute("CREATE TABLE IF NOT EXISTS custom_user_roles (slug TEXT PRIMARY KEY, label TEXT NOT NULL)")
        c.execute("INSERT INTO custom_user_roles(slug,label) VALUES(?,?)", (slug,label))
        c.commit(); flash("Role berhasil dibuat. Akses modul belum diberikan otomatis; perlu konfigurasi izin terpisah.","success")
    except Exception:
        c.conn.rollback(); flash("Role sudah ada atau tidak dapat dibuat.","error")
    finally: c.close()
    return redirect(url_for("users_page"))

@app.post("/users/<int:user_id>/delete")
@role_required("superadmin")
def delete_user(user_id):
    actor=current_user()
    if actor and int(actor["id"])==user_id:
        flash("Akun yang sedang digunakan tidak boleh dihapus.","error")
        return redirect(url_for("users_page"))
    c=db()
    try:
        target=c.execute("SELECT id,role FROM users WHERE id=?",(user_id,)).fetchone()
        if not target:
            flash("User tidak ditemukan.","error")
        elif target["role"]=="superadmin":
            flash("Akun Super Admin tidak dapat dihapus dari halaman ini.","error")
        else:
            # Soft-delete preserves account ownership, audit history and related data.
            c.execute("UPDATE users SET is_active=0 WHERE id=?",(user_id,))
            c.commit(); flash("User dinonaktifkan (soft delete). Data terkait tetap tersimpan.","success")
    finally: c.close()
    return redirect(url_for("users_page"))

@app.route("/users", methods=["GET", "POST"])
@role_required("superadmin")
def users_page():
    c=db()
    if request.method == "POST":
        username=request.form.get("username","").strip(); full_name=request.form.get("full_name","").strip(); email=request.form.get("email","").strip(); role=request.form.get("role","customer").strip(); password=request.form.get("password","")
        if role not in {r[0] for r in available_user_roles(c)} or not username or not password:
            flash("Lengkapi username, password, dan role.", "error")
        else:
            try:
                c.execute("INSERT INTO users(username,full_name,email,password_hash,role,is_active,created_at) VALUES(?,?,?,?,?,?,?)", (username,full_name,email,generate_password_hash(password),role,1,datetime.now().isoformat()))
                c.commit(); flash("User berhasil dibuat.", "success")
            except Exception as e:
                c.conn.rollback(); flash("Gagal membuat user: username mungkin sudah dipakai.", "error")
    roles=available_user_roles(c)
    users=c.execute("""SELECT id,username,full_name,email,role,is_active,created_at,last_login FROM users
        ORDER BY CASE role WHEN 'superadmin' THEN 1 WHEN 'supervisor_caprof' THEN 2
        WHEN 'admin_medsos' THEN 3 WHEN 'supervisor_pdn' THEN 4 WHEN 'admin_pdn' THEN 5
        WHEN 'customer' THEN 6 ELSE 7 END, LOWER(username), id""").fetchall()
    customer_keywords=c.execute("SELECT id,customer_id,keyword,period_start,period_end FROM user_keywords ORDER BY id DESC").fetchall()
    c.close()
    return render_template("users.html", users=users, roles=roles, role_names=dict(roles), customer_keywords=customer_keywords)

@app.post("/users/<int:user_id>/update")
@role_required("superadmin")
def update_user(user_id):
    role=request.form.get("role","customer")
    active=1 if request.form.get("is_active")=="1" else 0
    full_name=request.form.get("full_name","").strip()
    email=request.form.get("email","").strip()
    password=request.form.get("password","")
    username=request.form.get("username","").strip()
    c=db()
    try:
        old=c.execute("SELECT id,username,role FROM users WHERE id=?",(user_id,)).fetchone()
        if not old: return "User tidak ditemukan",404
        if role not in {r[0] for r in available_user_roles(c)}: return "Role tidak valid",400
        if not username or len(username)>100 or any(ch.isspace() for ch in username):
            flash("Username wajib diisi dan tidak boleh mengandung spasi.","error")
            return redirect(url_for("users_page"))
        if int(current_user()["id"])==user_id and (role!="superadmin" or not active):
            flash("Tidak dapat menonaktifkan atau mengganti role Super Admin yang sedang login.","error")
            return redirect(url_for("users_page"))
        if old["role"]=="superadmin" and role!="superadmin":
            flash("Role akun Super Admin tidak dapat diturunkan melalui halaman ini.","error")
            return redirect(url_for("users_page"))
        if password:
            c.execute("UPDATE users SET username=?,full_name=?,email=?,role=?,is_active=?,password_hash=? WHERE id=?",(username,full_name,email,role,active,generate_password_hash(password),user_id))
        else:
            c.execute("UPDATE users SET username=?,full_name=?,email=?,role=?,is_active=? WHERE id=?",(username,full_name,email,role,active,user_id))
        c.commit(); flash("User berhasil diperbarui.","success")
    except Exception:
        c.conn.rollback(); flash("Gagal menyimpan. Periksa apakah username sudah digunakan.","error")
    finally: c.close()
    return redirect(url_for("users_page"))

# Engagement connection setup UI only; no remote commands or credential storage.
@app.get("/engagement/<method>")
@role_required("superadmin", "admin_medsos")
def engagement_page(method):
    if method not in {"server", "phonefarm"}:
        abort(404)
    if method == "phonefarm":
        return redirect(url_for("phonefarm_page"))
    return render_template("engagement.html", user=current_user(), method=method)

@app.get("/engagement/<method>/<metric>")
@role_required("superadmin", "admin_medsos")
def engagement_legacy_page(method, metric):
    if method not in {"server", "phonefarm"} or metric not in {"views", "likes", "comments", "shares", "followers"}:
        abort(404)
    return redirect(url_for("engagement_page", method=method))

@app.get("/terms")
def terms_page():
    return render_template("terms.html")


@app.get("/privacy")
def privacy_page():
    return render_template("privacy.html")


@app.get("/")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def dashboard():
    try: page=max(int(request.args.get("page",1)),1)
    except (TypeError,ValueError): page=1
    try: per_page=min(max(int(request.args.get("per_page",STATS_PAGE_SIZE)),10),100)
    except (TypeError,ValueError): per_page=STATS_PAGE_SIZE

    c=db(); u=current_user()
    # Counts for sidebar tiers: all accounts visible to the current user, independent of selected tier.
    tier_scope=""
    tier_scope_params=()
    if u["role"]=="admin_medsos":
        tier_scope=" WHERE (assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))"
        tier_scope_params=(u["id"],u["full_name"] or u["username"])
    tier_rows=c.execute("SELECT COALESCE(follower_count,0) AS followers, LOWER(TRIM(COALESCE(category,''))) AS category_key, COUNT(*) AS n FROM accounts"+tier_scope+" GROUP BY COALESCE(follower_count,0), LOWER(TRIM(COALESCE(category,'')))",tier_scope_params).fetchall()
    tier_counts={"babon":0,"medium":0,"minor":0,"biasa":0,"special_zawiyah":0,"special_bisnis":0}
    for tr in tier_rows:
        n=int(tr["n"])
        followers=int(tr["followers"] or 0)
        key="special_zawiyah" if tr["category_key"]=="zawiyah" else "special_bisnis" if tr["category_key"] in ("bisnis","business") else "babon" if followers>=2000 else "medium" if followers>=1000 else "minor" if followers>=500 else "biasa"
        tier_counts[key]+=n
    tier_total=sum(tier_counts.values())
    conditions=[]; params=[]
    # Admin Medsos only sees accounts assigned to that login. Super Admin sees all.
    if u["role"]=="admin_medsos":
        conditions.append("(assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))")
        params.extend([u["id"], u["full_name"] or u["username"]])

    admin_filter=request.args.get("admin_filter", "").strip()
    if u["role"] in ("superadmin", "supervisor_caprof") and admin_filter:
        try:
            admin_id=int(admin_filter)
            conditions.append("assigned_admin_id=?")
            params.append(admin_id)
        except ValueError:
            conditions.append("LOWER(TRIM(admin_medsos))=LOWER(TRIM(?))")
            params.append(admin_filter)

    # Account filters are applied BEFORE LIMIT/OFFSET so counts and sorting cover all matching accounts.
    search_query=request.args.get("q", "").strip()[:150]
    category_filter=request.args.get("category_filter", "").strip()[:150]
    status_filter=request.args.get("status_filter", "").strip()
    follower_tier=request.args.get("follower_tier", "").strip().lower()
    if follower_tier in ("babon", "medium", "minor", "biasa", "special_zawiyah", "special_bisnis"):
        normal_category="LOWER(TRIM(COALESCE(category,''))) NOT IN ('zawiyah','bisnis','business')"
        tier_conditions={"babon":normal_category+" AND COALESCE(follower_count,0)>=2000", "medium":normal_category+" AND COALESCE(follower_count,0) BETWEEN 1000 AND 1999", "minor":normal_category+" AND COALESCE(follower_count,0) BETWEEN 500 AND 999", "biasa":normal_category+" AND COALESCE(follower_count,0) BETWEEN 0 AND 499", "special_zawiyah":"LOWER(TRIM(COALESCE(category,'')))='zawiyah'", "special_bisnis":"LOWER(TRIM(COALESCE(category,''))) IN ('bisnis','business')"}
        conditions.append(tier_conditions[follower_tier])
    else:
        follower_tier=""
    type_filter=request.args.get("type_filter", "").strip()
    sort_by=request.args.get("sort_by", "views")
    sort_direction=request.args.get("sort_direction", "desc")
    if search_query:
        conditions.append("(LOWER(COALESCE(username,'')) LIKE ? OR LOWER(COALESCE(email,'')) LIKE ? OR LOWER(COALESCE(display_name,'')) LIKE ?)")
        search_pattern="%"+search_query.lstrip('@').lower()+"%"
        params.extend([search_pattern]*3)
    if category_filter and admin_filter:
        # Validate category for selected admin before applying it to the database query.
        check_where=" WHERE "+" AND ".join(conditions+["COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori')=?"])
        found=c.execute("SELECT 1 FROM accounts"+check_where+" LIMIT 1",tuple(params+[category_filter])).fetchone()
        if not found: category_filter=""
    if category_filter:
        conditions.append("COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori')=?")
        params.append(category_filter)
    if status_filter=="verified": conditions.append("COALESCE(is_verified,0)<>0")
    elif status_filter=="unverified": conditions.append("COALESCE(is_verified,0)=0")
    # TikTok Login Kit does not expose business/personal type. Never mislabel categories as account type.
    if type_filter=="unknown": pass
    elif type_filter in ("business","personal"): conditions.append("1=0")
    sort_columns={"posts":"video_count","views":"view_count","likes":"likes_count","comments":"comment_count","shared":"shares_count","followers":"follower_count","following":"following_count","connected":"connected_at"}
    sort_by=sort_by if sort_by in sort_columns else "views"
    sort_direction=sort_direction if sort_direction in ("asc","desc") else "desc"
    sort_sql="COALESCE("+sort_columns[sort_by]+",0)" if sort_by!="connected" else "connected_at"
    sort_sql+=" "+sort_direction.upper()+", connected_at DESC"
    scope_where=(" WHERE " + " AND ".join(conditions)) if conditions else ""
    total=int(c.execute("SELECT COUNT(*) AS n FROM accounts"+scope_where,tuple(params)).fetchone()["n"])

    admin_performance=c.execute("""SELECT
        COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan') AS admin_medsos,
        COUNT(*) AS account_count,
        COALESCE(SUM(video_count),0) AS post_count,
        CASE WHEN COUNT(repost_count)=COUNT(*) THEN SUM(repost_count) ELSE NULL END AS total_reposts,
        COALESCE(SUM(view_count),0) AS total_views,
        COALESCE(SUM(likes_count),0) AS total_likes,
        COALESCE(SUM(comment_count),0) AS total_comments,
        COALESCE(SUM(shares_count),0) AS total_shares,
        COALESCE(SUM(follower_count),0) AS total_followers,
        COALESCE(SUM(following_count),0) AS total_following,
        COALESCE(SUM(CASE WHEN COALESCE(is_verified,0)<>0 THEN 1 ELSE 0 END),0) AS verified_accounts,
        COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(category,''))) IN ('bisnis','business') THEN 1 ELSE 0 END),0) AS business_category_accounts
        FROM accounts """+scope_where+
        " GROUP BY COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan')"
        " ORDER BY COALESCE(SUM(view_count),0) DESC,COALESCE(SUM(likes_count),0) DESC,COUNT(*) DESC",
        tuple(params)).fetchall()

    category_rows=c.execute("SELECT COALESCE(NULLIF(TRIM(category),''),'Lainnya') AS category_name, COUNT(*) AS n FROM accounts"+scope_where+" GROUP BY COALESCE(NULLIF(TRIM(category),''),'Lainnya') ORDER BY n DESC, category_name",tuple(params)).fetchall()
    category_counts=[{"name":r["category_name"],"count":int(r["n"])} for r in category_rows]
    account_status=c.execute("SELECT COUNT(*) AS total, COALESCE(SUM(CASE WHEN COALESCE(is_verified,0)<>0 THEN 1 ELSE 0 END),0) AS verified, COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(category,''))) IN ('bisnis','business') THEN 1 ELSE 0 END),0) AS business FROM accounts"+scope_where,tuple(params)).fetchone()
    # Statistik kategori per admin: gunakan filter dan cakupan akses yang sama dengan total admin.
    category_performance_rows=c.execute("""SELECT
        COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan') AS admin_medsos,
        COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori') AS category_name,
        COUNT(*) AS account_count,
        COALESCE(SUM(video_count),0) AS post_count,
        CASE WHEN COUNT(repost_count)=COUNT(*) THEN SUM(repost_count) ELSE NULL END AS total_reposts,
        COALESCE(SUM(view_count),0) AS total_views,
        COALESCE(SUM(likes_count),0) AS total_likes,
        COALESCE(SUM(comment_count),0) AS total_comments,
        COALESCE(SUM(shares_count),0) AS total_shares,
        COALESCE(SUM(follower_count),0) AS total_followers,
        COALESCE(SUM(following_count),0) AS total_following,
        COALESCE(SUM(CASE WHEN LOWER(TRIM(COALESCE(category,''))) IN ('bisnis','business') THEN 1 ELSE 0 END),0) AS business_category_accounts,
        COALESCE(SUM(CASE WHEN COALESCE(is_verified,0)<>0 THEN 1 ELSE 0 END),0) AS verified_accounts
        FROM accounts """+scope_where+
        " GROUP BY COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan'), COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori')"
        " ORDER BY COALESCE(SUM(view_count),0) DESC, COUNT(*) DESC, category_name",
        tuple(params)).fetchall()
    category_performance={}
    for r in category_performance_rows:
        category_performance.setdefault(r['admin_medsos'], []).append(dict(r))

    # Distinct category options must use permission scope, not current pagination or category selection.
    base_conditions=[]; base_params=[]
    if u["role"]=="admin_medsos":
        base_conditions.append("(assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))")
        base_params.extend([u["id"],u["full_name"] or u["username"]])
    base_where=(" WHERE "+" AND ".join(base_conditions)) if base_conditions else ""
    # Category options depend on the selected Admin Medsos (all matching accounts, no pagination).
    cat_scope=" WHERE "+" AND ".join(base_conditions) if base_conditions else ""
    cat_rows=c.execute("SELECT COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori') AS cat, assigned_admin_id, admin_medsos FROM accounts"+cat_scope,tuple(base_params)).fetchall()
    filter_categories=sorted({r["cat"] for r in cat_rows})
    # Build a per-admin lookup to update category choices immediately in the browser.
    admin_category_map={}
    admin_records=c.execute("SELECT id,full_name,username FROM users WHERE role='admin_medsos' AND is_active=1").fetchall()
    for adm in admin_records:
        admin_category_map[str(adm["id"])]=sorted({r["cat"] for r in cat_rows if str(r["assigned_admin_id"] or '')==str(adm["id"]) or (r["admin_medsos"] or '').strip().lower()==(adm["full_name"] or adm["username"] or '').strip().lower()})
    admin_category_map["Belum ditugaskan"]=sorted({r["cat"] for r in cat_rows if not r["assigned_admin_id"] and not (r["admin_medsos"] or '').strip()})
    if admin_filter:
        filter_categories=admin_category_map.get(str(admin_filter),[])
        if category_filter and category_filter not in filter_categories:
            category_filter=""
    total_pages=max((total+per_page-1)//per_page,1); page=min(page,total_pages); offset=(page-1)*per_page
    accounts=c.execute("""SELECT open_id,display_name,avatar_url,scope,connected_at,email,username,video_count,following_count,
        follower_count,likes_count,view_count,comment_count,shares_count,repost_count,stats_updated_at,category,admin_medsos,
        assigned_admin_id,profile_deep_link,is_verified FROM accounts """+scope_where+
        " ORDER BY "+sort_sql+" LIMIT ? OFFSET ?",
        tuple(params)+(per_page,offset)).fetchall()
    admins=c.execute("SELECT id,full_name,username FROM users WHERE role='admin_medsos' AND is_active=1 ORDER BY full_name,username").fetchall()
    c.close()
    if "bc_csrf" not in session:
        session["bc_csrf"] = secrets.token_urlsafe(24)
    return render_template("dashboard.html", follower_tier=follower_tier,tier_counts=tier_counts,tier_total=tier_total,accounts=accounts,admin_performance=admin_performance,category_performance=category_performance,page=page,per_page=per_page,
                           total_accounts=total,total_pages=total_pages,admins=admins,user=u,bc_csrf=session["bc_csrf"],admin_filter=admin_filter,category_counts=category_counts,account_status=account_status,filter_categories=filter_categories,admin_category_map=admin_category_map,all_filter_categories=sorted({r["cat"] for r in cat_rows}),search_query=search_query,category_filter=category_filter,status_filter=status_filter,type_filter=type_filter,sort_by=sort_by,sort_direction=sort_direction)

@app.get("/api/business-centres/<int:centre>/accounts")
@role_required("superadmin", "admin_medsos")
def business_centre_accounts(centre):
    if centre not in (1, 2): abort(404)
    u=current_user(); c=db()
    allowed="LOWER(TRIM(COALESCE(a.category,''))) IN ('zawiyah','special zawiyah','special_zawiyah','bisnis','business','special bisnis','special_bisnis')"
    params=[]
    if u['role']=='admin_medsos':
        allowed+=" AND (a.assigned_admin_id=? OR LOWER(TRIM(a.admin_medsos))=LOWER(TRIM(?)))"
        params.extend([u['id'],u['full_name'] or u['username']])
    rows=c.execute("""SELECT a.open_id,a.username,a.display_name,a.category,a.email,
        CASE WHEN b.open_id IS NULL THEN 0 ELSE 1 END AS selected
        FROM accounts a LEFT JOIN business_centre_accounts b
        ON b.open_id=a.open_id AND b.centre_number=?
        WHERE """+allowed+" ORDER BY a.category,a.username,a.display_name",tuple([centre]+params)).fetchall()
    c.close()
    return jsonify({'ok':True,'accounts':[dict(r) for r in rows]})

@app.post("/api/business-centres/<int:centre>/accounts")
@role_required("superadmin", "admin_medsos")
def business_centre_account_update(centre):
    if centre not in (1,2): abort(404)
    if not secrets.compare_digest(request.headers.get("X-BC-CSRF", ""), session.get("bc_csrf", "")):
        return jsonify({"ok":False,"message":"Sesi keamanan tidak valid. Muat ulang halaman."}),403
    data=request.get_json(silent=True) or {}
    open_id=str(data.get('open_id') or '').strip()
    if not open_id or len(open_id)>512: return jsonify({'ok':False,'message':'Akun tidak valid.'}),400
    selected=data.get('selected')
    if not isinstance(selected,bool): return jsonify({'ok':False,'message':'Status tidak valid.'}),400
    u=current_user();c=db()
    sql="SELECT open_id FROM accounts WHERE open_id=? AND LOWER(TRIM(COALESCE(category,''))) IN ('zawiyah','special zawiyah','special_zawiyah','bisnis','business','special bisnis','special_bisnis')"
    params=[open_id]
    if u['role']=='admin_medsos':
        sql+=" AND (assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))"
        params.extend([u['id'],u['full_name'] or u['username']])
    if not c.execute(sql,tuple(params)).fetchone():
        c.close();return jsonify({'ok':False,'message':'Akun tidak ditemukan atau tidak diizinkan.'}),403
    if selected:
        if c.is_pg:
            c.execute("INSERT INTO business_centre_accounts(centre_number,open_id,added_by,added_at) VALUES(?,?,?,?) ON CONFLICT(centre_number,open_id) DO NOTHING",(centre,open_id,u['id'],datetime.utcnow().isoformat()))
        else:
            c.execute("INSERT OR IGNORE INTO business_centre_accounts(centre_number,open_id,added_by,added_at) VALUES(?,?,?,?)",(centre,open_id,u['id'],datetime.utcnow().isoformat()))
    else:
        c.execute("DELETE FROM business_centre_accounts WHERE centre_number=? AND open_id=?",(centre,open_id))
    c.commit();c.close()
    return jsonify({'ok':True,'selected':selected})

@app.get("/admin/posts")
@role_required("superadmin", "admin_medsos")
def admin_posts():
    """On-demand post list. No background TikTok API calls or customer hashtag filtering."""
    u = current_user()
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (ValueError, TypeError):
        page = 1
    # Server-side filters: restrict account selection and video upload dates.
    admin_filter = request.args.get("admin", "").strip()
    start_text = request.args.get("start", "").strip()
    end_text = request.args.get("end", "").strip()
    try:
        start_date = datetime.strptime(start_text, "%Y-%m-%d").date() if start_text else None
        end_date = datetime.strptime(end_text, "%Y-%m-%d").date() if end_text else None
    except ValueError:
        return {"ok": False, "message": "Format tanggal harus YYYY-MM-DD."}, 400
    if start_date and end_date and start_date > end_date:
        return {"ok": False, "message": "Tanggal mulai melewati tanggal akhir."}, 400
    start_ts = int(datetime.combine(start_date, datetime.min.time()).replace(tzinfo=ZoneInfo("Asia/Jakarta")).timestamp()) if start_date else None
    end_ts = int(datetime.combine(end_date, datetime.max.time()).replace(tzinfo=ZoneInfo("Asia/Jakarta")).timestamp()) if end_date else None
    batch_size = 10  # Avoid 1,000 simultaneous TikTok requests.
    c = db()
    where = ""
    params = []
    if u["role"] == "admin_medsos":
        where = " WHERE (assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))"
        params = [u["id"], u["full_name"] or u["username"]]
    if admin_filter:
        where += (" AND " if where else " WHERE ") + "(CAST(assigned_admin_id AS TEXT)=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))"
        params.extend([admin_filter, admin_filter])
    total = int(c.execute("SELECT COUNT(*) AS n FROM accounts" + where, tuple(params)).fetchone()["n"])
    total_posts_all_accounts = int(c.execute("SELECT COALESCE(SUM(video_count), 0) AS n FROM accounts" + where, tuple(params)).fetchone()["n"])
    accounts = c.execute("SELECT * FROM accounts" + where + " ORDER BY COALESCE(view_count,0) DESC LIMIT ? OFFSET ?", tuple(params) + (batch_size, (page-1)*batch_size)).fetchall()
    c.close()
    posts = []
    errors = []
    for a in accounts:
        if "video.list" not in {v.strip() for v in (a["scope"] or "").split(",")}:
            errors.append((a["display_name"], "Scope video.list tidak tersedia"))
            continue
        try:
            token, err = _get_valid_access_token(a)
            if not token:
                errors.append((a["display_name"], err or "Token tidak tersedia"))
                continue
            r = requests.post(VIDEO_LIST_URL, params={"fields":"id,title,create_time,view_count,like_count,comment_count,share_count,share_url"}, headers=api_headers(token), json={"max_count":20}, timeout=20)
            payload = r.json()
            if not r.ok or payload.get("error", {}).get("code") not in (None, "", "ok"):
                errors.append((a["display_name"], payload.get("error", {}).get("message") or "TikTok API error"))
                continue
            for v in payload.get("data", {}).get("videos", []):
                try:
                    ts = int(v.get("create_time") or 0)
                    if ts > 1000000000000: ts //= 1000
                except (ValueError, TypeError):
                    ts = 0
                if start_ts is not None and ts < start_ts: continue
                if end_ts is not None and ts > end_ts: continue
                title = v.get("title") or ""
                posts.append({"video_id":v.get("id"), "title":title, "create_time":v.get("create_time"),
                              "view_count":int(v.get("view_count") or 0), "like_count":int(v.get("like_count") or 0),
                              "comment_count":int(v.get("comment_count") or 0), "share_count":int(v.get("share_count") or 0),
                              "share_url":v.get("share_url"), "display_name":a["display_name"],
                              "username":a["username"], "admin_medsos":a["admin_medsos"] or "Belum ditugaskan", "category":a["category"] or "Belum dikategorikan", "hashtags":list(dict.fromkeys(re.findall(r"#[\w]+", title)))})
        except Exception:
            app.logger.exception("Admin post list: failed to fetch posts for an account")
            errors.append((a["display_name"], "Gagal mengambil postingan"))
    posts.sort(key=lambda p:(p["view_count"],p["like_count"]), reverse=True)
    return {"ok":True,"posts":posts,"page":page,"total_accounts":total,"total_posts_all_accounts":total_posts_all_accounts,"has_more":page*batch_size<total,
            "scanned_accounts":len(accounts),"errors":[{"account":a,"message":m} for a,m in errors]}

@app.post("/account/<open_id>/details")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def account_details(open_id):
    u = current_user()
    email = request.form.get("email", "").strip()
    category = request.form.get("category", "").strip()
    c = db()
    account = c.execute("SELECT * FROM accounts WHERE open_id=?", (open_id,)).fetchone()
    if not account:
        c.close()
        if request.headers.get("X-Requested-With") == "XMLHttpRequest":
            return jsonify({"ok":False,"message":"Akun tidak ditemukan."}), 404
        flash("Akun tidak ditemukan.", "error"); return redirect(url_for("dashboard"))
    if u["role"] == "admin_medsos":
        # Ownership is set on OAuth connection, not on editing. Never let another admin claim an account.
        if account["assigned_admin_id"] != u["id"]:
            c.close()
            if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                return jsonify({"ok":False,"message":"Akun ini bukan tanggung jawab Anda."}), 403
            flash("Akun ini bukan tanggung jawab Anda.", "error"); return redirect(url_for("dashboard"))
        c.execute("UPDATE accounts SET email=?,category=? WHERE open_id=?", (email, category, open_id))
    else:
        username = request.form.get("username", "").strip().lstrip("@")
        selected = request.form.get("assigned_admin_id", "").strip()
        if selected:
            try: admin_id = int(selected)
            except ValueError:
                c.close()
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok":False,"message":"Admin tidak valid."}), 400
                flash("Admin tidak valid.", "error"); return redirect(url_for("dashboard"))
            admin = c.execute("SELECT id,full_name,username FROM users WHERE id=? AND role='admin_medsos' AND is_active=1", (admin_id,)).fetchone()
            if not admin:
                c.close()
                if request.headers.get("X-Requested-With") == "XMLHttpRequest":
                    return jsonify({"ok":False,"message":"Admin Medsos tidak ditemukan."}), 400
                flash("Admin Medsos tidak ditemukan.", "error"); return redirect(url_for("dashboard"))
            owner_name = admin["full_name"] or admin["username"]
        else:
            admin_id = None
            owner_name = ""
        c.execute("UPDATE accounts SET email=?,username=?,category=?,admin_medsos=?,assigned_admin_id=? WHERE open_id=?",
                  (email, username, category, owner_name, admin_id, open_id))
    c.commit(); c.close()
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return jsonify({"ok": True, "message": "Informasi akun berhasil disimpan.",
                        "email": email, "category": category,
                        "username": username if u["role"] in ("superadmin", "supervisor_caprof") else account["username"],
                        "admin_name": owner_name if u["role"] in ("superadmin", "supervisor_caprof") else (account["admin_medsos"] or ""),
                        "assigned_admin_id": admin_id if u["role"] in ("superadmin", "supervisor_caprof") else account["assigned_admin_id"]})
    # Permintaan form biasa tetap kembali ke halaman pagination asal.
    next_url = request.form.get("next", "")
    if next_url.startswith("/") and not next_url.startswith("//") and "\\" not in next_url:
        return redirect(next_url)
    return redirect(url_for("dashboard"))


def _update_account_tokens(open_id, token_data):
    """Persist rotated TikTok access/refresh tokens."""
    c = db()
    c.execute("""UPDATE accounts SET
        access_token=?,
        refresh_token=?,
        scope=?,
        expires_in=?
        WHERE open_id=?""", (
        token_data.get("access_token", ""),
        token_data.get("refresh_token", ""),
        token_data.get("scope", ""),
        token_data.get("expires_in", 0),
        open_id
    ))
    c.commit()
    c.close()


def refresh_tiktok_access_token(account):
    """Refresh an expired/invalid access token using TikTok OAuth v2."""
    refresh_token = (account["refresh_token"] or "").strip()
    if not refresh_token:
        return None, "refresh_token tidak tersedia; akun perlu Connect ulang."

    try:
        r = requests.post(
            TOKEN_URL,
            data={
                "client_key": CLIENT_KEY,
                "client_secret": CLIENT_SECRET,
                "grant_type": "refresh_token",
                "refresh_token": refresh_token,
            },
            headers={
                "Content-Type": "application/x-www-form-urlencoded",
                "Cache-Control": "no-cache",
            },
            timeout=30,
        )
        data = r.json()
    except Exception as e:
        return None, f"gagal refresh token: {e}"

    if not r.ok or not data.get("access_token"):
        desc = data.get("error_description") or data.get("error") or str(data)
        return None, f"refresh token ditolak: {desc}"

    _update_account_tokens(account["open_id"], data)
    return data["access_token"], None


def _fetch_user_profile(access_token):
    fields = (
        "open_id,display_name,avatar_url,avatar_url_100,avatar_large_url,"
        "follower_count,following_count,likes_count,video_count,username,"
        "profile_deep_link,is_verified"
    )
    r = requests.get(
        USER_URL,
        params={"fields": fields},
        headers={"Authorization": f"Bearer {access_token}"},
        timeout=30,
    )
    payload = r.json()
    return r, payload


def _save_profile(open_id, user):
    """Save profile/avatar returned by TikTok."""
    avatar = (
        user.get("avatar_large_url")
        or user.get("avatar_url_100")
        or user.get("avatar_url")
        or ""
    )
    c = db()
    c.execute("""UPDATE accounts SET
        display_name=?,
        avatar_url=?,
        username=?,
        profile_deep_link=?,
        video_count=?,
        following_count=?,
        follower_count=?,
        likes_count=?,
        is_verified=?
        WHERE open_id=?""", (
        user.get("display_name") or "",
        avatar,
        user.get("username") or "",
        user.get("profile_deep_link") or "",
        user.get("video_count"),
        user.get("following_count"),
        user.get("follower_count"),
        user.get("likes_count"),
        1 if user.get("is_verified") else 0,
        open_id
    ))
    c.commit()
    c.close()


def _get_valid_access_token(account):
    """
    Access tokens last about 24 hours. Refresh proactively when near expiry.
    Also returns a token for an immediate retry when the old token is rejected.
    """
    token = (account["access_token"] or "").strip()
    refresh_token = (account["refresh_token"] or "").strip()

    if not token:
        return None, "access_token kosong; akun perlu Connect ulang."

    # connected_at is stored as the token issuance/connection time in this app.
    try:
        connected = datetime.fromisoformat(str(account["connected_at"]).replace("Z", "+00:00"))
        if connected.tzinfo:
            connected = connected.replace(tzinfo=None)
        expires_in = int(account["expires_in"] or 0)
        if expires_in and (datetime.utcnow() - connected).total_seconds() >= max(expires_in - 900, 0):
            if refresh_token:
                new_token, err = refresh_tiktok_access_token(account)
                if new_token:
                    return new_token, None
                # Fall through: caller can report the refresh failure.
                return None, err
    except Exception:
        # If the old database timestamp cannot be parsed, don't destroy a usable token.
        pass

    return token, None


# Research API requires a separately approved research client. Do not reuse Login Kit tokens.
RESEARCH_REPOST_URL = "https://open.tiktokapis.com/v2/research/user/reposted_videos/"
_research_token_cache = {"token": None, "until": 0}
_research_token_lock = threading.Lock()


def _research_access_token():
    key = os.environ.get("TIKTOK_RESEARCH_CLIENT_KEY", "").strip()
    secret = os.environ.get("TIKTOK_RESEARCH_CLIENT_SECRET", "").strip()
    if not key or not secret:
        return None
    with _research_token_lock:
        if _research_token_cache["token"] and time.time() < _research_token_cache["until"]:
            return _research_token_cache["token"]
        response = requests.post(TOKEN_URL, data={"client_key": key, "client_secret": secret,
            "grant_type": "client_credentials"}, timeout=20)
        response.raise_for_status()
        payload = response.json()
        token = payload.get("access_token")
        if not token:
            app.logger.warning("Research API token unavailable: %s", payload.get("error", "unknown"))
            return None
        _research_token_cache.update(token=token, until=time.time()+max(60, int(payload.get("expires_in", 7200))-120))
        return token


def _research_repost_count(username):
    """Count accessible repost video IDs; return None if unavailable or incomplete."""
    if not username:
        return None
    token = _research_access_token()
    if not token:
        return None
    cursor = None
    seen_cursors = set()
    seen_ids = set()
    for _ in range(100):  # hard safety bound; never present a partial total as complete
        body = {"username": username.lstrip("@"), "max_count": 100}
        if cursor is not None:
            body["cursor"] = cursor
        response = requests.post(RESEARCH_REPOST_URL, params={"fields": "id"},
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"},
            json=body, timeout=30)
        response.raise_for_status()
        payload = response.json()
        if payload.get("error", {}).get("code") not in ("ok", "", None):
            raise ValueError("Research API returned " + str(payload.get("error", {}).get("code")))
        data = payload.get("data") or {}
        # Official spec lists user_reposted_videos; its example uses reposted_videos.
        videos = data.get("user_reposted_videos")
        if videos is None:
            videos = data.get("reposted_videos")
        if not isinstance(videos, list):
            raise ValueError("Research API repost list missing")
        seen_ids.update(str(v["id"]) for v in videos if isinstance(v, dict) and v.get("id") is not None)
        if not data.get("has_more"):
            return len(seen_ids)
        next_cursor = data.get("cursor")
        if next_cursor is None or next_cursor == cursor or next_cursor in seen_cursors:
            raise ValueError("Research API pagination incomplete")
        seen_cursors.add(next_cursor)
        cursor = next_cursor
        time.sleep(STATS_API_DELAY)
    raise ValueError("Research API pagination exceeded safety limit")


def _save_daily_snapshot(c, open_id):
    row = c.execute("""SELECT open_id,admin_medsos,category,video_count,repost_count,
        view_count,likes_count,comment_count,shares_count,follower_count,following_count,
        is_verified FROM accounts WHERE open_id=?""", (open_id,)).fetchone()
    if not row:
        return
    day = datetime.now(ZoneInfo("Asia/Jakarta")).date().isoformat()
    business = int((row['category'] or '').strip().lower() in ('bisnis','business'))
    values = (row['open_id'],day,row['admin_medsos'],row['category'],row['video_count'],
        row['repost_count'],row['view_count'],row['likes_count'],row['comment_count'],
        row['shares_count'],row['follower_count'],row['following_count'],row['is_verified'],business)
    c.execute("""INSERT INTO account_daily_stats
        (open_id,snapshot_date,admin_medsos,category,video_count,repost_count,view_count,
        likes_count,comment_count,shares_count,follower_count,following_count,is_verified,is_business)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(open_id,snapshot_date) DO UPDATE SET
        admin_medsos=excluded.admin_medsos,category=excluded.category,
        video_count=excluded.video_count,repost_count=excluded.repost_count,
        view_count=excluded.view_count,likes_count=excluded.likes_count,
        comment_count=excluded.comment_count,shares_count=excluded.shares_count,
        follower_count=excluded.follower_count,following_count=excluded.following_count,
        is_verified=excluded.is_verified,is_business=excluded.is_business""", values)


@app.route('/api/performance/history')
@role_required('superadmin','supervisor_caprof','admin_medsos')
def performance_history():
    metrics = {'akun':'COUNT(*)','post':'COALESCE(SUM(video_count),0)',
        'repost':'SUM(repost_count)','views':'COALESCE(SUM(view_count),0)',
        'likes':'COALESCE(SUM(likes_count),0)',
        'comments':'COALESCE(SUM(comment_count),0)',
        'shared':'COALESCE(SUM(shares_count),0)',
        'followers':'COALESCE(SUM(follower_count),0)',
        'following':'COALESCE(SUM(following_count),0)',
        'bisnis':'COALESCE(SUM(is_business),0)',
        'verified':'COALESCE(SUM(is_verified),0)'}
    metric = request.args.get('metric','followers')
    if metric not in metrics:
        return jsonify(ok=False,message='Metrik tidak dikenal'),400
    admin = request.args.get('admin','').strip()
    if not admin:
        return jsonify(ok=False,message='Admin diperlukan'),400
    category = request.args.get('category','').strip()
    days = max(1,min(365,int(request.args.get('days','30'))))
    u = current_user()
    c = db()
    try:
        where = ' WHERE admin_medsos=? AND snapshot_date>=?'
        start = (datetime.now(ZoneInfo('Asia/Jakarta')).date() - __import__('datetime').timedelta(days=days-1)).isoformat()
        params = [admin,start]
        if category:
            where += " AND COALESCE(NULLIF(TRIM(category),''),'Tanpa Kategori')=?"
            params.append(category)
        if u['role']=='admin_medsos':
            allowed = c.execute("SELECT COUNT(*) AS n FROM accounts WHERE (assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?))) AND COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan')=?",(u['id'],u['full_name'] or u['username'],admin)).fetchone()['n']
            if not allowed:
                return jsonify(ok=False,message='Akses ditolak'),403
        expr = metrics[metric]
        rows = c.execute(f'SELECT snapshot_date AS day,{expr} AS value,COUNT(*) AS sample_count FROM account_daily_stats'+where+' GROUP BY snapshot_date ORDER BY snapshot_date',tuple(params)).fetchall()
        points = [{'date':r['day'],'value':r['value'],'sample_count':r['sample_count']} for r in rows]
        return jsonify(ok=True,metric=metric,admin=admin,category=category,points=points)
    finally:
        c.close()


def _refresh_account_stats(open_id):
    """Refresh one account. Returns (ok, error_message)."""
    account = get_account(open_id)
    if not account:
        return False, "Akun TikTok tidak ditemukan."
    granted = {x.strip() for x in (account["scope"] or "").split(",") if x.strip()}
    if "user.info.stats" not in granted:
        return False, "Scope user.info.stats belum diberikan."
    try:
        access_token, token_error = _get_valid_access_token(account)
        if not access_token:
            return False, token_error or "token tidak valid"
        r, payload = _fetch_user_profile(access_token)
        err = payload.get("error", {})
        if err.get("code") == "access_token_invalid" or r.status_code == 401:
            new_token, refresh_error = refresh_tiktok_access_token(account)
            if not new_token:
                return False, refresh_error or "access token tidak valid"
            access_token = new_token
            r, payload = _fetch_user_profile(access_token)
            err = payload.get("error", {})
        if not r.ok or err.get("code") not in ("ok", "", None):
            return False, str(err.get("message", err.get("code", "unknown")))
        user = payload.get("data", {}).get("user", {})
        _save_profile(open_id, user)
        # An unavailable Research API must not turn missing repost data into zero.
        repost_count = None
        repost_updated_at = None
        try:
            repost_count = _research_repost_count(user.get("username") or account.get("username"))
            if repost_count is not None:
                repost_updated_at = datetime.utcnow().isoformat()
        except Exception as exc:
            app.logger.warning("Repost refresh unavailable for account: %s", type(exc).__name__)
        total_views = 0
        total_comments = 0
        total_shares = 0
        video_list_ok = True
        video_list_complete = False
        seen_cursors = set()
        if "video.list" in granted:
            cursor = None
            for _ in range(10000):
                body = {"max_count": 20}
                if cursor:
                    body["cursor"] = cursor
                vr = requests.post(VIDEO_LIST_URL, params={"fields": "id,view_count,comment_count,share_count"},
                                   headers=api_headers(access_token), json=body, timeout=30)
                try:
                    vp = vr.json()
                except ValueError:
                    video_list_ok = False
                    break
                verr = vp.get("error", {})
                if not vr.ok or verr.get("code") not in ("ok", "", None):
                    video_list_ok = False
                    break
                vd = vp.get("data", {})
                total_views += sum(int(v.get("view_count") or 0) for v in vd.get("videos", []))
                total_comments += sum(int(v.get("comment_count") or 0) for v in vd.get("videos", []))
                total_shares += sum(int(v.get("share_count") or 0) for v in vd.get("videos", []))
                if not vd.get("has_more"):
                    video_list_complete = True
                    break
                next_cursor = vd.get("cursor")
                if next_cursor is None or next_cursor == cursor or next_cursor in seen_cursors:
                    video_list_ok = False
                    break
                seen_cursors.add(next_cursor)
                cursor = next_cursor
                time.sleep(STATS_API_DELAY)
        # Never overwrite valid stored totals with zero/partial API results.
        c = db()
        try:
            c.execute("""UPDATE accounts SET display_name=?, avatar_url=?, username=?, video_count=?, following_count=?,
                follower_count=?, likes_count=?, is_verified=?, profile_deep_link=?, stats_updated_at=? WHERE open_id=?""",
                (user.get("display_name") or account["display_name"],
                 user.get("avatar_large_url") or user.get("avatar_url_100") or user.get("avatar_url") or account["avatar_url"],
                 user.get("username") or account.get("username"), user.get("video_count"), user.get("following_count"),
                 user.get("follower_count"), user.get("likes_count"), 1 if user.get("is_verified") else 0,
                 user.get("profile_deep_link") or account.get("profile_deep_link") or "",
                 datetime.utcnow().isoformat(), open_id))
            if "video.list" in granted and video_list_ok and video_list_complete:
                c.execute("UPDATE accounts SET view_count=?, comment_count=?, shares_count=? WHERE open_id=?",
                          (total_views, total_comments, total_shares, open_id))
            if repost_count is not None:
                c.execute("UPDATE accounts SET repost_count=?, repost_updated_at=? WHERE open_id=?",
                          (repost_count, repost_updated_at, open_id))
            _save_daily_snapshot(c, open_id)
            c.commit()
        finally:
            c.close()
        if "video.list" in granted and not (video_list_ok and video_list_complete):
            return False, "Profil diperbarui; daftar video tidak lengkap. Statistik views/comments/shares lama dipertahankan."
        return True, None
    except Exception as e:
        return False, str(e)
    finally:
        time.sleep(STATS_API_DELAY)


def _stats_worker():
    global _stats_worker_started
    while True:
        try:
            c = db()
            _expire_stale_stats_jobs(c)
            batch = c.execute("SELECT * FROM stats_refresh_batches WHERE status IN ('QUEUED','PROCESSING') ORDER BY id LIMIT 1").fetchone()
            if not batch:
                c.close(); time.sleep(2); continue
            if batch["status"] == "QUEUED":
                c.execute("UPDATE stats_refresh_batches SET status='PROCESSING',started_at=? WHERE id=?", (datetime.utcnow().isoformat(), batch["id"]))
                c.commit()
            job = c.execute("SELECT * FROM stats_refresh_jobs WHERE batch_id=? AND status='QUEUED' ORDER BY id LIMIT 1", (batch["id"],)).fetchone()
            c.close()
            if not job:
                c = db(); c.execute("UPDATE stats_refresh_batches SET status='DONE',finished_at=? WHERE id=?", (datetime.utcnow().isoformat(), batch["id"])); c.commit(); c.close(); continue
            c = db(); c.execute("UPDATE stats_refresh_jobs SET status='PROCESSING',started_at=? WHERE id=?", (datetime.utcnow().isoformat(), job["id"])); c.commit(); c.close()
            ok, err = _refresh_account_stats(job["open_id"])
            c = db()
            c.execute("UPDATE stats_refresh_jobs SET status=?,last_error=?,finished_at=? WHERE id=?", ("DONE" if ok else "FAILED", err, datetime.utcnow().isoformat(), job["id"]))
            c.execute("UPDATE stats_refresh_batches SET completed_count=completed_count+1, failed_count=failed_count+? WHERE id=?", (0 if ok else 1, batch["id"]))
            c.commit(); c.close()
        except Exception:
            app.logger.exception("Stats refresh background worker failed")
            try:
                c.close()
            except Exception:
                pass
            time.sleep(3)


def _ensure_stats_worker():
    global _stats_worker_started
    with _stats_worker_lock:
        if not _stats_worker_started:
            threading.Thread(target=_stats_worker, daemon=True, name="stats-refresh-worker").start()
            _stats_worker_started = True


@app.post("/stats/refresh-all")
def refresh_all_stats():
    u = current_user()
    if not u or u["role"] not in ("superadmin", "admin_medsos"):
        return {"ok": False, "message": "Akses ditolak."}, 403
    _ensure_stats_worker()
    c = db()
    if u["role"] == "superadmin":
        rows = c.execute("SELECT open_id FROM accounts ORDER BY COALESCE(view_count,0) DESC, connected_at DESC").fetchall()
    else:
        rows = c.execute("""SELECT open_id FROM accounts
            WHERE assigned_admin_id=? OR LOWER(TRIM(COALESCE(admin_medsos,'')))=LOWER(TRIM(?))
            ORDER BY COALESCE(view_count,0) DESC, connected_at DESC""",
            (u["id"], u["full_name"] or u["username"])).fetchall()
    now = datetime.utcnow().isoformat()
    if c.is_pg:
        cur = c.execute("INSERT INTO stats_refresh_batches(total_accounts,status,created_at) VALUES(?, 'QUEUED', ?) RETURNING id", (len(rows), now))
        batch_id = cur.fetchone()["id"]
    else:
        cur = c.execute("INSERT INTO stats_refresh_batches(total_accounts,status,created_at) VALUES(?, 'QUEUED', ?)", (len(rows), now))
        batch_id = cur.lastrowid
    for row in rows:
        c.execute("INSERT INTO stats_refresh_jobs(batch_id,open_id,status,created_at) VALUES(?,?,'QUEUED',?)", (batch_id, row["open_id"], now))
    c.commit(); c.close()
    session["stats_refresh_batches"] = (session.get("stats_refresh_batches", []) + [batch_id])[-20:]
    return {"ok": True, "batch_id": batch_id, "total": len(rows)}


@app.get("/stats/refresh-all/<int:batch_id>")
def refresh_all_status(batch_id):
    u = current_user()
    if not u or u["role"] not in ("superadmin", "admin_medsos"):
        return {"ok": False, "message": "Akses ditolak."}, 403
    _ensure_stats_worker()
    c = db()
    _expire_stale_stats_jobs(c)
    if u["role"] != "superadmin" and batch_id not in session.get("stats_refresh_batches", []):
        c.close()
        return {"ok": False, "message": "Batch bukan milik Anda."}, 403
    batch = c.execute("SELECT * FROM stats_refresh_batches WHERE id=?", (batch_id,)).fetchone()
    c.close()
    if not batch:
        return {"ok": False, "message": "Batch tidak ditemukan."}, 404
    return {"ok": True, "batch_id": batch_id, "total": batch["total_accounts"],
            "completed": batch["completed_count"], "failed": batch["failed_count"], "status": batch["status"]}


@app.post("/account/<open_id>/refresh-stats")
def refresh_stats(open_id):
    u = current_user()
    account = get_account(open_id)
    if not u or u["role"] not in ("superadmin", "admin_medsos"):
        return {"ok": False, "message": "Akses ditolak."}, 403
    if not account:
        return {"ok": False, "message": "Akun tidak ditemukan."}, 404
    account = dict(account)
    if u["role"] == "admin_medsos" and not (account.get("assigned_admin_id") == u["id"] or (account.get("admin_medsos") or "").strip().casefold() == (u["full_name"] or u["username"]).strip().casefold()):
        return {"ok": False, "message": "Akun bukan milik admin ini."}, 403
    # Queue refresh so long TikTok pagination cannot trigger Gunicorn request timeout.
    _ensure_stats_worker()
    c = db()
    try:
        _expire_stale_stats_jobs(c)
        existing = c.execute("SELECT j.batch_id FROM stats_refresh_jobs j WHERE j.open_id=? AND j.status IN ('QUEUED','PROCESSING') ORDER BY j.id DESC LIMIT 1", (open_id,)).fetchone()
        if existing:
            batch_id = existing['batch_id']
            c.close()
            session['stats_refresh_batches'] = (session.get('stats_refresh_batches', []) + [batch_id])[-20:]
            return {"ok": True, "queued": True, "batch_id": batch_id, "message": "Refresh akun ini masih diproses."}, 202
        now = datetime.utcnow().isoformat()
        if c.is_pg:
            cur = c.execute("INSERT INTO stats_refresh_batches(total_accounts,status,created_at) VALUES(1,'QUEUED',?) RETURNING id", (now,))
            batch_id = cur.fetchone()["id"]
        else:
            cur = c.execute("INSERT INTO stats_refresh_batches(total_accounts,status,created_at) VALUES(1,'QUEUED',?)", (now,))
            batch_id = cur.lastrowid
        c.execute("INSERT INTO stats_refresh_jobs(batch_id,open_id,status,created_at) VALUES(?,?,'QUEUED',?)", (batch_id, open_id, now))
        c.commit()
    except Exception:
        c.close()
        app.logger.exception("Unable to queue account statistics refresh")
        return {"ok": False, "message": "Gagal memasukkan refresh ke antrean."}, 500
    c.close()
    session["stats_refresh_batches"] = (session.get("stats_refresh_batches", []) + [batch_id])[-20:]
    return {"ok": True, "queued": True, "batch_id": batch_id, "message": "Refresh statistik berjalan di antrean."}, 202


@app.get("/account/<open_id>/refresh-stats/status/<int:batch_id>")
def refresh_stats_status(open_id, batch_id):
    u = current_user()
    account = get_account(open_id)
    if not u or u["role"] not in ("superadmin", "admin_medsos"):
        return {"ok": False, "message": "Akses ditolak."}, 403
    if not account:
        return {"ok": False, "message": "Akun tidak ditemukan."}, 404
    account = dict(account)
    if u["role"] == "admin_medsos" and not (account.get("assigned_admin_id") == u["id"] or (account.get("admin_medsos") or "").strip().casefold() == (u["full_name"] or u["username"]).strip().casefold()):
        return {"ok": False, "message": "Akun bukan milik admin ini."}, 403
    if u["role"] != "superadmin" and batch_id not in session.get("stats_refresh_batches", []):
        return {"ok": False, "message": "Refresh bukan milik Anda."}, 403
    _ensure_stats_worker()
    c = db()
    _expire_stale_stats_jobs(c)
    job = c.execute("SELECT * FROM stats_refresh_jobs WHERE batch_id=? AND open_id=? ORDER BY id DESC LIMIT 1", (batch_id, open_id)).fetchone()
    c.close()
    if not job:
        return {"ok": False, "message": "Pekerjaan refresh tidak ditemukan."}, 404
    status = job["status"]
    if status not in ("DONE", "FAILED"):
        return {"ok": True, "pending": True, "status": status}
    if status == "FAILED":
        return {"ok": False, "pending": False, "message": job["last_error"] or "Refresh gagal; data lama dipertahankan."}
    updated = dict(get_account(open_id))
    return {"ok": True, "pending": False, "stats": {key: updated.get(key) for key in ("video_count", "repost_count", "view_count", "likes_count", "comment_count", "shares_count", "follower_count", "following_count", "stats_updated_at")}}

@app.post("/account/<open_id>/disconnect")
def disconnect_account(open_id):
    account = get_account(open_id)
    if not account:
        flash("Akun TikTok tidak ditemukan.", "error")
        return redirect(url_for("dashboard"))
    try:
        rr = requests.post(REVOKE_URL, data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "token": account["access_token"]
        }, timeout=30)
        # Even if an expired token cannot be revoked, remove the local connection.
        c = db()
        c.execute("DELETE FROM publish_jobs WHERE open_id=?", (open_id,))
        c.execute("DELETE FROM accounts WHERE open_id=?", (open_id,))
        c.commit(); c.close()
        if rr.ok:
            flash("Akun TikTok berhasil di-unconnect dan izin aplikasi dicabut.", "success")
        else:
            flash("Akun dihapus dari DashboardFYEPE. TikTok tidak mengonfirmasi revoke token (mungkin token sudah kedaluwarsa).", "success")
    except Exception as e:
        c = db(); c.execute("DELETE FROM publish_jobs WHERE open_id=?", (open_id,)); c.execute("DELETE FROM accounts WHERE open_id=?", (open_id,)); c.commit(); c.close()
        flash("Akun dihapus dari DashboardFYEPE. Revoke TikTok tidak dapat dikonfirmasi: " + str(e), "success")
    return redirect(url_for("dashboard"))

@app.get("/auth/tiktok/login")
def tiktok_login():
    if not CLIENT_KEY or not CLIENT_SECRET:
        flash("TikTok credentials belum dipasang di Render Environment Variables.", "error")
        return redirect(url_for("dashboard"))
    state = secrets.token_urlsafe(32)
    session["oauth_state"] = state
    u = current_user()
    if not u or u["role"] not in ("superadmin", "admin_medsos"):
        flash("Silakan login sebagai admin terlebih dahulu.", "error")
        return redirect(url_for("login"))
    session["oauth_connect_user_id"] = u["id"]
    params = {
        "client_key": CLIENT_KEY,
        "response_type": "code",
        "scope": SCOPES,
        "redirect_uri": REDIRECT_URI,
        "state": state,
        "disable_auto_auth": "1"
    }
    return redirect(AUTH_URL + "?" + urlencode(params))

@app.get("/auth/tiktok/callback")
def tiktok_callback():
    if request.args.get("error"):
        flash("TikTok authorization gagal: " + request.args.get("error_description", request.args["error"]), "error")
        return redirect(url_for("dashboard"))

    if request.args.get("state") != session.pop("oauth_state", None):
        flash("OAuth state tidak cocok. Hubungkan akun lagi.", "error")
        return redirect(url_for("dashboard"))

    # Preserve the initiating admin through the OAuth redirect.
    initiating_user_id = session.pop("oauth_connect_user_id", None)
    initiating_user = current_user()
    if not initiating_user or initiating_user["id"] != initiating_user_id:
        flash("Sesi admin berubah selama otorisasi. Silakan Connect ulang.", "error")
        return redirect(url_for("login"))
    code = request.args.get("code", "")
    try:
        r = requests.post(TOKEN_URL, data={
            "client_key": CLIENT_KEY,
            "client_secret": CLIENT_SECRET,
            "code": code,
            "grant_type": "authorization_code",
            "redirect_uri": REDIRECT_URI
        }, timeout=30)
        data = r.json()
    except Exception as e:
        flash("Token exchange gagal: " + str(e), "error")
        return redirect(url_for("dashboard"))

    if not r.ok or "access_token" not in data:
        flash("Token exchange gagal: " + str(data), "error")
        return redirect(url_for("dashboard"))

    token = data["access_token"]
    open_id = data.get("open_id", "")
    try:
        ur = requests.get(
            USER_URL,
            params={"fields": "open_id,display_name,avatar_url,username"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        user = ur.json().get("data", {}).get("user", {})
    except Exception as e:
        flash("Gagal membaca profil TikTok: " + str(e), "error")
        return redirect(url_for("dashboard"))

    # Do not take ownership away from another admin when an account is reconnected.
    c = db()
    existing = c.execute("SELECT assigned_admin_id,admin_medsos,username FROM accounts WHERE open_id=?", (open_id,)).fetchone()
    owner_id = existing["assigned_admin_id"] if existing and existing["assigned_admin_id"] else (initiating_user["id"] if initiating_user["role"] == "admin_medsos" else None)
    owner_name = (existing["admin_medsos"] if existing and existing["assigned_admin_id"] else
                  (initiating_user["full_name"] or initiating_user["username"] if owner_id else ""))
    tik_username = (user.get("username") or (existing["username"] if existing else "") or "").lstrip("@")
    if c.is_pg:
        c.execute("""INSERT INTO accounts(
            open_id,display_name,avatar_url,access_token,refresh_token,scope,expires_in,connected_at,username,assigned_admin_id,admin_medsos
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT (open_id) DO UPDATE SET
            display_name=EXCLUDED.display_name,
            avatar_url=EXCLUDED.avatar_url,
            access_token=EXCLUDED.access_token,
            refresh_token=EXCLUDED.refresh_token,
            scope=EXCLUDED.scope,
            expires_in=EXCLUDED.expires_in,
            connected_at=EXCLUDED.connected_at,
            username=COALESCE(NULLIF(EXCLUDED.username,''),accounts.username),
            assigned_admin_id=COALESCE(accounts.assigned_admin_id,EXCLUDED.assigned_admin_id),
            admin_medsos=CASE WHEN accounts.assigned_admin_id IS NOT NULL THEN accounts.admin_medsos ELSE EXCLUDED.admin_medsos END""", (
            open_id, user.get("display_name", "TikTok account"), user.get("avatar_url", ""),
            token, data.get("refresh_token", ""), data.get("scope", ""),
            data.get("expires_in", 0), datetime.utcnow().isoformat(), tik_username, owner_id, owner_name
        ))
    else:
        c.execute("""INSERT INTO accounts(
            open_id,display_name,avatar_url,access_token,refresh_token,scope,expires_in,connected_at,username,assigned_admin_id,admin_medsos
        ) VALUES(?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(open_id) DO UPDATE SET
            display_name=excluded.display_name, avatar_url=excluded.avatar_url,
            access_token=excluded.access_token, refresh_token=excluded.refresh_token,
            scope=excluded.scope, expires_in=excluded.expires_in, connected_at=excluded.connected_at,
            username=COALESCE(NULLIF(excluded.username,''),accounts.username),
            assigned_admin_id=COALESCE(accounts.assigned_admin_id,excluded.assigned_admin_id),
            admin_medsos=CASE WHEN accounts.assigned_admin_id IS NOT NULL THEN accounts.admin_medsos ELSE excluded.admin_medsos END""", (
            open_id, user.get("display_name", "TikTok account"), user.get("avatar_url", ""),
            token, data.get("refresh_token", ""), data.get("scope", ""),
            data.get("expires_in", 0), datetime.utcnow().isoformat(), tik_username, owner_id, owner_name
        ))
    c.commit()
    c.close()
    flash("TikTok account berhasil terhubung.", "success")
    return redirect(url_for("dashboard"))


def _save_scheduled_upload(file_storage, prefix="scheduled"):
    folder = os.path.join(PHOTO_UPLOAD_DIR, "scheduled_files")
    os.makedirs(folder, exist_ok=True)
    ext = os.path.splitext(file_storage.filename.lower())[1]
    name = f"{prefix}_{secrets.token_urlsafe(12).replace('-', '').replace('_', '')}{ext}"
    path = os.path.join(folder, name)
    file_storage.save(path)
    return path

def _parse_schedule_datetime():
    date_value = request.form.get("schedule_date", "").strip()
    time_value = request.form.get("schedule_time", "").strip()
    if not date_value or not time_value:
        return None
    try:
        return datetime.strptime(f"{date_value} {time_value}", "%Y-%m-%d %H:%M")
    except ValueError:
        return None

def _add_schedule(open_id, post_type, scheduled_at, payload):
    c = db()
    c.execute("""INSERT INTO scheduled_posts(
        open_id,post_type,scheduled_at,payload_json,status,created_at,last_error
    ) VALUES(?,?,?,?,?,?,?)""", (
        open_id, post_type, scheduled_at.strftime("%Y-%m-%d %H:%M:%S"),
        json.dumps(payload, ensure_ascii=False), "SCHEDULED",
        datetime.now().isoformat(), ""
    ))
    c.commit()
    c.close()

@app.route("/create-post/<open_id>", methods=["GET", "POST"])
def create_post(open_id):
    account = get_account(open_id)
    if not account:
        flash("Akun TikTok tidak ditemukan.", "error")
        return redirect(url_for("dashboard"))

    r, info = get_creator_info(account)
    creator = info.get("data", {})
    api_error = info.get("error", {})

    if r is None or not r.ok or api_error.get("code") not in ("ok", "", None):
        return render_template(
            "create_post.html",
            account=account, creator=creator, api_error=api_error,
            max_upload_mb=64
        )

    if request.method == "GET":
        return render_template(
            "create_post.html",
            account=account, creator=creator, api_error=api_error,
            max_upload_mb=64
        )

    video = request.files.get("video")
    caption = request.form.get("caption", "").strip()
    privacy = request.form.get("privacy_level", "").strip()
    consent = request.form.get("consent") == "yes"

    if not consent:
        flash("Centang persetujuan sebelum mengirim video ke TikTok.", "error")
        return redirect(url_for("create_post", open_id=open_id))

    options = creator.get("privacy_level_options", [])
    if not privacy or privacy not in options:
        flash("Pilih privacy yang tersedia untuk akun ini.", "error")
        return redirect(url_for("create_post", open_id=open_id))

    if not video or not video.filename:
        flash("Pilih file video.", "error")
        return redirect(url_for("create_post", open_id=open_id))

    ext = os.path.splitext(video.filename.lower())[1]
    if ext not in {".mp4", ".mov", ".webm"}:
        flash("Format harus MP4, MOV, atau WEBM.", "error")
        return redirect(url_for("create_post", open_id=open_id))

    video_bytes = video.read()
    size = len(video_bytes)
    if size == 0:
        flash("File video kosong.", "error")
        return redirect(url_for("create_post", open_id=open_id))
    if size > MAX_UPLOAD_BYTES:
        flash("V2 ini membatasi upload maksimum 64 MB.", "error")
        return redirect(url_for("create_post", open_id=open_id))

    # User controls. If TikTok says an interaction is unavailable,
    # force it disabled regardless of submitted form values.
    allow_comment = request.form.get("allow_comment") == "yes"
    allow_duet = request.form.get("allow_duet") == "yes"
    allow_stitch = request.form.get("allow_stitch") == "yes"

    disable_comment = True if creator.get("comment_disabled") else not allow_comment
    disable_duet = True if creator.get("duet_disabled") else not allow_duet
    disable_stitch = True if creator.get("stitch_disabled") else not allow_stitch

    publish_when = request.form.get("publish_when", "now")
    if publish_when == "scheduled":
        scheduled_at = _parse_schedule_datetime()
        if not scheduled_at:
            flash("Isi tanggal dan jam scheduled post.", "error")
            return redirect(url_for("create_post", open_id=open_id))
        if scheduled_at <= datetime.now():
            flash("Waktu scheduled harus lebih besar dari waktu sekarang.", "error")
            return redirect(url_for("create_post", open_id=open_id))
        # Save locally for the scheduler worker.
        folder = os.path.join(PHOTO_UPLOAD_DIR, "scheduled_files")
        os.makedirs(folder, exist_ok=True)
        ext2 = os.path.splitext(video.filename.lower())[1]
        stored = os.path.join(folder, "video_" + secrets.token_urlsafe(12).replace("-", "").replace("_", "") + ext2)
        with open(stored, "wb") as sf:
            sf.write(video_bytes)
        _add_schedule(open_id, "video", scheduled_at, {
            "file_path": stored, "filename": video.filename, "caption": caption,
            "privacy_level": privacy, "allow_comment": allow_comment,
            "allow_duet": allow_duet, "allow_stitch": allow_stitch
        })
        flash("Video berhasil dimasukkan ke Scheduled Posts.", "success")
        return redirect(url_for("dashboard"))

    payload = {
        "post_info": {
            "title": caption,
            "privacy_level": privacy,
            "disable_comment": disable_comment,
            "disable_duet": disable_duet,
            "disable_stitch": disable_stitch
        },
        "source_info": {
            "source": "FILE_UPLOAD",
            "video_size": size,
            "chunk_size": size,
            "total_chunk_count": 1
        }
    }

    try:
        ir = requests.post(
            DIRECT_POST_INIT_URL,
            headers=api_headers(account["access_token"]),
            json=payload,
            timeout=60
        )
        init = ir.json()
    except Exception as e:
        flash("Direct Post init gagal: " + str(e), "error")
        return redirect(url_for("create_post", open_id=open_id))

    err = init.get("error", {})
    data = init.get("data", {})
    if not ir.ok or err.get("code") not in ("ok", "", None) or not data.get("upload_url"):
        flash("TikTok menolak Direct Post: " + json.dumps(init, ensure_ascii=False), "error")
        return redirect(url_for("create_post", open_id=open_id))

    upload_url = data["upload_url"]
    publish_id = data.get("publish_id", "")
    mime = mimetypes.guess_type(video.filename)[0] or "video/mp4"
    if mime not in {"video/mp4", "video/quicktime", "video/webm"}:
        mime = "video/mp4"

    try:
        up = requests.put(
            upload_url,
            headers={
                "Content-Type": mime,
                "Content-Length": str(size),
                "Content-Range": f"bytes 0-{size-1}/{size}"
            },
            data=video_bytes,
            timeout=240
        )
    except Exception as e:
        flash("Upload ke server TikTok gagal: " + str(e), "error")
        return redirect(url_for("create_post", open_id=open_id))

    if not up.ok:
        flash(f"Upload TikTok gagal HTTP {up.status_code}: {up.text[:500]}", "error")
        return redirect(url_for("create_post", open_id=open_id))

    c = db()
    if c.is_pg:
        c.execute("""INSERT INTO publish_jobs(
            publish_id,open_id,caption,privacy_level,status,created_at,last_response
        ) VALUES(?,?,?,?,?,?,?)
        ON CONFLICT (publish_id) DO UPDATE SET
            status=EXCLUDED.status,last_response=EXCLUDED.last_response""", (
            publish_id, open_id, caption, privacy, "PROCESSING",
            datetime.utcnow().isoformat(), json.dumps(init, ensure_ascii=False)
        ))
    else:
        c.execute("""INSERT OR REPLACE INTO publish_jobs(
            publish_id,open_id,caption,privacy_level,status,created_at,last_response
        ) VALUES(?,?,?,?,?,?,?)""", (
            publish_id, open_id, caption, privacy, "PROCESSING",
            datetime.utcnow().isoformat(), json.dumps(init, ensure_ascii=False)
        ))
    c.commit()
    c.close()

    flash("Video sudah dikirim ke TikTok dan sedang diproses.", "success")
    return redirect(url_for("publish_status", open_id=open_id, publish_id=publish_id))

@app.route("/create-photo/<open_id>", methods=["GET", "POST"])
def create_photo(open_id):
    account = get_account(open_id)
    if not account:
        flash("Akun TikTok tidak ditemukan.", "error")
        return redirect(url_for("dashboard"))

    mode = request.args.get("mode", "photo").strip().lower()
    if mode not in {"photo", "carousel"}:
        mode = "photo"

    r, info = get_creator_info(account)
    creator = info.get("data", {})
    api_error = info.get("error", {})

    if request.method == "GET":
        return render_template("create_photo.html", account=account, creator=creator,
                               api_error=api_error, mode=mode)

    caption = request.form.get("caption", "").strip()
    privacy = request.form.get("privacy_level", "").strip()
    consent = request.form.get("consent") == "yes"
    allow_comment = request.form.get("allow_comment") == "yes"
    auto_add_music = request.form.get("auto_add_music") == "yes"

    if not consent:
        flash("Centang persetujuan sebelum mengirim foto ke TikTok.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))

    options = creator.get("privacy_level_options", [])
    if not privacy or privacy not in options:
        flash("Pilih privacy yang tersedia untuk akun ini.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))

    photo_urls = [u.strip() for u in request.form.getlist("photo_url") if u.strip()]
    uploaded = request.files.getlist("photos")

    if uploaded and any(f and f.filename for f in uploaded):
        batch = secrets.token_urlsafe(10).replace("-", "").replace("_", "")
        batch_dir = os.path.join(PHOTO_UPLOAD_DIR, batch)
        os.makedirs(batch_dir, exist_ok=True)

        for idx, f in enumerate(uploaded, start=1):
            if not f or not f.filename:
                continue
            ext = os.path.splitext(f.filename.lower())[1]
            if ext not in {".jpg", ".jpeg", ".png", ".webp"}:
                flash("Format foto harus JPG, JPEG, PNG, atau WEBP.", "error")
                return redirect(url_for("create_photo", open_id=open_id, mode=mode))
            safe_name = f"{idx:02d}{ext}"
            f.save(os.path.join(batch_dir, safe_name))
            photo_urls.append(
                request.url_root.rstrip("/") +
                url_for("uploaded_photo", batch=batch, filename=safe_name)
            )

    if not photo_urls:
        flash("Pilih minimal satu foto.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))
    if mode == "photo" and len(photo_urls) != 1:
        flash("Create Photo hanya menerima 1 foto. Gunakan Create Carousel untuk beberapa foto.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))
    if mode == "carousel" and len(photo_urls) < 2:
        flash("Create Carousel membutuhkan minimal 2 foto.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))
    if len(photo_urls) > 35:
        flash("Maksimum 35 foto dalam satu carousel.", "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))

    publish_when = request.form.get("publish_when", "now")
    if publish_when == "scheduled":
        scheduled_at = _parse_schedule_datetime()
        if not scheduled_at:
            flash("Isi tanggal dan jam scheduled post.", "error")
            return redirect(url_for("create_photo", open_id=open_id, mode=mode))
        if scheduled_at <= datetime.now():
            flash("Waktu scheduled harus lebih besar dari waktu sekarang.", "error")
            return redirect(url_for("create_photo", open_id=open_id, mode=mode))
        _add_schedule(open_id, mode, scheduled_at, {
            "photo_urls": photo_urls, "caption": caption, "privacy_level": privacy,
            "allow_comment": allow_comment, "auto_add_music": auto_add_music
        })
        flash(("Carousel" if mode == "carousel" else "Foto") + " berhasil dimasukkan ke Scheduled Posts.", "success")
        return redirect(url_for("dashboard"))

    payload = {
        "post_info": {
            "title": caption[:90],
            "description": caption,
            "disable_comment": True if creator.get("comment_disabled") else not allow_comment,
            "privacy_level": privacy,
            "auto_add_music": auto_add_music
        },
        "source_info": {
            "source": "PULL_FROM_URL",
            "photo_cover_index": 0,
            "photo_images": photo_urls
        },
        "post_mode": "DIRECT_POST",
        "media_type": "PHOTO"
    }

    try:
        pr = requests.post(PHOTO_POST_URL, headers=api_headers(account["access_token"]),
                           json=payload, timeout=60)
        try:
            result = pr.json()
        except ValueError:
            result = {"error": {"code": "invalid_json", "message": pr.text[:1000]}}
    except Exception as e:
        flash("Photo/Carousel Post gagal: " + str(e), "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))

    err = result.get("error", {})
    publish_id = result.get("data", {}).get("publish_id", "")
    if not pr.ok or err.get("code") not in ("ok", "", None) or not publish_id:
        flash("TikTok menolak Photo/Carousel Post: " +
              json.dumps(result, ensure_ascii=False), "error")
        return redirect(url_for("create_photo", open_id=open_id, mode=mode))

    c = db()
    if c.is_pg:
        c.execute("""INSERT INTO publish_jobs(
            publish_id,open_id,caption,privacy_level,status,created_at,last_response
        ) VALUES(?,?,?,?,?,?,?)
        ON CONFLICT (publish_id) DO UPDATE SET
            status=EXCLUDED.status,last_response=EXCLUDED.last_response""",
        (publish_id, open_id, caption, privacy, "PROCESSING",
         datetime.utcnow().isoformat(), json.dumps(result, ensure_ascii=False)))
    else:
        c.execute("""INSERT OR REPLACE INTO publish_jobs(
            publish_id,open_id,caption,privacy_level,status,created_at,last_response
        ) VALUES(?,?,?,?,?,?,?)""",
        (publish_id, open_id, caption, privacy, "PROCESSING",
         datetime.utcnow().isoformat(), json.dumps(result, ensure_ascii=False)))
    c.commit()
    c.close()

    flash(("Carousel" if mode == "carousel" else "Foto") +
          " sudah dikirim ke TikTok dan sedang diproses.", "success")
    return redirect(url_for("publish_status", open_id=open_id, publish_id=publish_id))


@app.get("/publish-status/<open_id>/<path:publish_id>")
def publish_status(open_id, publish_id):
    account = get_account(open_id)
    if not account:
        flash("Akun TikTok tidak ditemukan.", "error")
        return redirect(url_for("dashboard"))

    try:
        r = requests.post(
            POST_STATUS_URL,
            headers=api_headers(account["access_token"]),
            json={"publish_id": publish_id},
            timeout=30
        )
        result = r.json()
    except Exception as e:
        result = {"data": {}, "error": {"code": "network_error", "message": str(e)}}

    status = result.get("data", {}).get("status", "UNKNOWN")
    c = db()
    c.execute(
        "UPDATE publish_jobs SET status=?,last_response=? WHERE publish_id=?",
        (status, json.dumps(result, ensure_ascii=False), publish_id)
    )
    c.commit()
    c.close()
    return render_template(
        "publish_status.html",
        account=account, publish_id=publish_id,
        status=status, result=result
    )

@app.get("/media/<batch>/<path:filename>")
def uploaded_photo(batch, filename):
    # TikTok's PULL_FROM_URL fetcher has no DashboardFYEPE login session.
    # Only randomly named JPEG batches and safe filenames are publicly served.
    if (not re.fullmatch(r"[A-Za-z0-9_-]{8,80}", batch)
            or not re.fullmatch(r"(?:[0-9]{2}|[A-Za-z0-9_-]{1,80})\.(?:jpg|jpeg)", filename, re.IGNORECASE)):
        abort(404)
    response = send_from_directory(os.path.join(PHOTO_UPLOAD_DIR, batch), filename,
                                   mimetype="image/jpeg", conditional=True)
    response.headers["Cache-Control"] = "public, max-age=86400"
    response.headers["X-Content-Type-Options"] = "nosniff"
    return response

@app.get(f"/{VERIFY_FILENAME}")
def verify_file():
    return send_from_directory(app.root_path, VERIFY_FILENAME, mimetype="text/plain")


BULK_START_INTERVAL_SECONDS = max(12, int(os.environ.get("BULK_START_INTERVAL_SECONDS", "12")))
_bulk_worker_lock = threading.Lock()
_bulk_worker_running = False

def _safe_privacy(account, requested_privacy):
    r, info = get_creator_info(account)
    creator = info.get("data", {})
    err = info.get("error", {})
    if r is None or not r.ok or err.get("code") not in ("ok", "", None):
        return None, creator, "creator_info: " + json.dumps(info, ensure_ascii=False)[:800]
    options = creator.get("privacy_level_options", [])
    privacy = requested_privacy if requested_privacy in options else ("SELF_ONLY" if "SELF_ONLY" in options else None)
    if not privacy:
        return None, creator, "Tidak ada privacy_level yang tersedia untuk akun ini."
    return privacy, creator, None

def _bulk_publish_video(account, file_path, original_name, caption, requested_privacy):
    privacy, creator, problem = _safe_privacy(account, requested_privacy)
    if problem: return False, problem, None
    try:
        size = os.path.getsize(file_path)
        if size <= 0 or size > MAX_UPLOAD_BYTES:
            return False, "Ukuran video tidak valid / melebihi 64 MB.", None
        with open(file_path, "rb") as f: video_bytes = f.read()
        payload = {"post_info":{"title":caption,"privacy_level":privacy,
                  "disable_comment":True if creator.get("comment_disabled") else False,
                  "disable_duet":True if creator.get("duet_disabled") else False,
                  "disable_stitch":True if creator.get("stitch_disabled") else False},
                  "source_info":{"source":"FILE_UPLOAD","video_size":size,"chunk_size":size,"total_chunk_count":1}}
        ir = requests.post(DIRECT_POST_INIT_URL, headers=api_headers(account["access_token"]), json=payload, timeout=60)
        init = ir.json(); ierr=init.get("error",{}); data=init.get("data",{})
        if not ir.ok or ierr.get("code") not in ("ok","",None) or not data.get("upload_url"):
            return False, json.dumps(init, ensure_ascii=False)[:1200], None
        mime=mimetypes.guess_type(original_name)[0] or "video/mp4"
        if mime not in {"video/mp4","video/quicktime","video/webm"}: mime="video/mp4"
        up=requests.put(data["upload_url"],headers={"Content-Type":mime,"Content-Length":str(size),"Content-Range":f"bytes 0-{size-1}/{size}"},data=video_bytes,timeout=240)
        if not up.ok: return False,f"Upload HTTP {up.status_code}: {up.text[:800]}",data.get("publish_id")
        return True,"PROCESSING",data.get("publish_id")
    except Exception as e: return False,str(e),None

def _bulk_publish_photo(account, file_paths, caption, requested_privacy):
    privacy, creator, problem = _safe_privacy(account, requested_privacy)
    if problem: return False, problem, None
    try:
        # TikTok photo Direct Post uses PULL_FROM_URL. These URLs point to DashboardFYEPE's public /media route.
        urls=[]
        for fp in file_paths:
            rel=os.path.relpath(fp, PHOTO_UPLOAD_DIR).replace(os.sep,"/")
            batch, filename = rel.split("/",1)
            urls.append(PUBLIC_BASE_URL + f"/media/{quote(batch)}/{quote(filename)}")
        payload={"post_info":{"title":caption[:90],"description":caption,
                 "disable_comment":True if creator.get("comment_disabled") else False,
                 "privacy_level":privacy,"auto_add_music":False},
                 "source_info":{"source":"PULL_FROM_URL","photo_cover_index":0,"photo_images":urls},
                 "post_mode":"DIRECT_POST","media_type":"PHOTO"}
        pr=requests.post(PHOTO_POST_URL,headers=api_headers(account["access_token"]),json=payload,timeout=60)
        try: result=pr.json()
        except ValueError: result={"error":{"code":"invalid_json","message":pr.text[:1000]}}
        err=result.get("error",{}); publish_id=result.get("data",{}).get("publish_id")
        if not pr.ok or err.get("code") not in ("ok","",None) or not publish_id:
            return False,json.dumps(result,ensure_ascii=False)[:1200],publish_id
        return True,"PROCESSING",publish_id
    except Exception as e: return False,str(e),None

def _batch_state(batch_id):
    c=db(); b=c.execute("SELECT * FROM bulk_batches WHERE id=?",(batch_id,)).fetchone(); c.close(); return b

def _run_bulk_queue():
    global _bulk_worker_running
    try:
        while True:
            c=db()
            job=c.execute("""SELECT j.*,b.file_path,b.original_name,b.caption,b.privacy_level,b.category,b.post_type,b.interval_seconds,b.paused,b.scheduled_at
                              FROM bulk_jobs j JOIN bulk_batches b ON b.id=j.batch_id
                              WHERE j.status='QUEUED' AND COALESCE(b.paused,0)=0
                                AND (b.scheduled_at IS NULL OR b.scheduled_at='' OR b.scheduled_at<=?)
                              ORDER BY j.id ASC LIMIT 1""",(datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)).fetchone()
            c.close()
            if not job:
                # Keep the lightweight worker alive when a future scheduled batch exists.
                c=db(); future=c.execute("""SELECT id FROM bulk_batches
                    WHERE status IN ('SCHEDULED','QUEUED') AND COALESCE(paused,0)=0
                    AND scheduled_at IS NOT NULL AND scheduled_at<>''
                    AND scheduled_at>? ORDER BY scheduled_at ASC LIMIT 1""",
                    (datetime.now().strftime("%Y-%m-%d %H:%M:%S"),)).fetchone(); c.close()
                if future:
                    time.sleep(15); continue
                break
            account=get_account(job["open_id"])
            c=db(); now=datetime.utcnow().isoformat()
            c.execute("UPDATE bulk_jobs SET status='PROCESSING',attempts=attempts+1,started_at=? WHERE id=?",(now,job["id"]))
            c.execute("UPDATE bulk_batches SET status='PROCESSING',started_at=COALESCE(started_at,?) WHERE id=?",(now,job["batch_id"]))
            c.commit(); c.close()
            if not account: ok,msg,publish_id=False,"Akun sudah tidak tersedia.",None
            elif job["post_type"]=="video":
                ok,msg,publish_id=_bulk_publish_video(account,job["file_path"],job["original_name"],job["caption"] or "",job["privacy_level"])
            else:
                try: paths=json.loads(job["file_path"])
                except Exception: paths=[]
                ok,msg,publish_id=_bulk_publish_photo(account,paths,job["caption"] or "",job["privacy_level"])
            c=db(); status="SENT" if ok else "FAILED"
            c.execute("UPDATE bulk_jobs SET status=?,publish_id=?,last_error=?,finished_at=? WHERE id=?",(status,publish_id,None if ok else msg,datetime.utcnow().isoformat(),job["id"]))
            if ok: c.execute("UPDATE bulk_batches SET success_count=success_count+1 WHERE id=?",(job["batch_id"],))
            else: c.execute("UPDATE bulk_batches SET failed_count=failed_count+1 WHERE id=?",(job["batch_id"],))
            remain=c.execute("SELECT COUNT(*) AS n FROM bulk_jobs WHERE batch_id=? AND status IN ('QUEUED','PROCESSING')",(job["batch_id"],)).fetchone()["n"]
            if int(remain)==0: c.execute("UPDATE bulk_batches SET status='DONE',finished_at=? WHERE id=?",(datetime.utcnow().isoformat(),job["batch_id"]))
            c.commit(); c.close()
            b=_batch_state(job["batch_id"])
            delay=max(12,int((b["interval_seconds"] if b and b["interval_seconds"] else BULK_START_INTERVAL_SECONDS)))
            # Sleep in 1-second slices so Pause can take effect promptly between accounts.
            for _ in range(delay):
                time.sleep(1)
                b=_batch_state(job["batch_id"])
                if b and int(b["paused"] or 0)==1: break
    finally:
        with _bulk_worker_lock: _bulk_worker_running=False

def _ensure_bulk_worker():
    global _bulk_worker_running
    with _bulk_worker_lock:
        if _bulk_worker_running: return
        _bulk_worker_running=True
        threading.Thread(target=_run_bulk_queue,daemon=True,name="DashboardFYEPE-bulk-worker").start()

@app.route("/bulk-upload",methods=["GET","POST"])
def bulk_upload():
    c=db(); categories=c.execute("""SELECT category,COUNT(*) AS total FROM accounts WHERE category IS NOT NULL AND TRIM(category)<>'' GROUP BY category ORDER BY category""").fetchall(); batches=c.execute("SELECT * FROM bulk_batches ORDER BY id DESC LIMIT 20").fetchall()
    latest_batch=batches[0] if batches else None; latest_jobs=[]
    if latest_batch:
        latest_jobs=c.execute("""SELECT j.*,a.display_name,a.username FROM bulk_jobs j LEFT JOIN accounts a ON a.open_id=j.open_id WHERE j.batch_id=? ORDER BY j.id""",(latest_batch["id"],)).fetchall()
    c.close()
    if request.method=="GET": return render_template("bulk_upload.html",categories=categories,batches=batches,interval=BULK_START_INTERVAL_SECONDS,latest_batch=latest_batch,latest_jobs=latest_jobs)
    category=request.form.get("category","").strip(); caption=request.form.get("caption","").strip(); privacy=request.form.get("privacy_level","SELF_ONLY").strip(); consent=request.form.get("consent")=="yes"
    post_type=request.form.get("post_type","video").strip().lower(); interval=max(12,min(3600,int(request.form.get("interval_seconds",BULK_START_INTERVAL_SECONDS) or BULK_START_INTERVAL_SECONDS)))
    publish_when=request.form.get("publish_when","now").strip().lower()
    scheduled_at=""
    if publish_when=="scheduled":
        d=request.form.get("schedule_date","").strip(); t=request.form.get("schedule_time","").strip()
        try:
            dt=datetime.strptime(f"{d} {t}", "%Y-%m-%d %H:%M")
            if dt<=datetime.now(): raise ValueError()
            scheduled_at=dt.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            flash("Isi tanggal dan jam schedule yang valid dan harus setelah waktu sekarang.","error"); return redirect(url_for("bulk_upload"))
    if post_type not in {"video","photo","carousel"}: post_type="video"
    if not consent: flash("Centang persetujuan bulk posting.","error"); return redirect(url_for("bulk_upload"))
    if not category: flash("Pilih kategori akun.","error"); return redirect(url_for("bulk_upload"))
    c=db(); accounts=c.execute("SELECT open_id FROM accounts WHERE LOWER(TRIM(category))=LOWER(TRIM(?)) ORDER BY connected_at",(category,)).fetchall()
    if not accounts: c.close(); flash("Tidak ada akun dalam kategori tersebut.","error"); return redirect(url_for("bulk_upload"))
    folder=os.path.join(PHOTO_UPLOAD_DIR,"bulk_"+secrets.token_urlsafe(10).replace("-","").replace("_","")); os.makedirs(folder,exist_ok=True)
    stored_value=""; original_name=""
    if post_type=="video":
        f=request.files.get("video")
        if not f or not f.filename: c.close(); flash("Pilih satu file video.","error"); return redirect(url_for("bulk_upload"))
        ext=os.path.splitext(f.filename.lower())[1]
        if ext not in {".mp4",".mov",".webm"}: c.close(); flash("Format video harus MP4, MOV, atau WEBM.","error"); return redirect(url_for("bulk_upload"))
        data=f.read()
        if not data or len(data)>MAX_UPLOAD_BYTES: c.close(); flash("Video kosong atau melebihi 64 MB.","error"); return redirect(url_for("bulk_upload"))
        stored_value=os.path.join(folder,"video"+ext); open(stored_value,"wb").write(data); original_name=f.filename
    else:
        files=[f for f in request.files.getlist("photos") if f and f.filename]
        need=1 if post_type=="photo" else 2
        if len(files)<need or (post_type=="photo" and len(files)!=1) or len(files)>35:
            c.close(); flash("Photo membutuhkan tepat 1 gambar; Carousel membutuhkan 2–35 gambar.","error"); return redirect(url_for("bulk_upload"))
        paths=[]
        for i,f in enumerate(files,1):
            ext=os.path.splitext(f.filename.lower())[1]
            if ext not in {".jpg",".jpeg",".png",".webp"}: c.close(); flash("Format foto harus JPG, JPEG, PNG, atau WEBP.","error"); return redirect(url_for("bulk_upload"))
            fp=os.path.join(folder,f"{i:02d}{ext}"); f.save(fp); paths.append(fp)
        stored_value=json.dumps(paths); original_name=" | ".join(f.filename for f in files)
    now=datetime.utcnow().isoformat(); initial_status="SCHEDULED" if scheduled_at else "QUEUED"
    if c.is_pg:
        cur=c.execute("""INSERT INTO bulk_batches(category,post_type,caption,privacy_level,file_path,original_name,total_accounts,queued_count,status,created_at,interval_seconds,paused,scheduled_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) RETURNING id""",(category,post_type,caption,privacy,stored_value,original_name,len(accounts),len(accounts),initial_status,now,interval,0,scheduled_at)); batch_id=cur.fetchone()["id"]
    else:
        cur=c.execute("""INSERT INTO bulk_batches(category,post_type,caption,privacy_level,file_path,original_name,total_accounts,queued_count,status,created_at,interval_seconds,paused,scheduled_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""",(category,post_type,caption,privacy,stored_value,original_name,len(accounts),len(accounts),initial_status,now,interval,0,scheduled_at)); batch_id=cur.lastrowid
    for a in accounts: c.execute("INSERT INTO bulk_jobs(batch_id,open_id,status,attempts,created_at) VALUES(?,?,?,?,?)",(batch_id,a["open_id"],"QUEUED",0,now))
    c.commit(); c.close(); _ensure_bulk_worker(); flash(f"Bulk {post_type} masuk antrean untuk {len(accounts)} akun kategori {category}.","success"); return redirect(url_for("bulk_status",batch_id=batch_id))

@app.get("/bulk-upload/<int:batch_id>")
def bulk_status(batch_id):
    _ensure_bulk_worker(); c=db(); batch=c.execute("SELECT * FROM bulk_batches WHERE id=?",(batch_id,)).fetchone(); jobs=c.execute("""SELECT j.*,a.display_name,a.username FROM bulk_jobs j LEFT JOIN accounts a ON a.open_id=j.open_id WHERE j.batch_id=? ORDER BY j.id""",(batch_id,)).fetchall(); c.close()
    if not batch: return "Batch tidak ditemukan",404
    return render_template("bulk_status.html",batch=batch,jobs=jobs,interval=batch["interval_seconds"] or BULK_START_INTERVAL_SECONDS)

@app.post("/bulk-upload/<int:batch_id>/pause")
def bulk_pause(batch_id):
    c=db(); c.execute("UPDATE bulk_batches SET paused=1,status='PAUSED' WHERE id=? AND status<>'DONE'",(batch_id,)); c.commit(); c.close(); flash("Antrean dijeda setelah proses akun yang sedang berjalan selesai.","success"); return redirect(url_for("bulk_status",batch_id=batch_id))

@app.post("/bulk-upload/<int:batch_id>/resume")
def bulk_resume(batch_id):
    c=db(); c.execute("UPDATE bulk_batches SET paused=0,status='PROCESSING' WHERE id=? AND status<>'DONE'",(batch_id,)); c.commit(); c.close(); _ensure_bulk_worker(); flash("Antrean dilanjutkan.","success"); return redirect(url_for("bulk_status",batch_id=batch_id))

@app.post("/bulk-upload/<int:batch_id>/interval")
def bulk_interval(batch_id):
    try: seconds=max(12,min(3600,int(request.form.get("interval_seconds","12"))))
    except ValueError: seconds=12
    c=db(); c.execute("UPDATE bulk_batches SET interval_seconds=? WHERE id=?",(seconds,batch_id)); c.commit(); c.close(); flash(f"Jeda antrean diubah menjadi {seconds} detik.","success"); return redirect(url_for("bulk_status",batch_id=batch_id))

@app.get("/schedule")
def schedule_page():
    c = db()
    rows = c.execute("""SELECT s.*, a.display_name
                        FROM scheduled_posts s
                        LEFT JOIN accounts a ON a.open_id=s.open_id
                        ORDER BY s.scheduled_at ASC""").fetchall()
    c.close()
    return render_template("schedule.html", schedules=rows)

@app.post("/schedule/<int:schedule_id>/delete")
def delete_schedule(schedule_id):
    c = db()
    c.execute("DELETE FROM scheduled_posts WHERE id=?", (schedule_id,))
    c.commit()
    c.close()
    flash("Scheduled post dihapus.", "success")
    return redirect(url_for("schedule_page"))

@app.get("/health")
def health():
    return {"status": "ok", "version": "DashboardFYEPE V2"}

@app.errorhandler(413)
def too_large(_):
    return "File terlalu besar. Maksimum 64 MB untuk DashboardFYEPE V2.", 413


# Ecommerce Phase 1: manual internal records only, no Shop/Affiliate API access.
def _ecom_scope(user):
    if user["role"] == "superadmin":
        return "", ()
    return " WHERE owner_user_id=?", (user["id"],)

@app.route("/ecommerce", methods=["GET", "POST"])
@role_required("superadmin", "admin_medsos")
def ecommerce_dashboard():
    u = current_user()
    if "ecommerce_csrf" not in session:
        session["ecommerce_csrf"] = secrets.token_urlsafe(24)
    c = db()
    if request.method == "POST":
        if not secrets.compare_digest(request.form.get("csrf_token", ""), session["ecommerce_csrf"]):
            c.close()
            abort(400)
        action = request.form.get("action", "")
        if action == "add_account":
            kind = request.form.get("account_type", "").upper()
            name = request.form.get("account_name", "").strip()[:120]
            if kind not in ("SELLER", "AFFILIATE") or not name:
                flash("Jenis dan nama akun wajib diisi.", "error")
            else:
                scope, params = _ecom_scope(u)
                n = c.execute("SELECT COUNT(*) AS n FROM ecommerce_accounts" + scope, params).fetchone()["n"]
                if int(n) >= 100:
                    flash("Batas 100 akun Ecommerce tercapai untuk cakupan Anda.", "error")
                else:
                    now = datetime.now(timezone.utc).isoformat()
                    owner = u["id"] if u["role"] == "admin_medsos" else u["id"]
                    c.execute("""INSERT INTO ecommerce_accounts
                        (account_type,account_name,tiktok_username,shop_name,notes,status,owner_user_id,created_at,updated_at)
                        VALUES(?,?,?,?,?,?,?,?,?)""",
                        (kind,name,request.form.get("tiktok_username", "").strip()[:120],
                         request.form.get("shop_name", "").strip()[:120],
                         request.form.get("notes", "").strip()[:500],"NOT_CONNECTED",owner,now,now))
                    c.commit()
                    flash("Akun dicatat. Belum terhubung ke TikTok Shop API.", "success")
        elif action == "add_product":
            try:
                aid = int(request.form.get("account_id", "0"))
                stock = int(request.form.get("stock", "0"))
                price = float(request.form.get("price", "0"))
            except (TypeError,ValueError):
                aid, stock, price = 0, -1, -1
            name = request.form.get("product_name", "").strip()[:160]
            scope, params = _ecom_scope(u)
            allowed = c.execute("SELECT id FROM ecommerce_accounts WHERE id=?" + (" AND owner_user_id=?" if scope else ""), (aid, *params)).fetchone()
            if not allowed or not name or stock < 0 or price < 0 or price > 1e12:
                flash("Data produk tidak valid atau akun tidak diizinkan.", "error")
            else:
                c.execute("INSERT INTO ecommerce_products(account_id,sku,product_name,price,stock,created_at) VALUES(?,?,?,?,?,?)",
                    (aid,request.form.get("sku", "").strip()[:80],name,price,stock,datetime.now(timezone.utc).isoformat()))
                c.commit()
                flash("Produk internal berhasil dicatat.", "success")
        c.close()
        return redirect(url_for("ecommerce_dashboard"))
    scope, params = _ecom_scope(u)
    accounts = [dict(r) for r in c.execute("SELECT * FROM ecommerce_accounts" + scope + " ORDER BY id DESC", params).fetchall()]
    product_where = " WHERE a.owner_user_id=?" if scope else ""
    products = [dict(r) for r in c.execute("""SELECT p.*,a.account_name,a.account_type FROM ecommerce_products p
        JOIN ecommerce_accounts a ON a.id=p.account_id""" + product_where + " ORDER BY p.id DESC LIMIT 100",params).fetchall()]
    c.close()
    return render_template("ecommerce.html", user=u, accounts=accounts, products=products,
        seller_count=sum(a["account_type"] == "SELLER" for a in accounts),
        affiliate_count=sum(a["account_type"] == "AFFILIATE" for a in accounts),
        csrf_token=session["ecommerce_csrf"])


# === Phone Farm Monitoring, outbound-only gateway (no additional Render env vars) ===
def _pf_ensure_gmail_table(c):
    # Hanya menyimpan alamat email, tidak menyimpan password atau token login.
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_gmail_assignments (
        gateway_id TEXT NOT NULL, serial TEXT NOT NULL, email TEXT NOT NULL,
        updated_at TEXT NOT NULL, PRIMARY KEY(gateway_id,serial))""")
    c.commit()


def _pf_ensure_gmail_tiers(c):
    """Additive migration; preserve legacy Gmail as tier 1."""
    _pf_ensure_gmail_table(c)
    c.execute("""CREATE TABLE IF NOT EXISTS phonefarm_gmail_tiers (
        gateway_id TEXT NOT NULL, serial TEXT NOT NULL,
        tier INTEGER NOT NULL CHECK(tier BETWEEN 1 AND 10),
        email TEXT NOT NULL, updated_at TEXT NOT NULL,
        PRIMARY KEY(gateway_id,serial,tier))""")
    c.execute("""INSERT INTO phonefarm_gmail_tiers(gateway_id,serial,tier,email,updated_at)
        SELECT a.gateway_id,a.serial,1,a.email,a.updated_at
        FROM phonefarm_gmail_assignments a
        WHERE NOT EXISTS (SELECT 1 FROM phonefarm_gmail_tiers t
          WHERE t.gateway_id=a.gateway_id AND t.serial=a.serial AND t.tier=1)""")
    # Legacy rows are now represented in tier storage; prevent deleted Tier 1 rows reappearing.
    c.execute("DELETE FROM phonefarm_gmail_assignments")
    c.commit()



def _pf_queue_open_gmail(c, gateway_id, serial, user_id, action="OPEN_GMAIL", email=None):
    now = datetime.now(timezone.utc)
    device = c.execute("SELECT last_seen FROM phonefarm_devices WHERE gateway_id=? AND serial=?", (gateway_id,serial)).fetchone()
    if not device:
        return "HP tidak ditemukan"
    try:
        if (now-datetime.fromisoformat(device["last_seen"])).total_seconds() > 100:
            return "HP offline"
    except (TypeError, ValueError):
        return "Heartbeat HP tidak valid"
    existing = c.execute("SELECT COUNT(*) AS n FROM phonefarm_commands WHERE gateway_id=? AND serial=? AND status IN ('PENDING','RUNNING')",(gateway_id,serial)).fetchone()["n"]
    if existing:
        return "HP sedang menjalankan perintah lain"
    payload = json.dumps({"email": email}) if action == "ADD_GOOGLE_ACCOUNT" else None
    c.execute("INSERT INTO phonefarm_commands(id,gateway_id,serial,action,status,requested_by,created_at,updated_at,result) VALUES(?,?,?,?,?,?,?,?,?)",
        (secrets.token_hex(16),gateway_id,serial,action,"PENDING",user_id,now.isoformat(),now.isoformat(),payload))
    return None


# PhoneFarm access control: authorization is enforced for every browser endpoint.
_PHONEFARM_OWNERS = {"anggi": "PhoneFarm Anggi", "fathan": "PhoneFarm Fathan", "mutiara": "PhoneFarm Mutiara"}

def _pf_allowed_farm_names(user=None):
    user = user or current_user()
    if not user:
        return set()
    if user["role"] in ("superadmin", "supervisor_caprof"):
        return set(_PHONEFARM_OWNERS.values())
    if user["role"] == "admin_medsos":
        username = (user["username"] or "").strip().lower()
        name = _PHONEFARM_OWNERS.get(username)
        return {name} if name else set()
    return set()

def _pf_require_gateway(gateway_id):
    if not gateway_id:
        abort(403)
    c = db()
    try:
        gateway = c.execute("SELECT name FROM phonefarm_gateways WHERE id=?", (gateway_id,)).fetchone()
    finally:
        c.close()
    if not gateway or gateway["name"] not in _pf_allowed_farm_names():
        abort(403)

def _pf_require_command(command_id):
    c = db()
    try:
        row = c.execute("SELECT gateway_id FROM phonefarm_commands WHERE id=?", (command_id,)).fetchone()
    finally:
        c.close()
    if not row:
        abort(403)
    _pf_require_gateway(row["gateway_id"])

@app.post("/phonefarm/gmail/assign")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_gmail_assign():
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    import csv as _csv
    import io as _io
    mode = request.form.get("mode", "single")
    gateway_id = request.form.get("gateway_id", "").strip()
    _pf_require_gateway(gateway_id)
    if not re.fullmatch(r"[0-9a-f]{16}",gateway_id):
        abort(400)
    c=db(); _pf_ensure_gmail_tiers(c)
    if not c.execute("SELECT 1 FROM phonefarm_gateways WHERE id=?",(gateway_id,)).fetchone():
        c.close();abort(404)
    operation=request.form.get("gmail_operation", "input")
    if operation not in ("input", "add"):
        c.close();abort(400)
    try:
        tier=int(request.form.get("tier", "1"))
    except (TypeError,ValueError):
        tier=0
    if not 1 <= tier <= 10:
        c.close();abort(400)
    entries=[]  # (nomor baris, nomor HP, email)
    if mode == "single":
        raw_numbers=request.form.getlist("panda_number")
        raw_emails=request.form.getlist("email")
        if len(raw_numbers)!=len(raw_emails):
            c.close();flash("Daftar HP dan email tidak sesuai.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
        entries=[(i,n,e) for i,(n,e) in enumerate(zip(raw_numbers,raw_emails),1) if e.strip()]
    elif mode == "bulk":
        upload=request.files.get("gmail_file")
        if not upload or not upload.filename:
            c.close();flash("Pilih file TXT/CSV.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
        content=upload.stream.read(100_001)
        if len(content)>100_000 or not upload.filename.lower().endswith((".txt",".csv")):
            c.close();flash("File harus TXT/CSV maksimal 100 KB.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
        source=content.decode("utf-8-sig",errors="replace")
        for lineno,line in enumerate(source.splitlines(),1):
            line=line.strip()
            if not line or line.startswith("#"):
                continue
            # CSV/TXT mendukung koma, titik koma, atau TAB. Email tidak berisi pemisah ini.
            delimiter="\t" if "\t" in line else ";" if ";" in line and "," not in line else ","
            row=next(_csv.reader([line],delimiter=delimiter,skipinitialspace=True),[])
            if not row:
                continue
            heading=re.sub(r"[^a-z0-9]","",row[0].strip().lower())
            if heading in ("no","nomor","hp","nohp","nomorhp","nomorpanda","pandanumber"):
                continue
            if len(row)!=2:
                c.close();flash(f"Baris {lineno}: format salah. Gunakan nomorHP,email (contoh: 1,akun@gmail.com). Tidak ada perintah dikirim.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
            entries.append((lineno,row[0],row[1]))
    else:
        c.close();abort(400)
    if not entries or len(entries)>100:
        c.close();flash("Isi minimal satu email dan maksimal 100 HP.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
    device_rows=c.execute("SELECT serial,panda_number,last_seen FROM phonefarm_devices WHERE gateway_id=? AND panda_number IS NOT NULL",(gateway_id,)).fetchall()
    devices={int(r["panda_number"]):r for r in device_rows}
    checked=[]; used=set(); valid_email=re.compile(r"^[^\s@,;]+@[^\s@,;]+\.[^\s@,;]+$")
    for lineno,raw_number,raw_email in entries:
        number_text=str(raw_number).strip()
        number_match=re.fullmatch(r"(?:HP\s*[-#:]?\s*)?0*([1-9][0-9]*)",number_text,re.IGNORECASE)
        number=int(number_match.group(1)) if number_match else -1
        email=str(raw_email).strip().lower()
        reason=None
        if number not in devices:
            reason=f"Nomor HP '{number_text}' tidak ditemukan pada nomor Panda PhoneFarm yang dipilih"
        elif number in used:
            reason=f"HP {number} tercantum lebih dari sekali dalam file"
        elif not valid_email.fullmatch(email) or len(email)>254:
            reason=f"alamat email untuk HP {number} tidak valid"
        if reason:
            c.close();flash(f"Baris {lineno}: {reason}. Nomor Panda tersedia: {', '.join(map(str,sorted(devices))) or 'belum ada'}. Tidak ada perintah dikirim.","error");return redirect(url_for("phonefarm_page")+"#pf-fold-1")
        used.add(number);checked.append((number,devices[number]["serial"],email))
    now=datetime.now(timezone.utc).isoformat()
    sent=0; errors=[]
    for number,serial,email in checked:
        # Simpan email hanya untuk HP yang dituju; data HP lain tidak diubah.
        c.execute("""INSERT INTO phonefarm_gmail_tiers(gateway_id,serial,tier,email,updated_at)
            VALUES(?,?,?,?,?) ON CONFLICT(gateway_id,serial,tier) DO UPDATE SET email=excluded.email,updated_at=excluded.updated_at""",(gateway_id,serial,tier,email,now))
        err=_pf_queue_open_gmail(c,gateway_id,serial,current_user()["id"],"ADD_GOOGLE_ACCOUNT",email)
        if err:errors.append(f"HP {number}: {err}")
        else:sent+=1
    c.commit();c.close()
    if errors:
        flash(f"{len(checked)} email tersimpan; {sent} perintah diantrikan; {len(errors)} belum dikirim. "+"; ".join(errors[:8]),"error")
    else:
        flash(f"{len(checked)} email tersimpan dan {sent} perintah pembukaan Login Google diantrikan. Periksa hasil di Riwayat Perintah; autentikasi dilakukan pada HP.","success")
    return redirect(url_for("phonefarm_page")+"#pf-fold-1")


@app.post("/phonefarm/gmail/edit")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_gmail_edit():
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    gateway_id=request.form.get("gateway_id", "").strip()
    _pf_require_gateway(gateway_id)
    serial=request.form.get("serial", "").strip()
    email=request.form.get("email", "").strip().lower()
    try: tier=int(request.form.get("tier", "0"))
    except (ValueError,TypeError): tier=0
    if not 1 <= tier <= 10 or not serial or len(serial)>160 or len(email)>254 or not re.fullmatch(r"[^\s@,]+@[^\s@,]+\.[^\s@,]+",email):
        abort(400)
    c=db();_pf_ensure_gmail_tiers(c)
    if not c.execute("SELECT 1 FROM phonefarm_devices WHERE gateway_id=? AND serial=?",(gateway_id,serial)).fetchone():
        c.close();abort(404)
    updated=c.execute("UPDATE phonefarm_gmail_tiers SET email=?,updated_at=? WHERE gateway_id=? AND serial=? AND tier=?",(email,datetime.now(timezone.utc).isoformat(),gateway_id,serial,tier))
    if updated.rowcount == 0:
        c.close();abort(404)
    c.commit();c.close()
    flash("Gmail Tier %s diperbarui pada database; akun Google di HP tidak berubah."%tier,"success")
    return redirect(url_for("phonefarm_page")+"#pf-gmail-saved")


@app.post("/phonefarm/gmail/clear")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_gmail_clear():
    # Clear only stored email assignments, never Android accounts or device data.
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    gateway_id = request.form.get("gateway_id", "").strip()
    _pf_require_gateway(gateway_id)
    if not re.fullmatch(r"[0-9a-f]{16}", gateway_id):
        abort(400)
    mode = request.form.get("mode", "single")
    c = db()
    _pf_ensure_gmail_tiers(c)
    if not c.execute("SELECT 1 FROM phonefarm_gateways WHERE id=?", (gateway_id,)).fetchone():
        c.close()
        abort(404)
    if mode == "single":
        try: tier=int(request.form.get("tier", "1"))
        except (ValueError,TypeError): tier=0
        if not 1 <= tier <= 10: c.close();abort(400)
        serial = request.form.get("serial", "").strip()
        if not serial or len(serial) > 160:
            c.close()
            abort(400)
        c.execute("DELETE FROM phonefarm_gmail_tiers WHERE gateway_id=? AND serial=? AND tier=?", (gateway_id, serial, tier))
        if tier == 1:
            c.execute("DELETE FROM phonefarm_gmail_assignments WHERE gateway_id=? AND serial=?", (gateway_id, serial))
        message = "Penugasan Gmail pada HP diproses untuk dihapus. Akun Google di HP tidak berubah."
    elif mode == "all":
        c.execute("DELETE FROM phonefarm_gmail_tiers WHERE gateway_id=?", (gateway_id,))
        c.execute("DELETE FROM phonefarm_gmail_assignments WHERE gateway_id=?", (gateway_id,))
        message = "Seluruh penugasan Gmail pada PhoneFarm terpilih dihapus. PhoneFarm lain tidak berubah."
    else:
        c.close()
        abort(400)
    c.commit()
    c.close()
    flash(message, "success")
    return redirect(url_for("phonefarm_page") + "#pf-gmail-saved")


@app.get("/phonefarm")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_page():
    if "phonefarm_csrf" not in session:
        session["phonefarm_csrf"] = secrets.token_urlsafe(24)
    allowed_farms = _pf_allowed_farm_names()
    if not allowed_farms: abort(403)
    c = db()
    _pf_ensure_gmail_tiers(c)
    gmail_tier_rows = [dict(r) for r in c.execute("SELECT gateway_id,serial,tier,email FROM phonefarm_gmail_tiers ORDER BY tier,serial").fetchall()]
    # Category comes from the same accounts table used by Account List.
    # Do not expose access/refresh tokens to the PhoneFarm template.
    tiktok_accounts = [dict(r) for r in c.execute("SELECT email,category FROM accounts WHERE email IS NOT NULL AND TRIM(email)<>'' ORDER BY connected_at DESC").fetchall()]
    gmail_assignments = {(r["gateway_id"],r["serial"]):r["email"] for r in gmail_tier_rows if r["tier"]==1}
    gateways = [dict(r) for r in c.execute("SELECT id,name,created_at,last_seen FROM phonefarm_gateways ORDER BY created_at DESC").fetchall()]
    devices = [dict(r) for r in c.execute("SELECT d.*,g.name AS gateway_name FROM phonefarm_devices d JOIN phonefarm_gateways g ON g.id=d.gateway_id ORDER BY g.name,CASE WHEN d.panda_number IS NULL THEN 1 ELSE 0 END,d.panda_number,d.serial").fetchall()]
    gateways = [g for g in gateways if g["name"] in allowed_farms]
    allowed_ids = {g["id"] for g in gateways}
    devices = [d for d in devices if d["gateway_id"] in allowed_ids]
    gmail_assignments = {k:v for k,v in gmail_assignments.items() if k[0] in allowed_ids}
    gmail_tier_rows = [r for r in gmail_tier_rows if r["gateway_id"] in allowed_ids]
    c.close()
    now = datetime.now(timezone.utc)
    for d in devices:
        try:
            d["online"] = (now-datetime.fromisoformat(d["last_seen"])).total_seconds() < 100
        except (TypeError, ValueError):
            d["online"] = False
    c = db()
    recent_commands = [dict(r) for r in c.execute("SELECT gateway_id,serial,action,status,created_at,result FROM phonefarm_commands ORDER BY created_at DESC LIMIT 100").fetchall() if r["gateway_id"] in allowed_ids and r["action"] != "SCREENSHOT"][:20]
    c.close()
    return render_template("phonefarm.html", user=current_user(), allowed_farms=allowed_farms, can_manage_gateways=current_user()["role"]=="superadmin", gateways=gateways, devices=devices, recent_commands=recent_commands,
                           csrf_token=session["phonefarm_csrf"], pairing_token=session.pop("phonefarm_new_token",None), gmail_assignments=gmail_assignments, gmail_tier_rows=gmail_tier_rows, tiktok_accounts=tiktok_accounts, gmail_email_lookup={gid+"|"+serial:email for (gid,serial),email in gmail_assignments.items()})

@app.post("/phonefarm/gateway/<gateway_id>/rename")
@role_required("superadmin")
def phonefarm_gateway_rename(gateway_id):
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    name = request.form.get("name", "").strip()[:80]
    if not name or not re.fullmatch(r"[0-9a-f]{16}", gateway_id):
        abort(400)
    c = db()
    exists = c.execute("SELECT id FROM phonefarm_gateways WHERE id=?", (gateway_id,)).fetchone()
    if not exists:
        c.close(); abort(404)
    if name not in _PHONEFARM_OWNERS.values(): c.close(); abort(400)
    c.execute("UPDATE phonefarm_gateways SET name=? WHERE id=?", (name, gateway_id))
    c.commit(); c.close()
    audit_write("PHONEFARM_RENAME", "Nama kluster diperbarui")
    flash("Nama kluster berhasil diperbarui.", "success")
    return redirect(url_for("phonefarm_page"))

@app.post("/phonefarm/device/number")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_device_number():
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    gid = request.form.get("gateway_id", "")
    _pf_require_gateway(gid)
    serial = request.form.get("serial", "")
    raw = request.form.get("panda_number", "").strip()
    if not re.fullmatch(r"[0-9a-f]{16}", gid) or not re.fullmatch(r"[A-Za-z0-9._:-]{4,70}", serial):
        abort(400)
    try:
        number = int(raw) if raw else None
    except ValueError:
        abort(400)
    if number is not None and not 1 <= number <= 100:
        abort(400)
    c = db()
    device = c.execute("SELECT 1 FROM phonefarm_devices WHERE gateway_id=? AND serial=?", (gid, serial)).fetchone()
    if not device:
        c.close(); abort(404)
    if number is not None:
        duplicate = c.execute("SELECT serial FROM phonefarm_devices WHERE gateway_id=? AND panda_number=? AND serial<>?", (gid, number, serial)).fetchone()
        if duplicate:
            c.close()
            flash("Nomor Panda tersebut sudah dipakai HP lain dalam kluster ini.", "error")
            return redirect(url_for("phonefarm_page"))
    c.execute("UPDATE phonefarm_devices SET panda_number=? WHERE gateway_id=? AND serial=?", (number, gid, serial))
    c.commit(); c.close()
    audit_write("PHONEFARM_NUMBER", "Mapping nomor Panda diperbarui")
    flash("Nomor HP Panda berhasil disimpan.", "success")
    return redirect(url_for("phonefarm_page"))

@app.post("/phonefarm/pair")
@role_required("superadmin")
def phonefarm_pair():
    if not secrets.compare_digest(request.form.get("csrf_token", ""),session.get("phonefarm_csrf", "")):
        abort(403)
    name = request.form.get("name", "").strip()[:80] or "Komputer Panda"
    gid = secrets.token_hex(8)
    token = secrets.token_urlsafe(36)
    c = db()
    c.execute("INSERT INTO phonefarm_gateways(id,name,token_hash,created_at) VALUES(?,?,?,?)",
              (gid,name,generate_password_hash(token),audit_now()))
    c.commit();c.close()
    session["phonefarm_new_token"] = gid+":"+token
    audit_write("PHONEFARM_PAIR", "Gateway dibuat: "+name)
    return redirect(url_for("phonefarm_page"))

@app.post("/phonefarm/gateway/<gateway_id>/refresh-token")
@role_required("superadmin")
def phonefarm_gateway_refresh_token(gateway_id):
    if not secrets.compare_digest(request.form.get("csrf_token", ""), session.get("phonefarm_csrf", "")):
        abort(403)
    if not re.fullmatch(r"[0-9a-f]{16}", gateway_id):
        abort(400)
    c = db()
    gateway = c.execute("SELECT id FROM phonefarm_gateways WHERE id=?", (gateway_id,)).fetchone()
    if not gateway:
        c.close(); abort(404)
    new_token = secrets.token_urlsafe(36)
    c.execute("UPDATE phonefarm_gateways SET token_hash=? WHERE id=?", (generate_password_hash(new_token), gateway_id))
    c.commit(); c.close()
    session["phonefarm_new_token"] = gateway_id + ":" + new_token
    audit_write("PHONEFARM_TOKEN", "Token gateway diperbarui")
    return redirect(url_for("phonefarm_page"))

@app.post("/phonefarm/revoke/<gateway_id>")
@role_required("superadmin")
def phonefarm_revoke(gateway_id):
    if not secrets.compare_digest(request.form.get("csrf_token", ""),session.get("phonefarm_csrf", "")):
        abort(403)
    c=db()
    c.execute("DELETE FROM phonefarm_screens WHERE gateway_id=?",(gateway_id,))
    c.execute("DELETE FROM phonefarm_devices WHERE gateway_id=?",(gateway_id,))
    c.execute("DELETE FROM phonefarm_gateways WHERE id=?",(gateway_id,))
    c.commit();c.close()
    audit_write("PHONEFARM_REVOKE", "Gateway dinonaktifkan")
    return redirect(url_for("phonefarm_page"))

@app.post("/api/phonefarm/heartbeat")
def phonefarm_heartbeat():
    # Only this endpoint bypasses browser session authentication.
    raw = request.headers.get("Authorization", "")
    if not raw.startswith("Bearer ") or len(raw)>200:
        return jsonify(ok=False,error="unauthorized"),401
    credential = raw[7:]
    if ":" not in credential:
        return jsonify(ok=False,error="unauthorized"),401
    gid,token = credential.split(":",1)
    if not re.fullmatch(r"[0-9a-f]{16}",gid):
        return jsonify(ok=False,error="unauthorized"),401
    c=db()
    gateway=c.execute("SELECT token_hash FROM phonefarm_gateways WHERE id=?",(gid,)).fetchone()
    if not gateway or not check_password_hash(gateway["token_hash"],token):
        c.close()
        return jsonify(ok=False,error="unauthorized"),401
    payload=request.get_json(silent=True)
    if not isinstance(payload,dict) or not isinstance(payload.get("devices"),list) or len(payload["devices"])>150:
        c.close();return jsonify(ok=False,error="invalid_payload"),400
    now=audit_now()
    for item in payload["devices"]:
        if not isinstance(item,dict): continue
        serial=str(item.get("serial", ""))[:70]
        model=str(item.get("model", ""))[:70]
        if not re.fullmatch(r"[a-zA-Z0-9._:-]{4,70}",serial):continue
        try: battery=int(item.get("battery"))
        except (ValueError,TypeError): battery=None
        if battery is not None and not 0<=battery<=100: battery=None
        c.execute("""INSERT INTO phonefarm_devices(gateway_id,serial,model,battery,last_seen)
                     VALUES(?,?,?,?,?) ON CONFLICT(gateway_id,serial) DO UPDATE SET
                     model=excluded.model,battery=excluded.battery,last_seen=excluded.last_seen""",
                  (gid,serial,model,battery,now))
    c.execute("UPDATE phonefarm_gateways SET last_seen=? WHERE id=?",(now,gid))
    c.commit();c.close()
    return jsonify(ok=True)



def db_read_phonefarm_commands():
    c=db()
    try:
        return c.execute("SELECT id,serial,action,status,created_at,updated_at,result FROM phonefarm_commands ORDER BY created_at DESC LIMIT 20").fetchall()
    finally:
        c.close()

@app.post("/phonefarm/command")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_command():
    if not secrets.compare_digest(request.form.get("csrf_token", ""),session.get("phonefarm_csrf", "")):
        abort(403)
    gid=request.form.get("gateway_id", "")
    _pf_require_gateway(gid)
    serial=request.form.get("serial", "")
    action=request.form.get("action", "")
    if action not in {"HOME", "BACK", "SCREENSHOT", "OPEN_GMAIL", "OPEN_TIKTOK"} or not re.fullmatch(r"[A-Za-z0-9._:-]{4,70}",serial):
        abort(400)
    c=db()
    row=c.execute("SELECT last_seen FROM phonefarm_devices WHERE gateway_id=? AND serial=?",(gid,serial)).fetchone()
    if not row:
        c.close();abort(404)
    try:
        age=(datetime.now(timezone.utc)-datetime.fromisoformat(row["last_seen"])).total_seconds()
    except (ValueError,TypeError): age=9999
    if age>100:
        c.close();flash("Perangkat offline. Perintah tidak dikirim.","error");return redirect(url_for("phonefarm_page"))
    # Reuse a screenshot already in flight; avoid a flood of duplicate captures.
    if action == "SCREENSHOT":
        cutoff=datetime.fromtimestamp(time.time()-35,timezone.utc).isoformat(timespec="seconds")
        existing=c.execute("""SELECT id FROM phonefarm_commands WHERE gateway_id=? AND serial=?
            AND action='SCREENSHOT' AND status IN ('PENDING','RUNNING')
            AND updated_at>=? LIMIT 1""",(gid,serial,cutoff)).fetchone()
        if existing:
            c.close()
            return jsonify(ok=True,already_queued=True) if request.headers.get("X-Requested-With") == "XMLHttpRequest" else redirect(url_for("phonefarm_page"))
    pending=c.execute("SELECT COUNT(*) AS n FROM phonefarm_commands WHERE gateway_id=? AND status IN ('PENDING','RUNNING')",(gid,)).fetchone()["n"]
    same_device=c.execute("SELECT COUNT(*) AS n FROM phonefarm_commands WHERE gateway_id=? AND serial=? AND status IN ('PENDING','RUNNING')",(gid,serial)).fetchone()["n"]
    if pending>=5 or same_device:
        c.close();flash("Perangkat masih memproses perintah sebelumnya. Tunggu status selesai.","error");return redirect(url_for("phonefarm_page"))
    now=audit_now()
    c.execute("INSERT INTO phonefarm_commands(id,gateway_id,serial,action,status,requested_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",(secrets.token_hex(16),gid,serial,action,"PENDING",current_user()["id"],now,now))
    c.commit();c.close()
    if action != "SCREENSHOT":
        audit_write("PHONEFARM_COMMAND", f"{action} ke {serial}")
    if action != "SCREENSHOT":
        flash("Perintah masuk antrean. Tunggu hingga gateway mengambilnya.","success")
    return (jsonify(ok=True) if action == "SCREENSHOT" and request.headers.get("X-Requested-With") == "XMLHttpRequest" else redirect(url_for("phonefarm_page")))

@app.post("/phonefarm/remote/action")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_remote_action():
    data=request.get_json(silent=True) or {}
    if not secrets.compare_digest(str(data.get("csrf_token", "")),session.get("phonefarm_csrf", "")):
        return jsonify(ok=False,message="Token sesi tidak valid"),403
    gid=str(data.get("gateway_id", "")); serial=str(data.get("serial", "")); action=str(data.get("action", ""))
    _pf_require_gateway(gid)
    if not re.fullmatch(r"[0-9a-f]{16}",gid) or not re.fullmatch(r"[A-Za-z0-9._:-]{4,70}",serial):
        return jsonify(ok=False,message="Perangkat tidak valid"),400
    if action not in {"TAP","SWIPE","HOME","BACK","SCREENSHOT"}:
        return jsonify(ok=False,message="Aksi tidak didukung"),400
    args={}
    if action in {"TAP","SWIPE"}:
        keys=("x","y") if action=="TAP" else ("x","y","x2","y2")
        try:
            args={k:float(data[k]) for k in keys}
            if not all(0<=v<=1 for v in args.values()):raise ValueError()
        except (ValueError,TypeError,KeyError):
            return jsonify(ok=False,message="Koordinat harus antara 0 dan 1"),400
        if action=="SWIPE":args["duration"]=max(100,min(900,int(data.get("duration",300))))
    c=db()
    row=c.execute("SELECT last_seen FROM phonefarm_devices WHERE gateway_id=? AND serial=?",(gid,serial)).fetchone()
    if not row:
        c.close();return jsonify(ok=False,message="HP tidak ditemukan"),404
    try: age=(datetime.now(timezone.utc)-datetime.fromisoformat(row["last_seen"])).total_seconds()
    except (TypeError,ValueError):age=999
    if age>100:
        c.close();return jsonify(ok=False,message="HP offline"),409
    # Recover abandoned commands before evaluating per-device queue state.
    now=audit_now()
    pending_cutoff=datetime.fromtimestamp(time.time()-900,timezone.utc).isoformat(timespec="seconds")
    running_cutoff=datetime.fromtimestamp(time.time()-180,timezone.utc).isoformat(timespec="seconds")
    c.execute("UPDATE phonefarm_commands SET status='EXPIRED',updated_at=?,result='Antrean kedaluwarsa' WHERE gateway_id=? AND serial=? AND status='PENDING' AND created_at<?",(now,gid,serial,pending_cutoff))
    c.execute("UPDATE phonefarm_commands SET status='FAILED',updated_at=?,result='Gateway tidak memberi konfirmasi' WHERE gateway_id=? AND serial=? AND status='RUNNING' AND updated_at<?",(now,gid,serial,running_cutoff))
    # Screenshots are background work: pending ones may be replaced by a touch.
    if action in {"TAP","SWIPE","HOME","BACK"}:
        c.execute("""UPDATE phonefarm_commands SET status='EXPIRED',updated_at=?,result='Digantikan kontrol interaktif'
            WHERE gateway_id=? AND serial=? AND action='SCREENSHOT' AND status='PENDING'""",(now,gid,serial))
    n=c.execute("""SELECT COUNT(*) AS n FROM phonefarm_commands WHERE gateway_id=? AND serial=?
        AND status IN ('PENDING','RUNNING')
        AND (action!='SCREENSHOT' OR ?='SCREENSHOT')""",(gid,serial,action)).fetchone()["n"]
    if n:
        c.commit();c.close();return jsonify(ok=False,message="Perintah sebelumnya masih diproses",retry_after=2),409
    if action == "SCREENSHOT":
        existing=c.execute("""SELECT id FROM phonefarm_commands WHERE gateway_id=? AND serial=?
            AND action='SCREENSHOT' AND status IN ('PENDING','RUNNING') LIMIT 1""",(gid,serial)).fetchone()
        if existing:
            c.commit();c.close();return jsonify(ok=True,command_id=existing["id"],already_queued=True),202
    command_id=secrets.token_hex(16)
    c.execute("INSERT INTO phonefarm_commands(id,gateway_id,serial,action,status,requested_by,created_at,updated_at,result) VALUES(?,?,?,?,?,?,?,?,?)",(command_id,gid,serial,action,"PENDING",current_user()["id"],now,now,json.dumps(args)))
    c.commit();c.close()
    return jsonify(ok=True,command_id=command_id),202

@app.get("/phonefarm/remote/command/<command_id>")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_remote_command_status(command_id):
    _pf_require_command(command_id)
    if not re.fullmatch(r"[0-9a-f]{32}",command_id):abort(404)
    c=db()
    row=c.execute("SELECT status,action,result,created_at FROM phonefarm_commands WHERE id=? AND requested_by=?",(command_id,current_user()["id"])).fetchone()
    c.close()
    if not row:abort(404)
    # Do not return internal request arguments as a user-facing message.
    result=row["result"] if row["status"] in ("DONE","FAILED","EXPIRED") else None
    return jsonify(ok=True,status=row["status"],action=row["action"],result=result)

def _phonefarm_gateway_auth():
    raw=request.headers.get("Authorization", "")
    if not raw.startswith("Bearer ") or len(raw)>200 or ":" not in raw:
        return None
    gid,token=raw[7:].split(":",1)
    if not re.fullmatch(r"[0-9a-f]{16}",gid):return None
    c=db()
    row=c.execute("SELECT token_hash FROM phonefarm_gateways WHERE id=?",(gid,)).fetchone()
    c.close()
    return gid if row and check_password_hash(row["token_hash"],token) else None

@app.get("/api/phonefarm/commands/next")
def phonefarm_next_command():
    gid=_phonefarm_gateway_auth()
    if not gid:return jsonify(ok=False,error="unauthorized"),401
    c=db()
    now=audit_now()
    # Prevent abandoned commands from blocking new requests after gateway interruption.
    cutoff=datetime.fromtimestamp(time.time()-900,timezone.utc).isoformat(timespec="seconds")
    running_cutoff=datetime.fromtimestamp(time.time()-180,timezone.utc).isoformat(timespec="seconds")
    c.execute("UPDATE phonefarm_commands SET status='FAILED',result='Gateway tidak mengonfirmasi perintah dalam 180 detik',updated_at=? WHERE gateway_id=? AND status='RUNNING' AND updated_at<?",(now,gid,running_cutoff))
    c.execute("UPDATE phonefarm_commands SET status='EXPIRED',updated_at=? WHERE gateway_id=? AND status='PENDING' AND created_at<?",(now,gid,cutoff))
    # Interactive controls must not wait behind a backlog of screenshots.
    row=c.execute("""SELECT id,serial,action,result FROM phonefarm_commands
        WHERE gateway_id=? AND status='PENDING'
        ORDER BY CASE WHEN action IN ('TAP','SWIPE','HOME','BACK','OPEN_GMAIL','OPEN_TIKTOK','ADD_GOOGLE_ACCOUNT') THEN 0 ELSE 1 END,
                 created_at ASC LIMIT 1""",(gid,)).fetchone()
    command=None
    if row:
        cur=c.execute("UPDATE phonefarm_commands SET status='RUNNING',updated_at=? WHERE id=? AND status='PENDING'",(now,row["id"]))
        if cur.rowcount==1:command=dict(row)
    c.commit();c.close()
    return jsonify(ok=True,command=command)

@app.post("/api/phonefarm/commands/ack")
def phonefarm_ack_command():
    gid=_phonefarm_gateway_auth()
    if not gid:return jsonify(ok=False,error="unauthorized"),401
    data=request.get_json(silent=True) or {}
    command_id=data.get("id","")
    status=data.get("status","")
    if not re.fullmatch(r"[0-9a-f]{32}",command_id) or status not in {"DONE","FAILED"}:
        return jsonify(ok=False,error="invalid_payload"),400
    c=db()
    c.execute("UPDATE phonefarm_commands SET status=?,result=?,updated_at=? WHERE id=? AND gateway_id=? AND status='RUNNING'",(status,str(data.get("result", ""))[:180],audit_now(),command_id,gid))
    c.commit();c.close()
    return jsonify(ok=True)


@app.post("/api/phonefarm/screens/upload")
def phonefarm_screen_upload():
    gid=_phonefarm_gateway_auth()
    if not gid:return jsonify(ok=False,error="unauthorized"),401
    serial=request.headers.get("X-Device-Serial","")
    if not re.fullmatch(r"[A-Za-z0-9._:-]{4,70}",serial):return jsonify(ok=False,error="serial"),400
    # Cap uploads to protect database and Render memory.
    if request.content_length is None or request.content_length>2_500_000:
        return jsonify(ok=False,error="image_too_large"),413
    data=request.get_data(cache=False)
    if not data.startswith(b"\x89PNG\r\n\x1a\n") or len(data)>2_500_000:
        return jsonify(ok=False,error="invalid_png"),400
    c=db()
    found=c.execute("SELECT 1 FROM phonefarm_devices WHERE gateway_id=? AND serial=?",(gid,serial)).fetchone()
    if not found:c.close();return jsonify(ok=False,error="unknown_device"),404
    payload=psycopg2.Binary(data) if c.is_pg else data
    c.execute("""INSERT INTO phonefarm_screens(gateway_id,serial,image_data,updated_at)
        VALUES(?,?,?,?) ON CONFLICT(gateway_id,serial) DO UPDATE SET
        image_data=excluded.image_data,updated_at=excluded.updated_at""",(gid,serial,payload,audit_now()))
    c.commit();c.close()
    return jsonify(ok=True)

@app.get("/phonefarm/screen/<gateway_id>/<serial>")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_screen(gateway_id,serial):
    _pf_require_gateway(gateway_id)
    if not re.fullmatch(r"[0-9a-f]{16}",gateway_id) or not re.fullmatch(r"[A-Za-z0-9._:-]{4,70}",serial):abort(404)
    c=db()
    row=c.execute("SELECT image_data,updated_at FROM phonefarm_screens WHERE gateway_id=? AND serial=?",(gateway_id,serial)).fetchone()
    c.close()
    if not row:return jsonify(ok=False,error="Layar belum tersedia"),404
    image_data=bytes(row["image_data"])
    return Response(image_data,mimetype="image/png",headers={"Cache-Control":"no-store, private","X-Screen-Updated":row["updated_at"],"X-Content-Type-Options":"nosniff"})

@app.get("/phonefarm/screen/status/<gateway_id>/<serial>")
@role_required("superadmin", "supervisor_caprof", "admin_medsos")
def phonefarm_screen_status(gateway_id,serial):
    _pf_require_gateway(gateway_id)
    c=db()
    row=c.execute("SELECT updated_at FROM phonefarm_screens WHERE gateway_id=? AND serial=?",(gateway_id,serial)).fetchone()
    c.close()
    return jsonify(ok=True,updated_at=row["updated_at"] if row else None)


from voucher_lht import register_voucher_lht
register_voucher_lht(app, db, current_user)

# Modul Check Video YouTube
from youtube_check import register_youtube_check
register_youtube_check(app, db, role_required, current_user)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
