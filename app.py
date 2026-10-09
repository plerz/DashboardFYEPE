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
from flask import Flask, render_template, redirect, request, session, url_for, flash, send_from_directory, Response

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
    c.commit()
    c.close()

PUBLIC_ENDPOINTS = {"login", "logout", "terms_page", "privacy_page", "health", "verify_file", "tiktok_callback", "static"}

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
                return redirect(url_for("login", next=request.path))
            if u["role"] not in roles:
                flash("Anda tidak memiliki akses ke halaman tersebut.", "error")
                return redirect(url_for("customer_dashboard" if u["role"] == "customer" else "dashboard"))
            return fn(*args, **kwargs)
        return wrapped
    return deco

@app.before_request
def _init():
    init_db()
    endpoint = request.endpoint or ""
    if endpoint in PUBLIC_ENDPOINTS:
        return
    u = current_user()
    if not u:
        return redirect(url_for("login", next=request.path))
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
        return redirect(url_for("customer_dashboard" if u["role"] == "customer" else "dashboard"))
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        c = db()
        u = c.execute("SELECT * FROM users WHERE LOWER(username)=LOWER(?) AND is_active=1", (username,)).fetchone()
        if u and check_password_hash(u["password_hash"], password):
            session.clear(); session["user_id"] = u["id"]; session["role"] = u["role"]
            c.execute("UPDATE users SET last_login=? WHERE id=?", (datetime.now().isoformat(), u["id"]))
            c.commit(); c.close()
            nxt = request.args.get("next") or request.form.get("next")
            if u["role"] == "customer": return redirect(url_for("customer_dashboard"))
            return redirect(nxt if nxt and nxt.startswith("/") else url_for("dashboard"))
        c.close(); flash("Username atau password salah.", "error")
    return render_template("login.html")

@app.post("/logout")
def logout():
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
                        "display_name": account["display_name"], "username": account.get("username"),
                        "avatar_url": account.get("avatar_url"), "is_verified": account.get("is_verified", 0),
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
    if keyword and not posts:
        posts = _fetch_campaign_posts(keyword)
        _store_campaign_posts(keyword["id"], posts)
        posts = _load_campaign_posts(keyword["id"])

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
        sentiment_data = summary(db, [p["video_id"] for p in posts])
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

@app.route("/users", methods=["GET", "POST"])
@role_required("superadmin")
def users_page():
    c=db()
    if request.method == "POST":
        username=request.form.get("username","").strip(); full_name=request.form.get("full_name","").strip(); email=request.form.get("email","").strip(); role=request.form.get("role","customer").strip(); password=request.form.get("password","")
        if role not in {"superadmin","admin_medsos","customer"} or not username or not password:
            flash("Lengkapi username, password, dan role.", "error")
        else:
            try:
                c.execute("INSERT INTO users(username,full_name,email,password_hash,role,is_active,created_at) VALUES(?,?,?,?,?,?,?)", (username,full_name,email,generate_password_hash(password),role,1,datetime.now().isoformat()))
                c.commit(); flash("User berhasil dibuat.", "success")
            except Exception as e:
                c.conn.rollback(); flash("Gagal membuat user: username mungkin sudah dipakai.", "error")
    users=c.execute("SELECT id,username,full_name,email,role,is_active,created_at,last_login FROM users ORDER BY id").fetchall()
    customer_keywords=c.execute("SELECT id,customer_id,keyword,period_start,period_end FROM user_keywords ORDER BY id DESC").fetchall()
    c.close()
    return render_template("users.html", users=users, customer_keywords=customer_keywords)

@app.post("/users/<int:user_id>/update")
@role_required("superadmin")
def update_user(user_id):
    role=request.form.get("role","customer"); active=1 if request.form.get("is_active")=="1" else 0; full_name=request.form.get("full_name","").strip(); email=request.form.get("email","").strip(); password=request.form.get("password","")
    if role not in {"superadmin","admin_medsos","customer"}: return "Role tidak valid",400
    c=db()
    if password:
        c.execute("UPDATE users SET full_name=?,email=?,role=?,is_active=?,password_hash=? WHERE id=?",(full_name,email,role,active,generate_password_hash(password),user_id))
    else:
        c.execute("UPDATE users SET full_name=?,email=?,role=?,is_active=? WHERE id=?",(full_name,email,role,active,user_id))
    c.commit(); c.close(); flash("User diperbarui.","success"); return redirect(url_for("users_page"))

@app.get("/terms")
def terms_page():
    return render_template("terms.html")


@app.get("/privacy")
def privacy_page():
    return render_template("privacy.html")


@app.get("/")
@role_required("superadmin", "admin_medsos")
def dashboard():
    try: page=max(int(request.args.get("page",1)),1)
    except (TypeError,ValueError): page=1
    try: per_page=min(max(int(request.args.get("per_page",STATS_PAGE_SIZE)),10),100)
    except (TypeError,ValueError): per_page=STATS_PAGE_SIZE

    c=db(); u=current_user()
    conditions=[]; params=[]
    # Admin Medsos only sees accounts assigned to that login. Super Admin sees all.
    if u["role"]=="admin_medsos":
        conditions.append("(assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))")
        params.extend([u["id"], u["full_name"] or u["username"]])

    admin_filter=request.args.get("admin_filter", "").strip()
    if u["role"]=="superadmin" and admin_filter:
        try:
            admin_id=int(admin_filter)
            conditions.append("assigned_admin_id=?")
            params.append(admin_id)
        except ValueError:
            conditions.append("LOWER(TRIM(admin_medsos))=LOWER(TRIM(?))")
            params.append(admin_filter)

    scope_where=(" WHERE " + " AND ".join(conditions)) if conditions else ""
    total=int(c.execute("SELECT COUNT(*) AS n FROM accounts"+scope_where,tuple(params)).fetchone()["n"])

    admin_performance=c.execute("""SELECT
        COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan') AS admin_medsos,
        COUNT(*) AS account_count,
        COALESCE(SUM(video_count),0) AS post_count,
        COALESCE(SUM(view_count),0) AS total_views,
        COALESCE(SUM(likes_count),0) AS total_likes,
        COALESCE(SUM(comment_count),0) AS total_comments,
        COALESCE(SUM(shares_count),0) AS total_shares
        FROM accounts """+scope_where+
        " GROUP BY COALESCE(NULLIF(TRIM(admin_medsos),''),'Belum ditugaskan')"
        " ORDER BY COALESCE(SUM(view_count),0) DESC,COALESCE(SUM(likes_count),0) DESC,COUNT(*) DESC",
        tuple(params)).fetchall()

    total_pages=max((total+per_page-1)//per_page,1); page=min(page,total_pages); offset=(page-1)*per_page
    accounts=c.execute("""SELECT open_id,display_name,avatar_url,scope,connected_at,email,username,video_count,following_count,
        follower_count,likes_count,view_count,comment_count,shares_count,stats_updated_at,category,admin_medsos,
        assigned_admin_id,profile_deep_link,is_verified FROM accounts """+scope_where+
        " ORDER BY COALESCE(view_count,0) DESC,COALESCE(likes_count,0) DESC,connected_at DESC LIMIT ? OFFSET ?",
        tuple(params)+(per_page,offset)).fetchall()
    admins=c.execute("SELECT id,full_name,username FROM users WHERE role='admin_medsos' AND is_active=1 ORDER BY full_name,username").fetchall()
    c.close()
    return render_template("dashboard.html",accounts=accounts,admin_performance=admin_performance,page=page,per_page=per_page,
                           total_accounts=total,total_pages=total_pages,admins=admins,user=u,admin_filter=admin_filter)

@app.get("/admin/posts")
@role_required("superadmin", "admin_medsos")
def admin_posts():
    """On-demand post list. No background TikTok API calls or customer hashtag filtering."""
    u = current_user()
    try:
        page = max(int(request.args.get("page", 1)), 1)
    except (ValueError, TypeError):
        page = 1
    batch_size = 10  # Avoid 1,000 simultaneous TikTok requests.
    c = db()
    where = ""
    params = []
    if u["role"] == "admin_medsos":
        where = " WHERE (assigned_admin_id=? OR LOWER(TRIM(admin_medsos))=LOWER(TRIM(?)))"
        params = [u["id"], u["full_name"] or u["username"]]
    total = int(c.execute("SELECT COUNT(*) AS n FROM accounts" + where, tuple(params)).fetchone()["n"])
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
                title = v.get("title") or ""
                posts.append({"video_id":v.get("id"), "title":title, "create_time":v.get("create_time"),
                              "view_count":int(v.get("view_count") or 0), "like_count":int(v.get("like_count") or 0),
                              "comment_count":int(v.get("comment_count") or 0), "share_count":int(v.get("share_count") or 0),
                              "share_url":v.get("share_url"), "display_name":a["display_name"],
                              "username":a["username"], "hashtags":re.findall(r"#[\w]+", title)})
        except Exception:
            app.logger.exception("Admin post list: failed to fetch posts for an account")
            errors.append((a["display_name"], "Gagal mengambil postingan"))
    posts.sort(key=lambda p:(p["view_count"],p["like_count"]), reverse=True)
    return {"ok":True,"posts":posts,"page":page,"total_accounts":total,"has_more":page*batch_size<total,
            "scanned_accounts":len(accounts),"errors":[{"account":a,"message":m} for a,m in errors]}

@app.post("/account/<open_id>/details")
@role_required("superadmin", "admin_medsos")
def account_details(open_id):
    email=request.form.get("email","").strip(); username=request.form.get("username","").strip().lstrip("@"); category=request.form.get("category","").strip(); admin_medsos=request.form.get("admin_medsos","").strip()
    assigned_admin_id=request.form.get("assigned_admin_id") or None
    u=current_user()
    c=db()
    if u["role"]=="admin_medsos": assigned_admin_id=u["id"]; admin_medsos=u["full_name"] or u["username"]
    c.execute("UPDATE accounts SET email=?,username=?,category=?,admin_medsos=?,assigned_admin_id=? WHERE open_id=?",(email,username,category,admin_medsos,assigned_admin_id,open_id))
    c.commit(); c.close(); flash("Informasi akun berhasil disimpan.","success"); return redirect(url_for("dashboard"))


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
        total_views = 0
        total_comments = 0
        total_shares = 0
        video_list_ok = True
        if "video.list" in granted:
            cursor = None
            for _ in range(10000):
                body = {"max_count": 20}
                if cursor:
                    body["cursor"] = cursor
                vr = requests.post(VIDEO_LIST_URL, params={"fields": "id,view_count,comment_count,share_count"},
                                   headers=api_headers(access_token), json=body, timeout=30)
                vp = vr.json()
                verr = vp.get("error", {})
                if not vr.ok or verr.get("code") not in ("ok", "", None):
                    video_list_ok = False
                    break
                vd = vp.get("data", {})
                total_views += sum(int(v.get("view_count") or 0) for v in vd.get("videos", []))
                total_comments += sum(int(v.get("comment_count") or 0) for v in vd.get("videos", []))
                total_shares += sum(int(v.get("share_count") or 0) for v in vd.get("videos", []))
                if not vd.get("has_more"):
                    break
                cursor = vd.get("cursor")
                time.sleep(STATS_API_DELAY)
        c = db()
        c.execute("""UPDATE accounts SET display_name=?, avatar_url=?, username=?, video_count=?, following_count=?,
            follower_count=?, likes_count=?, is_verified=?, profile_deep_link=?, view_count=?, comment_count=?, shares_count=?, stats_updated_at=? WHERE open_id=?""",
            (user.get("display_name") or account["display_name"],
             user.get("avatar_large_url") or user.get("avatar_url_100") or user.get("avatar_url") or account["avatar_url"],
             user.get("username") or account.get("username"), user.get("video_count"), user.get("following_count"),
             user.get("follower_count"), user.get("likes_count"), 1 if user.get("is_verified") else 0, user.get("profile_deep_link") or account.get("profile_deep_link") or "", total_views, total_comments,
             total_shares,
             datetime.utcnow().isoformat(), open_id))
        c.commit(); c.close()
        if not video_list_ok and "video.list" in granted:
            return False, "Profil diperbarui, tetapi video.list gagal dibaca."
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
            time.sleep(3)


def _ensure_stats_worker():
    global _stats_worker_started
    with _stats_worker_lock:
        if not _stats_worker_started:
            threading.Thread(target=_stats_worker, daemon=True, name="stats-refresh-worker").start()
            _stats_worker_started = True


@app.post("/stats/refresh-all")
def refresh_all_stats():
    _ensure_stats_worker()
    c = db()
    rows = c.execute("SELECT open_id FROM accounts ORDER BY COALESCE(view_count,0) DESC, connected_at DESC").fetchall()
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
    return {"ok": True, "batch_id": batch_id, "total": len(rows)}


@app.get("/stats/refresh-all/<int:batch_id>")
def refresh_all_status(batch_id):
    c = db()
    batch = c.execute("SELECT * FROM stats_refresh_batches WHERE id=?", (batch_id,)).fetchone()
    c.close()
    if not batch:
        return {"ok": False, "message": "Batch tidak ditemukan."}, 404
    return {"ok": True, "batch_id": batch_id, "total": batch["total_accounts"],
            "completed": batch["completed_count"], "failed": batch["failed_count"], "status": batch["status"]}


@app.post("/account/<open_id>/refresh-stats")
def refresh_stats(open_id):
    ok, err = _refresh_account_stats(open_id)
    if ok:
        flash("Statistik akun berhasil diperbarui.", "success")
    else:
        flash("Gagal memperbarui statistik: " + (err or "unknown"), "error")
    return redirect(url_for("dashboard", page=request.args.get("page", 1)))

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
            params={"fields": "open_id,display_name,avatar_url"},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30
        )
        user = ur.json().get("data", {}).get("user", {})
    except Exception as e:
        flash("Gagal membaca profil TikTok: " + str(e), "error")
        return redirect(url_for("dashboard"))

    c = db()
    if c.is_pg:
        c.execute("""INSERT INTO accounts(
            open_id,display_name,avatar_url,access_token,refresh_token,scope,expires_in,connected_at
        ) VALUES(?,?,?,?,?,?,?,?)
        ON CONFLICT (open_id) DO UPDATE SET
            display_name=EXCLUDED.display_name,
            avatar_url=EXCLUDED.avatar_url,
            access_token=EXCLUDED.access_token,
            refresh_token=EXCLUDED.refresh_token,
            scope=EXCLUDED.scope,
            expires_in=EXCLUDED.expires_in,
            connected_at=EXCLUDED.connected_at""", (
            open_id, user.get("display_name", "TikTok account"), user.get("avatar_url", ""),
            token, data.get("refresh_token", ""), data.get("scope", ""),
            data.get("expires_in", 0), datetime.utcnow().isoformat()
        ))
    else:
        c.execute("""INSERT OR REPLACE INTO accounts(
            open_id,display_name,avatar_url,access_token,refresh_token,scope,expires_in,connected_at
        ) VALUES(?,?,?,?,?,?,?,?)""", (
            open_id, user.get("display_name", "TikTok account"), user.get("avatar_url", ""),
            token, data.get("refresh_token", ""), data.get("scope", ""),
            data.get("expires_in", 0), datetime.utcnow().isoformat()
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
    return send_from_directory(os.path.join(PHOTO_UPLOAD_DIR, batch), filename)

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

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)), debug=False)
