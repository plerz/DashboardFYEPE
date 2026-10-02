import os, secrets, sqlite3, mimetypes, json, threading, time
from urllib.parse import quote

try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
from datetime import datetime
from urllib.parse import urlencode
import requests
from flask import Flask, render_template, redirect, request, session, url_for, flash, send_from_directory

app = Flask(__name__)
app.secret_key = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))
DB = os.environ.get("DATABASE_PATH", "DashboardFYEPE.db")
DATABASE_URL = os.environ.get("DATABASE_URL", "").strip()

CLIENT_KEY = os.environ.get("TIKTOK_CLIENT_KEY", "")
CLIENT_SECRET = os.environ.get("TIKTOK_CLIENT_SECRET", "")
REDIRECT_URI = os.environ.get(
    "TIKTOK_REDIRECT_URI",
    "https://dashboardFYEPE.islammoderat.my.id/auth/tiktok/callback"
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
PUBLIC_BASE_URL = os.environ.get("PUBLIC_BASE_URL", "https://dashboardFYEPE.islammoderat.my.id").rstrip("/")
os.makedirs(PHOTO_UPLOAD_DIR, exist_ok=True)

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
        "stats_updated_at": "TEXT",
        "category": "TEXT"
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
    c.commit()
    c.close()

@app.before_request
def _init():
    init_db()

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

@app.get("/")
def dashboard():
    # Dashboard selalu menampilkan akun berdasarkan total Views tertinggi.
    # Jika Views sama, akun terbaru tetap berada lebih atas.
    c = db()
    accounts = c.execute("""
        SELECT open_id,display_name,avatar_url,scope,connected_at,
               email,username,video_count,following_count,follower_count,
               likes_count,view_count,comment_count,stats_updated_at,category
        FROM accounts
        ORDER BY COALESCE(view_count, 0) DESC, connected_at DESC
    """).fetchall()
    c.close()
    return render_template("dashboard.html", accounts=accounts)

@app.post("/account/<open_id>/details")
def account_details(open_id):
    email = request.form.get("email", "").strip()
    username = request.form.get("username", "").strip().lstrip("@")
    category = request.form.get("category", "").strip()
    c = db()
    c.execute("UPDATE accounts SET email=?, username=?, category=? WHERE open_id=?",
              (email, username, category, open_id))
    c.commit()
    c.close()
    flash("Informasi akun berhasil disimpan.", "success")
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
        "profile_deep_link"
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
        video_count=?,
        following_count=?,
        follower_count=?,
        likes_count=?
        WHERE open_id=?""", (
        user.get("display_name") or "",
        avatar,
        user.get("username") or "",
        user.get("video_count"),
        user.get("following_count"),
        user.get("follower_count"),
        user.get("likes_count"),
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


@app.post("/account/<open_id>/refresh-stats")
def refresh_stats(open_id):
    account = get_account(open_id)
    if not account:
        flash("Akun TikTok tidak ditemukan.", "error")
        return redirect(url_for("dashboard"))

    granted = {x.strip() for x in (account["scope"] or "").split(",") if x.strip()}
    if "user.info.stats" not in granted:
        flash("Statistik belum muncul karena akun ini hanya memberi scope " + (account["scope"] or "-") + ". Tambahkan user.info.stats pada TikTok Developer, ubah TIKTOK_SCOPES di Render, lalu Connect ulang akun.", "error")
        return redirect(url_for("dashboard"))

    try:
        access_token, token_error = _get_valid_access_token(account)
        if not access_token:
            flash("TikTok menolak statistik: " + (token_error or "token tidak valid") +
                  ". Silakan Connect ulang akun TikTok.", "error")
            return redirect(url_for("dashboard"))

        # First attempt with current/refreshed token.
        r, payload = _fetch_user_profile(access_token)
        err = payload.get("error", {})

        # If TikTok says the token is invalid, refresh once and retry.
        if err.get("code") == "access_token_invalid" or r.status_code == 401:
            new_token, refresh_error = refresh_tiktok_access_token(account)
            if not new_token:
                flash("TikTok menolak statistik karena access token sudah tidak valid. " +
                      (refresh_error or "Silakan Connect ulang akun TikTok."), "error")
                return redirect(url_for("dashboard"))
            access_token = new_token
            r, payload = _fetch_user_profile(access_token)
            err = payload.get("error", {})

        user = payload.get("data", {}).get("user", {})
        if not r.ok or err.get("code") not in ("ok", "", None):
            flash("TikTok menolak statistik: " +
                  str(err.get("message", err.get("code", "unknown"))), "error")
            return redirect(url_for("dashboard"))

        # Save the fresh avatar/profile immediately.
        _save_profile(open_id, user)

        video_list_ok = True
        if "video.list" in granted:
            total_views = 0
            total_comments = 0
            cursor = None
            # Sum view_count + comment_count of the account's videos.
            for _ in range(10000):
                body = {"max_count": 20}
                if cursor:
                    body["cursor"] = cursor
                vr = requests.post(
                    VIDEO_LIST_URL,
                    params={"fields": "id,view_count,comment_count"},
                    headers=api_headers(access_token),
                    json=body,
                    timeout=30
                )
                vp = vr.json()
                verr = vp.get("error", {})
                if not vr.ok or verr.get("code") not in ("ok", "", None):
                    video_list_ok = False
                    total_views = 0
                    total_comments = 0
                    break
                vd = vp.get("data", {})
                videos = vd.get("videos", [])
                total_views += sum(int(v.get("view_count") or 0) for v in videos)
                total_comments += sum(int(v.get("comment_count") or 0) for v in videos)
                if not vd.get("has_more"):
                    break
                cursor = vd.get("cursor")

        c = db()
        c.execute("""UPDATE accounts SET
            display_name=?, avatar_url=?, username=?, video_count=?, following_count=?,
            follower_count=?, likes_count=?, view_count=?, comment_count=?, stats_updated_at=? WHERE open_id=?""", (
            user.get("display_name") or account["display_name"],
            (user.get("avatar_large_url") or user.get("avatar_url_100") or user.get("avatar_url") or account["avatar_url"]),
            user.get("username") or account.get("username"),
            user.get("video_count"), user.get("following_count"), user.get("follower_count"),
            user.get("likes_count"), total_views, total_comments, datetime.utcnow().isoformat(), open_id
        ))
        c.commit(); c.close()
        if "video.list" not in granted:
            flash("Statistik profil diperbarui. Total Views membutuhkan scope video.list dan Connect ulang akun.", "success")
        else:
            if "video.list" in granted and not video_list_ok:
                flash("Statistik profil diperbarui, tetapi daftar video TikTok tidak berhasil dibaca. Views/Comments ditampilkan 0 sampai video.list berhasil diakses.", "error")
            else:
                flash("Statistik akun, total views seluruh video publik, dan total comments berhasil diperbarui.", "success")
    except Exception as e:
        flash("Gagal memperbarui statistik: " + str(e), "error")
    return redirect(url_for("dashboard"))

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
