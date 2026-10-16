# DashboardFYEPE YouTube link checker
import csv, io, os, re, uuid, json
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
import requests
from flask import request, render_template, redirect, url_for, flash, abort, Response

# Metadata publik; tidak menggunakan API key.
OEMBED = "https://www.youtube.com/oembed"
CHECK_BATCH = 5

def check_public_video(vid):
    """Periksa playabilityStatus pada halaman watch, bukan oEmbed.

    Tidak menyatakan TAYANG jika halaman tidak memberikan status yang jelas.
    """
    try:
        response = requests.get(
            "https://www.youtube.com/watch",
            params={"v": vid, "hl": "id", "bpctr": "9999999999"},
            headers={"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0.0.0 Safari/537.36",
                     "Accept-Language": "id-ID,id;q=0.9,en;q=0.7"},
            timeout=(4, 12))
    except requests.RequestException:
        return "PERLU VERIFIKASI", "Koneksi ke YouTube gagal; coba lagi nanti", False
    if response.status_code in (403, 429, 500, 502, 503):
        return "PERLU VERIFIKASI", f"YouTube HTTP {response.status_code}; pemeriksaan dijeda", True
    if response.status_code != 200:
        return "PERLU VERIFIKASI", f"Halaman YouTube HTTP {response.status_code}; belum dapat dipastikan", False
    html = response.text
    match = re.search(r'(?:var\s+ytInitialPlayerResponse\s*=\s*|ytInitialPlayerResponse\s*=\s*|"ytInitialPlayerResponse"\s*:\s*)(\{)', html)
    if not match:
        return "PERLU VERIFIKASI", "YouTube tidak menyediakan data pemutaran yang dapat diverifikasi", False
    try:
        player, _ = json.JSONDecoder().raw_decode(html[match.start(1):])
    except (ValueError, TypeError):
        return "PERLU VERIFIKASI", "Data pemutaran YouTube tidak dapat dibaca", False
    playback = player.get("playabilityStatus") or {}
    state = str(playback.get("status", "")).upper()
    reason = str(playback.get("reason") or "").strip()[:180]
    if state == "OK":
        details = player.get("videoDetails") or {}
        if details.get("videoId") == vid and details.get("isPlayable", True):
            return "TAYANG", "Pemutar YouTube melaporkan status OK (ketersediaan dapat berbeda menurut wilayah/akun)", False
        return "PERLU VERIFIKASI", "Status OK tetapi identitas atau kemampuan pemutaran tidak dapat dipastikan", False
    if state in ("ERROR", "UNPLAYABLE", "LOGIN_REQUIRED", "AGE_CHECK_REQUIRED", "CONTENT_CHECK_REQUIRED", "LIVE_STREAM_OFFLINE"):
        return "TIDAK TERSEDIA", "Pemutar YouTube menolak pemutaran: " + (reason or state), False
    return "PERLU VERIFIKASI", "Status pemutar tidak dikenal: " + (state or "kosong"), False

def video_id(raw):
    raw = raw.strip()
    if not raw: return None
    try:
        u = urlparse(raw)
        host = (u.hostname or '').lower()
        if host in ('youtu.be','www.youtu.be'):
            vid = u.path.strip('/').split('/')[0]
        elif host in ('youtube.com','www.youtube.com','m.youtube.com','music.youtube.com'):
            parts = u.path.strip('/').split('/')
            vid = parse_qs(u.query).get('v',[''])[0] if u.path == '/watch' else (parts[1] if len(parts)>1 and parts[0] in ('shorts','live','embed') else '')
        else: return None
        return vid if re.fullmatch(r'[A-Za-z0-9_-]{11}',vid) else None
    except Exception: return None

def parse_txt(content):
    text = content.decode('utf-8-sig', errors='replace')
    output=[]; seen=set()
    for line in text.splitlines():
        if not line.strip(): continue
        cells=line.split('\t')
        match = next(((c.strip(),video_id(c.strip())) for c in reversed(cells) if video_id(c.strip())), None)
        if not match:
            # Allow a line containing just the link, or comma-separated data.
            links = re.findall(r'https?://[^\s,;]+',line)
            match = next(((x,video_id(x)) for x in links if video_id(x)), None)
        if not match: continue
        link, vid=match
        if vid in seen: continue
        seen.add(vid)
        # Preserve source metadata when tabular input is supplied.
        number = cells[0].strip() if len(cells)>1 else str(len(output)+1)
        title = cells[1].strip() if len(cells)>2 else ''
        artist = cells[2].strip() if len(cells)>3 else ''
        source_status = cells[3].strip() if len(cells)>4 else ''
        output.append((number,title,artist,source_status,link,vid))
    return output


def register_youtube_check(app, db, role_required, current_user):
    from flask import jsonify
    def ensure_tables(c):
        c.execute("""CREATE TABLE IF NOT EXISTS youtube_check_rows (
            batch_id TEXT NOT NULL, owner_id TEXT NOT NULL, row_no INTEGER NOT NULL,
            source_no TEXT, song_title TEXT, artist TEXT, source_status TEXT,
            video_url TEXT, video_id TEXT, youtube_status TEXT, checked_at TEXT,
            note TEXT, PRIMARY KEY(batch_id,row_no))""")
        c.commit()

    def scoped(batch, user):
        if user["role"]=="superadmin":
            return "batch_id=?", (batch,)
        return "batch_id=? AND owner_id=?", (batch,str(user["id"]))

    @app.route("/check-video-youtube",methods=["GET","POST"])
    @role_required("superadmin","admin_pdn")
    def youtube_check_page():
        user=current_user()
        if request.method=="POST":
            upload=request.files.get("video_txt")
            if not upload or not upload.filename.lower().endswith(".txt"):
                flash("Pilih file TXT.","error")
                return redirect(url_for("youtube_check_page"))
            # No fixed number-of-links limit; 32 MB per upload protects server memory.
            content=upload.stream.read(32*1024*1024+1)
            if len(content)>32*1024*1024:
                flash("Ukuran file maksimal 32 MB per upload.","error")
                return redirect(url_for("youtube_check_page"))
            records=parse_txt(content)
            if not records:
                flash("Tidak ada URL YouTube valid di TXT.","error")
                return redirect(url_for("youtube_check_page"))
            batch=uuid.uuid4().hex
            c=db(); ensure_tables(c)
            for i,(number,title,artist,source_status,link,vid) in enumerate(records,1):
                c.execute("""INSERT INTO youtube_check_rows
                    (batch_id,owner_id,row_no,source_no,song_title,artist,source_status,video_url,video_id,youtube_status,checked_at,note)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (batch,str(user["id"]),i,number,title,artist,source_status,link,vid,"BELUM DIPERIKSA","",""))
            c.commit(); c.close()
            return redirect(url_for("youtube_check_page",batch=batch))
        batch=request.args.get("batch","")
        try: page=max(1,int(request.args.get("page","1")))
        except ValueError: page=1
        rows=[]; counts={}; total=0
        tab=request.args.get("tab","semua")
        if tab not in ("semua","tidak-tersedia"):
            tab="semua"
        if batch:
            c=db(); ensure_tables(c); where,params=scoped(batch,user)
            counts={r["youtube_status"]:r["n"] for r in c.execute(
                "SELECT youtube_status,COUNT(*) n FROM youtube_check_rows WHERE "+where+" GROUP BY youtube_status",params).fetchall()}
            total=sum(counts.values())
            filter_sql=" AND youtube_status='TIDAK TERSEDIA'" if tab=="tidak-tersedia" else ""
            rows=[dict(r) for r in c.execute(
                "SELECT * FROM youtube_check_rows WHERE "+where+filter_sql+" ORDER BY row_no LIMIT 100 OFFSET ?",
                params+((page-1)*100,)).fetchall()]
            c.close()
            if not total: abort(404)
        return render_template("check_video_youtube.html",user=user,rows=rows,batch=batch,
            counts=counts,total=total,page=page,tab=tab,
            tab_total=(counts.get("TIDAK TERSEDIA",0) if tab=="tidak-tersedia" else total),api_ready=True)

    @app.post("/check-video-youtube/reset/<batch>")
    @role_required("superadmin","admin_pdn")
    def youtube_check_reset(batch):
        user=current_user()
        c=db(); ensure_tables(c); where,params=scoped(batch,user)
        found=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where,params).fetchone()["n"]
        if not found: c.close(); abort(404)
        c.execute("UPDATE youtube_check_rows SET youtube_status='BELUM DIPERIKSA',checked_at='',note='Menunggu pemeriksaan pemutar YouTube' WHERE "+where,params)
        c.commit(); c.close()
        flash("Status lama direset. Klik Mulai / Lanjutkan untuk memeriksa ulang melalui pemutar YouTube.","info")
        return redirect(url_for("youtube_check_page",batch=batch))

    @app.post("/check-video-youtube/step/<batch>")
    @role_required("superadmin","admin_pdn")
    def youtube_check_step(batch):
        user=current_user()
        c=db(); ensure_tables(c); where,params=scoped(batch,user)
        total=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where,params).fetchone()["n"]
        if not total: c.close(); abort(404)
        chunk=[dict(r) for r in c.execute("SELECT * FROM youtube_check_rows WHERE "+where+
            " AND youtube_status='BELUM DIPERIKSA' ORDER BY row_no LIMIT ?",params+(CHECK_BATCH,)).fetchall()]
        if not chunk:
            c.close()
            return jsonify(ok=True,done=True,processed=total,total=total)
        now=datetime.now(timezone.utc).isoformat()
        throttled=False
        for x in chunk:
            status,note,stop=check_public_video(x["video_id"])
            if stop:
                throttled=True
                break
            c.execute("UPDATE youtube_check_rows SET youtube_status=?,checked_at=?,note=? WHERE batch_id=? AND row_no=?",
                (status,now,note,batch,x["row_no"]))
        c.commit()
        remaining=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where+
            " AND youtube_status='BELUM DIPERIKSA'",params).fetchone()["n"]
        c.close()
        return jsonify(ok=True,done=(remaining==0),processed=total-remaining,total=total,throttled=throttled,warning=("YouTube membatasi akses. Jeda dan coba lagi nanti." if throttled else ""))

    @app.get("/check-video-youtube/export/<batch>")
    @role_required("superadmin","admin_pdn")
    def youtube_check_export(batch):
        user=current_user()
        c=db(); ensure_tables(c); where,params=scoped(batch,user)
        cursor=c.execute("SELECT * FROM youtube_check_rows WHERE "+where+" ORDER BY row_no",params)
        stream=io.StringIO()
        writer=csv.writer(stream)
        writer.writerow(["NO","Judul lagu","artis","status awal","link","video ID","status YouTube","waktu check (UTC)","keterangan"])
        n=0
        for x in cursor:
            n+=1
            writer.writerow([x["source_no"],x["song_title"],x["artist"],x["source_status"],
                x["video_url"],x["video_id"],x["youtube_status"],x["checked_at"],x["note"]])
        c.close()
        if not n: abort(404)
        return Response("\\ufeff"+stream.getvalue(),mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition":f"attachment; filename=hasil-check-youtube-{batch[:8]}.csv"})

    @app.get("/check-video-youtube/export-unavailable/<batch>")
    @role_required("superadmin","admin_pdn")
    def youtube_check_export_unavailable(batch):
        from openpyxl import Workbook
        from openpyxl.styles import Font, PatternFill, Alignment
        from openpyxl.utils import get_column_letter
        user=current_user()
        c=db(); ensure_tables(c); where,params=scoped(batch,user)
        exists=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where,params).fetchone()["n"]
        if not exists:
            c.close(); abort(404)
        cursor=c.execute("SELECT source_no,song_title,artist,source_status,video_url,video_id,checked_at,note FROM youtube_check_rows WHERE "+where+
            " AND youtube_status='TIDAK TERSEDIA' ORDER BY row_no",params)
        wb=Workbook(write_only=False)
        ws=wb.active; ws.title="Video Tidak Tersedia"
        headers=["NO","Judul lagu","Artis","Status awal","Link YouTube","Video ID","Status YouTube","Waktu check (UTC)","Keterangan"]
        ws.append(headers)
        for item in cursor:
            ws.append([item["source_no"],item["song_title"],item["artist"],item["source_status"],
                item["video_url"],item["video_id"],"VIDEO TIDAK TERSEDIA",item["checked_at"],item["note"]])
        c.close()
        for cell in ws[1]:
            cell.fill=PatternFill("solid",fgColor="183153")
            cell.font=Font(bold=True,color="FFFFFF")
            cell.alignment=Alignment(wrap_text=True)
        for i,w in enumerate((9,35,25,18,62,19,25,29,65),1):
            ws.column_dimensions[get_column_letter(i)].width=w
        ws.freeze_panes="A2"
        ws.auto_filter.ref=ws.dimensions
        buffer=io.BytesIO();wb.save(buffer);buffer.seek(0)
        from flask import send_file
        return send_file(buffer,as_attachment=True,download_name=f"video-tidak-tersedia-{batch[:8]}.xlsx",
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
