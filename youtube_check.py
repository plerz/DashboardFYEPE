# DashboardFYEPE YouTube link checker
import csv, io, os, re, uuid
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs
import requests
from flask import request, render_template, redirect, url_for, flash, abort, Response

API = "https://www.googleapis.com/youtube/v3/videos"

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
        if batch:
            c=db(); ensure_tables(c); where,params=scoped(batch,user)
            counts={r["youtube_status"]:r["n"] for r in c.execute(
                "SELECT youtube_status,COUNT(*) n FROM youtube_check_rows WHERE "+where+" GROUP BY youtube_status",params).fetchall()}
            total=sum(counts.values())
            rows=[dict(r) for r in c.execute(
                "SELECT * FROM youtube_check_rows WHERE "+where+" ORDER BY row_no LIMIT 100 OFFSET ?",
                params+((page-1)*100,)).fetchall()]
            c.close()
            if not total: abort(404)
        return render_template("check_video_youtube.html",user=user,rows=rows,batch=batch,
            counts=counts,total=total,page=page,api_ready=bool(os.environ.get("YOUTUBE_API_KEY")))

    @app.post("/check-video-youtube/step/<batch>")
    @role_required("superadmin","admin_pdn")
    def youtube_check_step(batch):
        user=current_user()
        key=os.environ.get("YOUTUBE_API_KEY","").strip()
        if not key: return jsonify(ok=False,error="YOUTUBE_API_KEY belum diatur"),400
        c=db(); ensure_tables(c); where,params=scoped(batch,user)
        total=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where,params).fetchone()["n"]
        if not total: c.close(); abort(404)
        chunk=[dict(r) for r in c.execute("SELECT * FROM youtube_check_rows WHERE "+where+
            " AND youtube_status='BELUM DIPERIKSA' ORDER BY row_no LIMIT 50",params).fetchall()]
        if not chunk:
            c.close()
            return jsonify(ok=True,done=True,processed=total,total=total)
        try:
            response=requests.get(API,params={"part":"snippet,status","id":",".join(x["video_id"] for x in chunk),
                "key":key,"maxResults":50},timeout=25)
            response.raise_for_status()
            found={x["id"]:x for x in response.json().get("items",[])}
        except requests.RequestException:
            c.close()
            return jsonify(ok=False,error="API YouTube gagal; periksa API key, kuota, atau koneksi."),502
        now=datetime.now(timezone.utc).isoformat()
        for x in chunk:
            item=found.get(x["video_id"])
            if not item: status,note="TIDAK TERSEDIA","Tidak ditemukan melalui API publik; penyebab belum pasti"
            elif item.get("status",{}).get("privacyStatus")=="public":
                status,note="TAYANG","Tersedia secara publik"
            else: status,note="TIDAK PUBLIK","Video tidak berstatus publik"
            c.execute("UPDATE youtube_check_rows SET youtube_status=?,checked_at=?,note=? WHERE batch_id=? AND row_no=?",
                (status,now,note,batch,x["row_no"]))
        c.commit()
        remaining=c.execute("SELECT COUNT(*) n FROM youtube_check_rows WHERE "+where+
            " AND youtube_status='BELUM DIPERIKSA'",params).fetchone()["n"]
        c.close()
        return jsonify(ok=True,done=(remaining==0),processed=total-remaining,total=total)

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
