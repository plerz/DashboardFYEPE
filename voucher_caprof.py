"""Harmoni Travel reimbursement vouchers. Attachments are stored in PostgreSQL, not Render /tmp."""
import io, os, re
from PIL import Image, ImageOps, UnidentifiedImageError
from datetime import timedelta
from decimal import Decimal, InvalidOperation
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from flask import request, redirect, url_for, render_template, flash, abort, Response
from werkzeug.utils import secure_filename

TZ = ZoneInfo('Asia/Jakarta')
ALLOWED = {'image/jpeg', 'image/png', 'image/webp', 'application/pdf'}
MAX_FILE = 8 * 1024 * 1024
MAX_ATTACHMENTS = 24


CONNECTORS_LOWER = {'dan', 'atau', 'untuk', 'di', 'ke', 'dari', 'pada', 'dengan', 'yang', 'serta', 'oleh', 'dalam', 'atas', 'agar', 'karena', 'sebagai', 'tanpa', 'hingga', 'sampai', 'tentang', 'bagi'}

def proper_indonesia(value):
    """Title-case ordinary words, preserving connector words except at the beginning."""
    words = re.split(r'(\s+)', str(value or '').strip())
    first = True
    result = []
    for word in words:
        if not word or word.isspace():
            result.append(word)
            continue
        def change(match):
            nonlocal first
            raw = match.group(0)
            lower = raw.lower()
            if not any(ch.isalpha() for ch in raw):
                return raw
            if lower in CONNECTORS_LOWER and not first:
                out = lower
            elif raw.isupper() and len(raw) <= 4 and len(raw) > 1:
                out = raw  # existing acronym, e.g. PT, CV
            else:
                out = lower[:1].upper() + lower[1:]
            first = False
            return out
        result.append(re.sub(r'[^\W_]+(?:[-’\'][^\W_]+)*', change, word, flags=re.UNICODE))
    return ''.join(result)

def compress_attachment(data, mime, filename):
    """Best-effort safe compression. Return original on failed/ineffective compression."""
    if mime.startswith('image/'):
        try:
            with Image.open(io.BytesIO(data)) as original:
                original.load()
                if original.width * original.height > 45_000_000:
                    raise ValueError('Image too large')
                im = ImageOps.exif_transpose(original)
                im.thumbnail((2200, 2200), Image.Resampling.LANCZOS)
                out = io.BytesIO()
                if mime == 'image/jpeg':
                    im.convert('RGB').save(out, format='JPEG', quality=83, optimize=True)
                elif mime == 'image/webp':
                    im.save(out, format='WEBP', quality=82, method=5)
                else:
                    im.save(out, format='PNG', optimize=True, compress_level=9)
                packed = out.getvalue()
                if len(packed) < len(data):
                    return packed, mime, filename
        except (UnidentifiedImageError, OSError, ValueError):
            pass
    elif mime == 'application/pdf':
        try:
            import fitz
            src = fitz.open(stream=data, filetype='pdf')
            if src.needs_pass:
                src.close(); return data, mime, filename
            packed = src.tobytes(garbage=4, deflate=True, clean=False)
            src.close()
            if len(packed) < len(data):
                return packed, mime, filename
        except Exception:
            pass
    return data, mime, filename

def register_voucher_caprof(app, db, current_user):
    def allowed():
        u = current_user()
        if not u or u['role'] not in ('superadmin', 'keuangan', 'caprof'):
            abort(403)
        return u

    def schema():
        c = db()
        try:
            pk = 'SERIAL PRIMARY KEY' if c.is_pg else 'INTEGER PRIMARY KEY AUTOINCREMENT'
            blob = 'BYTEA' if c.is_pg else 'BLOB'
            c.execute(f'''CREATE TABLE IF NOT EXISTS caprof_vouchers (
                id {pk}, owner_id INTEGER NOT NULL, pengaju TEXT NOT NULL,
                nominal BIGINT NOT NULL, rincian TEXT NOT NULL, keperluan TEXT,
                tanggal_pengajuan TEXT NOT NULL, tanggal_transaksi TEXT,
                bank TEXT, rekening TEXT, pemilik_rekening TEXT,
                status_pengajuan TEXT NOT NULL DEFAULT 'Tercatat',
                status_transfer TEXT NOT NULL DEFAULT 'Belum ditransfer',
                judul_voucher TEXT, created_at TEXT NOT NULL)''')
            c.execute(f'''CREATE TABLE IF NOT EXISTS caprof_attachments (
                id {pk}, voucher_id INTEGER NOT NULL, kind TEXT NOT NULL,
                filename TEXT NOT NULL, mime TEXT NOT NULL, content {blob} NOT NULL,
                uploaded_by INTEGER NOT NULL, uploaded_at TEXT NOT NULL)''')
            # Safe migration: add title without altering old vouchers or attachments.
            if c.is_pg:
                cols = {r['column_name'] for r in c.execute("SELECT column_name FROM information_schema.columns WHERE table_schema='public' AND table_name='caprof_vouchers'").fetchall()}
            else:
                cols = {r['name'] for r in c.execute('PRAGMA table_info(caprof_vouchers)').fetchall()}
            if 'judul_voucher' not in cols:
                c.execute('ALTER TABLE caprof_vouchers ADD COLUMN judul_voucher TEXT')
            c.execute('CREATE INDEX IF NOT EXISTS idx_caprof_attach_voucher ON caprof_attachments(voucher_id)')
            c.execute(f'''CREATE TABLE IF NOT EXISTS caprof_activity (
                id {pk}, user_id INTEGER, username TEXT, role TEXT, voucher_id INTEGER,
                voucher_number TEXT, action TEXT NOT NULL, detail TEXT, created_at TEXT NOT NULL)''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_caprof_activity_created ON caprof_activity(created_at)')
            # Cleanup is throttled by app process, and never deletes vouchers or attachments.
            now = datetime.now(TZ)
            if (now - getattr(app, '_caprof_last_cleanup', datetime.min.replace(tzinfo=TZ))).total_seconds() >= 3600:
                c.execute('DELETE FROM caprof_activity WHERE created_at < ?', ((now - timedelta(days=30)).isoformat(),))
                app._caprof_last_cleanup = now
            c.commit()
        finally:
            c.close()

    def audit(c, u, action, voucher_id=None, detail=''):
        # Called in the same DB transaction as the voucher mutation.
        username = str(u['username'] or '')
        number = f'CAPROF-{int(voucher_id):05d}' if voucher_id is not None else None
        c.execute('''INSERT INTO caprof_activity(user_id,username,role,voucher_id,voucher_number,action,detail,created_at)
                     VALUES(?,?,?,?,?,?,?,?)''',
                  (u['id'], username, u['role'], voucher_id, number, action, str(detail)[:1000], datetime.now(TZ).isoformat()))

    def parse_nominal(raw):
        value = str(raw or '').strip().replace('Rp', '').replace(' ', '')
        # Indonesian display format: 1.450.000,00
        if ',' in value:
            value = value.replace('.', '').replace(',', '.')
        elif '.' in value and all(len(part) == 3 for part in value.split('.')[1:]):
            value = value.replace('.', '')
        try:
            number = Decimal(value)
            return int(number) if number == number.to_integral_value() else 0
        except (InvalidOperation, ValueError):
            return 0

    def get_voucher(c, voucher_id):
        v = c.execute('SELECT * FROM caprof_vouchers WHERE id=?', (voucher_id,)).fetchone()
        if not v: abort(404)
        u = allowed()
        # Shared Harmoni workspace: all Harmoni users and superadmins may access vouchers.
        return v

    @app.get('/voucher-lht')
    def voucher_caprof():
        u = allowed(); schema(); c = db()
        try:
            # Shared vouchers: no owner filter for authorized Harmoni users.
            rows = c.execute('SELECT * FROM caprof_vouchers ORDER BY id DESC LIMIT 200').fetchall()
            rows = [dict(r) for r in rows]
            for v in rows:
                meta = c.execute('SELECT id,kind,filename FROM caprof_attachments WHERE voucher_id=? ORDER BY id DESC', (v['id'],)).fetchall()
                v['transfer_files'] = [dict(a) for a in meta if a['kind']=='transfer']
                v['pembayaran_files'] = [dict(a) for a in meta if a['kind']=='pembayaran']
                v['status_transfer'] = 'Sudah ditransfer' if v['transfer_files'] else 'Belum ditransfer'
        finally: c.close()
        return render_template('voucher_caprof.html', user=u, voucher_kind='caprof', vouchers=rows, today=datetime.now(TZ).date().isoformat(), proper_indonesia=proper_indonesia)

    @app.get('/voucher-lht/riwayat-aktivitas')
    def voucher_caprof_activity_history():
        u = allowed()
        if u['role'] not in ('superadmin','keuangan'):
            abort(403)
        schema()
        c = db()
        try:
            activities = []
            activities = [dict(r) for r in c.execute(
                'SELECT * FROM caprof_activity WHERE created_at >= ? ORDER BY id DESC LIMIT 300',
                ((datetime.now(TZ)-timedelta(days=30)).isoformat(),)).fetchall()]
            # Login/logout records already exist in app.py's audit_sessions.
            try:
                login_rows = c.execute('''SELECT username,role,login_at,logout_at FROM audit_sessions
                    WHERE role='harmoni' AND login_at >= ? ORDER BY login_at DESC LIMIT 200''',
                    ((datetime.now(timezone.utc)-timedelta(days=30)).isoformat(),)).fetchall()
                for r in login_rows:
                    rr = dict(r)
                    activities.append(dict(username=rr['username'], role=rr['role'], voucher_number='',
                        action='LOGIN', detail='Login berhasil', created_at=rr['login_at']))
                    if rr.get('logout_at'):
                        activities.append(dict(username=rr['username'], role=rr['role'], voucher_number='',
                            action='LOGOUT', detail='Logout / sesi berakhir', created_at=rr['logout_at']))
            except Exception:
                app.logger.exception('Could not read Harmoni login history')
            activities.sort(key=lambda x: datetime.fromisoformat(str(x.get('created_at') or '1970-01-01T00:00:00+00:00')).timestamp(), reverse=True)
            activities = activities[:300]
        finally:
            c.close()
        return render_template('voucher_caprof_activity.html', user=u, voucher_kind='caprof', activities=activities)

    @app.post('/voucher-lht/create')
    def voucher_caprof_create():
        u = allowed(); schema()
        nominal = parse_nominal(request.form.get('nominal', '0'))
        rincian = proper_indonesia(request.form.get('rincian','').strip()[:240])
        judul_voucher = proper_indonesia(request.form.get('judul_voucher','').strip()[:240])
        if not 0 < nominal <= 10**12 or not rincian or not judul_voucher:
            flash('Nominal, judul voucher, dan rincian wajib diisi dengan benar.', 'error')
            return redirect(url_for('voucher_caprof'))
        today = datetime.now(TZ).date().isoformat()
        def val(name, default='', size=150): return request.form.get(name, default).strip()[:size]
        def date(name):
            raw = val(name, today, 10)
            try: datetime.strptime(raw, '%Y-%m-%d'); return raw
            except ValueError: return today
        c = db()
        try:
            c.execute('''INSERT INTO caprof_vouchers(owner_id,pengaju,nominal,rincian,keperluan,tanggal_pengajuan,tanggal_transaksi,bank,rekening,pemilik_rekening,judul_voucher,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?)''', (u['id'], proper_indonesia(val('pengaju',u['full_name'] or u['username'])),nominal,rincian,val('keperluan'),date('tanggal_pengajuan'),date('tanggal_transaksi'),val('bank','BRI').upper(),val('rekening'),val('pemilik_rekening').upper(),judul_voucher,datetime.now(TZ).isoformat()))
            new_id = c.execute('SELECT id FROM caprof_vouchers WHERE owner_id=? ORDER BY id DESC LIMIT 1', (u['id'],)).fetchone()['id']
            audit(c,u,'CREATE',new_id,'Membuat voucher baru')
            c.commit()
        finally: c.close()
        flash('Voucher berhasil dibuat. Silakan unggah bukti.', 'success')
        return redirect(url_for('voucher_caprof'))

    @app.post('/voucher-lht/<int:voucher_id>/upload')
    def voucher_caprof_upload(voucher_id):
        u = allowed(); schema(); c = db()
        try:
            get_voucher(c, voucher_id)
            kind = request.form.get('kind','')
            if kind not in ('pembayaran','transfer'): abort(400)
            files = request.files.getlist('files')
            count = c.execute('SELECT COUNT(*) AS n FROM caprof_attachments WHERE voucher_id=?',(voucher_id,)).fetchone()['n']
            if not files or count+len(files)>MAX_ATTACHMENTS:
                flash('Maksimal 24 lampiran per voucher.', 'error'); return redirect(url_for('voucher_caprof'))
            staged=[]
            for f in files:
                name=secure_filename(f.filename or '')[:180]
                mime=f.mimetype
                data=f.read(MAX_FILE+1)
                if not name or mime not in ALLOWED or not data or len(data)>MAX_FILE:
                    flash('Lampiran harus JPG, PNG, WEBP atau PDF, maksimal 8 MB per file.', 'error')
                    return redirect(url_for('voucher_caprof'))
                # Verify file signatures; never trust the browser MIME alone.
                valid = (mime=='application/pdf' and data.startswith(b'%PDF-')) or (mime=='image/png' and data.startswith(b'\x89PNG\r\n\x1a\n')) or (mime=='image/jpeg' and data.startswith(b'\xff\xd8\xff')) or (mime=='image/webp' and data[:4]==b'RIFF' and data[8:12]==b'WEBP')
                if not valid:
                    flash('Format isi file tidak sesuai ekstensi.', 'error'); return redirect(url_for('voucher_caprof'))
                data,mime,name = compress_attachment(data,mime,name)
                staged.append((voucher_id,kind,name,mime,data,u['id'],datetime.now(TZ).isoformat()))
            for item in staged:
                c.execute('INSERT INTO caprof_attachments(voucher_id,kind,filename,mime,content,uploaded_by,uploaded_at) VALUES(?,?,?,?,?,?,?)',item)
            if kind=='transfer':
                c.execute("UPDATE caprof_vouchers SET status_transfer='Sudah ditransfer' WHERE id=?",(voucher_id,))
            audit(c,u,'UPLOAD',voucher_id,f'{len(staged)} file '+('Bukti Transfer' if kind=='transfer' else 'Struk / Kuitansi / Bukti Bayar')+': '+', '.join(x[2] for x in staged))
            c.commit(); flash(f'{len(staged)} lampiran berhasil diunggah.', 'success')
        finally: c.close()
        return redirect(url_for('voucher_caprof'))

    @app.post('/voucher-lht/<int:voucher_id>/edit')
    def voucher_caprof_edit(voucher_id):
        u=allowed(); schema(); c=db()
        try:
            original = get_voucher(c,voucher_id)
            field=request.form.get('field','')
            if field not in ('pengaju','rincian','keperluan','judul_voucher','nominal','tanggal_pengajuan','tanggal_transaksi','bank','rekening','pemilik_rekening'): abort(400)
            value=request.form.get('value','').strip()[:240]
            if field in ('pengaju', 'judul_voucher', 'rincian'):
                value = proper_indonesia(value)
            elif field in ('bank', 'pemilik_rekening'):
                value = value.upper()
            if field == 'nominal':
                value = parse_nominal(value)
                if not 0 < value <= 10**12: abort(400)
            if not value and field in ('pengaju','rincian','judul_voucher'):
                flash('Kolom wajib tidak boleh kosong.','error')
            else:
                before = original[field]
                c.execute(f'UPDATE caprof_vouchers SET {field}=? WHERE id=?',(value,voucher_id))
                audit(c,u,'EDIT',voucher_id,f'{field}: {before} → {value}')
                c.commit(); flash('Data voucher diperbarui.','success')
        finally: c.close()
        return redirect(url_for('voucher_caprof'))

    @app.post('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/delete')
    def voucher_caprof_clear_attachment(voucher_id, attachment_id):
        u=allowed(); schema(); c = db()
        try:
            get_voucher(c, voucher_id)
            attachment = c.execute(
                'SELECT id, kind FROM caprof_attachments WHERE id=? AND voucher_id=?',
                (attachment_id, voucher_id)).fetchone()
            if not attachment: abort(404)
            c.execute('DELETE FROM caprof_attachments WHERE id=? AND voucher_id=?',
                      (attachment_id, voucher_id))
            if attachment['kind'] == 'transfer':
                remaining = c.execute(
                    "SELECT COUNT(*) AS n FROM caprof_attachments WHERE voucher_id=? AND kind='transfer'",
                    (voucher_id,)).fetchone()['n']
                c.execute('UPDATE caprof_vouchers SET status_transfer=? WHERE id=?',
                          ('Sudah ditransfer' if remaining else 'Belum ditransfer', voucher_id))
            audit(c,u,'CLEAR',voucher_id,f'Menghapus satu lampiran ID {attachment_id} ({attachment["kind"]})')
            c.commit()
        finally:
            c.close()
        flash('Satu lampiran berhasil dihapus.', 'success')
        return redirect(url_for('voucher_caprof'))

    @app.post('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/replace')
    def voucher_caprof_replace_attachment(voucher_id, attachment_id):
        u = allowed(); schema()
        f = request.files.get('file')
        if not f:
            flash('Pilih satu file pengganti.', 'error')
            return redirect(url_for('voucher_caprof'))
        name = secure_filename(f.filename or '')[:180]
        mime = f.mimetype
        data = f.read(MAX_FILE + 1)
        valid = ((mime == 'application/pdf' and data.startswith(b'%PDF-')) or
                 (mime == 'image/png' and data.startswith(b'\x89PNG\r\n\x1a\n')) or
                 (mime == 'image/jpeg' and data.startswith(b'\xff\xd8\xff')) or
                 (mime == 'image/webp' and data[:4] == b'RIFF' and data[8:12] == b'WEBP'))
        if not name or not valid or not data or len(data) > MAX_FILE:
            flash('File harus JPG, PNG, WEBP, atau PDF maksimal 8 MB.', 'error')
            return redirect(url_for('voucher_caprof'))
        data,mime,name = compress_attachment(data,mime,name)
        c = db()
        try:
            get_voucher(c, voucher_id)
            exists = c.execute('SELECT id FROM caprof_attachments WHERE id=? AND voucher_id=?',
                               (attachment_id, voucher_id)).fetchone()
            if not exists: abort(404)
            c.execute("""UPDATE caprof_attachments
                         SET filename=?, mime=?, content=?, uploaded_by=?, uploaded_at=?
                         WHERE id=? AND voucher_id=?""",
                      (name, mime, data, u['id'], datetime.now(TZ).isoformat(),
                       attachment_id, voucher_id))
            audit(c,u,'REPLACE',voucher_id,f'Mengganti lampiran ID {attachment_id} dengan {name}')
            c.commit()
        finally:
            c.close()
        flash('Satu lampiran berhasil diganti.', 'success')
        return redirect(url_for('voucher_caprof'))

    @app.post('/voucher-lht/<int:voucher_id>/delete')
    def voucher_caprof_delete(voucher_id):
        u = allowed()
        if u['role'] not in ('superadmin','keuangan'): abort(403)
        schema(); c = db()
        try:
            get_voucher(c, voucher_id)  # Checks owner or superadmin.
            c.execute('DELETE FROM caprof_attachments WHERE voucher_id=?', (voucher_id,))
            c.execute('DELETE FROM caprof_vouchers WHERE id=?', (voucher_id,))
            audit(c,u,'DELETE',voucher_id,'Menghapus voucher beserta seluruh lampirannya')
            c.commit()
        finally:
            c.close()
        flash('Voucher dan seluruh lampirannya berhasil dihapus.', 'success')
        return redirect(url_for('voucher_caprof'))

    @app.get('/voucher-lht/<int:voucher_id>/voucher.png')
    def voucher_caprof_png(voucher_id):
        allowed(); schema()
        try:
            import fitz
        except ImportError:
            abort(503)
        c = db()
        try:
            v = dict(get_voucher(c, voucher_id))
            # Status is derived from actual transfer attachments.
            has_transfer = c.execute(
                "SELECT COUNT(*) AS n FROM caprof_attachments WHERE voucher_id=? AND kind='transfer'",
                (voucher_id,)).fetchone()['n']
            v['status_transfer'] = 'Sudah ditransfer' if has_transfer else 'Belum ditransfer'
        finally:
            c.close()
        doc = fitz.open()
        try:
            w, h = fitz.paper_size('a6')
            page = doc.new_page(width=w, height=h)
            _draw_voucher(page, fitz.Rect(0, 0, w, h), v, fitz)
            png = page.get_pixmap(matrix=fitz.Matrix(3, 3), alpha=False).tobytes('png')
        finally:
            doc.close()
        return Response(png, mimetype='image/png', headers={
            'Content-Disposition': f'attachment; filename="CAPROF-{voucher_id:05d}-VOUCHER-A6.png"',
            'Cache-Control': 'private, no-store',
            'X-Content-Type-Options': 'nosniff'})

    @app.get('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/pdf')
    def voucher_caprof_attachment_pdf(voucher_id,attachment_id):
        allowed()
        try: import fitz
        except ImportError: abort(503)
        c=db()
        try:
            get_voucher(c,voucher_id)
            r=c.execute('SELECT * FROM caprof_attachments WHERE id=? AND voucher_id=?',(attachment_id,voucher_id)).fetchone()
            if not r: abort(404)
            a=dict(r)
        finally: c.close()
        doc=fitz.open(); w,h=fitz.paper_size('a6'); page=doc.new_page(width=w,height=h)
        _draw_attachment(page,fitz.Rect(0,0,w,h),a,fitz)
        data=doc.tobytes(garbage=4,deflate=True); doc.close()
        return Response(data,mimetype='application/pdf',headers={'Content-Disposition':f'attachment; filename="CAPROF-{voucher_id:05d}-BUKTI-{attachment_id}-A6.pdf"','Cache-Control':'private, no-store'})

    @app.get('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>')
    def voucher_caprof_attachment(voucher_id,attachment_id):
        allowed(); c=db()
        try:
            get_voucher(c,voucher_id)
            r=c.execute('SELECT * FROM caprof_attachments WHERE id=? AND voucher_id=?',(attachment_id,voucher_id)).fetchone()
            if not r: abort(404)
            return Response(bytes(r['content']),mimetype=r['mime'],headers={'Content-Disposition':'attachment; filename="'+secure_filename(r['filename'])+'"','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})
        finally: c.close()

    def _render_caprof_pdf_bytes(voucher_id, mode, fitz):
        """One shared document builder for PDF and PNG; never returns redirects."""
        c=db()
        try:
            v=dict(get_voucher(c,voucher_id))
            attachments=[dict(r) for r in c.execute("SELECT * FROM caprof_attachments WHERE voucher_id=? ORDER BY CASE kind WHEN 'transfer' THEN 0 ELSE 1 END,id",(voucher_id,)).fetchall()] if mode=='a4' else []
        finally: c.close()
        w,h=fitz.paper_size('a6'); doc=fitz.open()
        if mode=='a7':
            page=doc.new_page(width=w,height=h); _draw_voucher(page,fitz.Rect(0,0,w,h),v,fitz)
        else:
            # A4 portrait contains exactly four A6 portrait cells (2 x 2).
            # Evidence order: transfers first, then receipts; spill to next A4 page.
            aw,ah=fitz.paper_size('a4')
            cw,ch=aw/2,ah/2
            slots=[fitz.Rect(0,0,cw,ch),fitz.Rect(cw,0,aw,ch),
                   fitz.Rect(0,ch,cw,ah),fitz.Rect(cw,ch,aw,ah)]
            page=doc.new_page(width=aw,height=ah)
            _draw_voucher(page,slots[0],v,fitz)
            position=1
            for a in attachments:
                if a['mime']=='application/pdf':
                    try:
                        source=fitz.open(stream=bytes(a['content']),filetype='pdf')
                        pages=list(range(source.page_count))
                    except Exception:
                        source=None; pages=[None]
                else:
                    source=None; pages=[None]
                for index in pages:
                    if position==4:
                        page=doc.new_page(width=aw,height=ah)
                        position=0
                    slot=slots[position]
                    if source is not None and index is not None:
                        _draw_pdf_attachment_page(page,slot,a,source,index,fitz)
                    else:
                        _draw_attachment(page,slot,a,fitz)
                    position+=1
                if source is not None: source.close()
        try:
            return doc.tobytes(garbage=4, deflate=True)
        finally:
            doc.close()

    @app.get('/voucher-lht/<int:voucher_id>/pdf/<mode>')
    def voucher_caprof_download(voucher_id, mode):
        allowed()
        if mode not in ('a7', 'a4'):
            abort(404)
        try:
            import pymupdf as fitz
        except ImportError:
            try:
                import fitz
            except ImportError:
                abort(503, description='PyMuPDF belum terpasang.')
        data = _render_caprof_pdf_bytes(voucher_id, mode, fitz)
        return Response(data, mimetype='application/pdf', headers={
            'Content-Disposition': f'attachment; filename="CAPROF-{voucher_id:05d}-{mode.upper()}.pdf"',
            'Cache-Control': 'private, no-store',
            'X-Content-Type-Options': 'nosniff',
        })

    @app.get('/voucher-lht/<int:voucher_id>/png/a4')
    def voucher_caprof_download_png_a4(voucher_id):
        """Download all A4 pages as ONE PNG, vertically stacked if multipage."""
        allowed()  # Same shared Harmoni permissions as PDF.
        try:
            import pymupdf as fitz
        except ImportError:
            try:
                import fitz
            except ImportError:
                abort(503, description='PyMuPDF belum terpasang.')
        from PIL import Image
        pdf_bytes = _render_caprof_pdf_bytes(voucher_id, 'a4', fitz)
        with fitz.open(stream=pdf_bytes, filetype='pdf') as pdf_doc:
            if pdf_doc.page_count == 0:
                abort(500, description='PDF voucher kosong.')
            # 180 DPI is legible for A4 and considerably faster than 300 DPI.
            dpi = 180
            scale = dpi / 72
            width = round(pdf_doc[0].rect.width * scale)
            page_height = round(pdf_doc[0].rect.height * scale)
            total_height = page_height * pdf_doc.page_count
            if width * total_height > 100_000_000:
                abort(413, description='Lampiran terlalu banyak untuk satu PNG; gunakan PDF A4.')
            merged = Image.new('RGB', (width, total_height), 'white')
            try:
                for i, page in enumerate(pdf_doc):
                    pix = page.get_pixmap(matrix=fitz.Matrix(scale, scale),
                                          colorspace=fitz.csRGB, alpha=False)
                    # PNG output is a raster image; it cannot contain selectable text.
                    rendered = Image.frombytes('RGB', (pix.width, pix.height), pix.samples)
                    try:
                        merged.paste(rendered, (0, i * page_height))
                    finally:
                        rendered.close()
                output = io.BytesIO()
                merged.save(output, format='PNG', optimize=False,
                            compress_level=1, dpi=(dpi, dpi))
                data = output.getvalue()
            finally:
                merged.close()
        app.logger.info('CAPROF PNG A4 generated voucher_id=%s bytes=%s', voucher_id, len(data))
        return Response(data, mimetype='image/png', headers={
            'Content-Disposition': f'attachment; filename="CAPROF-{voucher_id:05d}-ALL-A4.png"',
            'Content-Length': str(len(data)),
            'Cache-Control': 'private, no-store',
            'X-Content-Type-Options': 'nosniff',
        })


def _draw_voucher(page, rect, v, fitz):
    """Reference-style A6 layout, shared by PNG A6 and A4 PDF quadrant."""
    x, y, w, h = rect.x0, rect.y0, rect.width, rect.height
    sx, sy = w / 298.0, h / 420.0
    green = (0.015, 0.245, 0.17)
    pale = (0.91, 0.95, 0.89)
    light = (0.94, 0.965, 0.94)
    gold = (0.79, 0.64, 0.31)
    ink = (0.08, 0.12, 0.10)

    def rectbox(x0, y0, x1, y1, fill=None, stroke=None, radius=0, lw=0.7):
        r = fitz.Rect(x+x0*sx, y+y0*sy, x+x1*sx, y+y1*sy)
        if radius and hasattr(page, 'draw_rect'):
            # Standard PDF rectangle keeps identical appearance in both export formats.
            pass
        page.draw_rect(r, color=stroke, fill=fill, width=lw)

    def txt(value, xx, baseline, maxw, size, bold=False, color=ink, align='left', minsize=5):
        value = str(value or '-')
        font = 'hebo' if bold else 'helv'
        size *= sy
        max_width = maxw * sx
        while size > minsize*sy and fitz.get_text_length(value, fontname=font, fontsize=size) > max_width:
            size -= 0.3*sy
        if fitz.get_text_length(value, fontname=font, fontsize=size) > max_width:
            while value and fitz.get_text_length(value+'…', fontname=font, fontsize=size) > max_width:
                value=value[:-1]
            value += '…'
        width = fitz.get_text_length(value, fontname=font, fontsize=size)
        px = x + xx*sx + (max_width-width)/2 if align == 'center' else x + xx*sx
        page.insert_text((px, y+baseline*sy), value, fontname=font, fontsize=size, color=color)

    def tanggal(value):
        bulan = ['Januari','Februari','Maret','April','Mei','Juni','Juli','Agustus','September','Oktober','November','Desember']
        try:
            dt = datetime.strptime(str(value)[:10], '%Y-%m-%d')
            return f'{dt.day} {bulan[dt.month-1]} {dt.year}'
        except (ValueError, TypeError):
            return str(value or '-')

    # 9pt safe inset inside each A6 slot (including A4 quadrants).
    rectbox(10, 24, 288, 26, gold)
    txt('H A R M O N I   T R A V E L', 11, 18, 192, 9, True, green)
    voucher_number = f"CAPROF-{int(v['id']):05d}"
    txt(voucher_number, 221, 18, 66, 8, True, green, 'center')
    txt('PENGAJUAN REIMBURSE', 10, 59, 278, 22, True, green)
    rectbox(10, 74, 288, 168, pale)
    nominal = 'Rp' + f"{int(v['nominal']):,}".replace(',', '.')
    txt(nominal, 13, 122, 272, 42, True, green, 'center', minsize=16)
    txt(proper_indonesia(v.get('judul_voucher') or v.get('rincian') or '-'), 16, 152, 266, 17, True, green, 'center', minsize=7)

    rows = [
        ('Pengaju', proper_indonesia(v.get('pengaju') or '-')),
        ('Tgl. pengajuan', tanggal(v.get('tanggal_pengajuan'))),
        ('Tgl. transaksi', tanggal(v.get('tanggal_transaksi'))),
        ('Rincian', proper_indonesia(v.get('rincian') or '-')),
        ('Keperluan', v.get('keperluan') or '-'),
    ]
    for i, (label, value) in enumerate(rows):
        top = 176 + i*26
        rectbox(10, top, 102, top+21, light)
        txt(label, 16, top+14, 83, 9.5, True, green)
        txt(value, 111, top+14, 173, 10.2, False, ink, minsize=6.2)
        if i < len(rows)-1:
            rectbox(110, top+24, 287, top+24.5, (0.85,0.87,0.85))

    rectbox(10, 310, 288, 386, (0.87, 0.93, 0.85))
    txt('TRANSFER KE', 19, 329, 258, 11, True, green)
    bank = str(v.get('bank') or '-').strip().upper()
    rekening = str(v.get('rekening') or '-').strip()
    txt(f'{bank}  •  {rekening}', 19, 355, 256, 21, True, green, minsize=8)
    txt('a.n. ' + str(v.get('pemilik_rekening') or '-').upper(), 19, 376, 256, 12, True, green, minsize=7)
    rectbox(10, 394, 288, 414, (1, 0.975, 0.89), gold, lw=0.7)
    txt('Bukti pembelian: kirim foto bersama pengajuan.', 17, 408, 262, 9, True, green, minsize=6)


def _draw_attachment(page,rect,a,fitz):
    x,y=rect.x0,rect.y0; w=rect.width; h=rect.height
    label=('BUKTI TRANSFER' if a['kind']=='transfer' else 'STRUK / KUITANSI / BUKTI BAYAR')
    page.insert_text((x+8,y+15),label,fontsize=9,fontname='hebo',color=(.02,.28,.19))
    area=fitz.Rect(x+12,y+25,x+w-12,y+h-23)
    data=bytes(a['content'])
    try:
        if a['mime']=='application/pdf':
            src=fitz.open(stream=data,filetype='pdf')
            if src.page_count:
                p=src[0]
                rotation=_best_rotation(p.rect.width,p.rect.height,area.width,area.height)
                page.show_pdf_page(area,src,0,keep_proportion=True,rotate=rotation)
            src.close()
        else:
            pix=fitz.Pixmap(data)
            rotation=_best_rotation(pix.width,pix.height,area.width,area.height)
            page.insert_image(area,stream=data,keep_proportion=True,rotate=rotation)
    except Exception:
        page.insert_text((x+8,y+65),'Lampiran tidak dapat dipratinjau',fontsize=7)
    page.insert_text((x+8,y+h-7),str(a['filename'])[:38],fontsize=6,color=(.3,.3,.3))


def _best_rotation(content_w,content_h,box_w,box_h):
    """Choose 0 or 90 degrees to maximize legible evidence area without cropping."""
    if min(content_w,content_h,box_w,box_h)<=0: return 0
    normal=min(box_w/content_w,box_h/content_h)
    turned=min(box_w/content_h,box_h/content_w)
    return 90 if turned>normal*1.01 else 0


def _draw_pdf_attachment_page(page,rect,a,source,index,fitz):
    x,y,w,h=rect.x0,rect.y0,rect.width,rect.height
    label='BUKTI TRANSFER' if a['kind']=='transfer' else 'STRUK / KUITANSI / BUKTI BAYAR'
    page.insert_text((x+8,y+15),label,fontsize=8,fontname='hebo',color=(.02,.28,.19))
    area=fitz.Rect(x+12,y+25,x+w-12,y+h-23)
    p=source[index]
    rotate=_best_rotation(p.rect.width,p.rect.height,area.width,area.height)
    page.show_pdf_page(area,source,index,keep_proportion=True,rotate=rotate)
    page.insert_text((x+8,y+h-7),str(a['filename'])[:42],fontsize=6,color=(.3,.3,.3))
