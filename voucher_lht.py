"""Harmoni Travel reimbursement vouchers. Attachments are stored in PostgreSQL, not Render /tmp."""
import io, os, re
from datetime import datetime
from zoneinfo import ZoneInfo
from flask import request, redirect, url_for, render_template, flash, abort, Response
from werkzeug.utils import secure_filename

TZ = ZoneInfo('Asia/Jakarta')
ALLOWED = {'image/jpeg', 'image/png', 'image/webp', 'application/pdf'}
MAX_FILE = 8 * 1024 * 1024
MAX_ATTACHMENTS = 24


def register_voucher_lht(app, db, current_user):
    def allowed():
        u = current_user()
        if not u or u['role'] not in ('superadmin', 'harmoni'):
            abort(403)
        return u

    def schema():
        c = db()
        try:
            pk = 'SERIAL PRIMARY KEY' if c.is_pg else 'INTEGER PRIMARY KEY AUTOINCREMENT'
            blob = 'BYTEA' if c.is_pg else 'BLOB'
            c.execute(f'''CREATE TABLE IF NOT EXISTS lht_vouchers (
                id {pk}, owner_id INTEGER NOT NULL, pengaju TEXT NOT NULL,
                nominal BIGINT NOT NULL, rincian TEXT NOT NULL, keperluan TEXT,
                tanggal_pengajuan TEXT NOT NULL, tanggal_transaksi TEXT,
                bank TEXT, rekening TEXT, pemilik_rekening TEXT,
                status_pengajuan TEXT NOT NULL DEFAULT 'Tercatat',
                status_transfer TEXT NOT NULL DEFAULT 'Belum ditransfer',
                created_at TEXT NOT NULL)''')
            c.execute(f'''CREATE TABLE IF NOT EXISTS lht_attachments (
                id {pk}, voucher_id INTEGER NOT NULL, kind TEXT NOT NULL,
                filename TEXT NOT NULL, mime TEXT NOT NULL, content {blob} NOT NULL,
                uploaded_by INTEGER NOT NULL, uploaded_at TEXT NOT NULL)''')
            c.execute('CREATE INDEX IF NOT EXISTS idx_lht_attach_voucher ON lht_attachments(voucher_id)')
            c.commit()
        finally:
            c.close()

    def get_voucher(c, voucher_id):
        v = c.execute('SELECT * FROM lht_vouchers WHERE id=?', (voucher_id,)).fetchone()
        if not v: abort(404)
        u = allowed()
        if u['role'] != 'superadmin' and int(v['owner_id']) != int(u['id']): abort(403)
        return v

    @app.get('/voucher-lht')
    def voucher_lht():
        u = allowed(); schema(); c = db()
        try:
            if u['role'] == 'superadmin':
                rows = c.execute('SELECT * FROM lht_vouchers ORDER BY id DESC LIMIT 200').fetchall()
            else:
                rows = c.execute('SELECT * FROM lht_vouchers WHERE owner_id=? ORDER BY id DESC LIMIT 200', (u['id'],)).fetchall()
            rows = [dict(r) for r in rows]
            for v in rows:
                meta = c.execute('SELECT id,kind,filename FROM lht_attachments WHERE voucher_id=? ORDER BY id DESC', (v['id'],)).fetchall()
                v['transfer_files'] = [dict(a) for a in meta if a['kind']=='transfer']
                v['pembayaran_files'] = [dict(a) for a in meta if a['kind']=='pembayaran']
                v['status_transfer'] = 'Sudah ditransfer' if v['transfer_files'] else 'Belum ditransfer'
        finally: c.close()
        return render_template('voucher_lht.html', user=u, vouchers=rows, today=datetime.now(TZ).date().isoformat())

    @app.post('/voucher-lht/create')
    def voucher_lht_create():
        u = allowed(); schema()
        try: nominal = int(request.form.get('nominal', '0'))
        except ValueError: nominal = 0
        rincian = request.form.get('rincian','').strip()[:240]
        if not 0 < nominal <= 10**12 or not rincian:
            flash('Nominal dan rincian wajib diisi dengan benar.', 'error')
            return redirect(url_for('voucher_lht'))
        today = datetime.now(TZ).date().isoformat()
        def val(name, default='', size=150): return request.form.get(name, default).strip()[:size]
        def date(name):
            raw = val(name, today, 10)
            try: datetime.strptime(raw, '%Y-%m-%d'); return raw
            except ValueError: return today
        c = db()
        try:
            c.execute('''INSERT INTO lht_vouchers(owner_id,pengaju,nominal,rincian,keperluan,tanggal_pengajuan,tanggal_transaksi,bank,rekening,pemilik_rekening,created_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?)''', (u['id'], val('pengaju',u['full_name'] or u['username']),nominal,rincian,val('keperluan'),date('tanggal_pengajuan'),date('tanggal_transaksi'),val('bank','BRI'),val('rekening'),val('pemilik_rekening'),datetime.now(TZ).isoformat()))
            c.commit()
        finally: c.close()
        flash('Voucher berhasil dibuat. Silakan unggah bukti.', 'success')
        return redirect(url_for('voucher_lht'))

    @app.post('/voucher-lht/<int:voucher_id>/upload')
    def voucher_lht_upload(voucher_id):
        u = allowed(); schema(); c = db()
        try:
            get_voucher(c, voucher_id)
            kind = request.form.get('kind','')
            if kind not in ('pembayaran','transfer'): abort(400)
            files = request.files.getlist('files')
            count = c.execute('SELECT COUNT(*) AS n FROM lht_attachments WHERE voucher_id=?',(voucher_id,)).fetchone()['n']
            if not files or count+len(files)>MAX_ATTACHMENTS:
                flash('Maksimal 24 lampiran per voucher.', 'error'); return redirect(url_for('voucher_lht'))
            staged=[]
            for f in files:
                name=secure_filename(f.filename or '')[:180]
                mime=f.mimetype
                data=f.read(MAX_FILE+1)
                if not name or mime not in ALLOWED or not data or len(data)>MAX_FILE:
                    flash('Lampiran harus JPG, PNG, WEBP atau PDF, maksimal 8 MB per file.', 'error')
                    return redirect(url_for('voucher_lht'))
                # Verify file signatures; never trust the browser MIME alone.
                valid = (mime=='application/pdf' and data.startswith(b'%PDF-')) or (mime=='image/png' and data.startswith(b'\x89PNG\r\n\x1a\n')) or (mime=='image/jpeg' and data.startswith(b'\xff\xd8\xff')) or (mime=='image/webp' and data[:4]==b'RIFF' and data[8:12]==b'WEBP')
                if not valid:
                    flash('Format isi file tidak sesuai ekstensi.', 'error'); return redirect(url_for('voucher_lht'))
                staged.append((voucher_id,kind,name,mime,data,u['id'],datetime.now(TZ).isoformat()))
            for item in staged:
                c.execute('INSERT INTO lht_attachments(voucher_id,kind,filename,mime,content,uploaded_by,uploaded_at) VALUES(?,?,?,?,?,?,?)',item)
            if kind=='transfer':
                c.execute("UPDATE lht_vouchers SET status_transfer='Sudah ditransfer' WHERE id=?",(voucher_id,))
            c.commit(); flash(f'{len(staged)} lampiran berhasil diunggah.', 'success')
        finally: c.close()
        return redirect(url_for('voucher_lht'))

    @app.post('/voucher-lht/<int:voucher_id>/edit')
    def voucher_lht_edit(voucher_id):
        allowed(); schema(); c=db()
        try:
            get_voucher(c,voucher_id)
            field=request.form.get('field','')
            if field not in ('pengaju','rincian','keperluan'): abort(400)
            value=request.form.get('value','').strip()[:240]
            if not value and field in ('pengaju','rincian'):
                flash('Kolom wajib tidak boleh kosong.','error')
            else:
                c.execute(f'UPDATE lht_vouchers SET {field}=? WHERE id=?',(value,voucher_id))
                c.commit(); flash('Data voucher diperbarui.','success')
        finally: c.close()
        return redirect(url_for('voucher_lht'))

    @app.post('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/delete')
    def voucher_lht_clear_attachment(voucher_id, attachment_id):
        allowed(); schema(); c = db()
        try:
            get_voucher(c, voucher_id)
            attachment = c.execute(
                'SELECT id, kind FROM lht_attachments WHERE id=? AND voucher_id=?',
                (attachment_id, voucher_id)).fetchone()
            if not attachment: abort(404)
            c.execute('DELETE FROM lht_attachments WHERE id=? AND voucher_id=?',
                      (attachment_id, voucher_id))
            if attachment['kind'] == 'transfer':
                remaining = c.execute(
                    "SELECT COUNT(*) AS n FROM lht_attachments WHERE voucher_id=? AND kind='transfer'",
                    (voucher_id,)).fetchone()['n']
                c.execute('UPDATE lht_vouchers SET status_transfer=? WHERE id=?',
                          ('Sudah ditransfer' if remaining else 'Belum ditransfer', voucher_id))
            c.commit()
        finally:
            c.close()
        flash('Satu lampiran berhasil dihapus.', 'success')
        return redirect(url_for('voucher_lht'))

    @app.post('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/replace')
    def voucher_lht_replace_attachment(voucher_id, attachment_id):
        u = allowed(); schema()
        f = request.files.get('file')
        if not f:
            flash('Pilih satu file pengganti.', 'error')
            return redirect(url_for('voucher_lht'))
        name = secure_filename(f.filename or '')[:180]
        mime = f.mimetype
        data = f.read(MAX_FILE + 1)
        valid = ((mime == 'application/pdf' and data.startswith(b'%PDF-')) or
                 (mime == 'image/png' and data.startswith(b'\x89PNG\r\n\x1a\n')) or
                 (mime == 'image/jpeg' and data.startswith(b'\xff\xd8\xff')) or
                 (mime == 'image/webp' and data[:4] == b'RIFF' and data[8:12] == b'WEBP'))
        if not name or not valid or not data or len(data) > MAX_FILE:
            flash('File harus JPG, PNG, WEBP, atau PDF maksimal 8 MB.', 'error')
            return redirect(url_for('voucher_lht'))
        c = db()
        try:
            get_voucher(c, voucher_id)
            exists = c.execute('SELECT id FROM lht_attachments WHERE id=? AND voucher_id=?',
                               (attachment_id, voucher_id)).fetchone()
            if not exists: abort(404)
            c.execute("""UPDATE lht_attachments
                         SET filename=?, mime=?, content=?, uploaded_by=?, uploaded_at=?
                         WHERE id=? AND voucher_id=?""",
                      (name, mime, data, u['id'], datetime.now(TZ).isoformat(),
                       attachment_id, voucher_id))
            c.commit()
        finally:
            c.close()
        flash('Satu lampiran berhasil diganti.', 'success')
        return redirect(url_for('voucher_lht'))

    @app.get('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>/pdf')
    def voucher_lht_attachment_pdf(voucher_id,attachment_id):
        allowed()
        try: import fitz
        except ImportError: abort(503)
        c=db()
        try:
            get_voucher(c,voucher_id)
            r=c.execute('SELECT * FROM lht_attachments WHERE id=? AND voucher_id=?',(attachment_id,voucher_id)).fetchone()
            if not r: abort(404)
            a=dict(r)
        finally: c.close()
        doc=fitz.open(); w,h=fitz.paper_size('a6'); page=doc.new_page(width=w,height=h)
        _draw_attachment(page,fitz.Rect(0,0,w,h),a,fitz)
        data=doc.tobytes(garbage=4,deflate=True); doc.close()
        return Response(data,mimetype='application/pdf',headers={'Content-Disposition':f'attachment; filename="LHT-{voucher_id:05d}-BUKTI-{attachment_id}-A6.pdf"','Cache-Control':'private, no-store'})

    @app.get('/voucher-lht/<int:voucher_id>/attachment/<int:attachment_id>')
    def voucher_lht_attachment(voucher_id,attachment_id):
        allowed(); c=db()
        try:
            get_voucher(c,voucher_id)
            r=c.execute('SELECT * FROM lht_attachments WHERE id=? AND voucher_id=?',(attachment_id,voucher_id)).fetchone()
            if not r: abort(404)
            return Response(bytes(r['content']),mimetype=r['mime'],headers={'Content-Disposition':'attachment; filename="'+secure_filename(r['filename'])+'"','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})
        finally: c.close()

    @app.get('/voucher-lht/<int:voucher_id>/pdf/<mode>')
    def voucher_lht_download(voucher_id,mode):
        allowed()
        if mode not in ('a7','a4'): abort(404)
        try: import fitz
        except ImportError:
            flash('Library PyMuPDF belum terpasang. Tambahkan PyMuPDF ke requirements.txt.', 'error')
            return redirect(url_for('voucher_lht'))
        c=db()
        try:
            v=dict(get_voucher(c,voucher_id))
            attachments=[dict(r) for r in c.execute("SELECT * FROM lht_attachments WHERE voucher_id=? ORDER BY CASE kind WHEN 'transfer' THEN 0 ELSE 1 END,id",(voucher_id,)).fetchall()] if mode=='a4' else []
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
        pdf=doc.tobytes(garbage=4,deflate=True); doc.close()
        return Response(pdf,mimetype='application/pdf',headers={'Content-Disposition':f'attachment; filename="LHT-{voucher_id:05d}-{mode.upper()}.pdf"','Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'})


def _draw_voucher(page,rect,v,fitz):
    x,y=rect.x0,rect.y0; w=rect.width
    green=(.02,.28,.19)
    page.draw_rect(fitz.Rect(x+5,y+5,x+w-5,y+29),color=None,fill=(.92,.96,.9))
    def line(s, yy, size=9, bold=False, color=(0,0,0)):
        page.insert_text((x+10,y+yy),str(s)[:58],fontsize=size,fontname='hebo' if bold else 'helv',color=color)
    line('HARMONI TRAVEL',19,12,True,green)
    line('PENGAJUAN REIMBURSE',47,13,True,green)
    line('Rp'+f"{int(v['nominal']):,}".replace(',','.'),81,23,True,green)
    line(v['rincian'],99,10,True,green)
    rows=[('No. voucher',f"LHT-{int(v['id']):05d}"),('Pengaju',v['pengaju']),('Tgl pengajuan',v['tanggal_pengajuan']),('Tgl transaksi',v['tanggal_transaksi'] or '-'),('Keperluan',v['keperluan'] or '-'),('Status',v['status_transfer'])]
    for i,(k,value) in enumerate(rows):
        line(k,120+i*17,7,True,green)
        page.insert_text((x+80,y+120+i*17),str(value)[:27],fontsize=7,fontname='helv')
    page.draw_rect(fitz.Rect(x+5,y+220,x+w-5,y+277),color=None,fill=(.88,.94,.86))
    line('TRANSFER KE',234,8,True,green)
    line((v['bank'] or '-')+'  '+(v['rekening'] or '-'),251,11,True,green)
    line(v['pemilik_rekening'] or '-',266,8,False,green)
    line('Lampiran bukti pada cetakan A4 lengkap',289,6,False,green)


def _draw_attachment(page,rect,a,fitz):
    x,y=rect.x0,rect.y0; w=rect.width; h=rect.height
    label=('BUKTI TRANSFER KEUANGAN' if a['kind']=='transfer' else 'BUKTI PEMBAYARAN')
    page.insert_text((x+8,y+15),label,fontsize=9,fontname='hebo',color=(.02,.28,.19))
    area=fitz.Rect(x+6,y+22,x+w-6,y+h-20)
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
    area=fitz.Rect(x+6,y+23,x+w-6,y+h-18)
    p=source[index]
    rotate=_best_rotation(p.rect.width,p.rect.height,area.width,area.height)
    page.show_pdf_page(area,source,index,keep_proportion=True,rotate=rotate)
    page.insert_text((x+8,y+h-7),str(a['filename'])[:42],fontsize=6,color=(.3,.3,.3))
