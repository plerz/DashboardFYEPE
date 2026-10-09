# Modul komentar DashFYEPE — tahap 1

## Batasan
- Tidak mengubah OAuth, callback, posting, refresh token, atau pengambilan statistik video.
- Tidak melakukan scraping atau mengklaim Login Kit menyediakan teks komentar.
- Hanya mengolah komentar yang diperoleh dengan izin yang sesuai.
- Sentimen tahap ini **aturan kata kunci sederhana, bukan AI**. Untuk AI produksi diperlukan model/provider dan evaluasi bahasa Indonesia.

## File GitHub
Ganti `app.py` dan `templates/customer_dashboard.html`. Tambahkan `comment_service.py` dan `import_comments.py` di root. Jangan hapus file lain.

## Impor opsional (hanya CSV dari sumber berizin)
Kolom wajib: `comment_id,video_id,text`; opsional: `created_at`.
Contoh perintah pada environment dengan DATABASE_URL yang sama:
`python import_comments.py comments.csv --source consented-export`

## Penting
Database membuat tabel `monitored_comments` saat customer dashboard pertama dibuka. Impor CLI juga membuat tabel. Impor mengupdate komentar berdasarkan comment_id. Klasifikasi otomatis awal bisa salah dan tidak setara dengan analisis AI.

## Tahap selanjutnya
Setelah sumber komentar resmi terotorisasi tersedia, buat adapter pengambilan teks komentar ke tabel ini. Jangan menambahkan scope TikTok yang tidak didukung.
