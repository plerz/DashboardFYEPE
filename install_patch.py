"""Run from DashboardFYEPE repository root: python install_patch.py"""
from pathlib import Path
import shutil
root=Path.cwd(); app=root/'app.py'; dashboard=root/'templates/dashboard.html'
if not app.exists() or not dashboard.exists(): raise SystemExit('Jalankan dari folder repository yang berisi app.py dan templates/dashboard.html')
source=Path(__file__).resolve().parent
shutil.copy2(source/'youtube_check.py',root/'youtube_check.py')
shutil.copy2(source/'templates/check_video_youtube.html',root/'templates/check_video_youtube.html')
text=app.read_text(encoding='utf-8')
insert='\n# Check Video YouTube (TXT + YouTube Data API)\nfrom youtube_check import register_youtube_check\nregister_youtube_check(app, db, role_required, current_user)\n'
if 'register_youtube_check(app, db, role_required, current_user)' not in text:
    # Register after definitions, but before the application starts.
    if 'if __name__ == "__main__":' in text:
        text=text.replace('if __name__ == "__main__":',insert+'\nif __name__ == "__main__":',1)
    elif "if __name__ == '__main__':" in text:
        text=text.replace("if __name__ == '__main__':",insert+"\nif __name__ == '__main__':",1)
    else: text+=insert
    app.write_text(text,encoding='utf-8')
html=dashboard.read_text(encoding='utf-8')
nav='<a class="admin-top-link" href="/check-video-youtube" {% if request.path.startswith("/check-video-youtube") %}class="active"{% endif %}>▶ Check Video YouTube</a>'
# Avoid duplicate class attribute in link.
nav='<a class="admin-top-link" href="/check-video-youtube">▶ Check Video YouTube</a>'
if '/check-video-youtube' not in html:
    marker='<details class="admin-nav-group" id="nav-ads"'
    if marker not in html: raise SystemExit('Sidebar Engagement/Ads tidak ditemukan. Tambahkan link secara manual.')
    html=html.replace(marker,nav+'\n'+marker,1)
    dashboard.write_text(html,encoding='utf-8')
print('OK: Modul, halaman, route dan sidebar Check Video YouTube terpasang.')
