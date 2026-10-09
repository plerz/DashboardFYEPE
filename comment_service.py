"""Isolated, permission-aware comment storage and conservative sentiment classification.
No TikTok comment fetching is attempted: Login Kit does not grant comment text access.
"""
import re
from collections import Counter

LABELS = ("positif", "negatif", "netral", "campuran")
POSITIVE = {"bagus", "setuju", "mantap", "baik", "hebat", "dukung", "terima kasih", "keren", "sukses"}
NEGATIVE = {"buruk", "kecewa", "tolak", "bohong", "parah", "gagal", "jelek", "marah", "tidak setuju"}

def classify(text):
    """Conservative keyword baseline, not an AI model; sarcasm requires manual review."""
    words = set(re.findall(r"[\w]+", (text or "").lower()))
    lower = (text or "").lower()
    pos = sum(1 for w in POSITIVE if (w in words if " " not in w else w in lower))
    neg = sum(1 for w in NEGATIVE if (w in words if " " not in w else w in lower))
    if pos and neg: return "campuran"
    if pos: return "positif"
    if neg: return "negatif"
    return "netral"

def ensure_schema(db_factory):
    c = db_factory()
    try:
        c.execute("""CREATE TABLE IF NOT EXISTS monitored_comments (
            id VARCHAR(255) PRIMARY KEY,
            video_id VARCHAR(255) NOT NULL,
            text TEXT NOT NULL,
            sentiment VARCHAR(20) NOT NULL,
            source VARCHAR(100) NOT NULL,
            created_at VARCHAR(40),
            imported_at VARCHAR(40) NOT NULL
        )""")
        c.execute("CREATE INDEX IF NOT EXISTS idx_monitored_comments_video ON monitored_comments(video_id)")
        c.commit()
    finally: c.close()

def summary(db_factory, video_ids):
    result = {"count": 0, "counts": {label: 0 for label in LABELS}, "items": []}
    ids = list(dict.fromkeys(str(v) for v in video_ids if v))
    if not ids: return result
    c = db_factory()
    try:
        # Fixed size batches keep queries within SQLite parameter limits.
        for i in range(0, len(ids), 400):
            batch = ids[i:i+400]
            rows = c.execute("SELECT id,video_id,text,sentiment,source,created_at FROM monitored_comments WHERE video_id IN (" + ",".join("?" for _ in batch) + ") ORDER BY imported_at DESC", batch).fetchall()
            for row in rows:
                d = dict(row)
                result["items"].append(d)
                if d["sentiment"] in result["counts"]: result["counts"][d["sentiment"]] += 1
        result["count"] = len(result["items"])
        return result
    finally: c.close()
