"""Authorized CSV ingestion (operator CLI, no public upload endpoint).
CSV headers: comment_id,video_id,text,source,created_at. Run on trusted environment
with DATABASE_URL configured; source must describe an authorized export.
"""
import argparse, csv, os, sys
from datetime import datetime, timezone
from comment_service import classify, ensure_schema


def main():
    p=argparse.ArgumentParser()
    p.add_argument("csv_path")
    p.add_argument("--source", required=True, help="Authorized data provenance, e.g. consented-export")
    a=p.parse_args()
    if not a.source.strip(): p.error("Source is required")
    # Reuse application's existing database abstraction without modifying OAuth.
    from app import db
    ensure_schema(db)
    count=0
    with open(a.csv_path, newline="", encoding="utf-8-sig") as f:
        reader=csv.DictReader(f)
        required={"comment_id","video_id","text"}
        if not required.issubset(reader.fieldnames or []): p.error("Required CSV columns: comment_id,video_id,text")
        c=db()
        try:
            for row in reader:
                cid=(row.get("comment_id") or "").strip()
                vid=(row.get("video_id") or "").strip()
                txt=(row.get("text") or "").strip()
                if not cid or not vid or not txt: continue
                c.execute("""INSERT INTO monitored_comments(id,video_id,text,sentiment,source,created_at,imported_at)
                    VALUES(?,?,?,?,?,?,?) ON CONFLICT(id) DO UPDATE SET
                    text=excluded.text,sentiment=excluded.sentiment,source=excluded.source,
                    created_at=excluded.created_at,imported_at=excluded.imported_at""",
                    (cid,vid,txt,classify(txt),a.source,row.get("created_at") or "",datetime.now(timezone.utc).isoformat()))
                count+=1
            c.commit()
        finally:c.close()
    print(f"Imported/updated {count} authorized comments. Classification is keyword-based, not AI.")

if __name__=="__main__": main()
