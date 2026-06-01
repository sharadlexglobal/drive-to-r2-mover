#!/usr/bin/env python3
"""Crunchbase 2.8M companies (30 chunks) → filter India → Supabase.

India filter: `locations` contains 'india' (case-insensitive),
excluding 'indianapolis' and 'indian ocean'.
"""
import csv
import http.server
import io
import os
import re
import socketserver
import sys
import threading
import time
import traceback
import zipfile

import boto3
import psycopg2
from botocore.config import Config

PORT = int(os.environ.get("PORT", "10000"))
R2_BUCKET = os.environ.get("R2_BUCKET", "drive-archive-2026")
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET = os.environ["R2_SECRET_ACCESS_KEY"]
PG = dict(
    host=os.environ["PG_HOST"], port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "postgres"),
    user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"], sslmode="require",
)

ZIP_KEY = "Almost Full Crunchbase Database 2,807,492.zip"
TABLE = "crunchbase_india"
CHUNK_ROWS = 10_000

# Source CSV header -> dest column
COLUMN_MAP = {
    "id": "cb_id",
    "created_at": "created_at",
    "name": "name",
    "short_description": "short_description",
    "semrush_global_rank": "semrush_global_rank",
    "semrush_visits_latest_month": "semrush_visits_latest_month",
    "num_investors": "num_investors",
    "funding_total": "funding_total",
    "num_exits": "num_exits",
    "num_funding_rounds": "num_funding_rounds",
    "last_funding_type": "last_funding_type",
    "last_funding_at": "last_funding_at",
    "num_acquisitions": "num_acquisitions",
    "apptopia_total_apps": "apptopia_total_apps",
    "apptopia_total_downloads": "apptopia_total_downloads",
    "contact_email": "contact_email",
    "phone_number": "phone_number",
    "facebook": "facebook",
    "linkedin": "linkedin",
    "twitter": "twitter",
    "num_investments": "num_investments",
    "num_lead_investments": "num_lead_investments",
    "num_lead_investors": "num_lead_investors",
    "listed_stock_symbol": "listed_stock_symbol",
    "company_type": "company_type",
    "hub_tags": "hub_tags",
    "operating_status": "operating_status",
    "founded_on": "founded_on",
    "categories": "categories",
    "founders": "founders",
    "website": "website",
    "ipo_status": "ipo_status",
    "num_employees_enum": "num_employees_enum",
    "locations": "locations",
    "growth_insight_description": "growth_insight_description",
    "growth_insight_indicator": "growth_insight_indicator",
    "growth_insight_direction": "growth_insight_direction",
    "growth_insight_confidence": "growth_insight_confidence",
    "investor_insight_description": "investor_insight_description",
    "permalink": "permalink",
    "url": "url",
}
DEST_COLS = list(COLUMN_MAP.values())

csv.field_size_limit(sys.maxsize)
s3 = boto3.client("s3", endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS, aws_secret_access_key=R2_SECRET,
    config=Config(signature_version="s3v4", region_name="auto"))

state = {
    "status": "starting", "started_at": time.time(),
    "rows_scanned": 0, "rows_loaded": 0, "rows_skipped": 0,
    "chunks_done": 0, "chunks_total": 0, "current_chunk": "",
    "avg_rate": 0, "last_error": None,
}

INDIA_RE = re.compile(r"\bindia\b", re.IGNORECASE)
EXCLUDE_RE = re.compile(r"indianapolis|indian ocean|indian wells|indianola", re.IGNORECASE)


class S3RangeReader(io.RawIOBase):
    def __init__(self, bucket, key):
        head = s3.head_object(Bucket=bucket, Key=key)
        self.size = head["ContentLength"]
        self.bucket, self.key, self.pos = bucket, key, 0
    def readable(self): return True
    def seekable(self): return True
    def seek(self, o, w=0):
        self.pos = o if w==0 else (self.pos+o if w==1 else self.size+o); return self.pos
    def tell(self): return self.pos
    def read(self, n=-1):
        if n is None or n < 0 or self.pos + n > self.size: n = self.size - self.pos
        if n == 0: return b""
        end = self.pos + n - 1
        r = s3.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={self.pos}-{end}")
        d = r["Body"].read(); self.pos += len(d); return d
    def readinto(self, b):
        d = self.read(len(b)); n = len(d); b[:n] = d; return n


def esc(v):
    if not v: return r"\N"
    return v.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def get_writable_conn():
    for i in range(200):
        try:
            c = psycopg2.connect(**PG)
            c.autocommit = False
            cu = c.cursor()
            cu.execute("SET statement_timeout = 0")
            cu.execute("SET idle_in_transaction_session_timeout = 0")
            cu.execute("SET transaction_read_only = off")
            cu.execute("SHOW transaction_read_only")
            if cu.fetchone()[0] == "on":
                c.close(); time.sleep(0.3 + 0.05*i); continue
            return c, cu
        except Exception as e:
            print(f"conn fail {i}: {e}", flush=True); time.sleep(0.5)
    raise RuntimeError("no writable conn")


def is_india(loc):
    if not loc: return False
    if EXCLUDE_RE.search(loc):
        # Could have both — only skip if doesn't ALSO have a clear India match
        if not INDIA_RE.search(re.sub(EXCLUDE_RE, '', loc)):
            return False
    return bool(INDIA_RE.search(loc))


def run_import():
    try:
        state["status"] = "opening R2 zip"
        s3r = S3RangeReader(R2_BUCKET, ZIP_KEY)
        buf = io.BufferedReader(s3r, buffer_size=8*1024*1024)
        zf = zipfile.ZipFile(buf)
        chunks = sorted([m for m in zf.infolist()
                         if m.filename.endswith('.csv') and not m.filename.startswith('__MACOSX')],
                        key=lambda m: m.filename)
        state["chunks_total"] = len(chunks)
        print(f"found {len(chunks)} chunks", flush=True)

        conn, cur = get_writable_conn()
        cols_sql = ", ".join(DEST_COLS)
        copy_sql = f"COPY {TABLE}({cols_sql}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"

        state["status"] = "streaming"
        t_start = time.time()
        chunk_buf = io.StringIO()
        chunk_n = 0

        def flush():
            nonlocal chunk_buf, chunk_n, conn, cur
            if chunk_n == 0: return
            payload = chunk_buf.getvalue()
            ok = False
            for attempt in range(50):
                try:
                    cur.copy_expert(copy_sql, io.StringIO(payload))
                    conn.commit()
                    ok = True; break
                except (psycopg2.errors.ReadOnlySqlTransaction,
                        psycopg2.errors.OperationalError,
                        psycopg2.InterfaceError):
                    try: conn.close()
                    except Exception: pass
                    conn, cur = get_writable_conn()
            if ok:
                state["rows_loaded"] += chunk_n
                state["avg_rate"] = state["rows_loaded"] / max(0.01, time.time() - t_start)
                print(f"+{chunk_n} loaded | scanned {state['rows_scanned']:,} | "
                      f"chunk {state['chunks_done']}/{state['chunks_total']} | "
                      f"total {state['rows_loaded']:,}", flush=True)
            else:
                state["rows_skipped"] += chunk_n
            chunk_buf = io.StringIO()
            chunk_n = 0

        for chunk_meta in chunks:
            state["current_chunk"] = chunk_meta.filename.split('/')[-1]
            try:
                with zf.open(chunk_meta) as member:
                    text = io.TextIOWrapper(member, encoding="utf-8", errors="replace", newline="")
                    reader = csv.reader(text)
                    try:
                        header = next(reader)
                    except StopIteration:
                        continue
                    src_to_dest = {}
                    for src_idx, src_name in enumerate(header):
                        dest_name = COLUMN_MAP.get(src_name)
                        if dest_name:
                            src_to_dest[src_idx] = DEST_COLS.index(dest_name)
                    loc_idx = header.index("locations") if "locations" in header else -1
                    N_DEST = len(DEST_COLS)

                    for row in reader:
                        state["rows_scanned"] += 1
                        if loc_idx < 0 or loc_idx >= len(row):
                            continue
                        if not is_india(row[loc_idx]):
                            continue
                        out = [""] * N_DEST
                        for src_idx, dest_idx in src_to_dest.items():
                            if src_idx < len(row):
                                out[dest_idx] = row[src_idx]
                        chunk_buf.write("\t".join(esc(v) for v in out))
                        chunk_buf.write("\n")
                        chunk_n += 1
                        if chunk_n >= CHUNK_ROWS:
                            flush()
            except Exception as e:
                state["last_error"] = f"chunk {chunk_meta.filename}: {type(e).__name__}: {str(e)[:200]}"
                print(state["last_error"], flush=True)
            state["chunks_done"] += 1

        flush()
        conn.close()
        state["status"] = (f"done in {time.time()-t_start:.0f}s: {state['rows_loaded']:,} India rows "
                          f"(scanned {state['rows_scanned']:,} across {state['chunks_done']} chunks)")
        print(state["status"], flush=True)
    except Exception as e:
        state["status"] = f"FATAL: {type(e).__name__}: {e}"
        state["last_error"] = traceback.format_exc()[-1500:]
        print(state["status"], flush=True)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a, **k): pass
    def do_GET(self):
        body = (
            f"status:        {state['status']}\n"
            f"elapsed:       {int(time.time() - state['started_at'])}s\n"
            f"rows_scanned:  {state['rows_scanned']:,}\n"
            f"rows_loaded:   {state['rows_loaded']:,}\n"
            f"rows_skipped:  {state['rows_skipped']:,}\n"
            f"chunks:        {state['chunks_done']}/{state['chunks_total']}\n"
            f"current_chunk: {state['current_chunk']}\n"
            f"avg_rate:      {state['avg_rate']:,.0f}/s\n"
            f"last_error:    {state['last_error'] or 'none'}\n"
        )
        self.send_response(200); self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers(); self.wfile.write(body.encode())


def main():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=run_import, daemon=True).start()
    httpd.serve_forever()


if __name__ == "__main__":
    main()
