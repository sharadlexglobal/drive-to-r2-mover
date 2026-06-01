#!/usr/bin/env python3
"""Apollo per_all_fields.csv (54 GB, 93M people, TSV) → filter India → Supabase.

Reuses same R2 streaming + COPY pattern as import_linkedin.py but:
- Source is TSV (tab-delimited), not CSV
- Filter rows where person_location_country == 'India'
- Project only useful columns (drop internal Apollo IDs)
"""
import csv
import http.server
import io
import os
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

ZIP_KEY = "Entire Apollo Database 99,311,285.zip"
CSV_PATH = "Entire Apollo Database 99,311,285/Apollo_V7_V5_per_all_fields 93,239,628.csv"
TABLE = "apollo_india_people"
CHUNK_ROWS = 100_000
FILTER_COUNTRY = "India"

# Map source column name -> destination column name (None = skip)
COLUMN_MAP = {
    "person_name": "person_name",
    "person_first_name_unanalyzed": "first_name",
    "person_last_name_unanalyzed": "last_name",
    "person_name_unanalyzed_downcase": None,
    "person_title": "title",
    "person_functions": "functions",
    "person_seniority": "seniority",
    "person_email_status_cd": "email_status",
    "person_extrapolated_email_confidence": "email_confidence",
    "person_email": "email",
    "person_phone": "phone",
    "person_sanitized_phone": "sanitized_phone",
    "person_email_analyzed": None,
    "person_linkedin_url": "linkedin_url",
    "person_detailed_function": "detailed_function",
    "person_title_normalized": "title_normalized",
    "primary_title_normalized_for_faceting": None,
    "sanitized_organization_name_unanalyzed": "organization_name",
    "person_location_city": "location_city",
    "person_location_city_with_state_or_country": None,
    "person_location_state": "location_state",
    "person_location_state_with_country": None,
    "person_location_country": "location_country",
    "person_location_postal_code": "location_postal_code",
    "job_start_date": "job_start_date",
    "current_organization_ids": None,
    "modality": None,
    "prospected_by_team_ids": None,
    "person_excluded_by_team_ids": None,
    "relavence_boost": None,
    "person_num_linkedin_connections": "linkedin_connections",
    "person_location_geojson": None,
    "predictive_scores": None,
    "person_vacuumed_at": None,
    "random": None,
    "_index": None,
    "_type": None,
    "_id": None,
    "_score": None,
}
DEST_COLS = [v for v in COLUMN_MAP.values() if v]

csv.field_size_limit(sys.maxsize)
s3 = boto3.client("s3", endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS, aws_secret_access_key=R2_SECRET,
    config=Config(signature_version="s3v4", region_name="auto"))

state = {
    "status": "starting", "started_at": time.time(),
    "rows_scanned": 0, "rows_loaded": 0, "rows_skipped": 0,
    "last_chunk_rate": 0, "avg_rate": 0, "last_error": None,
}


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
        d = r["Body"].read()
        self.pos += len(d)
        return d
    def readinto(self, b):
        d = self.read(len(b)); n = len(d); b[:n] = d; return n


def esc(v):
    if not v: return r"\N"
    return v.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def get_writable_conn():
    for i in range(100):
        try:
            c = psycopg2.connect(**PG)
            c.autocommit = False
            cu = c.cursor()
            cu.execute("SET statement_timeout = 0")
            cu.execute("SET idle_in_transaction_session_timeout = 0")
            cu.execute("SHOW transaction_read_only")
            if cu.fetchone()[0] == "on":
                c.close(); time.sleep(0.5 + 0.1*i); continue
            return c, cu
        except Exception as e:
            print(f"conn fail {i}: {e}", flush=True); time.sleep(1)
    raise RuntimeError("no writable conn")


def run_import():
    try:
        state["status"] = "opening R2 zip"
        s3r = S3RangeReader(R2_BUCKET, ZIP_KEY)
        buf = io.BufferedReader(s3r, buffer_size=16*1024*1024)
        zf = zipfile.ZipFile(buf)
        state["status"] = f"opening {CSV_PATH}"
        member = zf.open(CSV_PATH)
        text = io.TextIOWrapper(member, encoding="utf-8", errors="replace", newline="")
        reader = csv.reader(text, delimiter='\t')
        header = next(reader)
        print(f"header: {len(header)} cols", flush=True)

        # Build src column -> dest column mapping by index
        src_to_dest = {}  # src_idx -> dest_idx (in DEST_COLS)
        for src_idx, src_name in enumerate(header):
            dest_name = COLUMN_MAP.get(src_name)
            if dest_name and dest_name in DEST_COLS:
                src_to_dest[src_idx] = DEST_COLS.index(dest_name)
        country_idx = header.index("person_location_country")
        print(f"country col idx: {country_idx}, kept {len(src_to_dest)} cols", flush=True)

        conn, cur = get_writable_conn()
        cols_sql = ", ".join(DEST_COLS)
        copy_sql = f"COPY {TABLE}({cols_sql}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"

        state["status"] = "streaming"
        t_start = time.time()
        chunk_buf = io.StringIO()
        chunk_n = 0
        N_DEST = len(DEST_COLS)

        for row in reader:
            state["rows_scanned"] += 1
            if len(row) <= country_idx or row[country_idx] != FILTER_COUNTRY:
                continue
            # Project columns
            out = [""] * N_DEST
            for src_idx, dest_idx in src_to_dest.items():
                if src_idx < len(row):
                    out[dest_idx] = row[src_idx]
            chunk_buf.write("\t".join(esc(v) for v in out))
            chunk_buf.write("\n")
            chunk_n += 1

            if chunk_n >= CHUNK_ROWS:
                payload = chunk_buf.getvalue()
                ok = False
                for attempt in range(30):
                    try:
                        cur.copy_expert(copy_sql, io.StringIO(payload))
                        conn.commit()
                        ok = True; break
                    except (psycopg2.errors.ReadOnlySqlTransaction,
                            psycopg2.errors.OperationalError,
                            psycopg2.InterfaceError) as e:
                        state["last_error"] = f"retry {attempt}: {type(e).__name__}"
                        try: conn.close()
                        except Exception: pass
                        conn, cur = get_writable_conn()
                if ok:
                    state["rows_loaded"] += chunk_n
                    state["last_chunk_rate"] = chunk_n / max(0.01, time.time() - t_start - (state["rows_loaded"] - chunk_n) / max(0.01, state.get("avg_rate", 1)))
                    state["avg_rate"] = state["rows_loaded"] / max(0.01, time.time() - t_start)
                    print(f"+{chunk_n:,} loaded | scanned {state['rows_scanned']:,} | "
                          f"avg {state['avg_rate']:,.0f}/s | total loaded {state['rows_loaded']:,}", flush=True)
                else:
                    state["rows_skipped"] += chunk_n
                chunk_buf = io.StringIO()
                chunk_n = 0

        # Final flush
        if chunk_n > 0:
            try:
                cur.copy_expert(copy_sql, io.StringIO(chunk_buf.getvalue()))
                conn.commit()
                state["rows_loaded"] += chunk_n
            except Exception:
                state["rows_skipped"] += chunk_n

        conn.close()
        state["status"] = f"done in {time.time()-t_start:.0f}s: {state['rows_loaded']:,} India rows (scanned {state['rows_scanned']:,})"
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
