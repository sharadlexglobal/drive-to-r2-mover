#!/usr/bin/env python3
"""LinkedIn India.csv (16 GB) → Supabase via streaming COPY in chunks.

Reads R2 zip → DEFLATE stream → csv.reader (lenient) → batch TSV → COPY.
Runs as a Render web service: exposes / and /status for live progress.
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

# --------------------------------------------------------------------- config

PORT = int(os.environ.get("PORT", "10000"))

R2_BUCKET = os.environ.get("R2_BUCKET", "drive-archive-2026")
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET = os.environ["R2_SECRET_ACCESS_KEY"]

PG = dict(
    host=os.environ["PG_HOST"],
    port=int(os.environ.get("PG_PORT", "5432")),
    dbname=os.environ.get("PG_DB", "postgres"),
    user=os.environ["PG_USER"],
    password=os.environ["PG_PASSWORD"],
    sslmode="require",
)

ZIP_KEY = os.environ.get(
    "ZIP_KEY", "Entire Linkedin Database 434,832,484.zip"
)
CSV_PATH = os.environ.get(
    "CSV_PATH",
    "Entire Linkedin Database 434,832,484/by Countries 339,408,442/"
    "Copy of India/India.csv",
)
TABLE = os.environ.get("TABLE", "linkedin_india")
CHUNK_ROWS = int(os.environ.get("CHUNK_ROWS", "200000"))

COLUMNS = [
    "full_name","industry","job_title","sub_role","industry_2",
    "emails","mobile","phone_numbers","company_name","company_industry",
    "company_website","company_size","company_founded","location","locality",
    "metro","region","skills","first_name","middle_initial",
    "middle_name","last_name","birth_year","birth_date","gender",
    "linkedin_url","linkedin_username","facebook_url","facebook_username","twitter_url",
    "twitter_username","github_url","github_username","company_linkedin_url","company_facebook_url",
    "company_twitter_url","company_location_name","company_location_locality","company_location_metro","company_location_region",
    "company_location_geo","company_location_street_address","company_location_address_line_2","company_location_postal_code","company_location_country",
    "company_location_continent","last_updated_company","start_date","job_summary","location_country",
    "location_continent","street_address","address_line_2","postal_code","location_geo",
    "last_updated","linkedin_connections","inferred_salary","years_experience","summary",
    "countries","interests",
]
N_COLS = len(COLUMNS)
assert N_COLS == 62

csv.field_size_limit(sys.maxsize)

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS,
    aws_secret_access_key=R2_SECRET,
    config=Config(signature_version="s3v4", region_name="auto"),
)

state = {
    "status": "starting",
    "started_at": time.time(),
    "rows_loaded": 0,
    "rows_skipped": 0,
    "last_chunk_rate": 0,
    "avg_rate": 0,
    "last_error": None,
    "last_update": time.time(),
}

# --------------------------------------------------------------------- S3 reader

class S3RangeReader(io.RawIOBase):
    def __init__(self, bucket, key):
        head = s3.head_object(Bucket=bucket, Key=key)
        self.size = head["ContentLength"]
        self.bucket = bucket
        self.key = key
        self.pos = 0
    def readable(self): return True
    def seekable(self): return True
    def seek(self, offset, whence=0):
        if whence == 0: self.pos = offset
        elif whence == 1: self.pos += offset
        elif whence == 2: self.pos = self.size + offset
        return self.pos
    def tell(self): return self.pos
    def read(self, n=-1):
        if n is None or n < 0 or self.pos + n > self.size:
            n = self.size - self.pos
        if n == 0: return b""
        end = self.pos + n - 1
        resp = s3.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={self.pos}-{end}")
        data = resp["Body"].read()
        self.pos += len(data)
        return data
    def readinto(self, b):
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

# --------------------------------------------------------------------- importer

def escape_tsv(v):
    if not v:
        return r"\N"
    return (
        v.replace("\\", "\\\\")
         .replace("\t", "\\t")
         .replace("\n", "\\n")
         .replace("\r", "\\r")
    )

def normalize_row(row):
    n = len(row)
    if n == N_COLS:
        return row
    if n < N_COLS:
        return row + [""] * (N_COLS - n)
    return row[: N_COLS - 1] + [",".join(row[N_COLS - 1 :])]

def run_import():
    try:
        state["status"] = "opening R2 zip"
        s3r = S3RangeReader(R2_BUCKET, ZIP_KEY)
        buffered = io.BufferedReader(s3r, buffer_size=16 * 1024 * 1024)
        zf = zipfile.ZipFile(buffered)
        state["status"] = f"opening member {CSV_PATH}"
        csv_bytes = zf.open(CSV_PATH)
        csv_text = io.TextIOWrapper(csv_bytes, encoding="utf-8", errors="replace", newline="")
        reader = csv.reader(csv_text)
        header = next(reader)
        print(f"header has {len(header)} fields", flush=True)

        state["status"] = "connecting Postgres"
        conn = psycopg2.connect(**PG)
        conn.autocommit = False
        cur = conn.cursor()
        cur.execute("SET statement_timeout = 0")
        cur.execute("SET idle_in_transaction_session_timeout = 0")
        cur.execute(
            "INSERT INTO import_progress(source, status, rows_loaded) VALUES (%s,'running',0) "
            "ON CONFLICT (source) DO UPDATE SET started_at=NOW(), status='running', rows_loaded=0",
            (CSV_PATH,),
        )
        conn.commit()

        cols_sql = ", ".join(COLUMNS)
        copy_sql = (
            f"COPY {TABLE}({cols_sql}) FROM STDIN "
            f"WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"
        )

        state["status"] = "streaming"
        t_start = time.time()
        buf = io.StringIO()
        chunk_n = 0

        for row in reader:
            try:
                row = normalize_row(row)
                buf.write("\t".join(escape_tsv(c) for c in row))
                buf.write("\n")
                chunk_n += 1
            except Exception:
                state["rows_skipped"] += 1
                continue

            if chunk_n >= CHUNK_ROWS:
                t0 = time.time()
                buf.seek(0)
                try:
                    cur.copy_expert(copy_sql, buf)
                    conn.commit()
                    state["rows_loaded"] += chunk_n
                    dt = time.time() - t0
                    state["last_chunk_rate"] = chunk_n / dt if dt > 0 else 0
                    state["avg_rate"] = state["rows_loaded"] / (time.time() - t_start)
                    state["last_update"] = time.time()
                    print(f"+{chunk_n:,} rows in {dt:.1f}s ({state['last_chunk_rate']:,.0f}/s) | "
                          f"total {state['rows_loaded']:,} ({state['avg_rate']:,.0f}/s avg)", flush=True)
                    cur.execute("UPDATE import_progress SET rows_loaded=%s, last_update=NOW() WHERE source=%s",
                                (state["rows_loaded"], CSV_PATH))
                    conn.commit()
                except Exception as e:
                    conn.rollback()
                    state["rows_skipped"] += chunk_n
                    state["last_error"] = f"{type(e).__name__}: {str(e)[:200]}"
                    print(f"CHUNK FAIL: {state['last_error']}", flush=True)
                buf = io.StringIO()
                chunk_n = 0

        # final flush
        if chunk_n > 0:
            buf.seek(0)
            try:
                cur.copy_expert(copy_sql, buf)
                conn.commit()
                state["rows_loaded"] += chunk_n
            except Exception as e:
                conn.rollback()
                state["rows_skipped"] += chunk_n
                state["last_error"] = f"{type(e).__name__}: {str(e)[:200]}"

        cur.execute(
            "UPDATE import_progress SET rows_loaded=%s, status='done', last_update=NOW(), notes=%s WHERE source=%s",
            (state["rows_loaded"], f"skipped={state['rows_skipped']}", CSV_PATH),
        )
        conn.commit()
        conn.close()
        elapsed = time.time() - t_start
        state["status"] = (
            f"done in {elapsed:.0f}s: {state['rows_loaded']:,} loaded, {state['rows_skipped']:,} skipped"
        )
        print(state["status"], flush=True)
    except Exception as e:
        state["status"] = f"FATAL: {type(e).__name__}: {e}"
        state["last_error"] = traceback.format_exc()[-1500:]
        print(state["status"], flush=True)
        print(state["last_error"], flush=True)


# --------------------------------------------------------------------- http

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a, **k): pass
    def do_GET(self):
        elapsed = int(time.time() - state["started_at"])
        body = (
            f"status:        {state['status']}\n"
            f"elapsed:       {elapsed}s\n"
            f"rows_loaded:   {state['rows_loaded']:,}\n"
            f"rows_skipped:  {state['rows_skipped']:,}\n"
            f"last_chunk:    {state['last_chunk_rate']:,.0f}/s\n"
            f"avg_rate:      {state['avg_rate']:,.0f}/s\n"
            f"last_update:   {int(time.time() - state['last_update'])}s ago\n"
            f"last_error:    {state['last_error'] or 'none'}\n"
        )
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode())


def main():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=run_import, daemon=True).start()
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
