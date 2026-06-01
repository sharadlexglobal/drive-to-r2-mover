#!/usr/bin/env python3
"""Apollo orgs (6M companies, TSV) → filter India → Supabase."""
import csv, http.server, io, os, socketserver, sys, threading, time, traceback, zipfile
import boto3, psycopg2
from botocore.config import Config

PORT = int(os.environ.get("PORT", "10000"))
R2_BUCKET = os.environ.get("R2_BUCKET", "drive-archive-2026")
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_ACCESS = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET = os.environ["R2_SECRET_ACCESS_KEY"]
PG = dict(
    host=os.environ["PG_HOST"], port=int(os.environ.get("PG_PORT","5432")),
    dbname=os.environ.get("PG_DB","postgres"),
    user=os.environ["PG_USER"], password=os.environ["PG_PASSWORD"], sslmode="require",
)

ZIP_KEY = "Entire Apollo Database 99,311,285.zip"
CSV_PATH = "Entire Apollo Database 99,311,285/Apollo_V7_V5_org_all_fields 6,071,657.csv"
TABLE = "apollo_india_orgs"
CHUNK_ROWS = 50_000
FILTER_COUNTRY = "India"

COLUMN_MAP = {
    "organization_id": "organization_id",
    "organization_name": "organization_name",
    "organization_revenue_in_thousands_int": "revenue_in_thousands",
    "organization_retail_location_count": "retail_location_count",
    "organization_public_symbol": "public_symbol",
    "organization_founded_year": "founded_year",
    "organization_alexa_ranking": "alexa_ranking",
    "organization_num_current_employees": "num_current_employees",
    "organization_relevant_keywords_str": "relevant_keywords",
    "organization_industries": "industries",
    "organization_linkedin_specialties": "linkedin_specialties",
    "organization_angellist_markets": "angellist_markets",
    "organization_yelp_categories": "yelp_categories",
    "organization_keywords": "keywords",
    "organization_short_description": "short_description",
    "organization_seo_description": "seo_description",
    "organization_website_url": "website_url",
    "organization_angellist_url": "angellist_url",
    "organization_facebook_url": "facebook_url",
    "organization_twitter_url": "twitter_url",
    "organization_languages": "languages",
    "organization_domain": "domain",
    "organization_phone": "phone",
    "organization_current_technologies": "current_technologies",
    "organization_num_linkedin_followers": "num_linkedin_followers",
    "job_functions": "job_functions",
    "organization_hq_location_city": "hq_city",
    "organization_hq_location_state": "hq_state",
    "organization_hq_location_country": "hq_country",
    "organization_hq_location_postal_code": "hq_postal_code",
    "organization_total_funding_long": "total_funding",
    "organization_latest_funding_stage_cd": "latest_funding_stage",
    "organization_latest_funding_round_amount_long": "latest_funding_round_amount",
    "organization_latest_funding_round_date": "latest_funding_round_date",
}
DEST_COLS = list(COLUMN_MAP.values())

csv.field_size_limit(sys.maxsize)
s3 = boto3.client("s3", endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS, aws_secret_access_key=R2_SECRET,
    config=Config(signature_version="s3v4", region_name="auto"))

state = {"status":"starting","started_at":time.time(),"rows_scanned":0,"rows_loaded":0,
         "rows_skipped":0,"avg_rate":0,"last_error":None}


class S3RangeReader(io.RawIOBase):
    def __init__(self, b, k):
        h = s3.head_object(Bucket=b, Key=k)
        self.size, self.bucket, self.key, self.pos = h["ContentLength"], b, k, 0
    def readable(self): return True
    def seekable(self): return True
    def seek(self,o,w=0):
        self.pos = o if w==0 else (self.pos+o if w==1 else self.size+o); return self.pos
    def tell(self): return self.pos
    def read(self,n=-1):
        if n is None or n < 0 or self.pos+n > self.size: n = self.size-self.pos
        if n == 0: return b""
        r = s3.get_object(Bucket=self.bucket, Key=self.key, Range=f"bytes={self.pos}-{self.pos+n-1}")
        d = r["Body"].read(); self.pos += len(d); return d
    def readinto(self, b):
        d = self.read(len(b)); n = len(d); b[:n] = d; return n


def esc(v):
    if not v: return r"\N"
    return v.replace("\\","\\\\").replace("\t","\\t").replace("\n","\\n").replace("\r","\\r")


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
                c.close(); time.sleep(0.3+0.05*i); continue
            return c, cu
        except Exception as e:
            print(f"conn {i}: {e}", flush=True); time.sleep(0.5)
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
        src_to_dest = {}
        for src_idx, src_name in enumerate(header):
            d = COLUMN_MAP.get(src_name)
            if d: src_to_dest[src_idx] = DEST_COLS.index(d)
        country_idx = header.index("organization_hq_location_country")
        N_DEST = len(DEST_COLS)

        conn, cur = get_writable_conn()
        cols_sql = ", ".join(DEST_COLS)
        copy_sql = f"COPY {TABLE}({cols_sql}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '\\N')"

        state["status"] = "streaming"
        t_start = time.time()
        chunk_buf = io.StringIO()
        chunk_n = 0

        for row in reader:
            state["rows_scanned"] += 1
            if len(row) <= country_idx or row[country_idx] != FILTER_COUNTRY:
                continue
            out = [""] * N_DEST
            for src_idx, dest_idx in src_to_dest.items():
                if src_idx < len(row): out[dest_idx] = row[src_idx]
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
                        try: conn.close()
                        except: pass
                        conn, cur = get_writable_conn()
                if ok:
                    state["rows_loaded"] += chunk_n
                    state["avg_rate"] = state["rows_loaded"] / max(0.01, time.time()-t_start)
                    print(f"+{chunk_n} loaded | scanned {state['rows_scanned']:,} | "
                          f"total {state['rows_loaded']:,} | avg {state['avg_rate']:,.0f}/s", flush=True)
                else:
                    state["rows_skipped"] += chunk_n
                chunk_buf = io.StringIO()
                chunk_n = 0
        if chunk_n > 0:
            try:
                cur.copy_expert(copy_sql, io.StringIO(chunk_buf.getvalue()))
                conn.commit()
                state["rows_loaded"] += chunk_n
            except Exception:
                state["rows_skipped"] += chunk_n
        conn.close()
        state["status"] = f"done in {time.time()-t_start:.0f}s: {state['rows_loaded']:,} India orgs (scanned {state['rows_scanned']:,})"
        print(state["status"], flush=True)
    except Exception as e:
        state["status"] = f"FATAL: {type(e).__name__}: {e}"
        state["last_error"] = traceback.format_exc()[-1500:]
        print(state["status"], flush=True)


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*a,**k): pass
    def do_GET(self):
        body = (f"status:        {state['status']}\n"
                f"elapsed:       {int(time.time()-state['started_at'])}s\n"
                f"rows_scanned:  {state['rows_scanned']:,}\n"
                f"rows_loaded:   {state['rows_loaded']:,}\n"
                f"rows_skipped:  {state['rows_skipped']:,}\n"
                f"avg_rate:      {state['avg_rate']:,.0f}/s\n"
                f"last_error:    {state['last_error'] or 'none'}\n")
        self.send_response(200); self.send_header("Content-Type","text/plain; charset=utf-8")
        self.end_headers(); self.wfile.write(body.encode())


def main():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=run_import, daemon=True).start()
    httpd.serve_forever()


if __name__ == "__main__":
    main()
