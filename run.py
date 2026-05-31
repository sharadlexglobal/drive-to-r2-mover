#!/usr/bin/env python3
import http.server
import os
import socketserver
import subprocess
import threading
import time
from pathlib import Path

PORT = int(os.environ.get("PORT", "10000"))
RCLONE_BIN = "/tmp/rclone"
RCLONE_CONF = "/tmp/rclone.conf"
LOG_FILE = "/tmp/rclone.log"
SENTINEL = "/tmp/done"

DRIVE_FOLDER_ID = os.environ["DRIVE_FOLDER_ID"]
GDRIVE_TOKEN_JSON = os.environ["GDRIVE_TOKEN_JSON"]
R2_ACCESS_KEY = os.environ["R2_ACCESS_KEY_ID"]
R2_SECRET = os.environ["R2_SECRET_ACCESS_KEY"]
R2_ENDPOINT = os.environ["R2_ENDPOINT"]
R2_BUCKET = os.environ.get("R2_BUCKET", "drive-archive-2026")

state = {"status": "starting", "started_at": time.time(), "stats": "", "exit_code": None}


def write_conf():
    Path(RCLONE_CONF).write_text(f"""[gdrive]
type = drive
scope = drive.readonly
token = {GDRIVE_TOKEN_JSON}

[r2]
type = s3
provider = Cloudflare
access_key_id = {R2_ACCESS_KEY}
secret_access_key = {R2_SECRET}
endpoint = {R2_ENDPOINT}
acl = private
""")


def install_rclone():
    if os.path.exists(RCLONE_BIN):
        return
    subprocess.run(
        "cd /tmp && curl -fsSL -o rclone.zip https://downloads.rclone.org/rclone-current-linux-amd64.zip "
        "&& unzip -j rclone.zip '*/rclone' -d /tmp && chmod +x /tmp/rclone",
        shell=True, check=True,
    )


def stats_tailer():
    last_size = 0
    while not os.path.exists(SENTINEL):
        try:
            if os.path.exists(LOG_FILE):
                with open(LOG_FILE) as f:
                    f.seek(last_size)
                    new = f.read()
                    last_size = f.tell()
                    for line in new.splitlines():
                        if "Transferred:" in line and "ETA" in line:
                            state["stats"] = line.strip()
        except Exception:
            pass
        time.sleep(5)


def run_transfer():
    state["status"] = "installing rclone"
    install_rclone()
    write_conf()
    state["status"] = "transferring"
    threading.Thread(target=stats_tailer, daemon=True).start()
    cmd = [
        RCLONE_BIN, "copy",
        "gdrive:", f"r2:{R2_BUCKET}/",
        "--config", RCLONE_CONF,
        "--drive-root-folder-id", DRIVE_FOLDER_ID,
        "--transfers", "4",
        "--checkers", "8",
        "--drive-acknowledge-abuse",
        "--s3-upload-concurrency", "4",
        "--s3-chunk-size", "32M",
        "--fast-list",
        "--stats", "20s",
        "--stats-one-line",
        "--log-file", LOG_FILE,
        "--log-level", "INFO",
    ]
    with open(LOG_FILE, "w") as logf:
        logf.write(f"START {time.ctime()}\nCMD: {' '.join(cmd)}\n\n")
    proc = subprocess.Popen(cmd)
    proc.wait()
    state["exit_code"] = proc.returncode
    Path(SENTINEL).write_text(f"exit={proc.returncode} at {time.ctime()}\n")
    state["status"] = "done" if proc.returncode == 0 else f"failed (exit {proc.returncode})"


class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *a, **k):
        pass

    def do_GET(self):
        elapsed = int(time.time() - state["started_at"])
        body = (
            f"status: {state['status']}\n"
            f"elapsed: {elapsed}s\n"
            f"exit_code: {state['exit_code']}\n"
            f"last_stats: {state['stats']}\n"
            f"\n--- last 50 log lines ---\n"
        )
        try:
            with open(LOG_FILE) as f:
                body += "".join(f.readlines()[-50:])
        except FileNotFoundError:
            body += "(no log yet)\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode())


def main():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    httpd = socketserver.ThreadingTCPServer(("0.0.0.0", PORT), Handler)
    threading.Thread(target=run_transfer, daemon=True).start()
    try:
        httpd.serve_forever()
    finally:
        httpd.server_close()


if __name__ == "__main__":
    main()
