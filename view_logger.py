# /opt/stats-log-view/app.py
from flask import Flask, Response, request
import subprocess, json, html, time, datetime

app = Flask(__name__)

@app.route("/")
def index():
    since = request.args.get("since", "6 hours ago")
    cmd = ["journalctl","-u","stats","--since",since,"-g","snapshots","-o","json"]
    out = subprocess.run(cmd, capture_output=True, text=True, check=False).stdout
    rows = []
    for line in out.splitlines():
        if not line.strip(): continue
        j = json.loads(line)
        ts_us = int(j.get("__REALTIME_TIMESTAMP","0"))
        ts = datetime.datetime.utcfromtimestamp(ts_us/1_000_000).isoformat()+"Z" if ts_us else ""
        msg = html.escape(j.get("MESSAGE",""))
        rows.append((ts, msg, j.get("_PID",""), j.get("_HOSTNAME","")))
    rows.sort(reverse=True)  # newest first

    html_page = [
      "<!doctype html><meta charset='utf-8'><title>Stats snapshots</title>",
      "<style>body{font:14px system-ui;margin:24px} table{border-collapse:collapse;width:100%} th,td{border:1px solid #ddd;padding:6px 8px} th{background:#f6f6f6} tr:nth-child(even){background:#fafafa} code{white-space:pre-wrap}</style>",
      "<h1>Stats snapshots</h1>",
      f"<div class='meta'>Window: {html.escape(since)} — generated {datetime.datetime.utcnow().isoformat()}Z</div>",
      "<table><thead><tr><th>Time (UTC)</th><th>Message</th><th>PID</th><th>Host</th></tr></thead><tbody>"
    ]
    for ts,msg,pid,host in rows:
        html_page.append(f"<tr><td>{ts}</td><td><code>{msg}</code></td><td>{pid}</td><td>{host}</td></tr>")
    html_page.append("</tbody></table>")
    return Response("\n".join(html_page), mimetype="text/html")

