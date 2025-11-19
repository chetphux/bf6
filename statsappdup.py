from flask import Flask, jsonify, request, send_from_directory
import sqlite3
from pathlib import Path

# Paths
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "stats.sqlite3"
WEB_DIR = BASE_DIR / "web"  # Put stats.html and any assets here

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="")

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# --- ROUTES ---

@app.get("/")
def index():
    """Serve the main stats page."""
    return send_from_directory(WEB_DIR, "stats.html")


@app.get("/api/players")
def api_players():
    """List of players."""
    print(DB_PATH)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, name FROM player ORDER BY name"
        ).fetchall()
        return jsonify([dict(r) for r in rows])

from flask import Flask, jsonify, request, send_from_directory
import sqlite3
from functools import lru_cache
from pathlib import Path
import re

# Paths
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "stats.sqlite3"
WEB_DIR = BASE_DIR / "web"  # Put stats.html and any assets here

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="")

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    # enable window functions (SQLite 3.25+ — most modern builds have this)
    return conn

# --- helpers ---

NUMERIC_TYPE_RE = re.compile(r"(INT|REAL|NUM|DEC|FLOA|DOUB)", re.I)

@lru_cache(maxsize=1)
def snapshot_numeric_columns():
    """
    Inspect the schema and return numeric columns to delta.
    Excludes identifiers and timestamp.
    """
    with db() as conn:
        rows = conn.execute("PRAGMA table_info(snapshot);").fetchall()
    cols = []
    for r in rows:
        name = r["name"]
        ctype = (r["type"] or "").upper()
        if name in ("id", "player_id", "ts"):
            continue
        # treat as numeric if declared numeric OR looks like a counter by name,
        # because some schemas use empty type in SQLite.
        if NUMERIC_TYPE_RE.search(ctype) or name.endswith("_gm_granitebr"):
            cols.append(name)
    return cols

def delta_sql(col: str, clamp: bool) -> str:
    """
    Build SQL for delta of one column, safe for NULLs and first row.
    First row delta -> 0.
    """
    # current and previous guarded for NULLs
    curr = f"COALESCE(s.{col}, 0)"
    prev = f"COALESCE(LAG(s.{col}) OVER (PARTITION BY s.player_id ORDER BY s.ts), {curr})"
    expr = f"({curr} - {prev})"
    if clamp:
        expr = f"MAX({expr}, 0)"
    return f"{expr} AS delta_{col}"

# --- ROUTES ---

@app.get("/")
def index():
    return send_from_directory(WEB_DIR, "stats.html")

@app.get("/api/players")
def api_players():
    with db() as conn:
        rows = conn.execute("SELECT id, name FROM player ORDER BY name").fetchall()
        return jsonify([dict(r) for r in rows])

@app.get("/api/snapshots")
def api_snapshots():
    player = request.args.get("player")
    limit = int(request.args.get("limit", "500"))
    since = request.args.get("from")
    until = request.args.get("to")
    order = request.args.get("order", "desc").lower()
    with_deltas = request.args.get("with_deltas", "0").lower() in ("1", "true", "yes")
    clamp = request.args.get("clamp", "0").lower() in ("1", "true", "yes")

    order_sql = "DESC" if order != "asc" else "ASC"

    select_cols = [
        "s.id",
        "s.player_id",
        "p.name AS player_name",
        "s.ts AS timestamp",
        "s.*",
    ]

    if with_deltas:
        cols = snapshot_numeric_columns()
        print("with_deltas=1 -> adding", cols)
        for col in cols:
            select_cols.append(delta_sql(col, clamp))

    sql = [
        "SELECT",
        ", ".join(select_cols),
        "FROM snapshot s",
        "JOIN player p ON p.id = s.player_id",
        "WHERE 1=1",
    ]
    params = []

    # 🚫 Hide Brett here (case-insensitive)
    sql.append("AND p.name <> ? COLLATE NOCASE")
    params.append("brett")

    # 👇 Rest of your filters
    if player:
        sql.append("AND p.name = ?")
        params.append(player)
    if since:
        sql.append("AND s.ts >= ?")
        params.append(since)
    if until:
        sql.append("AND s.ts < ?")
        params.append(until)

    sql.append(f"ORDER BY s.ts {order_sql}")
    sql.append("LIMIT ?")
    params.append(limit)

    with db() as conn:
        rows = conn.execute("\n".join(sql), params).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d.pop("ts", None)
            out.append(d)
        return jsonify(out)

@app.get("/api/last")
def api_last_per_player():
    """Latest snapshot per player."""
    with db() as conn:
        rows = conn.execute("""
            SELECT s.*
            FROM snapshot s
            JOIN (
                SELECT player_id, MAX(ts) AS max_ts
                FROM snapshot
                GROUP BY player_id
            ) x ON x.player_id = s.player_id AND x.max_ts = s.ts
        """).fetchall()
        return jsonify([dict(r) for r in rows])


if __name__ == "__main__":
    WEB_DIR.mkdir(exist_ok=True)
    app.run(host="0.0.0.0", port=8080, debug=False)
