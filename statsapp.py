from flask import Flask, jsonify, request, send_from_directory, redirect
import sqlite3
from contextlib import contextmanager
from functools import lru_cache
from pathlib import Path
import re
from datetime import datetime, timezone, timedelta

# Paths
BASE_DIR = Path(__file__).resolve().parent
DB_PATH = BASE_DIR / "stats.sqlite3"
WEB_DIR = BASE_DIR / "web"  # Put stats.html and any assets here
DEFAULT_SNAPSHOT_LIMIT = 100
MAX_SNAPSHOT_LIMIT = 2000

app = Flask(__name__, static_folder=str(WEB_DIR), static_url_path="")

@app.after_request
def log_response_size(response):
    size = response.calculate_content_length()
    if request.path.startswith("/api"):
        print(f"{request.path} -> {size} bytes")
    return response

BLACKLISTED_IPS = {
    "108.90.110.51",    # Dallas, Texas | Big Phil
}

@app.post("/api/trigger_refresh")
def api_trigger_refresh():
    from datetime import datetime

    with db() as conn:
        row = conn.execute(
            "SELECT updated_at FROM app_state WHERE key='force_refresh'"
        ).fetchone()

        if row:
            # SQLite's datetime('now') comes back like "YYYY-MM-DD HH:MM:SS"
            last_time = datetime.fromisoformat(row["updated_at"])
            delta = datetime.utcnow() - last_time
            if delta.total_seconds() < 30:
                return jsonify({"ok": False, "message": "Please wait before refreshing again."}), 429

        conn.execute("""
            INSERT INTO app_state (key, value, updated_at)
            VALUES ('force_refresh', '1', datetime('now'))
            ON CONFLICT(key) DO UPDATE SET value='1', updated_at=datetime('now')
        """)
        conn.commit()   # <-- important

    return jsonify({"ok": True, "message": "Refresh requested"})

def _get_state(key: str):
    with db() as conn:
        row = conn.execute("SELECT value FROM app_state WHERE key = ?", (key,)).fetchone()
        return None if not row else row["value"]

def read_timer_minutes(default=None):
    raw = _get_state("timer_minutes")
    if raw is None:
        return default
    try:
        return int(json.loads(raw))          # JSON int
    except Exception:
        try: return int(raw)                 # plain text fallback
        except Exception: return default

def read_next_tick_iso():
    raw = _get_state("next_tick_at")
    if raw is None:
        return None
    # Handle both JSON-quoted and plain text
    try:
        val = json.loads(raw)
        if isinstance(val, str):
            return val
    except Exception:
        pass
    return raw

def seconds_until_next_tick(now=None):
    iso = read_next_tick_iso()
    if not iso:
        return None
    try:
        iso_norm = str(iso).strip().replace("Z", "+00:00")
        dt = datetime.fromisoformat(iso_norm)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None
    now = now or datetime.now(timezone.utc)
    return max(0, int((dt - now).total_seconds()))


@app.get("/api/timer")
def api_timer():
    import json
    from datetime import datetime, timezone

    # read raw values directly
    with db() as conn:
        row_next = conn.execute(
            "SELECT value FROM app_state WHERE key = 'next_tick_at'"
        ).fetchone()
        row_min = conn.execute(
            "SELECT value FROM app_state WHERE key = 'timer_minutes'"
        ).fetchone()

    raw_iso = row_next["value"] if row_next else None
    raw_min = row_min["value"] if row_min else None

    # unquote both if they were JSON-encoded
    def dejson(x):
        if x is None:
            return None
        try:
            return json.loads(x)
        except Exception:
            return x

    iso_str = dejson(raw_iso)          # -> plain ISO string
    timer_minutes = None
    try:
        timer_minutes = int(dejson(raw_min))
    except Exception:
        pass

    # compute seconds remaining
    seconds_remaining = None
    if iso_str:
        try:
            iso_norm = str(iso_str).strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(iso_norm)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            seconds_remaining = max(0, int((dt - now).total_seconds()))
        except Exception as e:
            print(f"[api_timer] parse error for next_tick_at={iso_str!r}: {e}")

    return jsonify({
        "timer_minutes": timer_minutes,
        "next_tick_at": iso_str,
        "seconds_remaining": seconds_remaining
    })


@app.before_request
def log_ip():
    ip = request.headers.get("X-Forwarded-For", request.remote_addr)
    print(f"[{datetime.now().isoformat()}] Visit from {ip} -> {request.path}")
    raw_ip = ip.split(",")[0].strip() if ip else None    
    if raw_ip in BLACKLISTED_IPS:
        print(f'{raw_ip} redirected!')
        return redirect("https://www.youtube.com/watch?v=Eo-KmOd3i7s&list=RDEo-KmOd3i7s&start_radio=1", code=301)

@contextmanager
def db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()



@app.get("/api/players")
def api_players():
    """List of players."""
    print(DB_PATH)
    with db() as conn:
        rows = conn.execute(
            "SELECT id, name FROM player ORDER BY name"
        ).fetchall()
        return jsonify([dict(r) for r in rows])

# --- helpers ---

NUMERIC_TYPE_RE = re.compile(r"(INT|REAL|NUM|DEC|FLOA|DOUB)", re.I)

BASE_SNAPSHOT_COLS = [
    "s.id",
    "s.player_id",
    "p.name AS player_name",
    "s.ts AS timestamp",
    "s.kills_gm_granitebr",
    "s.deaths_gm_granitebr",
    "s.assists_gm_granitebr",
    "s.dmg_gm_granitebr",
    "s.wins_gm_granitebr",
    "s.tp_gm_granitebr",
    "s.scorein_gm_granitebr",
    "s.revives_gm_granitebr",
    "s.spot_gm_granitebr",
]

@lru_cache(maxsize=1)
def snapshot_numeric_columns():
    """
    Only the numeric snapshot columns that the frontend actually uses.
    """
    return [
        "kills_gm_granitebr",
        "deaths_gm_granitebr",
        "assists_gm_granitebr",
        "dmg_gm_granitebr",
        "wins_gm_granitebr",
        "tp_gm_granitebr",
    	"scorein_gm_granitebr",
        "revives_gm_granitebr",
        "spot_gm_granitebr"
    ]

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

@app.get("/api/snapshots")
def api_snapshots():
    player = request.args.get("player")
    paged = request.args.get("paged", "0").lower() in ("1", "true", "yes")
    cursor_ts = request.args.get("cursor_ts")
    cursor_id = None
    cursor_id_raw = request.args.get("cursor_id")
    if cursor_id_raw is not None:
        try:
            cursor_id = int(cursor_id_raw)
        except ValueError:
            cursor_id = None

    try:
        limit = int(request.args.get("limit", str(DEFAULT_SNAPSHOT_LIMIT)))
    except ValueError:
        limit = DEFAULT_SNAPSHOT_LIMIT
    limit = max(1, min(limit, MAX_SNAPSHOT_LIMIT))

    since = request.args.get("from")
    until = request.args.get("to")
    order = request.args.get("order", "desc").lower()
    with_deltas = request.args.get("with_deltas", "0").lower() in ("1", "true", "yes")
    clamp = request.args.get("clamp", "0").lower() in ("1", "true", "yes")

    order_sql = "DESC" if order != "asc" else "ASC"

    select_cols = list(BASE_SNAPSHOT_COLS)

    if with_deltas:
        cols = snapshot_numeric_columns()
        print("with_deltas=1 -> adding deltas for", cols)
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

    if player:
        sql.append("AND p.name = ?")
        params.append(player)
    if since:
        sql.append("AND s.ts >= ?")
        params.append(since)
    if until:
        sql.append("AND s.ts < ?")
        params.append(until)
    if cursor_ts:
        if cursor_id is not None:
            if order_sql == "DESC":
                sql.append("AND (s.ts < ? OR (s.ts = ? AND s.id < ?))")
            else:
                sql.append("AND (s.ts > ? OR (s.ts = ? AND s.id > ?))")
            params.extend([cursor_ts, cursor_ts, cursor_id])
        else:
            sql.append("AND s.ts < ?" if order_sql == "DESC" else "AND s.ts > ?")
            params.append(cursor_ts)

    sql.append(f"ORDER BY s.ts {order_sql}, s.id {order_sql}")
    sql.append("LIMIT ?")
    query_limit = limit + 1 if paged else limit
    params.append(query_limit)

    with db() as conn:
        rows = conn.execute("\n".join(sql), params).fetchall()
        page_rows = rows[:limit] if paged else rows
        out = []
        for r in page_rows:
            d = dict(r)
            # We already expose "timestamp" via SELECT; drop raw ts if present
            d.pop("ts", None)
            out.append(d)
        if not paged:
            return jsonify(out)

        has_more = len(rows) > limit
        next_cursor = None
        if has_more and page_rows:
            tail = page_rows[-1]
            next_cursor = {
                "ts": tail["timestamp"],
                "id": tail["id"],
            }
        return jsonify({
            "items": out,
            "has_more": has_more,
            "next_cursor": next_cursor,
        })


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


@app.get("/api/overall")
def api_overall():
    """
    Overall profile totals from each player's latest snapshot.
    Optional query param:
      - player: exact player name
    """
    player = request.args.get("player")

    sql = """
    WITH base AS (
        SELECT
            s.id,
            s.player_id,
            p.name AS player_name,
            s.ts,
            COALESCE(s.kills_gm_granitebr, 0) AS kills,
            COALESCE(s.deaths_gm_granitebr, 0) AS deaths,
            COALESCE(s.assists_gm_granitebr, 0) AS assists,
            COALESCE(s.dmg_gm_granitebr, 0) AS dmg,
            COALESCE(s.wins_gm_granitebr, 0) AS wins,
            COALESCE(s.intel_pickup_gm_granitebr, 0) AS intel_pickups,
            COALESCE(s.vehd_gm_granitebr, 0) AS vehicles_destroyed,
            COALESCE(s.tp_gm_granitebr, 0) AS tp,
            COALESCE(s.scorein_gm_granitebr, 0) AS score,
            COALESCE(s.revives_gm_granitebr, 0) AS revives,
            COALESCE(s.spot_gm_granitebr, 0) AS spots,
            ROW_NUMBER() OVER (
                PARTITION BY s.player_id
                ORDER BY s.ts DESC, s.id DESC
            ) AS rn_latest
        FROM snapshot s
        JOIN player p ON p.id = s.player_id
        WHERE (? IS NULL OR p.name = ?)
    )
    SELECT
        player_id,
        player_name,
        COUNT(*) AS snapshots,
        COUNT(*) AS matches_tracked,
        MIN(ts) AS first_seen,
        MAX(ts) AS last_seen,
        MAX(CASE WHEN rn_latest = 1 THEN kills END) AS overall_kills,
        MAX(CASE WHEN rn_latest = 1 THEN deaths END) AS overall_deaths,
        MAX(CASE WHEN rn_latest = 1 THEN assists END) AS overall_assists,
        MAX(CASE WHEN rn_latest = 1 THEN dmg END) AS overall_damage,
        MAX(CASE WHEN rn_latest = 1 THEN wins END) AS overall_wins,
        MAX(CASE WHEN rn_latest = 1 THEN intel_pickups END) AS overall_intel_picked_up,
        MAX(CASE WHEN rn_latest = 1 THEN vehicles_destroyed END) AS overall_vehicles_destroyed,
        MAX(CASE WHEN rn_latest = 1 THEN tp END) AS overall_time_played,
        MAX(CASE WHEN rn_latest = 1 THEN score END) AS overall_score,
        MAX(CASE WHEN rn_latest = 1 THEN revives END) AS overall_revives,
        MAX(CASE WHEN rn_latest = 1 THEN spots END) AS overall_spots
    FROM base
    GROUP BY player_id, player_name
    ORDER BY player_name ASC
    """

    with db() as conn:
        rows = conn.execute(sql, (player, player)).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            deaths = max(1, int(d.get("overall_deaths") or 0))
            kills = int(d.get("overall_kills") or 0)
            assists = int(d.get("overall_assists") or 0)
            wins = int(d.get("overall_wins") or 0)
            matches = int(d.get("matches_tracked") or 0)
            d["overall_kd"] = round(kills / deaths, 2)
            d["overall_kda"] = round((kills + assists) / deaths, 2)
            d["win_rate"] = round((wins / matches) * 100, 2) if matches > 0 else 0.0
            out.append(d)
        return jsonify(out)


if __name__ == "__main__":
    WEB_DIR.mkdir(exist_ok=True)
    app.run(host="0.0.0.0", port=8080, debug=False)
