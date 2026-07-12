"""
LAB42 Chart Dashboard — TEMPERATURE ONLY, DISK-CACHED build.

Graph-based prototype (line charts) — companion to the floor-plan map server.
Reads all data from the shared cache (lab42_temp_cache.sqlite) built by
lab42_cache_loader.py, so it starts instantly and never fetches from the BMS.
Each on-demand chart bin is derived from the cached 6-hour temperature values
(avg = mean of those points, max/min = their extremes), the same way the map
server derives its zoom tiers — so the two prototypes stay consistent.

Run: python graphstemp_cached.py            (port 8083)
First build the cache: python lab42_cache_loader.py
"""
import threading, uvicorn, requests, sqlite3, json, pathlib
from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, Response

API_URL       = "http://leffe.science.uva.nl:8044"   # single API: auth, rooms, binned data
DATA_START_MS = 1658016000000
DATA_END_MS   = 1781049600000
WEEK_MS       = 7 * 24 * 3600 * 1000
SIXH_MS       = 6 * 3600 * 1000

# CACHE_ONLY: read everything from lab42_temp_cache.sqlite, never touch the BMS
# for data. The cache also stores the room list, so the server can run fully
# offline. Set False to fall back to the BMS (auth + live fetch).
CACHE_PATH = pathlib.Path(__file__).parent / "lab42_temp_cache.sqlite"
CACHE_ONLY = True

# :8044 login — only used if CACHE_ONLY is False or the cache lacks the room list.
LOGIN = {"username": "marnix", "password": "marnixq1w2e3r4"}   # TODO: :8044 creds

# Temperature-only build: this variant fetches and shows ONLY temperature, so
# there is no sensor switching and only the temperature series is ever requested.
SENSORS = [
    {"key":"temperature","name":"Temperature","unit":"°C"},
]

# Shared HTTP session for connection reuse (keep-alive)
session = requests.Session()

# ── Auth (only used as a fallback when the cache lacks the room list) ───────────
def get_token():
    r = session.post(f"{API_URL}/auth/login", json=LOGIN, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]

TOKEN = None
HEADERS = {}
token_lock = threading.Lock()

def refresh_token():
    global TOKEN, HEADERS
    with token_lock:
        TOKEN = get_token()
        HEADERS = {"Authorization": f"Bearer {TOKEN}"}

# ── Q6H GRID + DISK CACHE ──────────────────────────────────────────────────────
# Same 6-hour grid the loader/map server use. Cells are keyed by ci, so the
# indices line up across all three programs as long as DATA_START/END match.
def _build_q6h_starts():
    step = SIXH_MS
    start = (DATA_START_MS // step) * step
    starts, t = [], start
    while t < DATA_END_MS:
        cs = max(t, DATA_START_MS)
        if min(t + step, DATA_END_MS) > cs:
            starts.append(cs)
        t += step
    return starts

Q6H_STARTS = _build_q6h_starts()
N_Q6H = len(Q6H_STARTS)
_q6h: dict = {}        # room_id -> {ci: temp}


def _load_cache():
    """Load cached cells (and the room list) from lab42_temp_cache.sqlite."""
    if not CACHE_PATH.exists():
        return None
    conn = sqlite3.connect(CACHE_PATH, check_same_thread=False)
    grid = conn.execute("SELECT v FROM meta WHERE k='grid'").fetchone()
    want = f"{DATA_START_MS}:{DATA_END_MS}"
    if grid and grid[0] != want:
        print(f"  !! cache grid {grid[0]} != {want}; ignoring stale cache")
        conn.close()
        return None
    n = 0
    for rid, ci, temp in conn.execute("SELECT room_id, ci, temp FROM cells"):
        _q6h.setdefault(rid, {})[ci] = temp
        n += 1
    rj = conn.execute("SELECT v FROM meta WHERE k='rooms_json'").fetchone()
    conn.close()
    print(f"  cache: {n} cells across {len(_q6h)} rooms")
    return json.loads(rj[0]) if rj else None


print("Loading disk cache...")
_rooms_raw = _load_cache()

if _rooms_raw is None:
    if CACHE_ONLY:
        print("  !! No usable cache and CACHE_ONLY=True.")
        print("  !! Run:  python lab42_cache_loader.py   to build it first.")
    print("Authenticating (BMS fallback for room list)...")
    refresh_token()
    _rooms_raw = session.get(f"{API_URL}/rooms", headers=HEADERS, timeout=15).json()

# ── Shared room set: keep only rooms the MAP can also display ───────────────────
# The map can only show rooms that have hand-placed coordinates. To keep BOTH
# programs on an identical room set for the study, filter the chart's rooms to the
# same coordinate whitelist, matching room numbers the way the map does.
import re as _re2
_COORD_KEYS = {
  0: {'01', '03', '05', '06', '09', '10', '11', '12', '13', '16'},
  1: {'01', '02', '04', '05', '07', '08', '10', '11', '12', '13', '14', '15', '16', '17'},
  2: {'02', '03', '04', '05', '06', '07', '08', '08b', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '22', '95a'},
  3: {'02a', '02b', '03', '04', '05', '06', '07', '08', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '23', '24', '25', '27', '32', '33', '35', '36', '37', '38'},
  4: {'02', '03', '04', '05', '06', '07', '08', '10', '11', '12', '13', '14', '15', '16', '17', '19', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '42', '43', '44', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '56', '57', '58', '59', '60', '61', '62', '63', '64'},
  5: {'02', '03', '04', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '40', '41', '42', '45', '46', '47', '48', '49', '50', '51', '52', '53', '54', '56', '57', '58', '59', '60', '61', '62', '63', '64'},
  6: {'02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12', '13', '14', '15', '16', '17', '18', '20', '21', '22', '23', '24', '25', '26', '27', '28', '29', '30', '31', '32', '33', '34', '35', '36', '37', '38', '39', '41', '42', '43', '44', '46', '47', '48', '49', '50', '51', '52', '53', '54', '55', '56', '57', '58', '59', '60', '61'},
}
def _sk(raw):
    n = raw.strip()
    no_prefix = _re2.sub(r'^[Ll]?\d+[.\-_ ]', '', n).strip()
    core = _re2.split(r'\s', no_prefix)[0] if no_prefix else no_prefix
    cands = []
    for base in (no_prefix, core):
        base = base.strip()
        if not base:
            continue
        nz = base.lstrip('0') or '0'
        cands += [base, base.lower(), nz, nz.lower(), base.zfill(2)]
        m = _re2.match(r'(\d+)', base)
        if m:
            num = m.group(1); num_nz = num.lstrip('0') or '0'
            cands += [num, num_nz, num.zfill(2)]
    return list(dict.fromkeys(cands))
def _floor_of(raw):
    m = _re2.match(r'^[Ll]?(\d+)[.\-_ ]', raw.strip())
    return int(m.group(1)) if m else None
def _has_coord(number, floor):
    bfloor = _floor_of(str(number))
    if bfloor is None:
        try: bfloor = int(floor)
        except Exception: return False
    keys = _COORD_KEYS.get(bfloor)
    if not keys: return False
    ci = {k.lower(): k for k in keys}
    for c in _sk(str(number)):
        if c in keys or c.lower() in ci:
            return True
    return False

# Group by floor (0-6) AND require a matching map coordinate.
FLOORS_BY_NUM: dict = defaultdict(list)
_dropped_no_coord = []
for room in _rooms_raw:
    try:
        fl = int(room["floor"])
    except (ValueError, TypeError):
        continue
    if not (0 <= fl <= 6):
        continue
    if _has_coord(room.get("number"), fl):
        FLOORS_BY_NUM[fl].append(room)
    else:
        _dropped_no_coord.append(room)

FLOOR_NUMS = sorted(FLOORS_BY_NUM.keys())
ALL_ROOM_IDS = [r["id"] for fl in FLOOR_NUMS for r in FLOORS_BY_NUM[fl]]
# Reverse lookup so max/min series can report which room a peak/trough came from.
ROOM_INFO_BY_ID = {r["id"]: {"floor": fl, "number": r["number"]}
                   for fl in FLOOR_NUMS for r in FLOORS_BY_NUM[fl]}
_kept = sum(len(v) for v in FLOORS_BY_NUM.values())
print(f"  {_kept} rooms kept across floors {FLOOR_NUMS} (filtered to match the map)")
if _dropped_no_coord:
    print(f"  Excluded {len(_dropped_no_coord)} chart room(s) with no map coordinate:")
    for r in sorted(_dropped_no_coord, key=lambda r: (str(r.get('floor')), str(r.get('number')))):
        print(f"    floor {r.get('floor')}  room {r.get('number')}  (id {r.get('id')})")

# ── Week timeline ──────────────────────────────────────────────────────────────
WEEK_STARTS: list[int] = []
t = DATA_START_MS
while t < DATA_END_MS:
    WEEK_STARTS.append(t)
    t += WEEK_MS
NW = len(WEEK_STARTS)
WEEK_LABELS = [datetime.fromtimestamp(w/1000, tz=timezone.utc).strftime("%b '%y")
               for w in WEEK_STARTS]

# ── Binned-data fetch (:8044) ───────────────────────────────────────────────────
# The /rooms/{id}/data endpoint accepts startTime / endTime / bins and returns
# one row per bin, each carrying <sensor>, <sensor>_min, <sensor>_max,
# <sensor>_count. So we fetch exactly the points we plot for the selected
# period, server-aggregated, in a single request — no full-history pull, no
# Python re-binning.

# cache key: (room_id, wi_min, wi_max) → list[bin_record]
_cache: dict = {}
_cache_lock = threading.Lock()

# second-level cache for combined series: (ids, wi_min, wi_max, key, agg)
combined_cache: dict = {}
combined_lock = threading.Lock()


def _iso(ms: int) -> str:
    return (datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


def fetch_binned(room_id: int, start_ms: int, end_ms: int, n_bins: int) -> list:
    base = {"startTime": _iso(start_ms), "endTime": _iso(end_ms), "bins": n_bins}
    recs, page, total = [], 1, 1

    while page <= total and page <= 50:          # 50-page safety cap
        params = {**base, "page": page}
        try:
            r = session.get(
                f"{API_URL}/rooms/{room_id}/data",
                headers=HEADERS,
                params=params,
                timeout=30,
            )

            # If token expired, refresh once and retry
            if r.status_code == 401:
                print(f"  room {room_id} page {page}: token expired, refreshing…")
                refresh_token()
                r = session.get(
                    f"{API_URL}/rooms/{room_id}/data",
                    headers=HEADERS,
                    params=params,
                    timeout=30,
                )

            r.raise_for_status()
        except Exception as e:
            # Let the caller know this failed — do NOT treat it as "no data"
            print(f"  room {room_id} fetch failed on page {page}: {e}")
            raise

        body = r.json()
        page_recs = body.get("results", [])
        recs += page_recs
        total = body.get("pagination", {}).get("totalPages", 1)
        page += 1

    # Optional debug: inspect one record’s keys
    if recs:
        print(f"Sample /rooms/{room_id}/data record keys:", list(recs[0].keys()))

    print(f"  room {room_id}: {len(recs)} bins  "
          f"({_iso(start_ms)[:10]} → {_iso(end_ms)[:10]}, requested {n_bins})")
    return recs


def cache_binned(room_id, start_ms, end_ms, n_bins):
    """Build binned records for one room from the cached 6-hour values.

    Returns the same record shape combine() expects:
      {timestamp, temperature, temperature_min, temperature_max, temperature_count}
    where, for each output bin, avg = mean of the 6-hour points falling in it and
    max/min = their extremes. Bins use identical edges for every room (derived
    from start_ms/end_ms/n_bins), so they line up by timestamp for combining.
    """
    cells = _q6h.get(room_id)
    span = end_ms - start_ms
    if not cells or span <= 0 or n_bins <= 0:
        return []
    bin_w = span / n_bins
    ci_lo = bisect_left(Q6H_STARTS, start_ms)
    ci_hi = bisect_left(Q6H_STARTS, end_ms)
    acc: dict = {}                       # bin_idx -> [sum, count, min, max]
    for ci in range(ci_lo, ci_hi):
        t = cells.get(ci)
        if t is None:
            continue
        bi = int((Q6H_STARTS[ci] - start_ms) / bin_w)
        if bi < 0: bi = 0
        elif bi >= n_bins: bi = n_bins - 1
        a = acc.get(bi)
        if a is None:
            acc[bi] = [t, 1, t, t]
        else:
            a[0] += t; a[1] += 1
            if t < a[2]: a[2] = t
            if t > a[3]: a[3] = t
    out = []
    for bi in sorted(acc):
        s, c, mn, mx = acc[bi]
        out.append({
            "timestamp":         _iso(int(start_ms + bi * bin_w)),
            "temperature":       round(s / c, 2),
            "temperature_min":   round(mn, 2),
            "temperature_max":   round(mx, 2),
            "temperature_count": c,
        })
    return out


def _safe_fetch(room_id, start_ms, end_ms, n_bins):
    if CACHE_ONLY:
        return cache_binned(room_id, start_ms, end_ms, n_bins)
    return fetch_binned(room_id, start_ms, end_ms, n_bins)


def get_binned_many(room_ids: list, wi_min: int, wi_max: int):
    """Return (list_of_per_room_bin_records, start_ms, end_ms).

    Each room shares identical startTime/endTime/bins, so their bins line up by
    timestamp and can be combined downstream. Per-room results are cached and
    missing ones are fetched in parallel.
    """
    start_ms = WEEK_STARTS[wi_min]
    end_ms   = min(WEEK_STARTS[wi_max] + WEEK_MS, DATA_END_MS)
    span_days = (end_ms - start_ms) / 86400000
    # ~1 bin/day, capped; you can further tune this if desired
    n_bins   = max(8, min(int(round(span_days)), 400))

    have, missing = {}, []
    with _cache_lock:
        for rid in room_ids:
            k = (rid, wi_min, wi_max)
            if k in _cache: have[rid] = _cache[k]
            else:           missing.append(rid)

    if missing:
        with ThreadPoolExecutor(max_workers=16) as ex:  # tune 8–32 depending on API capacity
            fetched = list(ex.map(
                lambda rid: (rid, _safe_fetch(rid, start_ms, end_ms, n_bins)),
                missing))
        with _cache_lock:
            for rid, recs in fetched:
                _cache[(rid, wi_min, wi_max)] = recs
                have[rid] = recs

    return [(rid, have[rid]) for rid in room_ids if rid in have], start_ms, end_ms


def combine(room_pairs: list, key: str, agg: str):
    """Combine aligned per-room bins into one series.

    avg = count-weighted mean of room averages; max = max of room maxes;
    min = min of room mins. For max/min we also record WHICH room produced the
    peak/trough in each bin, so the tooltip can name the floor and room. Also
    returns the total number of raw readings behind each bin.

    Returns (order, values, counts, sources) where sources[i] is
    {"floor", "number"} for max/min (or None for avg / empty bins).
    """
    acc, order = {}, []

    for rid, recs in room_pairs:
        for rec in recs:
            # Accept several possible timestamp field names
            ts = (
                rec.get("timestamp")
                or rec.get("time")
                or rec.get("bucket_start")
            )
            if ts is None:
                continue

            if ts not in acc:
                acc[ts] = {
                    "vsum": 0.0,
                    "wsum": 0.0,
                    "mx": None,
                    "mn": None,
                    "mx_rid": None,
                    "mn_rid": None,
                    "cnt": 0.0,
                }
                order.append(ts)
            a = acc[ts]

            # Try multiple naming conventions for the average field
            avg = rec.get(key)
            if avg is None:
                avg = rec.get(f"{key}_avg")  # e.g. 'temperature_avg'

            cnt = rec.get(f"{key}_count")

            mx = rec.get(f"{key}_max")
            mn = rec.get(f"{key}_min")

            # counts
            try:
                if cnt is not None:
                    a["cnt"] += float(cnt)
            except (ValueError, TypeError):
                pass

            # avg (weighted by count)
            try:
                if avg is not None:
                    w = float(cnt) if cnt else 1.0
                    a["vsum"] += float(avg) * w
                    a["wsum"] += w
            except (ValueError, TypeError):
                pass

            # max
            try:
                if mx is not None:
                    mxf = float(mx)
                    if a["mx"] is None or mxf > a["mx"]:
                        a["mx"] = mxf
                        a["mx_rid"] = rid
            except (ValueError, TypeError):
                pass

            # min
            try:
                if mn is not None:
                    mnf = float(mn)
                    if a["mn"] is None or mnf < a["mn"]:
                        a["mn"] = mnf
                        a["mn_rid"] = rid
            except (ValueError, TypeError):
                pass

    # sort timestamps (ISO strings or numeric)
    order.sort()
    values, counts, sources = [], [], []
    for ts in order:
        a = acc[ts]
        if   agg == "max": v, rid = a["mx"], a["mx_rid"]
        elif agg == "min": v, rid = a["mn"], a["mn_rid"]
        else:
            v = (a["vsum"] / a["wsum"]) if a["wsum"] > 0 else None
            rid = None
        values.append(round(v, 2) if v is not None else None)
        counts.append(int(a["cnt"]))
        sources.append(ROOM_INFO_BY_ID.get(rid) if rid is not None else None)
    return order, values, counts, sources


def get_combined_series(room_ids: list, wi_min: int, wi_max: int, key: str, agg: str):
    """Second-level cache: combined series for given scope+period+agg."""
    ck = (tuple(sorted(room_ids)), wi_min, wi_max, key, agg)
    with combined_lock:
        if ck in combined_cache:
            return combined_cache[ck]

    recs_list, start_ms, end_ms = get_binned_many(room_ids, wi_min, wi_max)
    order, values, counts, sources = combine(recs_list, key, agg)
    with combined_lock:
        combined_cache[ck] = (order, values, counts, sources, start_ms, end_ms)
    return order, values, counts, sources, start_ms, end_ms


def _label(dt: datetime, span_days: float) -> str:
    if span_days > 200: return dt.strftime("%b '%y")
    if span_days > 25:  return dt.strftime("%d %b")
    return dt.strftime("%d %b %Hh")


# ── API ────────────────────────────────────────────────────────────────────────
app = FastAPI()


@app.get("/api/meta")
def api_meta():
    """Metadata for the JS: floors, rooms per floor, week timeline, sensors."""
    floors_data = {}
    for fl in FLOOR_NUMS:
        rooms = FLOORS_BY_NUM[fl]
        # Sort rooms by number for consistent ordering
        floors_data[str(fl)] = sorted(
            [{"id": r["id"], "number": r["number"]} for r in rooms],
            key=lambda x: x["number"]
        )
    return {
        "floors":      FLOOR_NUMS,
        "rooms":       floors_data,
        "sensors":     SENSORS,
        "weeks":       WEEK_LABELS,
        "week_starts": WEEK_STARTS,
        "nw":          NW,
    }


@app.get("/api/roomdata")
def api_roomdata(
    room_id: int = Query(None),
    scope:   str = Query("room"),   # room | floor | building
    floor:   int = Query(None),
    si:      int = Query(0),
    wi_min:  int = Query(0),
    wi_max:  int = Query(NW - 1),
    agg:     str = Query("avg"),    # avg | max | min — picks the binned stat
):
    """
    Return a binned time series for one room, a whole floor, or the whole
    building, for one sensor over the selected period. avg/max/min map onto the
    API's per-bin fields; floor/building combine those across rooms.

    Response: { labels: [...], values: [val|null, ...] }
    """
    wi_min = max(0, min(wi_min, NW - 1))
    wi_max = max(wi_min, min(wi_max, NW - 1))
    si     = max(0, min(si, len(SENSORS) - 1))
    if agg not in ("avg", "max", "min"): agg = "avg"
    key = SENSORS[si]["key"]

    if scope == "building":
        ids = ALL_ROOM_IDS
    elif scope == "floor" and floor is not None:
        ids = [r["id"] for r in FLOORS_BY_NUM.get(int(floor), [])]
    else:
        ids = [room_id] if room_id is not None else []

    if not ids:
        return {"labels": [], "values": [], "error": "no rooms in scope", "fetched": False}

    try:
        order, values, counts, sources, start_ms, end_ms = get_combined_series(ids, wi_min, wi_max, key, agg)
    except Exception as e:
        # Surface the error to the frontend instead of pretending "no data"
        return {"labels": [], "values": [], "error": str(e), "fetched": False}

    span_days = (end_ms - start_ms) / 86400000

    labels = []
    for ts in order:
        try:
            # Support numeric timestamps (ms since epoch) and ISO strings
            if isinstance(ts, (int, float)):
                dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
            else:
                dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
            labels.append(_label(dt, span_days))
        except Exception:
            labels.append("")

    return {"labels": labels, "values": values, "counts": counts,
            "sources": sources, "agg": agg,
            "fetched": True, "rooms": len(ids)}


@app.get("/api/status")
def api_status():
    with _cache_lock:
        cached_bins = len(_cache)
    with combined_lock:
        cached_combined = len(combined_cache)
    return {
        "cached_rooms": cached_bins,
        "cached_series": cached_combined,
        "total_rooms": len(_rooms_raw),
    }


@app.get("/")
def index(): return HTMLResponse(HTML)


# ── HTML ───────────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>LAB42 Chart</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{
  background:#050507;
  font-family:'Segoe UI',sans-serif;
  overflow:hidden;
  display:flex;
  flex-direction:column;
  height:100vh;
  color:#e5e7eb;
}

/* ── Top bar ── */
#topbar{
  flex-shrink:0;
  background:#0b0c10;
  border-bottom:.5px solid rgba(255,255,255,.06);
  padding:8px 14px;
  display:flex;
  flex-direction:column;
  gap:6px;
}
.tb-row{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.tb-label{
  font-size:9px;
  font-weight:600;
  color:rgba(255,255,255,.35);
  letter-spacing:.08em;
  text-transform:uppercase;
  white-space:nowrap;
  min-width:60px;
}
.chip-group{display:flex;gap:5px;flex-wrap:wrap}
.chip{
  padding:9px 17px;
  border-radius:22px;
  font-size:14px;
  font-weight:600;
  border:1px solid rgba(255,255,255,.10);
  background:rgba(255,255,255,.02);
  color:rgba(229,231,235,.7);
  cursor:pointer;
  white-space:nowrap;
  transition:all .15s;
  user-select:none;
}
.chip:hover,.chip.hand-hover{
  border-color:#818cf8;
  background:rgba(129,140,248,.12);
  color:#e5e7eb;
}
.chip.active{
  border-color:#818cf8;
  background:rgba(129,140,248,.22);
  color:#f9fafb;
}
#period-row{overflow-x:auto;scrollbar-width:none;flex:1}
#period-row::-webkit-scrollbar{display:none}
#period-chips{display:flex;gap:6px;padding-bottom:2px}
#period-chips .chip{font-size:13px;padding:7px 13px}
#loading-bar{
  height:2px;
  background:rgba(129,140,248,.12);
  position:relative;
  flex-shrink:0;
}
#loading-fill{
  position:absolute;
  left:0;top:0;
  height:100%;
  background:#818cf8;
  transition:width .4s;
}
#loading-text{
  font-size:9px;
  color:rgba(255,255,255,.4);
  white-space:nowrap;
}

/* ── Main area ── */
#main{flex:1;display:flex;overflow:hidden;min-height:0}

/* ── Left panel: floor + room selector ── */
#selector{
  width:228px;
  flex-shrink:0;
  background:#0b0c10;
  border-right:.5px solid rgba(255,255,255,.06);
  display:flex;
  flex-direction:column;
  overflow:hidden;
}
#selector-inner{
  flex:1;
  overflow-y:auto;
  padding:8px;
}
#selector-inner::-webkit-scrollbar{width:4px}
#selector-inner::-webkit-scrollbar-thumb{
  background:rgba(255,255,255,.12);
  border-radius:2px;
}
.floor-section{margin-bottom:12px}
.floor-label{
  font-size:9px;
  font-weight:700;
  color:rgba(255,255,255,.4);
  letter-spacing:.1em;
  text-transform:uppercase;
  padding:4px 6px;
}
.room-chip{
  display:block;
  width:100%;
  text-align:left;
  padding:9px 13px;
  margin-bottom:5px;
  border-radius:10px;
  font-size:13.5px;
  font-weight:500;
  color:rgba(229,231,235,.7);
  background:transparent;
  border:.5px solid transparent;
  cursor:pointer;
  transition:all .12s;
}
.room-chip:hover,.room-chip.hand-hover{
  background:rgba(129,140,248,.14);
  color:#e5e7eb;
  border-color:#818cf8;
}
.room-chip.active{
  background:rgba(129,140,248,.25);
  color:#f9fafb;
  border-color:#818cf8;
}
/* building / floor aggregate chips */
.scope-chip{
  font-weight:700;
  color:rgba(229,231,235,.8);
  background:rgba(129,140,248,.10);
  border-color:rgba(129,140,248,.25);
}
.scope-chip.active{color:#f9fafb}

/* ── Chart area ── */
#chart-area{
  flex:1;
  display:flex;
  flex-direction:column;
  min-width:0;
  position:relative;
}
#avg-readout{
  position:absolute; top:8px; right:14px; z-index:5;
  background:rgba(129,140,248,.12);
  border:1px solid rgba(129,140,248,.42);
  border-radius:8px; padding:6px 12px; text-align:right;
  pointer-events:none;
}
#avg-readout .lbl{font-size:9px;letter-spacing:.04em;text-transform:uppercase;color:rgba(199,210,254,.85)}
#avg-readout .val{font-size:20px;font-weight:700;color:#c7d2fe;line-height:1.15}
#chart-title{
  padding:10px 16px 0;
  font-size:13px;
  font-weight:600;
  color:#e5e7eb;
  flex-shrink:0;
}
#chart{flex:1;min-height:0}
#chart-hint{
  padding:4px 16px 8px;
  font-size:10px;
  color:rgba(255,255,255,.45);
  flex-shrink:0;
}

/* ── Sidebar ── */
#sidebar{
  width:156px;
  flex-shrink:0;
  background:#090a0f;
  border-left:.5px solid rgba(255,255,255,.06);
  padding:10px 8px;
  display:flex;
  flex-direction:column;
  gap:10px;
}
.sbt{
  font-size:8px;
  color:rgba(255,255,255,.4);
  font-weight:600;
  letter-spacing:.08em;
  text-transform:uppercase;
}
#hand-panel{
  width:100%;
  aspect-ratio:4/3;
  border-radius:8px;
  background:#05060a;
  overflow:hidden;
}
.hint{
  font-size:9px;
  color:rgba(255,255,255,.5);
  line-height:1.8;
}
.hint b{color:rgba(255,255,255,.8)}

/* ── Hand cursor ── */
#cursor{
  position:fixed;
  width:18px;height:18px;
  border-radius:50%;
  border:2px solid #818cf8;
  pointer-events:none;
  z-index:200;
  transform:translate(-50%,-50%);
  display:none;
  background:rgba(129,140,248,.16);
  transition:border-color .1s,background .1s;
}
</style>
</head>
<body>

<div id="topbar">
  <div class="tb-row" style="display:none">
    <span class="tb-label">Sensor</span>
    <div class="chip-group" id="sensor-chips"></div>
  </div>
  <div class="tb-row">
    <span class="tb-label">Granularity</span>
    <div class="chip-group" id="gran-chips">
      <div class="chip active" data-gran="week">Week</div>
      <div class="chip"        data-gran="month">Month</div>
      <div class="chip"        data-gran="year">Year</div>
    </div>
    <div style="width:1px;height:20px;background:rgba(255,255,255,.08);margin:0 2px"></div>
    <span class="tb-label">Show</span>
    <div class="chip-group" id="agg-chips">
      <div class="chip active" data-agg="avg">Average</div>
      <div class="chip"        data-agg="max">Highest</div>
      <div class="chip"        data-agg="min">Lowest</div>
    </div>
  </div>
  <div class="tb-row" style="gap:4px">
    <span class="tb-label">Period</span>
    <div class="chip" id="prev-btn" style="padding:7px 15px;font-size:17px">&#8592;</div>
    <div id="period-row"><div id="period-chips"></div></div>
    <div class="chip" id="next-btn" style="padding:7px 15px;font-size:17px">&#8594;</div>
    <span id="loading-text"></span>
  </div>
  <div id="loading-bar"><div id="loading-fill" style="width:0%"></div></div>
</div>

<div id="main">
  <!-- Room selector -->
  <div id="selector">
    <div id="selector-inner">
      <div style="font-size:10px;color:rgba(255,255,255,.3);padding:6px;text-align:center">
        Loading rooms…
      </div>
    </div>
  </div>

  <!-- Chart -->
  <div id="chart-area">
    <div id="chart-title">Select a room to view data</div>
    <div id="avg-readout" style="display:none"><div class="lbl">Average</div><div class="val">—</div></div>
    <div id="chart"></div>
    <div id="chart-hint">Showing weekly aggregated values for the selected period</div>
  </div>

  <!-- Sidebar with hand preview -->
  <div id="sidebar">
    <div>
      <div class="sbt">Hand control</div>
      <div id="hand-panel"></div>
      <div class="hint" style="margin-top:6px">
        <b>Open hand</b> — hover<br>
        <b>Over chart</b> — inspect points<br>
        <b>Pinch</b> — click / scroll list<br>
        <b>Both hands up (1 open + 1 pinch)</b> — zoom chart
      </div>
    </div>
    <div class="hint">
      <div class="sbt" style="margin-bottom:3px">Keys</div>
      ← → — period<br>
      ↑ ↓ — room
    </div>
  </div>
</div>
<div id="cursor"></div>

<script type="module">
const _mp = import("https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.3/vision_bundle.mjs");

// ── State ──────────────────────────────────────────────────────────────────────
let meta=null;
let activeGran='week', activePeriodIdx=0, activeAgg='avg', activeSensor=0;
let activeScope=null;   // {type:'room'|'floor'|'building', roomId?, roomNum?, floor?, label}
let periods=[], allRoomChips=[];
let pollTimer=null, chartLoading=false;
let chartCounts=[];   // readings behind each plotted bin (for the tooltip)
let chartSources=[];  // for max/min: which {floor,number} produced each bin
let reqSeq=0;         // monotonic id so only the latest fetch updates the chart
let curDataLen=0, chartHovering=false, lastTipIdx=-1;   // hand-hover-over-chart state

const loadFill  = document.getElementById('loading-fill');
const loadText  = document.getElementById('loading-text');
const chartTitle= document.getElementById('chart-title');

// ── ECharts init ───────────────────────────────────────────────────────────────
const chartEl = document.getElementById('chart');
const ec = echarts.init(chartEl, 'dark');
window.addEventListener('resize', ()=>ec.resize());
new ResizeObserver(()=>ec.resize()).observe(chartEl);

ec.setOption({
  backgroundColor:'transparent',
  grid:{
    left:'6%',
    right:'4%',
    top:'8%',
    bottom:'14%',
    containLabel:true
  },
  xAxis:{
    type:'category',
    data:[],
    axisLabel:{color:'#94a3b8',fontSize:10},
    axisLine:{lineStyle:{color:'rgba(148,163,184,.6)'}},
    axisTick:{show:false}
  },
  yAxis:{
    type:'value',
    axisLabel:{color:'#94a3b8',fontSize:10},
    axisLine:{show:false},
    splitLine:{lineStyle:{color:'rgba(148,163,184,.18)'}}
  },
  tooltip:{
    trigger:'axis',
    backgroundColor:'rgba(10,11,18,.96)',
    borderColor:'rgba(148,163,184,.4)',
    textStyle:{color:'#e5e7eb',fontSize:12},
    formatter:(ps)=>{
      if(!ps||!ps.length)return '';
      const p=ps[0];
      const v=(p.value==null)?'—':p.value;
      const c=chartCounts[p.dataIndex];
      const src=chartSources[p.dataIndex];

      const srcLine = src
        ? `<div style="opacity:.85;font-size:11px;margin-top:2px">Floor ${src.floor} · Room ${src.number}</div>`
        : '';
      const cntLine = (c!=null && c>0)
        ? `<div style="opacity:.6;font-size:11px;margin-top:2px">${c.toLocaleString()} readings</div>`
        : '';

      return `${p.axisValue}
        <div style="margin-top:2px"><b>${p.marker} ${p.seriesName}: ${v}</b></div>
        ${srcLine}${cntLine}`;
    }
  },
  series:[{
    type:'line',
    data:[],
    smooth:true,
    lineStyle:{color:'#818cf8',width:2},
    itemStyle:{color:'#818cf8'},
    areaStyle:{
      color:{
        type:'linear',x:0,y:0,x2:0,y2:1,
        colorStops:[
          {offset:0,color:'rgba(129,140,248,.22)'},
          {offset:1,color:'rgba(129,140,248,.02)'}
        ]
      }
    }
  }]
});

// ── Period options — identical logic to floor plan version ─────────────────────
function buildPeriods(){
  if(!meta)return;
  const weeks=meta.week_starts, NW=weeks.length;
  periods=[];
  if(activeGran==='week'){
    for(let wi=0;wi<NW;wi++){
      const d=new Date(weeks[wi]);
      periods.push({label:`W${wi+1} · ${d.toLocaleDateString('en',{month:'short',day:'numeric',year:'2-digit'})}`,wi_min:wi,wi_max:wi});
    }
  } else if(activeGran==='month'){
    let cur=null;
    for(let wi=0;wi<NW;wi++){
      const d=new Date(weeks[wi]),ym=`${d.getFullYear()}-${d.getMonth()}`;
      if(!cur||cur.ym!==ym){if(cur)periods.push(cur);cur={ym,wi_min:wi,wi_max:wi,label:d.toLocaleDateString('en',{month:'long',year:'numeric'})};}
      else cur.wi_max=wi;
    }
    if(cur)periods.push(cur);
  } else {
    let cur=null;
    for(let wi=0;wi<NW;wi++){
      const yr=new Date(weeks[wi]).getFullYear();
      if(!cur||cur.yr!==yr){if(cur)periods.push(cur);cur={yr,wi_min:wi,wi_max:wi,label:String(yr)};}
      else cur.wi_max=wi;
    }
    if(cur)periods.push(cur);
  }
  activePeriodIdx=Math.min(activePeriodIdx,periods.length-1);
  renderPeriodChips();
  if(activeScope) fetchChart();
}

function renderPeriodChips(){
  const container=document.getElementById('period-chips');
  container.innerHTML='';
  periods.forEach((p,i)=>{
    const el=document.createElement('div');
    el.className='chip'+(i===activePeriodIdx?' active':'');
    el.textContent=p.label; el.dataset.idx=i;
    el.addEventListener('click',()=>selectPeriod(i));
    container.appendChild(el);
  });
  const active=container.querySelector('.chip.active');
  if(active)active.scrollIntoView({inline:'center',block:'nearest',behavior:'smooth'});
}

function selectPeriod(idx){
  activePeriodIdx=Math.max(0,Math.min(idx,periods.length-1));
  renderPeriodChips();
  clearTimeout(pollTimer);
  if(activeScope) fetchChart();
}
function stepPeriod(d){selectPeriod(activePeriodIdx+d);}

// ── Room / floor / building selector ────────────────────────────────────────────
function buildRoomSelector(){
  if(!meta)return;
  const inner=document.getElementById('selector-inner');
  inner.innerHTML='';
  allRoomChips=[];

  const addChip=(parent,label,cls,scope)=>{
    const el=document.createElement('div');
    el.className='room-chip'+(cls?(' '+cls):'');
    el.textContent=label;
    el.addEventListener('click',()=>selectScope(scope,el));
    parent.appendChild(el);
    allRoomChips.push(el);
    return el;
  };

  // Whole building
  const top=document.createElement('div');
  top.className='floor-section';
  addChip(top,'▦  Whole building','scope-chip',{type:'building',label:'Whole building'});
  inner.appendChild(top);

  meta.floors.forEach(fl=>{
    const rooms=meta.rooms[String(fl)]||[];
    if(!rooms.length)return;
    const sec=document.createElement('div');
    sec.className='floor-section';
    const lbl=document.createElement('div');
    lbl.className='floor-label';lbl.textContent=`Floor ${fl}`;
    sec.appendChild(lbl);
    // Whole floor
    addChip(sec,`All rooms · floor ${fl}`,'scope-chip',
            {type:'floor',floor:fl,label:`Floor ${fl} · all rooms`});
    // Individual rooms
    rooms.forEach(r=>{
      addChip(sec,r.number,'',
              {type:'room',roomId:r.id,roomNum:r.number,floor:fl,
               label:`Floor ${fl} · Room ${r.number}`});
    });
    inner.appendChild(sec);
  });

  // Auto-select the first individual room (cheap single-room load)
  const firstRoom=allRoomChips.find(c=>c.textContent && !c.classList.contains('scope-chip'));
  (firstRoom||allRoomChips[0]).click();
}

function selectScope(scope, el){
  document.querySelectorAll('.room-chip').forEach(c=>c.classList.remove('active'));
  el.classList.add('active');
  activeScope=scope;
  el.scrollIntoView({block:'nearest',behavior:'smooth'});
  fetchChart();
}

function stepRoom(delta){
  const idx=allRoomChips.findIndex(c=>c.classList.contains('active'));
  if(idx<0)return;
  const next=allRoomChips[Math.max(0,Math.min(idx+delta,allRoomChips.length-1))];
  if(next)next.click();
}

// ── Chart fetch ────────────────────────────────────────────────────────────────
async function fetchChart(){
  if(!activeScope||!periods.length)return;
  const myReq=++reqSeq;                 // this is now the latest request
  const pd=periods[activePeriodIdx];
  const si=activeSensor;
  const sensor=meta.sensors[si];
  loadText.textContent='Loading…';
  loadFill.style.width='30%';
  document.getElementById('avg-readout').style.display='none';

  // build scope query
  let scopeQ;
  if(activeScope.type==='room')          scopeQ=`room_id=${activeScope.roomId}`;
  else if(activeScope.type==='floor')    scopeQ=`scope=floor&floor=${activeScope.floor}`;
  else                                   scopeQ=`scope=building`;

  try{
    const r=await fetch(`/api/roomdata?${scopeQ}&si=${si}&wi_min=${pd.wi_min}&wi_max=${pd.wi_max}&agg=${activeAgg}`);
    if(myReq!==reqSeq) return;          // a newer selection superseded this one
    if(!r.ok)throw new Error(r.status);
    const d=await r.json();
    if(myReq!==reqSeq) return;          // …check again after parsing

    if(d.error && !d.fetched){
      // Server reported an error (e.g. token, API failure)
      loadText.textContent='Error loading data';
      loadFill.style.width='0%';
      chartTitle.textContent=`${activeScope.label} · ${sensor.name} · ${pd.label}`;
      ec.setOption({
        title: {text:'Error loading data', left:'center', top:'middle',
                textStyle:{color:'rgba(148,163,184,.6)',fontSize:13,fontWeight:'normal'}},
        xAxis:{data:[]},
        series:[{data:[], name:sensor.name}]
      });
      return;
    }

    loadFill.style.width='100%';
    setTimeout(()=>{ if(myReq===reqSeq) loadFill.style.width='0%'; },600);
    loadText.textContent='';

    const aggLabel={'avg':'Average','max':'Highest','min':'Lowest'}[activeAgg]||'';
    chartTitle.textContent=`${activeScope.label} · ${sensor.name} (${aggLabel}) · ${pd.label}`;

    chartCounts=d.counts||[];
    chartSources=d.sources||[];
    curDataLen=(d.values||[]).length;
    const totalReadings=chartCounts.reduce((a,b)=>a+(b||0),0);
    const nPts=(d.values||[]).filter(v=>v!=null).length;
    document.getElementById('chart-hint').textContent=
      `${nPts} bins · ${totalReadings.toLocaleString()} readings aggregated`
      +((d.rooms||1)>1?` · ${d.rooms} rooms`:``);

    const hasData=(d.values||[]).some(v=>v!=null);

    // Average mode: show the overall mean across the selected timescale beside the
    // graph. Count-weighted (by readings per bin) so it's the true period average.
    const avgBox=document.getElementById('avg-readout');
    if(activeAgg==='avg' && hasData){
      let s=0,w=0;
      (d.values||[]).forEach((v,i)=>{ if(v!=null){ const c=(d.counts&&d.counts[i])?d.counts[i]:1; s+=v*c; w+=c; }});
      if(w>0){
        avgBox.querySelector('.lbl').textContent=`Average · ${pd.label}`;
        avgBox.querySelector('.val').textContent=`${(s/w).toFixed(1)} ${sensor.unit}`;
        avgBox.style.display='block';
      } else avgBox.style.display='none';
    } else avgBox.style.display='none';

    ec.setOption({
      title: hasData ? {text:''} :
        {text:'No data for this period', left:'center', top:'middle',
         textStyle:{color:'rgba(148,163,184,.7)',fontSize:13,fontWeight:'normal'}},
      xAxis:{data:d.labels},
      yAxis:{name:sensor.unit,nameTextStyle:{color:'#94a3b8',fontSize:10}},
      series:[{
        data:d.values,
        name:sensor.name,
        lineStyle:{color:'#818cf8'},
        itemStyle:{color:'#818cf8'},
      }]
    });
  }catch(e){
    if(myReq!==reqSeq) return;          // stale error — ignore
    loadText.textContent='Error loading data';
    loadFill.style.width='0%';
    console.warn('fetchChart failed',e);
  }
}

// ── Chip event wiring ──────────────────────────────────────────────────────────
document.getElementById('gran-chips').addEventListener('click',e=>{
  const chip=e.target.closest('.chip[data-gran]');if(!chip)return;
  document.querySelectorAll('#gran-chips .chip').forEach(c=>c.classList.remove('active'));
  chip.classList.add('active');activeGran=chip.dataset.gran;
  activePeriodIdx=periods.length?periods.length-1:0;buildPeriods();
});
document.getElementById('agg-chips').addEventListener('click',e=>{
  const chip=e.target.closest('.chip[data-agg]');if(!chip)return;
  document.querySelectorAll('#agg-chips .chip').forEach(c=>c.classList.remove('active'));
  chip.classList.add('active');activeAgg=chip.dataset.agg;
  if(activeScope){clearTimeout(pollTimer);fetchChart();}
});
document.getElementById('prev-btn').addEventListener('click',()=>stepPeriod(-1));
document.getElementById('next-btn').addEventListener('click',()=>stepPeriod(1));

// ── Sensor buttons ──────────────────────────────────────────────────────────────
function buildSensorChips(){ /* temperature-only build: no sensor chips */ }
function selectSensor(i){ activeSensor=0; }      // locked to temperature
window.nextSensor=()=>{};                         // no-op (fist gesture does nothing)

window.addEventListener('keydown',e=>{
  if(e.key==='ArrowRight') stepPeriod(1);
  else if(e.key==='ArrowLeft') stepPeriod(-1);
  else if(e.key==='ArrowUp') stepRoom(-1);
  else if(e.key==='ArrowDown') stepRoom(1);
});

// ── Boot ───────────────────────────────────────────────────────────────────────
async function init(){
  try{
    const r=await fetch('/api/meta');
    meta=await r.json();
    buildSensorChips();
    buildRoomSelector();
    activePeriodIdx=0;buildPeriods();
    activePeriodIdx=periods.length-1;renderPeriodChips();
    if(activeScope)fetchChart();
  }catch(e){loadText.textContent='Failed: '+e.message;}
}
init();

// ── activateChip — every chip has its own (or delegated) click handler,
//    so a synthetic click drives the right action for all chip types. ───────────
function activateChip(chip){
  if(chip) chip.click();
}

function hitChip(sx,sy){
  return document.elementsFromPoint(sx,sy)
    .find(e=>e.classList.contains('chip')||e.classList.contains('room-chip'))||null;
}

// Drive the ECharts tooltip from the hand cursor: if the cursor is over the
// chart, snap to the nearest data point and show its tooltip; hide it otherwise.
function hideTip(){
  if(chartHovering){ec.dispatchAction({type:'hideTip'});chartHovering=false;lastTipIdx=-1;}
}
function updateChartHover(sx,sy){
  const rect=chartEl.getBoundingClientRect();
  const inside = sx>=rect.left && sx<=rect.right && sy>=rect.top && sy<=rect.bottom;
  if(!inside||!curDataLen){ hideTip(); return; }
  let idx=null;
  try{
    const v=ec.convertFromPixel({xAxisIndex:0}, sx-rect.left);
    idx=Array.isArray(v)?v[0]:v;
  }catch(e){}
  if(idx==null||isNaN(idx)) return;
  idx=Math.max(0,Math.min(Math.round(idx),curDataLen-1));
  chartHovering=true;
  if(idx!==lastTipIdx){
    ec.dispatchAction({type:'showTip',seriesIndex:0,dataIndex:idx});
    lastTipIdx=idx;
  }
}

// ── Hand tracking — identical to floor plan version ────────────────────────────
const video=document.createElement('video');
video.autoplay=true;video.playsInline=true;video.muted=true;video.style.display='none';
document.body.appendChild(video);
const cur=document.getElementById('cursor');
let hc,hctx,det,lastSpan=null,pinching=false,dragged=false,curX=0,curY=0;
let posBuffer=[],hoveredChip=null,pinchStartY=0,zoomMiss=0,zoomEnd=100;
let wasPinching=false;  // tracks previous frame's pinch state for edge detection
let _DU=null,_HC=null;

const dist=(a,b)=>Math.sqrt((a.x-b.x)**2+(a.y-b.y)**2);
const isOpen=lm=>{let e=0;[[8,6],[12,10],[16,14],[20,18]].forEach(([t,p])=>{if(lm[t].y<lm[p].y)e++;});return e>=3;};
function smoothed(x,y){posBuffer.push([x,y]);if(posBuffer.length>6)posBuffer.shift();
  const n=posBuffer.length;return posBuffer.reduce(([ax,ay],[px,py])=>[ax+px/n,ay+py/n],[0,0]);}

const panel=document.getElementById('hand-panel');
hc=document.createElement('canvas');hc.width=320;hc.height=240;
hc.style.cssText='width:100%;height:100%;display:block;';
panel.appendChild(hc);hctx=hc.getContext('2d');

(async()=>{
  try{
    const{HandLandmarker,FilesetResolver,DrawingUtils}=await _mp;
    _DU=DrawingUtils;_HC=HandLandmarker.HAND_CONNECTIONS;
    const vis=await FilesetResolver.forVisionTasks("https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.3/wasm");
    det=await HandLandmarker.createFromOptions(vis,{baseOptions:{
      modelAssetPath:"https://storage.googleapis.com/mediapipe-models/hand_landmarker/hand_landmarker/float16/1/hand_landmarker.task",
      delegate:"GPU"},runningMode:"VIDEO",numHands:2});
    const s=await navigator.mediaDevices.getUserMedia({video:{width:640,height:480}});
    video.srcObject=s;await video.play();requestAnimationFrame(hl);
  }catch(e){console.warn('Hands:',e);}
})();

function hl(){
  requestAnimationFrame(hl);
  if(!det||video.readyState<2||!_DU)return;
  hctx.save();hctx.translate(hc.width,0);hctx.scale(-1,1);
  hctx.drawImage(video,0,0,hc.width,hc.height);hctx.restore();
  let res;try{res=det.detectForVideo(video,performance.now());}catch(e){return;}
  if(!res?.landmarks?.length){
    cur.style.display='none';
    if(hoveredChip){hoveredChip.classList.remove('hand-hover');hoveredChip=null;}
    hideTip();
    return;
  }
  const du=new _DU(hctx);
  res.landmarks.forEach(lm=>{
    const m=lm.map(p=>({...p,x:1-p.x}));
    du.drawLandmarks(m,{color:'#e5e7eb',lineWidth:1,radius:2});
    du.drawConnectors(m,_HC||[],{color:'#9ca3af',lineWidth:1});
  });
  // Zoom chart: two hands, BOTH held up, ONE open + ONE fist. Requiring both
  // hands raised means a hand at your side won't trigger zoom (distant camera).
  const RAISE_Y=0.72;
  if(res.landmarks.length===2){
    const A=res.landmarks[0], B=res.landmarks[1];
    const pinchH=(h)=>dist(h[8],h[4])/(dist(h[0],h[9])||1e-4) < 0.45;
    const up=(h)=>((h[0].y+h[5].y+h[9].y+h[13].y+h[17].y)/5)<RAISE_Y;
    if(up(A)&&up(B)&&((isOpen(A)&&pinchH(B))||(isOpen(B)&&pinchH(A)))){
      const sp=dist(A[0],B[0]);
      if(lastSpan!==null && lastSpan>0){
        let f=sp/lastSpan;
        f=Math.max(0.92,Math.min(1.09,f));
        if(Math.abs(f-1)>0.012){
          zoomEnd=Math.max(8,Math.min(100,zoomEnd/f));   // apart (f>1) -> zoom in
          ec.dispatchAction({type:'dataZoom',start:0,end:zoomEnd});
        }
      }
      lastSpan=(lastSpan===null)?sp:lastSpan*0.6+sp*0.4;
      zoomMiss=0;
      cur.style.display='none';hideTip();return;
    }
  }
  if(lastSpan!==null && ++zoomMiss>4) lastSpan=null;
  const lm=res.landmarks[0];
  const t2=lm[8], th=lm[4];

  // Palm centre = mean of wrist + the four knuckles (MCPs). Stays put during a
  // pinch, so the cursor doesn't twitch when the index finger moves to the thumb.
  const PALM=[0,5,9,13,17];
  let palmX=0, palmY=0;
  PALM.forEach(i=>{ palmX+=lm[i].x; palmY+=lm[i].y; });
  palmX/=PALM.length; palmY/=PALM.length;

  // finger extended = tip above its pip joint (assumes a roughly upright hand)
  const idxExt = lm[8].y  < lm[6].y;
  const midExt = lm[12].y < lm[10].y;
  const rngExt = lm[16].y < lm[14].y;
  const pnkExt = lm[20].y < lm[18].y;
  const open   = [idxExt,midExt,rngExt,pnkExt].filter(Boolean).length >= 3;

  const isFist = (!idxExt && !midExt && !rngExt && !pnkExt);   // all curled
  // scale-invariant pinch (steadier than a fist): thumb-index gap vs hand size
  const isPinch = !isFist && (dist(t2,th)/(dist(lm[0],lm[9])||1e-4) < 0.45)
                          && (midExt||rngExt||pnkExt);

  // debug HUD so you can see what's detected
  hctx.fillStyle='rgba(0,0,0,.65)';
  hctx.fillRect(0,0,hc.width,18);
  hctx.fillStyle='#e5e7eb';
  hctx.font='11px monospace';
  hctx.fillText(isPinch?'PINCH (grab)':open?'OPEN (hover)':isFist?'FIST':'—',4,13);

  // Input dead-margins: the palm only has to move within this central band of
  // the camera frame to cover the WHOLE screen. Bigger margins = smaller band =
  // more reach/amplification (corners come easily). Smaller = finer control.
  const MX=.24,MY_T=.32,MY_B=.16;
  const mx=Math.max(0,Math.min(1,((1-palmX)-MX)/(1-2*MX)));
  const my=Math.max(0,Math.min(1,(palmY-MY_T)/(1-MY_T-MY_B)));
  const[sx,sy]=smoothed(mx*window.innerWidth,my*window.innerHeight);
  cur.style.display='block';cur.style.left=sx+'px';cur.style.top=sy+'px';

  updateChartHover(sx,sy);   // show ECharts tooltip when hovering the graph

  const chip=hitChip(sx,sy);
  if(chip!==hoveredChip){
    if(hoveredChip)hoveredChip.classList.remove('hand-hover');
    hoveredChip=chip;if(hoveredChip)hoveredChip.classList.add('hand-hover');
  }

  if(isPinch){
    // Pinch: drag-scroll the room list; click a chip on release if it wasn't a drag.
    if(!pinching){pinching=true;dragged=false;pinchStartY=sy;posBuffer=[];}
    else if(Math.abs(sy-pinchStartY)>12){
      dragged=true;
      document.getElementById('selector-inner').scrollBy(0,-(sy-curY));
    }
    wasPinching=true;
    cur.style.borderColor='#818cf8';
    cur.style.background='rgba(129,140,248,.30)';
  } else {
    if(wasPinching){                         // pinch just released -> click if not a drag
      if(!dragged && hoveredChip) activateChip(hoveredChip);
      wasPinching=false;pinching=false;dragged=false;
    }
    if(hoveredChip){
      cur.style.borderColor='#818cf8';
      cur.style.background='rgba(129,140,248,.24)';
    } else {
      cur.style.borderColor='#64748b';
      cur.style.background='rgba(100,116,139,.18)';
    }
  }
  curX=sx;curY=sy;

  hctx.beginPath();
  hctx.arc((1-palmX)*hc.width,palmY*hc.height,6,0,Math.PI*2);
  hctx.fillStyle=isPinch?'#818cf8':open?'#e5e7eb':'rgba(255,255,255,.5)';
  hctx.fill();
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)