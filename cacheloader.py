"""
lab42_cache_loader.py — build the shared temperature cache.

Fetches every room's full-history temperature data (6-hour resolution) from the
BMS and writes it to lab42_temp_cache.sqlite. The server programs then read that
file and serve instantly, never touching the BMS.

Run this before testing (and again whenever you want fresh data):

    python lab42_cache_loader.py            # resume: fetch only what's missing
    python lab42_cache_loader.py --fresh    # wipe and refetch everything

IMPORTANT: the grid constants below (DATA_START_MS, DATA_END_MS) must match the
servers, or the cache is considered stale and cleared. They already do.
"""
import sys
import time
import threading
import json
import sqlite3
from bisect import bisect_right, bisect_left
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter

# ── CONFIG (keep in sync with the servers) ─────────────────────────────────────
BASE_URL      = "http://leffe.science.uva.nl:8044"
USERNAME      = "marnix"
PASSWORD      = "marnixq1w2e3r4"
DATA_START_MS = 1658016000000
DATA_END_MS   = 1781049600000
SIXH_MS       = 6 * 3600 * 1000
CHUNK_Q6H     = 256            # cells per request; 256 = best measured throughput
FETCH_WORKERS = 48
TEMP_KEY      = "temperature"
CACHE_PATH    = "lab42_temp_cache.sqlite"
REQUEST_TIMEOUT = 90

# ── Q6H GRID (identical construction to the servers) ───────────────────────────
def _build_q6h_cells():
    step = SIXH_MS
    start = (DATA_START_MS // step) * step
    cells, t = [], start
    while t < DATA_END_MS:
        cs = max(t, DATA_START_MS)
        ce = min(t + step, DATA_END_MS)
        if ce > cs:
            cells.append((cs, ce))
        t += step
    return cells


Q6H        = _build_q6h_cells()
Q6H_STARTS = [c[0] for c in Q6H]
N_Q6H      = len(Q6H)
N_CHUNKS   = (N_Q6H + CHUNK_Q6H - 1) // CHUNK_Q6H

# ── HTTP session ───────────────────────────────────────────────────────────────
SESSION = requests.Session()
_pool = max(16, FETCH_WORKERS + 4)
SESSION.mount("http://",  HTTPAdapter(pool_connections=_pool, pool_maxsize=_pool))
TOKEN = None
_tok_lock = threading.Lock()


def get_token():
    r = SESSION.post(f"{BASE_URL}/auth/login",
                     json={"username": USERNAME, "password": PASSWORD}, timeout=15)
    r.raise_for_status()
    return r.json()["access_token"]


def _iso(ms):
    return (datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


def fetch_binned(room_id, start_ms, end_ms, n_bins):
    global TOKEN
    params = {"startTime": _iso(start_ms), "endTime": _iso(end_ms),
              "bins": n_bins, "page": 1}
    hdr = {"Authorization": f"Bearer {TOKEN}"}
    r = SESSION.get(f"{BASE_URL}/rooms/{room_id}/data", params=params,
                    headers=hdr, timeout=REQUEST_TIMEOUT)
    if r.status_code == 401:                      # token expired -> refresh once
        with _tok_lock:
            TOKEN = get_token()
        hdr = {"Authorization": f"Bearer {TOKEN}"}
        r = SESSION.get(f"{BASE_URL}/rooms/{room_id}/data", params=params,
                        headers=hdr, timeout=REQUEST_TIMEOUT)
    r.raise_for_status()
    return r.json().get("results", [])

# ── Cache ──────────────────────────────────────────────────────────────────────
_cache_lock = threading.Lock()


def open_cache(fresh=False):
    conn = sqlite3.connect(CACHE_PATH, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS cells "
                 "(room_id INT, ci INT, temp REAL, PRIMARY KEY(room_id, ci))")
    conn.execute("CREATE TABLE IF NOT EXISTS done_ranges (room_id INT, lo INT, hi INT)")
    grid = f"{DATA_START_MS}:{DATA_END_MS}"
    have = conn.execute("SELECT v FROM meta WHERE k='grid'").fetchone()
    if fresh or (have and have[0] != grid):
        if have and have[0] != grid:
            print(f"  grid changed -> clearing cache")
        conn.execute("DELETE FROM cells")
        conn.execute("DELETE FROM done_ranges")
    conn.execute("INSERT OR REPLACE INTO meta VALUES('grid', ?)", (grid,))
    conn.commit()
    return conn


def _merge(rs):
    rs = sorted(rs); out = []
    for lo, hi in rs:
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def load_done(conn):
    """Return set of (room_id, chunk_idx) already fully fetched."""
    ranges = {}
    for rid, lo, hi in conn.execute("SELECT room_id, lo, hi FROM done_ranges"):
        ranges.setdefault(rid, []).append((lo, hi))
    done = set()
    for rid, rs in ranges.items():
        merged = _merge(rs)
        for ck in range(N_CHUNKS):
            lo = ck * CHUNK_Q6H
            hi = min(lo + CHUNK_Q6H, N_Q6H)
            if any(a <= lo and hi <= b for a, b in merged):
                done.add((rid, ck))
    return done


def write_chunk(conn, room_id, lo, hi, bucket):
    with _cache_lock:
        if bucket:
            conn.executemany("INSERT OR REPLACE INTO cells VALUES (?,?,?)",
                             [(room_id, ci, t) for ci, t in bucket.items()])
        conn.execute("INSERT INTO done_ranges VALUES (?,?,?)", (room_id, lo, hi))
        conn.commit()

# ── Worker ─────────────────────────────────────────────────────────────────────
_progress_lock = threading.Lock()
_done_count = 0
_t0 = None


def do_chunk(conn, room_id, ck, total):
    global _done_count, _t0
    lo = ck * CHUNK_Q6H
    hi = min(lo + CHUNK_Q6H, N_Q6H)
    start_ms, end_ms, n_bins = Q6H[lo][0], Q6H[hi - 1][1], hi - lo
    try:
        recs = fetch_binned(room_id, start_ms, end_ms, n_bins)
    except Exception as e:
        print(f"  room {room_id} chunk {ck} failed: {e}")
        return
    bucket = {}
    for rec in recs:
        ts = rec.get("timestamp")
        try:
            ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            continue
        ci = bisect_right(Q6H_STARTS, ms) - 1
        if not (lo <= ci < hi):
            continue
        v = rec.get(TEMP_KEY)
        if v is None:
            continue
        try:
            bucket[ci] = round(float(v), 2)
        except (TypeError, ValueError):
            pass
    write_chunk(conn, room_id, lo, hi, bucket)
    with _progress_lock:
        _done_count += 1
        n = _done_count
    if n % 100 == 0 or n == total:
        elapsed = max(1e-3, time.time() - _t0)
        rate = n / elapsed
        eta = (total - n) / rate / 60 if rate > 0 else 0
        print(f"  {n}/{total} chunks  {rate:.1f}/s  ETA ~{eta:.1f} min")


def main():
    global TOKEN, _t0, _done_count
    fresh = "--fresh" in sys.argv
    print("Authenticating...")
    TOKEN = get_token()
    rooms = SESSION.get(f"{BASE_URL}/rooms",
                        headers={"Authorization": f"Bearer {TOKEN}"}, timeout=15).json()
    room_ids = [r["id"] for r in rooms]
    print(f"  {len(room_ids)} rooms; {N_Q6H} q6h cells; {N_CHUNKS} chunks/room")

    conn = open_cache(fresh=fresh)
    conn.execute("INSERT OR REPLACE INTO meta VALUES('rooms_json', ?)",
                 (json.dumps(rooms),))
    conn.commit()

    done = load_done(conn)
    jobs = [(rid, ck) for ck in range(N_CHUNKS) for rid in room_ids
            if (rid, ck) not in done]
    total_all = len(room_ids) * N_CHUNKS
    print(f"  already cached: {len(done)}/{total_all} chunks; fetching {len(jobs)} more")
    if not jobs:
        print("Cache already complete. Nothing to do.")
        return

    _t0 = time.time()
    _done_count = 0
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as ex:
        for rid, ck in jobs:
            ex.submit(do_chunk, conn, rid, ck, len(jobs))
    print("Done. Cache is ready — start the servers and they'll load from it.")


if __name__ == "__main__":
    main()