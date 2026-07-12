"""
LAB42 Map Server — TEMPERATURE ONLY, DISK-CACHED build.

Differences from the main map server:
  - Only the temperature sensor is fetched/stored/shown (single value per cell).
  - Every fetched q6h value is persisted to a local SQLite file
    (lab42_temp_cache.sqlite). The first run still pays the one-time BMS fetch
    (~30 min, BMS-bound), but every run after loads the whole building from disk
    in seconds and makes no BMS requests — so all views are instant.

Usage:
  python lab42_map_server_temp_cached.py            # serve (uses/builds the cache)
  python lab42_map_server_temp_cached.py --prewarm  # build the cache headless, then exit
Debug: visit /api/debug   (per-floor matched / has_data breakdown)
Note: deleting lab42_temp_cache.sqlite forces a clean re-fetch.
"""
import pathlib, threading, time, uvicorn, re as _re, sys, sqlite3
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from statistics import mean

import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except Exception:                       # pragma: no cover
    Retry = None
from fastapi import FastAPI, Query
from fastapi.responses import HTMLResponse, Response

# ── CONFIG ────────────────────────────────────────────────────────────────────
BASE_URL      = "http://leffe.science.uva.nl:8044"
DATA_START_MS = 1658016000000
DATA_END_MS   = 1781049600000
WEEK_MS       = 7 * 24 * 3600 * 1000
FETCH_WORKERS = 48   # concurrent BMS API requests (I/O-bound, so many help).
                     # Raise if the BMS tolerates it; watch the ETA log for the
                     # sweet spot — too many and the server starts throttling.

# ── LOAD-ALL CONFIG ────────────────────────────────────────────────────────────
# We fetch ONLY the q6h (6-hour) tier for every room across the whole range, then
# derive day/week/month/year locally by aggregating the member q6h cells. q6h is
# the finest data available, so deriving coarser tiers from it gives consistent
# avg / max / min for every zoom level from a single fetch pass per room.
#
# CHUNK_Q6H = number of 6-hour cells fetched per upstream request (one API call).
#   MEASURED: upstream latency is ~flat in bins (a 64-bin and a 5696-bin request
#   over the same range both take ~13-23s), and the full 4-year history returns
#   in a SINGLE page. Per-request overhead dominates, so small chunks are the
#   worst choice — they pay that fixed cost ~89x per room. Bigger chunks cut the
#   request count almost for free.  requests/room = ceil(5696 / CHUNK_Q6H):
#        64 -> 89 req/room (~24k total, the old slow default)
#      1024 ->  6 req/room (~1.6k total)   <- current
#      2048 ->  3 req/room (~0.8k total)
#      5696 ->  1 req/room (~270 total, whole history in one call)
#   Push higher if the BMS tolerates the heavier concurrent requests (watch the
#   [io] log: if ms/req stays sane and there are no timeouts, go bigger). If you
#   see read timeouts, lower CHUNK_Q6H or lower FETCH_WORKERS.
# PREFETCH_ALL = on startup, enqueue every room's full q6h range in the background
#   so the whole building loads and never unloads. The visible region is always
#   enqueued at high priority on top of this, so the view fills in immediately.
# HISTORY_DAYS = if set to an int, only the most recent N days are loaded instead
#   of the full ~4-year range. Loading time scales with the window. With big
#   chunks the request count is already low, so this matters less than it did,
#   but it still cuts the per-request scan time. Set to None for the full range.
CHUNK_Q6H    = 256   # MEASURED best throughput. The BMS parallelises many small
                     # requests well (~3 req/s at 48 workers) but chokes on big
                     # concurrent ones (1-yr chunks dropped it to 0.3 req/s). Do
                     # NOT raise this. To go faster, raise FETCH_WORKERS instead.

PREFETCH_ALL = True  # set False to load lazily (visible-region-only) instead
HISTORY_DAYS = None  # full timeline. With big CHUNK_Q6H the request count is low

# CACHE_ONLY: this server reads ONLY from the local cache (lab42_temp_cache.sqlite)
# and never fetches data from the BMS. Run lab42_cache_loader.py first to build /
# refresh the cache; then every server start is instant. Set False to let the
# server fall back to fetching missing data from the BMS itself.
CACHE_ONLY = True
                     # enough that the whole range loads in a few minutes.

# Apply the history window by trimming the start back from the (aligned) end,
# which keeps every cell boundary on the 6-hour grid.
if HISTORY_DAYS is not None:
    _DAY = 24 * 3600 * 1000
    DATA_START_MS = max(DATA_START_MS, DATA_END_MS - HISTORY_DAYS * _DAY)
    print(f"  HISTORY_DAYS={HISTORY_DAYS}: loading window "
          f"{DATA_START_MS} .. {DATA_END_MS}")

SENSORS = [
    {"key":"temperature","name":"Temperature","unit":"°C", "lo":"#bfdbfe","hi":"#ef4444","vmin":16, "vmax":30},
]
_FILTER_ZEROS = set()   # temperature-only build: no zero-filtered sensors

ROOMS = {
    0: {"01":{"x":0.146,"y":0.783,"w":0.351,"h":0.192},"03":{"x":0.019,"y":0.666,"w":0.120,"h":0.314},
        "05":{"x":0.023,"y":0.199,"w":0.117,"h":0.151},"06":{"x":0.263,"y":0.045,"w":0.149,"h":0.148},
        "09":{"x":0.442,"y":0.040,"w":0.179,"h":0.159},"10":{"x":0.626,"y":0.040,"w":0.172,"h":0.157},
        "11":{"x":0.804,"y":0.040,"w":0.179,"h":0.156},"12":{"x":0.863,"y":0.203,"w":0.123,"h":0.147},
        "13":{"x":0.622,"y":0.356,"w":0.206,"h":0.154},"16":{"x":0.743,"y":0.666,"w":0.243,"h":0.157}},
    1: {"17":{"x":0.261,"y":0.012,"w":0.123,"h":0.202},"02":{"x":0.410,"y":0.012,"w":0.330,"h":0.328},
        "01":{"x":0.776,"y":0.013,"w":0.212,"h":0.327},"16":{"x":0.012,"y":0.173,"w":0.245,"h":0.167},
        "14":{"x":0.012,"y":0.507,"w":0.121,"h":0.157},"15":{"x":0.166,"y":0.507,"w":0.087,"h":0.158},
        "13":{"x":0.012,"y":0.672,"w":0.120,"h":0.157},"04":{"x":0.852,"y":0.675,"w":0.135,"h":0.162},
        "05":{"x":0.626,"y":0.676,"w":0.222,"h":0.161},"07":{"x":0.501,"y":0.793,"w":0.121,"h":0.203},
        "08":{"x":0.380,"y":0.794,"w":0.117,"h":0.202},"12":{"x":0.012,"y":0.835,"w":0.121,"h":0.158},
        "11":{"x":0.138,"y":0.877,"w":0.115,"h":0.116},"10":{"x":0.258,"y":0.877,"w":0.117,"h":0.114}},
    2: {"03":{"x":0.013,"y":0.017,"w":0.121,"h":0.162},"04":{"x":0.139,"y":0.017,"w":0.056,"h":0.098},
        "05":{"x":0.199,"y":0.016,"w":0.181,"h":0.163},"02":{"x":0.013,"y":0.187,"w":0.121,"h":0.153},
        "07":{"x":0.748,"y":0.344,"w":0.087,"h":0.217},"06":{"x":0.839,"y":0.343,"w":0.150,"h":0.158},
        "22":{"x":0.013,"y":0.496,"w":0.125,"h":0.161},"21":{"x":0.013,"y":0.663,"w":0.126,"h":0.159},
        "16":{"x":0.262,"y":0.665,"w":0.178,"h":0.154},"12":{"x":0.444,"y":0.665,"w":0.178,"h":0.154},
        "08":{"x":0.625,"y":0.891,"w":0.377,"h":0.096},"20":{"x":0.013,"y":0.830,"w":0.126,"h":0.157},
        "19":{"x":0.142,"y":0.890,"w":0.056,"h":0.096},"18":{"x":0.203,"y":0.890,"w":0.056,"h":0.096},
        "17":{"x":0.265,"y":0.891,"w":0.056,"h":0.096},"15":{"x":0.325,"y":0.891,"w":0.056,"h":0.096},
        "14":{"x":0.384,"y":0.891,"w":0.056,"h":0.096},"13":{"x":0.444,"y":0.891,"w":0.056,"h":0.096},
        "11":{"x":0.505,"y":0.891,"w":0.056,"h":0.096},"10":{"x":0.565,"y":0.891,"w":0.057,"h":0.096},
        "95a":{"x":0.170,"y":0.500,"w":0.092,"h":0.155},"08b":{"x":0.865,"y":0.665,"w":0.131,"h":0.230}},
    3: {"02a":{"x":0.064,"y":0.015,"w":0.106,"h":0.158},"03":{"x":0.177,"y":0.015,"w":0.048,"h":0.111},
        "04":{"x":0.230,"y":0.015,"w":0.049,"h":0.111},"05":{"x":0.284,"y":0.015,"w":0.049,"h":0.111},
        "06":{"x":0.339,"y":0.015,"w":0.049,"h":0.111},"07":{"x":0.393,"y":0.015,"w":0.049,"h":0.111},
        "08":{"x":0.448,"y":0.015,"w":0.049,"h":0.111},"11":{"x":0.502,"y":0.015,"w":0.049,"h":0.111},
        "12":{"x":0.556,"y":0.015,"w":0.049,"h":0.111},"13":{"x":0.611,"y":0.015,"w":0.049,"h":0.111},
        "15":{"x":0.665,"y":0.015,"w":0.048,"h":0.111},"16":{"x":0.719,"y":0.015,"w":0.047,"h":0.110},
        "17":{"x":0.772,"y":0.015,"w":0.161,"h":0.159},"02b":{"x":0.064,"y":0.181,"w":0.106,"h":0.153},
        "14":{"x":0.555,"y":0.181,"w":0.157,"h":0.154},"18":{"x":0.827,"y":0.182,"w":0.106,"h":0.151},
        "19":{"x":0.718,"y":0.261,"w":0.059,"h":0.152},"20":{"x":0.718,"y":0.421,"w":0.058,"h":0.072},
        "21":{"x":0.827,"y":0.342,"w":0.152,"h":0.152},"38":{"x":0.064,"y":0.502,"w":0.106,"h":0.152},
        "37":{"x":0.064,"y":0.663,"w":0.106,"h":0.152},"23":{"x":0.827,"y":0.661,"w":0.106,"h":0.152},
        "36":{"x":0.064,"y":0.823,"w":0.214,"h":0.157},"35":{"x":0.284,"y":0.823,"w":0.102,"h":0.157},
        "33":{"x":0.393,"y":0.823,"w":0.101,"h":0.157},"32":{"x":0.499,"y":0.823,"w":0.104,"h":0.157},
        "27":{"x":0.609,"y":0.823,"w":0.102,"h":0.157},"25":{"x":0.718,"y":0.823,"w":0.049,"h":0.157},
        "24":{"x":0.772,"y":0.823,"w":0.161,"h":0.157}},
    4: {"04":{"x":0.059,"y":0.013,"w":0.055,"h":0.163},"05":{"x":0.120,"y":0.013,"w":0.049,"h":0.133},
        "06":{"x":0.174,"y":0.013,"w":0.063,"h":0.164},"07":{"x":0.243,"y":0.013,"w":0.078,"h":0.163},
        "08":{"x":0.327,"y":0.013,"w":0.062,"h":0.164},"12":{"x":0.395,"y":0.013,"w":0.049,"h":0.095},
        "13":{"x":0.449,"y":0.013,"w":0.049,"h":0.095},"16":{"x":0.504,"y":0.013,"w":0.049,"h":0.095},
        "17":{"x":0.559,"y":0.013,"w":0.049,"h":0.095},"21":{"x":0.614,"y":0.013,"w":0.049,"h":0.164},
        "22":{"x":0.669,"y":0.013,"w":0.049,"h":0.164},"24":{"x":0.724,"y":0.013,"w":0.049,"h":0.164},
        "25":{"x":0.779,"y":0.013,"w":0.049,"h":0.164},"26":{"x":0.834,"y":0.012,"w":0.049,"h":0.134},
        "27":{"x":0.889,"y":0.012,"w":0.055,"h":0.165},"03":{"x":0.060,"y":0.184,"w":0.063,"h":0.074},
        "28":{"x":0.880,"y":0.185,"w":0.065,"h":0.073},"02":{"x":0.061,"y":0.267,"w":0.062,"h":0.072},
        "29":{"x":0.880,"y":0.267,"w":0.066,"h":0.073},"10":{"x":0.338,"y":0.252,"w":0.053,"h":0.089},
        "11":{"x":0.395,"y":0.252,"w":0.050,"h":0.088},"19":{"x":0.395,"y":0.252,"w":0.049,"h":0.088},
        "14":{"x":0.449,"y":0.161,"w":0.049,"h":0.180},"15":{"x":0.505,"y":0.161,"w":0.049,"h":0.180},
        "19":{"x":0.560,"y":0.253,"w":0.049,"h":0.087},"20":{"x":0.615,"y":0.253,"w":0.049,"h":0.087},
        "33":{"x":0.724,"y":0.347,"w":0.077,"h":0.156},"31":{"x":0.834,"y":0.348,"w":0.112,"h":0.073},
        "32":{"x":0.834,"y":0.430,"w":0.112,"h":0.073},"64":{"x":0.060,"y":0.511,"w":0.110,"h":0.073},
        "63":{"x":0.060,"y":0.593,"w":0.110,"h":0.073},"62":{"x":0.203,"y":0.510,"w":0.077,"h":0.156},
        "61":{"x":0.061,"y":0.675,"w":0.109,"h":0.092},"60":{"x":0.061,"y":0.776,"w":0.109,"h":0.092},
        "59":{"x":0.061,"y":0.878,"w":0.108,"h":0.118},"54":{"x":0.284,"y":0.673,"w":0.049,"h":0.088},
        "53":{"x":0.339,"y":0.674,"w":0.049,"h":0.087},"51":{"x":0.394,"y":0.643,"w":0.049,"h":0.144},
        "48":{"x":0.449,"y":0.643,"w":0.049,"h":0.144},"47":{"x":0.504,"y":0.643,"w":0.049,"h":0.144},
        "44":{"x":0.560,"y":0.643,"w":0.049,"h":0.144},"42":{"x":0.614,"y":0.673,"w":0.105,"h":0.114},
        "34":{"x":0.724,"y":0.593,"w":0.077,"h":0.073},"35":{"x":0.834,"y":0.673,"w":0.110,"h":0.094},
        "36":{"x":0.834,"y":0.776,"w":0.110,"h":0.093},"37":{"x":0.834,"y":0.879,"w":0.110,"h":0.117},
        "58":{"x":0.175,"y":0.906,"w":0.049,"h":0.090},"57":{"x":0.230,"y":0.906,"w":0.049,"h":0.090},
        "56":{"x":0.284,"y":0.906,"w":0.049,"h":0.090},"52":{"x":0.339,"y":0.836,"w":0.049,"h":0.159},
        "50":{"x":0.394,"y":0.836,"w":0.049,"h":0.159},"49":{"x":0.449,"y":0.836,"w":0.049,"h":0.159},
        "46":{"x":0.505,"y":0.836,"w":0.049,"h":0.159},"45":{"x":0.560,"y":0.836,"w":0.049,"h":0.159},
        "43":{"x":0.615,"y":0.836,"w":0.049,"h":0.159},"40":{"x":0.670,"y":0.904,"w":0.049,"h":0.092},
        "39":{"x":0.725,"y":0.904,"w":0.049,"h":0.092},"38":{"x":0.780,"y":0.904,"w":0.049,"h":0.092}
        ,"23":{"x":0.669,"y":0.253,"w":0.049,"h":0.087}},
    5: {"04":{"x":0.065,"y":0.015,"w":0.054,"h":0.159},"06":{"x":0.122,"y":0.014,"w":0.051,"h":0.131},
        "07":{"x":0.176,"y":0.014,"w":0.051,"h":0.161},"08":{"x":0.231,"y":0.014,"w":0.050,"h":0.161},
        "09":{"x":0.284,"y":0.014,"w":0.050,"h":0.161},"10":{"x":0.338,"y":0.014,"w":0.050,"h":0.161},
        "13":{"x":0.392,"y":0.013,"w":0.063,"h":0.161},"15":{"x":0.459,"y":0.013,"w":0.064,"h":0.161},
        "17":{"x":0.526,"y":0.012,"w":0.078,"h":0.163},"19":{"x":0.608,"y":0.011,"w":0.050,"h":0.093},
        "24":{"x":0.661,"y":0.011,"w":0.050,"h":0.093},"25":{"x":0.714,"y":0.011,"w":0.051,"h":0.093},
        "26":{"x":0.769,"y":0.011,"w":0.051,"h":0.093},"27":{"x":0.823,"y":0.011,"w":0.051,"h":0.128},
        "28":{"x":0.877,"y":0.011,"w":0.055,"h":0.159},
        "02":{"x":0.065,"y":0.256,"w":0.065,"h":0.081},
        "03":{"x":0.065,"y":0.175,"w":0.065,"h":0.075},
        "21":{"x":0.661,"y":0.237,"w":0.051,"h":0.093},"11":{"x":0.338,"y":0.214,"w":0.050,"h":0.118},
        "12":{"x":0.391,"y":0.214,"w":0.051,"h":0.150},"14":{"x":0.446,"y":0.214,"w":0.050,"h":0.150},
        "16":{"x":0.499,"y":0.214,"w":0.049,"h":0.150},"18":{"x":0.554,"y":0.214,"w":0.050,"h":0.150},
        "20":{"x":0.608,"y":0.237,"w":0.053,"h":0.093},"29":{"x":0.823,"y":0.175,"w":0.109,"h":0.075},
        "30":{"x":0.823,"y":0.256,"w":0.109,"h":0.075},"31":{"x":0.714,"y":0.334,"w":0.061,"h":0.074},
        "32":{"x":0.823,"y":0.335,"w":0.109,"h":0.075},"34":{"x":0.714,"y":0.415,"w":0.061,"h":0.074},
        "33":{"x":0.823,"y":0.415,"w":0.109,"h":0.075},"63":{"x":0.063,"y":0.493,"w":0.111,"h":0.094},
        "62":{"x":0.063,"y":0.593,"w":0.110,"h":0.096},"35":{"x":0.695,"y":0.493,"w":0.100,"h":0.165},
        "54":{"x":0.284,"y":0.653,"w":0.051,"h":0.089},"52":{"x":0.338,"y":0.653,"w":0.050,"h":0.089},
        "51":{"x":0.392,"y":0.653,"w":0.050,"h":0.089},"49":{"x":0.446,"y":0.653,"w":0.051,"h":0.089},
        "46":{"x":0.499,"y":0.624,"w":0.105,"h":0.144},"61":{"x":0.064,"y":0.694,"w":0.109,"h":0.116},
        "60":{"x":0.064,"y":0.814,"w":0.056,"h":0.166},"53":{"x":0.338,"y":0.813,"w":0.051,"h":0.168},
        "50":{"x":0.392,"y":0.813,"w":0.077,"h":0.168},"48":{"x":0.473,"y":0.813,"w":0.063,"h":0.168},
        "47":{"x":0.540,"y":0.813,"w":0.064,"h":0.168},"45":{"x":0.607,"y":0.813,"w":0.051,"h":0.168},
        "38":{"x":0.877,"y":0.814,"w":0.055,"h":0.164},"59":{"x":0.122,"y":0.844,"w":0.051,"h":0.135},
        "58":{"x":0.176,"y":0.881,"w":0.051,"h":0.099},"57":{"x":0.231,"y":0.881,"w":0.050,"h":0.099},
        "56":{"x":0.285,"y":0.881,"w":0.049,"h":0.099},"42":{"x":0.661,"y":0.881,"w":0.051,"h":0.099},
        "41":{"x":0.716,"y":0.881,"w":0.050,"h":0.099},"40":{"x":0.770,"y":0.881,"w":0.050,"h":0.099},
        "64":{"x":0.201,"y":0.493,"w":0.078,"h":0.084},
        "36":{"x":0.823,"y":0.653,"w":0.106,"h":0.075},
        "37":{"x":0.823,"y":0.735,"w":0.106,"h":0.075},"39":{"x":0.823,"y":0.843,"w":0.050,"h":0.135}},
    6: {"04":{"x":0.068,"y":0.015,"w":0.052,"h":0.159},"05":{"x":0.127,"y":0.015,"w":0.048,"h":0.129},
        "03":{"x":0.069,"y":0.182,"w":0.106,"h":0.072},"02":{"x":0.069,"y":0.263,"w":0.106,"h":0.072},
        "06":{"x":0.181,"y":0.014,"w":0.047,"h":0.094},"07":{"x":0.234,"y":0.015,"w":0.048,"h":0.093},
        "08":{"x":0.288,"y":0.016,"w":0.048,"h":0.093},"11":{"x":0.341,"y":0.014,"w":0.047,"h":0.094},
        "12":{"x":0.396,"y":0.013,"w":0.046,"h":0.095},"15":{"x":0.448,"y":0.013,"w":0.048,"h":0.095},
        "16":{"x":0.502,"y":0.013,"w":0.049,"h":0.095},"20":{"x":0.556,"y":0.013,"w":0.049,"h":0.095},
        "21":{"x":0.611,"y":0.014,"w":0.048,"h":0.160},"24":{"x":0.664,"y":0.014,"w":0.048,"h":0.160},
        "25":{"x":0.718,"y":0.014,"w":0.048,"h":0.160},"26":{"x":0.772,"y":0.014,"w":0.048,"h":0.160},
        "27":{"x":0.826,"y":0.015,"w":0.048,"h":0.127},"28":{"x":0.879,"y":0.015,"w":0.054,"h":0.157},
        "29":{"x":0.867,"y":0.181,"w":0.066,"h":0.070},"30":{"x":0.867,"y":0.263,"w":0.066,"h":0.070},
        "31":{"x":0.867,"y":0.342,"w":0.066,"h":0.072},"32":{"x":0.867,"y":0.423,"w":0.066,"h":0.072},
        "09":{"x":0.288,"y":0.181,"w":0.048,"h":0.152},"10":{"x":0.342,"y":0.181,"w":0.048,"h":0.152},
        "13":{"x":0.395,"y":0.181,"w":0.048,"h":0.152},"14":{"x":0.449,"y":0.181,"w":0.048,"h":0.152},
        "17":{"x":0.502,"y":0.248,"w":0.048,"h":0.084},"18":{"x":0.556,"y":0.248,"w":0.048,"h":0.084},
        "22":{"x":0.610,"y":0.248,"w":0.049,"h":0.084},"23":{"x":0.664,"y":0.248,"w":0.049,"h":0.084},
        "61":{"x":0.069,"y":0.504,"w":0.061,"h":0.069},"60":{"x":0.069,"y":0.582,"w":0.061,"h":0.069},
        "59":{"x":0.069,"y":0.660,"w":0.061,"h":0.071},"58":{"x":0.069,"y":0.741,"w":0.061,"h":0.070},
        "57":{"x":0.069,"y":0.821,"w":0.061,"h":0.070},"53":{"x":0.288,"y":0.661,"w":0.101,"h":0.151},
        "50":{"x":0.395,"y":0.661,"w":0.049,"h":0.151},"44":{"x":0.449,"y":0.661,"w":0.048,"h":0.085},
        "43":{"x":0.502,"y":0.661,"w":0.048,"h":0.085},"42":{"x":0.556,"y":0.661,"w":0.048,"h":0.085},
        "33":{"x":0.868,"y":0.661,"w":0.066,"h":0.071},"34":{"x":0.868,"y":0.741,"w":0.066,"h":0.070},
        "35":{"x":0.868,"y":0.820,"w":0.066,"h":0.070},"56":{"x":0.068,"y":0.900,"w":0.106,"h":0.078},
        "55":{"x":0.180,"y":0.887,"w":0.049,"h":0.092},"54":{"x":0.234,"y":0.887,"w":0.049,"h":0.092},
        "52":{"x":0.289,"y":0.887,"w":0.049,"h":0.092},"51":{"x":0.343,"y":0.870,"w":0.048,"h":0.108},
        "49":{"x":0.396,"y":0.887,"w":0.048,"h":0.092},"48":{"x":0.451,"y":0.887,"w":0.048,"h":0.092},
        "47":{"x":0.504,"y":0.887,"w":0.048,"h":0.092},"46":{"x":0.557,"y":0.887,"w":0.048,"h":0.092},
        "41":{"x":0.610,"y":0.887,"w":0.048,"h":0.092},"39":{"x":0.663,"y":0.887,"w":0.048,"h":0.092},
        "38":{"x":0.717,"y":0.887,"w":0.048,"h":0.092},"37":{"x":0.771,"y":0.887,"w":0.048,"h":0.092},
        "36":{"x":0.826,"y":0.899,"w":0.107,"h":0.081}},
}

FLOORS_LIST = sorted(ROOMS.keys(), reverse=True)  # 6→0, top to bottom

# ── AUTH + ROOM LIST ──────────────────────────────────────────────────────────
SESSION = requests.Session()
_pool = max(16, FETCH_WORKERS + 4)
_adapter_kwargs = dict(pool_connections=_pool, pool_maxsize=_pool)
if Retry is not None:
    _adapter_kwargs["max_retries"] = Retry(total=2, backoff_factor=0.3,
                                           status_forcelist=(502, 503, 504))
_adapter = HTTPAdapter(**_adapter_kwargs)
SESSION.mount("http://",  _adapter)
SESSION.mount("https://", _adapter)


def get_token():
    r = SESSION.post(f"{BASE_URL}/auth/login",
                     json={"username":"marnix","password":"marnixq1w2e3r4"}, timeout=10)
    r.raise_for_status()
    return r.json()["access_token"]


print("Authenticating...")
TOKEN   = get_token()
HEADERS = {"Authorization": f"Bearer {TOKEN}"}
SESSION.headers.update(HEADERS)

print("Loading room list...")
r = SESSION.get(f"{BASE_URL}/rooms", headers=HEADERS, timeout=15)
r.raise_for_status()
ALL_ROOMS = r.json()   # [{id, number, floor, ...}, ...]
print(f"  {len(ALL_ROOMS)} rooms from API")

# ── WEEK TIMELINE ─────────────────────────────────────────────────────────────
WEEK_STARTS: list[int] = []
t = DATA_START_MS
while t < DATA_END_MS:
    WEEK_STARTS.append(t)
    t += WEEK_MS
NW = len(WEEK_STARTS)
WEEK_LABELS = [datetime.fromtimestamp(w/1000, tz=timezone.utc).strftime("%b '%y")
               for w in WEEK_STARTS]


def _iso(ms: int) -> str:
    return (datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))


DATA_RANGE_END = DATA_START_MS + NW * WEEK_MS

# ── ZOOM TIERS / TIME CELLS ───────────────────────────────────────────────────
from bisect import bisect_right


def _dt(y, m=1, d=1):
    return datetime(y, m, d, tzinfo=timezone.utc)


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _build_year_cells():
    s = datetime.fromtimestamp(DATA_START_MS / 1000, tz=timezone.utc)
    e = datetime.fromtimestamp(DATA_END_MS  / 1000, tz=timezone.utc)
    cells = []
    for y in range(s.year, e.year + 1):
        cs = max(_ms(_dt(y)),     DATA_START_MS)
        ce = min(_ms(_dt(y + 1)), DATA_END_MS)
        if ce > cs:
            cells.append((cs, ce, str(y)))
    return cells


def _build_month_cells():
    s = datetime.fromtimestamp(DATA_START_MS / 1000, tz=timezone.utc)
    e = datetime.fromtimestamp(DATA_END_MS  / 1000, tz=timezone.utc)
    cells = []
    y, m = s.year, s.month
    while (y, m) <= (e.year, e.month):
        ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
        cs = max(_ms(_dt(y, m)),  DATA_START_MS)
        ce = min(_ms(_dt(ny, nm)), DATA_END_MS)
        if ce > cs:
            cells.append((cs, ce, _dt(y, m).strftime("%b '%y")))
        y, m = ny, nm
    return cells


def _build_week_cells():
    cells = []
    for w in WEEK_STARTS:
        cs = w
        ce = min(w + WEEK_MS, DATA_END_MS)
        if ce > cs:
            lbl = datetime.fromtimestamp(w / 1000, tz=timezone.utc).strftime("%d %b '%y")
            cells.append((cs, ce, lbl))
    return cells


DAY_MS_C  = 24 * 3600 * 1000
SIXH_MS   = 6 * 3600 * 1000


def _build_step_cells(step_ms: int, label_fmt: str):
    """Fixed-width calendar cells (days, 6-hour blocks)."""
    start = (DATA_START_MS // step_ms) * step_ms
    cells, t = [], start
    while t < DATA_END_MS:
        cs = max(t, DATA_START_MS)
        ce = min(t + step_ms, DATA_END_MS)
        if ce > cs:
            lbl = datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime(label_fmt)
            cells.append((cs, ce, lbl))
        t += step_ms
    return cells


TIER_ORDER = ["year", "month", "week", "day", "q6h"]
CELLS = {
    "year":  _build_year_cells(),
    "month": _build_month_cells(),
    "week":  _build_week_cells(),
    "day":   _build_step_cells(DAY_MS_C, "%d %b"),
    "q6h":   _build_step_cells(SIXH_MS, "%H:%M"),
}
CELL_STARTS = {tier: [c[0] for c in cells] for tier, cells in CELLS.items()}

CHUNK_CELLS = {
    "year":  max(1, len(CELLS["year"])),
    "month": 12,
    "week":  52,
    "day":   30,
    "q6h":   48,
}


def _n_chunks(tier: str) -> int:
    n, sz = len(CELLS[tier]), CHUNK_CELLS[tier]
    return (n + sz - 1) // sz


print(f"  cells per tier: " +
      ", ".join(f"{t}={len(CELLS[t])}" for t in TIER_ORDER))

# ── Q6H AGGREGATION MAP ────────────────────────────────────────────────────────
# All coarse cell boundaries (year/month/week/day) align exactly to q6h boundaries,
# so each coarse cell maps to a contiguous slice [lo, hi) of q6h cells. We precompute
# that slice per coarse cell once; at request time we aggregate the member q6h
# values (avg / max / min) for whichever coarse tier the client is viewing.
from bisect import bisect_left

N_Q6H        = len(CELLS["q6h"])
_Q6H_STARTS  = CELL_STARTS["q6h"]
_Q6H_ENDS    = [c[1] for c in CELLS["q6h"]]

Q6H_MEMBERS: dict[str, list[tuple[int, int]]] = {}
for _tier in TIER_ORDER:
    members: list[tuple[int, int]] = []
    for (cs, ce, _lbl) in CELLS[_tier]:
        lo = bisect_left(_Q6H_STARTS, cs)
        # hi = first q6h cell whose start is >= this coarse cell's end
        hi = bisect_left(_Q6H_STARTS, ce)
        members.append((lo, hi))
    Q6H_MEMBERS[_tier] = members

# Number of upstream fetch chunks needed to cover the whole q6h range for one room.
N_Q6H_CHUNKS = (N_Q6H + CHUNK_Q6H - 1) // CHUNK_Q6H
print(f"  q6h cells={N_Q6H}, fetch chunks/room={N_Q6H_CHUNKS} "
      f"(CHUNK_Q6H={CHUNK_Q6H})")

# ── ROOM MATCHING ─────────────────────────────────────────────────────────────
def _floor_from_number(raw: str):
    m = _re.match(r'^[Ll]?(\d+)[.\-_ ]', raw.strip())
    return int(m.group(1)) if m else None


def _short_key(raw: str) -> list:
    """Generate candidate ROOMS keys from an upstream room 'number' string.

    Upstream formats vary ("L4.35", "4.35", "L4-35", "L3.02a", "4.35 Office"),
    so we strip the floor prefix, keep the first token, and try a few normalised
    forms (raw, lowercase, zero-stripped, zero-padded, and the bare numeric core
    without a trailing letter). All candidates are still matched exactly against
    the real ROOMS keys for that floor, so over-generating here is safe.
    """
    n = raw.strip()
    no_prefix = _re.sub(r'^[Ll]?\d+[.\-_ ]', '', n).strip()
    core = _re.split(r'\s', no_prefix)[0] if no_prefix else no_prefix

    cands: list = []
    for base in (no_prefix, core):
        base = base.strip()
        if not base:
            continue
        nz = base.lstrip('0') or '0'
        cands += [base, base.lower(), nz, nz.lower(), base.zfill(2)]
        mnum = _re.match(r'(\d+)', base)          # bare numeric core (drops 'a'/'b' suffix)
        if mnum:
            num = mnum.group(1)
            num_nz = num.lstrip('0') or '0'
            cands += [num, num_nz, num.zfill(2)]
    return list(dict.fromkeys(cands))


def _match(raw: str, api_floor) -> tuple | None:
    candidates = _short_key(raw)
    bfloor = _floor_from_number(raw)
    if bfloor is None:
        try:
            bfloor = int(api_floor)
        except Exception:
            return None
    rooms_pos = ROOMS.get(bfloor)
    if not rooms_pos:
        return None
    ci = {k.lower(): k for k in rooms_pos}
    for c in candidates:
        if c in rooms_pos:
            return (bfloor, c)
        if c.lower() in ci:
            return (bfloor, ci[c.lower()])
    return None


ROOM_MAP: dict[int, tuple] = {}
UNMATCHED_UPSTREAM: list = []     # upstream rooms that matched no coordinate box
for room in ALL_ROOMS:
    m = _match(room["number"], room["floor"])
    if m:
        ROOM_MAP[room["id"]] = m
    else:
        UNMATCHED_UPSTREAM.append({"id": room.get("id"),
                                   "number": room.get("number"),
                                   "floor": room.get("floor")})
print(f"  {len(ROOM_MAP)} matched, {len(UNMATCHED_UPSTREAM)} unmatched")

FLOOR_ROOM_IDS: dict[int, list[int]] = defaultdict(list)
for rid, (bfloor, _) in ROOM_MAP.items():
    FLOOR_ROOM_IDS[bfloor].append(rid)

# Which coordinate boxes (drawn on the map) actually have an upstream data source.
MATCHED_KEYS: dict[int, set] = defaultdict(set)
for rid, (bfloor, rkey) in ROOM_MAP.items():
    MATCHED_KEYS[bfloor].add(rkey)
COORD_KEYS: dict[int, set] = {fl: set(rooms.keys()) for fl, rooms in ROOMS.items()}

# Per-floor match summary (printed once so it's visible in the server log).
print("  per-floor match (coord boxes -> matched to a sensor):")
for fl in sorted(COORD_KEYS):
    nb = len(COORD_KEYS[fl]); nm = len(MATCHED_KEYS.get(fl, ()))
    miss = sorted(COORD_KEYS[fl] - MATCHED_KEYS.get(fl, set()))
    extra = "" if not miss else "  no-data boxes: " + ", ".join(miss[:12]) + ("…" if len(miss) > 12 else "")
    print(f"    floor {fl}: {nm}/{nb} matched{extra}")

# ── DATA STORE ────────────────────────────────────────────────────────────────
# We only ever store q6h data. Coarser tiers are derived on the fly.
#   _q6h[room_id][ci] = [v_sensor0, v_sensor1, ...]   (None where no reading)
# A room_id appears in _q6h only once at least one of its chunks has been fetched.
_q6h: dict[int, dict[int, list]] = {}
_q6h_lock = threading.Lock()


def _store_count() -> int:
    with _q6h_lock:
        return sum(len(cells) for cells in _q6h.values())


_loading_count = 0
_loading_lock  = threading.Lock()

# Bumped every time new q6h data is stored; lets /api/slice skip re-aggregating
# an unchanged view (the year view is ~1.8M ops, so this matters once loading
# settles or the user pans back over already-loaded cells).
_data_version = 0

# ── DISK CACHE (temperature only) ──────────────────────────────────────────────
# The BMS is the hard bottleneck (~13s/request), so we pay the full fetch ONCE
# and persist every q6h temperature value to a local SQLite file. On the next
# run the whole building loads from disk in seconds and no BMS request is made.
#
#   cells(room_id, ci, temp)        one temperature value per 6-hour cell
#   done_ranges(room_id, lo, hi)    which q6h cell ranges have been fetched
#                                   (so empty rooms/periods aren't refetched, and
#                                    the cache survives a CHUNK_Q6H change)
#   meta(k, v)                      grid signature; cache is cleared if it changes
CACHE_PATH = pathlib.Path(__file__).parent / "lab42_temp_cache.sqlite"
_cache_conn = sqlite3.connect(CACHE_PATH, check_same_thread=False)
_cache_conn.execute("PRAGMA journal_mode=WAL")
_cache_conn.execute("PRAGMA synchronous=NORMAL")
_cache_conn.execute("CREATE TABLE IF NOT EXISTS meta (k TEXT PRIMARY KEY, v TEXT)")
_cache_conn.execute("CREATE TABLE IF NOT EXISTS cells "
                    "(room_id INT, ci INT, temp REAL, PRIMARY KEY(room_id, ci))")
_cache_conn.execute("CREATE TABLE IF NOT EXISTS done_ranges (room_id INT, lo INT, hi INT)")
_cache_conn.commit()
_cache_lock = threading.Lock()


def _merge_ranges(rs):
    rs = sorted(rs)
    out = []
    for lo, hi in rs:
        if out and lo <= out[-1][1]:
            out[-1] = (out[-1][0], max(out[-1][1], hi))
        else:
            out.append((lo, hi))
    return out


def _covered(merged, lo, hi):
    for a, b in merged:
        if a <= lo and hi <= b:
            return True
    return False


def _cache_check_grid():
    """cells are keyed by ci, valid only while the q6h time grid is unchanged."""
    want = f"{DATA_START_MS}:{DATA_END_MS}"
    cur = _cache_conn.execute("SELECT v FROM meta WHERE k='grid'").fetchone()
    have = cur[0] if cur else None
    if have == want:
        return
    if have is not None:
        print(f"  cache grid changed ({have} -> {want}); clearing cache")
        _cache_conn.execute("DELETE FROM cells")
        _cache_conn.execute("DELETE FROM done_ranges")
    _cache_conn.execute("INSERT OR REPLACE INTO meta VALUES('grid', ?)", (want,))
    _cache_conn.commit()


def _cache_load():
    """Load persisted cells into _q6h and mark already-fetched chunks done."""
    _cache_check_grid()
    n = 0
    for rid, ci, temp in _cache_conn.execute("SELECT room_id, ci, temp FROM cells"):
        if rid in ROOM_MAP:
            _q6h.setdefault(rid, {})[ci] = [temp]
            n += 1
    ranges = defaultdict(list)
    for rid, lo, hi in _cache_conn.execute("SELECT room_id, lo, hi FROM done_ranges"):
        ranges[rid].append((lo, hi))
    merged = {rid: _merge_ranges(rs) for rid, rs in ranges.items()}
    nd = 0
    for rid in ROOM_MAP:
        rs = merged.get(rid)
        if not rs:
            continue
        for ck in range(N_Q6H_CHUNKS):
            lo = ck * CHUNK_Q6H
            hi = min(lo + CHUNK_Q6H, N_Q6H)
            if _covered(rs, lo, hi):
                _done.add((rid, ck))
                nd += 1
    print(f"  cache loaded: {n} cells, {nd}/{len(ROOM_MAP) * N_Q6H_CHUNKS} chunks already done")
    if nd == len(ROOM_MAP) * N_Q6H_CHUNKS:
        print("  cache is COMPLETE — serving entirely from disk, no BMS fetch needed")
    if n == 0 and CACHE_ONLY:
        print("  !! CACHE EMPTY and CACHE_ONLY=True — the map will be blank.")
        print("  !! Run:  python lab42_cache_loader.py   to build the cache first.")


def _cache_write_chunk(room_id: int, lo: int, hi: int, bucket: dict):
    """Persist a fetched chunk's cells and record its range as done."""
    with _cache_lock:
        if bucket:
            _cache_conn.executemany(
                "INSERT OR REPLACE INTO cells VALUES (?,?,?)",
                [(room_id, ci, row[0]) for ci, row in bucket.items() if row[0] is not None])
        _cache_conn.execute("INSERT INTO done_ranges VALUES (?,?,?)", (room_id, lo, hi))
        _cache_conn.commit()

# ── IO STATS (for tuning CHUNK_Q6H / FETCH_WORKERS) ─────────────────────────────
_io_lock      = threading.Lock()
_io_chunks    = 0      # completed chunk fetches
_io_secs      = 0.0    # cumulative request seconds (summed across workers)
_io_pages     = 0      # cumulative upstream pages pulled
_io_t0        = None   # wall-clock of first fetch
_TOTAL_JOBS   = None   # set after ROOM_MAP known


def _record_io(secs: float, pages: int):
    global _io_chunks, _io_secs, _io_pages, _io_t0
    with _io_lock:
        if _io_t0 is None:
            _io_t0 = time.time()
        _io_chunks += 1
        _io_secs   += secs
        _io_pages  += pages
        n = _io_chunks
        if n % 200 == 0:
            elapsed   = max(1e-3, time.time() - _io_t0)
            wall_rate = n / elapsed                       # chunks/sec (all workers)
            avg_ms    = 1000 * _io_secs / n               # mean latency per request
            ppc       = _io_pages / n                     # pages per chunk
            total     = _TOTAL_JOBS or n
            remaining = max(0, total - n)
            eta_min   = (remaining / wall_rate) / 60 if wall_rate > 0 else 0
            print(f"  [io] {n}/{total} chunks  {avg_ms:.0f} ms/req  "
                  f"{ppc:.2f} pages/chunk  {wall_rate:.1f} chunks/s  "
                  f"ETA ~{eta_min:.1f} min")


_TOTAL_JOBS = len(ROOM_MAP) * N_Q6H_CHUNKS


# ── FETCH PIPELINE ────────────────────────────────────────────────────────────
def _fetch_binned_range(room_id: int, start_ms: int, end_ms: int, n_bins: int):
    """
    Fetch binned data from the upstream API with simple token-refresh on 401.
    Returns (records, pages_pulled).
    """
    global TOKEN, HEADERS
    base_params = {"startTime": _iso(start_ms), "endTime": _iso(end_ms), "bins": n_bins}
    recs, page, total, pages = [], 1, 1, 0
    while page <= total and page <= 50:
        params = {**base_params, "page": page}
        r = SESSION.get(f"{BASE_URL}/rooms/{room_id}/data",
                        params=params, timeout=90)

        if r.status_code == 401:
            # Token expired: refresh once, then retry this page
            try:
                print("  401 from BMS API; refreshing token...")
                TOKEN = get_token()
                HEADERS = {"Authorization": f"Bearer {TOKEN}"}
                SESSION.headers.update(HEADERS)
                r = SESSION.get(f"{BASE_URL}/rooms/{room_id}/data",
                                params=params, timeout=90)
            except Exception as e:
                print(f"  re-auth for room {room_id} failed: {e}")
                r.raise_for_status()

        r.raise_for_status()
        body = r.json()
        recs += body.get("results", [])
        total = body.get("pagination", {}).get("totalPages", 1)
        page += 1
        pages += 1
    return recs, pages


def _cell_index(tier: str, ms: int) -> int:
    """Index of the cell whose range contains ms (−1 if before the first cell)."""
    return bisect_right(CELL_STARTS[tier], ms) - 1


def _fetch_room_q6h_chunk(room_id: int, chunk_idx: int) -> None:
    """
    Fetch one chunk of CHUNK_Q6H consecutive 6-hour cells for a room and store
    the raw per-sensor q6h values. We do NOT forward/back-fill gaps here: true
    gaps stay gaps, so derived avg/max/min only reflect real readings.
    """
    if room_id not in ROOM_MAP:
        return

    lo = chunk_idx * CHUNK_Q6H
    hi = min(lo + CHUNK_Q6H, N_Q6H)
    if lo >= hi:
        return

    start_ms = CELLS["q6h"][lo][0]
    end_ms   = CELLS["q6h"][hi - 1][1]
    n_bins   = hi - lo

    try:
        _t = time.time()
        recs, pages = _fetch_binned_range(room_id, start_ms, end_ms, n_bins)
        _record_io(time.time() - _t, pages)
    except Exception as e:
        print(f"  room {room_id} q6h chunk {chunk_idx} failed: {e}")
        raise   # let the worker leave it un-done so it can be retried

    # Bucket each record into its q6h cell, per sensor.
    bucket: dict[int, list] = {}   # ci -> [v_si...]
    for rec in recs:
        ts = rec.get("timestamp")
        try:
            ms = int(datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp() * 1000)
        except Exception:
            continue
        ci = bisect_right(_Q6H_STARTS, ms) - 1
        if ci < lo or ci >= hi:
            continue
        row = bucket.get(ci)
        if row is None:
            row = [None] * len(SENSORS)
            bucket[ci] = row
        for si, s in enumerate(SENSORS):
            v = rec.get(s["key"])
            if v is None:
                continue
            try:
                v_f = round(float(v), 2)
            except (ValueError, TypeError):
                continue
            if v_f == 0.0 and s["key"] in _FILTER_ZEROS:
                continue
            row[si] = v_f

    # Persist this chunk to disk — including empty ones, so a room/period with no
    # data is recorded as fetched and never requested from the BMS again.
    _cache_write_chunk(room_id, lo, hi, bucket)

    if not bucket:
        return

    global _data_version
    with _q6h_lock:
        room_cells = _q6h.setdefault(room_id, {})
        for ci, row in bucket.items():
            room_cells[ci] = row
        _data_version += 1


# ── AGGREGATION (q6h -> any tier) ──────────────────────────────────────────────
def _agg_value(vals: list, agg: str):
    """Aggregate a list of (non-None) numbers as avg / max / min."""
    if not vals:
        return None
    if agg == "max":
        return round(max(vals), 2)
    if agg == "min":
        return round(min(vals), 2)
    return round(sum(vals) / len(vals), 2)   # avg (default)


def _build_slice_values(tier: str, ci_min: int, ci_max: int, si: int,
                        agg: str, visible_floors: set) -> dict:
    """
    Build {ci: {floor: {room: value}}} for the requested coarse cells by
    aggregating each cell's member q6h values from _q6h.
    """
    members = Q6H_MEMBERS[tier]
    out: dict = {}

    # Snapshot the q6h store under the lock, then aggregate without holding it.
    with _q6h_lock:
        snapshot = {rid: cells for rid, cells in _q6h.items()}

    for ci in range(ci_min, ci_max + 1):
        lo, hi = members[ci]
        if hi <= lo:
            continue
        cell_out: dict = {}
        for rid, (bfloor, rkey) in ROOM_MAP.items():
            if bfloor not in visible_floors:
                continue
            room_cells = snapshot.get(rid)
            if not room_cells:
                continue
            vals = []
            for qci in range(lo, hi):
                row = room_cells.get(qci)
                if row is not None:
                    v = row[si]
                    if v is not None:
                        vals.append(v)
            if not vals:
                continue
            v_agg = _agg_value(vals, agg)
            if v_agg is None:
                continue
            cell_out.setdefault(bfloor, {})[rkey] = v_agg
        if cell_out:
            out[ci] = cell_out
    return out


# Memoize aggregated slices by (tier, agg, si, ci_min, ci_max, floors) against the
# current _data_version. While loading, the version bumps often so this mostly
# misses (every poll has new data, as expected). Once a view settles — or the user
# pans back over already-loaded cells — repeated identical requests return instantly
# instead of re-scanning ~1.8M q6h values for the year view on every poll.
_slice_cache: dict = {}
_slice_cache_lock = threading.Lock()
_SLICE_CACHE_MAX = 256


def _build_slice_cached(tier, ci_min, ci_max, si, agg, visible_floors):
    key = (tier, agg, si, ci_min, ci_max, tuple(sorted(visible_floors)))
    ver = _data_version
    with _slice_cache_lock:
        hit = _slice_cache.get(key)
        if hit is not None and hit[0] == ver:
            return hit[1]
    out = _build_slice_values(tier, ci_min, ci_max, si, agg, visible_floors)
    with _slice_cache_lock:
        if len(_slice_cache) > _SLICE_CACHE_MAX:
            _slice_cache.clear()
        _slice_cache[key] = (ver, out)
    return out


# ── PRIORITY FETCH QUEUE ──────────────────────────────────────────────────────
import heapq

PRIORITY_FG = 0
PRIORITY_BG = 1

Job = tuple  # (room_id, chunk_idx)   -- chunk_idx indexes q6h chunks

_pq: list = []
_pq_lock = threading.Lock()
_pq_seq  = 0

_fetching: set = set()
_done:     set = set()
_fetch_sets_lock = threading.Lock()

# job -> lowest priority currently sitting in the heap. Prevents the same job
# being queued thousands of times when the client re-polls the year view, while
# still allowing a background job to be re-queued at foreground priority.
_queued: dict = {}

_workers_alive = True
_pq_event = threading.Event()


def _pq_submit(job: Job, priority: int) -> bool:
    global _pq_seq
    with _fetch_sets_lock:
        if job in _done or job in _fetching:
            return False
    with _pq_lock:
        cur = _queued.get(job)
        if cur is not None and cur <= priority:
            return False          # already queued at the same or better priority
        _queued[job] = priority
        _pq_seq += 1
        heapq.heappush(_pq, (priority, _pq_seq, job))
    _pq_event.set()
    return True


def _worker():
    global _loading_count
    while _workers_alive:
        _pq_event.wait(timeout=1.0)
        while True:
            with _pq_lock:
                if not _pq:
                    _pq_event.clear()
                    break
                priority, _, job = heapq.heappop(_pq)
                _queued.pop(job, None)

            with _fetch_sets_lock:
                if job in _done or job in _fetching:
                    continue
                _fetching.add(job)

            with _loading_lock:
                _loading_count += 1

            try:
                room_id, chunk_idx = job
                _fetch_room_q6h_chunk(room_id, chunk_idx)
                # Only mark as done if we actually attempted the fetch without exception
                with _fetch_sets_lock:
                    _done.add(job)
            except Exception as e:
                # Do NOT mark as done here; allow retries on the next /api/slice poll
                print(f"  job {job} failed in worker: {e}")
            finally:
                with _fetch_sets_lock:
                    _fetching.discard(job)
                with _loading_lock:
                    _loading_count -= 1


print("Loading disk cache...")
_cache_load()

_worker_threads = [threading.Thread(target=_worker, daemon=True, name=f"fetch-{i}")
                   for i in range(FETCH_WORKERS)]
for t in _worker_threads:
    t.start()
print(f"Priority fetch queue started with {FETCH_WORKERS} workers")


def _enqueue_jobs(jobs: list, priority: int) -> int:
    return sum(1 for j in jobs if _pq_submit(j, priority))


# ── PREFETCH ALL (load whole building, never unload) ───────────────────────────
def _prefetch_all_q6h():
    """Enqueue every room's full q6h range at background priority.

    Ordering matters a lot for perceived speed: we go newest chunk first and
    interleave across ALL rooms, so the most recent data fills in for the whole
    building before any older history is fetched. (The old room-by-room,
    oldest-first order made some rooms fully load while others stayed blank, and
    left the recently-viewed data for last.)
    """
    rids = list(ROOM_MAP)
    jobs = [(rid, ck)
            for ck in range(N_Q6H_CHUNKS - 1, -1, -1)   # newest 6h chunk first
            for rid in rids]                              # all rooms per chunk
    n = _enqueue_jobs(jobs, PRIORITY_BG)
    print(f"Prefetch enqueued: {n} q6h chunks "
          f"({len(ROOM_MAP)} rooms × {N_Q6H_CHUNKS} chunks, newest-first)")


if PREFETCH_ALL and not CACHE_ONLY:
    threading.Thread(target=_prefetch_all_q6h, daemon=True,
                     name="prefetch-all").start()


# ── FASTAPI ───────────────────────────────────────────────────────────────────
SCRIPT_DIR = pathlib.Path(__file__).parent
app = FastAPI()


@app.get("/api/meta")
def api_meta():
    tier_cells = {
        tier: {
            "starts": [c[0] for c in cells],
            "ends":   [c[1] for c in cells],
            "labels": [c[2] for c in cells],
        }
        for tier, cells in CELLS.items()
    }
    return {
        "sensors":    SENSORS,
        "rooms":      {str(k): v for k, v in ROOMS.items()},
        "floors":     FLOORS_LIST,
        "data_start": DATA_START_MS,
        "data_end":   DATA_END_MS,
        "tiers":      TIER_ORDER,
        "cells":      tier_cells,
        "aggs":       ["avg", "max", "min"],
    }


@app.get("/api/slice")
def api_slice(
    tier:    str = Query("year"),
    agg:     str = Query("avg"),
    ci_min:  int = Query(0),
    ci_max:  int = Query(0),
    si:      int = Query(0),
    floors:  str = Query(""),
):
    if tier not in CELLS:
        tier = "year"
    if agg not in ("avg", "max", "min"):
        agg = "avg"
    n_cells = len(CELLS[tier])
    ci_min  = max(0, min(ci_min, n_cells - 1))
    ci_max  = max(ci_min, min(ci_max, n_cells - 1))
    si      = max(0, min(si, len(SENSORS) - 1))

    visible_order: list[int] = []
    seen: set[int] = set()
    if floors:
        for f in floors.split(","):
            try:
                fl = int(f.strip())
            except ValueError:
                continue
            if fl not in seen:
                seen.add(fl)
                visible_order.append(fl)
    if not visible_order:
        visible_order = list(ROOMS.keys())
    visible_floors = set(visible_order)

    # Which q6h chunks are needed to cover the requested coarse cells?
    members = Q6H_MEMBERS[tier]
    q6h_lo = members[ci_min][0]
    q6h_hi = members[ci_max][1]
    if q6h_hi <= q6h_lo:
        chunk_idxs: range = range(0, 0)
    else:
        chunk_idxs = range(q6h_lo // CHUNK_Q6H,
                           (q6h_hi - 1) // CHUNK_Q6H + 1)

    # Enqueue any missing chunks for visible rooms at high priority, and count
    # how many of this slice's chunks are not yet fetched (client polls on that).
    # In CACHE_ONLY mode we never fetch from the BMS, so nothing is pending.
    pending = 0
    fg_jobs: list = []
    if not CACHE_ONLY:
        for fl in visible_order:
            for rid in FLOOR_ROOM_IDS.get(fl, []):
                for ck in chunk_idxs:
                    job = (rid, ck)
                    if job not in _done:
                        pending += 1
                        fg_jobs.append(job)
    if fg_jobs:
        _enqueue_jobs(fg_jobs, PRIORITY_FG)

    out = _build_slice_cached(tier, ci_min, ci_max, si, agg, visible_floors)

    with _fetch_sets_lock:
        g_done = len(_done)
    g_total = len(ROOM_MAP) * N_Q6H_CHUNKS
    with _io_lock:
        _ic = _io_chunks
        _elapsed = (time.time() - _io_t0) if _io_t0 else 0
    _rate = (_ic / _elapsed) if _elapsed > 0 else 0
    g_eta = round((g_total - g_done) / _rate / 60, 1) if _rate > 0 else None

    return {
        "tier":           tier,
        "agg":            agg,
        "si":             si,
        "values":         out,
        "pending":        pending,
        "visible_floors": sorted(visible_floors),
        "global_done":    g_done,
        "global_total":   g_total,
        "global_eta_min": g_eta,
    }


@app.get("/api/status")
def api_status():
    with _fetch_sets_lock:
        done    = len(_done)
        loading = len(_fetching)
    with _pq_lock:
        queued = len(_pq)
    n_vals = _store_count()
    total_jobs = len(ROOM_MAP) * N_Q6H_CHUNKS
    with _io_lock:
        io_chunks = _io_chunks
        avg_ms    = (1000 * _io_secs / io_chunks) if io_chunks else 0
        ppc       = (_io_pages / io_chunks) if io_chunks else 0
        elapsed   = (time.time() - _io_t0) if _io_t0 else 0
    wall_rate = (io_chunks / elapsed) if elapsed > 0 else 0
    eta_min   = ((total_jobs - done) / wall_rate / 60) if wall_rate > 0 else None
    return {
        "done_jobs":     done,
        "total_jobs":    total_jobs,
        "loading":       loading,
        "queued":        queued,
        "values":        n_vals,
        "avg_ms_per_req": round(avg_ms, 1),
        "pages_per_chunk": round(ppc, 2),
        "chunks_per_sec": round(wall_rate, 1),
        "eta_min":       round(eta_min, 1) if eta_min is not None else None,
    }


@app.get("/api/debug")
def api_debug():
    n_vals = _store_count()
    with _q6h_lock:
        rooms_loaded = len(_q6h)
        cells_loaded = sum(len(c) for c in _q6h.values())
        rooms_with_data = {rid for rid, cells in _q6h.items() if cells}
    with _fetch_sets_lock:
        done = len(_done)

    # Per-floor breakdown: coordinate boxes drawn vs. matched to a sensor vs.
    # actually holding data so far. This pinpoints whether blank rooms are a
    # matching problem (matched < boxes) or just not-yet-loaded (with_data < matched).
    per_floor = {}
    for fl in sorted(COORD_KEYS):
        matched_rids = FLOOR_ROOM_IDS.get(fl, [])
        with_data = sum(1 for rid in matched_rids if rid in rooms_with_data)
        miss = sorted(COORD_KEYS[fl] - MATCHED_KEYS.get(fl, set()))
        per_floor[fl] = {
            "coord_boxes":   len(COORD_KEYS[fl]),
            "matched":       len(matched_rids),
            "has_data":      with_data,
            "no_data_boxes": miss,          # drawn but no upstream sensor matched
        }

    return {
        "upstream_rooms":   len(ALL_ROOMS),
        "matched_rooms":    len(ROOM_MAP),
        "unmatched_upstream": len(UNMATCHED_UPSTREAM),
        "unmatched_sample": UNMATCHED_UPSTREAM[:25],
        "rooms_loaded":     rooms_loaded,
        "done_jobs":        done,
        "total_jobs":       len(ROOM_MAP) * N_Q6H_CHUNKS,
        "q6h_cells_stored": cells_loaded,
        "total_values":     n_vals,
        "n_q6h_cells":      N_Q6H,
        "chunks_per_room":  N_Q6H_CHUNKS,
        "per_floor":        per_floor,
        "cells_per_tier":   {t: len(CELLS[t]) for t in TIER_ORDER},
    }


import os

# Directories and filename patterns we'll try for each floor's background image.
_SKETCH_DIRS = [SCRIPT_DIR / "floors", SCRIPT_DIR, pathlib.Path.cwd() / "floors",
                pathlib.Path.cwd()]
_SKETCH_EXTS = ["png", "jpg", "jpeg", "webp", "PNG", "JPG"]


def _find_sketch(floor_num: int):
    names = [f"floor{floor_num}", f"Floor{floor_num}", f"floor_{floor_num}",
             f"floor {floor_num}", f"{floor_num}", f"f{floor_num}"]
    for d in _SKETCH_DIRS:
        for nm in names:
            for ext in _SKETCH_EXTS:
                p = d / f"{nm}.{ext}"
                if p.exists():
                    return p
    return None


# Log which floor images were found so a 404 is easy to diagnose.
_found = {fl: _find_sketch(fl) for fl in ROOMS.keys()}
_ok = [fl for fl, p in _found.items() if p]
_miss = [fl for fl, p in _found.items() if not p]
if _ok:
    print(f"Floor images found: {sorted(_ok)}  (e.g. {_found[_ok[0]]})")
if _miss:
    print(f"Floor images MISSING for floors {sorted(_miss)} — the map still works "
          f"(vector room boxes), just without the plan background.")
    print(f"  Put images named e.g. floor4.png in one of: "
          f"{[str(d) for d in _SKETCH_DIRS[:2]]}")


@app.get("/api/floorimg/{floor_num}")
def api_floorimg(floor_num: int):
    path = _find_sketch(floor_num)
    if not path:
        return Response(status_code=404)
    ext = path.suffix.lower().lstrip(".")
    mime = "image/jpeg" if ext in ("jpg", "jpeg") else f"image/{ext}"
    mtime = int(path.stat().st_mtime)
    return Response(path.read_bytes(), media_type=mime,
                    headers={"Cache-Control":"no-cache,must-revalidate",
                             "ETag": str(mtime)})


@app.get("/")
def index():
    return HTMLResponse(HTML)


# ── HTML / JS ─────────────────────────────────────────────────────────────────
HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<title>LAB42 Map · Temperature (cached)</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{background:#0a0a18;font-family:'Segoe UI',sans-serif;overflow:hidden;display:flex;height:100vh;color:#fff}
#viewport{flex:1;overflow:hidden;position:relative}
#c{position:absolute;top:0;left:0;cursor:grab}
#c:active{cursor:grabbing}
#tooltip{position:fixed;background:rgba(10,10,26,.95);color:#fff;font-size:13px;padding:8px 14px;
         border-radius:8px;border:.5px solid rgba(255,255,255,.2);pointer-events:none;
         z-index:100;display:none;white-space:nowrap;line-height:1.6}
#sidebar{width:170px;flex-shrink:0;background:#12122a;border-left:.5px solid rgba(255,255,255,.08);
         padding:12px 10px;display:flex;flex-direction:column;gap:10px}
.sbt{font-size:8px;color:rgba(255,255,255,.3);font-weight:600;letter-spacing:.08em;text-transform:uppercase}
#status{font-size:10px;color:rgba(255,255,255,.5);line-height:1.6}
progress{width:100%;height:4px;margin-top:4px;border-radius:2px}
#hand-panel{width:100%;aspect-ratio:4/3;border-radius:8px;background:#0f0f1a;overflow:hidden}
.hint{font-size:9px;color:rgba(255,255,255,.4);line-height:1.9}
.hint b{color:rgba(255,255,255,.7)}
#agg-toggle{display:flex;gap:4px;margin-top:5px}
.agg-btn{flex:1;font-family:'Segoe UI',sans-serif;font-size:10px;font-weight:600;
         color:rgba(255,255,255,.55);background:#0f0f1a;border:.5px solid rgba(255,255,255,.12);
         border-radius:6px;padding:6px 0;cursor:pointer;transition:all .12s;text-align:center}
.agg-btn:hover{color:#fff;border-color:rgba(255,255,255,.3)}
.agg-btn.active{color:#0a0a18;background:#a5b4fc;border-color:#a5b4fc}
#cursor{position:fixed;width:14px;height:14px;border-radius:50%;border:2px solid #fff;
        pointer-events:none;z-index:200;transform:translate(-50%,-50%);display:none}
</style>
</head>
<body>
<div id="viewport"><canvas id="c"></canvas></div>
<div id="sidebar">
  <div>
    <div class="sbt">Status</div>
    <div id="status">Loading…</div>
    <progress id="prog" value="0" max="100"></progress>
  </div>
  <div>
    <div class="sbt">Value shown</div>
    <div id="agg-toggle">
      <button class="agg-btn active" data-agg="avg">Avg</button>
      <button class="agg-btn" data-agg="max">High</button>
      <button class="agg-btn" data-agg="min">Low</button>
    </div>
  </div>
  <div>
    <div class="sbt">Hand control</div>
    <div id="hand-panel"></div>
    <div class="hint" style="margin-top:6px">
      <b>Open hand</b> — hover<br>
      <b>Pinch + move</b> — pan<br>
      <b>Both hands up (1 open + 1 pinch)</b> — zoom timescale
    </div>
  </div>
  <div class="hint">
    <div class="sbt" style="margin-bottom:3px">Timescale</div>
    zoom out — years<br>
    zoom in — months → weeks<br>
    → days → 6-hour blocks<br>
    <div class="sbt" style="margin:6px 0 3px">Keys</div>
    ← → or A D — sensor<br>
    V — avg / high / low<br>
    scroll / drag — navigate
  </div>
</div>
<div id="tooltip"></div>
<div id="cursor"></div>

<script type="module">
const _mp = import("https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.3/vision_bundle.mjs");

// ── Layout constants ──────────────────────────────────────────────────────────
const CH = 380;
const FP_ASPECT = 500 / 380;
const COL_W = CH * FP_ASPECT;

const POLL_MS  = 1500;
const LOOKAHEAD_CELLS = 2;

const COL_PX_MIN = 150, COL_PX_BASE = 230, COL_PX_MAX = 470;
const SCALE_MIN = COL_PX_MIN / COL_W;
const SCALE_MAX = COL_PX_MAX / COL_W;
const PERROOM_CELL_PX = 150;

// ── State ─────────────────────────────────────────────────────────────────────
let tx=0, ty=0, scale=0.5;
let meta=null, sketches={}, hovered=null, activeSensor=0;
let velX=0, velY=0, rafMomentum=null, rafRender=null, dirty=true;
let tierIndex=0;
let activeTier='year';
let activeAgg='avg';

// ── Cache registry ─────────────────────────────────────────────────────────────
// The server holds ALL q6h data for every sensor, so switching sensor or agg
// never needs to discard anything — we just keep a separate client cache per
// (agg, sensorIndex) combination and swap which one `values`/`loadedCells`
// point at. Nothing ever unloads.
const _caches = {};   // key "agg|si" -> {values:{tier:{}}, loaded:{tier:Set}}
function _cacheKey(agg, si){ return agg + '|' + si; }
function _emptyCache(){
  return {
    values: { year:{}, month:{}, week:{}, day:{}, q6h:{} },
    loaded: { year:new Set(), month:new Set(), week:new Set(), day:new Set(), q6h:new Set() },
  };
}
function getCache(agg, si){
  const k = _cacheKey(agg, si);
  if(!_caches[k]) _caches[k] = _emptyCache();
  return _caches[k];
}

// `values` / `loadedCells` are live references into the active cache. selectCache()
// repoints them; all rendering / hit-testing code reads them unchanged.
let _active = getCache(activeAgg, 0);
let values = _active.values;
let loadedCells = _active.loaded;
function selectCache(){
  _active = getCache(activeAgg, activeSensor);
  values = _active.values;
  loadedCells = _active.loaded;
}

const vp  = document.getElementById('viewport');
const c   = document.getElementById('c');
const ctx = c.getContext('2d');
const tip = document.getElementById('tooltip');
const statusEl = document.getElementById('status');
const progEl   = document.getElementById('prog');
const DPR = window.devicePixelRatio || 1;

// ── Canvas sizing ─────────────────────────────────────────────────────────────
function resize(){
  c.width  = vp.clientWidth  * DPR;
  c.height = vp.clientHeight * DPR;
  c.style.width  = vp.clientWidth  + 'px';
  c.style.height = vp.clientHeight + 'px';
  dirty=true; schedRender();
}
window.addEventListener('resize', resize);
requestAnimationFrame(resize);
const W = ()=> vp.clientWidth;
const H = ()=> vp.clientHeight;

// ── Cell layout ───────────────────────────────────────────────────────────────
function colX(ci){ return ci * COL_W; }
function worldW(){ return meta.cells[activeTier].starts.length * COL_W; }

function cellAtX(tier, x){
  const n = meta.cells[tier].starts.length;
  return Math.max(0, Math.min(n-1, Math.floor(x / COL_W)));
}
function cellForTime(tier, t){
  const s = meta.cells[tier].starts; let lo=0, hi=s.length-1, ans=0;
  while(lo<=hi){ const m=(lo+hi)>>1; if(s[m]<=t){ ans=m; lo=m+1; } else hi=m-1; }
  return ans;
}
function timeAtScreenX(sx){
  if(!meta) return 0;
  const wx=(sx-tx)/scale, ci=cellAtX(activeTier,wx), cells=meta.cells[activeTier];
  const frac=Math.max(0,Math.min(1,(wx-ci*COL_W)/COL_W));
  return cells.starts[ci] + frac*(cells.ends[ci]-cells.starts[ci]);
}
function worldXForTime(tier, t){
  const ci=cellForTime(tier,t), cells=meta.cells[tier];
  const span=Math.max(1, cells.ends[ci]-cells.starts[ci]);
  const frac=Math.max(0,Math.min(1,(t-cells.starts[ci])/span));
  return (ci+frac)*COL_W;
}

// ── Colour helpers ─────────────────────────────────────────────────────────────
function hexRgb(h){ return [parseInt(h.slice(1,3),16),parseInt(h.slice(3,5),16),parseInt(h.slice(5,7),16)]; }
function lerp(lo,hi,t){
  t=Math.max(0,Math.min(1,t));
  const a=hexRgb(lo), b=hexRgb(hi);
  return `rgb(${Math.round(a[0]+(b[0]-a[0])*t)},${Math.round(a[1]+(b[1]-a[1])*t)},${Math.round(a[2]+(b[2]-a[2])*t)})`;
}
// Viridis: perceptually-uniform colormap (dark purple -> blue -> green -> yellow),
// clearer and colour-blind-safe for reading temperature magnitude.
const VIRIDIS=[[68,1,84],[72,40,120],[62,74,137],[49,104,142],[38,130,142],
               [31,158,137],[53,183,121],[110,206,88],[181,222,43],[253,231,37]];
function viridisRGB(t){
  t=Math.max(0,Math.min(1,t));
  const x=t*(VIRIDIS.length-1), i=Math.floor(x), f=x-i;
  const a=VIRIDIS[i], b=VIRIDIS[Math.min(i+1,VIRIDIS.length-1)];
  return [Math.round(a[0]+(b[0]-a[0])*f),
          Math.round(a[1]+(b[1]-a[1])*f),
          Math.round(a[2]+(b[2]-a[2])*f)];
}
function sColor(s,v){
  const [r,g,b]=viridisRGB((v-s.vmin)/Math.max(s.vmax-s.vmin,1));
  return `rgb(${r},${g},${b})`;
}

// ── Pan / zoom ────────────────────────────────────────────────────────────────
function setTier(idx){
  idx = Math.max(0, Math.min(meta.tiers.length-1, idx));
  if(idx===tierIndex) return;
  tierIndex = idx;
  activeTier = meta.tiers[tierIndex];
}
function clamp(){
  if(!meta) return;
  if(scale<SCALE_MIN) scale=SCALE_MIN; if(scale>SCALE_MAX) scale=SCALE_MAX;
  const ww=worldW()*scale, wh=meta.floors.length*CH*scale;
  tx = ww<=W() ? 0 : Math.min(0, Math.max(tx, W()-ww));
  ty = wh<=H() ? 0 : Math.min(0, Math.max(ty, H()-wh));
}
function zoom(f,px,py){
  if(!meta){ return; }
  const anchorT = timeAtScreenX(px);
  const r0 = scale;
  let s = scale * f;

  while(s*COL_W > COL_PX_MAX && tierIndex < meta.tiers.length-1){
    setTier(tierIndex+1);
    s *= COL_PX_BASE / COL_PX_MAX;
  }
  while(s*COL_W < COL_PX_MIN && tierIndex > 0){
    setTier(tierIndex-1);
    s *= COL_PX_BASE / COL_PX_MIN;
  }
  scale = Math.max(SCALE_MIN, Math.min(SCALE_MAX, s));

  tx = px - worldXForTime(activeTier, anchorT) * scale;
  ty = py - (py - ty) * (scale / r0);
  clamp(); dirty=true; schedRender();
}

// ── Slice fetching ────────────────────────────────────────────────────────────
let sliceTimer=null;
function schedSlice(){ clearTimeout(sliceTimer); sliceTimer=setTimeout(fetchSlice, 150); }

async function fetchSlice(){
  if(!meta) return;
  const tier = activeTier;
  const visL=(-tx)/scale, visR=(W()-tx)/scale;
  const visT=(-ty)/scale, visB=(H()-ty)/scale;

  const nCells = meta.cells[tier].starts.length;
  const ciMin = Math.max(0, cellAtX(tier, visL) - LOOKAHEAD_CELLS);
  const ciMax = Math.min(nCells-1, cellAtX(tier, visR) + LOOKAHEAD_CELLS);
  const si    = activeSensor;

  const fiMin = Math.max(0, Math.floor(visT/CH));
  const fiMax = Math.min(meta.floors.length-1, Math.ceil(visB/CH));
  const visibleFloors = meta.floors.slice(fiMin, fiMax+1).join(',');

  try {
    const r = await fetch(
      `/api/slice?tier=${tier}&agg=${activeAgg}&ci_min=${ciMin}&ci_max=${ciMax}&si=${si}&floors=${visibleFloors}`
    );
    if(!r.ok) return;
    const d = await r.json();
    mergeSlice(d);
    updateStatus(d);
    if(d.pending > 0) startPoll();
  } catch(e){ console.warn('slice failed', e); }
}

function mergeSlice(d){
  const tier = d.tier || activeTier;
  // Route the response into the cache it was requested for, not necessarily the
  // currently-active one (the user may have switched sensor/agg mid-flight).
  const agg = d.agg || activeAgg;
  const si  = (d.si !== undefined) ? d.si : activeSensor;
  const cache = getCache(agg, si);
  const store = cache.values[tier], loaded = cache.loaded[tier];
  let added = 0;
  for(const [ciStr, byFloor] of Object.entries(d.values||{})){
    const ci = +ciStr;
    if(!store[ci]) store[ci]={};
    for(const [flStr, byRoom] of Object.entries(byFloor)){
      const fl = +flStr;
      if(!store[ci][fl]) store[ci][fl]={};
      for(const [k,v] of Object.entries(byRoom)){
        if(store[ci][fl][k] !== v){ store[ci][fl][k]=v; added++; }
      }
    }
    if(Object.keys(byFloor).length > 0) loaded.add(ci);
  }
  // Only repaint if this response is for what's on screen right now.
  if(added > 0 && agg===activeAgg && si===activeSensor){ dirty=true; schedRender(); }
}

let lastLine1='Loading…';
let globalLine='';
let buildingDone=false;

function renderStatus(){
  statusEl.textContent = lastLine1 + (globalLine ? `\n${globalLine}` : '');
}
function setGlobal(gd, gt, eta){
  if(gt && gt>0){
    const pct = Math.min(100, Math.round(100*gd/gt));
    progEl.value = pct;
    if(pct>=100){ globalLine='building loaded ✓'; buildingDone=true; }
    else globalLine = `building ${pct}%` + (eta!=null ? ` · ~${eta} min left` : '');
  }
}
function updateStatus(d){
  // Ignore stale responses for a sensor/agg the user has already switched away from.
  const agg = d.agg || activeAgg;
  const si  = (d.si !== undefined) ? d.si : activeSensor;
  if(agg!==activeAgg || si!==activeSensor) return;
  const tierLbl = (d.tier||activeTier);
  if(d.pending > 0){
    lastLine1 = `Loading ${d.pending} cells… (${tierLbl})`;
  } else {
    const store = values[tierLbl]||{};
    const n = Object.values(store).flatMap(c=>Object.values(c).flatMap(f=>Object.values(f))).length;
    lastLine1 = `${n} values (${tierLbl})`;
  }
  setGlobal(d.global_done, d.global_total, d.global_eta_min);
  renderStatus();
}

// Lightweight global progress poller. Hits /api/status (no aggregation) so it
// can run while the whole building loads in the background without re-scanning
// the q6h store on every tick.
const GLOBAL_POLL_MS = 3000;
let gpollTimer=null;
async function globalPoll(){
  if(buildingDone) return;
  try {
    const r = await fetch('/api/status');
    if(r.ok){
      const s = await r.json();
      setGlobal(s.done_jobs, s.total_jobs, s.eta_min);
      renderStatus();
    }
  } catch(e){}
  if(!buildingDone){ clearTimeout(gpollTimer); gpollTimer=setTimeout(globalPoll, GLOBAL_POLL_MS); }
}

let pollTimer=null;
function startPoll(){
  clearTimeout(pollTimer);
  pollTimer=setTimeout(fetchSlice, POLL_MS);
}

// ── Hit testing ───────────────────────────────────────────────────────────────
function worldXY(sx,sy){ return [(sx-tx)/scale,(sy-ty)/scale]; }

function hitRoom(wx,wy){
  if(!meta) return null;
  const fi=Math.floor(wy/CH);
  if(fi<0||fi>=meta.floors.length) return null;
  const floor=meta.floors[fi];

  const tier=activeTier, cells=meta.cells[tier];
  const ci=cellAtX(tier, wx);
  if(ci<0||ci>=cells.starts.length) return null;
  const cx0=colX(ci), cy=fi*CH;
  if(wx<cx0||wx>cx0+COL_W) return null;

  const rooms=meta.rooms[String(floor)]||{};
  const fv=((values[tier][ci]||{})[floor])||{};
  for(const [num,pos] of Object.entries(rooms)){
    if(wx>=cx0+pos.x*COL_W && wx<=cx0+(pos.x+pos.w)*COL_W &&
       wy>=cy +pos.y*CH    && wy<=cy +(pos.y+pos.h)*CH){
      return {ci,floor,num,val:fv[num],sensor:meta.sensors[activeSensor],
              period:cells.labels[ci]};
    }
  }
  return null;
}

// ── Mouse input ───────────────────────────────────────────────────────────────
let dragging=false,lx=0,ly=0;
c.addEventListener('mousedown',e=>{
  dragging=true;lx=e.clientX;ly=e.clientY;velX=0;velY=0;
  if(rafMomentum){cancelAnimationFrame(rafMomentum);rafMomentum=null;}
});
window.addEventListener('mousemove',e=>{
  if(!dragging)return;
  const dx=e.clientX-lx, dy=e.clientY-ly;
  velX=dx;velY=dy;tx+=dx;ty+=dy;lx=e.clientX;ly=e.clientY;
  clamp();dirty=true;schedRender();schedSlice();
});
window.addEventListener('mouseup',()=>{
  dragging=false;
  (function step(){velX*=.9;velY*=.9;tx+=velX;ty+=velY;clamp();dirty=true;schedRender();
    if(Math.abs(velX)>.5||Math.abs(velY)>.5) rafMomentum=requestAnimationFrame(step);
    else schedSlice();})();
});
let _wpend=false,_wdelta=0,_wx=0,_wy=0;
c.addEventListener('wheel',e=>{
  e.preventDefault();_wdelta+=e.deltaY;_wx=e.clientX;_wy=e.clientY;
  if(!_wpend){_wpend=true;requestAnimationFrame(()=>{
    zoom(_wdelta<0?1.12:1/1.12,_wx,_wy);_wdelta=0;_wpend=false;schedSlice();
  });}
},{passive:false});
c.addEventListener('mousemove',e=>{
  const [wx,wy]=worldXY(e.clientX,e.clientY);
  const h=hitRoom(wx,wy);
  if(JSON.stringify(h)!==JSON.stringify(hovered)){hovered=h;dirty=true;schedRender();}
  if(h?.val!==undefined){
    tip.style.display='block';
    tip.style.left=(e.clientX+16)+'px';tip.style.top=(e.clientY-10)+'px';
    tip.innerHTML=`<b>Floor ${h.floor} · Room ${h.num}</b><br>`+
      `${h.sensor.name}: ${h.val.toFixed(1)}\u202f${h.sensor.unit}<br>`+
      `<span style="opacity:.5">${h.period}</span>`;
  } else tip.style.display='none';
});
c.addEventListener('mouseleave',()=>{tip.style.display='none';hovered=null;dirty=true;schedRender();});
window.handDrag=(dx,dy)=>{tx+=dx;ty+=dy;clamp();dirty=true;schedRender();schedSlice();};
window.handZoom=f=>{zoom(f,W()/2,H()/2);schedSlice();};

window.nextSensor=()=>{
  if(!meta)return;
  activeSensor=(activeSensor+1)%meta.sensors.length;
  selectCache();            // swap to this sensor's cache (no wipe, no refetch from API)
  dirty=true;schedRender();fetchSlice();
};
window.prevSensor=()=>{
  if(!meta)return;
  activeSensor=(activeSensor+meta.sensors.length-1)%meta.sensors.length;
  selectCache();
  dirty=true;schedRender();fetchSlice();
};

// ── Aggregation toggle (avg / high / low) ──────────────────────────────────────
const _AGGS=['avg','max','min'];
function setAgg(agg){
  if(!meta || agg===activeAgg) return;
  activeAgg=agg;
  selectCache();
  document.querySelectorAll('.agg-btn').forEach(b=>
    b.classList.toggle('active', b.dataset.agg===agg));
  dirty=true;schedRender();fetchSlice();
}
window.cycleAgg=()=>{ setAgg(_AGGS[(_AGGS.indexOf(activeAgg)+1)%_AGGS.length]); };
document.querySelectorAll('.agg-btn').forEach(b=>
  b.addEventListener('click',()=>setAgg(b.dataset.agg)));

window.addEventListener('keydown',e=>{
  if(!meta)return;
  if(e.key==='ArrowRight'||e.key==='d'||e.key==='D') window.nextSensor();
  else if(e.key==='ArrowLeft'||e.key==='a'||e.key==='A') window.prevSensor();
  else if(e.key==='v'||e.key==='V') window.cycleAgg();
});

// ── Render ────────────────────────────────────────────────────────────────────
function schedRender(){ if(!rafRender) rafRender=requestAnimationFrame(render); }

function render(){
  rafRender=null;
  if(!c.width||!c.height){requestAnimationFrame(()=>{resize();schedRender();});return;}
  ctx.setTransform(1,0,0,1,0,0);
  ctx.clearRect(0,0,c.width,c.height);
  ctx.setTransform(DPR,0,0,DPR,0,0);

  if(!meta){
    ctx.fillStyle='rgba(255,255,255,.5)';ctx.font='16px Segoe UI';
    ctx.textAlign='center';ctx.fillText('Loading…',W()/2,H()/2);return;
  }

  const NF=meta.floors.length;
  const si=activeSensor;
  const sensor=meta.sensors[si];

  const tier=activeTier;
  const cells=meta.cells[tier];
  const tierVals=values[tier], tierLoaded=loadedCells[tier];

  const visL=(-tx)/scale, visT=(-ty)/scale;
  const visR=(W()-tx)/scale, visB=(H()-ty)/scale;
  const ciMin=Math.max(0, cellAtX(tier, visL));
  const ciMax=Math.min(cells.starts.length-1, cellAtX(tier, visR));
  const fiMin=Math.max(0,Math.floor(visT/CH));
  const fiMax=Math.min(NF-1,Math.ceil(visB/CH));

  ctx.save();
  ctx.translate(tx,ty); ctx.scale(scale,scale);
  ctx.imageSmoothingEnabled=true; ctx.imageSmoothingQuality='high';

  for(let fi=fiMin;fi<=fiMax;fi++){
    const floor=meta.floors[fi];
    const rowY=fi*CH;

    for(let ci=ciMin;ci<=ciMax;ci++){
      const cx=colX(ci);
      const cellPx=COL_W*scale;
      const fv=(tierVals[ci]||{})[floor]||{};
      const rooms=meta.rooms[String(floor)]||{};
      const sketch=sketches[floor];

      if(sketch) ctx.drawImage(sketch,cx,rowY,COL_W,CH);
      else { ctx.fillStyle='#0d0d1f'; ctx.fillRect(cx,rowY,COL_W,CH); }

      if(cellPx < PERROOM_CELL_PX){
        const vals=Object.values(fv);
        if(vals.length){
          ctx.globalAlpha=0.55;
          ctx.fillStyle=sColor(sensor, vals.reduce((a,b)=>a+b,0)/vals.length);
          ctx.fillRect(cx,rowY,COL_W,CH); ctx.globalAlpha=1;
        } else if(!tierLoaded.has(ci)){
          ctx.globalAlpha=0.06; ctx.fillStyle='#fff';
          ctx.fillRect(cx,rowY,COL_W,CH); ctx.globalAlpha=1;
        }
      } else {
        const isLoaded = tierLoaded.has(ci);
        for(const [num,pos] of Object.entries(rooms)){
          const val=fv[num];
          const rx=cx+pos.x*COL_W, ry=rowY+pos.y*CH, rw=pos.w*COL_W, rh=pos.h*CH;

          if(val===undefined){
            if(!isLoaded){
              ctx.globalAlpha=0.08; ctx.fillStyle='#fff';
              ctx.fillRect(rx,ry,rw,rh); ctx.globalAlpha=1;
            }
            continue;
          }

          const isHov = hovered?.floor===floor && hovered?.num===num && hovered?.ci===ci;
          ctx.globalAlpha = isHov ? 0.90 : 0.68;
          ctx.fillStyle = sColor(sensor,val);
          ctx.fillRect(rx,ry,rw,rh);
          if(isHov){
            ctx.globalAlpha=1; ctx.strokeStyle='#fff'; ctx.lineWidth=2/scale;
            ctx.strokeRect(rx,ry,rw,rh);
          }
          ctx.globalAlpha=1;

          const fs=Math.min(rh*0.5, rw/Math.max(1.6, num.length*0.62));
          if(fs*scale >= 6){
            ctx.font=`600 ${fs}px Segoe UI`;
            ctx.textAlign='center'; ctx.textBaseline='middle';
            ctx.fillStyle='rgba(0,0,0,.72)';
            ctx.fillText(num, rx+rw/2, ry+rh/2);
            ctx.textAlign='left'; ctx.textBaseline='alphabetic';
          }
        }
      }
      ctx.strokeStyle='rgba(0,0,0,.10)'; ctx.lineWidth=.6/scale;
      ctx.strokeRect(cx,rowY,COL_W,CH);
    }
  }
  ctx.restore();

  ctx.font='bold 11px Segoe UI';
  for(let fi=fiMin;fi<=fiMax;fi++){
    const floor=meta.floors[fi];
    const sy=fi*CH*scale+ty;
    const lbl=`Floor ${floor}`;
    const tw=ctx.measureText(lbl).width;
    ctx.fillStyle='rgba(15,15,40,.85)'; ctx.fillRect(0,sy+2,tw+14,17);
    ctx.fillStyle='#e0e7ff'; ctx.fillText(lbl,7,sy+14);
    if(fi>fiMin){
      ctx.strokeStyle='rgba(80,80,200,.5)'; ctx.lineWidth=1;
      ctx.beginPath(); ctx.moveTo(0,sy); ctx.lineTo(W(),sy); ctx.stroke();
    }
  }

  ctx.font='10px Segoe UI';
  for(let ci=ciMin;ci<=ciMax;ci++){
    const sx=colX(ci)*scale+tx;
    if(sx>W()) continue;
    const lbl=cells.labels[ci];
    const cellPx=COL_W*scale;
    const tw=ctx.measureText(lbl).width;
    const emphasised=(tier==='year')
      ||(tier==='month'&&lbl.startsWith('Jan'))
      ||(tier==='day'&&lbl.startsWith('01 '))
      ||(tier==='q6h'&&lbl==='00:00');
    if(emphasised){
      ctx.fillStyle='rgba(79,70,229,.62)'; ctx.fillRect(Math.max(sx,0),0,tw+10,18);
      ctx.strokeStyle='rgba(165,180,252,.8)'; ctx.lineWidth=1.5;
    } else {
      ctx.fillStyle='rgba(15,15,40,.78)'; ctx.fillRect(Math.max(sx,0),0,tw+8,15);
      ctx.strokeStyle='rgba(255,255,255,.06)'; ctx.lineWidth=.5;
    }
    if(sx>=0){ ctx.beginPath(); ctx.moveTo(sx,emphasised?18:15); ctx.lineTo(sx,H()); ctx.stroke(); }
    ctx.fillStyle=emphasised?'#c7d2fe':'rgba(255,255,255,.75)';
    if(cellPx>tw+14 || ci===ciMin)
      ctx.fillText(lbl,Math.max(sx,0)+4,emphasised?13:12);
  }

  ctx.font='bold 11px Segoe UI'; ctx.textAlign='right';
  const tlbl={year:'Yearly',month:'Monthly',week:'Weekly',day:'Daily',q6h:'6-hour'}[tier]+' view';
  ctx.fillStyle='rgba(10,10,30,.78)';
  const tlw=ctx.measureText(tlbl).width;
  ctx.fillRect(W()-tlw-22,24,tlw+16,18);
  ctx.fillStyle='#c7d2fe'; ctx.fillText(tlbl,W()-12,37);
  ctx.textAlign='left';

  const bW=160,bH=8,bX=W()-bW-12,bY=H()-24;
  const grad=ctx.createLinearGradient(bX,0,bX+bW,0);
  for(let i=0;i<VIRIDIS.length;i++){
    grad.addColorStop(i/(VIRIDIS.length-1),`rgb(${VIRIDIS[i][0]},${VIRIDIS[i][1]},${VIRIDIS[i][2]})`);
  }
  ctx.fillStyle='rgba(10,10,30,.78)'; ctx.fillRect(bX-4,bY-16,bW+8,bH+22);
  ctx.fillStyle=grad; ctx.fillRect(bX,bY,bW,bH);
  ctx.font='10px Segoe UI'; ctx.fillStyle='rgba(255,255,255,.8)';
  ctx.textAlign='left';  ctx.fillText(`${sensor.vmin}${sensor.unit}`,bX,bY+bH+12);
  ctx.textAlign='right'; ctx.fillText(`${sensor.vmax}${sensor.unit}`,bX+bW,bY+bH+12);
  ctx.textAlign='center'; ctx.font='bold 11px Segoe UI'; ctx.fillStyle='#fff';
  const _aggLbl={avg:'avg',max:'high',min:'low'}[activeAgg]||activeAgg;
  ctx.fillText(`${sensor.name} · ${_aggLbl}`,bX+bW/2,bY-4);
  ctx.textAlign='left';
}

// ── Floor images ──────────────────────────────────────────────────────────────
function loadSketches(floors){
  floors.forEach(f=>{
    const img=new Image();
    img.onload=()=>{ sketches[f]=img; dirty=true; schedRender(); };
    img.src=`/api/floorimg/${f}?v=${Date.now()}`;
  });
}

// ── Boot ──────────────────────────────────────────────────────────────────────
async function init(){
  try {
    const r=await fetch('/api/meta');
    meta=await r.json();
    loadSketches(meta.floors);
    tierIndex=0; activeTier=meta.tiers[0];
    scale=COL_PX_BASE/COL_W; tx=0; ty=0; clamp();
    dirty=true; schedRender();
    await fetchSlice();
    globalPoll();
  } catch(e){ statusEl.textContent='Failed to load metadata: '+e.message; }
}
init();

// ── Hand tracking ─────────────────────────────────────────────────────────────
const video=document.createElement('video');
video.autoplay=true;video.playsInline=true;video.muted=true;video.style.display='none';
document.body.appendChild(video);
const cur=document.getElementById('cursor');
let hc,hctx,det,lastSpan=null,pinching=false,dragged=false,curX=0,curY=0;
let posBuffer=[],fistWasDown=false,lastBtnClick=0,zoomMiss=0;
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
    const vis=await FilesetResolver.forVisionTasks(
      "https://cdn.jsdelivr.net/npm/@mediapipe/tasks-vision@0.10.3/wasm");
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
  if(!res?.landmarks?.length){cur.style.display='none';tip.style.display='none';return;}
  const du=new _DU(hctx);
  res.landmarks.forEach(lm=>{
    const m=lm.map(p=>({...p,x:1-p.x}));
    du.drawLandmarks(m,{color:'#00ff88',lineWidth:1,radius:2});
    du.drawConnectors(m,_HC||[],{color:'#00ff88',lineWidth:1});
  });
  // Zoom timescale: two hands, BOTH held up, ONE open + ONE pinching. Pinch keeps
  // the palm steady (a fist jerks the cursor), and requiring both hands raised
  // means a hand resting at your side won't trigger zoom even with a distant camera.
  const RAISE_Y=0.72;                       // palm must be in the upper ~72% of frame
  if(res.landmarks.length===2){
    const A=res.landmarks[0], B=res.landmarks[1];
    const pinchH=(h)=>dist(h[8],h[4])/(dist(h[0],h[9])||1e-4) < 0.45;   // scale-invariant
    const up=(h)=>((h[0].y+h[5].y+h[9].y+h[13].y+h[17].y)/5)<RAISE_Y;
    if(up(A)&&up(B)&&((isOpen(A)&&pinchH(B))||(isOpen(B)&&pinchH(A)))){
      const sp=dist(A[0],B[0]);
      if(lastSpan!==null && lastSpan>0){
        let f=sp/lastSpan;
        f=Math.max(0.92,Math.min(1.09,f));           // clamp any per-frame jump
        if(Math.abs(f-1)>0.012) window.handZoom(f);  // small deadzone kills jitter
      }
      lastSpan=(lastSpan===null)?sp:lastSpan*0.6+sp*0.4;   // smooth the span
      zoomMiss=0;
      cur.style.display='none';tip.style.display='none';return;
    }
  }
  if(lastSpan!==null && ++zoomMiss>4) lastSpan=null;   // keep across brief dropouts

  const lm=res.landmarks[0];
  // Track the PALM centroid (wrist + finger bases) for a steady cursor.
  const palm={x:(lm[0].x+lm[5].x+lm[9].x+lm[13].x+lm[17].x)/5,
              y:(lm[0].y+lm[5].y+lm[9].y+lm[13].y+lm[17].y)/5};
  const open=isOpen(lm);
  const fd=[[8,6],[12,10],[16,14],[20,18]].filter(([t,p])=>lm[t].y>lm[p].y).length;
  const isFist=(fd>=4);
  const isPinch=!isFist && (dist(lm[8],lm[4])/(dist(lm[0],lm[9])||1e-4) < 0.45); // pan / click
  // Input dead-margins: the palm only has to move within this central band of
  // the camera frame to cover the WHOLE screen. Bigger margins = smaller band =
  // more reach/amplification (corners come easily). Smaller = finer control.
  const MX=.24,MY_T=.32,MY_B=.16;
  const mx=Math.max(0,Math.min(1,((1-palm.x)-MX)/(1-2*MX)));
  const my=Math.max(0,Math.min(1,(palm.y-MY_T)/(1-MY_T-MY_B)));
  const[sx,sy]=smoothed(mx*window.innerWidth,my*window.innerHeight);
  cur.style.display='block';cur.style.left=sx+'px';cur.style.top=sy+'px';

  // UI mode: cursor over a sidebar button -> a pinch clicks it (hands-free select).
  const _uiEl=document.elementFromPoint(sx,sy);
  const _btn=(_uiEl&&_uiEl.closest)?_uiEl.closest('.agg-btn'):null;
  document.querySelectorAll('.agg-btn').forEach(b=>b.style.outline=(b===_btn?'2px solid #a5b4fc':''));
  if(_btn){
    if(isPinch && performance.now()-lastBtnClick>600){ _btn.click(); lastBtnClick=performance.now(); }
    cur.style.borderColor='#a5b4fc';cur.style.background='rgba(165,180,252,.3)';
    tip.style.display='none';pinching=false;
    hctx.beginPath();hctx.arc((1-palm.x)*hc.width,palm.y*hc.height,7,0,Math.PI*2);
    hctx.fillStyle='#a5b4fc';hctx.fill();
    return;
  }
  if(isPinch){
    // Pinch to pan.
    if(!pinching){pinching=true;dragged=false;posBuffer=[];}
    else{const dx=sx-curX,dy=sy-curY;if(Math.abs(dx)>3||Math.abs(dy)>3){window.handDrag(dx,dy);dragged=true;}}
    curX=sx;curY=sy;
    cur.style.borderColor='#fbbf24';cur.style.background='rgba(251,191,36,.25)';
    tip.style.display='none';
  } else if(open){
    if(pinching){pinching=false;dragged=false;}
    curX=sx;curY=sy;
    cur.style.borderColor='#00ff88';cur.style.background='rgba(0,255,136,.15)';
    const[wx,wy]=worldXY(sx,sy);
    const h=hitRoom(wx,wy);
    const pk=hovered?`${hovered.floor}|${hovered.num}|${hovered.ci}`:'';
    const nk=h?`${h.floor}|${h.num}|${h.ci}`:'';
    if(pk!==nk){hovered=h;dirty=true;schedRender();}
    if(h?.val!==undefined){
      tip.style.display='block';
      tip.style.left=Math.min(sx+18,window.innerWidth-210)+'px';
      tip.style.top=Math.max(sy-10,4)+'px';
      tip.innerHTML=`<b>Floor ${h.floor} · Room ${h.num}</b><br>`+
        `${h.sensor.name}: ${h.val.toFixed(1)}\u202f${h.sensor.unit}<br>`+
        `<span style="opacity:.5">${h.period}</span>`;
    } else tip.style.display='none';
  } else {
    pinching=false;dragged=false;
    cur.style.borderColor='rgba(255,255,255,.3)';cur.style.background='transparent';
    tip.style.display='none';
  }
  hctx.beginPath();hctx.arc((1-palm.x)*hc.width,palm.y*hc.height,6,0,Math.PI*2);
  hctx.fillStyle=isPinch?'#fbbf24':open?'#00ff88':'rgba(255,255,255,.4)';
  hctx.fill();
}
</script>
</body>
</html>
"""

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)