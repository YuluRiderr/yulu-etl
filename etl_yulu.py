"""
Yulu ETL — Sweep / Octopus / Stuck / Parts Summary
Fetches from Metabase, processes with pandas, pushes to Google Sheets.
Run daily via GitHub Actions at 1 AM IST.

PATCH NOTE (this version, BLR-only):
  Sweep, Stuck, and the external broken-bike sheet are all scoped to BLR
  only now (the broken-bike fetch used to also pull BOM/NCR/HYD tabs even
  though only BLR was processed downstream — that inflated the "missing
  bike_state_id" count with bikes that could never match, hiding the real
  BLR-only mismatches in the noise).
  Also added: the unmatched bike IDs are now printed explicitly (not just
  a count) wherever a broken-bike entry has no match in Sweep, so you can
  check those specific bike numbers in Metabase to find the real cause
  (decommissioned, wrong bike_category, typo, etc).

PATCH NOTE (this version, Octopus Type fix):
  process_octopus() used to map bike_group -> Type with a plain dict
  `.map()`. Any bike_group value not present as an exact key (e.g. a new
  variant like "DeX 3.5 GR" that was never added to the dict) silently
  became NaN, which clean_for_sheets() then turned into a blank cell in
  the 'Type' column — no warning, no error, easy to miss on the sheet.
  Now classify_bike_group() first checks the explicit dict (unchanged
  behaviour for all known values), and falls back to a prefix-based rule
  (DeX -> 2x/3x by version number, Express -> Express, Miracle ->
  Miracle) for anything new. Anything that STILL can't be classified is
  left blank as before, but now prints a warning listing exactly which
  bike_group values were unmapped, so it gets caught in the run log
  instead of silently going blank in the sheet.

PATCH NOTE (this version, Daily Ops Metrics added):
  New STEP F / process_daily_ops_metrics(): one row per BLR cluster per
  day in a new 'Daily_Ops_Metrics' tab, combining DAU (Demand Metrics,
  card 12245), service_swap/Attach fulfillment% + TAT (Token Flow, card
  11765), mechanic productivity for mechanics >90 days old (Mechanic Flow
  Testing, card 8966), and enquiry counts/conversion (Token Flow). Unlike
  every other step in this file (which full-replaces its tab), this one
  is a RUNNING LOG: delete_rows_for_date_and_append() deletes only
  yesterday's rows (if a prior run already wrote them) then appends fresh
  ones, so history accumulates across days instead of being overwritten.

  Two robustness fixes carried over from a proven production Metabase
  sync script (pm_sync_full.py), applied to the fetch path used by this
  new step specifically (not touched elsewhere in this file, to avoid
  changing behaviour of the already-working Sweep/Octopus/Stuck/Warehouse
  steps):
    1. ID/text columns (bike/user/token IDs, cluster/city/status names)
       are forced to string dtype at CSV-parse time via a dtype map, so
       pandas never round-trips them through float ("5038508.0") the way
       it silently can when left to infer dtypes.
    2. Metabase's /api/card/:id/query/csv endpoint can return HTTP 200
       with a query-execution error (e.g. a missing warehouse table)
       disguised as the CSV body — confirmed to happen in production.
       _looks_like_metabase_error_payload() sniffs for this before
       handing the response to pd.read_csv, which would otherwise parse
       the JSON error blob as a bogus but valid-looking 0-row DataFrame.
  Both fetches also retry transient errors (timeouts, dropped
  connections, 429/500/502/503) with exponential backoff via
  retry_on_api_error(), same pattern as that reference script.

PATCH NOTE (this version, Daily Ops Metrics backfill folded in):
  The standalone backfill_daily_ops_metrics.py script (and its own
  dedicated GitHub Actions workflow) is retired — this file now does both
  jobs, triggered by an optional CLI flag, so there is exactly one script
  and one workflow to maintain instead of two that could drift apart.

  compute_daily_ops_metrics_range()/refresh_daily_ops_metrics_range() are
  new: they fill in a whole [start_date, end_date] range of Daily_Ops_Metrics
  history in one run, fetching each of the 3 cards ONCE for the whole
  range instead of once per day. The per-date math itself
  (_compute_daily_ops_rows_for_date) is now a single shared function used
  by BOTH the daily run (process_daily_ops_metrics, one real Metabase call
  per card scoped to exactly yesterday) and the backfill (one bulk fetch,
  sliced locally per date) — previously this logic was duplicated
  practically verbatim between this file and the standalone backfill
  script, which is exactly the kind of drift risk folding them together
  removes.

  CHUNKING/CACHING: per explicit instruction, this copies the SAME
  calendar-month chunking + on-disk cache model already proven out for
  Mechanics>60 Audit / Cluster KPI in the reference sync script
  (pm_sync_full.py) — _month_windows()/_is_month_closed(), one gzipped CSV
  cache file per (source, YYYY-MM), a CLOSED month (fully ended before the
  current calendar month) served from disk with zero Metabase calls on
  every future run, the current/open month always fetched fresh
  (ignore_cache=True). This replaces the standalone backfill script's own
  fixed 21-day chunk scheme, which used a different, ad hoc model.

  Run it via: `python etl_yulu.py --daily-ops-backfill-start 2026-01-01`
  (optionally `--daily-ops-backfill-end YYYY-MM-DD`, defaults to
  yesterday) — this runs ONLY the backfill and skips Sweep/Octopus/Stuck/
  Warehouse/the daily STEP F entirely. With no flags, behavior is
  unchanged from before (the normal daily run for yesterday).
"""

import argparse
import io
import json
import os
from datetime import datetime, timedelta
from functools import wraps
from time import sleep

import pandas as pd
import requests
import gspread
from gspread.exceptions import APIError

# ─────────────────────────────────────────────────────────────
# CONFIG  (values come from GitHub Secrets / env variables)
# ─────────────────────────────────────────────────────────────
METABASE_URL      = os.environ["METABASE_URL"].rstrip("/")
METABASE_EMAIL    = os.environ["METABASE_EMAIL"]
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]

CARD_ID_SWEEP     = 654
CARD_ID_OCTOPUS   = 7433
CARD_ID_STUCK     = 9705
CARD_ID_WAREHOUSE = 6214

# Daily Ops Metrics (STEP F) — reports.yulu.bike/question/<id>-...
CARD_ID_DEMAND_METRICS = 12245   # demand-metrics-blr (already BLR-scoped)
CARD_ID_TOKEN_FLOW     = 11765   # token-flow-blr (already BLR-scoped)
CARD_ID_MECH_FLOW      = 8966    # mechanic-flow-testing (multi-city; filtered to BLR here)

# BLR-only scope
CITY = "BLR"

MASTER_SHEET_ID       = "1fBjHKwlxRGwjsOSjzHOB6cUjvaKrhvtXdaPGeZjZuH0"
BROKEN_BIKE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1eGDS2Sj33Gqk63QxmOzw302f05v_WoeSqSZr7Oz2dTE/edit"

DAILY_OPS_SHEET_TAB            = "Daily_Ops_Metrics"
MECH_PRODUCTIVITY_MIN_DAYS_OLD = 90   # mechanic must be older than this (DOJ) to count
LIVE_REPAIR_NORMALIZATION      = 3    # 3 live repairs == 1 regular repair, per spec
BLR_TOTAL_LABEL                = "BLR (Total)"   # synthetic "cluster" row = whole-city rollup

DAILY_OPS_COLS_ORDER = [
    "cluster", "date", "dau",
    "service_swap_fulfillment_pct_user", "service_swap_fulfillment_pct_token", "service_swap_tat_mins",
    "attachment_fulfillment_pct_user", "attachment_fulfillment_pct_token", "attachment_tat_mins",
    "mechanic_productivity_90d",
    "enquiry_total", "enquiry_to_attachment_pct",
]

# On-disk cache for CLOSED calendar months only -- same model/naming
# convention as the reference sync script's MECHANICS_AUDIT_CACHE_DIR /
# CLUSTER_KPI_CACHE_DIR. One gzipped CSV per (source_label, "YYYY-MM").
DAILY_OPS_CACHE_DIR = os.environ.get("DAILY_OPS_CACHE_DIR", ".cache/daily_ops_months")

# ID/text-like columns across the three Daily Ops Metrics cards that must
# never be silently coerced to float by pandas dtype inference (the
# "5038508.0" class of bug) — forced to string at CSV-parse time instead.
DAILY_OPS_STR_COLS = {
    # demand_metrics (card 12245)
    "cluster_name", "city_code",
    # token_flow (card 11765)
    "user_id", "token_id", "token_number", "yc_id", "yc_name", "city",
    "cluster", "existing_bike_name", "new_bike_name", "existing_bike_group",
    "new_bike_group", "token_type", "token_type_derived", "token_status",
    "action_status", "bike_type", "created_by", "fulfilled_by", "closed_by",
    "updated_by", "user_type",
    # mechanic_flow (card 8966)
    "phone_number", "bike_name", "username", "role", "display_name",
    "sub_role", "start_cluster", "primary_role", "QC pass/fail",
    "task_status", "task_type",
}

# "Bikes in Warehouse" card (6214) actually returns these columns.
# `city` is used only to filter to BLR — it isn't written to the sheet.
# `at_warehouse` / `whs_in_epoch` aren't needed downstream, so they're
# fetched but dropped before writing.
WAREHOUSE_METABASE_COLUMNS = [
    "city", "cluster", "yc_name", "bike_name", "category", "version_no",
    "at_warehouse", "whs_in_epoch", "issues", "part_name", "updated_part_name",
]

# Maps each Metabase column we DO write to its exact column letter in the
# 'Bikes_WHS' tab. Columns NOT in this map (Version NO, No of Faults, Flag,
# Fault Buckets, Stuck Part, State id) are sheet-side formulas and must
# never be cleared or written to — including ones like "Version NO" (F)
# that sit *between* two of our data columns (E and G).
WAREHOUSE_SHEET_COLUMN_MAP = {
    "cluster":           "A",
    "yc_name":            "B",
    "bike_name":          "C",
    "category":           "D",
    "version_no":         "E",
    # column F ("Version NO") is a formula — intentionally not mapped
    "issues":             "G",
    "part_name":          "H",
    "updated_part_name":  "I",
    # columns J–N (No of Faults, Flag, Fault Buckets, Stuck Part, State id)
    # are formulas — intentionally not mapped
}


def letter_to_index(letter: str) -> int:
    idx = 0
    for ch in letter:
        idx = idx * 26 + (ord(ch.upper()) - ord("A") + 1)
    return idx


# ─────────────────────────────────────────────────────────────
# GOOGLE SHEETS AUTH
# ─────────────────────────────────────────────────────────────
def get_gspread_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        import tempfile
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(sa_json)
            tmp_path = f.name
        return gspread.service_account(filename=tmp_path)
    return gspread.service_account(filename="service_account.json")


# ─────────────────────────────────────────────────────────────
# METABASE FETCH
# ─────────────────────────────────────────────────────────────
def metabase_session() -> dict:
    """Authenticate and return headers with session token."""
    resp = requests.post(
        f"{METABASE_URL}/api/session",
        json={"username": METABASE_EMAIL, "password": METABASE_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    return {"X-Metabase-Session": resp.json()["id"]}


def fetch_metabase_csv(card_id: int, city: str = None, city_tag: str = "City") -> pd.DataFrame:
    """
    Run a saved Metabase card and return the result as a DataFrame.

    FIX (confirmed live against reports.yulu.bike): the /query/csv export
    endpoint requires a FORM-ENCODED body with `parameters` as a
    JSON-*string* field -- a raw `json=` body (the previous behaviour
    here) is accepted with HTTP 200, but the City parameter is never
    actually bound server-side, so this was silently fetching EVERY
    city's full data on every call (verified directly: 71,925 rows via
    the old json= body vs 23,943 via the fixed form-encoded body for card
    654 alone). Every caller of this function already re-filters by city
    in pandas afterward, which is why the final sheet output was still
    correct -- this fix only removes the ~3x wasted fetch/transfer, it
    doesn't change any existing tab's contents.

    `city_tag` FIX (also confirmed live): the City template-tag's exact
    NAME is inconsistent across cards -- confirmed capital "City" on
    Sweep (654), but lowercase "city" on Stuck (9705) and Warehouse
    (6214). Targeting a template-tag name that doesn't exist on a given
    card causes Metabase to throw an unhandled 500 (unlike a genuinely
    *unfilled* existing tag, which fails gracefully with a 200 + JSON
    error body -- see _looks_like_metabase_error_payload). This is why
    the old json= bug never surfaced this: an unbound parameter was
    silently dropped entirely rather than validated against the card's
    real tag names. Each caller now passes the tag name that actually
    matches its card (checked directly against each card's
    dataset_query.native.template-tags).
    """
    headers = metabase_session()

    parameters = []
    if city:
        parameters.append({
            "type":   "text",
            "target": ["variable", ["template-tag", city_tag]],
            "value":  city,
        })

    csv_resp = requests.post(
        f"{METABASE_URL}/api/card/{card_id}/query/csv",
        data={"parameters": json.dumps(parameters)},
        headers=headers,
        timeout=180,
    )
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text), low_memory=False)
    print(f"  [Card {card_id}] city={city or 'ALL'} | {len(df)} rows | cols: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────────────────────
# METABASE FETCH — robustness helpers for STEP F (Daily Ops Metrics)
#
# Scoped to the new fetch path only, so the already-working
# Sweep/Octopus/Stuck/Warehouse steps above are not touched.
# ─────────────────────────────────────────────────────────────
def retry_on_api_error(max_retries=5, initial_delay=2, backoff_factor=2):
    """
    Retry decorator for transient errors with exponential backoff — same
    idea proven out in a production Metabase sync script. Retries dropped
    connections, timeouts, and 429/500/502/503 from both `requests`
    (Metabase) and gspread (Google Sheets); anything else propagates
    immediately.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            delay = initial_delay
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except requests.exceptions.RequestException as e:
                    last_exception = e
                    error_code = e.response.status_code if e.response is not None else None
                    is_timeout = isinstance(e, requests.exceptions.Timeout)
                    is_connection_error = isinstance(e, requests.exceptions.ConnectionError)
                    if (
                        (error_code in (429, 500, 502, 503) or is_timeout or is_connection_error)
                        and attempt < max_retries - 1
                    ):
                        reason = error_code or ("Timeout" if is_timeout else "ConnectionError")
                        print(f"  WARNING: {reason} in {func.__name__}, retrying in {delay}s "
                              f"(attempt {attempt + 1}/{max_retries})")
                        sleep(delay)
                        delay *= backoff_factor
                        continue
                    raise
                except APIError as e:
                    last_exception = e
                    error_code = None
                    if hasattr(e, "response") and hasattr(e.response, "status_code"):
                        error_code = e.response.status_code
                    if error_code in (429, 500, 502, 503) and attempt < max_retries - 1:
                        print(f"  WARNING: gspread API error {error_code} in {func.__name__}, "
                              f"retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                        sleep(delay)
                        delay *= backoff_factor
                        continue
                    raise
            raise last_exception
        return wrapper
    return decorator


class MetabaseQueryError(RuntimeError):
    """
    Raised when Metabase's /api/card/:id/query/csv endpoint returns HTTP
    200 claiming CSV, but the body is actually a query-execution error
    (e.g. a missing/renamed warehouse table) disguised as data — confirmed
    to happen in production. Deliberately not retried: retrying a broken
    query doesn't fix it, so this should abort STEP F for this run rather
    than silently writing 0s/blanks as if the underlying data were empty.
    """


def _looks_like_metabase_error_payload(text: str) -> bool:
    """
    pd.read_csv on a JSON error blob doesn't raise -- it silently parses
    the braces/commas as one meaningless header line with 0 data rows,
    which is indistinguishable from genuine emptiness unless sniffed for
    first.
    """
    stripped = text.lstrip()
    if not stripped.startswith("{"):
        return False
    head = stripped[:2000]
    return '"status":"failed"' in head or '"error_type":' in head or (
        '"error":' in head and '"started_at":' in head
    )


def get_yesterday() -> str:
    return (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")


@retry_on_api_error(max_retries=5, initial_delay=2, backoff_factor=2)
def fetch_metabase_csv_range(card_id: int, start_date: str, end_date: str,
                              force_str_cols: set = None, ignore_cache: bool = True) -> pd.DataFrame:
    """
    Like fetch_metabase_csv(), but for cards parameterised by a
    {{start_date}}/{{end_date}} date range (Demand Metrics, Token Flow,
    Mechanic Flow Testing) instead of a {{City}} tag, with the two fixes
    described in the module docstring: force_str_cols keeps ID/text
    columns as strings at parse time (never float-coerced), and the
    response is checked for a disguised Metabase query-execution error
    before being handed to pd.read_csv. ignore_cache defaults to True
    since STEP F always asks for a specific already-closed day — a stale
    cached answer for that day is never desirable here.

    FIX (confirmed live against reports.yulu.bike): unlike the plain
    /api/card/:id/query endpoint the Metabase web UI uses (which accepts
    a JSON body), the /query/csv EXPORT endpoint requires a
    FORM-ENCODED body with `parameters` as a JSON-*string* field — a raw
    `json=` request body here is silently accepted (HTTP 200) but the
    parameter values are never actually bound, so Metabase's engine falls
    through to "no value provided" and fails every single call with
    "Error determining value for parameter ... You'll need to pick a
    value for 'Start date'". Reproduced directly (same JSON body, both
    failing identically) and fixed by switching to `data=` (form-encoded)
    with `parameters` JSON-stringified — confirmed to return real CSV
    rows. This matches the working pattern already used elsewhere for
    exactly this reason (a proven production Metabase sync script uses
    `data={"parameters": json.dumps(parameters), ...}` for this same
    endpoint, never `json=`).
    """
    headers = metabase_session()
    parameters = [
        {"type": "date/single", "value": start_date, "target": ["variable", ["template-tag", "start_date"]]},
        {"type": "date/single", "value": end_date,   "target": ["variable", ["template-tag", "end_date"]]},
    ]

    csv_resp = requests.post(
        f"{METABASE_URL}/api/card/{card_id}/query/csv",
        data={
            "parameters": json.dumps(parameters),
            "ignore_cache": "true" if ignore_cache else "false",
        },
        headers=headers,
        timeout=180,
    )
    csv_resp.raise_for_status()

    if _looks_like_metabase_error_payload(csv_resp.text):
        raise MetabaseQueryError(
            f"[card_{card_id}] Metabase returned HTTP 200 claiming CSV, but the "
            f"body is a query-execution error payload, not real data. First "
            f"800 chars: {csv_resp.text[:800]}"
        )

    peek = pd.read_csv(io.StringIO(csv_resp.text), nrows=0)
    peek.columns = [c.strip() for c in peek.columns]
    dtype_map = {c: str for c in peek.columns if force_str_cols and c in force_str_cols}

    df = pd.read_csv(io.StringIO(csv_resp.text), dtype=dtype_map or None, low_memory=False)
    df.columns = [c.strip() for c in df.columns]

    print(f"  [Card {card_id}] {start_date} -> {end_date} | {len(df)} rows | cols: {df.columns.tolist()}")
    return df


# ─────────────────────────────────────────────────────────────
# SHEET HELPER
# ─────────────────────────────────────────────────────────────
def col_letter(n: int) -> str:
    result = ""
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


def clean_for_sheets(df: pd.DataFrame) -> list:
    """
    Convert DataFrame to a list of lists safe for JSON serialisation.
    """
    df = df.copy()
    df = df.replace([float("inf"), float("-inf")], "")
    df = df.where(pd.notnull(df), "")
    str_df = df.astype(str).replace({"nan": "", "NaN": "", "NaT": "", "None": "", "<NA>": ""})
    return str_df.values.tolist()


def clear_and_upload(gc: gspread.Client, sheet_id: str, tab: str, df: pd.DataFrame):
    ws       = gc.open_by_key(sheet_id).worksheet(tab)
    last_col = col_letter(max(len(df.columns), 1))
    ws.batch_clear([f"A2:{last_col}"])
    values = clean_for_sheets(df)
    if values:
        ws.update(
            values,
            f"A2:{last_col}{len(values) + 1}",
            value_input_option="user_entered",
        )
    print(f"  '{tab}' → {len(df)} rows uploaded.")


def _matches_target_date(val, target_date: str) -> bool:
    """
    Compares a cell value against target_date ('YYYY-MM-DD') tolerating
    Google Sheets' own reformatting: writing an ISO date string via
    value_input_option="user_entered" makes Sheets parse it as a real
    date and re-render it per the spreadsheet's locale/column format
    (commonly DD/MM/YYYY for an India-locale sheet) -- a plain string
    equality check against the original ISO string would then silently
    never match again, breaking the delete-before-append idempotency on
    any re-run for the same date (confirmed as a real risk here, not
    hypothetical -- this file writes dates as plain ISO strings with no
    explicit @TEXT/plain-text column format forcing them to stay text).
    """
    if val is None:
        return False
    s = str(val).strip()
    if not s:
        return False
    if s == target_date:
        return True
    for fmt in ("%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
        try:
            if datetime.strptime(s, fmt).strftime("%Y-%m-%d") == target_date:
                return True
        except ValueError:
            continue
    return False


@retry_on_api_error(max_retries=5, initial_delay=2, backoff_factor=2)
def delete_rows_for_date_and_append(gc: gspread.Client, sheet_id: str, tab: str,
                                     df: pd.DataFrame, date_col_letter: str, target_date: str,
                                     header: list[str] | None = None):
    """
    Running-log write helper for STEP F (Daily Ops Metrics), mirroring the
    delete-for-date-then-append pattern used for daily Metabase syncs
    elsewhere: deletes any existing rows in `tab` whose `date_col_letter`
    column matches `target_date` (see _matches_target_date -- tolerant of
    Sheets reformatting a plain ISO string into a locale date), then
    appends `df`'s rows for that date at the bottom. Unlike
    clear_and_upload(), this never touches rows for any other date, so
    the tab accumulates history across days instead of being overwritten
    on every run.

    If `tab` doesn't exist yet, it's created with `header` as row 1 (or
    df's own columns if `header` is omitted) -- plain .worksheet(tab)
    raises WorksheetNotFound on a brand-new tab, which previously meant
    this step silently failed every run (caught by main()'s non-blocking
    try/except around STEP F) until someone manually created the tab.
    """
    try:
        ws = gc.open_by_key(sheet_id).worksheet(tab)
    except gspread.exceptions.WorksheetNotFound:
        print(f"  '{tab}' does not exist yet — creating it...")
        ss = gc.open_by_key(sheet_id)
        header_row = header if header is not None else list(df.columns)
        ws = ss.add_worksheet(title=tab, rows=1000, cols=max(len(header_row) + 2, 10))
        ws.append_row(header_row, value_input_option="RAW")

    col_idx = letter_to_index(date_col_letter)
    date_vals = ws.col_values(col_idx)

    rows_to_delete = [
        i + 1 for i, val in enumerate(date_vals)
        if i > 0 and _matches_target_date(val, target_date)
    ]

    if rows_to_delete:
        groups = []
        start = end = rows_to_delete[0]
        for row in rows_to_delete[1:]:
            if row == end + 1:
                end = row
            else:
                groups.append((start, end))
                start = end = row
        groups.append((start, end))

        delete_requests = {
            "requests": [
                {"deleteDimension": {"range": {
                    "sheetId": ws.id, "dimension": "ROWS",
                    "startIndex": s - 1, "endIndex": e,
                }}}
                for s, e in reversed(groups)
            ]
        }
        gc.open_by_key(sheet_id).batch_update(delete_requests)
        print(f"  '{tab}' → deleted {len(rows_to_delete)} existing row(s) for {target_date}.")

    values = clean_for_sheets(df)
    if values:
        ws.append_rows(values, value_input_option="user_entered")
    print(f"  '{tab}' → appended {len(values)} row(s) for {target_date}.")


def update_named_columns(gc: gspread.Client, sheet_id: str, tab: str,
                          df: pd.DataFrame, column_letters: dict):
    """
    Update ONLY the specific named columns given in `column_letters`
    (a {df_column_name: sheet_column_letter} map), leaving every other
    column in `tab` — including formula columns sitting between two of
    our target columns — completely untouched.

    - Row range: row 2 down to the sheet's last row is CLEARED (so no
      stale rows linger below this run's data if today's fetch is
      shorter than a previous one), then the new values are written
      starting at row 2. Row 1 (the header) is never touched.
    - Column range: target columns are grouped into contiguous runs
      (e.g. A:E) and each run is cleared/written as its own separate
      range. A gap (a column between two targets that isn't in the map,
      like a formula column) breaks the run, so that gap column is
      never included in any clear/update call.
    """
    ws = gc.open_by_key(sheet_id).worksheet(tab)
    last_row = max(ws.row_count, 2)  # sheet's actual last row; never below 2

    ordered = sorted(
        ((col, column_letters[col]) for col in df.columns if col in column_letters),
        key=lambda pair: letter_to_index(pair[1]),
    )
    if not ordered:
        print(f"  '{tab}' → no matching columns to update, skipping.")
        return

    ordered_cols = [col for col, _ in ordered]
    values_all   = clean_for_sheets(df[ordered_cols])
    n_rows       = len(values_all)
    if n_rows == 0:
        print(f"  '{tab}' → no rows to write, skipping (nothing cleared).")
        return

    # Split into contiguous column runs (e.g. A-E, then G-I) so a formula
    # column in between (like F) never falls inside a clear/update range.
    groups = [[ordered[0]]]
    for prev, curr in zip(ordered, ordered[1:]):
        if letter_to_index(curr[1]) == letter_to_index(prev[1]) + 1:
            groups[-1].append(curr)
        else:
            groups.append([curr])

    col_offset = 0
    for group in groups:
        width        = len(group)
        start_letter = group[0][1]
        end_letter   = group[-1][1]
        sub_values   = [row[col_offset:col_offset + width] for row in values_all]
        col_offset  += width

        clear_range  = f"{start_letter}2:{end_letter}{last_row}"
        write_range  = f"{start_letter}2:{end_letter}{n_rows + 1}"
        ws.batch_clear([clear_range])
        ws.update(sub_values, write_range, value_input_option="user_entered")
        print(f"  '{tab}' → cleared {clear_range}, wrote {n_rows} rows to {write_range} "
              f"({', '.join(c for c, _ in group)}).")


# ─────────────────────────────────────────────────────────────
# HELPER: Normalise bike ID to plain string
# FIX: gspread sometimes returns bike IDs as "5038508.0" (float)
#      or as int. This strips the .0 and forces string consistently.
# ─────────────────────────────────────────────────────────────
def normalise_bike_id(series: pd.Series) -> pd.Series:
    return (
        series.astype(str)
              .str.strip()
              .str.replace(r"\.0$", "", regex=True)
              .str.replace(r"\s+", "", regex=True)
    )


# ─────────────────────────────────────────────────────────────
# STEP A — SWEEP  (card 654)
# ─────────────────────────────────────────────────────────────
def process_sweep(gc: gspread.Client) -> pd.DataFrame:
    print("\n── STEP A: Sweep ──")
    df = fetch_metabase_csv(CARD_ID_SWEEP, city=CITY)
    df = df.replace([None], ["NA"], regex=True)

    # FIX: normalise bike IDs early, before any category filtering, so the
    # diagnostic lookup below can tell "not in Sweep at all" apart from
    # "in Sweep but filtered out by bike_category".
    df["bike"] = normalise_bike_id(df["bike"])

    # DIAGNOSTIC: keep a category lookup of every bike returned for this
    # city, *before* restricting to DeX/Express/Miracle. Used later to
    # explain unmatched bikes instead of just counting them.
    all_categories_lookup = df[["bike", "bike_category"]].drop_duplicates(subset="bike")

    wanted = [
        "city", "bike", "bike_category", "bike_group", "at_warehouse",
        "nearest_yz", "no_of_days_since_rnt", "flag_bike_fault",
        "reserved_bike", "on_biker_map", "on_fleet_map", "is_test_vehicle",
        "flag_stolen", "flag_unavailable", "bike_state_id", "version_no",
        "operational_cluster","last_rtd_dt",
    ]
    df = df[[c for c in wanted if c in df.columns]]

    df = df[df["city"].isin([CITY])]
    # UPDATED: include Miracle bikes alongside DeX and Express
    df = df[df["bike_category"].isin(["DeX", "Express", "Miracle"])]

    # UPDATED: version_group checks bike_category first.
    # Express → "Express", Miracle → "Miracle", DeX → "2x"/"3x" by version_no prefix.
    df["version_group"] = df.apply(
        lambda row: "Express" if row["bike_category"] == "Express"
        else ("Miracle" if row["bike_category"] == "Miracle"
              else ("2x" if str(row["version_no"]).startswith("2.")
                    else ("3x" if str(row["version_no"]).startswith("3.")
                          else "Unknown"))),
        axis=1
    )

    clear_and_upload(gc, MASTER_SHEET_ID, "Sweep", df)
    df.attrs["all_categories_lookup"] = all_categories_lookup
    return df


# ─────────────────────────────────────────────────────────────
# STEP B — OCTOPUS  (card 7433)
# ─────────────────────────────────────────────────────────────

# Explicit bike_group -> Type map. Checked first; any value found here
# uses this mapping exactly (unchanged behaviour from before).
OCTOPUS_TYPE_MAP = {
    "DeX 2.0":               "2x",
    "DeX 2.5":               "2x",
    "DeX 3.2 GR":            "3x",
    "DeX 3.3 GR":            "3x",
    "Dex_3.0 GR":            "3x",
    "Express BGAUSS":        "Express",
    "Express MV":            "Express",
    "Express NYX":           "Express",
    "Express Yadea Y1S Pro": "Express",
    "Miracle 2.0":           "Miracle",
    "Miracle 2.5":           "Miracle",
    "Miracle 3.0 GR":        "Miracle",
    "Miracle 3.1 GR":        "Miracle",
    "Miracle 3.2 GR":        "Miracle",
}


def classify_bike_group(bike_group) -> str:
    """
    Classify a bike_group string into 2x / 3x / Express / Miracle.

    FIX: the old code was a plain dict `.map(OCTOPUS_TYPE_MAP)`, so any
    bike_group value not already an exact key (e.g. a new variant like
    "DeX 3.5 GR" that was never added to the dict) silently became NaN,
    which then rendered as a blank 'Type' cell on the sheet — no warning.

    Now: exact dict match first (identical behaviour to before for every
    known value). If it's not in the dict, fall back to a prefix-based
    guess so new variants still get classified:
      - starts with "DeX"     -> "2x" if version has "2.", "3x" if "3."
      - starts with "Express" -> "Express"
      - starts with "Miracle" -> "Miracle"
    If neither the dict nor the fallback can classify it, return None
    (still renders as a blank cell, same as before) — but the caller
    prints a warning listing every value that fell through, so it shows
    up in the run log instead of only being visible as a blank cell on
    the sheet.
    """
    bg = str(bike_group).strip()

    if bg in OCTOPUS_TYPE_MAP:
        return OCTOPUS_TYPE_MAP[bg]

    if bg.startswith("DeX") or bg.startswith("Dex"):
        if "2." in bg:
            return "2x"
        if "3." in bg:
            return "3x"
        return None

    if bg.startswith("Express"):
        return "Express"

    if bg.startswith("Miracle"):
        return "Miracle"

    return None


def process_octopus(gc: gspread.Client):
    print("\n── STEP B: Octopus ──")
    df1 = fetch_metabase_csv(CARD_ID_OCTOPUS)

    df1["Type"] = df1["bike_group"].apply(classify_bike_group)

    # WARNING: surface any bike_group values that still couldn't be
    # classified (neither an exact dict match nor a DeX/Express/Miracle
    # prefix), so these show up in the run log instead of only as a
    # silent blank cell in the 'Type' column on the sheet.
    unmapped = sorted(df1.loc[df1["Type"].isna(), "bike_group"].dropna().unique().tolist())
    if unmapped:
        print(f"  WARNING: {len(unmapped)} bike_group value(s) in Octopus could not be "
              f"classified into a Type and will show blank on the sheet: {unmapped}")

    clear_and_upload(gc, MASTER_SHEET_ID, "Octopus", df1)


# ─────────────────────────────────────────────────────────────
# STEP C — STUCK / TO BE MOVED  (card 9705)
# ─────────────────────────────────────────────────────────────
def fetch_broken_bikes(gc: gspread.Client) -> pd.DataFrame:
    sh       = gc.open_by_url(BROKEN_BIKE_SHEET_URL)
    all_data = []
    # PATCH: only the BLR tab now — was looping ["BLR", "BOM", "NCR", "HYD"]
    # which pulled in bikes from other cities that could never match the
    # BLR-only Sweep data, inflating the "missing bike_state_id" count.
    for tab in [CITY]:
        ws = sh.worksheet(tab)
        # FIX: raised from 1000 → 10000 so we never silently truncate
        for start_col, end_col in [("G", "H"), ("J", "K")]:
            data = ws.get(f"{start_col}2:{end_col}10000")
            if not data:
                continue
            tmp = pd.DataFrame(data)

            # Make sure we always have exactly 2 columns even if some rows
            # only have 1 value (gspread drops trailing empty cells)
            if tmp.shape[1] == 1:
                tmp[1] = ""
            tmp = tmp.iloc[:, :2].copy()
            tmp.columns = ["bike", "Reason"]

            # Drop rows where BOTH cells are empty
            tmp = tmp[~((tmp["bike"].astype(str).str.strip() == "") &
                        (tmp["Reason"].astype(str).str.strip() == ""))]

            # Drop header rows that leaked in (e.g. "bike", "Reason" as values)
            tmp = tmp[~((tmp["bike"].astype(str).str.lower().str.strip() == "bike") &
                        (tmp["Reason"].astype(str).str.lower().str.strip() == "reason"))]

            tmp["City"] = tab
            all_data.append(tmp)

    if not all_data:
        print("  WARNING: No data found in broken bikes sheet!")
        return pd.DataFrame(columns=["bike", "Reason", "City"])

    merged = pd.concat(all_data, ignore_index=True)

    # FIX: normalise bike IDs — gspread may return "5038508.0" or int
    merged["bike"] = normalise_bike_id(merged["bike"])

    # FIX: normalise Reason to lowercase for consistent matching, then
    #      map to canonical display names (case-insensitive)
    merged["Reason"] = merged["Reason"].astype(str).str.strip()

    reason_map = {
        "chassis damage":            "Chassis Damage",
        "neck broken beyond repair": "Neck Broken Beyond Repair",
        "neck broke beyond repair":  "Neck Broken Beyond Repair",
        "swing arm bush exposed":    "Swing Arm Bush Exposed",
    }
    merged["Reason"] = merged["Reason"].str.lower().map(reason_map).fillna(merged["Reason"].str.strip())

    # FIX: drop duplicate (bike, Reason, City) rows from the source sheet
    before = len(merged)
    merged = merged.drop_duplicates(subset=["bike", "Reason", "City"])
    dropped = before - len(merged)
    if dropped:
        print(f"  Dropped {dropped} duplicate rows from broken bikes sheet.")

    # Drop rows with clearly invalid/garbled bike IDs.
    # FIX: was r"^5\d{6}$" — wrongly assumed every bike ID is a 7-digit
    # number starting with "5". Express bikes use a different numbering
    # scheme (e.g. 3001032), so that regex was silently discarding every
    # Express bike in the sheet before it ever reached the Sweep merge.
    # Now we only drop entries that aren't a plain digit string at all
    # (blank cells, the literal "nan", stray text) — anything that's a
    # real bike ID but doesn't exist in Sweep will still surface later
    # via the "not in sweep at all" diagnostic in process_stuck, which is
    # the right place to catch genuinely bogus IDs.
    valid_mask = merged["bike"].str.match(r"^\d+$")
    invalid = merged[~valid_mask]
    if not invalid.empty:
        print(f"  WARNING: Dropping {len(invalid)} rows with non-numeric bike IDs: {invalid['bike'].tolist()}")
    merged = merged[valid_mask].reset_index(drop=True)

    print(f"  Broken bikes sheet total: {len(merged)} rows across {merged['City'].value_counts().to_dict()}")
    return merged


def process_stuck(gc: gspread.Client, sweep_df: pd.DataFrame):
    print("\n── STEP C: Stuck / To Be Moved ──")
    df2 = fetch_metabase_csv(CARD_ID_STUCK, city=CITY, city_tag="city")

    def map_version(v):
        s = str(v).strip()
        if s.startswith("2."):  return "2x"
        if s.startswith("3."):  return "3x"
        return "Express"

    df2["version_no"] = df2["version_no"].apply(map_version)

    # FIX: handle both 'issues' and 'updated_part_name' columns
    if "issues" in df2.columns:
        if "updated_part_name" in df2.columns:
            df2 = df2.drop(columns=["issues"])
        else:
            df2 = df2.rename(columns={"issues": "updated_part_name"})

    df2["updated_part_name"] = df2["updated_part_name"].astype(str).str.strip()
    df2 = (
        df2.assign(updated_part_name=df2["updated_part_name"].str.split(","))
        .explode("updated_part_name")
        .reset_index(drop=True)
    )
    df2["updated_part_name"] = df2["updated_part_name"].str.strip()

    # FIX: normalise bike IDs in df2
    bike_col = "bike_name" if "bike_name" in df2.columns else "bike"
    df2[bike_col] = normalise_bike_id(df2[bike_col])

    # Motor 2x override
    df2.loc[
        (df2["updated_part_name"].str.lower() == "motor") & (df2["version_no"] == "2x"),
        "updated_part_name",
    ] = "Motor 2x"

    # External broken-bike sheet
    print("  Fetching external broken-bike sheet…")
    broken = fetch_broken_bikes(gc)

    # sweep_df bike IDs already normalised in process_sweep
    # Just ensure no duplicates in the lookup table
    sweep_lookup = (
        sweep_df[["bike", "bike_state_id", "reserved_bike", "version_group"]]
        .drop_duplicates(subset="bike")
    )

    print(f"  Broken bikes before merge: {len(broken)}")
    df_final = broken.merge(sweep_lookup, on="bike", how="left")
    print(f"  After merge: {len(df_final)} rows")

    missing_mask = df_final["bike_state_id"].isna()
    print(f"  Rows with missing bike_state_id (not in sweep): {missing_mask.sum()}")

    # DIAGNOSTIC: print the actual unmatched bike IDs, and explain *why*
    # each one didn't match — not found in Sweep at all for this city, vs.
    # found but excluded by the DeX/Express/Miracle category filter.
    if missing_mask.any():
        missing_bikes = df_final.loc[missing_mask, "bike"].tolist()
        cat_lookup = sweep_df.attrs.get("all_categories_lookup")
        if cat_lookup is not None:
            found_other_cat = cat_lookup[cat_lookup["bike"].isin(missing_bikes)]
            not_in_sweep_at_all = set(missing_bikes) - set(found_other_cat["bike"])
            print(f"  -> {len(found_other_cat)} of these exist in Sweep but with a "
                  f"different bike_category: {found_other_cat.set_index('bike')['bike_category'].to_dict()}")
            print(f"  -> {len(not_in_sweep_at_all)} of these don't appear in the "
                  f"Sweep extract for {CITY} at all: {sorted(not_in_sweep_at_all)}")
        else:
            print(f"  -> Unmatched bike IDs: {missing_bikes}")

    # Filter out bikes already in state 64 or 65
    before = len(df_final)
    df_final = df_final[~df_final["bike_state_id"].isin([64, 65])]
    print(f"  After removing state 64/65: {len(df_final)} rows (removed {before - len(df_final)})")

    # Filter out LTR reserved bikes
    before = len(df_final)
    df_final = df_final[~df_final["reserved_bike"].isin(["LTR"])]
    print(f"  After removing LTR: {len(df_final)} rows (removed {before - len(df_final)})")

    # Filter out bikes not found in sweep (version_group is NaN = not a sweep-matched DeX/Express/Miracle bike)
    before = len(df_final)
    df_final = df_final[df_final["version_group"].notna()]
    print(f"  After removing non-sweep bikes: {len(df_final)} rows (removed {before - len(df_final)})")

    clear_and_upload(gc, MASTER_SHEET_ID, "To be moved", df_final)
    return df_final, df2


# ─────────────────────────────────────────────────────────────
# STEP D — FINALISE STUCK + PARTS SUMMARY
# ─────────────────────────────────────────────────────────────
def process_parts_summary(gc: gspread.Client, df_final: pd.DataFrame, df2: pd.DataFrame):
    print("\n── STEP D: Stuck + Parts Summary ──")

    severe_bikes = (
        df_final[df_final["Reason"].isin(["Chassis Damage", "Neck Broken Beyond Repair"])]
        ["bike"].astype(str).str.strip()
    )

    bike_col      = "bike_name" if "bike_name" in df2.columns else "bike"
    df2[bike_col] = normalise_bike_id(df2[bike_col])
    df2_clean     = df2[~df2[bike_col].isin(severe_bikes)].copy()

    clear_and_upload(gc, MASTER_SHEET_ID, "Stuck", df2_clean)

    # Parts priority
    part_priorities = [
        "motor solid", "motor pneumatic", "cone set", "iot",
        "rear shocker", "front shocker t handle", "front shocker",
        "motor controller",
    ]
    bike_2x_parts      = ["front shocker t handle", "rear shocker", "front shocker"]
    bike_3x_parts      = ["motor solid", "motor pneumatic", "cone set", "iot"]
    bike_express_parts = ["motor solid", "motor controller"]

    df2_clean["updated_part_name"] = df2_clean["updated_part_name"].str.lower().str.strip()
    df2_clean[bike_col]            = df2_clean[bike_col].str.lower().str.strip()

    def bike_matches_part(bike_name: str, part_name: str) -> bool:
        if "2x" in bike_name:      return part_name in bike_2x_parts
        if "3x" in bike_name:      return part_name in bike_3x_parts
        if "express" in bike_name: return part_name in bike_express_parts
        return True

    df_filtered = df2_clean[
        df2_clean.apply(lambda r: bike_matches_part(r[bike_col], r["updated_part_name"]), axis=1)
    ].copy()

    priority_map              = {n: i for i, n in enumerate(part_priorities)}
    df_filtered["priority"]   = df_filtered["updated_part_name"].map(priority_map).fillna(99).astype(int)
    df_sorted                 = df_filtered.sort_values(["city", "priority", bike_col])

    summary = (
        df_sorted.groupby(["city", "updated_part_name"])
        .agg({bike_col: lambda x: list(x.unique())})
        .reset_index()
        .rename(columns={bike_col: "unique_bikes"})
    )
    summary["priority"] = summary["updated_part_name"].map(priority_map)
    summary = summary.sort_values(["city", "priority"])

    final_rows = []
    for city in summary["city"].unique():
        city_df    = summary[summary["city"] == city]
        seen_bikes = set()
        for pname in part_priorities:
            match = city_df[city_df["updated_part_name"] == pname]
            if not match.empty:
                bikes = [b for b in match.iloc[0]["unique_bikes"] if b not in seen_bikes]
                if bikes:
                    final_rows.append({
                        "city": city, "updated_part_name": pname,
                        "unique_bike_count": len(bikes),
                    })
                    seen_bikes.update(bikes)

    if not final_rows:
        print("  No parts summary data — skipping.")
        return

    result_df = pd.DataFrame(final_rows)
    pivot_df  = (
        result_df.pivot(index="updated_part_name", columns="city", values="unique_bike_count")
        .fillna(0).astype(int).reset_index().rename_axis(None, axis=1)
    )
    clear_and_upload(gc, MASTER_SHEET_ID, "Parts_Summary", pivot_df)


# ─────────────────────────────────────────────────────────────
# STEP E — BIKES IN WAREHOUSE  (card 6214)
# ─────────────────────────────────────────────────────────────
def process_warehouse(gc: gspread.Client) -> pd.DataFrame:
    """
    Pulls the "Bikes in Warehouse" card (6214), filtered to BLR, and writes
    only the Metabase-sourced columns into the 'Bikes_WHS' tab — via
    update_named_columns, which touches only A:E and G:I (per
    WAREHOUSE_SHEET_COLUMN_MAP), never the formula columns F, J, K, L, M, N.
    """
    print("\n── STEP E: Bikes in Warehouse ──")
    # No server-side city parameter here (per explicit decision) -- fetch
    # everything and rely entirely on the client-side filter below.
    df = fetch_metabase_csv(CARD_ID_WAREHOUSE)

    # Filter to BLR only (the only city filtering this step does now).
    if "city" in df.columns:
        df = df[df["city"] == CITY]

    missing = [c for c in WAREHOUSE_METABASE_COLUMNS if c not in df.columns]
    if missing:
        print(f"  NOTE: card {CARD_ID_WAREHOUSE} is missing expected columns: {missing}")

    # Only keep columns we actually write to the sheet (drops city,
    # at_warehouse, whs_in_epoch — none of those go to 'Bikes_WHS').
    keep = [c for c in WAREHOUSE_SHEET_COLUMN_MAP if c in df.columns]
    df = df[keep]

    update_named_columns(gc, MASTER_SHEET_ID, "Bikes_WHS", df, WAREHOUSE_SHEET_COLUMN_MAP)
    return df


# ─────────────────────────────────────────────────────────────
# STEP F — DAILY OPS METRICS
#   Demand Metrics (12245) + Token Flow (11765) + Mechanic Flow Testing (8966)
# ─────────────────────────────────────────────────────────────
def _token_fulfillment_metrics(token_df: pd.DataFrame, type_value: str,
                                col_prefix: str) -> pd.DataFrame:
    """
    Per-cluster fulfillment% (user-level and token-level) + mean TAT for
    one token_type_derived value (e.g. "service_swap" or "Attach").

      token-level fulfillment% = closed tokens / total tokens of that type
      user-level fulfillment%  = unique users with >=1 closed token of
                                  that type / unique users with >=1 token
                                  of that type at all
      TAT                      = mean checkin_to_fulfilled_tat_mins over
                                  CLOSED tokens of that type only
    """
    cols = ["cluster",
            f"{col_prefix}_fulfillment_pct_user",
            f"{col_prefix}_fulfillment_pct_token",
            f"{col_prefix}_tat_mins"]

    sub = token_df[token_df["token_type_derived"] == type_value]
    if sub.empty:
        return pd.DataFrame(columns=cols)

    closed = sub[sub["token_status"] == "Closed"]

    token_total  = sub.groupby("cluster")["token_id"].nunique().rename("token_total")
    token_closed = closed.groupby("cluster")["token_id"].nunique().rename("token_closed")
    user_total   = sub.groupby("cluster")["user_id"].nunique().rename("user_total")
    user_closed  = closed.groupby("cluster")["user_id"].nunique().rename("user_closed")
    tat          = closed.groupby("cluster")["checkin_to_fulfilled_tat_mins"].mean().rename("tat_mins")

    out = pd.concat([token_total, token_closed, user_total, user_closed, tat], axis=1)
    out[["token_total", "token_closed", "user_total", "user_closed"]] = (
        out[["token_total", "token_closed", "user_total", "user_closed"]].fillna(0)
    )

    out[f"{col_prefix}_fulfillment_pct_user"] = out.apply(
        lambda r: round(100 * r["user_closed"] / r["user_total"], 1) if r["user_total"] else None, axis=1)
    out[f"{col_prefix}_fulfillment_pct_token"] = out.apply(
        lambda r: round(100 * r["token_closed"] / r["token_total"], 1) if r["token_total"] else None, axis=1)
    out[f"{col_prefix}_tat_mins"] = out["tat_mins"].round(1)

    return out.reset_index()[cols]


def _compute_daily_ops_rows_for_date(demand_day: pd.DataFrame, token_day: pd.DataFrame,
                                      mech_day: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """
    Pure per-date computation, shared by BOTH the daily STEP F run
    (process_daily_ops_metrics — each input already scoped to exactly one
    day by its own single-day Metabase fetch) and the range backfill
    (compute_daily_ops_metrics_range — each input is a date-filtered slice
    of one bulk multi-month fetch). Identical math either way; only where
    the per-day data comes from differs.

    One row per BLR cluster for `report_date`, combining:
      - dau                                               (Demand Metrics, 12245)
      - service_swap fulfillment% (user/token) + TAT      (Token Flow, 11765)
      - attachment fulfillment% (user/token) + TAT         (Token Flow, 11765)
      - mechanic productivity, mechanics >90 days old       (Mechanic Flow, 8966)
      - enquiry_total + enquiry->attachment%                (Token Flow, 11765)

    Mechanic productivity (confirmed logic): for mechanics with
    primary_role == "Maintenance" and date_of_joining more than
    MECH_PRODUCTIVITY_MIN_DAYS_OLD days before the report date, in a given
    cluster: (unique bikes with live_repair_flag==0 and QC pass/fail !=
    "fail" [blank counts as not-fail]) + (unique bikes with
    live_repair_flag==1 / LIVE_REPAIR_NORMALIZATION), divided by the
    unique count of those mechanics. No task_type filter (repair +
    non-repair tasks both count, per explicit confirmation).

    Enquiry->Attachment%: of users with an Enquiry token in a cluster on
    the report date, the % who also have an Attach token in that SAME
    cluster and day (no direct link between the two token types exists in
    the data, so this is a same-user/same-cluster/same-day proxy).

    BLR total row: every metric also gets a synthetic "BLR (Total)" row —
    RECOMPUTED across the whole city, not a sum/average of the per-cluster
    rows (fulfillment%/TAT/productivity don't aggregate linearly — e.g.
    city-wide fulfillment% is closed÷total across ALL BLR tokens, not the
    mean of each cluster's %). Implemented by duplicating each city's
    already-BLR-filtered rows under the label BLR_TOTAL_LABEL before the
    per-cluster groupby, so the exact same aggregation code produces both
    the per-cluster rows and the city-total row in one pass.

    Every input is assumed already hard-filtered to city == "BLR" by the
    caller — no other city's rows should ever reach this function.
    """
    base = demand_day[["cluster", "dau"]].copy()
    base = pd.concat(
        [pd.DataFrame([{"cluster": BLR_TOTAL_LABEL, "dau": base["dau"].sum()}]), base],
        ignore_index=True,
    )

    # Duplicate every BLR row under BLR_TOTAL_LABEL so the same per-cluster
    # groupby logic below also produces a genuine whole-city aggregate row.
    token_df = pd.concat(
        [token_day, token_day.assign(cluster=BLR_TOTAL_LABEL)], ignore_index=True)

    swap_metrics   = _token_fulfillment_metrics(token_df, "service_swap", "service_swap")
    attach_metrics = _token_fulfillment_metrics(token_df, "Attach", "attachment")

    enquiry_df = token_df[token_df["token_type_derived"] == "Enquiry"]
    enquiry_total = enquiry_df.groupby("cluster")["token_id"].nunique().rename("enquiry_total")

    attach_users_by_cluster = (
        token_df[token_df["token_type_derived"] == "Attach"]
        .groupby("cluster")["user_id"].apply(set)
    )
    enquiry_users_by_cluster = enquiry_df.groupby("cluster")["user_id"].apply(set)

    def _enquiry_to_attach_pct(cluster: str, users: set) -> float | None:
        if not users:
            return None
        attach_users = attach_users_by_cluster.get(cluster, set())
        return round(100 * len(users & attach_users) / len(users), 1)

    enquiry_pct = pd.Series(
        {c: _enquiry_to_attach_pct(c, u) for c, u in enquiry_users_by_cluster.items()},
        name="enquiry_to_attachment_pct",
    )
    enquiry_summary = (
        pd.concat([enquiry_total, enquiry_pct], axis=1)
        .reset_index().rename(columns={"index": "cluster"})
    )

    # Denominator = ONLY maintenance-profile people: primary_role ==
    # "Maintenance" (the mechanic's actual HR role, not the task's own
    # "role" column, which is always "Maintenance" for every row in this
    # report regardless of who performed it) AND DOJ > 90 days — same
    # eligibility rule as compute_mechanics_audit() in the reference sync
    # script. mechanic_count below is the unique count of exactly this
    # population; it is never task/row counts.
    eligible = mech_day[
        (mech_day["primary_role"] == "Maintenance")
        & mech_day["days_old"].notna()
        & (mech_day["days_old"] > MECH_PRODUCTIVITY_MIN_DAYS_OLD)
    ].copy()

    if eligible.empty:
        print(f"  WARNING: no eligible mechanics (Maintenance, DOJ > "
              f"{MECH_PRODUCTIVITY_MIN_DAYS_OLD}d) found for {report_date} — "
              f"mechanic_productivity_90d will be 0 for every cluster.")
        productivity = pd.DataFrame(columns=["cluster", "mechanic_productivity_90d"])
    else:
        eligible["qc_norm"] = eligible["QC pass/fail"].fillna("").astype(str).str.strip().str.lower()
        eligible["live_repair_flag"] = pd.to_numeric(
            eligible["live_repair_flag"], errors="coerce").fillna(0).astype(int)

        # Duplicate under BLR_TOTAL_LABEL so the city-total productivity is
        # (all BLR regular bikes + all BLR live bikes/3) / all eligible BLR
        # mechanics — not an average of the per-cluster ratios. Done AFTER
        # qc_norm/live_repair_flag are derived, so both copies carry them.
        eligible = pd.concat(
            [eligible, eligible.assign(start_cluster=BLR_TOTAL_LABEL)], ignore_index=True)

        not_failed   = eligible["qc_norm"] != "fail"
        regular_mask = not_failed & (eligible["live_repair_flag"] == 0)
        live_mask    = not_failed & (eligible["live_repair_flag"] == 1)

        mechanic_counts = eligible.groupby("start_cluster")["user_id"].nunique().rename("mechanic_count")
        regular_counts  = eligible[regular_mask].groupby("start_cluster")["bike_name"].nunique().rename("regular_bikes")
        live_counts     = eligible[live_mask].groupby("start_cluster")["bike_name"].nunique().rename("live_bikes")

        productivity = pd.concat([mechanic_counts, regular_counts, live_counts], axis=1).fillna(0)
        productivity["mechanic_productivity_90d"] = productivity.apply(
            lambda r: round(
                (r["regular_bikes"] + r["live_bikes"] / LIVE_REPAIR_NORMALIZATION) / r["mechanic_count"], 3
            ) if r["mechanic_count"] else None,
            axis=1,
        )
        productivity = productivity.reset_index().rename(
            columns={"start_cluster": "cluster"})[["cluster", "mechanic_productivity_90d"]]

    # ---- Merge everything onto the Demand Metrics cluster list ----
    result = base.merge(swap_metrics, on="cluster", how="left")
    result = result.merge(attach_metrics, on="cluster", how="left")
    result = result.merge(productivity, on="cluster", how="left")
    result = result.merge(enquiry_summary, on="cluster", how="left")

    # No eligible mechanics (Maintenance, DOJ > 90d) for a cluster that day
    # reads as 0 productivity, not blank — confirmed explicitly.
    result["mechanic_productivity_90d"] = result["mechanic_productivity_90d"].fillna(0)

    result.insert(1, "date", report_date)
    return result[DAILY_OPS_COLS_ORDER]


def process_daily_ops_metrics(gc: gspread.Client):
    """
    New tab 'Daily_Ops_Metrics': one row per BLR cluster for yesterday
    (the last fully-closed day) — see _compute_daily_ops_rows_for_date()
    for the full metric definitions/math, which this just feeds with a
    single day's worth of data from 3 fresh, single-day Metabase fetches.

    Every source is hard-filtered to city == "BLR" before any computation
    — no other city's rows ever reach a groupby here.

    Written as a running log via delete_rows_for_date_and_append() — see
    that function's docstring — not a full replace.
    """
    print("\n── STEP F: Daily Ops Metrics ──")
    report_date = get_yesterday()
    report_dt = datetime.strptime(report_date, "%Y-%m-%d").date()

    demand_df = fetch_metabase_csv_range(
        CARD_ID_DEMAND_METRICS, report_date, report_date, force_str_cols=DAILY_OPS_STR_COLS)
    demand_df = demand_df[demand_df["city_code"] == CITY].copy()
    demand_df = demand_df[demand_df["cluster_name"].notna()].copy()
    demand_df = demand_df.rename(columns={"cluster_name": "cluster"})

    token_df = fetch_metabase_csv_range(
        CARD_ID_TOKEN_FLOW, report_date, report_date, force_str_cols=DAILY_OPS_STR_COLS)
    token_df = token_df[token_df["city"] == CITY].copy()

    mech_df = fetch_metabase_csv_range(
        CARD_ID_MECH_FLOW, report_date, report_date, force_str_cols=DAILY_OPS_STR_COLS)
    mech_df = mech_df[mech_df["city"] == CITY].copy()
    # day_start_dt (the mechanic's SHIFT day) rather than task_start_dt (the
    # raw calendar date of the task timestamp) -- confirmed live these can
    # differ: a task at 05:48 on the 14th can carry day_start_dt=13th when
    # it belongs to a shift that started the evening of the 13th. Using
    # task_start_dt would misattribute overnight tasks to the wrong
    # business day, both for which date's row they land in AND for the
    # >90-day eligibility check below (which must be "as of" the correct day).
    mech_df["day_start_dt"]    = pd.to_datetime(mech_df["day_start_dt"], errors="coerce").dt.date
    mech_df["date_of_joining"] = pd.to_datetime(mech_df["date_of_joining"], errors="coerce").dt.date
    mech_df = mech_df[mech_df["day_start_dt"] == report_dt].copy()
    mech_df["bike_name"] = normalise_bike_id(mech_df["bike_name"])
    mech_df["days_old"] = mech_df["date_of_joining"].map(
        lambda d: (report_dt - d).days if pd.notna(d) else None)

    result = _compute_daily_ops_rows_for_date(demand_df, token_df, mech_df, report_date)

    delete_rows_for_date_and_append(
        gc, MASTER_SHEET_ID, DAILY_OPS_SHEET_TAB, result,
        date_col_letter="B", target_date=report_date, header=DAILY_OPS_COLS_ORDER,
    )
    return result


# ─────────────────────────────────────────────────────────────
# STEP F (BACKFILL) — same 3 cards, whole date range in one run
#
# Copies the exact calendar-month chunking + on-disk cache model already
# proven out for Mechanics>60 Audit / Cluster KPI in the reference sync
# script (pm_sync_full.py): _month_windows()/_is_month_closed(), one
# gzipped CSV cache file per (source, "YYYY-MM"), a CLOSED month served
# from disk with zero Metabase calls on every future run, the current/
# open month always fetched fresh.
# ─────────────────────────────────────────────────────────────
def _month_windows(start_date: str, end_date: str) -> list[tuple[str, str]]:
    """Split [start_date, end_date] into calendar-month windows."""
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    if start > end:
        return []
    windows = []
    cur = start
    while cur <= end:
        if cur.month == 12:
            next_month_start = cur.replace(year=cur.year + 1, month=1, day=1)
        else:
            next_month_start = cur.replace(month=cur.month + 1, day=1)
        window_end = min(end, next_month_start - timedelta(days=1))
        windows.append((cur.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
        cur = next_month_start
    return windows


def _is_month_closed(window_start: str, as_of_date: str) -> bool:
    """A month is "closed" (safe to cache forever) if it's not the same
    calendar month as_of_date itself falls in."""
    start = datetime.strptime(window_start, "%Y-%m-%d").date()
    as_of = datetime.strptime(as_of_date, "%Y-%m-%d").date()
    return (start.year, start.month) != (as_of.year, as_of.month)


def _daily_ops_month_cache_path(source_label: str, window_start: str) -> str:
    y_m = window_start[:7]  # "YYYY-MM"
    return os.path.join(DAILY_OPS_CACHE_DIR, f"{source_label}_{y_m}.csv.gz")


def _load_daily_ops_month_cache(source_label: str, window_start: str, force_str_cols: set):
    path = _daily_ops_month_cache_path(source_label, window_start)
    if not os.path.exists(path):
        return None
    try:
        peek = pd.read_csv(path, compression="gzip", nrows=0)
        dtype_map = {c: str for c in peek.columns if force_str_cols and c in force_str_cols}
        df = pd.read_csv(path, compression="gzip", dtype=dtype_map or None, low_memory=False)
        print(f"  [DailyOps/Cache] HIT {source_label} {window_start[:7]} ({len(df):,} rows) — no Metabase call.")
        return df
    except Exception as e:
        print(f"  [DailyOps/Cache] {source_label} {window_start[:7]} cache file unreadable ({e}) — refetching.")
        return None


def _save_daily_ops_month_cache(source_label: str, window_start: str, df: pd.DataFrame) -> None:
    if df.empty:
        # Never cache an empty result -- could be a genuine gap or a
        # transient Metabase hiccup; retried fresh next run either way.
        print(f"  [DailyOps/Cache] NOT caching {source_label} {window_start[:7]} "
              f"(0 rows) — retried fresh next run.")
        return
    path = _daily_ops_month_cache_path(source_label, window_start)
    try:
        os.makedirs(DAILY_OPS_CACHE_DIR, exist_ok=True)
        df.to_csv(path, index=False, compression="gzip")
        print(f"  [DailyOps/Cache] Cached {source_label} {window_start[:7]} ({len(df):,} rows) -> {path}")
    except Exception as e:
        print(f"  [DailyOps/Cache] Failed to cache {source_label} {window_start[:7]} ({e}) — will refetch next run.")


def _fetch_daily_ops_source_range(card_id: int, source_label: str, start_date: str, end_date: str,
                                   force_str_cols: set) -> pd.DataFrame:
    """
    Fetch one Daily Ops Metrics card for [start_date, end_date] in
    CALENDAR-MONTH chunks, caching CLOSED months to disk under
    DAILY_OPS_CACHE_DIR — no Metabase call at all for a month that's
    already cached from a prior run. The current/open month is always
    fetched fresh (ignore_cache=True — it's still accumulating today's
    rows, so a cached answer would be stale by definition).
    """
    frames = []
    cache_hits = cache_misses = 0
    for window_start, window_end in _month_windows(start_date, end_date):
        closed = _is_month_closed(window_start, end_date)

        cached_df = _load_daily_ops_month_cache(source_label, window_start, force_str_cols) if closed else None
        if cached_df is not None:
            frames.append(cached_df)
            cache_hits += 1
            continue

        print(f"  [DailyOps] Fetching {source_label} {window_start} -> {window_end}"
              f"{' (closed month, will cache)' if closed else ' (open month, never cached)'}...")
        df = fetch_metabase_csv_range(
            card_id, window_start, window_end, force_str_cols=force_str_cols, ignore_cache=True)
        if closed:
            _save_daily_ops_month_cache(source_label, window_start, df)
        cache_misses += 1
        if not df.empty:
            frames.append(df)

    print(f"  [DailyOps] {source_label} month cache summary: {cache_hits} hit(s), {cache_misses} fetched.")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


# Columns _compute_daily_ops_rows_for_date() needs present (even if empty)
# on the token/mechanic per-day slices, so a range with zero rows for a
# whole source doesn't KeyError on a column that would normally come from
# a non-empty Metabase CSV export.
_TOKEN_DAY_EMPTY_COLS = ["cluster", "token_id", "user_id", "token_type_derived",
                          "token_status", "checkin_to_fulfilled_tat_mins"]
_MECH_DAY_EMPTY_COLS = ["primary_role", "days_old", "QC pass/fail",
                         "live_repair_flag", "user_id", "bike_name", "start_cluster"]


def compute_daily_ops_metrics_range(start_date: str, end_date: str) -> dict[str, pd.DataFrame]:
    """
    Computes Daily Ops Metrics for EVERY date in [start_date, end_date],
    fetching each of the 3 Metabase cards ONCE for the whole range
    (month-chunked, closed months cached — see _fetch_daily_ops_source_range),
    then looping locally per date and calling
    _compute_daily_ops_rows_for_date() — the exact same math
    process_daily_ops_metrics() uses for a single day, just fed from a
    date-filtered slice of the bulk fetch instead of a fresh Metabase call
    per day. Returns {date: DataFrame} for every date with at least some
    Demand Metrics data; the caller decides which dates to actually write.
    """
    print(f"\n── Daily Ops Metrics backfill: {start_date} -> {end_date} ──")

    demand_all = _fetch_daily_ops_source_range(
        CARD_ID_DEMAND_METRICS, "demand", start_date, end_date, DAILY_OPS_STR_COLS)
    if demand_all.empty:
        print("  WARNING: no Demand Metrics data for the whole range — nothing to compute.")
        return {}
    demand_all = demand_all[demand_all["city_code"] == CITY].copy()
    demand_all = demand_all[demand_all["cluster_name"].notna()].copy()
    demand_all = demand_all.rename(columns={"cluster_name": "cluster"})

    token_all = _fetch_daily_ops_source_range(
        CARD_ID_TOKEN_FLOW, "token_flow", start_date, end_date, DAILY_OPS_STR_COLS)
    token_all = token_all[token_all["city"] == CITY].copy() if not token_all.empty else token_all

    mech_all = _fetch_daily_ops_source_range(
        CARD_ID_MECH_FLOW, "mech_flow", start_date, end_date, DAILY_OPS_STR_COLS)
    if not mech_all.empty:
        mech_all = mech_all[mech_all["city"] == CITY].copy()
        # day_start_dt (shift day), not task_start_dt (raw task timestamp's
        # calendar date) -- see the identical comment in
        # process_daily_ops_metrics() for why these can differ and why
        # day_start_dt is the correct one for date bucketing.
        mech_all["day_start_dt"]    = pd.to_datetime(mech_all["day_start_dt"], errors="coerce").dt.date
        mech_all["date_of_joining"] = pd.to_datetime(mech_all["date_of_joining"], errors="coerce").dt.date
        mech_all["bike_name"] = normalise_bike_id(mech_all["bike_name"])

    dates_in_range = sorted(demand_all["date"].dropna().unique().tolist())
    print(f"  {len(dates_in_range)} date(s) with Demand Metrics data in range.")

    results: dict[str, pd.DataFrame] = {}
    for d in dates_in_range:
        demand_day = demand_all[demand_all["date"] == d]

        if token_all.empty:
            token_day = pd.DataFrame(columns=_TOKEN_DAY_EMPTY_COLS)
        else:
            token_day = token_all[token_all["checkin_date"] == d].copy()

        if mech_all.empty:
            mech_day = pd.DataFrame(columns=_MECH_DAY_EMPTY_COLS)
        else:
            report_dt = datetime.strptime(d, "%Y-%m-%d").date()
            mech_day = mech_all[mech_all["day_start_dt"] == report_dt].copy()
            mech_day["days_old"] = mech_day["date_of_joining"].map(
                lambda dd: (report_dt - dd).days if pd.notna(dd) else None)

        day_result = _compute_daily_ops_rows_for_date(demand_day, token_day, mech_day, d)
        if not day_result.empty:
            results[d] = day_result
            print(f"    {d}: {len(day_result)} row(s) computed.")

    return results


def refresh_daily_ops_metrics_range(gc: gspread.Client, start_date: str, end_date: str) -> None:
    """
    Backfills 'Daily_Ops_Metrics' for every date in [start_date, end_date]
    that isn't already in the sheet. Unlike the daily run (which
    delete-then-appends exactly one day, so it can safely re-run today's
    row), this is a fill-the-gaps job: existing dates are read once up
    front and skipped, and every new date's rows are appended in ONE
    batch write at the end — appropriate for a wide historical range
    where a per-date Sheets round-trip would be far slower than the
    Metabase fetch itself.
    """
    print(f"\n── Daily Ops Metrics BACKFILL: {start_date} -> {end_date} ──")

    ss = gc.open_by_key(MASTER_SHEET_ID)
    try:
        ws = ss.worksheet(DAILY_OPS_SHEET_TAB)
        raw_date_vals = [v for v in ws.col_values(2)[1:] if v]  # column B = date
    except gspread.exceptions.WorksheetNotFound:
        print(f"  '{DAILY_OPS_SHEET_TAB}' does not exist yet — creating it...")
        ws = ss.add_worksheet(
            title=DAILY_OPS_SHEET_TAB, rows=1000, cols=max(len(DAILY_OPS_COLS_ORDER) + 2, 10))
        ws.append_row(DAILY_OPS_COLS_ORDER, value_input_option="RAW")
        raw_date_vals = []

    all_possible_dates = []
    cur = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()
    while cur <= end:
        all_possible_dates.append(cur.isoformat())
        cur += timedelta(days=1)

    existing_dates = {
        d for d in all_possible_dates if any(_matches_target_date(v, d) for v in raw_date_vals)
    } if raw_date_vals else set()
    print(f"  '{DAILY_OPS_SHEET_TAB}' already has {len(existing_dates)} date(s) in range — those will be skipped.")

    results = compute_daily_ops_metrics_range(start_date, end_date)

    rows_to_write = [df for d, df in sorted(results.items()) if d not in existing_dates]
    if not rows_to_write:
        print("  Nothing new to write — every date in range is already in the sheet.")
        return

    final_df = pd.concat(rows_to_write, ignore_index=True)
    values = clean_for_sheets(final_df)
    ws.append_rows(values, value_input_option="user_entered")
    print(f"  Appended {len(values)} row(s) across {len(rows_to_write)} date(s) to '{DAILY_OPS_SHEET_TAB}'.")


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="Yulu ETL — Sweep/Octopus/Stuck/Warehouse/Daily Ops Metrics sync."
    )
    parser.add_argument(
        "--daily-ops-backfill-start",
        default=None,
        help=(
            "If set (YYYY-MM-DD), run ONLY a Daily Ops Metrics backfill for "
            "[start, end] instead of the normal daily ETL — skips Sweep/"
            "Octopus/Stuck/Parts_Summary/Warehouse and the daily STEP F "
            "entirely. Fetches each of the 3 cards once for the whole "
            "range (month-chunked, closed months cached to disk) rather "
            "than once per day."
        ),
    )
    parser.add_argument(
        "--daily-ops-backfill-end",
        default=None,
        help="End date (YYYY-MM-DD) for --daily-ops-backfill-start. Defaults to yesterday.",
    )
    args = parser.parse_args()

    print("Authenticating with Google Sheets…")
    gc = get_gspread_client()

    if args.daily_ops_backfill_start:
        end_date = args.daily_ops_backfill_end or get_yesterday()
        refresh_daily_ops_metrics_range(gc, args.daily_ops_backfill_start, end_date)
        print("\n✅ Daily Ops Metrics backfill complete.")
        return

    sweep_df      = process_sweep(gc)
    process_octopus(gc)
    df_final, df2 = process_stuck(gc, sweep_df)
    process_parts_summary(gc, df_final, df2)
    process_warehouse(gc)

    try:
        process_daily_ops_metrics(gc)
    except Exception as e:
        # Non-blocking: a failure here (e.g. MetabaseQueryError, a missing
        # column on a report that changed shape) should never take down
        # the rest of the ETL run above it.
        print(f"  WARNING: Daily Ops Metrics step failed, skipping: {e}")

    print("\n✅ ETL complete.")


if __name__ == "__main__":
    main()
