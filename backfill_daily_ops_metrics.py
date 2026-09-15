"""
One-off backfill for the 'Daily_Ops_Metrics' tab.

WHY THIS EXISTS: etl_yulu.py's STEP F only ever computes YESTERDAY's row
each day it runs — that's correct for the ongoing daily job, but it means
the tab starts empty and only grows one day at a time from whenever you
first deploy it. This script fills in the history from a start date
(e.g. 2026-01-01) through yesterday, IN ONE RUN, so the Period Comparison
view (0-7d / 7-14d / 14-35d) has real trailing data immediately instead of
waiting ~5 weeks for it to accumulate naturally.

EFFICIENCY: each of the 3 Metabase cards (Demand Metrics, Token Flow,
Mechanic Flow Testing) is fetched EXACTLY ONCE for the whole date range —
they already accept {{start_date}}/{{end_date}} — then every date's
per-cluster metrics are computed locally in pandas from that one fetch.
This is NOT a loop that calls Metabase once per day (that would be ~250+
API calls for a Jan-1 backfill and take a very long time / risk rate
limits).

HOW TO RUN (locally, once):
    1. Make sure these env vars are set (same ones etl_yulu.py's GitHub
       Actions workflow uses — copy their values from your GitHub repo
       secrets, or from wherever you keep them):
           METABASE_URL
           METABASE_EMAIL
           METABASE_PASSWORD
           GOOGLE_SERVICE_ACCOUNT_JSON   (the full service account JSON,
                                          as one string) — or place a
                                          service_account.json file next
                                          to this script instead.
    2. pip install pandas requests gspread
    3. Place this file in the SAME folder as etl_yulu.py (it imports
       shared helpers/constants from it — nothing is duplicated).
    4. Run:
           python backfill_daily_ops_metrics.py --start 2026-01-01
       (defaults to 2026-01-01 if --start is omitted)

WHAT IT WRITES: appends one row per (date, cluster) — including the BLR
(Total) row — to the 'Daily_Ops_Metrics' tab, for every date in the range
that isn't already present there (so it's safe to re-run: it will only
fill in gaps, never duplicate a date it already wrote). This does NOT
touch any other tab (Sweep, Octopus, Stuck, Parts_Summary, Bikes_WHS).

CAVEAT — how far back real data actually exists: this script will happily
ask Metabase for 2026-01-01 onward, but if a card's underlying data only
starts later (e.g. Token Flow only has clean data from March), those
earlier dates will just come back with 0 rows for that source and the
affected metrics will be blank for those dates — not an error, just
"no data available yet" for that period. Check the cards directly in
Metabase if you want to confirm the real start date before running a
multi-month backfill.

CACHING (same pattern as pm_sync_full.py's Mechanics>60 Audit /
_fetch_repairs_range_for_audit): each of the 3 cards is fetched in
CALENDAR-MONTH chunks rather than one giant range request. A month that
has fully ENDED before the current month (a "closed" month — see
_is_month_closed) is cached to a local gzipped CSV under
DAILY_OPS_BACKFILL_CACHE_DIR (default ".cache/daily_ops_backfill_months")
the first time it's fetched; every later run (including a re-run after
this script was interrupted, or a future run that extends the end date)
loads that month straight from disk with NO Metabase call at all. The
CURRENT, still-open month is never cached — it's always fetched fresh,
since it's still accumulating today's rows. This is what makes the
backfill safe to re-run or resume: a Jan-1 backfill that dies halfway
through August, when re-run, skips straight past every already-cached
month (Jan-Jul) and only re-fetches August onward.
"""

import argparse
import os
from datetime import date, timedelta

import pandas as pd

import gspread

from etl_yulu import (
    CARD_ID_DEMAND_METRICS, CARD_ID_TOKEN_FLOW, CARD_ID_MECH_FLOW,
    CITY, MASTER_SHEET_ID, DAILY_OPS_SHEET_TAB, DAILY_OPS_STR_COLS,
    BLR_TOTAL_LABEL, MECH_PRODUCTIVITY_MIN_DAYS_OLD, LIVE_REPAIR_NORMALIZATION,
    fetch_metabase_csv_range, get_gspread_client, normalise_bike_id,
    clean_for_sheets, _token_fulfillment_metrics, get_yesterday,
    _matches_target_date,
)

# ─────────────────────────────────────────────────────────────
# FIXED-SIZE CHUNKED CACHING — mirrors pm_sync_full.py's PILOT TASK AUDIT
# caching (_day_windows / chunk-keyed cache), not the calendar-month one
# Mechanics Audit uses. Pilot Task Audit's report was heavy enough that a
# full calendar month (28-31 days) risked the fetch timeout, so it used
# fixed 15-day chunks instead — smaller, more predictable request size,
# and lighter on Metabase per call. Same reasoning applies here: we don't
# know in advance how heavy Demand Metrics / Token Flow / Mechanic Flow
# Testing are over a wide range, so DAILY_OPS_BACKFILL_CHUNK_DAYS (21 by
# default -- "3 weeks at once", per explicit request) is used instead of
# a variable-length calendar month.
# ─────────────────────────────────────────────────────────────
DAILY_OPS_BACKFILL_CACHE_DIR = os.environ.get(
    "DAILY_OPS_BACKFILL_CACHE_DIR", ".cache/daily_ops_backfill_chunks"
)
DAILY_OPS_BACKFILL_CHUNK_DAYS = int(os.environ.get("DAILY_OPS_BACKFILL_CHUNK_DAYS", "21"))


def _day_windows(start_date: str, end_date: str, days: int) -> list[tuple[str, str]]:
    """Split [start_date, end_date] into fixed-size windows of `days` calendar days each."""
    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)
    if start > end:
        return []
    windows = []
    cur = start
    while cur <= end:
        window_end = min(end, cur + timedelta(days=days - 1))
        windows.append((cur.strftime("%Y-%m-%d"), window_end.strftime("%Y-%m-%d")))
        cur = window_end + timedelta(days=1)
    return windows


def _chunk_cache_path(source_label: str, window_start: str, window_end: str) -> str:
    return os.path.join(DAILY_OPS_BACKFILL_CACHE_DIR, f"{source_label}_{window_start}_{window_end}.csv.gz")


def _load_chunk_from_cache(source_label: str, window_start: str, window_end: str,
                            force_str_cols: set) -> pd.DataFrame | None:
    path = _chunk_cache_path(source_label, window_start, window_end)
    if not os.path.exists(path):
        return None
    try:
        # Only force the same ID/text columns fetch_metabase_csv_range()
        # would have forced on a live fetch -- everything else is left to
        # pandas' normal numeric inference, so a cache HIT and a cache
        # MISS produce identically-typed DataFrames (dau/tat_mins/etc.
        # stay numeric either way; cluster/token_id/user_id/etc. stay
        # strings either way -- never round-tripped through float).
        peek = pd.read_csv(path, compression="gzip", nrows=0)
        dtype_map = {c: str for c in peek.columns if c in force_str_cols}
        df = pd.read_csv(path, compression="gzip", dtype=dtype_map or None)
        print(f"  [Cache] HIT {source_label} {window_start} -> {window_end} ({len(df):,} rows) — no Metabase call.")
        return df
    except Exception as e:
        print(f"  [Cache] {source_label} {window_start} -> {window_end} cache file unreadable ({e}) — refetching.")
        return None


def _save_chunk_to_cache(source_label: str, window_start: str, window_end: str, df: pd.DataFrame) -> None:
    if df.empty:
        # Never cache an empty result -- could be a genuine gap, or a
        # transient Metabase hiccup; retried fresh next run either way.
        return
    path = _chunk_cache_path(source_label, window_start, window_end)
    try:
        os.makedirs(DAILY_OPS_BACKFILL_CACHE_DIR, exist_ok=True)
        df.to_csv(path, index=False, compression="gzip")
        print(f"  [Cache] Cached {source_label} {window_start} -> {window_end} ({len(df):,} rows) -> {path}")
    except Exception as e:
        print(f"  [Cache] Failed to cache {source_label} {window_start} -> {window_end} ({e}) — will refetch next run.")


def fetch_range_cached(card_id: int, source_label: str, start_date: str, end_date: str,
                        force_str_cols: set) -> pd.DataFrame:
    """
    Fetch `card_id` for [start_date, end_date] in fixed
    DAILY_OPS_BACKFILL_CHUNK_DAYS-day chunks (21 by default -- "3 weeks at
    once"), serving CLOSED chunks (anything that ends before end_date,
    i.e. not the trailing/current chunk) from the on-disk cache with no
    Metabase call at all, and always fetching the last (still "open" in
    the sense that a re-run with a later end_date would extend it) chunk
    fresh.
    """
    frames = []
    cache_hits = cache_misses = 0
    windows = _day_windows(start_date, end_date, DAILY_OPS_BACKFILL_CHUNK_DAYS)
    for window_start, window_end in windows:
        closed = window_end < end_date  # ISO date strings compare correctly lexicographically

        cached_df = _load_chunk_from_cache(source_label, window_start, window_end, force_str_cols) if closed else None
        if cached_df is not None:
            frames.append(cached_df)
            cache_hits += 1
            continue

        print(f"  Fetching {source_label} {window_start} -> {window_end}"
              f"{' (closed chunk, will cache)' if closed else ' (trailing chunk, never cached)'}...")
        df = fetch_metabase_csv_range(card_id, window_start, window_end, force_str_cols=force_str_cols)
        if closed:
            _save_chunk_to_cache(source_label, window_start, window_end, df)
        cache_misses += 1
        frames.append(df)

    print(f"  {source_label}: {len(windows)} chunk(s) of {DAILY_OPS_BACKFILL_CHUNK_DAYS} day(s) -- "
          f"{cache_hits} cache hit(s), {cache_misses} fetched.")
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

COLS_ORDER = [
    "cluster", "date", "dau",
    "service_swap_fulfillment_pct_user", "service_swap_fulfillment_pct_token", "service_swap_tat_mins",
    "attachment_fulfillment_pct_user", "attachment_fulfillment_pct_token", "attachment_tat_mins",
    "mechanic_productivity_90d",
    "enquiry_total", "enquiry_to_attachment_pct",
]


def compute_one_date(demand_all: pd.DataFrame, token_all: pd.DataFrame,
                      mech_all: pd.DataFrame, report_date: str) -> pd.DataFrame:
    """
    Exactly the same per-date logic as etl_yulu.py's process_daily_ops_metrics(),
    just operating on a date-filtered SLICE of already-fetched full-range
    DataFrames instead of making a fresh Metabase call for this one date.
    """
    report_dt = date.fromisoformat(report_date)

    demand_df = demand_all[demand_all["date"] == report_date]
    if demand_df.empty:
        return pd.DataFrame(columns=COLS_ORDER)

    base = demand_df[["cluster", "dau"]].copy()
    base = pd.concat(
        [pd.DataFrame([{"cluster": BLR_TOTAL_LABEL, "dau": base["dau"].sum()}]), base],
        ignore_index=True,
    )

    token_df = token_all[token_all["checkin_date"] == report_date].copy()
    token_df = pd.concat(
        [token_df, token_df.assign(cluster=BLR_TOTAL_LABEL)], ignore_index=True)

    swap_metrics = _token_fulfillment_metrics(token_df, "service_swap", "service_swap")
    attach_metrics = _token_fulfillment_metrics(token_df, "Attach", "attachment")

    enquiry_df = token_df[token_df["token_type_derived"] == "Enquiry"]
    enquiry_total = enquiry_df.groupby("cluster")["token_id"].nunique().rename("enquiry_total")
    attach_users_by_cluster = (
        token_df[token_df["token_type_derived"] == "Attach"].groupby("cluster")["user_id"].apply(set)
    )
    enquiry_users_by_cluster = enquiry_df.groupby("cluster")["user_id"].apply(set)

    def _enquiry_to_attach_pct(cluster, users):
        if not users:
            return None
        attach_users = attach_users_by_cluster.get(cluster, set())
        return round(100 * len(users & attach_users) / len(users), 1)

    enquiry_pct = pd.Series(
        {c: _enquiry_to_attach_pct(c, u) for c, u in enquiry_users_by_cluster.items()},
        name="enquiry_to_attachment_pct",
    )
    enquiry_summary = (
        pd.concat([enquiry_total, enquiry_pct], axis=1).reset_index().rename(columns={"index": "cluster"})
    )

    mech_df = mech_all[mech_all["task_start_dt"] == report_dt].copy()
    eligible = mech_df[
        (mech_df["primary_role"] == "Maintenance")
        & mech_df["days_old"].notna()
        & (mech_df["days_old"] > MECH_PRODUCTIVITY_MIN_DAYS_OLD)
    ].copy()

    if eligible.empty:
        productivity = pd.DataFrame(columns=["cluster", "mechanic_productivity_90d"])
    else:
        eligible["qc_norm"] = eligible["QC pass/fail"].fillna("").astype(str).str.strip().str.lower()
        eligible["live_repair_flag"] = pd.to_numeric(eligible["live_repair_flag"], errors="coerce").fillna(0).astype(int)
        eligible = pd.concat(
            [eligible, eligible.assign(start_cluster=BLR_TOTAL_LABEL)], ignore_index=True)

        not_failed = eligible["qc_norm"] != "fail"
        regular_mask = not_failed & (eligible["live_repair_flag"] == 0)
        live_mask = not_failed & (eligible["live_repair_flag"] == 1)

        mechanic_counts = eligible.groupby("start_cluster")["user_id"].nunique().rename("mechanic_count")
        regular_counts = eligible[regular_mask].groupby("start_cluster")["bike_name"].nunique().rename("regular_bikes")
        live_counts = eligible[live_mask].groupby("start_cluster")["bike_name"].nunique().rename("live_bikes")

        productivity = pd.concat([mechanic_counts, regular_counts, live_counts], axis=1).fillna(0)
        productivity["mechanic_productivity_90d"] = productivity.apply(
            lambda r: round((r["regular_bikes"] + r["live_bikes"] / LIVE_REPAIR_NORMALIZATION) / r["mechanic_count"], 3)
            if r["mechanic_count"] else None, axis=1)
        productivity = productivity.reset_index().rename(columns={"start_cluster": "cluster"})[["cluster", "mechanic_productivity_90d"]]

    result = base.merge(swap_metrics, on="cluster", how="left")
    result = result.merge(attach_metrics, on="cluster", how="left")
    result = result.merge(productivity, on="cluster", how="left")
    result = result.merge(enquiry_summary, on="cluster", how="left")
    result["mechanic_productivity_90d"] = result["mechanic_productivity_90d"].fillna(0)
    result.insert(1, "date", report_date)
    return result[COLS_ORDER]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--start", default="2026-01-01", help="First date to backfill (YYYY-MM-DD)")
    args = parser.parse_args()

    start_date = args.start
    end_date = get_yesterday()
    print(f"Backfilling Daily_Ops_Metrics from {start_date} to {end_date} (inclusive)...")

    gc = get_gspread_client()
    ss = gc.open_by_key(MASTER_SHEET_ID)
    try:
        ws = ss.worksheet(DAILY_OPS_SHEET_TAB)
        raw_date_vals = [v for v in ws.col_values(2)[1:] if v]  # column B = date
    except gspread.exceptions.WorksheetNotFound:
        # Tab doesn't exist yet -- create it now (same header etl_yulu.py's
        # STEP F uses) rather than leaving `ws` unbound for the append at
        # the end of this function.
        print(f"'{DAILY_OPS_SHEET_TAB}' doesn't exist yet — creating it...")
        ws = ss.add_worksheet(title=DAILY_OPS_SHEET_TAB, rows=1000, cols=len(COLS_ORDER) + 2)
        ws.append_row(COLS_ORDER, value_input_option="RAW")
        raw_date_vals = []

    # Normalize each existing cell to a plain ISO date before comparing --
    # Google Sheets reformats a plain ISO string it recognizes as a date
    # (written via value_input_option="user_entered") into the
    # spreadsheet's locale format (e.g. DD/MM/YYYY) for display, so a raw
    # string-equality check against dates_in_range's ISO strings would
    # silently never match on a re-run and re-append every date. Reuses
    # etl_yulu.py's _matches_target_date() as the single source of truth
    # for which alternate formats are recognized.
    all_possible_dates = [
        (date.fromisoformat(start_date) + timedelta(days=n)).isoformat()
        for n in range((date.fromisoformat(end_date) - date.fromisoformat(start_date)).days + 1)
    ]
    existing_dates = {
        d for d in all_possible_dates
        if any(_matches_target_date(v, d) for v in raw_date_vals)
    } if raw_date_vals else set()
    print(f"'{DAILY_OPS_SHEET_TAB}' already has {len(existing_dates)} date(s) in range — those will be skipped.")

    print(f"Cache directory: {DAILY_OPS_BACKFILL_CACHE_DIR} "
          f"(closed months load from here on any re-run — no Metabase call).")

    print("\nFetching Demand Metrics (month-chunked, closed months cached)...")
    demand_all = fetch_range_cached(
        CARD_ID_DEMAND_METRICS, "demand", start_date, end_date, DAILY_OPS_STR_COLS)
    demand_all = demand_all[demand_all["city_code"] == CITY].copy()
    demand_all = demand_all[demand_all["cluster_name"].notna()].copy()
    demand_all = demand_all.rename(columns={"cluster_name": "cluster"})

    print("\nFetching Token Flow (month-chunked, closed months cached)...")
    token_all = fetch_range_cached(
        CARD_ID_TOKEN_FLOW, "token_flow", start_date, end_date, DAILY_OPS_STR_COLS)
    token_all = token_all[token_all["city"] == CITY].copy()

    print("\nFetching Mechanic Flow Testing (month-chunked, closed months cached)...")
    mech_all = fetch_range_cached(
        CARD_ID_MECH_FLOW, "mech_flow", start_date, end_date, DAILY_OPS_STR_COLS)
    mech_all = mech_all[mech_all["city"] == CITY].copy()
    mech_all["task_start_dt"] = pd.to_datetime(mech_all["task_start_dt"], errors="coerce").dt.date
    mech_all["date_of_joining"] = pd.to_datetime(mech_all["date_of_joining"], errors="coerce").dt.date
    mech_all["bike_name"] = normalise_bike_id(mech_all["bike_name"])

    dates_in_range = sorted(demand_all["date"].dropna().unique().tolist())
    dates_to_fill = [d for d in dates_in_range if d not in existing_dates]
    print(f"{len(dates_in_range)} date(s) with Demand Metrics data in range; "
          f"{len(dates_to_fill)} not yet in the sheet.")

    all_rows = []
    for d in dates_to_fill:
        report_dt = date.fromisoformat(d)
        mech_all["days_old"] = mech_all["date_of_joining"].map(
            lambda dd: (report_dt - dd).days if pd.notna(dd) else None)
        day_result = compute_one_date(demand_all, token_all, mech_all, d)
        if not day_result.empty:
            all_rows.append(day_result)
            print(f"  {d}: {len(day_result)} row(s) computed.")

    if not all_rows:
        print("Nothing new to write — every date in range is already in the sheet.")
        return

    final_df = pd.concat(all_rows, ignore_index=True)
    values = clean_for_sheets(final_df)
    ws.append_rows(values, value_input_option="user_entered")
    print(f"Appended {len(values)} row(s) across {len(dates_to_fill)} date(s) to '{DAILY_OPS_SHEET_TAB}'. Done.")


if __name__ == "__main__":
    main()
