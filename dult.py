"""DULT: BLR Miracle KPIs for the DULT dashboard sheet -- rolling 365 days, month-cached."""

import argparse
import io
import json
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone
from time import sleep

import gspread
import pandas as pd
import requests

IST = timezone(timedelta(hours=5, minutes=30))

CARD_JOURNEYS = 1754
CARD_PM_GM = 9771
SHEET_ID = os.environ.get("DULT_SHEET_ID", "1SxxCBItWhrx2cVXsM36_plKyG2CDxKtnh_NnWIkIm1s")
TAB_SUMMARY, TAB_DAILY, TAB_PEAK = "KPI Summary", "KPI Daily", "KPI Peak Hours"

WINDOW_DAYS = 365
CACHE_AFTER_DAYS = 5
CACHE_DIR = os.environ.get("DULT_CACHE_DIR", ".cache/dult_months")
FETCH_WORKERS = 3

CITY = "BLR"
BIKE_CATEGORY = "Miracle"
BLR_TOTAL = "BLR (Total)"
OUTSIDE = "Outside cluster"
# Only these clusters get their own rows and count towards BLR (Total); every other cluster is folded into OUTSIDE.
KEEP_CLUSTERS = ["CBD", "Electronic City", "Hebbal", "Indiranagar", "Jayanagar", "Koramangala", "ORR", "Whitefield"]
# Known clusters that are folded into OUTSIDE. Any cluster name in neither list is dropped entirely.
FOLD_CLUSTERS = ["BLR_DSU", "Cluster 1", "Cluster 2", "Cluster 3", "Yelahanka", "Outside cluster"]
CACHE_TAG = "v3"  # bump when KEEP_CLUSTERS or the folding rules change: old cache files are then ignored

START_CLUSTER_COL = "Start Operation Cluster Parent Geozone Name"
END_CLUSTER_COL = "End Operation Cluster Parent Geozone Name"
J_REQUIRED = {"journey_start_dt", "Journey_Id", "User Id", "Journey_Type",
              "Journey Distance", "Total Journey Time", "Start Time", START_CLUSTER_COL}
J_OPTIONAL = {"City", "Bike Category", "End Time", END_CLUSTER_COL}
DAILY_COLS = ["date", "cluster", "trips_started", "trips_ended", "total_km", "total_minutes", "active_users"]
HOURLY_COLS = ["date", "cluster", "hour", "trips"]
BIKE_COLS = ["date", "cluster", "deployed", "not_operational"]

DAILY_OUT = ["date", "cluster", "deployed_bikes", "not_operational_bikes", "operational_bikes",
             "trips_started", "trips_ended", "total_km", "active_users",
             "avg_km_per_user", "avg_ride_min_per_user", "avg_trip_duration_min"]
MONTHLY_OUT = ["month", "cluster", "days_in_window", "avg_deployed_bikes", "avg_operational_bikes",
               "trips_started", "trips_ended", "total_km", "avg_daily_km", "user_days",
               "avg_km_per_user", "avg_ride_min_per_user", "avg_trip_duration_min"]
PEAK_SUMMARY_OUT = ["cluster", "day_type", "peak_hour", "second_hour", "third_hour",
                    "best_2h_window", "best_2h_share_pct", "avg_trips_per_day"]
PEAK_OUT = ["cluster", "day_type", "hour", "avg_trips_per_day", "share_of_day_pct"]

DEFINITIONS = [
    f"Clusters: {', '.join(KEEP_CLUSTERS)} are reported individually and make up BLR (Total). {', '.join(FOLD_CLUSTERS[:-1])} and trips with no cluster are folded into Outside cluster, which is not part of BLR (Total). Any other cluster name is dropped.",
    "Trip = shared journey of a Miracle bike in BLR, any ride status. Long-term-rental rows are excluded (0 km, not trips).",
    "A trip counts on the day it starts and in the cluster it starts in. Trips ended = same trips grouped by the cluster they end in.",
    "Active users = distinct riders. BLR (Total) is distinct across all of BLR, so it is not the sum of the clusters.",
    "Avg km / ride minutes per user = total km / minutes divided by active users. Monthly = total divided by the sum of daily active users.",
    "Avg trip duration = total ride minutes / trips (start to end, pauses included).",
    "Deployed bikes = sum of pm_bor. A day with pm_bor of 0 is treated as missing and filled with the average of the last and next day that had bikes (same for not operational). Not operational = sum of pm_fault_bikes_in_cluster. Operational = deployed minus not operational. BLR (Total) = sum of the eight named clusters.",
    "Peak hours = trips by start hour, averaged per day over the window (days with no trips count as zero).",
]


class MetabaseQueryError(RuntimeError):
    pass


_session = {"token": None}
_session_lock = threading.Lock()
_tag_cache = {}


def _base_url():
    return os.environ["METABASE_URL"].rstrip("/")


def _login(force=False):
    with _session_lock:
        if _session["token"] and not force:
            return _session["token"]
        resp = requests.post(
            f"{_base_url()}/api/session",
            json={"username": os.environ["METABASE_EMAIL"], "password": os.environ["METABASE_PASSWORD"]},
            timeout=60,
        )
        resp.raise_for_status()
        _session["token"] = resp.json()["id"]
        return _session["token"]


def _with_retries(fn, what, attempts=5, delay=3):
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except MetabaseQueryError as e:
            if "exhausted resources" not in str(e).lower() or attempt == attempts:
                raise
            reason = "Athena capacity"
        except (requests.exceptions.RequestException, gspread.exceptions.APIError) as e:
            response = getattr(e, "response", None)
            status = getattr(response, "status_code", None)
            if status == 401:
                _login(force=True)
            transient = (status in (401, 429, 500, 502, 503, 504)
                         or isinstance(e, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)))
            if not transient or attempt == attempts:
                raise
            reason = status or type(e).__name__
        print(f"  retry {attempt}/{attempts - 1} for {what} ({reason}) in {delay}s")
        sleep(delay)
        delay *= 2


def _looks_like_error(text):
    head = text.lstrip()[:2000]
    return head.startswith("{") and ('"status":"failed"' in head or '"error_type":' in head
                                     or ('"error":' in head and '"started_at":' in head))


def template_tags(card_id):
    if card_id not in _tag_cache:
        def call():
            resp = requests.get(f"{_base_url()}/api/card/{card_id}",
                                headers={"X-Metabase-Session": _login()}, timeout=60)
            resp.raise_for_status()
            return resp.json()
        card = _with_retries(call, f"card {card_id} metadata")
        tags = ((card.get("dataset_query") or {}).get("native") or {}).get("template-tags") or {}
        _tag_cache[card_id] = {k.lower(): {**t, "name": t.get("name", k)} for k, t in tags.items()}
    return _tag_cache[card_id]


def build_parameters(card_id, values):
    tags = template_tags(card_id)
    missing = [k for k in ("start_date", "end_date") if k not in tags]
    if missing:
        raise RuntimeError(f"card {card_id} has no template tag {missing}; available: {sorted(tags)}")
    params = []
    for key, value in values.items():
        tag = tags.get(key.lower())
        if tag is None:
            print(f"  note: card {card_id} has no '{key}' parameter (available: {sorted(tags)}); filtering in-script instead")
            continue
        name, ttype = tag["name"], tag.get("type", "text")
        if ttype == "dimension":
            widget = tag.get("widget-type") or "string/="
            params.append({"type": widget, "target": ["dimension", ["template-tag", name]],
                           "value": value if widget.startswith("date") else [value]})
        else:
            params.append({"type": "date/single" if ttype == "date" else "text",
                           "target": ["variable", ["template-tag", name]], "value": value})
    return params


def fetch_csv(card_id, values, timeout=900):
    def call():
        resp = requests.post(
            f"{_base_url()}/api/card/{card_id}/query/csv",
            data={"parameters": json.dumps(build_parameters(card_id, values)), "ignore_cache": "true"},
            headers={"X-Metabase-Session": _login()},
            timeout=timeout,
        )
        resp.raise_for_status()
        if _looks_like_error(resp.text):
            raise MetabaseQueryError(f"card {card_id}: {resp.text[:600]}")
        return resp.text.lstrip("﻿")
    return _with_retries(call, f"card {card_id} {values.get('start_date')}..{values.get('end_date')}")


def _fold_cluster(series):
    """Map raw names to a kept name or OUTSIDE; names on neither list become None (dropped by the caller)."""
    norm = lambda c: " ".join(c.lower().split()).removesuffix(" cluster")
    keep = {norm(c): c for c in KEEP_CLUSTERS}
    fold = {norm(c) for c in FOLD_CLUSTERS} | {""}
    key = series.fillna("").astype(str).map(norm)
    out = key.map(keep)
    out = out.where(out.notna(), key.map(lambda k: OUTSIDE if k in fold else None))
    return out


def aggregate_journeys(text):
    empty = (pd.DataFrame(columns=DAILY_COLS), pd.DataFrame(columns=HOURLY_COLS))
    if not text.strip():
        return empty
    wanted = J_REQUIRED | J_OPTIONAL
    df = pd.read_csv(io.StringIO(text), usecols=lambda c: c.strip() in wanted, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    missing = sorted(J_REQUIRED - set(df.columns))
    if missing:
        raise RuntimeError(f"journey report is missing columns {missing}")

    raw_rows = len(df)
    df = df.drop_duplicates(subset="Journey_Id")
    dupes = raw_rows - len(df)
    if "City" in df.columns:
        df = df[df["City"].fillna("").str.strip().str.upper() == CITY]
    if "Bike Category" in df.columns:
        df = df[df["Bike Category"].fillna("").str.strip().str.lower() == BIKE_CATEGORY.lower()]
    is_ltr = df["Journey_Type"].fillna("").str.lower().str.contains("ltr")
    print(f"  journeys: {raw_rows} rows, {dupes} duplicate ids dropped, {int(is_ltr.sum())} long-term-rental excluded")
    df = df[~is_ltr].copy()
    if df.empty:
        return empty

    df["date"] = df["journey_start_dt"].str[:10]
    df["km"] = pd.to_numeric(df["Journey Distance"], errors="coerce").fillna(0.0)
    df["minutes"] = pd.to_numeric(df["Total Journey Time"], errors="coerce").fillna(0.0) / 60.0
    df["hour"] = pd.to_numeric(df["Start Time"].fillna("").str[:2], errors="coerce")
    df["start_cluster"] = _fold_cluster(df[START_CLUSTER_COL])
    unknown = df[START_CLUSTER_COL].fillna("")[df["start_cluster"].isna()]
    if len(unknown):
        print(f"  journeys: dropped {len(unknown)} trips from unlisted clusters {sorted(set(unknown))[:10]}")
    df = df[df["start_cluster"].notna()].copy()
    if df.empty:
        return empty
    has_end = df["End Time"].fillna("").str.strip() != "" if "End Time" in df.columns else pd.Series(True, index=df.index)
    end_col = df[END_CLUSTER_COL] if END_CLUSTER_COL in df.columns else pd.Series("", index=df.index)
    df["end_cluster"] = _fold_cluster(end_col)
    df["ended"] = has_end

    started = (df.groupby(["date", "start_cluster"])
               .agg(trips_started=("Journey_Id", "size"), total_km=("km", "sum"),
                    total_minutes=("minutes", "sum"), active_users=("User Id", "nunique"))
               .reset_index().rename(columns={"start_cluster": "cluster"}))
    ended = (df[df["ended"]].groupby(["date", "end_cluster"]).size().rename("trips_ended")
             .reset_index().rename(columns={"end_cluster": "cluster"}))
    daily = started.merge(ended, on=["date", "cluster"], how="outer").fillna(0)

    df_in = df[df["start_cluster"] != OUTSIDE]  # BLR (Total) = kept clusters only
    total = (df_in.groupby("date")
             .agg(trips_started=("Journey_Id", "size"), total_km=("km", "sum"),
                  total_minutes=("minutes", "sum"), active_users=("User Id", "nunique"))
             .reset_index())
    total["trips_ended"] = total["date"].map(df_in[df_in["ended"]].groupby("date").size()).fillna(0)
    total["cluster"] = BLR_TOTAL
    daily = pd.concat([daily[DAILY_COLS], total[DAILY_COLS]], ignore_index=True)

    timed = df[df["hour"].between(0, 23)].copy()
    timed["hour"] = timed["hour"].astype(int)
    hourly = (timed.groupby(["date", "start_cluster", "hour"]).size().rename("trips")
              .reset_index().rename(columns={"start_cluster": "cluster"}))
    hourly_total = timed[timed["start_cluster"] != OUTSIDE].groupby(["date", "hour"]).size().rename("trips").reset_index()
    hourly_total["cluster"] = BLR_TOTAL
    hourly = pd.concat([hourly[HOURLY_COLS], hourly_total[HOURLY_COLS]], ignore_index=True)
    return daily, hourly


def parse_bikes(text):
    if not text.strip():
        return pd.DataFrame(columns=BIKE_COLS)
    wanted = {"date", "city", "cluster_name", "pm_bor", "pm_fault_bikes_in_cluster"}
    df = pd.read_csv(io.StringIO(text), usecols=lambda c: c.strip() in wanted, dtype=str)
    df.columns = [c.strip() for c in df.columns]
    missing = sorted(wanted - set(df.columns))
    if missing:
        raise RuntimeError(f"PM/GM report is missing columns {missing}")
    df = df[df["city"].fillna("").str.strip().str.upper() == CITY]
    df = df[df["cluster_name"].fillna("").str.strip() != ""].copy()
    df["date"] = df["date"].str[:10]
    df["cluster"] = _fold_cluster(df["cluster_name"])
    df = df[df["cluster"].notna()]
    df["deployed"] = pd.to_numeric(df["pm_bor"], errors="coerce").fillna(0)
    df["not_operational"] = pd.to_numeric(df["pm_fault_bikes_in_cluster"], errors="coerce").fillna(0)
    return df.groupby(["date", "cluster"], as_index=False)[["deployed", "not_operational"]].sum()


def _cache_path(source, name, first):
    return os.path.join(CACHE_DIR, f"{source}_{name}_{CACHE_TAG}_{first:%Y-%m}.csv.gz")


def _read_cache(source, name, first):
    path = _cache_path(source, name, first)
    if not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, compression="gzip", dtype={"date": str, "cluster": str})
    except Exception as e:
        print(f"  [{source}] cache file {path} unreadable ({e}); refetching")
        return None


def _write_cache(source, name, first, df):
    os.makedirs(CACHE_DIR, exist_ok=True)
    df.to_csv(_cache_path(source, name, first), index=False, compression="gzip")


def month_ranges(win_start, yesterday):
    out, cur = [], win_start.replace(day=1)
    while cur <= yesterday:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)
        out.append((cur, nxt - timedelta(days=1)))
        cur = nxt
    return out


def month_tables(source, names, first, last, yesterday, today, fetch):
    # A month is frozen into the cache only once it has been over for CACHE_AFTER_DAYS (late data settles).
    cacheable = (today - last).days >= CACHE_AFTER_DAYS
    if cacheable:
        cached = {n: _read_cache(source, n, first) for n in names}
        if all(v is not None for v in cached.values()):
            print(f"  [{source}] {first:%Y-%m}: cache hit")
            return cached
    end = last if cacheable else min(last, yesterday)
    print(f"  [{source}] {first:%Y-%m}: fetching {first} -> {end}" + (" (will cache)" if cacheable else ""))
    tables = fetch(first, end)
    if cacheable and any(not t.empty for t in tables.values()):
        for n, t in tables.items():
            _write_cache(source, n, first, t)
    return tables


def load_source(source, names, months, yesterday, today, fetch):
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        futures = [pool.submit(month_tables, source, names, f, l, yesterday, today, fetch) for f, l in months]
        results = [f.result() for f in futures]
    out = {}
    for n in names:
        frames = [r[n] for r in results if not r[n].empty]
        out[n] = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=results[0][n].columns)
    return out


def fetch_journeys(first, end):
    text = fetch_csv(CARD_JOURNEYS, {"start_date": str(first), "end_date": str(end),
                                     "bike_category": BIKE_CATEGORY, "city": CITY})
    daily, hourly = aggregate_journeys(text)
    return {"daily": daily, "hourly": hourly}


def fetch_bikes(first, end):
    text = fetch_csv(CARD_PM_GM, {"start_date": str(first), "end_date": str(end)}, timeout=300)
    return {"bikes": parse_bikes(text)}


def _name_key(name):
    k = " ".join(str(name).strip().lower().split())
    return k[:-len(" cluster")] if k.endswith(" cluster") else k


def canonicalise_clusters(daily, hourly, bikes):
    display = {}
    for df in (bikes, daily, hourly):
        for raw in df["cluster"].unique():
            if raw not in (BLR_TOTAL, OUTSIDE):
                display.setdefault(_name_key(raw), raw)
    only_one = {}
    for label, df in (("journeys", daily), ("bikes", bikes)):
        for raw in df["cluster"].unique():
            if raw not in (BLR_TOTAL, OUTSIDE):
                only_one.setdefault(_name_key(raw), set()).add(label)
    for key, labels in sorted(only_one.items()):
        if len(labels) == 1:
            print(f"  note: cluster '{display[key]}' appears only in {next(iter(labels))}")
    fixed = []
    for df in (daily, hourly, bikes):
        df = df.copy()
        df["cluster"] = df["cluster"].map(lambda c: c if c in (BLR_TOTAL, OUTSIDE) else display[_name_key(c)])
        fixed.append(df)
    return fixed


def _neighbour_mean(known):
    """Fill gaps with the mean of the previous and next known value (one side only at the ends)."""
    prev, nxt = known.ffill(), known.bfill()
    return ((prev + nxt) / 2).fillna(prev).fillna(nxt)


def fill_zero_bor(bikes, dates):
    """pm_bor of 0 (or no row) is a data gap, not an empty cluster: replace it with the average of the last
    and the next day that had bikes. Not-operational is filled the same way on those days so operational
    stays consistent. A cluster that never has bikes is left alone."""
    all_dates = sorted(set(dates) | set(bikes["date"]))
    parts = []
    for cluster, g in bikes.groupby("cluster"):
        g = g.drop_duplicates("date").set_index("date").reindex(all_dates)
        known = g["deployed"] > 0
        if not known.any():
            parts.append(g.reindex(dates).assign(cluster=cluster).reset_index(names="date"))
            continue
        dep = _neighbour_mean(g["deployed"].where(known))
        nop = _neighbour_mean(g["not_operational"].where(known))
        g["deployed"] = g["deployed"].where(known, dep)
        g["not_operational"] = g["not_operational"].where(known, nop)
        parts.append(g.reindex(dates).assign(cluster=cluster).reset_index(names="date"))
    out = pd.concat(parts, ignore_index=True) if parts else pd.DataFrame(columns=BIKE_COLS)
    return out[BIKE_COLS]


def _div(num, den):
    return num / den.where(den > 0)


def build_tables(daily_j, hourly_j, bikes, win_start, yesterday):
    dates = [(win_start + timedelta(days=i)).isoformat() for i in range((yesterday - win_start).days + 1)]
    daily_j, hourly_j, bikes = canonicalise_clusters(daily_j, hourly_j, bikes)
    named = sorted((set(daily_j["cluster"]) | set(hourly_j["cluster"]) | set(bikes["cluster"])) - {BLR_TOTAL, OUTSIDE},
                   key=str.lower)
    order = [BLR_TOTAL, *named, OUTSIDE]
    index = pd.MultiIndex.from_product([dates, order], names=["date", "cluster"])

    base = (daily_j[daily_j["date"].isin(dates)].groupby(["date", "cluster"])
            [["trips_started", "trips_ended", "total_km", "total_minutes", "active_users"]].sum()
            .reindex(index).fillna(0))

    bk = fill_zero_bor(bikes, dates)
    bk_total = bk[bk["cluster"] != OUTSIDE].groupby("date", as_index=False)[["deployed", "not_operational"]].sum()
    bk_total["cluster"] = BLR_TOTAL
    bk = pd.concat([bk[BIKE_COLS], bk_total[BIKE_COLS]], ignore_index=True)
    b = bk.groupby(["date", "cluster"])[["deployed", "not_operational"]].sum().reindex(index)
    base["deployed_bikes"] = b["deployed"]
    base["not_operational_bikes"] = b["not_operational"]
    not_outside = base.index.get_level_values("cluster") != OUTSIDE
    for col in ("deployed_bikes", "not_operational_bikes"):
        base.loc[not_outside, col] = base.loc[not_outside, col].fillna(0)
    base["operational_bikes"] = base["deployed_bikes"] - base["not_operational_bikes"]

    base["avg_km_per_user"] = _div(base["total_km"], base["active_users"])
    base["avg_ride_min_per_user"] = _div(base["total_minutes"], base["active_users"])
    base["avg_trip_duration_min"] = _div(base["total_minutes"], base["trips_started"])
    daily = base.reset_index()
    for col in ("trips_started", "trips_ended", "active_users"):
        daily[col] = daily[col].astype(int)
    d = daily.copy()
    d["month"] = d["date"].str[:7]
    monthly = (d.groupby(["month", "cluster"], sort=False)
               .agg(days_in_window=("date", "nunique"), avg_deployed_bikes=("deployed_bikes", "mean"),
                    avg_operational_bikes=("operational_bikes", "mean"), trips_started=("trips_started", "sum"),
                    trips_ended=("trips_ended", "sum"), total_km=("total_km", "sum"),
                    total_minutes=("total_minutes", "sum"), user_days=("active_users", "sum"))
               .reset_index())
    monthly["avg_daily_km"] = monthly["total_km"] / monthly["days_in_window"]
    monthly["avg_km_per_user"] = _div(monthly["total_km"], monthly["user_days"])
    monthly["avg_ride_min_per_user"] = _div(monthly["total_minutes"], monthly["user_days"])
    monthly["avg_trip_duration_min"] = _div(monthly["total_minutes"], monthly["trips_started"])
    monthly = monthly[MONTHLY_OUT]

    peak_summary, peak_hours = build_peak(hourly_j, dates, order)
    total_trips = daily[daily["cluster"] == BLR_TOTAL].set_index("date")["trips_started"]
    zero_days = [x for x in dates if total_trips.get(x, 0) == 0]
    return {"daily": daily, "monthly": monthly, "peak_summary": peak_summary, "peak_hours": peak_hours,
            "zero_days": zero_days, "dates": dates}


def build_peak(hourly, dates, order):
    weekend = {s for s in dates if date.fromisoformat(s).weekday() >= 5}
    day_types = {"All": set(dates), "Weekday": set(dates) - weekend, "Weekend": weekend}
    h = hourly[hourly["date"].isin(dates)]
    summary_rows, hour_rows = [], []
    for cluster in order:
        hc = h[h["cluster"] == cluster]
        for day_type, days in day_types.items():
            n = len(days)
            totals = hc[hc["date"].isin(days)].groupby("hour")["trips"].sum().reindex(range(24), fill_value=0)
            avg = (totals / n) if n else totals * 0.0
            day_total = float(avg.sum())
            for hour in range(24):
                hour_rows.append([cluster, day_type, hour, float(avg[hour]),
                                  float(avg[hour] / day_total * 100) if day_total else None])
            if not day_total:
                summary_rows.append([cluster, day_type, None, None, None, None, None, 0.0])
                continue
            top = sorted(range(24), key=lambda x: (-avg[x], x))[:3]
            pair = max(range(23), key=lambda x: (avg[x] + avg[x + 1], -x))
            window_share = float((avg[pair] + avg[pair + 1]) / day_total * 100)
            summary_rows.append([cluster, day_type, top[0], top[1], top[2],
                                 f"{pair:02d}:00-{pair + 2:02d}:00", window_share, day_total])
    return pd.DataFrame(summary_rows, columns=PEAK_SUMMARY_OUT), pd.DataFrame(hour_rows, columns=PEAK_OUT)


def _cell(v):
    if v is None:
        return ""
    if isinstance(v, bool):
        return v
    if hasattr(v, "item"):
        v = v.item()
    if isinstance(v, float):
        return "" if v != v else round(v, 4)
    return v


def _records(df, cols):
    return [[_cell(v) for v in row] for row in df[cols].itertuples(index=False, name=None)]


def summary_rows(tables, win_start, yesterday, refreshed):
    daily, monthly = tables["daily"], tables["monthly"]
    latest = daily[daily["date"] == yesterday.isoformat()]
    rows = [
        ["DULT - BLR KPIs (Miracle)"],
        ["Data through", yesterday.isoformat(), "Window", f"{win_start} to {yesterday} ({WINDOW_DAYS} days)"],
        ["Last refreshed (IST)", refreshed],
        ["Days with no trips", ", ".join(tables["zero_days"]) or "none"],
        [""], ["DEFINITIONS"], *[[t] for t in DEFINITIONS], [""],
        [f"LATEST DAY ({yesterday})"], DAILY_OUT, *_records(latest, DAILY_OUT), [""],
        ["MONTHLY (days_in_window shows partial months)"], MONTHLY_OUT, *_records(monthly, MONTHLY_OUT), [""],
        ["PEAK HOURS - SUMMARY (hours are start hours, 24h clock)"], PEAK_SUMMARY_OUT,
        *_records(tables["peak_summary"], PEAK_SUMMARY_OUT),
    ]
    return rows


def _client():
    raw = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if raw:
        return gspread.service_account_from_dict(json.loads(raw))
    return gspread.service_account(filename="service_account.json")


def write_tab(ss, title, rows):
    width = max(len(r) for r in rows)
    try:
        ws = ss.worksheet(title)
        _with_retries(ws.clear, f"clear '{title}'")
    except gspread.exceptions.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=len(rows) + 20, cols=max(width, 12))
    if ws.row_count < len(rows) + 5 or ws.col_count < width:
        ws.resize(rows=max(ws.row_count, len(rows) + 5), cols=max(ws.col_count, width))
    _with_retries(lambda: ws.update(values=rows, range_name="A1"), f"write '{title}'")
    print(f"  '{title}': wrote {len(rows)} rows")


def run(dry_run=False):
    now = datetime.now(IST)
    today = now.date()
    yesterday = today - timedelta(days=1)
    win_start = yesterday - timedelta(days=WINDOW_DAYS - 1)
    months = month_ranges(win_start, yesterday)
    print(f"DULT window {win_start} -> {yesterday} ({WINDOW_DAYS} days), {len(months)} month(s)")

    journeys = load_source("journeys", ["daily", "hourly"], months, yesterday, today, fetch_journeys)
    bikes = load_source("bikes", ["bikes"], months, yesterday, today, fetch_bikes)
    tables = build_tables(journeys["daily"], journeys["hourly"], bikes["bikes"], win_start, yesterday)

    daily = tables["daily"]
    print(f"daily rows {len(daily)}, monthly rows {len(tables['monthly'])}, "
          f"peak rows {len(tables['peak_hours'])}, zero-trip days {len(tables['zero_days'])}")
    if dry_run:
        print(daily[daily["date"] == yesterday.isoformat()].to_string(index=False))
        return tables

    ss = _client().open_by_key(SHEET_ID)
    refreshed = now.strftime("%Y-%m-%d %H:%M")
    write_tab(ss, TAB_DAILY, [DAILY_OUT, *_records(daily, DAILY_OUT)])
    write_tab(ss, TAB_PEAK, [PEAK_OUT, *_records(tables["peak_hours"], PEAK_OUT)])
    write_tab(ss, TAB_SUMMARY, summary_rows(tables, win_start, yesterday, refreshed))

    recent = [x for x in tables["zero_days"] if x >= (yesterday - timedelta(days=2)).isoformat()]
    if tables["zero_days"]:
        print(f"::warning title=DULT::days with no trips: {', '.join(tables['zero_days'][:20])}")
    if recent:
        raise RuntimeError(f"no trips found for recent day(s) {recent}; sheet was updated but data looks incomplete")
    return tables


def main():
    parser = argparse.ArgumentParser(description="DULT BLR KPI refresh")
    parser.add_argument("--dry-run", action="store_true", help="compute everything but do not write the sheet")
    args = parser.parse_args()
    try:
        run(dry_run=args.dry_run)
    except Exception as e:
        message = f"{type(e).__name__}: {e}".replace("\r", " ").replace("\n", " ")[:700]
        print(f"::error title=DULT failed::{message}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
