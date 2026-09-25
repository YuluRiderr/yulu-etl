"""
Builds data.js — a static snapshot of the 'Daily_Ops_Metrics' tab (written
daily by etl_yulu.py's STEP F), pre-aggregated two ways:

  1. Period comparison per cluster and metric:
       latest   yesterday only (the most recent date captured)
       p0_7     trailing 7 days before yesterday (days 1-7 back)
       p7_14    the 7 days before that (days 8-14 back)
       p14_35   the 3 weeks before that (days 15-35 back)

  2. A daily time series (last SERIES_DAYS calendar days) per cluster and
     metric, for trend charts -- one shared "dates" axis plus a value
     array per (cluster, metric), null for any date with no row.

Run headless by .github/workflows/deploy-dashboard.yml — on push, on a
daily schedule, and right after refresh.yml's ETL workflow completes (see
that workflow's `workflow_run` trigger). Not interactive.

Reuses the exact same Google auth pattern and secret name
(GOOGLE_SERVICE_ACCOUNT_JSON) as etl_yulu.py, and the same MASTER_SHEET_ID
/ 'Daily_Ops_Metrics' tab that its STEP F writes to — nothing here talks to
Metabase directly.

Also fetches 'Daily_Ops_Metrics_ByCentre' (written alongside the main tab
by the same STEP F run) and aggregates it the same two ways, nested under
a separate top-level "by_centre" key in data.js — fulfillment%/TAT only,
grouped by individual Yulu Centre instead of cluster. Additive: the
existing top-level "clusters"/"dates"/"series" keys are untouched.
"""

import json
import os
import tempfile
from datetime import date, timedelta

import gspread

MASTER_SHEET_ID = "1fBjHKwlxRGwjsOSjzHOB6cUjvaKrhvtXdaPGeZjZuH0"
SHEET_TAB = "Daily_Ops_Metrics"
BYCENTRE_SHEET_TAB = "Daily_Ops_Metrics_ByCentre"
BLR_TOTAL_LABEL = "BLR (Total)"

METRICS = [
    "dau",
    "service_swap_fulfillment_pct_user", "service_swap_fulfillment_pct_token", "service_swap_tat_mins",
    "attachment_fulfillment_pct_user", "attachment_fulfillment_pct_token", "attachment_tat_mins",
    "mechanic_productivity_90d",
    "enquiry_total", "enquiry_to_attachment_pct",
]

# Fulfillment%/TAT only -- BYCENTRE_SHEET_TAB has no dau/mechanic
# productivity columns (see etl_yulu.py's
# _compute_daily_ops_rows_by_centre_for_date for why).
CENTRE_METRICS = [
    "service_swap_fulfillment_pct_user", "service_swap_fulfillment_pct_token", "service_swap_tat_mins",
    "attachment_fulfillment_pct_user", "attachment_fulfillment_pct_token", "attachment_tat_mins",
]
# Every metric, including enquiry_total, is AVERAGED per day over a
# period -- not summed. A summed window total isn't comparable against a
# single day's value (e.g. a 21-day sum vs. "Latest Day" always reads as
# a huge, meaningless drop), so every period column stays in "typical
# per-day" units the same way DAU/fulfillment%/productivity already are.
SUM_METRICS: set[str] = set()

PERIODS = [
    ("latest", 0, 0),
    ("p0_7", 1, 7),
    ("p7_14", 8, 14),
    ("p14_21", 15, 21),
    ("p21_28", 22, 28),
]

# How many trailing calendar days of daily-level detail to ship for trend
# charts. Independent of PERIODS above (which only needs 28 days) -- kept
# a little longer so a "last 60 days" trend line has room to breathe.
SERIES_DAYS = 60


def get_gspread_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(sa_json)
            tmp_path = f.name
        return gspread.service_account(filename=tmp_path)
    return gspread.service_account(filename="service_account.json")


def fetch_rows(tab: str) -> list[dict]:
    gc = get_gspread_client()
    ws = gc.open_by_key(MASTER_SHEET_ID).worksheet(tab)
    return ws.get_all_records()


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_period_comparison(by_cluster_date: dict, anchor: date, metrics: list[str] = METRICS) -> dict:
    clusters_out = {}
    for cluster, date_map in by_cluster_date.items():
        metrics_out = {}
        for metric in metrics:
            periods_out = {}
            for label, start_off, end_off in PERIODS:
                vals = []
                for offset in range(start_off, end_off + 1):
                    d = (anchor - timedelta(days=offset)).isoformat()
                    row = date_map.get(d)
                    if row is None:
                        continue
                    v = to_float(row.get(metric))
                    if v is not None:
                        vals.append(v)
                if not vals:
                    periods_out[label] = None
                elif metric in SUM_METRICS:
                    periods_out[label] = round(sum(vals), 2)
                else:
                    periods_out[label] = round(sum(vals) / len(vals), 2)
            metrics_out[metric] = periods_out
        clusters_out[cluster] = metrics_out
    return clusters_out


def build_series(by_cluster_date: dict, anchor: date, metrics: list[str] = METRICS) -> tuple[list[str], dict]:
    series_dates = [
        (anchor - timedelta(days=offset)).isoformat()
        for offset in range(SERIES_DAYS - 1, -1, -1)
    ]
    series_out = {}
    for cluster, date_map in by_cluster_date.items():
        metric_series = {}
        for metric in metrics:
            values = []
            for d in series_dates:
                row = date_map.get(d)
                values.append(to_float(row.get(metric)) if row else None)
            metric_series[metric] = values
        series_out[cluster] = metric_series
    return series_dates, series_out


def aggregate(records: list[dict], group_field: str = "cluster", metrics: list[str] = METRICS) -> dict:
    """
    `group_field`/`metrics` let this same function build either the
    cluster-level result (group_field="cluster", METRICS -- the original/
    default behaviour, unchanged) or the Yulu-Centre-wise result
    (group_field="yulu_centre", CENTRE_METRICS). The output dict's
    "clusters" key is really just "records keyed by whatever group_field
    was" in both cases -- kept as "clusters" even for the centre-wise
    call so the shape matches what the existing dashboard already expects
    at the top level; the centre-wise result is nested under its own
    "by_centre" key instead of replacing anything (see main()).
    """
    dates = sorted({r["date"] for r in records if r.get("date")})
    if not dates:
        return {"anchor_date": None, "days_captured": 0, "clusters": {}, "dates": [], "series": {}}

    anchor = date.fromisoformat(dates[-1])

    by_group_date: dict[str, dict[str, dict]] = {}
    for r in records:
        d, g = r.get("date"), r.get(group_field)
        if not d or not g:
            continue
        by_group_date.setdefault(g, {})[d] = r

    clusters_out = build_period_comparison(by_group_date, anchor, metrics)
    series_dates, series_out = build_series(by_group_date, anchor, metrics)

    return {
        "anchor_date": dates[-1],
        "days_captured": len(dates),
        "clusters": clusters_out,
        "dates": series_dates,
        "series": series_out,
    }


def main():
    try:
        records = fetch_rows(SHEET_TAB)
        print(f"Fetched {len(records)} row(s) from '{SHEET_TAB}'.")
    except Exception as e:
        print(f"WARNING: could not fetch '{SHEET_TAB}' ({e}); writing an empty data.js")
        records = []

    result = aggregate(records)

    # Yulu-Centre-wise fulfillment view -- a separate, additive top-level
    # key. Fetched/aggregated independently so a problem here (e.g. the
    # tab doesn't exist yet on a fresh deploy) never blocks the existing
    # cluster-level data above.
    try:
        centre_records = fetch_rows(BYCENTRE_SHEET_TAB)
        print(f"Fetched {len(centre_records)} row(s) from '{BYCENTRE_SHEET_TAB}'.")
    except Exception as e:
        print(f"WARNING: could not fetch '{BYCENTRE_SHEET_TAB}' ({e}); by_centre will be empty")
        centre_records = []

    result["by_centre"] = aggregate(centre_records, group_field="yulu_centre", metrics=CENTRE_METRICS)

    with open("data.js", "w", encoding="utf-8") as f:
        f.write("window.OPS_DATA = ")
        json.dump(result, f, indent=2)
        f.write(";\n")

    by_centre = result["by_centre"]
    print(
        f"Wrote data.js — anchor_date={result.get('anchor_date')}, "
        f"days_captured={result.get('days_captured')}, "
        f"{len(result.get('clusters', {}))} cluster(s), "
        f"{len(result.get('dates', []))} series date(s); "
        f"by_centre: {len(by_centre.get('clusters', {}))} centre(s), "
        f"{by_centre.get('days_captured', 0)} day(s) captured."
    )


if __name__ == "__main__":
    main()
