"""
Builds data.js — a static snapshot of the 'Daily_Ops_Metrics' tab (written
daily by etl_yulu.py's STEP F), pre-aggregated into period-comparison KPIs
per cluster and metric:

    latest   yesterday only (the most recent date captured)
    p0_7     trailing 7 days before yesterday (days 1-7 back)
    p7_14    the 7 days before that (days 8-14 back)
    p14_35   the 3 weeks before that (days 15-35 back)

Run headless by .github/workflows/deploy-dashboard.yml — on push and on a
daily schedule shortly after refresh.yml's ETL run. Not interactive.

Reuses the exact same Google auth pattern and secret name
(GOOGLE_SERVICE_ACCOUNT_JSON) as etl_yulu.py, and the same MASTER_SHEET_ID
/ 'Daily_Ops_Metrics' tab that its STEP F writes to — nothing here talks to
Metabase directly.
"""

import json
import os
import tempfile
from datetime import date, timedelta

import gspread

MASTER_SHEET_ID = "1fBjHKwlxRGwjsOSjzHOB6cUjvaKrhvtXdaPGeZjZuH0"
SHEET_TAB = "Daily_Ops_Metrics"
BLR_TOTAL_LABEL = "BLR (Total)"

METRICS = [
    "dau",
    "service_swap_fulfillment_pct_user", "service_swap_fulfillment_pct_token", "service_swap_tat_mins",
    "attachment_fulfillment_pct_user", "attachment_fulfillment_pct_token", "attachment_tat_mins",
    "mechanic_productivity_90d",
    "enquiry_total", "enquiry_to_attachment_pct",
]
# enquiry_total is a per-day count -> summed over a period; every other
# metric is a per-day rate/average -> averaged over a period.
SUM_METRICS = {"enquiry_total"}

PERIODS = [
    ("latest", 0, 0),
    ("p0_7", 1, 7),
    ("p7_14", 8, 14),
    ("p14_35", 15, 35),
]


def get_gspread_client() -> gspread.Client:
    sa_json = os.environ.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if sa_json:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(sa_json)
            tmp_path = f.name
        return gspread.service_account(filename=tmp_path)
    return gspread.service_account(filename="service_account.json")


def fetch_rows() -> list[dict]:
    gc = get_gspread_client()
    ws = gc.open_by_key(MASTER_SHEET_ID).worksheet(SHEET_TAB)
    return ws.get_all_records()


def to_float(v):
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def aggregate(records: list[dict]) -> dict:
    dates = sorted({r["date"] for r in records if r.get("date")})
    if not dates:
        return {"anchor_date": None, "days_captured": 0, "clusters": {}}

    anchor = date.fromisoformat(dates[-1])

    by_cluster_date: dict[str, dict[str, dict]] = {}
    for r in records:
        d, c = r.get("date"), r.get("cluster")
        if not d or not c:
            continue
        by_cluster_date.setdefault(c, {})[d] = r

    clusters_out = {}
    for cluster, date_map in by_cluster_date.items():
        metrics_out = {}
        for metric in METRICS:
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

    return {
        "anchor_date": dates[-1],
        "days_captured": len(dates),
        "clusters": clusters_out,
    }


def main():
    try:
        records = fetch_rows()
        print(f"Fetched {len(records)} row(s) from '{SHEET_TAB}'.")
    except Exception as e:
        print(f"WARNING: could not fetch '{SHEET_TAB}' ({e}); writing an empty data.js")
        records = []

    result = aggregate(records)
    with open("data.js", "w", encoding="utf-8") as f:
        f.write("window.OPS_DATA = ")
        json.dump(result, f, indent=2)
        f.write(";\n")

    print(
        f"Wrote data.js — anchor_date={result.get('anchor_date')}, "
        f"days_captured={result.get('days_captured')}, "
        f"{len(result.get('clusters', {}))} cluster(s)."
    )


if __name__ == "__main__":
    main()
