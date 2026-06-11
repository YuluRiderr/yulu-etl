"""
Yulu ETL — Sweep / Octopus / Stuck / Parts Summary
Fetches from Metabase, processes with pandas, pushes to Google Sheets.
Run daily via GitHub Actions at 1 AM IST.
"""

import io
import os

import pandas as pd
import requests
import gspread

# ─────────────────────────────────────────────────────────────
# CONFIG  (values come from GitHub Secrets / env variables)
# ─────────────────────────────────────────────────────────────
METABASE_URL      = os.environ["METABASE_URL"].rstrip("/")
METABASE_EMAIL    = os.environ["METABASE_EMAIL"]
METABASE_PASSWORD = os.environ["METABASE_PASSWORD"]

CARD_ID_SWEEP   = 654
CARD_ID_OCTOPUS = 7433
CARD_ID_STUCK   = 9705

MASTER_SHEET_ID       = "1fBjHKwlxRGwjsOSjzHOB6cUjvaKrhvtXdaPGeZjZuH0"
BROKEN_BIKE_SHEET_URL = "https://docs.google.com/spreadsheets/d/1eGDS2Sj33Gqk63QxmOzw302f05v_WoeSqSZr7Oz2dTE/edit"


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


def fetch_metabase_csv(card_id: int, city: str = None) -> pd.DataFrame:
    """
    Run a saved Metabase card and return the result as a DataFrame.
    """
    headers = metabase_session()

    parameters = []
    if city:
        parameters.append({
            "type":   "text",
            "target": ["variable", ["template-tag", "City"]],
            "value":  city,
        })

    csv_resp = requests.post(
        f"{METABASE_URL}/api/card/{card_id}/query/csv",
        json={"parameters": parameters},
        headers=headers,
        timeout=180,
    )
    csv_resp.raise_for_status()

    df = pd.read_csv(io.StringIO(csv_resp.text), low_memory=False)
    print(f"  [Card {card_id}] {len(df)} rows | cols: {df.columns.tolist()}")
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
    df = fetch_metabase_csv(CARD_ID_SWEEP, city='BLR')
    df = df.replace([None], ["NA"], regex=True)

    wanted = [
        "city", "bike", "bike_category", "bike_group", "at_warehouse",
        "nearest_yz", "no_of_days_since_rnt", "flag_bike_fault",
        "reserved_bike", "on_biker_map", "on_fleet_map", "is_test_vehicle",
        "flag_stolen", "flag_unavailable", "bike_state_id", "version_no",
        "operational_cluster",
    ]
    df = df[[c for c in wanted if c in df.columns]]

    df = df[df["city"].isin(["BLR"])]
    df = df[df["bike_category"].isin(["DeX", "Express"])]

    # UPDATED: version_group now checks bike_category first.
    # Express bikes are always "Express" regardless of version_no.
    # DeX bikes are classified by version_no prefix: "2." → "2x", "3." → "3x", else "Unknown".
    df["version_group"] = df.apply(
        lambda row: "Express" if row["bike_category"] == "Express"
        else ("2x" if str(row["version_no"]).startswith("2.")
              else ("3x" if str(row["version_no"]).startswith("3.")
                    else "Unknown")),
        axis=1
    )

    # FIX: normalise bike IDs so merge works regardless of int/float/string
    df["bike"] = normalise_bike_id(df["bike"])

    clear_and_upload(gc, MASTER_SHEET_ID, "Sweep", df)
    return df


# ─────────────────────────────────────────────────────────────
# STEP B — OCTOPUS  (card 7433)
# ─────────────────────────────────────────────────────────────
def process_octopus(gc: gspread.Client):
    print("\n── STEP B: Octopus ──")
    df1 = fetch_metabase_csv(CARD_ID_OCTOPUS)

    mapping = {
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
    df1["Type"] = df1["bike_group"].map(mapping)

    clear_and_upload(gc, MASTER_SHEET_ID, "Octopus", df1)


# ─────────────────────────────────────────────────────────────
# STEP C — STUCK / TO BE MOVED  (card 9705)
# ─────────────────────────────────────────────────────────────
def fetch_broken_bikes(gc: gspread.Client) -> pd.DataFrame:
    sh       = gc.open_by_url(BROKEN_BIKE_SHEET_URL)
    all_data = []
    for tab in ["BLR", "BOM", "NCR", "HYD"]:
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

    # Drop rows with clearly invalid/garbled bike IDs
    # Valid IDs are 7-digit numbers starting with 5
    valid_mask = merged["bike"].str.match(r"^5\d{6}$")
    invalid = merged[~valid_mask]
    if not invalid.empty:
        print(f"  WARNING: Dropping {len(invalid)} rows with invalid bike IDs: {invalid['bike'].tolist()}")
    merged = merged[valid_mask].reset_index(drop=True)

    print(f"  Broken bikes sheet total: {len(merged)} rows across {merged['City'].value_counts().to_dict()}")
    return merged


def process_stuck(gc: gspread.Client, sweep_df: pd.DataFrame):
    print("\n── STEP C: Stuck / To Be Moved ──")
    df2 = fetch_metabase_csv(CARD_ID_STUCK, city='BLR')

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
    print(f"  Rows with missing bike_state_id (not in sweep): {df_final['bike_state_id'].isna().sum()}")

    # Filter out bikes already in state 64 or 65
    before = len(df_final)
    df_final = df_final[~df_final["bike_state_id"].isin([64, 65])]
    print(f"  After removing state 64/65: {len(df_final)} rows (removed {before - len(df_final)})")

    # Filter out LTR reserved bikes
    before = len(df_final)
    df_final = df_final[~df_final["reserved_bike"].isin(["LTR"])]
    print(f"  After removing LTR: {len(df_final)} rows (removed {before - len(df_final)})")

    # Filter out bikes not found in sweep (version_group is NaN = not a BLR DeX/Express bike)
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
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("Authenticating with Google Sheets…")
    gc = get_gspread_client()

    sweep_df      = process_sweep(gc)
    process_octopus(gc)
    df_final, df2 = process_stuck(gc, sweep_df)
    process_parts_summary(gc, df_final, df2)

    print("\n✅ ETL complete.")


if __name__ == "__main__":
    main()
