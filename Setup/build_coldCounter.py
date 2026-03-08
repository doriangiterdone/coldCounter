# -*- coding: utf-8 -*-
"""
build_coldCounter.py

Unified ETL pipeline for the coldCounter database.

Pipeline Stages
----------------
0. Load NCIC offense code dimension table
1. Ingest public ICE datasets from the Deportation Data Project
2. Build hold room statistics fact table
3. Build facility statistics fact table
4. Run sanity checks

Author: DG
"""

import sqlite3
import pandas as pd
import requests
from io import BytesIO
from datetime import datetime
from pathlib import Path
import os
from pyfiglet import Figlet
from colorama import Fore, Style, init

# --------------------------------------------------
# PATH CONFIGURATION
# --------------------------------------------------

current_dir = Path(__file__).parent
workspace_root = current_dir.parent

db_path = workspace_root / "coldCounter.db"
ncic_excel = current_dir / "ICOTS_NCIC_OffenseCodesList.xlsx"


# --------------------------------------------------
# DATASETS
# --------------------------------------------------

datasets = [
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/arrests-latest.xlsx", "table": "arrests"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detainers-latest.xlsx", "table": "detainers"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stints-latest.xlsx", "table": "detention_stints"},
    {"url": "https://github.com/deportationdata/ice/raw/refs/heads/main/data/detention-stays-latest.xlsx", "table": "detention_stays"},
]


# --------------------------------------------------
# LOGGING UTILITIES
# --------------------------------------------------

def log(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

def banner(msg):
    print("\n" + "="*65)
    print(msg)
    print("="*65)


# --------------------------------------------------
# STAGE 0 — LOAD NCIC DIMENSION TABLE
# --------------------------------------------------

def load_ncic_dimension(conn):

    banner("STAGE 0 — LOADING NCIC OFFENSE CODE DIMENSION")

    if not ncic_excel.exists():
        log(f"ERROR: NCIC offense code file not found: {ncic_excel}")
        return

    log(f"Reading offense code list from {ncic_excel.name}")

    df = pd.read_excel(ncic_excel)

    log(f"Offense codes loaded: {len(df)} rows")

    df.to_sql("dim_ncic_offense_codes", conn, if_exists="replace", index=False)

    log("dim_ncic_offense_codes table refreshed")

    if "Code" in df.columns:
        log(f"Unique offense codes available: {df['Code'].nunique()}")


# --------------------------------------------------
# STAGE 1 — INGEST PUBLIC DATASETS
# --------------------------------------------------

def ingest_datasets(conn):

    banner("STAGE 1 — DOWNLOADING CURRENT DEPORTATION DATA PROJECT DATASETS")

    for dataset in datasets:

        url = dataset["url"]
        table_name = dataset["table"]

        try:
            log(f"Fetching dataset: {table_name}")

            r = requests.get(url)
            r.raise_for_status()

            log(f"Download successful ({len(r.content):,} bytes)")
            log("Loading dataframe into memory...")
            df = pd.read_excel(BytesIO(r.content))

            log(f"{table_name}: {len(df):,} rows loaded")
            log("Writing data to coldCounter...")
            df.to_sql(table_name, conn, if_exists="replace", index=False)

            log(f"Table '{table_name}' updated")

        except Exception as e:
            log(f"ERROR loading {table_name}: {e}")

    log("Raw ingestion stage completed")


# --------------------------------------------------
# STAGE 2 — BUILD HOLD ROOM FACT TABLE
# --------------------------------------------------

def build_hold_room_facts(conn):

    banner("STAGE 2 — HOLD ROOM USAGE TABLE")
    log("Collecting evidence for the Hague...")
    query = """
    SELECT st.*, cd.*
    FROM detention_stints st
    LEFT JOIN dim_ncic_offense_codes cd
    ON st.most_serious_conviction_code = cd.Code
    """

    df = pd.read_sql_query(
        query,
        conn,
        parse_dates=["book_in_date_time", "book_out_date_time"]
    )

    log(f"detention_stints loaded: {len(df):,} rows")

    filtered = df[
        (df.likely_duplicate == 0) &
        (df.book_out_date_time.notna()) &
        (df.detention_facility_code.str.endswith("HOLD"))
    ].copy()

    log(f"HOLD facility records: {len(filtered):,}")

    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year

    filtered["hours_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 3600

    filtered["nights_without_bed"] = (filtered["hours_imprisoned"] >= 12).astype(int)

    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]

    filtered["criminal"] = filtered["criminality_code"] == "1"
    filtered["charged"] = filtered["criminality_code"] == "2"
    filtered["no_charge"] = filtered["criminality_code"] == "3"

    grouped = (
        filtered
        .groupby(["state","detention_facility","detention_facility_code"])
        .agg(
            total_encounters=("stay_ID","count"),
            nights_without_bed=("nights_without_bed","sum"),
            min_age=("age","min"),
            max_age=("age","max"),
            count_convicted_criminals=("criminal","sum"),
            count_charged=("charged","sum"),
            count_no_charges=("no_charge","sum"),
            min_hours=("hours_imprisoned","min"),
            max_hours=("hours_imprisoned","max")
        )
        .reset_index()
    )

    grouped.rename(columns={"state":"state_code"}, inplace=True)

    grouped.to_sql("fact_hold_rooms", conn, if_exists="replace", index=False)

    log(f"fact_hold_rooms written ({len(grouped)} rows)")


# --------------------------------------------------
# STAGE 3 — BUILD FACILITY STATISTICS FACT TABLE
# --------------------------------------------------

def build_facility_statistics(conn):

    banner("STAGE 3 — FACILITY CHARACTERIZATION")

    df = pd.read_sql_query(
        "SELECT * FROM detention_stints",
        conn,
        parse_dates=["book_in_date_time","book_out_date_time"]
    )

    filtered = df[
        (df.likely_duplicate == 0) &
        (df.book_out_date_time.notna())
    ].copy()

    filtered["age"] = filtered["book_out_date_time"].dt.year - filtered.birth_year

    filtered["days_imprisoned"] = (
        filtered.book_out_date_time - filtered.book_in_date_time
    ).dt.total_seconds() / 86400

    filtered["criminality_code"] = filtered["book_in_criminality"].astype(str).str[0]

    filtered["criminal"] = filtered["criminality_code"] == "1"
    filtered["charged"] = filtered["criminality_code"] == "2"
    filtered["no_charge"] = filtered["criminality_code"] == "3"

    def categorize_facility(row):

        code = row["detention_facility_code"]
        name = str(row["detention_facility"])

        if code.endswith("HOLD"):
            return "hold room"
        elif "JAIL" in name or "COR" in name:
            return "jail"
        elif "HOS" in name:
            return "hospital"
        else:
            return "other"

    filtered["facility_category"] = filtered.apply(categorize_facility, axis=1)

    grouped = (
        filtered
        .groupby(["state","detention_facility","detention_facility_code"])
        .agg(
            facility_category=("facility_category","first"),
            total_encounters=("stay_ID","count"),
            min_age=("age","min"),
            max_age=("age","max"),
            count_criminal=("criminal","sum"),
            count_pending=("charged","sum"),
            count_no_charges=("no_charge","sum"),
            avg_days=("days_imprisoned","mean"),
            max_days=("days_imprisoned","max")
        )
        .reset_index()
    )

    grouped.rename(columns={"state":"state_code"}, inplace=True)

    grouped.to_sql("fact_facility_statistics", conn, if_exists="replace", index=False)

    log(f"fact_facility_statistics written ({len(grouped)} rows)")


# --------------------------------------------------
# SANITY CHECKS
# --------------------------------------------------

def sanity_checks(conn):

    banner("PIPELINE SANITY CHECKS")

    tables = [
        "detention_stints",
        "fact_hold_rooms",
        "fact_facility_statistics",
        "dim_ncic_offense_codes"
    ]

    for t in tables:

        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
            log(f"{t}: {count:,} rows")

        except:
            log(f"{t}: table missing")

    log("Integrity checks complete")


# --------------------------------------------------
# MAIN PIPELINE
# --------------------------------------------------

def run_pipeline():

    banner("COLDCOUNTER ETL PIPELINE INITIATED")
    
    conn = sqlite3.connect(db_path)

    load_ncic_dimension(conn)

    ingest_datasets(conn)

    build_hold_room_facts(conn)

    build_facility_statistics(conn)

    sanity_checks(conn)

    conn.close()

    banner("DATABASE BUILD COMPLETE")

    log("coldCounter database successfully updated.")

    
def redhulk(text):
        print(Fore.RED + text + Style.RESET_ALL)

def big_title(text):
        f = Figlet(font="slant")
        print(Fore.CYAN + f.renderText(text) + Style.RESET_ALL)

for _ in range(4):
    redhulk("EL PUEBLO UNIDO JAMAS SERA VENCIDO")

big_title("NO")
big_title("CONCENTRATION CAMPS")
big_title("IN COLORADO")

for _ in range(4):
    redhulk("EL PUEBLO UNIDO JAMAS SERA VENCIDO")

if __name__ == "__main__":
    run_pipeline()