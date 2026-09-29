"""
=============================================================================
COLD-CHAIN MEDICINE MONITORING SYSTEM
STAGE 1: PANDAS DATA PREPARATION & CLEANING
=============================================================================
Academic Topic: Data Processing using Spark DataFrames (Syllabus 5.2)
Component: Pandas Preprocessing Pipeline

Role:
- Loads the raw IoT sensor dataset.
- Performs exploratory data inspection (head, info, describe, isnull).
- Cleans missing, corrupt, and duplicate readings.
- Formats column types and standardizes timestamps and values.
- Exports the verified, cleaned dataset for Spark DataFrame batch & stream processing.
=============================================================================
"""

import os
import pandas as pd


def prepare_cold_chain_data():
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    raw_data_path = os.path.join(base_dir, "data", "cold_chain_sensor_data.csv")
    cleaned_data_path = os.path.join(base_dir, "data", "cleaned_cold_chain_data.csv")

    if not os.path.exists(raw_data_path):
        raise FileNotFoundError(f"Raw dataset not found at {raw_data_path}. Please generate it first.")

    print("\n" + "=" * 60)
    print("PANDAS DATA PREPARATION")
    print("Cold-Chain Medicine Sensor Dataset Inspection & Cleaning")
    print("=" * 60)

    # 1. DATASET PREVIEW
    print("\n------------------------------------------------------------")
    print("1. DATASET PREVIEW (head())")
    print("------------------------------------------------------------")
    df = pd.read_csv(raw_data_path)
    print(df.head(10))
    print(f"\nInitial Record Count: {len(df)} rows, {len(df.columns)} columns")

    # 2. DATASET INFORMATION
    print("\n------------------------------------------------------------")
    print("2. DATASET INFORMATION (info() & describe())")
    print("------------------------------------------------------------")
    print("\n[DataFrame Summary Info]:")
    df.info()

    print("\n[Descriptive Statistics for Numeric Attributes]:")
    print(df.describe())

    # 3. MISSING VALUE CHECK
    print("\n------------------------------------------------------------")
    print("3. MISSING VALUE CHECK (isnull().sum())")
    print("------------------------------------------------------------")
    missing_summary = df.isnull().sum()
    print(missing_summary)

    # Also check for empty string representations
    empty_strings = (df == "").sum()
    print("\n[Empty String Count]:")
    print(empty_strings[empty_strings > 0])

    # 4. DATA CLEANING
    print("\n------------------------------------------------------------")
    print("4. DATA CLEANING & RECTIFICATION")
    print("------------------------------------------------------------")
    initial_count = len(df)

    # 4.1 Remove duplicate records
    dup_count = df.duplicated().sum()
    if dup_count > 0:
        df = df.drop_duplicates()
        print(f"[*] Removed {dup_count} duplicate record(s).")
    else:
        print("[*] No duplicate records found.")

    # 4.2 Replace empty strings with NaN
    df.replace("", pd.NA, inplace=True)

    # 4.3 Drop records missing mandatory identifiers or critical sensor values (event_id, shipment_id, temperature)
    # Medicine safety cannot afford imputed temperature readings for critical compliance
    critical_missing = df["temperature"].isna() | df["shipment_id"].isna() | df["event_id"].isna()
    dropped_critical = critical_missing.sum()
    if dropped_critical > 0:
        df = df[~critical_missing]
        print(f"[*] Dropped {dropped_critical} record(s) lacking critical identifiers or temperature.")

    # 4.4 Impute missing auxiliary telemetry (humidity) using median
    df["humidity"] = pd.to_numeric(df["humidity"], errors="coerce")
    median_humidity = round(df["humidity"].median(), 2)
    missing_humidity_count = df["humidity"].isna().sum()
    if missing_humidity_count > 0:
        df["humidity"] = df["humidity"].fillna(median_humidity)
        print(f"[*] Imputed {missing_humidity_count} missing humidity value(s) with median ({median_humidity}%).")

    # 4.5 Filter out physically impossible telemetry (e.g. negative battery levels)
    df["battery_level"] = pd.to_numeric(df["battery_level"], errors="coerce")
    invalid_battery = (df["battery_level"] < 0) | (df["battery_level"] > 100)
    dropped_battery = invalid_battery.sum()
    if dropped_battery > 0:
        df = df[~invalid_battery]
        print(f"[*] Filtered {dropped_battery} record(s) with invalid battery telemetry (<0% or >100%).")

    # 4.6 Convert columns to appropriate data types
    df["temperature"] = df["temperature"].astype(float)
    df["humidity"] = df["humidity"].astype(float)
    df["battery_level"] = df["battery_level"].astype(float)
    df["event_time"] = pd.to_datetime(df["event_time"]).dt.strftime("%Y-%m-%d %H:%M:%S")

    # Clean categorical text
    df["door_status"] = df["door_status"].str.strip().str.upper()
    df["medicine"] = df["medicine"].str.strip()
    df["location"] = df["location"].str.strip()

    final_count = len(df)
    print(f"[*] Total rows cleaned and retained: {final_count} (out of initial {initial_count})")

    # 5. CLEANED DATASET
    print("\n------------------------------------------------------------")
    print("5. CLEANED DATASET PREVIEW")
    print("------------------------------------------------------------")
    print(df.head(10))
    print("\n[Cleaned Missing Values Check]:")
    print(df.isnull().sum())

    # 6. CLEANED DATASET SAVED
    print("\n------------------------------------------------------------")
    print("6. CLEANED DATASET SAVED")
    print("------------------------------------------------------------")
    os.makedirs(os.path.dirname(cleaned_data_path), exist_ok=True)
    df.to_csv(cleaned_data_path, index=False)
    print(f"[SUCCESS] Cleaned cold-chain dataset saved to:")
    print(f"          -> {cleaned_data_path}")
    print(f"          -> Total rows ready for Spark DataFrames: {len(df)}")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    prepare_cold_chain_data()
