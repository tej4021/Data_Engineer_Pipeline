import os
import sys
import pandas as pd
import yaml

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)["paths"]

# Define required tables and their unique keys
DATASETS = {
    "customers_clean.csv": ["customer_id"],
    "subscriptions_clean.csv": ["subscription_id", "Customer"],
    "invoicing_clean.csv": ["subscription_id"],
    "costs_clean.csv": ["subscription_id"],
}

def dq_checks():
    bronze = cfg["bronze"]
    silver = cfg["silver"]

    print("Post-Transform Data Quality Check")
    all_passed = True

    for fname, key_cols in DATASETS.items():
        silver_path = os.path.join(silver, fname)
        bronze_path = os.path.join(bronze, fname.replace("_clean", ""))  # map back to raw bronze

        if not os.path.exists(silver_path):
            print(f"FAIL: {fname} missing in silver")
            all_passed = False
            continue

        df_silver = pd.read_csv(silver_path)

        # Row counts comparison
        bronze_count = None
        if os.path.exists(bronze_path):
            df_bronze = pd.read_csv(bronze_path)
            bronze_count = len(df_bronze)

        silver_count = len(df_silver)
        print(f"{fname}: bronze rows={bronze_count}, silver rows={silver_count}")

        if df_silver.empty:
            print(f"FAIL: {fname} is empty after transform")
            all_passed = False
            continue

        # Null check on key columns
        for col in key_cols:
            if col not in df_silver.columns:
                print(f"FAIL: {fname} missing expected key column {col}")
                all_passed = False
                continue
            nulls = df_silver[col].isnull().sum()
            if nulls > 0:
                print(f"FAIL: {fname} has {nulls} NULL values in {col}")
                all_passed = False

        # Duplicate check at full-record level
        dupes_all = df_silver.duplicated().sum()
        if dupes_all > 0:
            print(f"FAIL: {fname} has {dupes_all} duplicate full records")
            all_passed = False

        if all_passed:
            print(f"PASS: {fname}")

    if not all_passed:
        print("Post-transform DQ failed. Stopping pipeline.")
        sys.exit(1)
    else:
        print("Post-transform DQ passed")

if __name__ == "__main__":
    dq_checks()
