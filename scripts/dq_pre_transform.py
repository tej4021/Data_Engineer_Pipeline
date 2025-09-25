import os
import sys
import pandas as pd
import yaml

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)["paths"]

def dq_checks():
    raw, bronze = cfg["raw"], cfg["bronze"]

    print("Data Quality Check: Raw vs Bronze")
    all_passed = True

    rules = {
        "customers.csv": {"required": ["customer_id"]},
        "subscriptions.csv": {"required": ["subscription_id", "Customer"]},
        "invoicing.csv": {"required": ["subscription_id"]},
        "costs.csv": {"required": ["subscription_id"]},
        "sales_tags.csv": {"required": ["subscription_id"]},
    }

    for fname, meta in rules.items():
        raw_path = os.path.join(raw, fname)
        bronze_path = os.path.join(bronze, fname)

        if not os.path.exists(raw_path):
            print(f"WARNING: {fname} missing in raw, skipping")
            continue
        if not os.path.exists(bronze_path):
            print(f"FAIL: {fname} missing in bronze")
            all_passed = False
            continue

        raw_df = pd.read_csv(raw_path)
        bronze_df = pd.read_csv(bronze_path)

        # Row count check
        if len(raw_df) != len(bronze_df):
            print(f"FAIL: {fname} row count mismatch (raw={len(raw_df)}, bronze={len(bronze_df)})")
            all_passed = False

        # Schema check
        if len(raw_df.columns) != len(bronze_df.columns):
            print(f"FAIL: {fname} schema mismatch (raw cols={len(raw_df.columns)}, bronze cols={len(bronze_df.columns)})")
            all_passed = False

        # Not-null checks for required columns
        for col in meta["required"]:
            if col in bronze_df.columns:
                nulls = bronze_df[col].isnull().sum()
                if nulls > 0:
                    print(f"FAIL: {fname} has {nulls} null values in required column {col}")
                    all_passed = False

        # Duplicate check on entire rows (record-level)
        dups = bronze_df.duplicated().sum()
        if dups > 0:
            print(f"FAIL: {fname} has {dups} full duplicate rows (record-level)")
            all_passed = False

        if all_passed:
            print(f"PASS: {fname} checks passed")

    if not all_passed:
        print("Data quality checks failed. Stopping pipeline.")
        sys.exit(1)

if __name__ == "__main__":
    dq_checks()
