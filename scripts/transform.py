# Transform Bronze TO Silver
import os
import pandas as pd
import yaml

def transform():
    with open("config/config.yaml") as f:
        cfg = yaml.safe_load(f)["paths"]

    bronze = cfg["bronze"]
    silver = cfg["silver"]

    os.makedirs(silver, exist_ok=True)

    # Define required not-null fields
    rules = {
        "customers": {"required": ["customer_id"]},
        "subscriptions": {"required": ["subscription_id", "Customer"]},
        "invoicing": {"required": ["subscription_id"]},
        "costs": {"required": ["subscription_id"]},
        "sales_tags": {"required": ["subscription_id"]},
    }

    for t, meta in rules.items():
        path = os.path.join(bronze, f"{t}.csv")
        if not os.path.exists(path):
            print(f"WARNING: {path} not found, skipping")
            continue

        df = pd.read_csv(path)

        # Step 1: Cast date columns
        if t == "invoicing":
            for col in df.columns:
                if "created" in col.lower() or "start" in col.lower():
                    try:
                        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")
                        print(f"{t}: converted {col} to YYYY-MM-DD")
                    except Exception as e:
                        print(f"{t}: could not convert {col} to date ({e})")

        if t == "costs":
            if "month" in df.columns:
                try:
                    df["month"] = pd.to_datetime(df["month"], errors="coerce", dayfirst=True).dt.strftime("%Y-%m-%d")
                    print(f"{t}: converted month to YYYY-MM-DD")
                except Exception as e:
                    print(f"{t}: could not convert month to date ({e})")

            # Keep only latest cost record for subscription_id + month
            before = len(df)
            df = df.drop_duplicates(subset=["subscription_id", "month"], keep="last")
            after = len(df)
            if before != after:
                print(f"{t}: kept only latest cost record for duplicates ({before - after} rows dropped)")

        # Step 2: Drop rows where required fields are null
        for col in meta["required"]:
            if col in df.columns:
                before = len(df)
                df = df[df[col].notnull()]
                after = len(df)
                if before != after:
                    print(f"{t}: dropped {before - after} rows with null {col}")

        # Step 3: Drop fully duplicate rows
        before = len(df)
        df = df.drop_duplicates()
        after = len(df)
        if before != after:
            print(f"{t}: dropped {before - after} fully duplicate rows")

        # Special join logic: subscriptions ← sales_tags
        if t == "subscriptions":
            tags_path = os.path.join(bronze, "sales_tags.csv")
            if os.path.exists(tags_path):
                tags = pd.read_csv(tags_path)

                if "subscription_id" not in tags.columns or "tag" not in tags.columns:
                    print("WARNING: sales_tags missing expected columns, skipping join")
                else:
                    # Extract part before ":" as sale_type (uppercase)
                    tags["sale_type"] = tags["tag"].astype(str).str.split(":").str[0].str.upper()
                    tags = tags[["subscription_id", "sale_type"]].drop_duplicates()

                    df = df.merge(tags, on="subscription_id", how="left")

        out = os.path.join(silver, f"{t}_clean.csv")
        df.to_csv(out, index=False)
        print(f"WROTE: {out}")

    print("Transform complete")

if __name__ == "__main__":
    transform()
