import os
import pandas as pd
import yaml

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)["paths"]

def ingest():
    raw, bronze = cfg["raw"], cfg["bronze"]
    os.makedirs(bronze, exist_ok=True)

    for fname in os.listdir(raw):
        if fname.endswith(".csv"):
            src = os.path.join(raw, fname)
            dest = os.path.join(bronze, fname)
            df = pd.read_csv(src)
            df.to_csv(dest, index=False)
            print(f"Ingested {fname} to bronze")

if __name__ == "__main__":
    ingest()
