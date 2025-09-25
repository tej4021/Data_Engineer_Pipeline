import os
import pandas as pd
import yaml
import sqlite3

with open("config/config.yaml") as f:
    cfg = yaml.safe_load(f)["paths"]

def model():
    silver, gold = cfg["silver"], cfg["gold"]
    os.makedirs(gold, exist_ok=True)

    def safe_read(name):
        path = os.path.join(silver, name + "_clean.csv")
        if os.path.exists(path):
            return pd.read_csv(path)
        return pd.DataFrame()

    customers = safe_read("customers")
    subscriptions = safe_read("subscriptions")
    invoicing = safe_read("invoicing")
    costs = safe_read("costs")

    # Dimension tables
    dim_customer = customers.drop_duplicates(subset=["customer_id"]) if not customers.empty else pd.DataFrame()
    dim_subscription = subscriptions.drop_duplicates(subset=["subscription_id"]) if not subscriptions.empty else pd.DataFrame()

    # Fact table: sales
    fct_sales = invoicing.copy() if not invoicing.empty else pd.DataFrame()

    # Fact table: profitability
    fct_profitability = pd.DataFrame()
    if not invoicing.empty:
        # Use start column instead of created_at
        if "start" in invoicing.columns:
            invoicing["start"] = pd.to_datetime(invoicing["start"], errors="coerce")
            invoicing["month"] = invoicing["start"].dt.to_period("M").astype(str)
        else:
            print("WARNING: No 'start' column found in invoicing, skipping profitability calc")

        # Normalize costs dates
        if not costs.empty and "month" in costs.columns:
            costs["month"] = pd.to_datetime(costs["month"], errors="coerce", dayfirst=True)
            costs["month"] = costs["month"].dt.to_period("M").astype(str)

        # Aggregate invoicing by subscription + month
        if "calculated_cost" in invoicing.columns:
            sales_by = (
                invoicing.groupby(["subscription_id", "month"], dropna=False)["calculated_cost"]
                .sum()
                .reset_index()
            )
            sales_by.rename(columns={"calculated_cost": "sales_amount"}, inplace=True)
        else:
            sales_by = pd.DataFrame()

        # Aggregate costs by subscription + month
        if not costs.empty and "Cost" in costs.columns:
            costs_by = (
                costs.groupby(["subscription_id", "month"], dropna=False)["Cost"]
                .sum()
                .reset_index()
            )
            costs_by.rename(columns={"Cost": "cost_amount"}, inplace=True)
        else:
            costs_by = pd.DataFrame()

        # Always keep all sales, even if costs missing
        if not sales_by.empty:
            fct_profitability = pd.merge(
                sales_by,
                costs_by,
                on=["subscription_id", "month"],
                how="left"
            )
            # Ensure no NaNs remain in numeric columns
            fct_profitability["sales_amount"] = fct_profitability["sales_amount"].fillna(0.0).astype(float)
            fct_profitability["cost_amount"] = fct_profitability["cost_amount"].fillna(0.0).astype(float)

            fct_profitability["profit"] = (
                fct_profitability["sales_amount"] - fct_profitability["cost_amount"]
            )

            #Setting a flag
            #fct_profitability["profit_made"] = fct_profitability["profit"].apply(lambda x: "Yes" if x > 0 else "No")

    # Fact table: churn
    fct_churn = pd.DataFrame()
    if not subscriptions.empty:
        if "status" in subscriptions.columns or "cancelled_at" in subscriptions.columns:
            subscriptions["is_churned"] = (
                subscriptions["status"].str.lower().str.contains("cancel|terminate|churn", na=False)
                | subscriptions["cancelled_at"].notna()
            )
            subscriptions["is_churned"] = subscriptions["is_churned"].astype(bool)
            fct_churn = subscriptions[["subscription_id", "is_churned"]]

    # Save results
    tables = {
        "dim_customer": dim_customer,
        "dim_subscription": dim_subscription,
        "fct_sales": fct_sales,
        "fct_profitability": fct_profitability,
        "fct_churn": fct_churn,
    }

    conn = sqlite3.connect(os.path.join(gold, "gold.db"))
    for name, df in tables.items():
        if not df.empty:
            df.to_csv(os.path.join(gold, name + ".csv"), index=False)
            df.to_sql(name, conn, if_exists="replace", index=False)
            print(f"Wrote {name} to gold (CSV + SQLite)")
    conn.close()

if __name__ == "__main__":
    model()
