import sqlite3
import pandas as pd

def query_gold():
    conn = sqlite3.connect("data/gold/gold.db")

    # Updated query: use month (no created_at in fct_profitability)
    q1 = """
        SELECT subscription_id, month, sales_amount, cost_amount, profit
        FROM fct_profitability
        LIMIT 10;
    """
    print("Sample Profitability Data:")
    print(pd.read_sql(q1, conn))

    # Churn summary (True = churned, False = active)
    q2 = """
        SELECT is_churned, COUNT(*) AS subs
        FROM fct_churn
        GROUP BY is_churned;
    """
    print("\nChurn Summary:")
    print(pd.read_sql(q2, conn))

    conn.close()

if __name__ == "__main__":
    query_gold()
