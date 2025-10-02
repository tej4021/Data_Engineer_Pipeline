# Data Pipeline

## Overview
This project builds a **local data pipeline** using Python & Pandas, following the **Bronze -> Silver -> Gold** architecture.  

- **Bronze**: Raw CSV ingestion  
- **Silver**: Cleaned & standardized data  
- **Gold**: Business-ready fact & dimension tables  

Gold tables are stored both as **CSV files** and in a local **SQLite database** (`data/gold/gold.db`) for easy querying.

- * if you need csv's to run the pipeline, please connect with me via email : tajender.singh4021@gmail.com
- https://www.linkedin.com/in/tajender-singh-katal-83084291/

---

## Steps to Run (ANACONDA PROMPT)

### 1. Clone / Extract Project
Navigate to the project folder:
## EXAMPLE
```bash
cd C:\Users\HP\OneDrive\Desktop\telness_pipeline_tajender
```
1. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

2. **Place CSVs into `data/raw/`**  
   Example:
   - Customers.csv
   - Subscriptions.csv
   - Invoicing.csv
   - Costs.csv
   - Sales tags.csv

3. **Run full pipeline**
   ```
   python -m scripts.run_pipeline  -- IN CASE OF ERROR -> Run : python -m scripts.run_pipeline.py
   ```

   This will:
   - Ingest CSV → Bronze
   - Run Data Quality (DQ) checks -- `skip for time being
   - Transform Bronze → Silver
   - Run Data Quality (DQ) checks
   - Build Gold tables (dim & fact)
   - Store Gold as CSV and SQLite (`data/gold/gold.db`)

4. **Query Gold tables** `You will be able to see the data 
   ```
   python scripts/query_gold.py
   ```

5. **Explore manually in SQLite** `By Running this you will get sql interface to query on final tables`
   ```
   sqlite3 data/gold/gold.db
   ```

   Example queries:
   ```sql queires to validate the data
   
   -- Monthly profitability
	SELECT 
		subscription_id, month, ROUND(SUM(profit), 2) AS total_profit
	FROM fct_profitability
	GROUP BY subscription_id, month
	LIMIT 5;
	
   -- Churn summary
	SELECT 
		is_churned, COUNT(*) AS subs
	FROM fct_churn
	GROUP BY is_churned;
	
   -- Top 5 customers by sales
	SELECT 
        c.customer_id, 
        ROUND(SUM(f.sales_amount), 2) AS total_sales
    FROM fct_profitability f
    JOIN dim_subscription d 
        ON f.subscription_id = d.subscription_id
    JOIN dim_customer c 
        ON d.Customer = c.customer_id
    GROUP BY c.customer_id
    ORDER BY total_sales DESC
    LIMIT 5;

   ```
