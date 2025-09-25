-- DQ VALIDATIONS

-- CUSTOMER CSV
custDf = spark.read.csv("raw/Customers.csv", header=True, inferSchema=True)
custDf.printSchema()

custDf.createOrReplaceTempView("customer")
cols = ", ".join(custDf.columns)

-- DUPLCIATE
query = f"""
SELECT SUM(cnt - 1) AS duplicate_rows
FROM (
    SELECT COUNT(*) AS cnt
    FROM customer
    GROUP BY {cols}
    HAVING COUNT(*) > 1
) t
"""
spark.sql(query).show()

-- NULL CHECK
spark.sql(""" SELECT COUNT(*) as cnt FROM customer
   WHERE customer_id IS NULL
""").show(truncate=False)

-- COST CSV
costDf = spark.read.csv("raw/costs.csv", header=True, inferSchema=True)
costDf.printSchema()
costDf.show(5, truncate=False)

-- DUPLCIATE 
costDf.createOrReplaceTempView("costs")
spark.sql(""" SELECT subscription_id, month, Cost, COUNT(*) as cnt FROM costs
    GROUP BY subscription_id, month, Cost HAVING COUNT(*) > 1
""").show(truncate=False)

-- NULL CHECK
spark.sql(""" SELECT COUNT(*) as cnt FROM costs
   WHERE subscription_id IS NULL
""").show(truncate=False)

-- INVOICING CSV
invoiceDf = spark.read.csv("raw/Invoicing.csv", header=True, inferSchema=True)
invoiceDf.printSchema()

invoiceDf.createOrReplaceTempView("invoice")
cols = ", ".join(invoiceDf.columns)

-- DUPLCIATE
query = f"""
SELECT SUM(cnt - 1) AS duplicate_rows
FROM (
    SELECT COUNT(*) AS cnt
    FROM invoice
    GROUP BY {cols}
    HAVING COUNT(*) > 1
) t
"""
spark.sql(query).show()

-- See values
dupInvData = f"""
SELECT {cols}, COUNT(*) AS duplicate_count
FROM invoice
GROUP BY {cols}
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
"""
spark.sql(dupInvData).show(5,truncate=False)

-- NULL CHECK
spark.sql(""" SELECT COUNT(*) as cnt FROM invoice
   WHERE subscription_id IS NULL
""").show(truncate=False)

-- SUBSCRIPTION CSV

subsDf = spark.read.csv("raw/Subscriptions.csv", header=True, inferSchema=True)
subsDf.printSchema()

subsDf.createOrReplaceTempView("subscription")
cols = ", ".join([f"`{c}`" for c in subsDf.columns])

-- DUPLCIATE
query = f"""
SELECT SUM(cnt - 1) AS duplicate_rows
FROM (
    SELECT COUNT(*) AS cnt
    FROM subscription
    GROUP BY {cols}
    HAVING COUNT(*) > 1
) t
"""
spark.sql(query).show()

-- See values
dupSubData = f"""
SELECT {cols}, COUNT(*) AS duplicate_count
FROM subscription
GROUP BY {cols}
HAVING COUNT(*) > 1
ORDER BY duplicate_count DESC
"""
spark.sql(dupSubData).show(truncate=False)


-- NULL CHECK
spark.sql(""" SELECT COUNT(*) as cnt FROM subscription
   WHERE subscription_id IS NULL
""").show(truncate=False)

spark.sql(""" SELECT COUNT(*) as cnt FROM subscription
   WHERE Customer IS NULL
""").show(truncate=False)

-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-----------------------------------------    VALIDATION
"""
Profit Calculation
-- SAMPLE 1 --> 0066eba3-et5t-46c3-3t2e-3e69c5216173
-- SAMPLE 2 --> 02323929-4c3a-4b42-9et0-333fbaeec26e
"""
costDf = spark.read.csv("raw/costs.csv", header=True, inferSchema=True)
costDf.createOrReplaceTempView("costs")

-- picking 1 sub_id
spark.sql(""" SELECT * FROM costs WHERE subscription_id = '0066eba3-et5t-46c3-3t2e-3e69c5216173'
ORDER BY month
""").show(truncate=False)
+------------------------------------+----------+--------+
|subscription_id                     |month     |Cost    |
+------------------------------------+----------+--------+
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-09-01|53.97184|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-10-01|34.17152|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-10-01|74.2512 | --> Latest
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-11-01|54.77024|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-12-01|90.85792|
+------------------------------------+----------+--------+

-- Invoice
invoiceDf = spark.read.csv("raw/Invoicing.csv", header=True, inferSchema=True)
invoiceDf.createOrReplaceTempView("invoice")

spark.sql(""" SELECT subscription_id,calculated_cost,start,end,created_at FROM invoice WHERE subscription_id = '0066eba3-et5t-46c3-3t2e-3e69c5216173'
ORDER BY start
""").show(truncate=False)
+------------------------------------+---------------+----------+----------+--------------------------+
|subscription_id                     |calculated_cost|start     |end       |created_at                |
+------------------------------------+---------------+----------+----------+--------------------------+
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-09-01|2022-09-30|2022-10-05 19:42:38.6228  |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-09-01|2022-09-30|2022-11-01 19:37:01.677686|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-10-01|2022-10-31|2022-11-01 19:37:01.677686|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-11-01|2022-11-30|2022-12-18 20:54:09.209639|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-11-01|2022-11-30|2022-11-29 16:44:52.49871 |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2022-12-01|2022-12-31|2022-12-18 20:54:09.209639|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|29.0           |2023-01-01|2023-01-31|2023-01-11 15:45:55.113256|
+------------------------------------+---------------+----------+----------+--------------------------+

-- Final layer -- fct_profitability
profitDf = spark.read.csv("gold/fct_profitability.csv", header=True, inferSchema=False)
profitDf.createOrReplaceTempView("fct_profitability")

spark.sql(""" SELECT * FROM fct_profitability WHERE subscription_id = '0066eba3-et5t-46c3-3t2e-3e69c5216173'
ORDER BY month
""").show(truncate=False)
+------------------------------------+-------+------------+-----------+------------------+
|subscription_id                     |month  |sales_amount|cost_amount|profit            |
+------------------------------------+-------+------------+-----------+------------------+
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-09|58.0        |53.97184   |4.02816           |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-10|29.0        |74.2512    |-45.2512          |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-11|58.0        |54.77024   |3.229759999999999 |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-12|29.0        |90.85792   |-61.85791999999999|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2023-01|29.0        |0.0        |29.0              |
+------------------------------------+-------+------------+-----------+------------------+

-- SAMPLE 2 --> 02323929-4c3a-4b42-9et0-333fbaeec26e

costDf = spark.read.csv("raw/costs.csv", header=True, inferSchema=True)
costDf.createOrReplaceTempView("costs")

spark.sql(""" SELECT * FROM costs WHERE subscription_id = '02323929-4c3a-4b42-9et0-333fbaeec26e'
ORDER BY month
""").show(truncate=False)
+------------------------------------+----------+--------+
|subscription_id                     |month     |Cost    |
+------------------------------------+----------+--------+
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-09-01|39.28128|
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-10-01|31.936  |
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-10-01|52.37504| --> Latest
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-11-01|69.62048|
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-12-01|36.40704|
+------------------------------------+----------+--------+

-- Invoice
invoiceDf = spark.read.csv("raw/Invoicing.csv", header=True, inferSchema=True)
invoiceDf.createOrReplaceTempView("invoice")

spark.sql(""" SELECT subscription_id,calculated_cost,start,end,created_at FROM invoice WHERE subscription_id = '02323929-4c3a-4b42-9et0-333fbaeec26e'
ORDER BY start
""").show(truncate=False)
+------------------------------------+---------------+----------+----------+--------------------------+
|subscription_id                     |calculated_cost|start     |end       |created_at                |
+------------------------------------+---------------+----------+----------+--------------------------+
|02323929-4c3a-4b42-9et0-333fbaeec26e|29.0           |2022-09-01|2022-09-30|2022-10-05 19:32:06.735091|
|02323929-4c3a-4b42-9et0-333fbaeec26e|29.0           |2022-10-01|2022-10-31|2022-11-01 19:37:30.931282|
|02323929-4c3a-4b42-9et0-333fbaeec26e|29.0           |2022-11-01|2022-11-30|2022-11-29 16:43:58.525387|
|02323929-4c3a-4b42-9et0-333fbaeec26e|29.0           |2022-12-01|2022-12-31|2022-12-18 20:53:11.350095|
|02323929-4c3a-4b42-9et0-333fbaeec26e|29.0           |2023-01-01|2023-01-31|2023-01-11 15:43:34.144538|
+------------------------------------+---------------+----------+----------+--------------------------+

-- Final layer -- fct_profitability
profitDf = spark.read.csv("gold/fct_profitability.csv", header=True, inferSchema=False)
profitDf.createOrReplaceTempView("fct_profitability")

spark.sql(""" SELECT * FROM fct_profitability WHERE subscription_id = '02323929-4c3a-4b42-9et0-333fbaeec26e'
ORDER BY month
""").show(truncate=False)
+------------------------------------+-------+------------+-----------+-------------------+
|subscription_id                     |month  |sales_amount|cost_amount|profit             |
+------------------------------------+-------+------------+-----------+-------------------+
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-09|29.0        |39.28128   |-10.281280000000002|
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-10|29.0        |52.37504   |-23.37504          | --> This
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-11|29.0        |69.62048   |-40.62048          |
|02323929-4c3a-4b42-9et0-333fbaeec26e|2022-12|29.0        |36.40704   |-7.407040000000002 |
|02323929-4c3a-4b42-9et0-333fbaeec26e|2023-01|29.0        |0.0        |29.0               |
+------------------------------------+-------+------------+-----------+-------------------+

"""
Churn Calculation
"""
subsDf = spark.read.csv("raw/Subscriptions.csv", header=True, inferSchema=True)
subsDf.createOrReplaceTempView("subscription")

-- Total count
spark.sql("""
SELECT count(*) AS total_cnt FROM subscription
WHERE status is NOT NULL
""").show(truncate=False)
+---------+
|total_cnt|
+---------+
|273      |
+---------+

spark.sql("""
SELECT DISTINCT(status) AS status FROM subscription
WHERE status is NOT NULL
""").show(truncate=False)
+-----------------------------+
|status                       |
+-----------------------------+
|SUBSCRIPTION_STATUS_ACTIVATED|
|SUBSCRIPTION_STATUS_PENDING  |
|SUBSCRIPTION_STATUS_CANCELLED| --> This 
|SUBSCRIPTION_STATUS_PAUSED   |
+-----------------------------+

-- subscription_id, cancelled_at, status
-- CANCELLED COUNT
spark.sql("""
SELECT count(*) AS is_churned_cnt FROM subscription
WHERE status LIKE '%CANCELLED%'
""").show(truncate=False)
+--------------+
|is_churned_cnt|
+--------------+
|22            |
+--------------+

-- NOT CANCELLED COUNT
spark.sql("""
SELECT count(*) AS is_not_churned_cnt FROM subscription
WHERE status NOT LIKE '%CANCELLED%'
""").show(truncate=False)
+------------------+
|is_not_churned_cnt|
+------------------+
|251               |
+------------------+

-- 251 + 22 = 273

-- Final layer -- fct_churn
churnDf = spark.read.csv("gold/fct_churn.csv", header=True, inferSchema=True)
churnDf.createOrReplaceTempView("fct_churn")

spark.sql(""" SELECT count(*) AS is_churned_cnt FROM fct_churn WHERE is_churned = 'true' """).show(truncate=False)
+--------------+
|is_churned_cnt|
+--------------+
|22            |
+--------------+

spark.sql(""" SELECT count(*) AS is_not_churned_cnt FROM fct_churn WHERE is_churned = 'false' """).show(truncate=False)
+------------------+
|is_not_churned_cnt|
+------------------+
|251               |
+------------------+

"""
tag transform validations
"""

tagsDf = spark.read.csv("silver/subscriptions_clean.csv", header=True, inferSchema=True)
tagsDf.createOrReplaceTempView("tran_subscriptions")

spark.sql(""" SELECT DISTINCT(sale_type) FROM tran_subscriptions """).show(truncate=False)
+-----------+
|sale_type  |
+-----------+
|SUPPORTSALE|
|DIRECTSALE |
|ONLINESALE |
|PARTNERSALE|
|NULL       |
+-----------+

"""
CAST DATE VALIDATIONS AND LATEST RECORD FROM COST DATASET OR CSV
"""

-- Latest record
costDf = spark.read.csv("raw/costs.csv", header=True, inferSchema=True)
costDf.createOrReplaceTempView("costs")
spark.sql(""" SELECT * FROM costs WHERE subscription_id = '0066eba3-et5t-46c3-3t2e-3e69c5216173'
ORDER BY month
""").show(truncate=False)
+------------------------------------+----------+--------+
|subscription_id                     |month     |Cost    |
+------------------------------------+----------+--------+
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-09-01|53.97184|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-10-01|34.17152|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-10-01|74.2512 |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-11-01|54.77024|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-12-01|90.85792|
+------------------------------------+----------+--------+

dateCostDf = spark.read.csv("silver/costs_clean.csv", header=True, inferSchema=False)
dateCostDf.createOrReplaceTempView("tran_cost")
spark.sql(""" SELECT * FROM tran_cost WHERE subscription_id = '0066eba3-et5t-46c3-3t2e-3e69c5216173'
ORDER BY month
""").show(truncate=False)
+------------------------------------+----------+--------+
|subscription_id                     |month     |Cost    |
+------------------------------------+----------+--------+
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-01-09|53.97184|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-01-10|74.2512 |
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-01-11|54.77024|
|0066eba3-et5t-46c3-3t2e-3e69c5216173|2022-01-12|90.85792|
+------------------------------------+----------+--------+

invTranDf = spark.read.csv("silver/Invoicing_clean.csv", header=True, inferSchema=True)
invTranDf.createOrReplaceTempView("invoice_trans")

spark.sql(""" """).show(truncate=False)


-- EXTRA VALIDATION/ANALYSIS -- ADDED AT README

dimProfitDf = spark.read.csv("gold/fct_profitability.csv", header=True, inferSchema=False)
dimProfitDf.createOrReplaceTempView("fct_profitability")

dimSubDf = spark.read.csv("gold/dim_subscription.csv", header=True, inferSchema=False)
dimSubDf.createOrReplaceTempView("dim_subscription")

dimCustDf = spark.read.csv("gold/dim_customer.csv", header=True, inferSchema=False)
dimCustDf.createOrReplaceTempView("dim_customer")

fctChurnDf = spark.read.csv("gold/fct_churn.csv", header=True, inferSchema=False)
fctChurnDf.createOrReplaceTempView("fct_churn")

--
spark.sql("""
SELECT 
		subscription_id, month, ROUND(SUM(profit), 2) AS total_profit
	FROM fct_profitability
	GROUP BY subscription_id, month
	LIMIT 5
""").show(truncate=False)

-- 
spark.sql("""
SELECT 
		is_churned, COUNT(*) AS subs
	FROM fct_churn
	GROUP BY is_churned
""").show(truncate=False)

-- 
spark.sql("""
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
    LIMIT 5
""").show(truncate=False)