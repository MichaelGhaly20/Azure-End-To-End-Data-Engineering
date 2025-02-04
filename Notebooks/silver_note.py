# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

df_processed = spark.read.format("parquet")\
        .option("header", "true")\
        .option("inferSchema", "true")\
        .load("abfss://processed@mgazuredatalake.dfs.core.windows.net/")


# COMMAND ----------

df_processed.display()

# COMMAND ----------

df_processed.printSchema()

# COMMAND ----------

df_processed.filter(df_processed['order_id'] == 'ORD-00054449').display()

# COMMAND ----------

df_processed.count()

# COMMAND ----------

df = spark.read.format("parquet")\
        .option('inferSchema', 'true')\
        .load("abfss://bronze@mgazuredatalake.dfs.core.windows.net/raw_data")

# COMMAND ----------

# DBTITLE 1,er()
df.filter((df['order_id'] == 'ORD-00054449') & (df['payment_method'] == 'PayPal')).display()

# COMMAND ----------

df.count()

# COMMAND ----------

def format_sales(value):
    if value >= 1e9:
        return f"{round(value / 1e9, 2)} Billion USD"
    elif value >= 1e6:
        return f"{round(value / 1e6, 2)} Million USD"
    elif value >= 1e3:
        return f"{round(value / 1e3, 2)} Thousand USD"
    else:
        return f"{value:.2f} USD"


# COMMAND ----------

# Ensure Python's built-in round is used
def format_sales(value):
    value = float(value)  # Ensure it's a Python float
    if value >= 1e9:
        return f"{__builtins__.round(value / 1e9, 2)} Billion USD"
    elif value >= 1e6:
        return f"{__builtins__.round(value / 1e6, 2)} Million USD"
    elif value >= 1e3:
        return f"{__builtins__.round(value / 1e3, 2)} Thousand USD"
    else:
        return f"{value:.2f} USD"

# Customer Lifetime Value (CLV) Calculation
customer_orders = df.groupBy("customer_id").agg(count("order_id").alias("order_count"))
customer_sales = df.groupBy("customer_id").agg(sum("sales").alias("total_sales"))

# Join customer_orders and customer_sales and calculate CLV
clv = customer_sales.join(customer_orders, "customer_id") \
    .withColumn("CLV", (col("total_sales") / col("order_count")).cast("double"))  # Ensure CLV is numeric

# Collect the top 5 CLV values
clv_top_5 = clv.orderBy(desc("CLV")).limit(5).collect()

# Format and print the output
for row in clv_top_5:
    clv_value = float(row.CLV)  # Explicitly cast to float
    print(f"{row.customer_id}: {format_sales(clv_value)}")

# COMMAND ----------

# Profit Margin by Product Category
# Profit Margin = (Profit / Sales) * 100
profit_margin = df.groupBy("category").agg((sum("profit") / sum("sales") * 100).alias("Profit Margin (%)"))
profit_margin.display()

# COMMAND ----------

df.select('state','city').distinct().display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformation

# COMMAND ----------

df = df.withColumn("Delivery_Time_Days", datediff(col("Ship_Date"), col("Order_Date")))

# COMMAND ----------

df = df.drop("Delivery_Time_Days")

# COMMAND ----------

# Sales by Category
df.groupBy("Category", "Sub_Category").agg(sum("Sales").alias("Total_Sales")).orderBy(desc("Total_Sales")).display()


# COMMAND ----------

# Calculate total sales by region
sales_by_region = df.groupBy("Region", "State").agg(sum("Sales").alias("Total_Sales"))

# Format the Total_Sales column to show values in millions or billions
sales_by_region = sales_by_region.withColumn(
    "Formatted_Sales",
    when(col("Total_Sales") >= 1e9, concat(round(col("Total_Sales") / 1e9, 2), lit(" Billion USD")))
    .when(col("Total_Sales") >= 1e6, concat(round(col("Total_Sales") / 1e6, 2), lit(" Million USD")))
    .otherwise(col("Total_Sales"))
)

# Show the results
sales_by_region.display()

# COMMAND ----------

# 6. Total Sales by Ship Mode
df.groupBy("Ship_Mode").agg(sum("Sales").alias("Total_Sales")).orderBy("Total_Sales", ascending=False).display()

# COMMAND ----------

# 4. Total Sales by Customer Segment and Region
df.groupBy("Segment", "Region").agg(sum("Sales").alias("Total_Sales")).orderBy("Total_Sales", ascending=False).display()

# COMMAND ----------

# 5. Top 5 Customers by Sales in a Specific Region (e.g., "West")
df.filter(col("Region") == "West").groupBy("Customer_Name").agg(sum("Sales").alias("Total_Sales")).orderBy("Total_Sales", ascending=False).limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

df.write.format("parquet")\
    .mode("overwrite")\
    .option("path","abfss://silver@mgazuredatalake.dfs.core.windows.net/product_sales")\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Query silver data

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@mgazuredatalake.dfs.core.windows.net/product_sales`

# COMMAND ----------

