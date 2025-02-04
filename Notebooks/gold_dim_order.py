# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Flag Parameter

# COMMAND ----------

dbutils.widgets.text('incremental_flag','0')
incremental_flag = dbutils.widgets.get('incremental_flag')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Silver Layer Data

# COMMAND ----------

silver_path = "abfss://silver@mgazuredatalake.dfs.core.windows.net/product_sales"
df_silver_order = spark.read.format("parquet").load(silver_path) \
    .select(
        col("Order_ID"),
        col("Order_Date"),
        col("Ship_Date"),
        col("Ship_Mode"),
        col("Payment_Method"),
        col("Order_Priority"),
        col("Return_Status"),
        col("Return_Reason"),
        col("Shipping_Cost"),
        col("Carrier")
    ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if Gold Table Exists
# MAGIC

# COMMAND ----------

gold_table_name = "sales_catalog.gold.dim_order"
if spark.catalog.tableExists(gold_table_name):
    df_gold_order = spark.read.format("delta").table(gold_table_name)
else:
    # Create an empty DataFrame with the same schema
    df_gold_order = spark.createDataFrame([], df_silver_order.schema.add("dim_order_key", "long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Upsert Records

# COMMAND ----------

df_order_filter = df_silver_order.join(df_gold_order, ["Order_ID"], "left") \
    .select(
        df_silver_order["*"],
        df_gold_order["dim_order_key"]
    )

# COMMAND ----------

# Split into old and new records
df_order_filter_old = df_order_filter.filter(col("dim_order_key").isNotNull())
df_order_filter_new = df_order_filter.filter(col("dim_order_key").isNull()) \
    .select(df_silver_order["*"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Surrogate Keys for New Records

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql(f"SELECT MAX(dim_order_key) FROM {gold_table_name}")
    max_value = max_value_df.collect()[0][0] or 0  # Handle null case

df_order_filter_new = df_order_filter_new.withColumn(
    "dim_order_key",
    max_value + row_number().over(Window.orderBy("Order_ID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Old and New Records

# COMMAND ----------

df_order_final = df_order_filter_old.union(df_order_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Table

# COMMAND ----------

delta_path = "abfss://gold@mgazuredatalake.dfs.core.windows.net/dim_order"

if spark.catalog.tableExists(gold_table_name):
    # Perform SCD Type 1 Merge
    delta_tbl = DeltaTable.forName(spark, gold_table_name)
    delta_tbl.alias("tgt").merge(
        df_order_final.alias("src"),
        "tgt.dim_order_key = src.dim_order_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # Initial load
    df_order_final.write.format("delta").mode("overwrite") \
        .option("path", delta_path).saveAsTable(gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Table

# COMMAND ----------

# Log success
spark.sql(f"OPTIMIZE {gold_table_name} ZORDER BY (dim_order_key)")

# COMMAND ----------

print(f"Successfully processed {df_order_final.count()} order records.")

# COMMAND ----------

