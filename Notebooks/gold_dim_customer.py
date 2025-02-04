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
df_silver_customer = spark.read.format("parquet").load(silver_path) \
    .select(
        col("Customer_ID"),
        col("Customer_Name"),
        col("Customer_Gender").alias("Gender"),
        col("Customer_Age").alias("Age"),
        col("Customer_Loyalty_Tier").alias("Loyalty_Tier"),
        col("CLV"),
        col("Segment"),
        col("City"),
        col("State"),
        col("Postal_Code"),
        col("Latitude"),
        col("Longitude"),
        col("Region"),
        col("Customer_Since")
    ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if Gold Table Exists
# MAGIC

# COMMAND ----------

gold_table_name = "sales_catalog.gold.dim_customer"
if spark.catalog.tableExists(gold_table_name):
    df_gold_customer = spark.read.format("delta").table(gold_table_name)
else:
    # Create an empty DataFrame with the same schema
    df_gold_customer = spark.createDataFrame([], df_silver_customer.schema.add("dim_customer_key", "long"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Upsert Records

# COMMAND ----------

df_customer_filter = df_silver_customer.join(df_gold_customer, ["Customer_ID"], "left") \
    .select(
        df_silver_customer["*"],
        df_gold_customer["dim_customer_key"]
    )

# COMMAND ----------

# Split into old and new records
df_customer_filter_old = df_customer_filter.filter(col("dim_customer_key").isNotNull())
df_customer_filter_new = df_customer_filter.filter(col("dim_customer_key").isNull()) \
    .select(df_silver_customer["*"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Surrogate Keys for New Records

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql(f"SELECT MAX(dim_customer_key) FROM {gold_table_name}")
    max_value = max_value_df.collect()[0][0] or 0  # Handle null case

df_customer_filter_new = df_customer_filter_new.withColumn(
    "dim_customer_key",
    max_value + row_number().over(Window.orderBy("Customer_ID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Old and New Records

# COMMAND ----------

df_customer_final = df_customer_filter_old.union(df_customer_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Table

# COMMAND ----------

delta_path = "abfss://gold@mgazuredatalake.dfs.core.windows.net/dim_customer"

if spark.catalog.tableExists(gold_table_name):
    # Perform SCD Type 1 Merge
    delta_tbl = DeltaTable.forName(spark, gold_table_name)
    delta_tbl.alias("tgt").merge(
        df_customer_final.alias("src"),
        "tgt.dim_customer_key = src.dim_customer_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # Initial load
    df_customer_final.write.format("delta").mode("overwrite") \
        .option("path", delta_path).saveAsTable(gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Table

# COMMAND ----------

spark.sql(f"OPTIMIZE {gold_table_name} ZORDER BY (dim_customer_key)")

# COMMAND ----------

# Log success
print(f"Successfully processed {df_customer_final.count()} customer records.")

# COMMAND ----------

