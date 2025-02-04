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
df_silver_product = spark.read.format("parquet").load(silver_path) \
    .select(
        col("Product_ID"),
        col("Category"),
        col("Sub_Category"),
        col("Product_Name"),
        col("Manufacturer")
    ).distinct()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if Gold Table Exists
# MAGIC

# COMMAND ----------

gold_table_name = "sales_catalog.gold.dim_product"
if spark.catalog.tableExists(gold_table_name):
    df_gold_product = spark.read.format("delta").table(gold_table_name)
else:
    # Create an empty DataFrame with the same schema
    df_gold_product = spark.createDataFrame([], df_silver_product.schema.add("dim_product_key", "long"))


# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Upsert Records

# COMMAND ----------

df_product_filter = df_silver_product.join(df_gold_product, ["Product_ID"], "left") \
    .select(
        df_silver_product["*"],
        df_gold_product["dim_product_key"]
    )

# COMMAND ----------

# Split into old and new records
df_product_filter_old = df_product_filter.filter(col("dim_product_key").isNotNull())
df_product_filter_new = df_product_filter.filter(col("dim_product_key").isNull()) \
    .select(df_silver_product["*"])

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Surrogate Keys for New Records

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_value_df = spark.sql(f"SELECT MAX(dim_product_key) FROM {gold_table_name}")
    max_value = max_value_df.collect()[0][0] or 0  # Handle null case

# Use row_number() instead of monotonically_increasing_id() for better performance
df_product_filter_new = df_product_filter_new.withColumn(
    "dim_product_key",
    max_value + row_number().over(Window.orderBy("Product_ID"))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Old and New Records

# COMMAND ----------

df_product_final = df_product_filter_old.union(df_product_filter_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Table

# COMMAND ----------

delta_path = "abfss://gold@mgazuredatalake.dfs.core.windows.net/dim_product"

if spark.catalog.tableExists(gold_table_name):
    # Perform SCD Type 1 Merge
    delta_tbl = DeltaTable.forName(spark, gold_table_name)
    delta_tbl.alias("tgt").merge(
        df_product_final.alias("src"),
        "tgt.dim_product_key = src.dim_product_key"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # Initial load
    df_product_final.write.format("delta").mode("overwrite") \
        .option("path", delta_path).saveAsTable(gold_table_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Table

# COMMAND ----------

spark.sql(f"OPTIMIZE {gold_table_name} ZORDER BY (dim_product_key)")

# COMMAND ----------

# Log success
print(f"Successfully processed {df_product_final.count()} records.")

# COMMAND ----------

