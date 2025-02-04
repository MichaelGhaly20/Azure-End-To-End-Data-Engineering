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
df_silver_fact = spark.read.format("parquet").load(silver_path) \
    .select(
        col("Customer_ID"),
        col("Product_ID"),
        col("Order_ID"),
        col("Sales"),
        col("Cost"),
        col("Profit"),
        col("Quantity"),
        col("Discount"),
        col("Shipping_Cost"),
        col("Return_Status"),
        col("Return_Reason"),
        col("Order_Date"),
        col("Ship_Date"),
        col("Create_Date"),
        col("Modification_Date")
    ).distinct()

# COMMAND ----------

# Repartition the Silver layer data
df_silver_fact = df_silver_fact.repartition(200, "Customer_ID", "Product_ID", "Order_ID").cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Read Dimension Tables

# COMMAND ----------

gold_table_customer = "sales_catalog.gold.dim_customer"
gold_table_product = "sales_catalog.gold.dim_product"
gold_table_order = "sales_catalog.gold.dim_order"

df_dim_customer = spark.read.format("delta").table(gold_table_customer).select("Customer_ID", "dim_customer_key")
df_dim_product = spark.read.format("delta").table(gold_table_product).select("Product_ID", "dim_product_key")
df_dim_order = spark.read.format("delta").table(gold_table_order).select("Order_ID", "dim_order_key")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Join Silver Data with Dimension Tables

# COMMAND ----------

df_fact_joined = df_silver_fact \
    .join(df_dim_customer, "Customer_ID", "left") \
    .join(df_dim_product, "Product_ID", "left") \
    .join(df_dim_order, "Order_ID", "left") \
    .select(
        col("dim_customer_key").alias("Customer_Key"),
        col("dim_product_key").alias("Product_Key"),
        col("dim_order_key").alias("Order_Key"),
        col("Sales"),
        col("Cost"),
        col("Profit"),
        col("Quantity"),
        col("Discount"),
        col("Shipping_Cost"),
        col("Return_Status"),
        col("Return_Reason"),
        col("Order_Date"),
        col("Ship_Date"),
        col("Create_Date"),
        col("Modification_Date")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generate Surrogate Key for Fact Table

# COMMAND ----------

window_spec = Window.orderBy("Order_Date", "Customer_Key", "Product_Key")
df_fact_final = df_fact_joined.withColumn("Row_ID", row_number().over(window_spec))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write to Delta Table

# COMMAND ----------

gold_table_fact = "sales_catalog.gold.fact_sales"
delta_path_fact = "abfss://gold@mgazuredatalake.dfs.core.windows.net/fact_sales"

if spark.catalog.tableExists(gold_table_fact):
    # Perform SCD Type 1 Merge
    delta_tbl = DeltaTable.forName(spark, gold_table_fact)
    delta_tbl.alias("tgt").merge(
        df_fact_final.alias("src"),
        """
        tgt.Customer_Key = src.Customer_Key AND
        tgt.Product_Key = src.Product_Key AND
        tgt.Order_Key = src.Order_Key
        """
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    # Initial load
    df_fact_final.write.format("delta") \
        .mode("overwrite") \
        .option("path", delta_path_fact) \
        .saveAsTable(gold_table_fact)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Optimize Delta Table (Optional but Recommended)

# COMMAND ----------

spark.sql(f"OPTIMIZE {gold_table_fact} ZORDER BY (Order_Date, Customer_Key, Product_Key)")

# COMMAND ----------

# Log success
print(f"Successfully processed {df_fact_final.count()} records.")

# COMMAND ----------

df_silver_fact.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from sales_catalog.gold.fact_sales order by Customer_Key,Product_Key,Order_Key

# COMMAND ----------

