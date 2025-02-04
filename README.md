# Data Pipeline for Synthetic Sales Data Analysis

## Overview

This project demonstrates a scalable and production-ready data pipeline using **Azure Data Factory (ADF)** and various Azure services to process synthetic sales data. The solution is designed for efficient ingestion, transformation, and analysis of large-scale datasets, leveraging services like **Azure Data Lake Storage Gen2**, **Azure SQL Database**, **Azure Blob Storage**, **Databricks**, and **Power BI**. Additionally, we implemented **CI/CD pipelines** via **Azure DevOps**, automating the release process of ADF pipelines across Dev, Test, and Production environments while integrating **Azure DevOps Repos** with ADF and **Git**.

The pipeline processes 10 million rows of synthetic sales data, transforming it into analytics-ready dimension and fact tables for reporting and dashboards.

---

## Architecture

![System Architecture](https://github.com/user-attachments/assets/bceb5bfb-8a5d-4af6-9e37-1712d3065a53)


- **Data Sources**:  
  - CSV files in **Azure Blob Storage** containing synthetic sales data.
  - Python scripts generate realistic sales data with attributes like customer demographics, product details, and order information.

- **Data Storage**:  
  - **Azure Data Lake Storage Gen2 (ADLS2)**: Stores raw, transformed, and modeled data in Bronze, Silver, and Gold layers.

- **Data Processing**:  
  - **Azure Data Factory (ADF)** orchestrates the entire data pipeline using activities such as:
    - Pipelines for incremental data loads.
    - **Select**, **Filter**, **Pivot**, and **Lookup** transformations.
    - **Data Flows** for advanced transformations.
    - **Datasets** and **Linked Services** for connecting to Azure Blob Storage, Azure SQL Database, and ADLS2.


---

## Features

- **Synthetic Data Generation**:  
  Generates 10 million rows of realistic sales data using Python and the Faker library. Attributes include customer demographics, product categories, payment methods, and order details.

![image](https://github.com/user-attachments/assets/ac8544a4-5e04-4123-8f3c-936de7adb05b)

- **Incremental Data Processing**:  
  Uses a watermark table to track and process only new or updated records, ensuring efficient data ingestion.

![image](https://github.com/user-attachments/assets/4cc6f86d-63ad-4db8-9f9a-ae447afcf3f9)

- **Data Transformations**:  
  Applies filtering, pivoting, lookups, and aggregations using ADF Data Flow to clean and enrich the data.

![image](https://github.com/user-attachments/assets/b2c8fd56-28c6-4743-87c5-af4c834e7105)


- **Scalable Storage**:  
  Stores raw, transformed, and modeled data in Azure Data Lake Storage Gen2, organized into Bronze, Silver, and Gold layers.

- **CI/CD Integration**:  
  Automates the deployment of ADF pipelines across environments (Dev, Test, Production) using Azure DevOps.

---

## Prerequisites

Before running the pipeline, ensure you have the following:

1. **Azure Subscription**: An active Azure subscription to deploy resources.
2. **Azure Services**:
   - Azure Blob Storage
   - Azure SQL Database
   - Azure Data Lake Storage Gen2
   - Azure Data Factory
3. **Python Environment**:
   - Python 3.8+ installed.
   - Required libraries: `pandas`, `faker`, `random`.
4. **Development Tools**:
   - Visual Studio Code or any IDE for editing code.
   - Azure CLI or Azure Portal for managing resources.
5. **Azure DevOps**:
   - Configured CI/CD pipelines for automating deployments.


## Pipeline Details

### 1. Synthetic Data Generation

The script `generate_synthetic_data.py` generates 10 million rows of sales data with attributes such as:
- Customer demographics (gender, age, loyalty tier, etc.)
- Product details (category, sub-category, price, etc.)
- Order information (order ID, ship date, payment method, etc.)

### 2. Data Ingestion Pipeline

The ADF pipeline `ingest_data_pipeline.json` performs the following steps:
1. Validates the source file metadata.
2. Copies data from Azure Blob Storage to Azure SQL Database (`sales_src` table).

### 3. Incremental Data Pipeline

The ADF pipeline `increment_data_pipeline.json` handles incremental loads:
1. Reads new or updated records from Azure SQL Database using a watermark table (`water_tbl`).
2. Writes incremental data to Parquet files in Azure Data Lake (Bronze layer).

### 4. Data Flow Transformations

The Data Flow `df_transformation` applies transformations such as:
- Filtering data based on payment method and customer gender.
- Pivoting sales data by gender and product category.
- Enriching data with geographical details using lookups.

---
Certainly! Below is an updated version of the **README.md** section that includes details about loading data from the Bronze layer to the Silver layer, merging and modeling dimensions and fact tables in the Gold layer using Databricks.

---

### 5. Loading Data from Bronze to Silver Layer

After the initial transformations in the Data Flow, the processed data is written to the **Bronze layer** in Azure Data Lake Storage Gen2. The next step involves loading this data into the **Silver layer** for further cleaning, deduplication, and transformation.

#### **Steps in Databricks:**

1. **Read Data from Bronze Layer**:
   - Use PySpark to read Parquet files from the Bronze layer stored in Azure Data Lake Storage Gen2.
   - Example:
     ```python
     bronze_df = spark.read.format("parquet").load("abfss://bronze@datalake.dfs.core.windows.net/raw_data/")
     ```

2. **Clean and Deduplicate Data**:
   - Remove duplicate rows and handle missing values.
   - Example:
     ```python
     silver_df = bronze_df.dropDuplicates(["Row_ID"]).na.fill({"Customer_Gender": "Unknown"})
     ```

3. **Transform Data**:
   - Apply additional transformations such as standardizing column names, formatting dates, and converting data types.
   - Example:
     ```python
     from pyspark.sql.functions import col
     silver_df = silver_df.withColumn("Customer_Age", col("Customer_Age").cast("integer"))
     ```

4. **Write Data to Silver Layer**:
   - Write the cleaned and transformed data back to Azure Data Lake Storage Gen2 in the Silver layer.
   - Example:
     ```python
     silver_df.write.format("parquet").mode("overwrite").save("abfss://silver@datalake.dfs.core.windows.net/transformed_data/")
     ```

---

### 6. Merging and Modeling Dimensions and Fact Tables in Gold Layer

The final step in the pipeline is to merge and model the data into **dimension** and **fact tables** in the **Gold layer**. This process involves creating star-schema models for analytics and reporting.

#### **Steps in Databricks:**

1. **Load Data from Silver Layer**:
   - Read the transformed data from the Silver layer.
   - Example:
     ```python
     silver_df = spark.read.format("parquet").load("abfss://silver@datalake.dfs.core.windows.net/transformed_data/")
     ```

2. **Create Dimension Tables**:
   - Extract unique attributes to create dimension tables such as `Dim_Customer`, `Dim_Product`, and `Dim_Order`.
   - Example for `Dim_Customer`:
     ```python
     dim_customer = silver_df.select(
         "Customer_ID",
         "Customer_Name",
         "Customer_Gender",
         "Customer_Age",
         "Customer_Loyalty_Tier"
     ).distinct()
     dim_customer.write.format("delta").mode("overwrite").save("abfss://gold@datalake.dfs.core.windows.net/dim_customer/")
     ```

3. **Create Fact Table**:
   - Aggregate transactional data to create the `Fact_Sales` table.
   - Example:
     ```python
     from pyspark.sql.functions import sum
     fact_sales = silver_df.groupBy(
         "Order_ID",
         "Customer_ID",
         "Product_ID",
         "Order_Date"
     ).agg(
         sum("Sales").alias("Total_Sales"),
         sum("Profit").alias("Total_Profit")
     )
     fact_sales.write.format("delta").mode("overwrite").save("abfss://gold@datalake.dfs.core.windows.net/fact_sales/")
     ```

4. **Implement SCD Type 1 Updates**:
   - For slowly changing dimensions (e.g., `Dim_Customer`), implement SCD Type 1 updates to overwrite existing records with new data.
   - Example:
     ```python
     from pyspark.sql.functions import lit
     dim_customer_updated = dim_customer.withColumn("Modification_Date", lit(current_timestamp()))
     dim_customer_updated.write.format("delta").mode("overwrite").save("abfss://gold@datalake.dfs.core.windows.net/dim_customer/")
     ```

5. **Optimize Delta Tables**:
   - Optimize the Gold layer Delta tables for query performance using `OPTIMIZE` and `ZORDER BY`.
   - Example:
     ```python
     spark.sql("OPTIMIZE delta.`abfss://gold@datalake.dfs.core.windows.net/fact_sales/` ZORDER BY (Order_Date)")
     ```

---

### 7. Analytics Layer

The modeled data in the **Gold layer** is now ready for analytics and reporting. Tools like **Power BI**, **Tableau**, or **Azure Synapse Analytics** can be used to create dashboards and visualizations.

---

This structure provides a comprehensive overview of the entire pipeline, from raw data ingestion to analytics-ready data modeling. Let me know if you'd like to refine any part of it further!
