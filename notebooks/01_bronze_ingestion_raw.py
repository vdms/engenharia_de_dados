# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Data Ingestion
# MAGIC
# MAGIC This notebook ingests the raw PIX CSV files into Databricks, preserving their original structure and saving them as Bronze Delta tables without any transformation or business rules.

# COMMAND ----------

path_2023 = "/Volumes/mvp_pix/dados/bronze/pix_2023.csv"
path_2024 = "/Volumes/mvp_pix/dados/bronze/pix_2024.csv"

# COMMAND ----------

df_2023 = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path_2023)
)

df_2024 = (
    spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path_2024)
)

# COMMAND ----------

display(df_2023.limit(10))
display(df_2024.limit(10))

print("Rows 2023:", df_2023.count())
print("Rows 2024:", df_2024.count())

print(df_2023.columns)

# COMMAND ----------

# Inspect distinct age values for payer and receiver to identify inconsistencies or non-standard representations (e.g., ranges, missing values)
df_2023.select("PAG_IDADE", "REC_IDADE").distinct().limit(20).display()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mvp_pix.bronze;

# COMMAND ----------

(
    df_2023.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("mvp_pix.bronze.pix_2023_raw")
)

(
    df_2024.write
        .mode("overwrite")
        .format("delta")
        .saveAsTable("mvp_pix.bronze.pix_2024_raw")
)


# COMMAND ----------

spark.table("mvp_pix.bronze.pix_2023_raw").limit(5).display()
spark.table("mvp_pix.bronze.pix_2024_raw").limit(5).display()

# COMMAND ----------

# MAGIC %md
# MAGIC > In the Bronze layer, the original PIX datasets for 2023 and 2024 were ingested and persisted as Delta tables without any transformation. All original attributes and values were preserved, including categorical age ranges and administrative values such as “Not applicable” and “Not informed”. This layer serves as a faithful representation of the source data and as the foundation for subsequent cleaning and standardization steps.