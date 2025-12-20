# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Cleaning and Standardization
# MAGIC
# MAGIC This notebook unifies the Bronze datasets (2023 and 2024), standardizes types and key categorical fields, and prepares the data for analytical modeling in the Gold layer.
# MAGIC

# COMMAND ----------

b2023 = spark.table("mvp_pix.bronze.pix_2023_raw")
b2024 = spark.table("mvp_pix.bronze.pix_2024_raw")

df = b2023.unionByName(b2024)


# COMMAND ----------

# Casts AnoMes to string and QUANTIDADE to long for consistency.

from pyspark.sql import functions as F

df = (
  df.withColumn("AnoMes", F.col("AnoMes").cast("string"))
    .withColumn("QUANTIDADE", F.col("QUANTIDADE").cast("long"))
)


# COMMAND ----------

# Adjustment added after final review.
# Keep only the analytical scope: 2023–2024.

df = df.filter(
    (F.col("AnoMes") >= "202301") &
    (F.col("AnoMes") <= "202412")
)

# COMMAND ----------

# Test the analytical scope.

df.select(
    F.min("AnoMes").alias("min_AnoMes"),
    F.max("AnoMes").alias("max_AnoMes"),
    F.count("*").alias("rows")
).display()


# COMMAND ----------

# Standardize age groups

from pyspark.sql import functions as F

# Safety: make sure inputs are strings and trimmed
df = df.withColumn("PAG_IDADE", F.trim(F.col("PAG_IDADE").cast("string"))) \
       .withColumn("REC_IDADE", F.trim(F.col("REC_IDADE").cast("string")))

df = df.withColumn(
    "PAG_AGE_GROUP",
    F.when(F.col("PAG_IDADE").rlike("até 19"), "<20")
     .when(F.col("PAG_IDADE").rlike("20 e 29"), "20–29")
     .when(F.col("PAG_IDADE").rlike("30 e 39"), "30–39")
     .when(F.col("PAG_IDADE").rlike("40 e 49"), "40–49")
     .when(F.col("PAG_IDADE").rlike("50 e 59"), "50–59")
     .when(F.col("PAG_IDADE").rlike(r"^60"), "60+")
     .otherwise("Not informed")
)

df = df.withColumn(
    "REC_AGE_GROUP",
    F.when(F.col("REC_IDADE").rlike("até 19"), "<20")
     .when(F.col("REC_IDADE").rlike("20 e 29"), "20–29")
     .when(F.col("REC_IDADE").rlike("30 e 39"), "30–39")
     .when(F.col("REC_IDADE").rlike("40 e 49"), "40–49")
     .when(F.col("REC_IDADE").rlike("50 e 59"), "50–59")
     .when(F.col("REC_IDADE").rlike(r"^60"), "60+")
     .otherwise("Not informed")
)


# COMMAND ----------

# Standardize categorical fields: trim, normalize admin values, and enforce UPPERCASE consistently.

from pyspark.sql import functions as F

def normalize_admin_values(col):
    c = F.upper(F.trim(F.col(col).cast("string")))
    return (
        F.when(c.isNull() | (c == ""), "NOT INFORMED")
         .when(c.isin("NÃO INFORMADO", "NAO INFORMADO"), "NOT INFORMED")
         .when(c.isin("NÃO DISPONÍVEL", "NAO DISPONIVEL"), "NOT AVAILABLE")
         .when(c.isin("NÃO SE APLICA", "NAO SE APLICA"), "NOT APPLICABLE")
         .otherwise(c)
    )

for c in ["PAG_REGIAO","REC_REGIAO","NATUREZA","FINALIDADE","FORMAINICIACAO"]:
    df = df.withColumn(c, normalize_admin_values(c))


# COMMAND ----------

cols = [
  "AnoMes",
  "PAG_PFPJ","REC_PFPJ",
  "PAG_REGIAO","REC_REGIAO",
  "PAG_IDADE","REC_IDADE",
  "PAG_AGE_GROUP","REC_AGE_GROUP", 
  "FORMAINICIACAO",
  "NATUREZA","FINALIDADE",
  "VALOR","QUANTIDADE"
]
df = df.select(*cols)


# COMMAND ----------

# Standardize and cast VALOR to decimal(18,2).
# Reason: VALOR comes as a string with Brazilian formatting (thousands separator as '.' and decimal as ',').
# This cell removes thousands separators, trims spaces, replaces ',' with '.', and casts to decimal for analysis.

valor_str = F.trim(F.col("VALOR").cast("string"))

valor_sem_milhar = F.regexp_replace(valor_str, r"\.", "")
valor_sem_milhar = F.regexp_replace(valor_sem_milhar, r"\s+", "")
valor_padrao = F.regexp_replace(valor_sem_milhar, r",", ".")

df = df.withColumn("VALOR", valor_padrao.cast("decimal(18,2)"))

# COMMAND ----------

# Test the VALOR column.

df.select(
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("VALOR").isNull(), 1).otherwise(0)).alias("valor_null_after_cast"),
    F.min("VALOR").alias("valor_min"),
    F.max("VALOR").alias("valor_max"),
).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mvp_pix.silver;
# MAGIC

# COMMAND ----------

(df.write
  .mode("overwrite")
  .format("delta")
  .saveAsTable("mvp_pix.silver.pix_transacoes"))


# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes on Silver layer decisions
# MAGIC
# MAGIC Two issues were addressed during the Silver build:
# MAGIC
# MAGIC 1) **`VALOR` typing**  
# MAGIC The source provides `VALOR` as a string using Brazilian numeric formatting (thousand separator `.` and decimal separator `,`). The pipeline normalizes the string and casts the field to `decimal(18,2)` to avoid floating-point artifacts.
# MAGIC
# MAGIC 2) **Analytical scope (2023–2024)**  
# MAGIC After combining the 2023 and 2024 raw datasets, a strict filter is applied to keep only `AnoMes` between `202301` and `202412`, enforcing the MVP time window before persisting the Silver table.
# MAGIC
# MAGIC These decisions ensure Silver enforces typing, scope control, and categorical standardization prior to Gold modeling.
# MAGIC