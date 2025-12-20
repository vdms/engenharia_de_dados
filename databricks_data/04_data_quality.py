# Databricks notebook source
# MAGIC %md
# MAGIC # Data Quality — MVP PIX (Bronze → Silver → Gold)
# MAGIC
# MAGIC This notebook provides **run-all evidence** that the pipeline outputs are:
# MAGIC - strictly within the analytical scope (**2023–2024**, `AnoMes` 202301–202412),
# MAGIC - correctly typed (notably `VALOR` as `decimal(18,2)`),
# MAGIC - free of critical nulls and negative metrics,
# MAGIC - consistent with the Gold star schema (no missing or orphan foreign keys),
# MAGIC - dimension keys are unique.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

b2023  = spark.table("mvp_pix.bronze.pix_2023_raw")
b2024  = spark.table("mvp_pix.bronze.pix_2024_raw")

silver = spark.table("mvp_pix.silver.pix_transacoes")

fact          = spark.table("mvp_pix.gold.fato_transacoes_pix")
dim_tempo     = spark.table("mvp_pix.gold.dim_tempo")
dim_usuario   = spark.table("mvp_pix.gold.dim_usuario")
dim_regiao    = spark.table("mvp_pix.gold.dim_regiao")
dim_natureza  = spark.table("mvp_pix.gold.dim_natureza")
dim_finalidade= spark.table("mvp_pix.gold.dim_finalidade")
dim_forma     = spark.table("mvp_pix.gold.dim_forma_iniciacao")


# COMMAND ----------

counts = [
  ("bronze.pix_2023_raw", b2023.count()),
  ("bronze.pix_2024_raw", b2024.count()),
  ("silver.pix_transacoes", silver.count()),
  ("gold.dim_tempo", dim_tempo.count()),
  ("gold.dim_usuario", dim_usuario.count()),
  ("gold.dim_regiao", dim_regiao.count()),
  ("gold.dim_natureza", dim_natureza.count()),
  ("gold.dim_finalidade", dim_finalidade.count()),
  ("gold.dim_forma_iniciacao", dim_forma.count()),
  ("gold.fato_transacoes_pix", fact.count()),
]
spark.createDataFrame(counts, ["table", "row_count"]).display()


# COMMAND ----------

silver.select(
  F.min("AnoMes").alias("silver_min_AnoMes"),
  F.max("AnoMes").alias("silver_max_AnoMes"),
  F.sum(F.when((F.col("AnoMes") < "202301") | (F.col("AnoMes") > "202412"), 1).otherwise(0)).alias("silver_rows_outside_scope"),
  F.sum(F.when(~F.col("AnoMes").rlike(r"^[0-9]{6}$"), 1).otherwise(0)).alias("silver_invalid_AnoMes_format"),
).display()

dim_tempo.select(
  F.min("AnoMes").alias("dim_tempo_min_AnoMes"),
  F.max("AnoMes").alias("dim_tempo_max_AnoMes"),
  F.countDistinct("AnoMes").alias("dim_tempo_distinct_months"),
).display()


# COMMAND ----------

dtypes = dict(silver.dtypes)

checks = [
  ("AnoMes", "string", dtypes.get("AnoMes")),
  ("VALOR", "decimal(18,2)", dtypes.get("VALOR")),
]

rows = [(c, exp, got, got == exp) for c, exp, got in checks]

# QUANTIDADE is expected to be integer-like
q = dtypes.get("QUANTIDADE")
rows.append(("QUANTIDADE", "int/long/bigint", q, q in {"int", "long", "bigint"}))

spark.createDataFrame(rows, ["column", "expected_dtype", "actual_dtype", "pass"]).display()


# COMMAND ----------

critical_silver = [
  "AnoMes",
  "PAG_PFPJ","REC_PFPJ",
  "PAG_REGIAO","REC_REGIAO",
  "PAG_AGE_GROUP","REC_AGE_GROUP",
  "FORMAINICIACAO",
  "NATUREZA","FINALIDADE",
  "VALOR","QUANTIDADE"
]

exprs = [F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(f"null_{c}") for c in critical_silver]

silver.select(
  *exprs,
  F.sum(F.when(F.col("VALOR") < 0, 1).otherwise(0)).alias("negative_VALOR"),
  F.sum(F.when(F.col("QUANTIDADE") < 0, 1).otherwise(0)).alias("negative_QUANTIDADE"),
).display()


# COMMAND ----------

allowed_age = ["<20", "20–29", "30–39", "40–49", "50–59", "60+", "Not informed"]

dim_usuario.select(
  F.sum(F.when(F.col("faixa_etaria").isNull(), 1).otherwise(0)).alias("null_faixa_etaria")
).display()

dim_usuario.select("faixa_etaria").distinct() \
  .where(~F.col("faixa_etaria").isin(allowed_age)) \
  .display()

dim_usuario.groupBy("faixa_etaria").count().orderBy("faixa_etaria").display()

# Note: only categories present in the 2023–2024 scope will appear in the distribution.


# COMMAND ----------

fact.select(
  F.sum(F.when(F.col("valor_total").isNull(), 1).otherwise(0)).alias("null_valor_total"),
  F.sum(F.when(F.col("quantidade_transacoes").isNull(), 1).otherwise(0)).alias("null_quantidade_transacoes"),
  F.sum(F.when(F.col("valor_total") < 0, 1).otherwise(0)).alias("negative_valor_total"),
  F.sum(F.when(F.col("quantidade_transacoes") < 0, 1).otherwise(0)).alias("negative_quantidade_transacoes"),
).display()

fact.select(
  F.sum(F.when(F.col("id_tempo").isNull(), 1).otherwise(0)).alias("null_id_tempo"),
  F.sum(F.when(F.col("id_usuario_pagador").isNull(), 1).otherwise(0)).alias("null_id_usuario_pagador"),
  F.sum(F.when(F.col("id_usuario_recebedor").isNull(), 1).otherwise(0)).alias("null_id_usuario_recebedor"),
  F.sum(F.when(F.col("id_regiao_pagador").isNull(), 1).otherwise(0)).alias("null_id_regiao_pagador"),
  F.sum(F.when(F.col("id_regiao_recebedor").isNull(), 1).otherwise(0)).alias("null_id_regiao_recebedor"),
  F.sum(F.when(F.col("id_natureza").isNull(), 1).otherwise(0)).alias("null_id_natureza"),
  F.sum(F.when(F.col("id_finalidade").isNull(), 1).otherwise(0)).alias("null_id_finalidade"),
  F.sum(F.when(F.col("id_forma_iniciacao").isNull(), 1).otherwise(0)).alias("null_id_forma_iniciacao"),
).display()


# COMMAND ----------

def orphan_fk_count(fact_df, fk_col, dim_df, dim_pk):
    return (
        fact_df.select(F.col(fk_col).alias("fk")).distinct()
        .join(dim_df.select(F.col(dim_pk).alias("pk")).distinct(), on=F.col("fk") == F.col("pk"), how="left_anti")
        .count()
    )

orphan_checks = [
  ("id_tempo", "dim_tempo", orphan_fk_count(fact, "id_tempo", dim_tempo, "id_tempo")),
  ("id_usuario_pagador", "dim_usuario", orphan_fk_count(fact, "id_usuario_pagador", dim_usuario, "id_usuario")),
  ("id_usuario_recebedor", "dim_usuario", orphan_fk_count(fact, "id_usuario_recebedor", dim_usuario, "id_usuario")),
  ("id_regiao_pagador", "dim_regiao", orphan_fk_count(fact, "id_regiao_pagador", dim_regiao, "id_regiao")),
  ("id_regiao_recebedor", "dim_regiao", orphan_fk_count(fact, "id_regiao_recebedor", dim_regiao, "id_regiao")),
  ("id_natureza", "dim_natureza", orphan_fk_count(fact, "id_natureza", dim_natureza, "id_natureza")),
  ("id_finalidade", "dim_finalidade", orphan_fk_count(fact, "id_finalidade", dim_finalidade, "id_finalidade")),
  ("id_forma_iniciacao", "dim_forma_iniciacao", orphan_fk_count(fact, "id_forma_iniciacao", dim_forma, "id_forma_iniciacao")),
]

spark.createDataFrame(orphan_checks, ["fk_column", "dimension", "orphan_distinct_fk_values"]).display()


# COMMAND ----------

# DQ — Concentration view consistency (shares should sum to ~1 per month and role)

from pyspark.sql import functions as F

vw = spark.table("mvp_pix.gold.vw_regional_concentration")

# 1) Share sums per (AnoMes, papel) should be ~1.0
# Use a small tolerance to avoid float drift.
tol = 1e-6

share_sums = (
    vw.groupBy("AnoMes", "papel")
      .agg(
          F.sum("share_valor").alias("sum_share_valor"),
          F.sum("share_quantidade").alias("sum_share_quantidade"),
          F.max("valor_total_nacional").alias("valor_total_nacional"),
          F.max("quantidade_transacoes_nacional").alias("quantidade_transacoes_nacional"),
          F.countDistinct("regiao").alias("distinct_regions"),
      )
      .withColumn("share_valor_ok", (F.abs(F.col("sum_share_valor") - F.lit(1.0)) <= F.lit(tol)) | (F.col("valor_total_nacional") == 0))
      .withColumn("share_quantidade_ok", (F.abs(F.col("sum_share_quantidade") - F.lit(1.0)) <= F.lit(tol)) | (F.col("quantidade_transacoes_nacional") == 0))
)

share_sums.orderBy("AnoMes", "papel").display()

# 2) Violations (should be empty)
violations = share_sums.where(~F.col("share_valor_ok") | ~F.col("share_quantidade_ok"))
violations.display()


# COMMAND ----------

def pk_dupes(df, pk):
    return df.groupBy(pk).count().where(F.col("count") > 1).count()

checks = [
  ("dim_tempo", "id_tempo", pk_dupes(dim_tempo, "id_tempo")),
  ("dim_usuario", "id_usuario", pk_dupes(dim_usuario, "id_usuario")),
  ("dim_regiao", "id_regiao", pk_dupes(dim_regiao, "id_regiao")),
  ("dim_natureza", "id_natureza", pk_dupes(dim_natureza, "id_natureza")),
  ("dim_finalidade", "id_finalidade", pk_dupes(dim_finalidade, "id_finalidade")),
  ("dim_forma_iniciacao", "id_forma_iniciacao", pk_dupes(dim_forma, "id_forma_iniciacao")),
]

spark.createDataFrame(checks, ["table", "pk", "duplicate_pk_groups"]).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion (Data Quality)
# MAGIC
# MAGIC The outputs above provide objective evidence that:
# MAGIC - Silver is strictly constrained to the **2023–2024** scope (`AnoMes` within `202301–202412` and valid `YYYYMM` format).
# MAGIC - Silver enforces the type contract, including **`VALOR` as `decimal(18,2)`**.
# MAGIC - Critical fields show no unexpected nulls and no negative metrics.
# MAGIC - Age groups are validated in the **Gold business dimension** (`dim_usuario.faixa_etaria`) against a controlled domain.
# MAGIC - The Gold fact table has complete foreign keys (no nulls) and **no orphan keys** relative to its dimensions.
# MAGIC - Dimension primary keys are unique.
# MAGIC
# MAGIC Limitations:
# MAGIC - The dataset is aggregated (not transaction-level), so analyses reflect grouped PIX activity rather than individual transfers.
# MAGIC