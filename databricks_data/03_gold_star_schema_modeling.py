# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Star Schema Modeling
# MAGIC
# MAGIC This notebook builds the analytical model (star schema) from the Silver layer by creating dimension tables and an aggregated fact table optimized for analytical queries.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table("mvp_pix.silver.pix_transacoes")

# Quick sanity
silver.select(
    F.min("AnoMes").alias("min_AnoMes"),
    F.max("AnoMes").alias("max_AnoMes"),
    F.count("*").alias("rows")
).display()


# COMMAND ----------

scope = silver.select(
    F.min("AnoMes").alias("min_AnoMes"),
    F.max("AnoMes").alias("max_AnoMes")
).collect()[0]

assert scope["min_AnoMes"] >= "202301" and scope["max_AnoMes"] <= "202412", f"Out of scope: {scope}"
print("OK: temporal scope is within 2023–2024")


# COMMAND ----------

dim_tempo = (
    silver.select("AnoMes").distinct()
        .withColumn("ano", F.substring("AnoMes", 1, 4).cast("int"))
        .withColumn("mes", F.substring("AnoMes", 5, 2).cast("int"))
        .withColumn("trimestre", F.ceil(F.col("mes") / 3).cast("int"))
        .withColumn(
            "id_tempo",
            F.abs(F.xxhash64(F.col("AnoMes"))).cast("long")
        )
        .select("id_tempo", "AnoMes", "ano", "mes", "trimestre")
)

# COMMAND ----------

u_pag = (
    silver.select(
        F.col("PAG_PFPJ").alias("tipo_pessoa"),
        F.col("PAG_AGE_GROUP").alias("faixa_etaria")
    )
)

u_rec = (
    silver.select(
        F.col("REC_PFPJ").alias("tipo_pessoa"),
        F.col("REC_AGE_GROUP").alias("faixa_etaria")
    )
)

dim_usuario = (
    u_pag.unionByName(u_rec).distinct()
    .withColumn(
        "id_usuario",
        F.abs(F.xxhash64(
            F.coalesce(F.col("tipo_pessoa"), F.lit("NOT INFORMED")),
            F.coalesce(F.col("faixa_etaria"), F.lit("Not informed"))
        )).cast("long")
    )
    .select("id_usuario", "tipo_pessoa", "faixa_etaria")
)


# COMMAND ----------

r_pag = (
    silver.select(F.col("PAG_REGIAO").alias("regiao"))
    .withColumn("papel", F.lit("Pagador"))
)

r_rec = (
    silver.select(F.col("REC_REGIAO").alias("regiao"))
    .withColumn("papel", F.lit("Recebedor"))
)

dim_regiao = (
    r_pag.unionByName(r_rec).distinct()
    .withColumn(
        "id_regiao",
        F.abs(F.xxhash64(
            F.coalesce(F.col("papel"), F.lit("UNKNOWN")),
            F.coalesce(F.col("regiao"), F.lit("NOT INFORMED"))
        )).cast("long")
    )
    .select("id_regiao", "papel", "regiao")
)


# COMMAND ----------

dim_natureza = (
    silver.select("NATUREZA").distinct()
    .withColumnRenamed("NATUREZA", "natureza")
    .withColumn("id_natureza", F.abs(F.xxhash64(F.coalesce(F.col("natureza"), F.lit("NOT INFORMED")))).cast("long"))
    .select("id_natureza", "natureza")
)

dim_finalidade = (
    silver.select("FINALIDADE").distinct()
    .withColumnRenamed("FINALIDADE", "finalidade")
    .withColumn("id_finalidade", F.abs(F.xxhash64(F.coalesce(F.col("finalidade"), F.lit("NOT INFORMED")))).cast("long"))
    .select("id_finalidade", "finalidade")
)

dim_forma_iniciacao = (
    silver.select("FORMAINICIACAO").distinct()
    .withColumnRenamed("FORMAINICIACAO", "forma_iniciacao")
    .withColumn("id_forma_iniciacao", F.abs(F.xxhash64(F.coalesce(F.col("forma_iniciacao"), F.lit("NOT INFORMED")))).cast("long"))
    .select("id_forma_iniciacao", "forma_iniciacao")
)

# COMMAND ----------

# Build fact at a clear grain: (tempo, usuario_pagador, usuario_recebedor, regiao_pagador, regiao_recebedor, natureza, finalidade, forma)
# Always aggregate metrics to prevent accidental duplication.

from pyspark.sql import functions as F

def nz(col_name: str, default: str = "NOT INFORMED"):
    return F.coalesce(F.col(col_name).cast("string"), F.lit(default))

fato_transacoes_pix = (
    silver
    # Null-safe hashing inputs (avoid null IDs)
    .withColumn("id_tempo", F.abs(F.xxhash64(nz("AnoMes"))).cast("long"))
    .withColumn("id_usuario_pagador", F.abs(F.xxhash64(nz("PAG_PFPJ"), nz("PAG_AGE_GROUP", "Not informed"))).cast("long"))
    .withColumn("id_usuario_recebedor", F.abs(F.xxhash64(nz("REC_PFPJ"), nz("REC_AGE_GROUP", "Not informed"))).cast("long"))
    .withColumn("id_regiao_pagador", F.abs(F.xxhash64(F.lit("Pagador"), nz("PAG_REGIAO"))).cast("long"))
    .withColumn("id_regiao_recebedor", F.abs(F.xxhash64(F.lit("Recebedor"), nz("REC_REGIAO"))).cast("long"))
    .withColumn("id_natureza", F.abs(F.xxhash64(nz("NATUREZA"))).cast("long"))
    .withColumn("id_finalidade", F.abs(F.xxhash64(nz("FINALIDADE"))).cast("long"))
    .withColumn("id_forma_iniciacao", F.abs(F.xxhash64(nz("FORMAINICIACAO"))).cast("long"))
    # Aggregate
    .groupBy(
        "id_tempo",
        "id_usuario_pagador",
        "id_usuario_recebedor",
        "id_regiao_pagador",
        "id_regiao_recebedor",
        "id_natureza",
        "id_finalidade",
        "id_forma_iniciacao",
    )
    .agg(
        F.sum(F.col("QUANTIDADE")).cast("long").alias("quantidade_transacoes"),
        F.sum(F.col("VALOR")).cast("decimal(18,2)").alias("valor_total"),
    )
)

# Optional: quick peek (keep ONE display, not many)
fato_transacoes_pix.limit(10).display()

# Grain check: no duplicates at the fact key level
key_cols = [
    "id_tempo",
    "id_usuario_pagador",
    "id_usuario_recebedor",
    "id_regiao_pagador",
    "id_regiao_recebedor",
    "id_natureza",
    "id_finalidade",
    "id_forma_iniciacao",
]

dup = (
    fato_transacoes_pix
    .groupBy(*key_cols).count()
    .where(F.col("count") > 1)
    .count()
)

print(f"duplicate fact key groups: {dup}")

# COMMAND ----------

fato_transacoes_pix.select(
    F.count("*").alias("rows"),
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

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS mvp_pix.gold;
# MAGIC

# COMMAND ----------

def save_delta(df, table_name):
    (df.write
      .mode("overwrite")
      .format("delta")
      .option("overwriteSchema", "true")
      .saveAsTable(table_name))

save_delta(dim_tempo, "mvp_pix.gold.dim_tempo")
save_delta(dim_usuario, "mvp_pix.gold.dim_usuario")
save_delta(dim_regiao, "mvp_pix.gold.dim_regiao")
save_delta(dim_natureza, "mvp_pix.gold.dim_natureza")
save_delta(dim_finalidade, "mvp_pix.gold.dim_finalidade")
save_delta(dim_forma_iniciacao, "mvp_pix.gold.dim_forma_iniciacao")
save_delta(fato_transacoes_pix, "mvp_pix.gold.fato_transacoes_pix")
print("OK: Gold tables saved")


# COMMAND ----------

# Regional concentration view (analytical helper)
# Purpose: provide a repeatable, query-ready artifact for concentration analysis
# across regions, comparing value and transaction count.
#
# Grain: (AnoMes, papel, regiao)
# Metrics: totals, share of national totals, ranks, cumulative shares.

spark.sql("""
CREATE OR REPLACE VIEW mvp_pix.gold.vw_regional_concentration AS
WITH base AS (
  -- Project payer and receiver regional perspectives into a single role-playing view
  SELECT
    f.id_tempo,
    'Pagador'  AS papel,
    f.id_regiao_pagador  AS id_regiao,
    f.valor_total,
    f.quantidade_transacoes
  FROM mvp_pix.gold.fato_transacoes_pix f

  UNION ALL

  SELECT
    f.id_tempo,
    'Recebedor' AS papel,
    f.id_regiao_recebedor AS id_regiao,
    f.valor_total,
    f.quantidade_transacoes
  FROM mvp_pix.gold.fato_transacoes_pix f
),
by_region AS (
  SELECT
    t.AnoMes,
    b.papel,
    r.regiao,
    SUM(b.valor_total)               AS valor_total_regiao,
    SUM(b.quantidade_transacoes)     AS quantidade_transacoes_regiao
  FROM base b
  INNER JOIN mvp_pix.gold.dim_tempo  t ON t.id_tempo  = b.id_tempo
  INNER JOIN mvp_pix.gold.dim_regiao r ON r.id_regiao = b.id_regiao
  GROUP BY t.AnoMes, b.papel, r.regiao
),
with_totals AS (
  SELECT
    *,
    SUM(valor_total_regiao) OVER (PARTITION BY AnoMes, papel)           AS valor_total_nacional,
    SUM(quantidade_transacoes_regiao) OVER (PARTITION BY AnoMes, papel) AS quantidade_transacoes_nacional
  FROM by_region
),
final AS (
  SELECT
    AnoMes,
    papel,
    regiao,
    valor_total_regiao,
    quantidade_transacoes_regiao,
    valor_total_nacional,
    quantidade_transacoes_nacional,

    CASE WHEN valor_total_nacional = 0 THEN 0
         ELSE valor_total_regiao / valor_total_nacional
    END AS share_valor,

    CASE WHEN quantidade_transacoes_nacional = 0 THEN 0
         ELSE quantidade_transacoes_regiao / quantidade_transacoes_nacional
    END AS share_quantidade,

    DENSE_RANK() OVER (PARTITION BY AnoMes, papel ORDER BY valor_total_regiao DESC)           AS rank_valor,
    DENSE_RANK() OVER (PARTITION BY AnoMes, papel ORDER BY quantidade_transacoes_regiao DESC) AS rank_quantidade,

    SUM(
      CASE WHEN valor_total_nacional = 0 THEN 0
           ELSE valor_total_regiao / valor_total_nacional
      END
    ) OVER (PARTITION BY AnoMes, papel ORDER BY valor_total_regiao DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_share_valor,

    SUM(
      CASE WHEN quantidade_transacoes_nacional = 0 THEN 0
           ELSE quantidade_transacoes_regiao / quantidade_transacoes_nacional
      END
    ) OVER (PARTITION BY AnoMes, papel ORDER BY quantidade_transacoes_regiao DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_share_quantidade

  FROM with_totals
)
SELECT * FROM final
""")

print("OK: Created view mvp_pix.gold.vw_regional_concentration")


# COMMAND ----------

spark.table("mvp_pix.gold.vw_regional_concentration").select(F.count("*").alias("rows")).display()


# COMMAND ----------

spark.table("mvp_pix.gold.fato_transacoes_pix").select(F.count("*").alias("rows")).display()
