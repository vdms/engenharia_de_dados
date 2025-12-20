# Databricks notebook source
# MAGIC %md
# MAGIC # Business Questions — Evidence Notebook (Gold Layer)
# MAGIC
# MAGIC This notebook provides **reproducible analytical evidence** for the MVP business questions using the Gold star schema.
# MAGIC
# MAGIC **Scope:** Jan/2023 to Dec/2024 (`AnoMes` 202301–202412)  
# MAGIC **Source:** `mvp_pix.gold` (fact + dimensions + helper view)
# MAGIC
# MAGIC Notes:
# MAGIC - Each answered question produces a single primary output designed for screenshots.
# MAGIC - Two business questions (income level; essential vs non-essential expenses) are intentionally not answered due to dataset limitations and are discussed in the final conclusion.

# COMMAND ----------

from pyspark.sql import functions as F

SCOPE_START = "202301"
SCOPE_END   = "202412"

SNAPSHOT_MONTH = "202412"  # used to generate chart-friendly, screenshot-ready outputs

fact = spark.table("mvp_pix.gold.fato_transacoes_pix")
t    = spark.table("mvp_pix.gold.dim_tempo")
u    = spark.table("mvp_pix.gold.dim_usuario")
r    = spark.table("mvp_pix.gold.dim_regiao")
n    = spark.table("mvp_pix.gold.dim_natureza")
p    = spark.table("mvp_pix.gold.dim_finalidade")
m    = spark.table("mvp_pix.gold.dim_forma_iniciacao")

vw_conc = spark.table("mvp_pix.gold.vw_regional_concentration")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Q1 — Monthly evolution + top payer age groups
# MAGIC **Question:** Which age groups most frequently make PIX payments, and how does this pattern evolve over time?
# MAGIC
# MAGIC Evidence is provided in two outputs:
# MAGIC 1) Monthly evolution of total PIX activity (baseline trend).
# MAGIC 2) Top payer age groups in the 2023–2024 scope (to identify the most active profiles).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.AnoMes,
# MAGIC   SUM(f.quantidade_transacoes) AS total_transactions,
# MAGIC   SUM(f.valor_total)           AS total_value_brl
# MAGIC FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC JOIN mvp_pix.gold.dim_tempo t
# MAGIC   ON f.id_tempo = t.id_tempo
# MAGIC WHERE t.AnoMes BETWEEN '202301' AND '202412'
# MAGIC GROUP BY t.AnoMes
# MAGIC ORDER BY t.AnoMes;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   up.faixa_etaria AS payer_age_group,
# MAGIC   SUM(f.quantidade_transacoes) AS total_transactions,
# MAGIC   SUM(f.valor_total)           AS total_value_brl
# MAGIC FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC JOIN mvp_pix.gold.dim_tempo   t  ON f.id_tempo           = t.id_tempo
# MAGIC JOIN mvp_pix.gold.dim_usuario up ON f.id_usuario_pagador = up.id_usuario
# MAGIC WHERE t.AnoMes BETWEEN '202301' AND '202412'
# MAGIC GROUP BY up.faixa_etaria
# MAGIC ORDER BY total_transactions DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q2 — Payer vs receiver age-group interaction
# MAGIC **Question:** Are there relevant differences between payers and receivers in terms of age distribution and transaction volume?
# MAGIC
# MAGIC Primary evidence: a ranked table of payer → receiver age-group combinations (top interactions).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   up.faixa_etaria AS payer_age_group,
# MAGIC   ur.faixa_etaria AS receiver_age_group,
# MAGIC   SUM(f.quantidade_transacoes) AS total_transactions,
# MAGIC   SUM(f.valor_total)           AS total_value_brl
# MAGIC FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC JOIN mvp_pix.gold.dim_usuario up ON f.id_usuario_pagador   = up.id_usuario
# MAGIC JOIN mvp_pix.gold.dim_usuario ur ON f.id_usuario_recebedor = ur.id_usuario
# MAGIC JOIN mvp_pix.gold.dim_tempo   t  ON f.id_tempo            = t.id_tempo
# MAGIC WHERE t.AnoMes BETWEEN '202301' AND '202412'
# MAGIC GROUP BY up.faixa_etaria, ur.faixa_etaria
# MAGIC ORDER BY total_transactions DESC
# MAGIC LIMIT 25;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q3 — Purpose by age group (payer perspective)
# MAGIC **Question:** How does transaction purpose vary across different age groups?
# MAGIC
# MAGIC Primary evidence: purpose distribution by payer age group (by transaction count).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     u.faixa_etaria AS payer_age_group,
# MAGIC     p.finalidade   AS purpose,
# MAGIC     SUM(f.quantidade_transacoes) AS total_transactions
# MAGIC   FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC   JOIN mvp_pix.gold.dim_tempo      t ON f.id_tempo           = t.id_tempo
# MAGIC   JOIN mvp_pix.gold.dim_usuario    u ON f.id_usuario_pagador = u.id_usuario
# MAGIC   JOIN mvp_pix.gold.dim_finalidade p ON f.id_finalidade      = p.id_finalidade
# MAGIC   WHERE t.AnoMes = '202412'
# MAGIC   GROUP BY payer_age_group, purpose
# MAGIC ),
# MAGIC top_purposes AS (
# MAGIC   SELECT purpose
# MAGIC   FROM base
# MAGIC   GROUP BY purpose
# MAGIC   ORDER BY SUM(total_transactions) DESC
# MAGIC   LIMIT 6
# MAGIC ),
# MAGIC filtered AS (
# MAGIC   SELECT
# MAGIC     payer_age_group,
# MAGIC     CASE WHEN purpose IN (SELECT purpose FROM top_purposes) THEN purpose ELSE 'Other' END AS purpose_group,
# MAGIC     SUM(total_transactions) AS total_transactions
# MAGIC   FROM base
# MAGIC   GROUP BY payer_age_group, purpose_group
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM filtered
# MAGIC ORDER BY payer_age_group, total_transactions DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q4 — Regional differences (purpose and nature)
# MAGIC **Question:** Are there regional differences in PIX usage considering transaction purpose, nature, and volume?
# MAGIC
# MAGIC Primary evidence: payer region × nature (by total value).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     rp.regiao  AS payer_region,
# MAGIC     n.natureza AS nature,
# MAGIC     SUM(f.valor_total) AS total_value_brl
# MAGIC   FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC   JOIN mvp_pix.gold.dim_tempo    t  ON f.id_tempo          = t.id_tempo
# MAGIC   JOIN mvp_pix.gold.dim_regiao   rp ON f.id_regiao_pagador = rp.id_regiao
# MAGIC   JOIN mvp_pix.gold.dim_natureza n  ON f.id_natureza       = n.id_natureza
# MAGIC   WHERE t.AnoMes = '202412'
# MAGIC   GROUP BY payer_region, nature
# MAGIC ),
# MAGIC ranked AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     DENSE_RANK() OVER (PARTITION BY payer_region ORDER BY total_value_brl DESC) AS rnk
# MAGIC   FROM base
# MAGIC )
# MAGIC SELECT payer_region, nature, total_value_brl
# MAGIC FROM ranked
# MAGIC WHERE rnk <= 4
# MAGIC ORDER BY payer_region, total_value_brl DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q5 — Most common patterns (age × nature × purpose)
# MAGIC **Question:** Which combinations of age group, transaction nature, and transaction purpose represent the most common PIX usage patterns?
# MAGIC
# MAGIC Primary evidence: Top patterns by transaction count (payer perspective).
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   u.faixa_etaria AS payer_age_group,
# MAGIC   n.natureza     AS nature,
# MAGIC   p.finalidade   AS purpose,
# MAGIC   SUM(f.quantidade_transacoes) AS total_transactions,
# MAGIC   SUM(f.valor_total)           AS total_value_brl
# MAGIC FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC JOIN mvp_pix.gold.dim_tempo      t ON f.id_tempo           = t.id_tempo
# MAGIC JOIN mvp_pix.gold.dim_usuario    u ON f.id_usuario_pagador = u.id_usuario
# MAGIC JOIN mvp_pix.gold.dim_natureza   n ON f.id_natureza        = n.id_natureza
# MAGIC JOIN mvp_pix.gold.dim_finalidade p ON f.id_finalidade      = p.id_finalidade
# MAGIC WHERE t.AnoMes BETWEEN '202301' AND '202412'
# MAGIC GROUP BY payer_age_group, nature, purpose
# MAGIC ORDER BY total_transactions DESC
# MAGIC LIMIT 30;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q6 — Payer vs receiver profiles across regions and age groups
# MAGIC **Question:** How does the distribution of PIX usage differ between payer and receiver profiles across regions and age groups?
# MAGIC
# MAGIC Primary evidence: role-playing comparison (payer vs receiver) by region and age group.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH payer AS (
# MAGIC   SELECT
# MAGIC     rp.regiao AS region,
# MAGIC     up.faixa_etaria AS age_group,
# MAGIC     SUM(f.quantidade_transacoes) AS total_transactions
# MAGIC   FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC   JOIN mvp_pix.gold.dim_tempo   t  ON f.id_tempo           = t.id_tempo
# MAGIC   JOIN mvp_pix.gold.dim_regiao  rp ON f.id_regiao_pagador  = rp.id_regiao
# MAGIC   JOIN mvp_pix.gold.dim_usuario up ON f.id_usuario_pagador = up.id_usuario
# MAGIC   WHERE t.AnoMes = '202412'
# MAGIC   GROUP BY region, age_group
# MAGIC ),
# MAGIC receiver AS (
# MAGIC   SELECT
# MAGIC     rr.regiao AS region,
# MAGIC     ur.faixa_etaria AS age_group,
# MAGIC     SUM(f.quantidade_transacoes) AS total_transactions
# MAGIC   FROM mvp_pix.gold.fato_transacoes_pix f
# MAGIC   JOIN mvp_pix.gold.dim_tempo   t  ON f.id_tempo            = t.id_tempo
# MAGIC   JOIN mvp_pix.gold.dim_regiao  rr ON f.id_regiao_recebedor = rr.id_regiao
# MAGIC   JOIN mvp_pix.gold.dim_usuario ur ON f.id_usuario_recebedor= ur.id_usuario
# MAGIC   WHERE t.AnoMes = '202412'
# MAGIC   GROUP BY region, age_group
# MAGIC ),
# MAGIC unioned AS (
# MAGIC   SELECT region, age_group, 'Payer' AS role, total_transactions FROM payer
# MAGIC   UNION ALL
# MAGIC   SELECT region, age_group, 'Receiver' AS role, total_transactions FROM receiver
# MAGIC )
# MAGIC SELECT region, role, total_transactions
# MAGIC FROM unioned
# MAGIC WHERE age_group = '30–39'
# MAGIC ORDER BY region, role;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Q7 — Regional concentration (value vs transaction count)
# MAGIC **Question:** How concentrated is PIX usage across regions when comparing total transaction value and transaction count?
# MAGIC
# MAGIC Primary evidence uses the Gold helper view:
# MAGIC `mvp_pix.gold.vw_regional_concentration`
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   AnoMes,
# MAGIC   papel   AS role_pt,
# MAGIC   regiao  AS region,
# MAGIC   share_valor             AS value_share,
# MAGIC   share_quantidade        AS transaction_share,
# MAGIC   rank_valor              AS value_rank,
# MAGIC   rank_quantidade         AS transaction_rank,
# MAGIC   cumulative_share_valor  AS cumulative_value_share,
# MAGIC   cumulative_share_quantidade AS cumulative_transaction_share
# MAGIC FROM mvp_pix.gold.vw_regional_concentration
# MAGIC WHERE AnoMes BETWEEN '202301' AND '202412'
# MAGIC ORDER BY AnoMes, role_pt, value_rank;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC
# MAGIC This notebook focuses on evidence generation for the business questions that are answerable with the available aggregated PIX dataset (2023–2024).
# MAGIC
# MAGIC Two proposed questions are not answered here and are discussed in the final conclusion:
# MAGIC - income level of the most active users
# MAGIC - essential vs non-essential expense classification
# MAGIC