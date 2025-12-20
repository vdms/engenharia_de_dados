# Databricks notebook source
# MAGIC %md
# MAGIC # Conclusion
# MAGIC
# MAGIC This MVP successfully delivered a **complete end-to-end data engineering pipeline** in Databricks (Bronze → Silver → Gold) using public, aggregated PIX data from the Central Bank of Brazil.
# MAGIC
# MAGIC The project covered the full lifecycle of a data pipeline, including raw data ingestion, standardization and scope enforcement in the Silver layer, analytical modeling through a star schema in the Gold layer, and reproducible data quality validation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Business Questions Coverage
# MAGIC
# MAGIC Out of the **9 proposed business questions**, **7 were successfully addressed** using the available dataset for the 2023–2024 period.
# MAGIC
# MAGIC These questions focused on:
# MAGIC - temporal evolution of PIX usage,
# MAGIC - differences between payer and receiver profiles,
# MAGIC - behavioral patterns by age group,
# MAGIC - regional segmentation,
# MAGIC - transaction nature and purpose,
# MAGIC - and regional concentration of PIX activity.
# MAGIC
# MAGIC All answers were supported by **query-based analytical evidence** generated from the Gold layer and documented in a dedicated analysis notebook.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Limitations of the Dataset
# MAGIC
# MAGIC Two business questions could not be answered within the scope of this MVP:
# MAGIC
# MAGIC - **Income level of PIX users**  
# MAGIC   The dataset does not include income information or reliable proxies. Addressing this question would require enrichment with external socioeconomic data sources (e.g., IBGE datasets).
# MAGIC
# MAGIC - **Essential vs non-essential expenses**  
# MAGIC   Although transaction nature and purpose are available, the dataset does not provide an official or standardized classification distinguishing essential from non-essential expenses. Answering this question would require an explicit semantic classification layer, documented as an analytical assumption.
# MAGIC
# MAGIC These limitations reflect **constraints of the source data**, not shortcomings of the pipeline design or implementation.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Technical Learnings
# MAGIC
# MAGIC A key technical takeaway from this MVP was the importance of **clear contracts between pipeline layers**, particularly:
# MAGIC
# MAGIC - enforcing analytical scope and data types in the Silver layer,
# MAGIC - modeling facts and dimensions with explicit grain in the Gold layer,
# MAGIC - and validating assumptions through deterministic data quality checks.
# MAGIC
# MAGIC This approach reduced ambiguity, simplified analysis, and increased confidence in the analytical outputs.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Possible Extensions
# MAGIC
# MAGIC Future iterations of this project could explore:
# MAGIC - automated data quality monitoring,
# MAGIC - richer metadata and lineage documentation,
# MAGIC - enrichment with complementary public datasets to expand analytical scope.
# MAGIC
# MAGIC These extensions could be introduced incrementally without changing the core pipeline structure established in this MVP.
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## Final Note
# MAGIC
# MAGIC This MVP prioritizes **correctness, transparency, and reproducibility** over complexity.
# MAGIC
# MAGIC All insights are derived from **aggregated monthly data**, not transaction-level records, and should be interpreted accordingly. The project demonstrates how a well-structured data engineering pipeline can support analytical exploration while making its limitations explicit.
# MAGIC