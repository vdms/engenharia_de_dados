# Databricks notebook source
# MAGIC %md
# MAGIC Client: Federal Government of Brazil / Central Bank of Brazil  
# MAGIC Topic: PIX user behavior  
# MAGIC Analyzed period: 2023 and 2024  
# MAGIC Platform: Databricks Free Edition  
# MAGIC Storage: Unity Catalog + Volumes
# MAGIC ---
# MAGIC # Project Context
# MAGIC
# MAGIC This work is part of the MVP for the Data Engineering module and aims to build a cloud-based data pipeline, from data collection to analysis, using real and public data.
# MAGIC
# MAGIC The project was developed in a governmental context, considering the Central Bank as the main stakeholder. The focus is on analyzing the behavior of users of the PIX instant payment system, seeking to understand usage patterns over time, differences across user profiles, and regional variations.
# MAGIC
# MAGIC Although the project applies Data Engineering concepts and tools, it prioritizes clarity, organization, and end-to-end process understanding, focusing on building a functional and well-documented pipeline rather than overly complex solutions.
# MAGIC
# MAGIC ---
# MAGIC # Dataset Used
# MAGIC
# MAGIC The dataset used in this MVP comes from the Central Bank of Brazil through the PIX – Open Data initiative, made available via a public API.
# MAGIC
# MAGIC The data represents aggregated monthly statistics of PIX transactions, including information on transaction volume, total value, and general characteristics of the users involved, such as age group, region, transaction nature, and purpose.
# MAGIC
# MAGIC For this work, data from the years 2023 and 2024 was used, stored in two separate CSV files:
# MAGIC
# MAGIC * pix_2023.csv  
# MAGIC * pix_2024.csv  
# MAGIC
# MAGIC The files were downloaded locally from the official API and later uploaded to the Databricks environment, where they became part of the raw data layer of the pipeline.
# MAGIC
# MAGIC Main columns used in the analysis:
# MAGIC
# MAGIC * AnoMes: transaction period (year and month)  
# MAGIC * PAG_IDADE: payer age group  
# MAGIC * REC_IDADE: receiver age group  
# MAGIC * PAG_REGIAO: payer region  
# MAGIC * REC_REGIAO: receiver region  
# MAGIC * NATUREZA: transaction nature  
# MAGIC * FINALIDADE: transaction purpose  
# MAGIC * QUANTIDADE: number of transactions  
# MAGIC * VALOR: total transacted value  
# MAGIC
# MAGIC In addition, some extra columns were kept in the raw data only for documentation and contextual purposes, but were not directly used in the final analyses.
# MAGIC
# MAGIC ---
# MAGIC # Data Pipeline Architecture
# MAGIC
# MAGIC To organize the pipeline, a layered architecture was adopted, widely used in data projects for facilitating separation of responsibilities and transformation traceability.
# MAGIC
# MAGIC ## Bronze Layer (raw data)
# MAGIC
# MAGIC The Bronze layer contains the data exactly as obtained, without any transformation or analytical treatment.
# MAGIC
# MAGIC In this layer:
# MAGIC
# MAGIC * the original CSV files are stored in Databricks;  
# MAGIC * the goal is to preserve the original data source;  
# MAGIC * potential issues or inconsistencies are not yet addressed.
# MAGIC
# MAGIC This layer serves as a reliable reference point for auditing and reprocessing, if needed.
# MAGIC
# MAGIC ## Silver Layer (processed data)
# MAGIC
# MAGIC The Silver layer contains cleaned and standardized data, ready for analytical modeling.
# MAGIC
# MAGIC At this stage:
# MAGIC
# MAGIC * data from 2023 and 2024 is unified;  
# MAGIC * data types are adjusted (e.g., numeric values and dates);  
# MAGIC * relevant columns are organized and standardized;  
# MAGIC * the data gains a consistent structure.
# MAGIC
# MAGIC The goal of the Silver layer is to prepare the data for analysis without yet applying specific business rules.
# MAGIC
# MAGIC ## Gold Layer (analytical model)
# MAGIC
# MAGIC The Gold layer represents the final analytical view of the data, structured to facilitate queries and analysis.
# MAGIC
# MAGIC In this layer:
# MAGIC
# MAGIC * data is organized into a star schema;  
# MAGIC * dimension tables are created (time, user, region, nature, purpose);  
# MAGIC * a fact table is created to concentrate the main transaction and value metrics.
# MAGIC
# MAGIC This structure enables clear and efficient answers to the business questions defined at the beginning of the project.
# MAGIC
# MAGIC ---
# MAGIC # Databricks Organization
# MAGIC
# MAGIC The project uses Unity Catalog for data organization, with the following structure:
# MAGIC
# MAGIC * Catalog: mvp_pix  
# MAGIC * Schema: dados  
# MAGIC * Volume (Bronze): /Volumes/mvp_pix/dados/bronze/
# MAGIC
# MAGIC This organization facilitates pipeline visualization, project documentation, and the generation of evidence for evaluation.
# MAGIC
# MAGIC ---
# MAGIC # Final Note
# MAGIC
# MAGIC This MVP does not aim to exhaust all analytical possibilities of the dataset, but rather to demonstrate, in a structured and functional way, the construction of a complete data pipeline—from data collection to analysis—with justified technical decisions and clear documentation.