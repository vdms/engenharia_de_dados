# Data Catalog — PIX MVP

This document provides a practical data catalog for the MVP, covering the dataset used, key fields, domains, types, and basic quality validations.

## Dataset Summary

**Source:** Central Bank of Brazil — PIX Open Data (Olinda API)  
**Files ingested (raw):**
- `pix_2023.csv`
- `pix_2024.csv`

**Analytical scope used in this MVP:** 2023-01 to 2024-12  
Note: Bronze preserves the original files as received; Silver enforces the analytical scope.

---

## Layered Data Catalog

### Bronze (Raw)
Tables:
- `mvp_pix.bronze.pix_2023_raw`
- `mvp_pix.bronze.pix_2024_raw`

Definition:
- Raw ingestion, no transformations.
- Columns preserved as-is from the CSV/API.

---

### Silver (Standardized)
Table:
- `mvp_pix.silver.pix_transacoes`

Key transformations:
1) **Scope filter**
- Keep only `AnoMes` between `202301` and `202412`.

2) **Type standardization**
- `AnoMes` → string
- `QUANTIDADE` → long/integer
- `VALOR` → decimal(18,2)

3) **Age group standardization**
- `PAG_FAIXA_ETARIA`, `REC_FAIXA_ETARIA` created from original columns.
- Unknown/administrative values mapped to `Desconhecida`.

---

### Gold (Analytical)
Tables:
- `mvp_pix.gold.fato_transacoes_pix`
- `mvp_pix.gold.dim_tempo`
- `mvp_pix.gold.dim_usuario`
- `mvp_pix.gold.dim_regiao`
- `mvp_pix.gold.dim_natureza`
- `mvp_pix.gold.dim_finalidade`

Definition:
- Star schema for analysis (see `docs/modelo_estrela.md`).

---

## Column Catalog (Core Analytical Fields)

### Time
**`AnoMes`**
- Meaning: YearMonth (YYYYMM)
- Type (Silver): string
- Domain: `202301` … `202412` (analytical scope)
- Example: `202401`
- Quality checks:
  - Not null
  - Matches regex: `^[0-9]{6}$`
  - Min/max within scope

---

### Payer / Receiver age
**`PAG_IDADE`, `REC_IDADE`**
- Meaning: Original age range categories from the source
- Type: string
- Examples:
  - `até 19 anos`, `entre 20 e 29 anos`, `mais de 60 anos`
  - `Não informado`, `Não se aplica`
- Notes:
  - Preserved in Silver for traceability

**`PAG_FAIXA_ETARIA`, `REC_FAIXA_ETARIA`**
- Meaning: Standardized age groups for analysis
- Type: string
- Domain:
  - `Até 19`, `20–29`, `30–39`, `40–49`, `50–59`, `60+`, `Desconhecida`
- Quality checks:
  - Not null
  - Values must be in the allowed domain list

---

### Region
**`PAG_REGIAO`, `REC_REGIAO`**
- Meaning: Region of payer / receiver
- Type: string
- Typical domain:
  - `NORTE`, `NORDESTE`, `SUDESTE`, `SUL`, `CENTRO-OESTE`
  - plus possible administrative values (e.g., `Nao informado`)
- Standardization (Silver):
  - Trim + uppercase recommended
- Quality checks:
  - Not null (or quantify nulls if any)
  - Distinct values reviewed and documented

---

### Transaction attributes
**`NATUREZA`**
- Meaning: Transaction nature category
- Type: string
- Quality checks:
  - Not null
  - Distinct values reviewed

**`FINALIDADE`**
- Meaning: Transaction purpose category
- Type: string
- Quality checks:
  - Not null
  - Distinct values reviewed

---

### Measures
**`QUANTIDADE`**
- Meaning: Number of transactions
- Type (Silver): long/integer
- Constraints:
  - Should be >= 0
- Quality checks:
  - Not null
  - No negative values

**`VALOR`**
- Meaning: Total transacted value
- Type (Silver): decimal(18,2)
- Constraints:
  - Should be >= 0
- Quality checks:
  - Not null
  - No negative values
  - Conversion produces 0 nulls after cast

---

## Unused Columns Kept for Context (Raw only)

The following fields were kept in Bronze (and optionally Silver) for documentation, but not used in the final analytical questions:

- `FORMA_INICIACAO` — initiation method (context only)
- `PAG_PFPJ` — payer type (PF/PJ)
- `REC_PFPJ` — receiver type (PF/PJ)

Rationale:
- They may support future segmentation, but were not necessary for the current MVP questions.