# Star Schema — Analytical Model (Gold Layer)

This document describes the analytical data model implemented in the Gold layer of the MVP. The model follows a **Star Schema** design to support fast and clear analytical queries.

## Scope and Grain

**Analytical scope:** January 2023 to December 2024 (enforced in the Silver layer).  
**Fact table grain (one row represents):**  
A monthly aggregated PIX observation for a unique combination of:
- time (YearMonth)
- payer age group
- receiver age group
- payer region
- receiver region
- transaction nature
- transaction purpose
- initiation method

This grain matches the dataset’s aggregated structure and avoids creating artificial detail.

---

## Tables Overview

### Fact Table
**`mvp_pix.gold.fato_transacoes_pix`**

**Measures**
- `quantidade_transacoes` (Number of transactions)
- `valor_total` (Total transacted value)

**Foreign Keys**
- `id_tempo`
- `id_usuario_pagador`
- `id_usuario_recebedor`
- `id_regiao_pagador`
- `id_regiao_recebedor`
- `id_natureza`
- `id_finalidade`
- `id_forma_iniciacao`

---

### Dimensions

#### `mvp_pix.gold.dim_tempo`
Time dimension at monthly granularity.

Columns:
- `id_tempo` (deterministic key based on `AnoMes`)
- `AnoMes` (YearMonth, e.g., 202401)
- `ano` (year)
- `mes` (month)
- `trimestre` (quarter)

Notes:
- `id_tempo` was generated using a deterministic hash strategy to ensure stable IDs across runs.

---

#### `mvp_pix.gold.dim_usuario`
User dimension represented by **standardized age group**.

Columns:
- `id_usuario`
- `faixa_etaria` (standardized categories:
  `<20`, `20–29`, `30–39`, `40–49`, `50–59`, `60+`, `Not informed`)

Role-playing dimension:
- Used twice in the fact table:
  - payer (`id_usuario_pagador`)
  - receiver (`id_usuario_recebedor`)

---

#### `mvp_pix.gold.dim_regiao`
Region dimension with role distinction (payer vs receiver).

Columns:
- `id_regiao`
- `regiao` (e.g., NORTE, SUDESTE, SUL, NORDESTE, CENTRO-OESTE)
- `papel` (Pagador | Recebedor)

Role-playing dimension:
- Used twice in the fact table:
  - payer (`id_regiao_pagador`)
  - receiver (`id_regiao_recebedor`)

---

#### `mvp_pix.gold.dim_natureza`
Transaction nature.

Columns:
- `id_natureza`
- `natureza`

---

#### `mvp_pix.gold.dim_finalidade`
Transaction purpose.

Columns:
- `id_finalidade`
- `finalidade`

---

#### `mvp_pix.gold.dim_forma_iniciacao`
Transaction initiation method.

Columns:
- `id_forma_iniciacao`
- `forma_iniciacao`

Notes:
- Included as a low-cardinality analytical dimension.
- Allows segmentation of PIX usage by initiation channel without affecting fact grain.

---

## Join Map (How to Query)

Typical query pattern:

- Join fact → time using `id_tempo`
- Join fact → users using payer/receiver keys
- Join fact → regions using payer/receiver keys
- Join fact → nature and purpose using their IDs

Example:
- "Monthly evolution": fact + dim_tempo
- "Regional flows": fact + dim_regiao (payer + receiver)
- "Age behavior": fact + dim_usuario (payer + receiver)

---

## Why This Model Works for the MVP

- Reflects the dataset’s aggregated nature (monthly stats)
- Enables clear analytical questions without complex transformations
- Supports role-playing dimensions (payer vs receiver) cleanly
- Keeps the Gold layer focused on analysis (not cleaning)
