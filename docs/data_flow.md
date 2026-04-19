# Data Flow

## End-to-End Flow

```
Excel → Extract → Raw → Staging → Product Identity → Dimensions → Facts → Output
```

The pipeline transforms raw operational data into a structured analytical model ready for BI tools.

---

## Extract

**Input**

- Excel files (`.xlsx`, `.xls`)
- multiple sheets per file (per sales location)

**Logic**

- scans input directory
- selects sheets containing `#` (sales points)
- extracts metadata:
  - `report_date_raw` (from filename)
  - `spot_id_raw` (from sheet name)

- reads data without transformation

**Output**

- combined DataFrame with all files and sheets

---

## Raw

**Input**

- data from Extract

**Logic**

- stores data without any transformation

**Purpose**

- reproducibility
- debugging reference
- separation from source files

---

## Staging

**Input**

- raw data

**Logic**

- normalizes structure and data types
- parses dates and numeric values
- applies mappings from JSON:
  - `spots.json`
  - `sale_types.json`

- validates data consistency
- builds unified product fields
- generates `product_work_key`

**Output**

- cleaned operational dataset (`staging_df`)

---

## Product Identity

**Input**

- `staging_df`

**Problem solved**

- no stable product identifiers in source data

**Logic**

- assigns stable `product_business_key` using:
  - `RECIPE` (highest reliability)
  - `NAME_MATCH`
  - `TEMPORARY` fallback

- ensures consistency across time

**Output**

- `product_identity_df`

---

## Dimensions

### dim_date

- generated from min/max dates
- full calendar attributes

### dim_spot

- maps `spot_id_raw` → `spot_id`
- enriches location data

### dim_sale_type

- fixed types from config
- dynamic discount types (`%` columns)

### dim_movement_type

- defines inventory movements
- includes direction (`IN` / `OUT`)

### dim_product

- aggregates product-level data
- links to `product_business_key`
- resolves identity conflicts

---

## Facts

### fact_sales_quantity

- grain: date + spot + product + sale_type + is_heated
- quantity split by sale type

### fact_sales_gross

- grain: date + spot + product + is_heated
- total revenue (no sale type split)

### fact_productcost

- grain: date + product
- unit cost from inventory receipts
- conflicts resolved by selecting highest value

### productcost_daily_lookup

- expands productcost to daily level
- forward-fills missing days

### fact_inventory_movement

- grain: date + spot + product + movement_type
- quantity from staging
- cost:
  - receipts → direct source value
  - other movements → quantity × productcost

- missing cost → fallback to 0 + warning

---

## Final Output

**Location**

- `data/output/`

**Structure**

- one CSV per table:
  - dimensions
  - facts

**Usage**

- ready for Power BI / analytical tools
- no additional cleaning required
