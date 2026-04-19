# Architecture

This project implements a layered data pipeline designed to transform messy operational Excel data into a structured analytical model (star schema).

The architecture prioritizes:

- data traceability
- robustness to inconsistent input data
- clear separation of responsibilities between pipeline stages

Pipeline flow:

EXTRACT → RAW → STAGING → DATA MODEL → EXPORT

---

## Layers

### 1. Extract

Responsible for reading source files and extracting technical metadata.

- reads Excel files (`.xlsx`, `.xls`)
- processes multiple sheets per file
- identifies sales sheets using `#spot_id`
- extracts:
  - `report_date_raw` (from filename)
  - `spot_id_raw` (from sheet name)
- attaches technical metadata:
  - `source_file`
  - `source_sheet`
  - `source_sheet_clean`
  - `load_timestamp`

**Key principle:** no business logic, only raw data ingestion.

---

### 2. Raw

Stores extracted data without transformations.

- direct output from Extract layer
- no cleaning or transformations

**Purpose:**

- reproducibility
- debugging reference
- isolation from source files

---

### 3. Staging

Prepares data for the analytical model.

- normalizes column structure and naming
- parses dates and numeric values
- standardizes data types
- applies mappings from JSON configuration
- builds product-related fields:
  - `product_work_key`
  - `product_name_selected`
  - `recipe_number_clean`
- assigns `product_group_final`
- validates data consistency
- adds data quality flags

**Key principle:** staging handles transformation, but not final modeling logic.

---

### 4. Product Identity

Dedicated layer for resolving inconsistent product identifiers.

Because source data does not provide stable product IDs, identity is built using:

- `RECIPE` → based on recipe number (highest priority)
- `NAME_MATCH` → exact normalized name match
- `TEMPORARY` → fallback for unresolved cases

**Outputs:**

- `product_business_key`
- `product_identity_type`
- `is_historical_stable`

**Purpose:**

- ensure historical consistency
- separate identity resolution from dimension modeling

---

### 5. Data Model (Star Schema)

Final analytical model used for reporting.

#### Dimensions

- `dim_date`
- `dim_spot`
- `dim_product`
- `dim_sale_type`
- `dim_movement_type`

#### Facts

- `fact_sales_quantity`
- `fact_sales_gross`
- `fact_productcost`
- `fact_inventory_movement`

#### Supporting layer

- `productcost_daily_lookup` (time-based cost propagation)

---

## Key Design Decisions

- **Separation of quantity and value**
  - `fact_sales_quantity` and `fact_sales_gross` have different grains

- **No artificial allocation of revenue**
  - gross values are not split into sale types if not present in source data

- **Product identity handled separately**
  - avoids mixing business logic with dimension building

- **Do not rely on unstable source identifiers**
  - product codes and names may change over time

- **`is_heated` as a fact attribute**
  - treated as an event-level property, not a product dimension

- **Product cost as a time-dependent value**
  - requires daily lookup and forward-fill logic

---

## Data Flow

1. **Extract**
   - Excel → DataFrame (raw + metadata)

2. **Raw**
   - DataFrame → stored without changes

3. **Staging**
   - cleaned and standardized operational data

4. **Product Identity**
   - mapping `product_work_key` → `product_business_key`

5. **Dimensions**
   - built from staging + identity

6. **Facts**
   - built from staging + dimensions

7. **Product Cost Lookup**
   - expands cost values across time

8. **Inventory Movement**
   - calculates movement quantities and costs

9. **Export**
   - all tables saved as CSV for BI tools

---

## Configuration (JSON-driven logic)

Business logic is externalized into JSON files:

- `sale_types.json`
- `movement_types.json`
- `product_source_rules.json`
- `spots.json`

**Advantages:**

- no hardcoded logic in Python
- easier maintenance and updates
- version-controlled business rules

---

## Error Handling

Custom exception types:

- `SourceDataError` → source file or structure issues
- `DataValidationError` → invalid or inconsistent data
- `ConfigurationError` → invalid JSON configuration

**Approach:**

- critical errors → stop pipeline
- recoverable issues → logged as warnings

---

## Assumptions and Constraints

- report date is extracted from filename (`dd.mm.yy`)
- sheets containing `#` represent valid sales data
- source structure may change over time
- input data is inherently inconsistent
- some data quality issues are expected and handled via warnings

---

## Summary

This architecture focuses on:

- handling imperfect real-world data
- maintaining full traceability from source to output
- separating data ingestion, transformation, and modeling
- building a reliable analytical foundation for reporting
