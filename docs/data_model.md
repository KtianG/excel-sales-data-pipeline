# Data Model

This project uses a **star schema** designed for operational reporting and analysis.

The model separates:

- **dimensions** (descriptive attributes)
- **facts** (events and measurements)

---

## Overview

### Dimensions

- `dim_date`
- `dim_spot`
- `dim_product`
- `dim_sale_type`
- `dim_movement_type`

### Facts

- `fact_sales_quantity`
- `fact_sales_gross`
- `fact_productcost`
- `fact_inventory_movement`

### Supporting table

- `productcost_daily_lookup`

---

## Grain (Critical Design Element)

Each fact table has a clearly defined grain:

| Table                     | Grain                                                  |
| ------------------------- | ------------------------------------------------------ |
| `fact_sales_quantity`     | date + spot_id + product_id + sale_type_id + is_heated |
| `fact_sales_gross`        | date + spot_id + product_id + is_heated                |
| `fact_productcost`        | date + product_id                                      |
| `fact_inventory_movement` | date + spot_id + product_id + movement_type_id         |

**Key decision:**  
Quantity and value are intentionally separated due to source data limitations.

---

## Dimensions

### dim_product

Represents business-level product identity.

Key fields:

- `product_id` (surrogate key)
- `product_business_key` (stable identifier)
- `product_identity_type` (`RECIPE`, `NAME_MATCH`, `TEMPORARY`)
- `is_historical_stable`
- `recipe_number`
- `product_name`
- `normalized_product_name`
- `product_type`
- `source`
- `product_alternative_names`

**Notes:**

- built using a dedicated identity resolution layer
- handles unstable source identifiers
- supports multiple names per product

---

### dim_spot

Represents sales locations.

Key fields:

- `spot_id`
- `spot_id_raw`
- `spot_name`
- `address`
- `city`

---

### dim_date

Calendar dimension built from data range.

Key fields:

- `date`
- `year`, `quarter`, `month`
- `week`, `day`
- `day_of_week`, `day_name`
- `is_weekend`
- `year_month`, `year_week`

---

### dim_sale_type

Represents types of sales.

Includes:

- fixed sale types (from JSON)
- dynamically detected discount types (e.g. `30%`)

Key fields:

- `sale_type_id`
- `sale_type_code`
- `sale_type_name`
- `sale_type_kind` (`FIXED`, `DISCOUNT`)
- `discount_pct`
- `source_column`

---

### dim_movement_type

Represents inventory movement categories.

Key fields:

- `movement_type_id`
- `movement_type_column`
- `movement_type_name`
- `movement_direction` (`IN`, `OUT`)
- `is_additional_detail`
- `parent_movement_type_column`

---

## Fact Tables

### fact_sales_quantity

Tracks product quantities sold.

Grain:

- date + spot_id + product_id + sale_type_id + is_heated

Measure:

- `quantity`

**Notes:**

- built from multiple columns (wide → long transformation)
- linked to sale types via `source_column`

---

### fact_sales_gross

Tracks revenue.

Grain:

- date + spot_id + product_id + is_heated

Measure:

- `amount`

**Important:**

- no allocation of revenue to sale types (not available in source data)

---

### fact_productcost

Tracks unit product cost over time.

Grain:

- date + product_id

Measure:

- `productcost`

Rules:

- based on `Przyjęto > 0`
- conflicts → highest value is used
- missing → handled with warnings

---

### fact_inventory_movement

Tracks inventory changes and cost.

Grain:

- date + spot_id + product_id + movement_type_id

Measures:

- `quantity`
- `cost`

Cost logic:

- receipts (`Przyjęto`) → direct cost from source
- other movements → `quantity * productcost`
- missing cost → fallback or warning

---

## Supporting Table

### productcost_daily_lookup

Expands product cost to daily level.

Rules:

- forward-fills last known cost
- does not create values before first known date

Grain:

- date + product_id

---

## Relationships

Core relationships:

- `fact_sales_quantity.product_id` → `dim_product.product_id`
- `fact_sales_gross.product_id` → `dim_product.product_id`
- `fact_inventory_movement.product_id` → `dim_product.product_id`

- `fact_sales_quantity.spot_id` → `dim_spot.spot_id`
- `fact_sales_gross.spot_id` → `dim_spot.spot_id`

- `fact_sales_quantity.sale_type_id` → `dim_sale_type.sale_type_id`

- `fact_inventory_movement.movement_type_id` → `dim_movement_type.movement_type_id`

- all facts → `dim_date.date`

---

## Key Modeling Decisions

### 1. Separation of quantity and value

Sales quantity and revenue are stored separately due to different source granularity.

---

### 2. Product identity abstraction

Instead of relying on unstable source codes:

- products are resolved into `product_business_key`
- identity is handled before building dimensions

---

### 3. Event-level attributes

`is_heated` is stored in fact tables because:

- it varies per transaction
- it is not a product property

---

### 4. Time-dependent cost

Product cost changes over time:

- stored as events (`fact_productcost`)
- expanded into daily lookup for calculations

---

### 5. JSON-driven logic

Key parts of the model depend on configuration:

- sale types
- movement types
- product source classification

---

## Summary

This data model is designed for:

- handling inconsistent operational data
- preserving historical correctness
- supporting flexible analytical use cases
- enabling reliable reporting in BI tools
