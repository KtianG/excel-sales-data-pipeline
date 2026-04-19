# Table Grains

This document defines the **grain (level of detail)** for each fact table.

Grain determines:

- how data is aggregated
- how tables can be joined
- what analyses are valid

---

## General Rules

- all fact tables are aggregated to their defined grain
- no duplicate rows exist for grain-defining keys
- values are deterministic (same input → same output)

---

## fact_sales_quantity

**Grain**

- date
- spot_id
- product_id
- sale_type_id
- is_heated

**Description**
Represents total quantity sold for a specific combination of:

- day
- location
- product
- sale type
- serving state (heated / non-heated)

**Notes**

- quantity is aggregated from source data
- multiple sale types can exist for the same product and day
- enables detailed sales breakdown (e.g. discounts vs full price)

---

## fact_sales_gross

**Grain**

- date
- spot_id
- product_id
- is_heated

**Description**
Represents total gross revenue for a specific combination of:

- day
- location
- product
- serving state

**Notes**

- no `sale_type_id` by design
- source data does not allow reliable revenue split by sale type
- should not be joined directly with `fact_sales_quantity` on sale_type level

---

## fact_productcost

**Grain**

- date
- product_id

**Description**
Represents unit product cost observed on a specific day.

**Notes**

- based only on inventory receipts (`Przyjęto > 0`)
- not all dates have values
- conflicts resolved by selecting highest value (with warning)
- full daily continuity is handled separately via `productcost_daily_lookup`

---

## fact_inventory_movement

**Grain**

- date
- spot_id
- product_id
- movement_type_id

**Description**
Represents aggregated inventory movement for a specific combination of:

- day
- location
- product
- movement type

**Notes**

- quantity is always absolute (no negative values)
- movement direction (`IN` / `OUT`) is defined by movement type
- cost calculation:
  - receipts → direct source value
  - other movements → quantity × productcost

- missing productcost:
  - cost = 0
  - warning generated

---

## Key Design Implications

- each fact table has a **different grain → they cannot always be joined directly**
- joining facts incorrectly can lead to:
  - duplicated rows
  - inflated metrics

- correct analysis requires:
  - understanding grain
  - joining through dimensions (not directly fact-to-fact)
