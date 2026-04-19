# Mapping Rules

This document describes how raw operational data is transformed into structured analytical entities.

The focus is on **handling inconsistent data and defining deterministic mapping logic**.

---

## Sale Type Mapping

**Sources**

- percentage-based columns (e.g. `30%`, `50%`)
- fixed columns (e.g. standard sales, Too Good To Go)

**Strategy**

- percentage columns → dynamically generate sale types
- fixed columns → mapped via configuration (`sale_types.json`)

**Key rules**

- each sale type has a stable `sale_type_code`
- multiple sale types can exist for a single transaction
- final breakdown happens in `fact_sales_quantity`

---

## Movement Type Mapping

**Source**

- configuration (`movement_types.json`)

**Strategy**

- each movement column is mapped to a `movement_type_id`
- movement direction is explicitly defined:
  - `IN` (e.g. receipts)
  - `OUT` (e.g. sales, returns)

**Key rules**

- quantity is always stored as absolute value
- direction is derived from movement type (not from sign)
- hierarchical relationships supported (main vs detailed movements)

---

## Product Identity (Core Problem)

**Problem**

- source data does not contain stable product identifiers
- product names and codes are inconsistent over time

**Solution**
A dedicated identity layer assigns a stable `product_business_key`.

**Identification strategy (priority order)**

1. **RECIPE**
   - based on `recipe_number`
   - considered fully stable over time

2. **NAME_MATCH**
   - exact match on normalized name
   - requires consistent product grouping

3. **TEMPORARY**
   - fallback when no match is possible
   - generates unique technical identifier

**Key rules**

- processing is chronological (first occurrence defines identity)
- one product can have multiple source names
- conflicting product groups:
  - prevent merging
  - generate warnings

---

## Product Attribute Resolution

**Sources**

- staging data
- configuration (`product_source_rules.json`)

**Strategy**

- attributes are derived from aggregated observations
- conflicts are resolved deterministically

**Examples**

- product name → representative value from available variants
- product type → most consistent classification
- source → rule-based assignment using JSON

**Rule engine (priority-based)**

- first matching rule is applied
- fallback → default value

---

## Product Cost (productcost)

**Source**

- inventory receipt data (`Przyjęto` + unit cost)

**Strategy**

- only rows with `Przyjęto > 0` are used
- for each (date, product):
  - multiple values → highest value selected
  - warning is generated

**Missing data**

- missing cost → default to `0`
- warning is generated

**Time handling**

- cost is stored as point-in-time values (`fact_productcost`)
- daily continuity is handled via forward-fill (`productcost_daily_lookup`)

---

## General Design Principles

- business logic is externalized to JSON (config-driven pipeline)
- data issues are **exposed, not hidden**
- deterministic transformations (same input → same output)
- no reliance on unstable source identifiers
- separation of concerns:
  - staging = data cleaning
  - identity = entity resolution
  - dimensions/facts = analytical model
