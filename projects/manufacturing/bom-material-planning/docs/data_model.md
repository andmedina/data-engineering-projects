# BOM Material Planning Data Model

## Purpose

This document defines the implemented PostgreSQL relational model. The model
supports deterministic, single-level BOM material planning: finished-product
demand is converted into time-phased raw-material requirements, then netted
against inventory and timely purchase-order receipts.

## Modeling Principles

- Every table has one explicit grain.
- Master data is separated from planning transactions.
- Quantities use the material's documented base unit of measure.
- Dates remain visible so the planning engine can determine whether supply is
  available before demand is due.
- Supplier purchasing rules are stored by supplier-material combination rather
  than assumed globally.
- Purchase recommendations are calculated output, not source-system facts.

## Relationship Overview

```text
products ──< bills_of_materials ──< bom_components >── materials
    │                                                   │
    └──< production_demand                              ├──< inventory_balances
                                                        ├──< supplier_materials >── suppliers
                                                        └──< purchase_orders >────── suppliers
```

## 1. Products

**Table:** `products`

**Grain:** One finished product that can appear in the production plan.

| Column | Type | Rules and purpose |
|---|---|---|
| `product_id` | BIGINT | Primary key, generated identity |
| `product_code` | VARCHAR(30) | Unique business identifier |
| `product_name` | VARCHAR(120) | Descriptive product name |
| `product_family` | VARCHAR(60) | Reporting and demand grouping |
| `base_unit_of_measure` | VARCHAR(10) | Finished-product planning unit; initially `EA` |
| `active_flag` | BOOLEAN | Whether new demand may reference the product |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- `product_code` must be unique.
- Product code and name cannot be blank.
- The first version supports finished products measured in eaches (`EA`).

## 2. Materials

**Table:** `materials`

**Grain:** One purchased raw material in its base inventory unit.

| Column | Type | Rules and purpose |
|---|---|---|
| `material_id` | BIGINT | Primary key, generated identity |
| `material_code` | VARCHAR(30) | Unique business identifier |
| `material_name` | VARCHAR(120) | Descriptive material name |
| `material_category` | VARCHAR(60) | Examples: wire, coating, packaging |
| `base_unit_of_measure` | VARCHAR(10) | Unit used by BOM, inventory, and purchasing quantities |
| `standard_unit_cost` | NUMERIC(14,4) | Reference cost per base unit |
| `active_flag` | BOOLEAN | Whether the material can be planned |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- `material_code` must be unique.
- Supported units are explicitly controlled, initially `KG`, `L`, and `EA`.
- Standard unit cost cannot be negative.

## 3. Suppliers

**Table:** `suppliers`

**Grain:** One external supplier organization.

| Column | Type | Rules and purpose |
|---|---|---|
| `supplier_id` | BIGINT | Primary key, generated identity |
| `supplier_code` | VARCHAR(20) | Unique business identifier |
| `supplier_name` | VARCHAR(120) | Supplier name |
| `supplier_status` | VARCHAR(20) | `Approved`, `Conditional`, or `Inactive` |
| `quality_rating` | NUMERIC(5,2) | Reference rating from 0 through 100 |
| `on_time_delivery_pct` | NUMERIC(5,2) | Reference delivery performance from 0 through 100 |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- `supplier_code` must be unique.
- Ratings must remain between 0 and 100.
- Only approved or conditional supplier-material sources are eligible for
  recommendations.

## 4. Bills of Materials

**Table:** `bills_of_materials`

**Grain:** One BOM revision for one finished product.

| Column | Type | Rules and purpose |
|---|---|---|
| `bom_id` | BIGINT | Primary key, generated identity |
| `product_id` | BIGINT | Foreign key to `products` |
| `revision_code` | VARCHAR(20) | Product-specific revision identifier |
| `effective_start_date` | DATE | First date the revision may be used |
| `effective_end_date` | DATE | Optional last effective date |
| `bom_status` | VARCHAR(20) | `Draft`, `Active`, or `Obsolete` |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- A product and revision-code combination must be unique.
- End date, when present, cannot precede the start date.
- The seeded dataset contains one active BOM per active product.
- Preventing overlapping effective revisions requires validation beyond a
  basic row-level check. The seeded data avoids overlaps, but the current
  schema does not enforce that rule across multiple rows.

## 5. BOM Components

**Table:** `bom_components`

**Grain:** One raw-material requirement line within one BOM revision.

| Column | Type | Rules and purpose |
|---|---|---|
| `bom_component_id` | BIGINT | Primary key, generated identity |
| `bom_id` | BIGINT | Foreign key to `bills_of_materials` |
| `line_number` | INTEGER | Display and processing order within the BOM |
| `material_id` | BIGINT | Foreign key to `materials` |
| `quantity_per_unit` | NUMERIC(16,6) | Base material quantity per one finished product |
| `expected_loss_pct` | NUMERIC(6,3) | Planned process-loss allowance from 0 through less than 100 |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- Line number must be positive and unique within a BOM.
- The same material cannot appear twice in one BOM revision.
- Quantity per unit must be greater than zero.
- Expected loss must be at least zero and less than 100%.

### Gross-requirement formula

The loss percentage represents expected input loss. Required input is grossed
up so enough usable material remains:

```text
gross requirement =
    demand quantity × quantity per unit / (1 - expected loss percentage)
```

For example, a 100 kg usable requirement with 2% expected loss requires
approximately 102.041 kg of material input, not simply 102 kg.

## 6. Supplier Materials

**Table:** `supplier_materials`

**Grain:** One approved sourcing option for one material from one supplier.

| Column | Type | Rules and purpose |
|---|---|---|
| `supplier_material_id` | BIGINT | Primary key, generated identity |
| `supplier_id` | BIGINT | Foreign key to `suppliers` |
| `material_id` | BIGINT | Foreign key to `materials` |
| `supplier_material_code` | VARCHAR(40) | Supplier's item identifier |
| `unit_price` | NUMERIC(14,4) | Price per material base unit |
| `lead_time_days` | INTEGER | Calendar days from order placement to expected receipt |
| `minimum_order_quantity` | NUMERIC(16,3) | Smallest allowed order quantity |
| `order_multiple` | NUMERIC(16,3) | Quantity increment above the minimum |
| `preferred_flag` | BOOLEAN | Preferred eligible source for the material |
| `source_status` | VARCHAR(20) | `Approved`, `Conditional`, or `Inactive` |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- Supplier and material combination must be unique.
- Price and minimum order quantity must be nonnegative.
- Lead time cannot be negative.
- Order multiple must be greater than zero.
- The seed data provides exactly one preferred approved source per active
  material; the schema permits alternatives for later sourcing analysis.

## 7. Production Demand

**Table:** `production_demand`

**Grain:** One planned requirement for one finished product on one need date.

| Column | Type | Rules and purpose |
|---|---|---|
| `demand_id` | BIGINT | Primary key, generated identity |
| `demand_reference` | VARCHAR(40) | Unique planning or production-plan identifier |
| `product_id` | BIGINT | Foreign key to `products` |
| `required_date` | DATE | Date the finished quantity is required |
| `demand_quantity` | NUMERIC(16,3) | Required finished-product quantity |
| `demand_status` | VARCHAR(20) | `Planned`, `Released`, `Completed`, or `Cancelled` |
| `priority` | VARCHAR(15) | `Standard`, `High`, or `Critical` |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- Demand reference must be unique.
- Demand quantity must be greater than zero.
- Cancelled and completed demand are excluded from open material planning.

## 8. Inventory Balances

**Table:** `inventory_balances`

**Grain:** One material balance at one inventory location.

| Column | Type | Rules and purpose |
|---|---|---|
| `inventory_balance_id` | BIGINT | Primary key, generated identity |
| `material_id` | BIGINT | Foreign key to `materials` |
| `location_code` | VARCHAR(20) | Warehouse or storage-location identifier |
| `on_hand_quantity` | NUMERIC(16,3) | Physically recorded inventory |
| `reserved_quantity` | NUMERIC(16,3) | Quantity committed to other requirements |
| `restricted_quantity` | NUMERIC(16,3) | Quality-hold or otherwise unusable quantity |
| `safety_stock_quantity` | NUMERIC(16,3) | Protected minimum balance |
| `last_counted_at` | TIMESTAMPTZ | Inventory observation timestamp |

### Constraints

- Material and location combination must be unique.
- All quantities must be nonnegative.
- Reserved plus restricted inventory cannot exceed on-hand inventory.

### Availability formula

```text
usable on hand =
    max(on hand - reserved - restricted - safety stock, 0)
```

Balances from all eligible locations are aggregated by material for the first
version. Location transfer costs and timing are outside scope.

## 9. Purchase Orders

**Table:** `purchase_orders`

**Grain:** One purchase-order line for one supplier and one material.

| Column | Type | Rules and purpose |
|---|---|---|
| `purchase_order_id` | BIGINT | Primary key, generated identity |
| `purchase_order_number` | VARCHAR(30) | Business purchase-order identifier |
| `line_number` | INTEGER | Line number within the purchase order |
| `supplier_id` | BIGINT | Foreign key to `suppliers` |
| `material_id` | BIGINT | Foreign key to `materials` |
| `order_date` | DATE | Date the line was placed |
| `expected_receipt_date` | DATE | Current expected delivery date |
| `ordered_quantity` | NUMERIC(16,3) | Original line quantity |
| `received_quantity` | NUMERIC(16,3) | Quantity already received |
| `unit_price` | NUMERIC(14,4) | Agreed line price per base unit |
| `purchase_order_status` | VARCHAR(20) | `Open`, `Partially Received`, `Received`, or `Cancelled` |
| `created_at` | TIMESTAMPTZ | Audit timestamp |

### Constraints

- Purchase-order number and line number combination must be unique.
- Line number and ordered quantity must be positive.
- Received quantity must be between zero and ordered quantity.
- Expected receipt date cannot precede the order date.
- Unit price cannot be negative.

### Scheduled-receipt formula

```text
open receipt quantity = ordered quantity - received quantity
```

Only open or partially received lines with an expected receipt date on or
before the requirement date can satisfy that requirement.

## Calculated Planning Output

The planning engine calculates a purchase-recommendation dataset with one row
per material shortage and need date. It is not stored as a source table.

Output fields include:

| Field | Purpose |
|---|---|
| `material_code` | Material requiring supply |
| `need_date` | Date the material is required |
| `gross_requirement` | BOM-derived requirement including expected loss |
| `available_inventory` | Usable on-hand supply applied to the requirement |
| `scheduled_receipts` | Timely open purchase-order supply applied |
| `net_requirement` | Remaining uncovered requirement |
| `recommended_supplier` | Preferred eligible sourcing option |
| `recommended_order_quantity` | Quantity after MOQ and order-multiple rules |
| `recommended_order_date` | Need date minus lead time |
| `urgency_status` | `Future`, `Due Today`, or `Past Due` |
| `estimated_purchase_cost` | Recommendation quantity multiplied by unit price |

## Time-Phased Netting Requirement

Material requirements must be processed in need-date order. Inventory or a
scheduled receipt used for an earlier requirement cannot be reused for a later
requirement. The planning engine therefore maintains a projected available
balance rather than calculating every demand row independently.

This time-phased behavior is essential: a simple aggregate comparison of total
demand against total supply could incorrectly treat a late purchase receipt as
available for an earlier production requirement.

## Indexing Strategy

Indexes support the principal join and planning paths:

- BOM lookup by product, status, and effective date;
- component lookup by BOM;
- demand ordering by status and required date;
- inventory aggregation by material;
- purchase-order receipt lookup by material, status, and expected date; and
- supplier-source lookup by material, preference, and status.

## Deferred Model Extensions

Future versions may add:

- subassemblies and recursive multi-level BOM explosion;
- warehouses and location-transfer planning;
- purchase-order header and line separation;
- material lots and shelf-life controls;
- supplier capacity and allocation rules;
- demand history and forecasts;
- forecast accuracy and bias tracking; and
- stored planning runs for audit and scenario comparison.
