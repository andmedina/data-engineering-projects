# Manufacturing Data Model

## Overview

This document defines the initial relational data model for the Manufacturing Intelligence Platform.

The model represents a vertically integrated aerospace fastener manufacturing environment. It connects operational data from customer orders, production planning, manufacturing execution, raw-material traceability, quality inspection, machine downtime, maintenance, and industrial sensor systems.

The purpose of the model is to support:

- Manufacturing KPI calculation
- Production-performance monitoring
- Quality and defect analysis
- Supplier and material-lot traceability
- Downtime root-cause analysis
- Machine-health monitoring
- Predictive-maintenance modeling
- Defect-prediction modeling
- Future AI-assisted operational analysis

The model is designed for PostgreSQL and will serve as the foundation for the project’s synthetic data generation, ETL pipelines, dashboards, SQL analytics, and machine-learning components.

---

## Manufacturing Process

The simulated facility produces aerospace fasteners through a multi-stage manufacturing process.

```text
Customer Order
      ↓
Production Order
      ↓
Raw Material Lot Assignment
      ↓
Cold Heading
      ↓
Thread Rolling
      ↓
Heat Treatment
      ↓
Surface Finishing
      ↓
Assembly
      ↓
Quality Inspection
      ↓
Packaging
```

Not every product requires every operation.

For example:

- Solid rivets may not require assembly.
- Blind rivets may require assembly of multiple components.
- Some materials may require heat treatment.
- Different products may require different surface finishes.
- Inspection may occur during production and after final processing.

A production order may therefore follow a different operation route depending on the product family, material, specification, and finish requirements.

---

## Data Domains

The data model is organized into six major business domains.

| Domain | Description |
|---|---|
| Master Data | Stable reference data such as customers, products, materials, machines, operators, suppliers, and defect codes |
| Order Management | Customer demand and internal production orders |
| Manufacturing Execution | Production runs, machine activity, quantities, cycle times, and operation sequences |
| Material Traceability | Raw materials, supplier lots, and material consumption |
| Quality Management | Inspections, measurements, defects, disposition, and corrective actions |
| Equipment Management | Downtime events, maintenance activity, and machine sensor readings |

---

# Entity Definitions

## Master Data Entities

### `customers`

Stores fictional customer organizations that purchase manufactured aerospace fasteners.

**Table grain:** One row per customer.

| Column | Description |
|---|---|
| `customer_id` | Unique internal customer identifier |
| `customer_name` | Fictional customer organization name |
| `customer_type` | Customer classification |
| `country` | Customer country |
| `industry_segment` | Aerospace, defense, commercial aviation, or distribution |
| `active_flag` | Indicates whether the customer is active |

---

### `suppliers`

Stores organizations that provide raw materials or manufacturing supplies.

**Table grain:** One row per supplier.

| Column | Description |
|---|---|
| `supplier_id` | Unique supplier identifier |
| `supplier_name` | Fictional supplier name |
| `supplier_category` | Raw material, coating, tooling, or component supplier |
| `country` | Supplier country |
| `approved_status` | Approved, conditional, suspended, or inactive |
| `quality_rating` | Supplier quality score |
| `average_lead_time_days` | Average delivery lead time |

---

### `materials`

Defines the raw materials used in manufacturing.

**Table grain:** One row per material specification.

| Column | Description |
|---|---|
| `material_id` | Unique material identifier |
| `material_code` | Internal material code |
| `material_name` | Material description |
| `material_category` | Aluminum, titanium, steel, or nickel alloy |
| `alloy` | Specific alloy designation |
| `material_form` | Wire, rod, bar, sheet, or component |
| `unit_of_measure` | Pounds, kilograms, feet, units, or another inventory unit |

Example materials may include:

- Aluminum 2117
- Aluminum 2024
- Aluminum 7050
- Titanium Ti-6Al-4V
- Stainless Steel
- Nickel Alloy

---

### `products`

Defines the finished fastener products manufactured by the simulated facility.

**Table grain:** One row per manufactured part number.

| Column | Description |
|---|---|
| `product_id` | Unique product identifier |
| `part_number` | Internal manufactured part number |
| `product_family` | Solid rivet, blind rivet, blind bolt, temporary fastener, or threaded insert |
| `material_type` | Primary material used |
| `diameter_in` | Product diameter in inches |
| `length_in` | Product length in inches |
| `finish_type` | Surface finish or coating |
| `aerospace_specification` | Simulated engineering or aerospace specification |
| `standard_cycle_time_seconds` | Expected cycle time per unit |
| `unit_cost` | Estimated manufacturing cost per unit |
| `active_flag` | Indicates whether the part is currently manufactured |

---

### `machines`

Defines the production equipment used throughout the manufacturing process.

**Table grain:** One row per machine.

| Column | Description |
|---|---|
| `machine_id` | Unique machine identifier |
| `machine_code` | Human-readable machine code |
| `machine_name` | Machine description |
| `operation_type` | Primary operation performed |
| `production_line` | Assigned production line or department |
| `manufacturer` | Equipment manufacturer |
| `model` | Equipment model |
| `install_date` | Date placed into service |
| `rated_capacity_per_hour` | Expected hourly production capacity |
| `status` | Active, idle, maintenance, or retired |

Example operation types include:

- Cold Heading
- Thread Rolling
- Heat Treatment
- Surface Finishing
- Assembly
- Inspection
- Packaging

---

### `operators`

Stores anonymized manufacturing employees who operate machines or inspect production.

**Table grain:** One row per employee.

| Column | Description |
|---|---|
| `operator_id` | Unique employee identifier |
| `employee_code` | Anonymized employee code |
| `operator_name` | Fictional or anonymized employee name |
| `shift` | Assigned production shift |
| `experience_level` | Entry, intermediate, senior, or lead |
| `hire_date` | Employee hire date |
| `certification_status` | Current operator certification status |
| `active_flag` | Indicates whether the employee is active |

---

### `defect_types`

Defines standardized quality-defect categories.

**Table grain:** One row per defect type.

| Column | Description |
|---|---|
| `defect_type_id` | Unique defect identifier |
| `defect_code` | Standardized defect code |
| `defect_name` | Defect name |
| `defect_category` | Dimensional, material, thread, surface, coating, or assembly defect |
| `severity` | Minor, major, or critical |
| `description` | Detailed defect description |

Example defects include:

| Defect Code | Defect Name |
|---|---|
| `DIM-001` | Diameter Out of Tolerance |
| `HEAD-001` | Incorrect Head Height |
| `THR-001` | Thread Damage |
| `SUR-001` | Surface Scratch |
| `CRK-001` | Material Crack |
| `FIN-001` | Improper Coating |
| `ASM-001` | Incorrect Assembly |

---

## Order Management Entities

Order management follows a standard header-detail design commonly used in ERP systems.

- `customer_orders` stores order-level information (order header).
- `customer_order_items` stores the individual products requested within each order (order lines).
- `production_orders` stores the internal manufacturing orders created to fulfill each customer order item.

### `customer_orders`

Stores customer purchase orders received by the manufacturer.

**Table grain:** One row per customer purchase order (order header).

| Column | Description |
|---|---|
| `customer_order_id` | Unique customer-order identifier |
| `customer_id` | Customer placing the order |
| `customer_order_number` | Business-facing customer order number |
| `order_date` | Date the order was received |
| `requested_delivery_date` | Customer-requested delivery date |
| `priority` | Standard, expedited, or critical |
| `order_status` | Open, planned, in production, completed, shipped, or cancelled |

A customer order contains one or more customer order items.

---

### `customer_order_items`

Stores the individual products requested within a customer order.

**Table grain:** One row per product line within a customer order.

| Column | Description |
|---|---|
| `customer_order_item_id` | Unique customer order line identifier |
| `customer_order_id` | Parent customer order |
| `product_id` | Product requested by the customer |
| `ordered_quantity` | Quantity requested |
| `unit_price` | Price per unit (optional) |
| `line_status` | Open, allocated, completed, or cancelled |

Each customer order item may generate one or more production orders.

---

### `production_orders`

Stores internal manufacturing orders created to fulfill a specific customer order item.

**Table grain:** One row per manufacturing order.

| Column | Description |
|---|---|
| `production_order_id` | Unique production-order identifier |
| `customer_order_item_id` | Related customer order line |
| `planned_quantity` | Quantity scheduled for production |
| `actual_quantity` | Final completed quantity |
| `planned_start_date` | Planned production start |
| `actual_start_time` | Actual production start timestamp |
| `planned_end_date` | Planned production completion |
| `actual_end_time` | Actual production completion timestamp |
| `order_status` | Planned, released, in progress, completed, on hold, or cancelled |

---

## Material Traceability Entities

### `material_lots`

Stores individual raw-material deliveries and supplier lot information.

**Table grain:** One row per received raw-material lot.

| Column | Description |
|---|---|
| `material_lot_id` | Unique internal material-lot identifier |
| `material_id` | Material specification |
| `supplier_id` | Supplier that provided the lot |
| `supplier_lot_number` | Supplier-provided lot number |
| `received_date` | Date the lot was received |
| `received_quantity` | Original quantity received |
| `remaining_quantity` | Current available quantity |
| `certificate_status` | Status of material certification documentation |
| `inspection_status` | Incoming inspection status |

This table supports traceability from the finished product back to the source material and supplier.

---

### `production_order_materials`

Connects production orders to the material lots consumed during manufacturing.

**Table grain:** One row per material lot consumed by a production order.

| Column | Description |
|---|---|
| `production_order_material_id` | Unique bridge-table identifier |
| `production_order_id` | Production order consuming the material |
| `material_lot_id` | Material lot used |
| `quantity_consumed` | Quantity consumed from the lot |
| `assigned_date` | Date the material was issued to production |

This bridge table resolves the many-to-many relationship between production orders and material lots.

A production order may consume multiple material lots, and a material lot may be used across multiple production orders.

---

## Manufacturing Execution Entities

### `production_runs`

Stores the execution of an individual manufacturing operation.

**Table grain:** One row per production-order operation executed on one machine.

| Column | Description |
|---|---|
| `production_run_id` | Unique production-run identifier |
| `production_order_id` | Related production order |
| `machine_id` | Machine used for the operation |
| `operator_id` | Primary operator responsible for the run |
| `operation_sequence` | Numerical sequence of the operation |
| `operation_type` | Manufacturing operation performed |
| `start_time` | Actual run start timestamp |
| `end_time` | Actual run end timestamp |
| `planned_cycle_time_seconds` | Expected cycle time |
| `actual_cycle_time_seconds` | Observed average cycle time |
| `input_quantity` | Quantity entering the operation |
| `good_quantity` | Quantity completed successfully |
| `scrap_quantity` | Quantity scrapped |
| `rework_quantity` | Quantity requiring rework |
| `run_status` | Planned, running, completed, interrupted, or cancelled |

The `production_runs` table is the central operational entity in the data model.

It connects:

- Production orders
- Products
- Machines
- Operators
- Manufacturing operations
- Production quantities
- Quality inspections
- Downtime events
- Sensor conditions

---

## Quality Management Entities

### `quality_inspections`

Stores inspections performed during or after a manufacturing operation.

**Table grain:** One row per inspection event for one production run.

| Column | Description |
|---|---|
| `inspection_id` | Unique inspection identifier |
| `production_run_id` | Production run being inspected |
| `inspector_id` | Employee performing the inspection |
| `inspection_time` | Inspection timestamp |
| `sample_size` | Number of units inspected |
| `passed_quantity` | Number of inspected units passing |
| `failed_quantity` | Number of inspected units failing |
| `inspection_result` | Pass, fail, conditional, or pending |
| `measurement_type` | Characteristic measured |
| `measured_value` | Recorded measurement |
| `lower_spec_limit` | Minimum allowable value |
| `upper_spec_limit` | Maximum allowable value |

Example measurement types include:

- Diameter
- Length
- Head Height
- Thread Pitch
- Surface Finish
- Coating Thickness
- Tensile Strength

For the initial version, one inspection row stores one primary measurement result. A future version may separate inspection headers from individual measurement records.

---

### `quality_defects`

Stores defects identified during an inspection.

**Table grain:** One row per defect type recorded during one inspection.

| Column | Description |
|---|---|
| `quality_defect_id` | Unique quality-defect identifier |
| `inspection_id` | Inspection where the defect was recorded |
| `defect_type_id` | Standardized defect classification |
| `defect_quantity` | Number of units affected |
| `disposition` | Final decision for affected units |
| `root_cause_category` | Suspected or confirmed root cause |
| `corrective_action` | Action taken to prevent recurrence |

Possible dispositions include:

- Scrap
- Rework
- Use As Is
- Return to Supplier
- Pending Review

Possible root-cause categories include:

- Machine
- Material
- Method
- Measurement
- Operator
- Environment
- Unknown

---

## Equipment Management Entities

### `downtime_events`

Stores machine stoppages and production interruptions.

**Table grain:** One row per continuous downtime incident.

| Column | Description |
|---|---|
| `downtime_event_id` | Unique downtime-event identifier |
| `machine_id` | Machine affected |
| `production_run_id` | Production run affected, when applicable |
| `downtime_start` | Downtime start timestamp |
| `downtime_end` | Downtime end timestamp |
| `downtime_minutes` | Total downtime duration |
| `downtime_category` | Standardized downtime category |
| `downtime_reason` | Detailed downtime explanation |
| `planned_flag` | Indicates whether the downtime was planned |

Example downtime categories include:

- Mechanical Failure
- Tool Change
- Setup
- Material Shortage
- Quality Hold
- Preventive Maintenance
- Operator Unavailable
- Changeover
- Power Interruption

---

### `maintenance_events`

Stores preventive and corrective maintenance performed on machines.

**Table grain:** One row per maintenance action.

| Column | Description |
|---|---|
| `maintenance_event_id` | Unique maintenance-event identifier |
| `machine_id` | Machine receiving maintenance |
| `maintenance_type` | Preventive, corrective, predictive, calibration, or inspection |
| `reported_time` | Time the problem or service need was reported |
| `maintenance_start` | Maintenance start timestamp |
| `maintenance_end` | Maintenance completion timestamp |
| `technician` | Fictional or anonymized technician |
| `failure_component` | Component associated with the service |
| `maintenance_action` | Work performed |
| `maintenance_cost` | Estimated maintenance cost |
| `machine_hours_at_service` | Machine operating hours at service time |

---

### `sensor_readings`

Stores time-series machine telemetry.

**Table grain:** One row per machine per sensor-reading timestamp.

| Column | Description |
|---|---|
| `sensor_reading_id` | Unique sensor-reading identifier |
| `machine_id` | Machine producing the reading |
| `reading_timestamp` | Timestamp of the reading |
| `temperature_c` | Machine temperature in Celsius |
| `vibration_mm_s` | Vibration velocity |
| `power_kw` | Electrical power consumption |
| `pressure_psi` | Operating or hydraulic pressure |
| `rpm` | Rotational speed |

Sensor readings will initially be generated at five-minute intervals.

Not every sensor value will apply to every machine type. Non-applicable measurements may be stored as null values.

---

# Table Grain Summary

The grain defines exactly what one row represents in each table.

| Table | Grain |
|---|---|
| `customers` | One row per customer |
| `suppliers` | One row per supplier |
| `materials` | One row per material specification |
| `products` | One row per manufactured part number |
| `machines` | One row per machine |
| `operators` | One row per employee |
| `defect_types` | One row per standardized defect type |
| `customer_orders` | One row per customer purchase order (order header) |
| `customer_order_items` | One row per product line within a customer order |
| `production_orders` | One row per manufacturing order |
| `material_lots` | One row per received supplier material lot |
| `production_order_materials` | One row per material lot consumed by a production order |
| `production_runs` | One row per production-order operation executed on one machine |
| `quality_inspections` | One row per inspection event for a production run |
| `quality_defects` | One row per defect type recorded during an inspection |
| `downtime_events` | One row per continuous machine downtime incident |
| `maintenance_events` | One row per machine maintenance action |
| `sensor_readings` | One row per machine per sensor-reading timestamp |

---
# Entity Relationships

## High-Level Relationship Diagram

```text
customers
    │
    └── customer_orders
            │
            └── customer_order_items
                    ├── products
                    │
                    └── production_orders
                            │
                            ├── production_runs
                            │       ├── machines
                            │       ├── operators
                            │       ├── quality_inspections
                            │       │       └── quality_defects
                            │       │               └── defect_types
                            │       │
                            │       └── downtime_events
                            │
                            └── production_order_materials
                                    └── material_lots
                                            ├── materials
                                            └── suppliers

machines
    ├── maintenance_events
    └── sensor_readings
```

---

## Cardinality Summary

| Parent Entity | Relationship | Child Entity |
|---|---|---|
| `customers` | One-to-many | `customer_orders` |
| `customer_orders` | One-to-many | `customer_order_items` |
| `products` | One-to-many | `customer_order_items` |
| `customer_order_items` | One-to-many | `production_orders` |
| `production_orders` | One-to-many | `production_runs` |
| `machines` | One-to-many | `production_runs` |
| `operators` | One-to-many | `production_runs` |
| `production_orders` | Many-to-many through bridge | `material_lots` |
| `materials` | One-to-many | `material_lots` |
| `suppliers` | One-to-many | `material_lots` |
| `production_runs` | One-to-many | `quality_inspections` |
| `quality_inspections` | One-to-many | `quality_defects` |
| `defect_types` | One-to-many | `quality_defects` |
| `machines` | One-to-many | `downtime_events` |
| `production_runs` | One-to-many | `downtime_events` |
| `machines` | One-to-many | `maintenance_events` |
| `machines` | One-to-many | `sensor_readings` |

---

# Analytical Use Cases

The model should support questions such as:

## Production Performance

- Which production lines have the highest throughput?
- Which machines operate above or below their expected cycle times?
- Which production orders are at risk of missing their due dates?
- Which product families have the longest manufacturing lead times?
- Which shifts produce the highest good-part quantities?

## Quality

- Which products have the highest defect rates?
- Which machines are associated with increased dimensional defects?
- Which defect categories occur most frequently?
- Does first-pass yield vary by machine, shift, product, or supplier?
- Which material lots are associated with increased scrap or rework?

## Downtime

- Which machines generate the most unplanned downtime?
- What are the most common downtime reasons?
- Which departments experience the greatest production loss?
- Does downtime increase as machines age?
- Does preventive maintenance reduce future downtime?

## Supplier Performance

- Which suppliers have the highest material-related defect rates?
- Which suppliers have the longest lead times?
- Which material lots are linked to quality failures?
- Does supplier quality rating align with observed production performance?

## Predictive Analytics

- Can sensor readings predict machine failure?
- Can temperature or vibration anomalies predict downtime?
- Can production defects be predicted before final inspection?
- Which variables have the strongest relationship with scrap?
- Can maintenance needs be predicted from machine telemetry?

---

# KPI Support

The model will support calculation of the following manufacturing KPIs.

## Overall Equipment Effectiveness

Overall Equipment Effectiveness combines:

- Availability
- Performance
- Quality

```text
OEE = Availability × Performance × Quality
```

## Availability

```text
Availability =
Operating Time / Planned Production Time
```

## Performance

```text
Performance =
Ideal Cycle Time × Total Units / Operating Time
```

## Quality

```text
Quality =
Good Units / Total Units Produced
```

## First Pass Yield

```text
First Pass Yield =
Units Passing Without Rework / Total Units Entering the Process
```

## Scrap Rate

```text
Scrap Rate =
Scrap Quantity / Total Quantity Produced
```

## Rework Rate

```text
Rework Rate =
Rework Quantity / Total Quantity Produced
```

## Defect Rate

```text
Defect Rate =
Defective Units / Units Inspected
```

## Schedule Attainment

```text
Schedule Attainment =
Completed Production / Planned Production
```

Additional metrics may include:

- Throughput
- Average Cycle Time
- Capacity Utilization
- Mean Time Between Failures
- Mean Time to Repair
- Supplier Defect Rate
- On-Time Completion Rate

---

# Design Assumptions

The initial model uses the following assumptions:

1. The project represents one manufacturing facility.
2. All customers, suppliers, employees, and part numbers are fictional.
3. A customer order contains one or more customer order items.
4. Each customer order item represents one product requested by the customer.
5. Each production order fulfills one customer order item.
6. A production order may contain multiple production runs.
7. Each production run represents one manufacturing operation on one machine.
8. A production run has one primary operator.
9. A production order may consume multiple material lots.
10. One material lot may be consumed across multiple production orders.
11. Quality inspections are linked to production runs.
12. One inspection may identify multiple defect types.
13. Downtime may occur during a production run or outside an active production run.
14. Maintenance events are recorded separately from downtime events.
15. Sensor readings are captured every five minutes.
16. Sensor availability varies by machine type.
17. Production quantities remain internally consistent:

```text
input_quantity =
good_quantity + scrap_quantity + rework_quantity
```

18. Downstream operation quantities should not exceed the available quantity from the preceding operation.
19. Completed production orders should have an actual completion timestamp.
20. Cancelled production orders should not generate completed production runs.
21. Material-lot consumption should not exceed the lot's received quantity.

---

# Data Quality Rules

The ETL pipeline and automated tests should validate the following rules:

- Primary keys must be unique.
- Required foreign keys must reference valid parent records.
- Quantities cannot be negative.
- End timestamps cannot occur before start timestamps.
- `remaining_quantity` cannot exceed `received_quantity`.
- `actual_quantity` should not exceed valid completed output without explanation.
- Production-run quantities must reconcile.
- Inspection passed and failed quantities should not exceed sample size.
- Defect quantities should not exceed failed inspection quantities.
- Downtime duration should agree with downtime start and end timestamps.
- Machine sensor timestamps should be unique for each machine.
- Completed runs must contain start and end timestamps.
- Production runs must use machines capable of performing the assigned operation.
- Material-lot assignments must use materials compatible with the product.
- Sensor values should fall within physically plausible ranges.
- Maintenance completion cannot occur before maintenance start.
- Inactive or retired machines should not receive new production runs.

---

# Initial Database Build Order

Tables should be created in dependency order.

```text
1. customers
2. suppliers
3. materials
4. products
5. machines
6. operators
7. defect_types
8. customer_orders
9. customer_order_items
10. production_orders
11. material_lots
12. production_order_materials
13. production_runs
14. quality_inspections
15. quality_defects
16. downtime_events
17. maintenance_events
18. sensor_readings
```

---

# Future Schema Enhancements

The current schema focuses on the core entities required to support manufacturing operations, analytics, and machine learning workflows. Future iterations may expand the model with additional entities commonly found in enterprise manufacturing and ERP systems.

## Product Routing

A `product_routes` table could define the required operation sequence for each product.

```text
product
    ↓
operation sequence
    ↓
required machine type
    ↓
standard cycle time
```

## Production Shifts

A dedicated `shifts` table could replace shift values stored directly on operators.

## Individual Inspection Measurements

Quality data may be normalized into:

- `inspection_headers`
- `inspection_measurements`
- `measurement_specifications`

This would allow one inspection to contain multiple dimensional measurements.

## Tooling

Tool usage and tool life could be modeled with:

- `tools`
- `machine_tools`
- `tool_usage_events`
- `tool_replacement_events`

## Work Centers

Machines could be grouped into formal work centers or departments.

## Inventory Transactions

Detailed inventory movements could be stored as:

- Receipts
- Material issues
- Returns
- Transfers
- Adjustments
- Finished-goods receipts

## Product Genealogy

Finished-product lots could be linked to:

- Raw-material lots
- Production runs
- Operators
- Machines
- Inspection results

This would support end-to-end product traceability.

## Maintenance Work Orders

Maintenance activity could be expanded into:

- Work-order headers
- Labor records
- Replacement parts
- Failure codes
- Service schedules

## Environmental Sensors

Facility-level data could include:

- Ambient temperature
- Humidity
- Air quality
- Power consumption

## Slowly Changing Dimensions

A future warehouse layer could preserve historical changes to:

- Supplier status
- Product specifications
- Machine assignments
- Operator certifications

## Data Warehouse Layer

The normalized operational model may later feed an analytical star schema containing:

- `fact_production`
- `fact_quality`
- `fact_downtime`
- `fact_maintenance`
- `dim_date`
- `dim_product`
- `dim_machine`
- `dim_supplier`
- `dim_operator`
- `dim_shift`

---

# Next Steps

After this data model is reviewed, the next implementation steps are:

1. Create the PostgreSQL DDL in `database/schema.sql`.
2. Define primary keys and foreign keys.
3. Add validation constraints and indexes.
4. Create reference data in `database/seed.sql`.
5. Generate synthetic source-system datasets.
6. Build the Python ETL pipeline.
7. Add automated data-quality tests.
8. Build SQL KPI queries and dashboards.