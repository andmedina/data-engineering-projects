# Synthetic Data Business Rules

## Purpose

This document records the business rules used to generate transactional data
for the Manufacturing Intelligence Platform.

The rules are designed to produce internally consistent, realistic synthetic
data for a simulated aerospace fastener manufacturer. They complement the
structural definitions in `docs/data_model.md` and the enforceable database
constraints in `database/schema.sql`.

The rules fall into three categories:

- **Database constraints** are mandatory rules enforced by PostgreSQL.
- **Documented business rules** come from the project data model.
- **Simulation assumptions** are adjustable choices made to create realistic
  synthetic data where no formal requirement exists.

---

## Customer Orders

**Table:** `customer_orders`

**Grain:** One row per customer purchase-order header.

### Generation Rules

- Generate 500 customer orders.
- Retrieve valid customer IDs from PostgreSQL.
- Generate unique order numbers beginning with `SO-100001`.
- Generate order dates within the preceding year.
- Set requested delivery 7–45 days after the order date.
- Leave `notes` empty for the initial version.

### Priority Distribution

| Priority | Probability |
|---|---:|
| Low | 5% |
| Standard | 70% |
| High | 20% |
| Rush | 5% |

### Status Distribution

| Status | Probability |
|---|---:|
| Open | 15% |
| Released | 15% |
| Partially Fulfilled | 8% |
| Completed | 60% |
| Cancelled | 2% |

---

## Customer Order Items

**Table:** `customer_order_items`

**Grain:** One row per product line within a customer order.

### Generation Rules

- Generate 1–5 lines per customer order.
- Retrieve active products and standard unit costs from PostgreSQL.
- Do not repeat a product within the same customer order.
- Number lines sequentially beginning with 1.
- Derive ordered quantity from the product family.
- Calculate unit price from standard unit cost, a family-specific markup, and
  a volume discount.
- Keep line status consistent with the parent order status.

### Quantity Rules

| Product family | Minimum | Maximum | Increment |
|---|---:|---:|---:|
| Solid Rivet | 1,000 | 20,000 | 100 |
| Blind Rivet | 500 | 10,000 | 100 |
| Blind Bolt | 100 | 3,000 | 50 |
| Temporary Fastener | 100 | 2,000 | 50 |
| Threaded Insert | 250 | 5,000 | 50 |
| Installation Tool | 1 | 20 | 1 |
| Other | 100 | 1,000 | 50 |

### Status Mapping

| Customer-order status | Order-line status |
|---|---|
| Open | Open |
| Released | Allocated |
| Partially Fulfilled | Partially Fulfilled |
| Completed | Completed |
| Cancelled | Cancelled |

### Pricing Assumptions

- Family-specific markups range from approximately 30% to 110% above standard
  unit cost.
- Orders reaching at least 40% of the family maximum receive a 4% discount.
- Orders reaching at least 75% of the family maximum receive an 8% discount.
- Unit prices are stored to four decimal places.

---

## Production Orders

**Table:** `production_orders`

**Grain:** One row per manufacturing work order.

### Eligibility and Status Rules

| Order-line status | Production behavior |
|---|---|
| Open | No production order |
| Allocated | Released or Scheduled production order |
| Partially Fulfilled | In Production work order |
| Completed | Completed work order |
| Cancelled | Approximately half retain a Cancelled work order |

Cancelled work orders represent customer lines cancelled after manufacturing
planning had already begun. Cancelled lines without work orders represent
cancellation before release to production.

### Machine Assignment

| Product family | Primary operation |
|---|---|
| Solid Rivet | Cold Heading |
| Blind Rivet | Cold Heading |
| Blind Bolt | Cold Heading |
| Temporary Fastener | Cold Heading |
| Threaded Insert | Thread Rolling |
| Installation Tool | Assembly |
| Other | Multi-Purpose |

- Retrieve compatible machine IDs from PostgreSQL.
- Released orders may remain unscheduled with no machine assignment.
- Scheduled, in-production, completed, and cancelled work orders receive a
  compatible primary machine when scheduled.

### Quantity Rules

- Add a 1–4% production allowance for expected fastener scrap.
- Installation tools have a 10% chance of one additional planned unit.
- Released, Scheduled, and Cancelled orders have zero completed and scrapped
  quantities.
- In Production orders are approximately 25–75% complete.
- Completed orders produce the full customer-ordered quantity.
- For completed orders, extra planned units become scrapped units.
- Completed plus scrapped quantity cannot exceed planned quantity.

### Date Rules

- Schedule production between the customer order date and requested delivery
  date.
- Production duration varies by product family.
- Released orders have no schedule or actual timestamps.
- Scheduled orders have schedule dates but no actual timestamps.
- In Production orders have an actual start but no actual end.
- Completed orders have both actual start and actual end timestamps.
- An end date or timestamp cannot precede its corresponding start value.

---

## Material Lots

**Table:** `material_lots`

**Grain:** One row per received supplier material lot.

### Generation Rules

- Retrieve active materials from PostgreSQL.
- Retrieve active approved or conditional raw-material suppliers from
  PostgreSQL.
- Generate replenishment lots from monthly production demand.
- Add three background lots per material to represent broader inventory
  history.
- Generate receipts from 120 days before the earliest customer order through
  the current simulated operating date.
- Prefer suppliers with higher quality ratings during random selection.
- Generate globally unique supplier lot numbers beginning with `ML-`.
- Base receipt quantity ranges on material form.

- Monthly replenishment quantity covers calculated demand plus 25% safety
  stock.
- Split a monthly receipt into multiple lots when required supply exceeds the
  material form's normal maximum receipt size.

### Receipt Quantity Ranges

| Material form | Minimum | Maximum |
|---|---:|---:|
| Wire | 1,000 lb | 6,000 lb |
| Rod | 1,000 lb | 5,000 lb |
| Bar | 1,500 lb | 7,000 lb |
| Coil | 2,000 lb | 9,000 lb |
| Sheet | 1,000 lb | 5,000 lb |
| Component | 500 units | 3,000 units |

### Status and Inventory Rules

- Supplier quality rating influences rejection probability.
- On-hold probability is 6%.
- Depleted probability is 22%.
- Available lots retain approximately 15–90% of received quantity.
- On-hold lots retain approximately 40–100% of received quantity.
- Depleted and rejected lots have zero available quantity.
- Available quantity cannot exceed received quantity.

---

## Production Order Materials

**Table:** `production_order_materials`

**Grain:** One row per material lot allocated to one production order.

### Allocation Rules

- Allocate material only to Scheduled, In Production, and Completed work
  orders.
- Use the primary material assigned to the ordered product in PostgreSQL.
- A material lot must have been received before the scheduled production start.
- Allocate lots using first-in, first-out order by receipt date.
- Allow one production order to consume multiple lots when necessary.
- Never allocate rejected material.
- Prevent duplicate production-order and material-lot combinations.

### Material Requirement Assumptions

- Estimate fastener weight from product diameter, length, material density, and
  a product-family geometry factor.
- Assume installation tools require 3 pounds of primary material per unit.
- Add 2–5% process loss to the calculated material requirement.
- Completed orders use historical consumed capacity from non-rejected lots.
- Scheduled and in-production orders reserve current inventory from Available
  lots.
- Current lot inventory is reduced in the same database transaction as the
  allocation insert.
- An Available lot becomes Depleted when its remaining quantity reaches zero.

---

## Production Runs

**Table:** `production_runs`

**Grain:** One continuous execution of one manufacturing operation for one
production order on one machine.

### Routing Rules

- Define an ordered operation route for each product family.
- End every route with Inspection and Packaging.
- Retrieve compatible machines from PostgreSQL by operation type.
- Assign currently certified operators to manufacturing operations.
- Assign currently certified inspectors to Inspection operations.
- Number route operations sequentially beginning with 1.

### Status Rules

| Production-order status | Production-run behavior |
|---|---|
| Released | No production runs yet |
| Scheduled | Full route of Planned runs |
| In Production | Completed upstream runs, one Running run, and Planned downstream runs |
| Completed | Full route of Completed runs |
| Cancelled | Full route of Cancelled runs retained for planning history |

### Quantity Rules

- Every run satisfies `input = good + scrap + rework`.
- Scrap permanently reduces the quantity passed downstream.
- Rework remains eligible to continue to the next operation.
- Planned and Cancelled runs have zero processed quantities.
- For Completed work orders, total run scrap reconciles to production-order
  scrap and final packaged good quantity reconciles to production-order
  completed quantity.
- For In Production work orders, quantities represent the batch processed to
  date; downstream Planned runs remain at zero.

### Cycle-Time and Timestamp Rules

- Derive planned cycle time from the product standard cycle time and an
  operation-specific multiplier.
- Generate actual cycle time within approximately 92–118% of planned time.
- Planned and Cancelled runs do not have actual cycle times or timestamps.
- Completed operation timestamps occur sequentially within the parent work
  order's actual execution window.
- A Running operation has a start timestamp and no end timestamp.
- An end timestamp cannot precede its start timestamp.

---

## Quality Inspections

**Table:** `quality_inspections`

**Grain:** One inspection event with one primary measurement for one production
run.

### Inspection Eligibility

- Inspect every Completed dedicated Inspection operation.
- Give Completed in-process manufacturing operations a 20% chance of an
  additional process inspection.
- Allow a Running dedicated Inspection operation to have a Pending event.
- Do not inspect Planned or Cancelled runs.
- Do not generate separate process inspections for Packaging operations.

### Sampling and Result Rules

- Select a target sample size from 5, 10, 20, 32, or 50 units.
- Do not sample more units than the run has processed.
- Derive failure probability partly from the run's observed scrap and rework
  rate, bounded between 0.5% and 15%.
- Require `sample_size = passed_quantity + failed_quantity`.
- Assign Pass when the sample has no failures.
- Assign Conditional when failures are no more than 5% of the sample.
- Assign Fail when failures exceed 5% of the sample.
- Pending inspections have zero sampled quantities and no measured value.

### Measurement Rules

- Select measurement type from the manufacturing operation, such as Diameter,
  Thread Pitch, Tensile Strength, Surface Finish, Coating Thickness, or
  Assembly Gap.
- Derive dimensional limits from product master data where available.
- Derive tensile limits from material category.
- Keep Pass measurements within specification.
- Place Fail measurements outside specification.
- Allow Conditional measurements near a specification boundary.
- Store measurements and specification limits to four decimal places.

### Inspector Rules

- Retrieve inspectors from PostgreSQL.
- Use only active employees with the Inspector role and Current certification.
- Timestamp completed inspections at the end of their production run.
- Timestamp pending inspections at the running operation's start.

---

## Quality Defects

**Table:** `quality_defects`

**Grain:** One standardized defect type identified during one inspection.

### Defect Generation Rules

- Generate defects only when an inspection has failed sampled units.
- Do not generate defects for Pass or Pending inspections.
- Assign one to three unique compatible defect types per failed inspection.
- Map measurement types to active defect codes retrieved from PostgreSQL.
- Partition failed units across selected defect types.
- Require every generated defect quantity to be positive.
- Require the sum of defect quantities for an inspection to equal its failed
  quantity.

### Disposition Rules

- Use severity and inspection result to influence disposition.
- Favor Scrap or Return to Supplier for Critical material defects.
- Favor Rework for Major defects.
- Favor Rework or Use As Is for Minor defects.
- Restrict Conditional inspections to Rework, Use As Is, or Pending Review.
- Use Return to Supplier only for material-category defects.

### Root Cause and Corrective Action Rules

- Select root-cause categories from distributions appropriate to the defect
  category.
- Favor Machine or Method causes for dimensional and thread defects.
- Favor Material causes for material defects.
- Favor Method or Environment causes for coating defects.
- Favor Operator or Method causes for assembly and packaging defects.
- Derive corrective-action text from the selected root-cause category.

---

## Downtime Events

**Table:** `downtime_events`

**Grain:** One continuous downtime event for one machine.

### Event Generation Rules

- Give Completed production runs an 8% chance of one linked downtime event.
- Give Running production runs a 25% chance of one linked downtime event.
- Generate at most one linked downtime event per production run.
- Require a linked event's machine to match the production run's machine.
- Keep linked downtime within the production run's execution window.
- Add two to five standalone planned events per active or idle machine across
  the simulated operating period.
- Leave `production_run_id` null for standalone events.

### Category Rules

- Treat Tool Change, Setup, Preventive Maintenance, and Changeover as planned.
- Treat Mechanical Failure, Material Shortage, Quality Hold, Operator
  Unavailable, and Power Interruption as unplanned.
- Increase Tool Change likelihood for forming and thread-rolling operations.
- Increase Quality Hold likelihood for inspection operations.
- Increase Operator Unavailable likelihood for assembly and packaging.
- Derive duration range and reason text from downtime category.

### Timestamp and Duration Rules

- Require downtime end to occur after downtime start.
- Keep event duration within its available operating window.
- Store `downtime_minutes` as the exact whole-minute difference between start
  and end timestamps.
- Generate standalone planned events within a 12-hour operating window.

---

## Maintenance Events

**Table:** `maintenance_events`

**Grain:** One maintenance activity performed on one machine.

### Downtime-Driven Maintenance

- Generate Corrective maintenance for every Mechanical Failure downtime event.
- Generate Preventive maintenance for every Preventive Maintenance downtime
  event.
- Use the same machine and service window as the originating downtime event.
- Report Corrective work zero to 30 minutes before the downtime begins.
- Schedule Preventive work seven to 30 days before its service window.
- Do not automatically convert other downtime categories into maintenance.

### Routine Maintenance

- Generate one Predictive and one equipment Inspection event per active or idle
  machine across the operating period.
- Generate two Calibration events for each inspection machine.
- Retrieve technician names from active employees with the Technician role and
  Current certification.
- Select the serviced component from the machine's operation type.
- Derive maintenance action text from maintenance type.

### Cost and Machine-Hour Rules

- Use maintenance-type cost ranges, adjusted slightly by service duration.
- Require nonnegative maintenance cost and machine hours.
- Estimate machine hours from installation date, service date, a 16-hour
  operating day, and a stable machine-specific utilization factor.
- Ensure estimated machine hours increase as service dates advance.
- Require maintenance end to occur on or after maintenance start.

---

## Sensor Readings

**Table:** `sensor_readings`

**Grain:** One reading per machine per five-minute timestamp.

### Telemetry Window and Frequency

- Generate 12 months of telemetry for the two Cold Heading machines used by
  the predictive-maintenance proof of concept.
- Retain a recent 30-day telemetry window for all other active or idle
  machines.
- Generate one reading every five minutes for every active or idle machine.
- End telemetry at the latest completed five-minute interval so future sensor
  readings are never created.
- Require each machine and timestamp combination to be unique.
- Load readings into PostgreSQL in batches of 5,000 rows.

### Machine-State Rules

- Use weekday, hour, and machine status to estimate operating versus idle
  state.
- Favor operation during 06:00–22:00 on weekdays.
- Reduce operating probability overnight, on weekends, and for Idle machines.
- Force an idle sensor state during recorded downtime.
- Use lower power, temperature, vibration, pressure, and RPM ranges while idle.

### Sensor Applicability and Ranges

- Define operation-specific ranges for temperature, vibration, power, pressure,
  and RPM.
- Store NULL when a sensor does not apply to a machine type.
- Heat-treatment equipment has high operating temperatures but no vibration,
  pressure, or RPM reading in the initial model.
- Inspection equipment records ambient temperature and power only.
- Require every recorded sensor value to be nonnegative.

### Cold-Heading Failure Signatures

- Link a Mechanical Failure to the failed component recorded by its corrective
  maintenance event.
- Simulate forming-die deterioration with higher vibration and power plus a
  moderate temperature increase.
- Simulate hydraulic-system deterioration with lower pressure plus higher
  temperature and power.
- Simulate feed-system deterioration with lower RPM and power plus higher
  vibration.
- Apply these component signatures only during the 60 minutes preceding a
  Mechanical Failure and only while the machine is operating.

### Anomaly Rules

- Use the component-specific signatures above for Cold Heading machines.
- For other machine families, use a generic pre-failure signature with higher
  temperature, vibration, and power plus lower pressure and RPM.
- Add a 0.2% background-anomaly probability outside known failures.
- Preserve downtime behavior over anomaly behavior when a machine is stopped.

---

## Validation Strategy

Each transactional stage is validated at three levels:

1. Generator tests confirm that Python business logic behaves as expected.
2. PostgreSQL constraints reject structurally invalid data.
3. Post-load SQL checks verify cross-table relationships and distributions.

This document should be updated whenever a generation rule changes or a new
transactional table is implemented.
