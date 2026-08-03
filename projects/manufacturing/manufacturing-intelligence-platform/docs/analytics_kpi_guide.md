# Analytics and KPI Guide

## Purpose

The analytics layer converts generated manufacturing transactions into
operational measures that can be reviewed in the terminal or exported for a
dashboard.

Run the complete report from the project root:

```bash
python -m src.analytics.kpis
```

Create dashboard-ready CSV files:

```bash
python -m src.analytics.export_dashboard_data
```

The command writes six extracts to `outputs/analytics/`. The generated files
are excluded from Git because they can be recreated from PostgreSQL at any
time.

## KPI definitions

### First-pass yield (FPY)

**Question:** What percentage of units completed an operation without scrap or
rework?

```text
FPY = good quantity / input quantity * 100
```

Higher is better. In this project, FPY is calculated from completed
`production_runs`, so it is an operation-level measure rather than final
finished-goods yield.

### Scrap rate

**Question:** What percentage of production input was discarded?

```text
Scrap rate = scrap quantity / input quantity * 100
```

Lower is better.

### Rework rate

**Question:** What percentage of production input required additional work?

```text
Rework rate = rework quantity / input quantity * 100
```

Lower is generally better because rework consumes additional capacity and
labor.

### Inspection pass rate

**Question:** What percentage of inspected sample units passed inspection?

```text
Inspection pass rate = passed quantity / sample size * 100
```

This differs from FPY because quality inspections may evaluate samples rather
than every produced unit.

### Downtime

**Questions:** How much machine interruption was recorded, and how much was
unplanned?

Total downtime includes both planned and unplanned events. Unplanned downtime
includes events such as failures, shortages, and power interruptions where
`planned_flag` is false.

## Reports

### Plant KPI summary

**Grain:** One row representing the entire generated dataset.

**Questions answered:**

- What is the plant's overall FPY?
- What percentages of input became scrap or rework?
- What percentage of inspected units passed?
- How many total and unplanned downtime minutes were recorded?

**Tables used:**

- `production_runs`
- `quality_inspections`
- `downtime_events`

The three tables are aggregated separately and combined only after aggregation.
This prevents a many-to-many join from multiplying quantities.

### Machine KPI comparison

**Grain:** One row per machine.

**Questions answered:**

- Which machines have the highest and lowest FPY?
- Which machines generate the most scrap or rework?
- Which machines experience the most unplanned downtime?
- Which machines have no completed production activity?

**Tables and join path:**

```text
machines
  -> aggregated production_runs by machine_id
  -> aggregated downtime_events by machine_id
```

Production and downtime are aggregated before joining to `machines`. A left
join retains machines with no completed runs, which display `N/A` for
production percentages.

### Product-family performance

**Grain:** One row per product family.

**Questions answered:**

- Which product families have the best and worst FPY?
- Which families generate the most scrap or rework?
- Which families have the longest average actual cycle time?
- How much completed run activity belongs to each family?

**Tables and join path:**

```text
products
  -> customer_order_items
  -> production_orders
  -> production_runs
```

Only completed runs are included. Quantities are operation-level totals; a unit
may appear in multiple operations as it moves through its manufacturing route.

### Quality-defect analysis

**Grain:** One row per machine, defect category, and severity combination.

**Questions answered:**

- Which machines have the largest concentrations of recorded defects?
- Which defect categories occur most frequently?
- Are the defects minor, major, or critical?

**Tables and join path:**

```text
quality_defects
  -> defect_types
  -> quality_inspections
  -> production_runs
  -> machines
```

`defect_records` counts defect records, while `defect_quantity` measures the
number of defective sampled units recorded by those records.

### Downtime-cause analysis

**Grain:** One row per downtime category and planned/unplanned classification.

**Questions answered:**

- What causes the most downtime minutes?
- Which causes occur most frequently?
- What is the average duration of each event type?
- Which causes are planned versus unplanned?

**Table used:** `downtime_events`

The report includes event count, total minutes, and average minutes per event.
These are reliability indicators, but the report does not claim MTBF because
the current schema does not store a complete scheduled operating calendar.

### Monthly KPI trends

**Grain:** One row per calendar month.

**Questions answered:**

- Are FPY, scrap, or rework rates changing over time?
- Which months have the most downtime?
- Is unplanned downtime increasing or decreasing?

**Tables used:**

- `production_runs`, grouped by `start_timestamp`
- `downtime_events`, grouped by `downtime_start`

The two monthly aggregates are combined with a full outer join so a month is
retained even if it contains only production or only downtime data.

All analytics reports use the current timestamp as an as-of boundary. Future
scheduled activity is excluded from historical KPIs so incomplete future
periods do not distort dashboard trends.

## OEE limitation

The project does not currently report Overall Equipment Effectiveness (OEE).
Performance and quality can be derived from existing production data, but true
availability requires planned machine production time.

The current `operators.shift` field identifies an employee's assigned shift; it
does not define the scheduled operating window for each machine. A future
`machine_shift_schedules` table would provide the missing machine-and-shift
grain needed for defensible OEE.

## Validation approach

- SQL queries retrieve and aggregate source records.
- `calculate_percentage()` performs reusable rate calculations.
- Python-only pytest tests validate percentage math, rounding, and division by
  zero without connecting to PostgreSQL.
- PostgreSQL constraints and ETL validation protect the underlying data.
