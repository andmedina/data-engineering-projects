# Manufacturing Intelligence Platform Architecture

## Purpose

This project simulates how a manufacturing data scientist could integrate
operational data from an aerospace fastener company into a single analytical
platform. It covers source-system modeling, synthetic data generation, ETL,
data validation, operational analytics, dashboarding, and predictive
maintenance.

The implementation is intentionally local and reproducible for a portfolio
demonstration. PostgreSQL represents the integrated operational data store;
Python represents the ingestion, analytics, and modeling layers.

## System Overview

```text
Simulated operational systems

 ERP                  MES                QMS
 customers            production runs   inspections
 orders               machine activity  defects
 suppliers            downtime          defect types
 materials            maintenance
 inventory lots
       \                 |                 /
        \                |                /
         +------ Python synthetic ETL ---+
                         |
                         v
                  PostgreSQL database
                         |
             +-----------+------------+
             |           |            |
             v           v            v
         SQL/Python    Tableau     scikit-learn
         analytics     dashboard   predictive maintenance
```

## Architectural Layers

### 1. Database definition and master data

`database/schema.sql` defines 18 normalized PostgreSQL tables, their primary
and foreign keys, uniqueness rules, check constraints, and supporting indexes.

`database/seed.sql` inserts seven relatively stable master-data domains:

- customers
- suppliers
- materials
- products
- machines
- operators
- defect types

PostgreSQL is the source of truth for valid master records. Transactional
generators retrieve these records rather than duplicating them in Python.

### 2. Synthetic transactional ETL

`src/generate_data.py` orchestrates the transactional pipeline in dependency
order. Individual generator modules in `src/etl/` apply documented business
rules, and `src/etl/load.py` inserts the generated dictionaries with SQLAlchemy
Core.

```text
Extract PostgreSQL master/parent records
                    |
                    v
Generate related Python dictionaries
                    |
                    v
Validate generator assumptions and database constraints
                    |
                    v
Load transactional rows into PostgreSQL
```

The transactional sequence preserves referential integrity:

```text
customer orders -> order items -> production orders
       -> material lots and allocations -> production runs
       -> inspections -> defects -> downtime -> maintenance -> sensors
```

Loaders skip tables that already contain data. This makes routine reruns safe,
while a full reset remains explicit through `database/schema.sql`.

### 3. Integrated manufacturing model

The PostgreSQL schema combines operational domains that would normally come
from separate systems:

| Simulated source | Project data |
|---|---|
| ERP | customers, customer orders, products, suppliers, materials, material lots |
| MES | production orders, routings represented by production runs, machines, operators |
| QMS | quality inspections, quality defects, defect types |
| Maintenance system | downtime events, maintenance events |
| Industrial IoT | five-minute sensor readings |

The detailed table grain, columns, relationships, and constraints are defined
in `docs/data_model.md`. That document also serves as the project data
dictionary.

### 4. Analytics and KPI layer

`src/analytics/kpis.py` and `src/analytics/analysis.py` execute reusable SQL
queries and calculate manufacturing measures. The layer provides:

- plant and machine KPI summaries
- first-pass yield, scrap, and rework rates
- inspection pass rate
- product-family performance
- quality defects by machine, category, and severity
- downtime causes by frequency and duration
- monthly quality and downtime trends

Queries aggregate facts before combining them to avoid accidental row
multiplication. All historical reports apply the current timestamp as an
as-of boundary.

`src/analytics/export_dashboard_data.py` exports six reproducible CSV datasets
for Tableau. Generated extracts remain outside Git; the packaged Tableau
workbook contains its own local copies for portfolio viewing.

### 5. Visualization layer

The Tableau executive dashboard communicates plant KPIs, monthly trends, and
downtime performance to nontechnical stakeholders. A packaged `.twbx` workbook
and dashboard image are stored in the repository.

The predictive-maintenance results report compares candidate models, presents
the selected model's test results, explains its alert tradeoff, and summarizes
permutation feature importance.

### 6. Machine-learning layer

The predictive-maintenance proof of concept targets only cold-heading machines
CH-01 and CH-02. It predicts whether a mechanical failure will begin within 60
minutes.

`src/models/predictive_maintenance.py`:

- joins machine, sensor, downtime, and maintenance data
- removes readings recorded during downtime
- creates trailing 60-minute features without crossing downtime gaps
- labels future mechanical-failure windows
- performs chronological splits

`src/models/train_predictive_maintenance.py`:

- compares Logistic Regression, Random Forest, and Histogram Gradient Boosting
- selects probability thresholds on validation data
- selects the final model using validation average precision
- evaluates once on an untouched future test period
- calculates model-agnostic permutation importance

This separation keeps data preparation independently testable and prevents
future information from leaking into training.

### 7. Validation and testing

Quality is enforced at several levels:

| Layer | Controls |
|---|---|
| PostgreSQL | types, foreign keys, uniqueness, checks, indexes |
| Generators | valid master IDs, coherent statuses, quantities, and timestamps |
| Loaders | dependency order, transactions, idempotent table checks |
| Analytics | reusable formulas and aggregation before joins |
| Machine learning | downtime exclusion, chronological splits, untouched test period |
| Automated tests | generator rules, KPI math, labels, splits, and threshold selection |

## Configuration

`src/config.py` reads the optional `DATABASE_URL` environment variable. Without
an override, SQLAlchemy connects to a local PostgreSQL database named
`manufacturing_intelligence` using the current operating-system user.

No credentials or proprietary manufacturing data are stored in the
repository. All operational records are synthetic.

## Deliberate Scope Boundaries

This portfolio version is a batch-oriented local system, not a deployed
production service. It does not claim:

- true OEE, because the schema lacks planned machine-and-shift schedules
- real-time streaming ingestion
- production model serving or automated retraining
- statistically validated causal relationships
- production security, orchestration, or enterprise data governance

These boundaries are documented so the demonstrated results are technically
defensible. The architecture can be extended with scheduling data, workflow
orchestration, model monitoring, and governed cloud deployment if those become
project requirements.
