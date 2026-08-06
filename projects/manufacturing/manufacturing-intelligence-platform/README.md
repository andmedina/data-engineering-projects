# Manufacturing Intelligence Platform

![Aerospace fastener manufacturing intelligence platform](docs/images/manufacturing_intelligence_banner.png)

> **Status:** Complete portfolio implementation

## Overview

The Manufacturing Intelligence Platform is an end-to-end manufacturing analytics platform designed to simulate a modern aerospace manufacturing environment. The project integrates operational data from Enterprise Resource Planning (ERP), Manufacturing Execution Systems (MES), Quality Management Systems (QMS), Inventory Management, Maintenance Records, and Industrial IoT Sensors into a centralized PostgreSQL data warehouse.

Built using modern data engineering and analytics practices, the platform demonstrates how organizations can transform fragmented manufacturing data into actionable business intelligence, predictive insights, and operational decision support.

---

# Executive Dashboard

![Manufacturing Executive Dashboard](docs/images/manufacturing_executive_dashboard.png)

The Tableau executive dashboard summarizes plant-level first-pass yield, scrap,
rework, inspection performance, downtime, and monthly quality trends. The
packaged workbook includes its local analytics data extracts and can be
downloaded here:

[Download the Tableau packaged workbook](tableau/manufacturing_intelligence_dashboard.twbx)

---

# Business Problem

Modern manufacturing facilities generate large volumes of operational data across multiple business systems. ERP systems manage production orders and inventory, MES systems capture machine activity, quality systems record inspection results, maintenance systems track equipment servicing, and industrial sensors continuously monitor machine health.

Although each system provides valuable information independently, these data sources often exist in isolation. This fragmentation makes it difficult for engineers, production managers, and plant leadership to answer critical operational questions, including:

- Why did scrap increase yesterday?
- Which machines experience the most downtime?
- Which production lines consistently achieve the highest first-pass yield?
- Are equipment failures preceded by abnormal sensor readings?
- Which suppliers are associated with higher defect rates?
- Which additional scheduling data would be required to calculate defensible Overall Equipment Effectiveness (OEE)?

This project simulates the role of a Manufacturing Data Scientist responsible for designing a unified analytics platform that consolidates these operational systems into a centralized environment for reporting, root cause analysis, and predictive analytics.

---

# Project Goals

The primary objectives of this project are to:

- Design a normalized PostgreSQL database for manufacturing operations.
- Simulate realistic manufacturing datasets across multiple operational systems.
- Build automated ETL pipelines using Python.
- Calculate key manufacturing performance indicators (KPIs).
- Develop interactive operational dashboards.
- Perform root cause analysis of production losses.
- Build predictive machine learning models using manufacturing data.
- Demonstrate software engineering best practices through modular, maintainable code.

---

# Simulated Manufacturing Environment

This project models a vertically integrated aerospace fastener manufacturer responsible for producing high-precision fastening systems used within the aerospace industry.

The simulated manufacturing process includes:

```text
Raw Material
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

Each production order progresses through multiple manufacturing operations while generating production, quality, inventory, maintenance, and machine sensor data.

---

# Data Sources

The platform integrates simulated data from several manufacturing systems.

| System | Description |
|----------|-------------|
| ERP | Production orders, customers, inventory, suppliers |
| MES | Machine events, cycle times, production history, downtime |
| Quality Management | Inspections, defects, measurements, first-pass yield |
| Inventory | Material lots, finished goods, warehouse transactions |
| Maintenance | Preventive maintenance, repairs, equipment history |
| Industrial IoT | Temperature, vibration, power consumption, pressure, RPM |

---

# Project Architecture

```
ERP
MES
Quality
Inventory
Maintenance
IoT Sensors
        │
        ▼
Python ETL Pipelines
        │
        ▼
PostgreSQL Data Warehouse
        │
        ├──────────────► SQL Analytics
        ├──────────────► KPI Calculations
        ├──────────────► Tableau Dashboards
        └──────────────► Machine Learning Models
```

See [`docs/architecture.md`](docs/architecture.md) for the component design,
source-system mappings, data flow, validation layers, and scope boundaries.

---

# Manufacturing Analytics

The implemented analytics layer reports:

- First-pass yield (FPY)
- Scrap and rework rates
- Inspection pass rate
- Total and unplanned downtime
- Machine and product-family performance
- Average production cycle time
- Defect concentrations by machine, category, and severity
- Downtime causes by frequency and duration
- Monthly quality and downtime trends

Run the manufacturing analytics report:

```bash
python -m src.analytics.kpis
```

Export dashboard-ready CSV datasets:

```bash
python -m src.analytics.export_dashboard_data
```

See [`docs/analytics_kpi_guide.md`](docs/analytics_kpi_guide.md) for each
report's business questions, source tables, join path, grain, formulas, and
limitations.

True OEE is deliberately excluded because the current schema does not contain
planned production time at the machine-and-shift grain. The analytics guide
documents the missing data rather than presenting a misleading approximation.

---

# Technology Stack

## Data Engineering

- Python
- SQL
- PostgreSQL
- Pandas

## Analytics

- Tableau
- Statistical Analysis
- Manufacturing KPIs

## Machine Learning

- Scikit-learn
- Predictive Analytics

The predictive-maintenance proof of concept focuses on cold-heading machines
and predicts mechanical-failure risk within the next 60 minutes. See
[`docs/ml_predictive_maintenance.md`](docs/ml_predictive_maintenance.md) for
the population, features, target, evaluation design, and limitations.

![Predictive maintenance model results](docs/images/predictive_maintenance_results.png)

---

# Reproduce the Project

## Prerequisites

- Python 3.11 or a compatible recent Python version
- PostgreSQL
- Conda or another Python environment manager
- Tableau is optional and needed only to edit the packaged workbook

From the project directory, create and activate an environment:

```bash
conda create -n data_engineering python=3.11
conda activate data_engineering
pip install -r requirements.txt
```

Create and populate the PostgreSQL database:

```bash
createdb manufacturing_intelligence
psql -d manufacturing_intelligence -f database/schema.sql
psql -d manufacturing_intelligence -f database/seed.sql
python -m src.generate_data
```

The default connection uses the current operating-system user. For a different
PostgreSQL connection, set `DATABASE_URL` before running Python:

```bash
export DATABASE_URL="postgresql+psycopg2://user:password@localhost/manufacturing_intelligence"
```

`database/schema.sql` drops and recreates all project tables, so it is the
explicit clean-reset command. The transactional loaders otherwise skip tables
that already contain data.

## Demonstration Commands

Run these commands from the repository root:

```bash
# Validate Python business logic and calculations
python -m pytest -q

# Display manufacturing KPIs and detailed analyses
python -m src.analytics.kpis

# Recreate the six Tableau-ready CSV extracts
python -m src.analytics.export_dashboard_data

# Compare three models and evaluate the selected model
python -m src.models.train_predictive_maintenance

# Recreate the portfolio ML results image from the recorded experiment
python -m src.models.create_ml_results_report
```

---

# Project Roadmap

## Phase 1 — Data Architecture

- [x] Design Entity Relationship Diagram (ERD)
- [x] Design relational database schema
- [x] Define manufacturing entities and relationships

## Phase 2 — Data Generation

- [x] Generate realistic manufacturing datasets
- [x] Simulate ERP data
- [x] Simulate MES data
- [x] Simulate Quality data
- [x] Simulate Sensor data

## Phase 3 — ETL Pipeline

- [x] Extract
- [x] Transform
- [x] Load into PostgreSQL

## Phase 4 — Analytics

- [x] Manufacturing KPIs
- [x] Downtime and quality-defect cause analysis
- [x] Executive Dashboard

## Phase 5 — Machine Learning

- [x] Predictive maintenance proof of concept

## Future Enhancements

- Machine-and-shift schedules for true OEE and capacity utilization
- Model serving, drift monitoring, and automated retraining
- Defect prediction and downtime forecasting
- Production orchestration with a tool such as dbt and a workflow scheduler
- AI-powered manufacturing assistant using Large Language Models (LLMs)
- Real-time streaming data integration

---

# Repository Structure

```
manufacturing-intelligence-platform/
├── database/              # PostgreSQL schema and master-data seed
├── docs/                  # Architecture, data model, rules, and guides
├── outputs/analytics/     # Reproducible dashboard CSV extracts (Git-ignored)
├── src/
│   ├── analytics/         # KPI queries, reports, and exports
│   ├── etl/               # Transaction generators and loaders
│   ├── models/            # ML preparation, training, and result reporting
│   ├── config.py          # Shared database configuration
│   └── generate_data.py   # Transactional ETL entry point
├── tableau/               # Packaged executive dashboard
├── tests/                 # Automated data-quality, KPI, and ML tests
├── requirements.txt
└── README.md
```

---

# License

This project is intended for educational and portfolio purposes.
