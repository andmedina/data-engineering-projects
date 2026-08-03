"""Calculate plant-level manufacturing KPIs from PostgreSQL."""

from sqlalchemy import create_engine, text


DATABASE_URL = (
    "postgresql+psycopg2://amed@localhost/"
    "manufacturing_intelligence"
)


KPI_SUMMARY_QUERY = text(
    """
    WITH run_totals AS (
        SELECT
            COALESCE(SUM(input_quantity), 0) AS input_quantity,
            COALESCE(SUM(good_quantity), 0) AS good_quantity,
            COALESCE(SUM(scrap_quantity), 0) AS scrap_quantity,
            COALESCE(SUM(rework_quantity), 0) AS rework_quantity
        FROM production_runs
        WHERE run_status = 'Completed'
          AND start_timestamp <= CURRENT_TIMESTAMP
    ),
    inspection_totals AS (
        SELECT
            COALESCE(SUM(sample_size), 0) AS inspected_quantity,
            COALESCE(SUM(passed_quantity), 0) AS passed_quantity,
            COALESCE(SUM(failed_quantity), 0) AS failed_quantity
        FROM quality_inspections
        WHERE inspection_result <> 'Pending'
          AND inspection_timestamp <= CURRENT_TIMESTAMP
    ),
    downtime_totals AS (
        SELECT
            COALESCE(SUM(downtime_minutes), 0) AS total_downtime_minutes,
            COALESCE(
                SUM(downtime_minutes) FILTER (WHERE planned_flag = FALSE),
                0
            ) AS unplanned_downtime_minutes
        FROM downtime_events
        WHERE downtime_start <= CURRENT_TIMESTAMP
    )
    SELECT
        r.input_quantity,
        r.good_quantity,
        r.scrap_quantity,
        r.rework_quantity,
        i.inspected_quantity,
        i.passed_quantity,
        i.failed_quantity,
        d.total_downtime_minutes,
        d.unplanned_downtime_minutes
    FROM run_totals r
    CROSS JOIN inspection_totals i
    CROSS JOIN downtime_totals d
    """
)


MACHINE_KPI_QUERY = text(
    """
    WITH run_totals AS (
        SELECT
            machine_id,
            COUNT(*) AS completed_runs,
            COALESCE(SUM(input_quantity), 0) AS input_quantity,
            COALESCE(SUM(good_quantity), 0) AS good_quantity,
            COALESCE(SUM(scrap_quantity), 0) AS scrap_quantity,
            COALESCE(SUM(rework_quantity), 0) AS rework_quantity
        FROM production_runs
        WHERE run_status = 'Completed'
          AND start_timestamp <= CURRENT_TIMESTAMP
        GROUP BY machine_id
    ),
    downtime_totals AS (
        SELECT
            machine_id,
            COALESCE(SUM(downtime_minutes), 0) AS downtime_minutes,
            COALESCE(
                SUM(downtime_minutes) FILTER (WHERE planned_flag = FALSE),
                0
            ) AS unplanned_downtime_minutes
        FROM downtime_events
        WHERE downtime_start <= CURRENT_TIMESTAMP
        GROUP BY machine_id
    )
    SELECT
        m.machine_code,
        m.machine_name,
        m.operation_type,
        COALESCE(r.completed_runs, 0) AS completed_runs,
        COALESCE(r.input_quantity, 0) AS input_quantity,
        COALESCE(r.good_quantity, 0) AS good_quantity,
        COALESCE(r.scrap_quantity, 0) AS scrap_quantity,
        COALESCE(r.rework_quantity, 0) AS rework_quantity,
        COALESCE(d.downtime_minutes, 0) AS downtime_minutes,
        COALESCE(d.unplanned_downtime_minutes, 0)
            AS unplanned_downtime_minutes
    FROM machines m
    LEFT JOIN run_totals r ON r.machine_id = m.machine_id
    LEFT JOIN downtime_totals d ON d.machine_id = m.machine_id
    ORDER BY m.machine_code
    """
)


def get_engine():
    """Create and return the SQLAlchemy database engine."""
    return create_engine(DATABASE_URL)


def calculate_percentage(numerator, denominator):
    """Return a percentage rounded to two decimals, or None for no data."""
    if denominator == 0:
        return None

    return round(numerator / denominator * 100, 2)


def get_kpi_summary(engine):
    """Return the current plant-level KPI summary."""
    with engine.connect() as connection:
        kpis = dict(connection.execute(KPI_SUMMARY_QUERY).mappings().one())

    kpis["first_pass_yield_pct"] = calculate_percentage(
        kpis["good_quantity"], kpis["input_quantity"]
    )
    kpis["scrap_rate_pct"] = calculate_percentage(
        kpis["scrap_quantity"], kpis["input_quantity"]
    )
    kpis["rework_rate_pct"] = calculate_percentage(
        kpis["rework_quantity"], kpis["input_quantity"]
    )
    kpis["inspection_pass_rate_pct"] = calculate_percentage(
        kpis["passed_quantity"], kpis["inspected_quantity"]
    )

    return kpis


def get_machine_kpis(engine):
    """Return production and downtime KPIs for each machine."""
    with engine.connect() as connection:
        machine_kpis = [
            dict(row)
            for row in connection.execute(MACHINE_KPI_QUERY).mappings()
        ]

    for machine in machine_kpis:
        machine["first_pass_yield_pct"] = calculate_percentage(
            machine["good_quantity"], machine["input_quantity"]
        )
        machine["scrap_rate_pct"] = calculate_percentage(
            machine["scrap_quantity"], machine["input_quantity"]
        )
        machine["rework_rate_pct"] = calculate_percentage(
            machine["rework_quantity"], machine["input_quantity"]
        )

    return machine_kpis


def format_percentage(value):
    """Format a percentage for display when data may be unavailable."""
    return "N/A" if value is None else f"{value}%"


def print_kpi_summary(kpis):
    """Print the KPI summary in a readable terminal format."""
    print("\nMANUFACTURING KPI SUMMARY")
    print("=" * 30)
    print(
        "First-pass yield:       "
        f"{format_percentage(kpis['first_pass_yield_pct'])}"
    )
    print(
        "Scrap rate:             "
        f"{format_percentage(kpis['scrap_rate_pct'])}"
    )
    print(
        "Rework rate:            "
        f"{format_percentage(kpis['rework_rate_pct'])}"
    )
    print(
        "Inspection pass rate:   "
        f"{format_percentage(kpis['inspection_pass_rate_pct'])}"
    )
    print(f"Total downtime:        {kpis['total_downtime_minutes']} minutes")
    print(
        "Unplanned downtime:    "
        f"{kpis['unplanned_downtime_minutes']} minutes"
    )
    print("\nSupporting quantities")
    print("-" * 30)
    print(f"Run input:             {kpis['input_quantity']}")
    print(f"Good:                  {kpis['good_quantity']}")
    print(f"Scrap:                 {kpis['scrap_quantity']}")
    print(f"Rework:                {kpis['rework_quantity']}")
    print(f"Inspected:             {kpis['inspected_quantity']}")
    print(f"Passed inspection:     {kpis['passed_quantity']}")
    print(f"Failed inspection:     {kpis['failed_quantity']}")


def print_machine_kpis(machine_kpis):
    """Print a compact comparison of machine-level KPIs."""
    print("\nMACHINE KPI COMPARISON")
    print("=" * 79)
    print(
        f"{'Machine':<10} {'Runs':>6} {'FPY':>9} {'Scrap':>9} "
        f"{'Rework':>9} {'Downtime':>11} {'Unplanned':>11}"
    )
    print("-" * 79)

    for machine in machine_kpis:
        print(
            f"{machine['machine_code']:<10} "
            f"{machine['completed_runs']:>6} "
            f"{format_percentage(machine['first_pass_yield_pct']):>9} "
            f"{format_percentage(machine['scrap_rate_pct']):>9} "
            f"{format_percentage(machine['rework_rate_pct']):>9} "
            f"{machine['downtime_minutes']:>11} "
            f"{machine['unplanned_downtime_minutes']:>11}"
        )


def print_product_family_kpis(rows):
    """Print production KPIs by product family."""
    print("\nPRODUCT FAMILY PERFORMANCE")
    print("=" * 75)
    print(
        f"{'Product family':<22} {'Runs':>6} {'FPY':>9} {'Scrap':>9} "
        f"{'Rework':>9} {'Avg cycle':>11}"
    )
    print("-" * 75)
    for row in rows:
        print(
            f"{row['product_family']:<22} {row['completed_runs']:>6} "
            f"{format_percentage(row['first_pass_yield_pct']):>9} "
            f"{format_percentage(row['scrap_rate_pct']):>9} "
            f"{format_percentage(row['rework_rate_pct']):>9} "
            f"{row['average_cycle_time_seconds']:>11}"
        )


def print_defect_analysis(rows, limit=10):
    """Print the largest machine/category defect combinations."""
    print("\nTOP QUALITY DEFECT CONCENTRATIONS")
    print("=" * 65)
    print(
        f"{'Machine':<10} {'Category':<15} {'Severity':<10} "
        f"{'Records':>8} {'Quantity':>10}"
    )
    print("-" * 65)
    for row in rows[:limit]:
        print(
            f"{row['machine_code']:<10} {row['defect_category']:<15} "
            f"{row['severity']:<10} {row['defect_records']:>8} "
            f"{row['defect_quantity']:>10}"
        )


def print_downtime_causes(rows):
    """Print downtime frequency and duration by cause."""
    print("\nDOWNTIME CAUSE ANALYSIS")
    print("=" * 73)
    print(
        f"{'Category':<25} {'Type':<11} {'Events':>8} "
        f"{'Minutes':>10} {'Avg event':>12}"
    )
    print("-" * 73)
    for row in rows:
        downtime_type = "Planned" if row["planned_flag"] else "Unplanned"
        print(
            f"{row['downtime_category']:<25} {downtime_type:<11} "
            f"{row['event_count']:>8} {row['downtime_minutes']:>10} "
            f"{row['average_event_minutes']:>12}"
        )


def print_monthly_trends(rows):
    """Print monthly quality and downtime trends."""
    print("\nMONTHLY KPI TRENDS")
    print("=" * 72)
    print(
        f"{'Month':<12} {'FPY':>9} {'Scrap':>9} {'Rework':>9} "
        f"{'Downtime':>11} {'Unplanned':>11}"
    )
    print("-" * 72)
    for row in rows:
        print(
            f"{str(row['month']):<12} "
            f"{format_percentage(row['first_pass_yield_pct']):>9} "
            f"{format_percentage(row['scrap_rate_pct']):>9} "
            f"{format_percentage(row['rework_rate_pct']):>9} "
            f"{row['downtime_minutes']:>11} "
            f"{row['unplanned_downtime_minutes']:>11}"
        )


def main():
    """Run and display the complete manufacturing analytics report."""
    from .analysis import (
        get_defect_analysis,
        get_downtime_causes,
        get_monthly_trends,
        get_product_family_kpis,
    )

    engine = get_engine()
    kpis = get_kpi_summary(engine)
    machine_kpis = get_machine_kpis(engine)
    print_kpi_summary(kpis)
    print_machine_kpis(machine_kpis)
    print_product_family_kpis(get_product_family_kpis(engine))
    print_defect_analysis(get_defect_analysis(engine))
    print_downtime_causes(get_downtime_causes(engine))
    print_monthly_trends(get_monthly_trends(engine))


if __name__ == "__main__":
    main()
