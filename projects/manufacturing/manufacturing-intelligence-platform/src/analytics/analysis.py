"""Detailed manufacturing analyses used by reports and dashboards."""

from sqlalchemy import text

from .kpis import calculate_percentage


PRODUCT_FAMILY_QUERY = text(
    """
    SELECT
        p.product_family,
        COUNT(DISTINCT po.production_order_id) AS production_orders,
        COUNT(pr.production_run_id) AS completed_runs,
        COALESCE(SUM(pr.input_quantity), 0) AS input_quantity,
        COALESCE(SUM(pr.good_quantity), 0) AS good_quantity,
        COALESCE(SUM(pr.scrap_quantity), 0) AS scrap_quantity,
        COALESCE(SUM(pr.rework_quantity), 0) AS rework_quantity,
        ROUND(AVG(pr.actual_cycle_time_seconds), 2)
            AS average_cycle_time_seconds
    FROM products p
    JOIN customer_order_items coi ON coi.product_id = p.product_id
    JOIN production_orders po
        ON po.customer_order_item_id = coi.customer_order_item_id
    JOIN production_runs pr
        ON pr.production_order_id = po.production_order_id
    WHERE pr.run_status = 'Completed'
    GROUP BY p.product_family
    ORDER BY p.product_family
    """
)


DEFECT_ANALYSIS_QUERY = text(
    """
    SELECT
        m.machine_code,
        dt.defect_category,
        dt.severity,
        COUNT(qd.quality_defect_id) AS defect_records,
        COALESCE(SUM(qd.defect_quantity), 0) AS defect_quantity
    FROM quality_defects qd
    JOIN defect_types dt ON dt.defect_type_id = qd.defect_type_id
    JOIN quality_inspections qi ON qi.inspection_id = qd.inspection_id
    JOIN production_runs pr ON pr.production_run_id = qi.production_run_id
    JOIN machines m ON m.machine_id = pr.machine_id
    GROUP BY m.machine_code, dt.defect_category, dt.severity
    ORDER BY defect_quantity DESC, m.machine_code, dt.defect_category
    """
)


DOWNTIME_CAUSE_QUERY = text(
    """
    SELECT
        downtime_category,
        planned_flag,
        COUNT(*) AS event_count,
        COALESCE(SUM(downtime_minutes), 0) AS downtime_minutes,
        ROUND(AVG(downtime_minutes), 2) AS average_event_minutes
    FROM downtime_events
    GROUP BY downtime_category, planned_flag
    ORDER BY downtime_minutes DESC, downtime_category
    """
)


MONTHLY_TREND_QUERY = text(
    """
    WITH run_months AS (
        SELECT
            DATE_TRUNC('month', start_timestamp)::date AS month,
            COALESCE(SUM(input_quantity), 0) AS input_quantity,
            COALESCE(SUM(good_quantity), 0) AS good_quantity,
            COALESCE(SUM(scrap_quantity), 0) AS scrap_quantity,
            COALESCE(SUM(rework_quantity), 0) AS rework_quantity
        FROM production_runs
        WHERE run_status = 'Completed'
          AND start_timestamp IS NOT NULL
        GROUP BY DATE_TRUNC('month', start_timestamp)::date
    ),
    downtime_months AS (
        SELECT
            DATE_TRUNC('month', downtime_start)::date AS month,
            COALESCE(SUM(downtime_minutes), 0) AS downtime_minutes,
            COALESCE(
                SUM(downtime_minutes) FILTER (WHERE planned_flag = FALSE),
                0
            ) AS unplanned_downtime_minutes
        FROM downtime_events
        GROUP BY DATE_TRUNC('month', downtime_start)::date
    )
    SELECT
        COALESCE(r.month, d.month) AS month,
        COALESCE(r.input_quantity, 0) AS input_quantity,
        COALESCE(r.good_quantity, 0) AS good_quantity,
        COALESCE(r.scrap_quantity, 0) AS scrap_quantity,
        COALESCE(r.rework_quantity, 0) AS rework_quantity,
        COALESCE(d.downtime_minutes, 0) AS downtime_minutes,
        COALESCE(d.unplanned_downtime_minutes, 0)
            AS unplanned_downtime_minutes
    FROM run_months r
    FULL OUTER JOIN downtime_months d ON d.month = r.month
    ORDER BY month
    """
)


def execute_query(engine, query):
    """Execute a read-only analytics query and return dictionaries."""
    with engine.connect() as connection:
        return [dict(row) for row in connection.execute(query).mappings()]


def add_production_rates(rows):
    """Add FPY, scrap, and rework percentages to aggregate rows."""
    for row in rows:
        row["first_pass_yield_pct"] = calculate_percentage(
            row["good_quantity"], row["input_quantity"]
        )
        row["scrap_rate_pct"] = calculate_percentage(
            row["scrap_quantity"], row["input_quantity"]
        )
        row["rework_rate_pct"] = calculate_percentage(
            row["rework_quantity"], row["input_quantity"]
        )
    return rows


def get_product_family_kpis(engine):
    """Return production KPIs grouped by product family."""
    return add_production_rates(execute_query(engine, PRODUCT_FAMILY_QUERY))


def get_defect_analysis(engine):
    """Return defect quantities grouped by machine, category, and severity."""
    return execute_query(engine, DEFECT_ANALYSIS_QUERY)


def get_downtime_causes(engine):
    """Return downtime frequency and duration grouped by cause."""
    return execute_query(engine, DOWNTIME_CAUSE_QUERY)


def get_monthly_trends(engine):
    """Return monthly production-quality and downtime trends."""
    return add_production_rates(execute_query(engine, MONTHLY_TREND_QUERY))
