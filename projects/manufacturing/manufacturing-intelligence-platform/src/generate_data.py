from datetime import date, datetime, time, timedelta, timezone

from sqlalchemy import create_engine, text

from .etl.generate_customer_order_items import generate_customer_order_items
from .etl.generate_customer_orders import generate_customer_orders
from .etl.generate_material_lots import generate_material_lots
from .etl.generate_production_order_materials import (
    generate_production_order_materials,
)
from .etl.generate_production_orders import generate_production_orders
from .etl.generate_production_runs import generate_production_runs
from .etl.generate_quality_inspections import generate_quality_inspections
from .etl.generate_quality_defects import generate_quality_defects
from .etl.generate_downtime_events import generate_downtime_events
from .etl.generate_maintenance_events import generate_maintenance_events
from .etl.generate_sensor_readings import generate_sensor_readings
from .etl.load import (
    load_customer_order_items,
    load_customer_orders,
    load_material_lots,
    load_production_order_materials,
    load_production_orders,
    load_production_runs,
    load_quality_inspections,
    load_quality_defects,
    load_downtime_events,
    load_maintenance_events,
    load_sensor_readings,
)


DATABASE_URL = (
    "postgresql+psycopg2://amed@localhost/"
    "manufacturing_intelligence"
)


def get_engine():
    """Create and return the SQLAlchemy database engine."""
    return create_engine(DATABASE_URL)


def get_customer_ids(engine):
    """Retrieve valid customer IDs from PostgreSQL."""

    query = text(
        """
        SELECT customer_id
        FROM customers
        ORDER BY customer_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)
        return [row.customer_id for row in result]


def get_products(engine):
    """Retrieve product information from PostgreSQL."""

    query = text(
        """
        SELECT
            product_id,
            product_name,
            product_family,
            standard_unit_cost
        FROM products
        WHERE active_flag = TRUE
        ORDER BY product_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)

        return [
            {
                "product_id": row.product_id,
                "product_name": row.product_name,
                "product_family": row.product_family,
                "standard_unit_cost": row.standard_unit_cost,
            }
            for row in result
        ]


def get_customer_order_count(engine):
    """Return the number of customer orders in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM customer_orders
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_customer_orders(engine):
    """Retrieve customer orders needed to generate their line items."""

    query = text(
        """
        SELECT customer_order_id, order_status
        FROM customer_orders
        ORDER BY customer_order_id
        """
    )

    with engine.connect() as connection:
        result = connection.execute(query)
        return [
            {
                "customer_order_id": row.customer_order_id,
                "order_status": row.order_status,
            }
            for row in result
        ]


def get_customer_order_item_count(engine):
    """Return the number of customer order items in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM customer_order_items
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_customer_order_items_for_production(engine):
    """Retrieve order lines and header data needed for work-order generation."""

    query = text(
        """
        SELECT
            coi.customer_order_item_id,
            coi.ordered_quantity,
            coi.line_status,
            p.product_family,
            co.order_date,
            co.requested_delivery_date
        FROM customer_order_items AS coi
        JOIN products AS p
            ON p.product_id = coi.product_id
        JOIN customer_orders AS co
            ON co.customer_order_id = coi.customer_order_id
        ORDER BY coi.customer_order_item_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_machines_by_operation(engine):
    """Retrieve available machine IDs grouped by manufacturing operation."""

    query = text(
        """
        SELECT machine_id, operation_type
        FROM machines
        WHERE status IN ('Active', 'Idle')
        ORDER BY machine_id
        """
    )

    machines_by_operation = {}

    with engine.connect() as connection:
        for row in connection.execute(query):
            machines_by_operation.setdefault(row.operation_type, []).append(
                row.machine_id
            )

    return machines_by_operation


def get_production_order_count(engine):
    """Return the number of production orders in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM production_orders
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_active_materials(engine):
    """Retrieve active materials needed for supplier-lot generation."""

    query = text(
        """
        SELECT material_id, material_form, unit_of_measure
        FROM materials
        WHERE active_flag = TRUE
        ORDER BY material_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_raw_material_suppliers(engine):
    """Retrieve active raw-material suppliers from PostgreSQL."""

    query = text(
        """
        SELECT supplier_id, quality_rating
        FROM suppliers
        WHERE supplier_category = 'Raw Material'
          AND active_flag = TRUE
          AND approved_status IN ('Approved', 'Conditional')
        ORDER BY supplier_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_material_lot_date_range(engine):
    """Return the simulated operating period for material receipts."""

    query = text(
        """
        SELECT
            MIN(order_date) AS earliest_order_date,
            MAX(requested_delivery_date) AS latest_delivery_date
        FROM customer_orders
        """
    )

    with engine.connect() as connection:
        row = connection.execute(query).one()

    if row.earliest_order_date is None:
        raise ValueError("Customer orders are required before material lots.")

    start_date = row.earliest_order_date - timedelta(days=120)
    end_date = min(row.latest_delivery_date, date.today())
    return start_date, end_date


def get_material_lot_count(engine):
    """Return the number of material lots in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM material_lots
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_production_orders_for_material_allocation(engine):
    """Retrieve work orders and product details needed for material allocation."""

    query = text(
        """
        SELECT
            po.production_order_id,
            po.production_order_number,
            po.planned_quantity,
            po.production_status,
            po.scheduled_start_date,
            p.material_id,
            p.product_family,
            p.diameter_in,
            p.length_in,
            m.material_category
        FROM production_orders AS po
        JOIN customer_order_items AS coi
            ON coi.customer_order_item_id = po.customer_order_item_id
        JOIN products AS p
            ON p.product_id = coi.product_id
        JOIN materials AS m
            ON m.material_id = p.material_id
        ORDER BY po.production_order_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_material_lots_for_allocation(engine):
    """Retrieve material-lot balances needed for FIFO allocation."""

    query = text(
        """
        SELECT
            material_lot_id,
            material_id,
            received_date,
            quantity_received,
            quantity_available,
            lot_status
        FROM material_lots
        ORDER BY received_date, material_lot_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_production_order_material_count(engine):
    """Return the number of production-order material allocations."""

    query = text(
        """
        SELECT COUNT(*)
        FROM production_order_materials
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_production_orders_for_runs(engine):
    """Retrieve production orders and products needed for routed runs."""

    query = text(
        """
        SELECT
            po.production_order_id,
            po.production_status,
            po.actual_start_timestamp,
            po.actual_end_timestamp,
            po.completed_quantity,
            po.scrapped_quantity,
            p.product_family,
            p.standard_cycle_time_seconds
        FROM production_orders AS po
        JOIN customer_order_items AS coi
            ON coi.customer_order_item_id = po.customer_order_item_id
        JOIN products AS p
            ON p.product_id = coi.product_id
        ORDER BY po.production_order_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_operators_by_role(engine):
    """Retrieve active, currently certified employees grouped by role."""

    query = text(
        """
        SELECT operator_id, role_type
        FROM operators
        WHERE active_flag = TRUE
          AND certification_status = 'Current'
          AND role_type IN ('Operator', 'Inspector')
        ORDER BY operator_id
        """
    )

    operators_by_role = {}

    with engine.connect() as connection:
        for row in connection.execute(query):
            operators_by_role.setdefault(row.role_type, []).append(
                row.operator_id
            )

    return operators_by_role


def get_production_run_count(engine):
    """Return the number of production runs in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM production_runs
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_production_runs_for_inspection(engine):
    """Retrieve production runs and product data needed for inspections."""

    query = text(
        """
        SELECT
            pr.production_run_id,
            pr.operation_type,
            pr.start_timestamp,
            pr.end_timestamp,
            pr.input_quantity,
            pr.good_quantity,
            pr.scrap_quantity,
            pr.rework_quantity,
            pr.run_status,
            p.diameter_in,
            p.length_in,
            m.material_category
        FROM production_runs AS pr
        JOIN production_orders AS po
            ON po.production_order_id = pr.production_order_id
        JOIN customer_order_items AS coi
            ON coi.customer_order_item_id = po.customer_order_item_id
        JOIN products AS p
            ON p.product_id = coi.product_id
        JOIN materials AS m
            ON m.material_id = p.material_id
        ORDER BY pr.production_run_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_certified_inspector_ids(engine):
    """Retrieve active, currently certified quality inspectors."""

    query = text(
        """
        SELECT operator_id
        FROM operators
        WHERE active_flag = TRUE
          AND certification_status = 'Current'
          AND role_type = 'Inspector'
        ORDER BY operator_id
        """
    )

    with engine.connect() as connection:
        return [row.operator_id for row in connection.execute(query)]


def get_quality_inspection_count(engine):
    """Return the number of quality inspections in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM quality_inspections
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_inspections_for_defect_generation(engine):
    """Retrieve inspection failures needed for defect generation."""

    query = text(
        """
        SELECT
            inspection_id,
            failed_quantity,
            inspection_result,
            measurement_type
        FROM quality_inspections
        ORDER BY inspection_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_active_defect_types(engine):
    """Retrieve active standardized defect classifications."""

    query = text(
        """
        SELECT
            defect_type_id,
            defect_code,
            defect_category,
            severity
        FROM defect_types
        WHERE active_flag = TRUE
        ORDER BY defect_type_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_quality_defect_count(engine):
    """Return the number of quality defects in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM quality_defects
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_production_runs_for_downtime(engine):
    """Retrieve executed production runs needed for downtime generation."""

    query = text(
        """
        SELECT
            production_run_id,
            machine_id,
            operation_type,
            start_timestamp,
            end_timestamp,
            run_status
        FROM production_runs
        WHERE run_status IN ('Completed', 'Running')
        ORDER BY production_run_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_available_machines(engine):
    """Retrieve active or idle machines for standalone downtime events."""

    query = text(
        """
        SELECT machine_id
        FROM machines
        WHERE status IN ('Active', 'Idle')
        ORDER BY machine_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_downtime_date_range(engine):
    """Return the operating date range represented by production runs."""

    query = text(
        """
        SELECT
            MIN(start_timestamp)::date AS start_date,
            MAX(COALESCE(end_timestamp, start_timestamp))::date AS end_date
        FROM production_runs
        WHERE start_timestamp IS NOT NULL
        """
    )

    with engine.connect() as connection:
        row = connection.execute(query).one()

    if row.start_date is None:
        raise ValueError("Executed production runs are required for downtime.")

    return row.start_date, row.end_date


def get_downtime_event_count(engine):
    """Return the number of downtime events in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM downtime_events
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_downtime_events_for_maintenance(engine):
    """Retrieve downtime events that may require maintenance activity."""

    query = text(
        """
        SELECT
            machine_id,
            downtime_start,
            downtime_end,
            downtime_category
        FROM downtime_events
        ORDER BY downtime_start, downtime_event_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_machines_for_maintenance(engine):
    """Retrieve machine details needed for maintenance generation."""

    query = text(
        """
        SELECT machine_id, operation_type, install_date
        FROM machines
        WHERE status IN ('Active', 'Idle')
        ORDER BY machine_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_certified_technician_names(engine):
    """Retrieve active, currently certified maintenance technicians."""

    query = text(
        """
        SELECT operator_name
        FROM operators
        WHERE active_flag = TRUE
          AND certification_status = 'Current'
          AND role_type = 'Technician'
        ORDER BY operator_id
        """
    )

    with engine.connect() as connection:
        return [row.operator_name for row in connection.execute(query)]


def get_maintenance_event_count(engine):
    """Return the number of maintenance events in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM maintenance_events
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def get_machines_for_sensor_readings(engine):
    """Retrieve machines and operation types needed for telemetry generation."""

    query = text(
        """
        SELECT machine_id, operation_type, status
        FROM machines
        WHERE status IN ('Active', 'Idle')
        ORDER BY machine_id
        """
    )

    with engine.connect() as connection:
        return [dict(row._mapping) for row in connection.execute(query)]


def get_recent_downtime_events(engine, start_timestamp, end_timestamp):
    """Retrieve downtime events that overlap the telemetry window."""

    query = text(
        """
        SELECT
            d.machine_id,
            d.downtime_start,
            d.downtime_end,
            d.downtime_category,
            me.failure_component
        FROM downtime_events d
        LEFT JOIN maintenance_events me
            ON me.machine_id = d.machine_id
           AND me.maintenance_type = 'Corrective'
           AND me.maintenance_start = d.downtime_start
        WHERE d.downtime_end >= :start_timestamp
          AND d.downtime_start <= :end_timestamp
        ORDER BY d.machine_id, d.downtime_start
        """
    )

    with engine.connect() as connection:
        result = connection.execute(
            query,
            {
                "start_timestamp": start_timestamp,
                "end_timestamp": end_timestamp,
            },
        )
        return [dict(row._mapping) for row in result]


def get_sensor_reading_count(engine):
    """Return the number of machine sensor readings in PostgreSQL."""

    query = text(
        """
        SELECT COUNT(*)
        FROM sensor_readings
        """
    )

    with engine.connect() as connection:
        return connection.execute(query).scalar_one()


def main():
    engine = get_engine()

    customer_ids = get_customer_ids(engine)
    products = get_products(engine)

    print("Database connected successfully.")
    print(f"Customer rows found: {len(customer_ids)}")
    print(f"Product rows found: {len(products)}")

    existing_order_count = get_customer_order_count(engine)

    if existing_order_count == 0:
        customer_orders = generate_customer_orders(
            customer_ids=customer_ids,
            num_orders=500,
        )

        print(f"Generated {len(customer_orders)} customer orders.")

        load_customer_orders(
            engine=engine,
            customer_orders=customer_orders,
        )
    else:
        print(
            "Customer orders already exist. "
            "Skipping customer order generation."
        )

    stored_count = get_customer_order_count(engine)

    print(f"Customer orders stored in PostgreSQL: {stored_count}")

    existing_item_count = get_customer_order_item_count(engine)

    if existing_item_count == 0:
        customer_orders = get_customer_orders(engine)
        customer_order_items = generate_customer_order_items(
            customer_orders=customer_orders,
            products=products,
        )

        print(f"Generated {len(customer_order_items)} customer order items.")

        load_customer_order_items(
            engine=engine,
            customer_order_items=customer_order_items,
        )
    else:
        print(
            "Customer order items already exist. "
            "Skipping customer order item generation."
        )

    stored_item_count = get_customer_order_item_count(engine)
    print(f"Customer order items stored in PostgreSQL: {stored_item_count}")

    existing_production_order_count = get_production_order_count(engine)

    if existing_production_order_count == 0:
        customer_order_items = get_customer_order_items_for_production(engine)
        machines_by_operation = get_machines_by_operation(engine)
        production_orders = generate_production_orders(
            customer_order_items=customer_order_items,
            machines_by_operation=machines_by_operation,
        )

        print(f"Generated {len(production_orders)} production orders.")

        load_production_orders(
            engine=engine,
            production_orders=production_orders,
        )
    else:
        print(
            "Production orders already exist. "
            "Skipping production order generation."
        )

    stored_production_order_count = get_production_order_count(engine)
    print(
        "Production orders stored in PostgreSQL: "
        f"{stored_production_order_count}"
    )

    existing_material_lot_count = get_material_lot_count(engine)

    if existing_material_lot_count == 0:
        materials = get_active_materials(engine)
        suppliers = get_raw_material_suppliers(engine)
        production_orders = get_production_orders_for_material_allocation(
            engine
        )
        start_date, end_date = get_material_lot_date_range(engine)
        material_lots = generate_material_lots(
            materials=materials,
            suppliers=suppliers,
            production_orders=production_orders,
            start_date=start_date,
            end_date=end_date,
        )

        print(f"Generated {len(material_lots)} material lots.")

        load_material_lots(
            engine=engine,
            material_lots=material_lots,
        )
    else:
        print(
            "Material lots already exist. "
            "Skipping material lot generation."
        )

    stored_material_lot_count = get_material_lot_count(engine)
    print(f"Material lots stored in PostgreSQL: {stored_material_lot_count}")

    existing_allocation_count = get_production_order_material_count(engine)

    if existing_allocation_count == 0:
        production_orders = get_production_orders_for_material_allocation(
            engine
        )
        material_lots = get_material_lots_for_allocation(engine)
        production_order_materials, updated_material_lots = (
            generate_production_order_materials(
                production_orders=production_orders,
                material_lots=material_lots,
            )
        )

        print(
            "Generated "
            f"{len(production_order_materials)} material allocations."
        )

        load_production_order_materials(
            engine=engine,
            production_order_materials=production_order_materials,
            updated_material_lots=updated_material_lots,
        )
    else:
        print(
            "Production order materials already exist. "
            "Skipping material allocation generation."
        )

    stored_allocation_count = get_production_order_material_count(engine)
    print(
        "Production order material allocations stored in PostgreSQL: "
        f"{stored_allocation_count}"
    )

    existing_production_run_count = get_production_run_count(engine)

    if existing_production_run_count == 0:
        production_orders = get_production_orders_for_runs(engine)
        machines_by_operation = get_machines_by_operation(engine)
        operators_by_role = get_operators_by_role(engine)
        production_runs = generate_production_runs(
            production_orders=production_orders,
            machines_by_operation=machines_by_operation,
            operators_by_role=operators_by_role,
        )

        print(f"Generated {len(production_runs)} production runs.")

        load_production_runs(
            engine=engine,
            production_runs=production_runs,
        )
    else:
        print(
            "Production runs already exist. "
            "Skipping production run generation."
        )

    stored_production_run_count = get_production_run_count(engine)
    print(
        "Production runs stored in PostgreSQL: "
        f"{stored_production_run_count}"
    )

    existing_inspection_count = get_quality_inspection_count(engine)

    if existing_inspection_count == 0:
        production_runs = get_production_runs_for_inspection(engine)
        inspector_ids = get_certified_inspector_ids(engine)
        quality_inspections = generate_quality_inspections(
            production_runs=production_runs,
            inspector_ids=inspector_ids,
        )

        print(f"Generated {len(quality_inspections)} quality inspections.")

        load_quality_inspections(
            engine=engine,
            quality_inspections=quality_inspections,
        )
    else:
        print(
            "Quality inspections already exist. "
            "Skipping quality inspection generation."
        )

    stored_inspection_count = get_quality_inspection_count(engine)
    print(
        "Quality inspections stored in PostgreSQL: "
        f"{stored_inspection_count}"
    )

    existing_defect_count = get_quality_defect_count(engine)

    if existing_defect_count == 0:
        quality_inspections = get_inspections_for_defect_generation(engine)
        defect_types = get_active_defect_types(engine)
        quality_defects = generate_quality_defects(
            quality_inspections=quality_inspections,
            defect_types=defect_types,
        )

        print(f"Generated {len(quality_defects)} quality defects.")

        load_quality_defects(
            engine=engine,
            quality_defects=quality_defects,
        )
    else:
        print(
            "Quality defects already exist. "
            "Skipping quality defect generation."
        )

    stored_defect_count = get_quality_defect_count(engine)
    print(f"Quality defects stored in PostgreSQL: {stored_defect_count}")

    existing_downtime_count = get_downtime_event_count(engine)

    if existing_downtime_count == 0:
        production_runs = get_production_runs_for_downtime(engine)
        machines = get_available_machines(engine)
        start_date, end_date = get_downtime_date_range(engine)
        downtime_events = generate_downtime_events(
            production_runs=production_runs,
            machines=machines,
            start_date=start_date,
            end_date=end_date,
        )

        print(f"Generated {len(downtime_events)} downtime events.")

        load_downtime_events(
            engine=engine,
            downtime_events=downtime_events,
        )
    else:
        print(
            "Downtime events already exist. "
            "Skipping downtime event generation."
        )

    stored_downtime_count = get_downtime_event_count(engine)
    print(f"Downtime events stored in PostgreSQL: {stored_downtime_count}")

    existing_maintenance_count = get_maintenance_event_count(engine)

    if existing_maintenance_count == 0:
        downtime_events = get_downtime_events_for_maintenance(engine)
        machines = get_machines_for_maintenance(engine)
        technician_names = get_certified_technician_names(engine)
        start_date, end_date = get_downtime_date_range(engine)
        maintenance_events = generate_maintenance_events(
            downtime_events=downtime_events,
            machines=machines,
            technician_names=technician_names,
            start_date=start_date,
            end_date=end_date,
        )

        print(f"Generated {len(maintenance_events)} maintenance events.")

        load_maintenance_events(
            engine=engine,
            maintenance_events=maintenance_events,
        )
    else:
        print(
            "Maintenance events already exist. "
            "Skipping maintenance event generation."
        )

    stored_maintenance_count = get_maintenance_event_count(engine)
    print(
        "Maintenance events stored in PostgreSQL: "
        f"{stored_maintenance_count}"
    )

    existing_sensor_count = get_sensor_reading_count(engine)

    if existing_sensor_count == 0:
        current_timestamp = datetime.now(timezone.utc)
        end_timestamp = current_timestamp.replace(
            minute=(current_timestamp.minute // 5) * 5,
            second=0,
            microsecond=0,
        )
        machines = get_machines_for_sensor_readings(engine)
        cold_heading_machines = [
            machine
            for machine in machines
            if machine["operation_type"] == "Cold Heading"
        ]
        other_machines = [
            machine
            for machine in machines
            if machine["operation_type"] != "Cold Heading"
        ]
        cold_heading_start = end_timestamp - timedelta(days=365)
        other_machine_start = end_timestamp - timedelta(days=30)
        downtime_events = get_recent_downtime_events(
            engine,
            cold_heading_start,
            end_timestamp,
        )
        cold_heading_readings = generate_sensor_readings(
            machines=cold_heading_machines,
            downtime_events=downtime_events,
            start_timestamp=cold_heading_start,
            end_timestamp=end_timestamp,
        )
        other_machine_readings = generate_sensor_readings(
            machines=other_machines,
            downtime_events=downtime_events,
            start_timestamp=other_machine_start,
            end_timestamp=end_timestamp,
        )
        sensor_readings = cold_heading_readings + other_machine_readings

        print(f"Generated {len(sensor_readings)} sensor readings.")

        load_sensor_readings(
            engine=engine,
            sensor_readings=sensor_readings,
        )
    else:
        print(
            "Sensor readings already exist. "
            "Skipping sensor reading generation."
        )

    stored_sensor_count = get_sensor_reading_count(engine)
    print(f"Sensor readings stored in PostgreSQL: {stored_sensor_count}")


if __name__ == "__main__":
    main()
