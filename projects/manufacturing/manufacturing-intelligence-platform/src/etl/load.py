from sqlalchemy import text


def load_customer_orders(engine, customer_orders):
    """Insert generated customer orders into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM customer_orders
        """
    )

    insert_query = text(
        """
        INSERT INTO customer_orders (
            customer_order_number,
            customer_id,
            order_date,
            requested_delivery_date,
            priority,
            order_status,
            notes
        )
        VALUES (
            :customer_order_number,
            :customer_id,
            :order_date,
            :requested_delivery_date,
            :priority,
            :order_status,
            :notes
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "customer_orders already contains data. "
                "The load was stopped to prevent duplicate order numbers."
            )

        connection.execute(insert_query, customer_orders)

    print(f"Loaded {len(customer_orders)} customer orders into PostgreSQL.")


def load_customer_order_items(engine, customer_order_items):
    """Insert generated customer order items into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM customer_order_items
        """
    )

    insert_query = text(
        """
        INSERT INTO customer_order_items (
            customer_order_id,
            line_number,
            product_id,
            ordered_quantity,
            unit_price,
            line_status
        )
        VALUES (
            :customer_order_id,
            :line_number,
            :product_id,
            :ordered_quantity,
            :unit_price,
            :line_status
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "customer_order_items already contains data. "
                "The load was stopped to prevent duplicate order lines."
            )

        connection.execute(insert_query, customer_order_items)

    print(
        f"Loaded {len(customer_order_items)} customer order items "
        "into PostgreSQL."
    )


def load_production_orders(engine, production_orders):
    """Insert generated production orders into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM production_orders
        """
    )

    insert_query = text(
        """
        INSERT INTO production_orders (
            production_order_number,
            customer_order_item_id,
            machine_id,
            scheduled_start_date,
            scheduled_end_date,
            actual_start_timestamp,
            actual_end_timestamp,
            planned_quantity,
            completed_quantity,
            scrapped_quantity,
            production_status
        )
        VALUES (
            :production_order_number,
            :customer_order_item_id,
            :machine_id,
            :scheduled_start_date,
            :scheduled_end_date,
            :actual_start_timestamp,
            :actual_end_timestamp,
            :planned_quantity,
            :completed_quantity,
            :scrapped_quantity,
            :production_status
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "production_orders already contains data. "
                "The load was stopped to prevent duplicate work orders."
            )

        connection.execute(insert_query, production_orders)

    print(f"Loaded {len(production_orders)} production orders into PostgreSQL.")


def load_material_lots(engine, material_lots):
    """Insert generated material lots into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM material_lots
        """
    )

    insert_query = text(
        """
        INSERT INTO material_lots (
            material_id,
            supplier_id,
            supplier_lot_number,
            received_date,
            quantity_received,
            quantity_available,
            lot_status
        )
        VALUES (
            :material_id,
            :supplier_id,
            :supplier_lot_number,
            :received_date,
            :quantity_received,
            :quantity_available,
            :lot_status
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "material_lots already contains data. "
                "The load was stopped to prevent duplicate supplier lots."
            )

        connection.execute(insert_query, material_lots)

    print(f"Loaded {len(material_lots)} material lots into PostgreSQL.")


def load_production_order_materials(
    engine,
    production_order_materials,
    updated_material_lots,
):
    """Insert material allocations and update remaining lot quantities."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM production_order_materials
        """
    )

    insert_query = text(
        """
        INSERT INTO production_order_materials (
            production_order_id,
            material_lot_id,
            allocated_quantity
        )
        VALUES (
            :production_order_id,
            :material_lot_id,
            :allocated_quantity
        )
        """
    )

    update_lot_query = text(
        """
        UPDATE material_lots
        SET
            quantity_available = :quantity_available,
            lot_status = :lot_status
        WHERE material_lot_id = :material_lot_id
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "production_order_materials already contains data. "
                "The load was stopped to prevent duplicate allocations."
            )

        connection.execute(insert_query, production_order_materials)
        connection.execute(update_lot_query, updated_material_lots)

    print(
        f"Loaded {len(production_order_materials)} material allocations "
        "into PostgreSQL."
    )


def load_production_runs(engine, production_runs):
    """Insert generated manufacturing operation runs into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM production_runs
        """
    )

    insert_query = text(
        """
        INSERT INTO production_runs (
            production_order_id,
            machine_id,
            operator_id,
            operation_sequence,
            operation_type,
            start_timestamp,
            end_timestamp,
            planned_cycle_time_seconds,
            actual_cycle_time_seconds,
            input_quantity,
            good_quantity,
            scrap_quantity,
            rework_quantity,
            run_status
        )
        VALUES (
            :production_order_id,
            :machine_id,
            :operator_id,
            :operation_sequence,
            :operation_type,
            :start_timestamp,
            :end_timestamp,
            :planned_cycle_time_seconds,
            :actual_cycle_time_seconds,
            :input_quantity,
            :good_quantity,
            :scrap_quantity,
            :rework_quantity,
            :run_status
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "production_runs already contains data. "
                "The load was stopped to prevent duplicate operation runs."
            )

        connection.execute(insert_query, production_runs)

    print(f"Loaded {len(production_runs)} production runs into PostgreSQL.")


def load_quality_inspections(engine, quality_inspections):
    """Insert generated quality inspection events into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM quality_inspections
        """
    )

    insert_query = text(
        """
        INSERT INTO quality_inspections (
            production_run_id,
            inspector_id,
            inspection_timestamp,
            sample_size,
            passed_quantity,
            failed_quantity,
            inspection_result,
            measurement_type,
            measured_value,
            lower_spec_limit,
            upper_spec_limit
        )
        VALUES (
            :production_run_id,
            :inspector_id,
            :inspection_timestamp,
            :sample_size,
            :passed_quantity,
            :failed_quantity,
            :inspection_result,
            :measurement_type,
            :measured_value,
            :lower_spec_limit,
            :upper_spec_limit
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "quality_inspections already contains data. "
                "The load was stopped to prevent duplicate inspections."
            )

        connection.execute(insert_query, quality_inspections)

    print(
        f"Loaded {len(quality_inspections)} quality inspections "
        "into PostgreSQL."
    )


def load_quality_defects(engine, quality_defects):
    """Insert generated quality defect records into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM quality_defects
        """
    )

    insert_query = text(
        """
        INSERT INTO quality_defects (
            inspection_id,
            defect_type_id,
            defect_quantity,
            disposition,
            root_cause_category,
            corrective_action
        )
        VALUES (
            :inspection_id,
            :defect_type_id,
            :defect_quantity,
            :disposition,
            :root_cause_category,
            :corrective_action
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "quality_defects already contains data. "
                "The load was stopped to prevent duplicate defects."
            )

        connection.execute(insert_query, quality_defects)

    print(f"Loaded {len(quality_defects)} quality defects into PostgreSQL.")


def load_downtime_events(engine, downtime_events):
    """Insert generated machine downtime events into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM downtime_events
        """
    )

    insert_query = text(
        """
        INSERT INTO downtime_events (
            machine_id,
            production_run_id,
            downtime_start,
            downtime_end,
            downtime_minutes,
            downtime_category,
            downtime_reason,
            planned_flag
        )
        VALUES (
            :machine_id,
            :production_run_id,
            :downtime_start,
            :downtime_end,
            :downtime_minutes,
            :downtime_category,
            :downtime_reason,
            :planned_flag
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "downtime_events already contains data. "
                "The load was stopped to prevent duplicate downtime events."
            )

        connection.execute(insert_query, downtime_events)

    print(f"Loaded {len(downtime_events)} downtime events into PostgreSQL.")


def load_maintenance_events(engine, maintenance_events):
    """Insert generated equipment maintenance events into PostgreSQL."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM maintenance_events
        """
    )

    insert_query = text(
        """
        INSERT INTO maintenance_events (
            machine_id,
            maintenance_type,
            reported_timestamp,
            maintenance_start,
            maintenance_end,
            technician,
            failure_component,
            maintenance_action,
            maintenance_cost,
            machine_hours_at_service
        )
        VALUES (
            :machine_id,
            :maintenance_type,
            :reported_timestamp,
            :maintenance_start,
            :maintenance_end,
            :technician,
            :failure_component,
            :maintenance_action,
            :maintenance_cost,
            :machine_hours_at_service
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "maintenance_events already contains data. "
                "The load was stopped to prevent duplicate maintenance."
            )

        connection.execute(insert_query, maintenance_events)

    print(
        f"Loaded {len(maintenance_events)} maintenance events "
        "into PostgreSQL."
    )


def load_sensor_readings(engine, sensor_readings, batch_size=5000):
    """Insert machine telemetry into PostgreSQL in manageable batches."""

    count_query = text(
        """
        SELECT COUNT(*)
        FROM sensor_readings
        """
    )

    insert_query = text(
        """
        INSERT INTO sensor_readings (
            machine_id,
            reading_timestamp,
            temperature_c,
            vibration_mm_s,
            power_kw,
            pressure_psi,
            rpm
        )
        VALUES (
            :machine_id,
            :reading_timestamp,
            :temperature_c,
            :vibration_mm_s,
            :power_kw,
            :pressure_psi,
            :rpm
        )
        """
    )

    with engine.begin() as connection:
        existing_count = connection.execute(count_query).scalar_one()

        if existing_count > 0:
            raise ValueError(
                "sensor_readings already contains data. "
                "The load was stopped to prevent duplicate telemetry."
            )

        for batch_start in range(0, len(sensor_readings), batch_size):
            batch = sensor_readings[batch_start:batch_start + batch_size]
            connection.execute(insert_query, batch)

    print(f"Loaded {len(sensor_readings)} sensor readings into PostgreSQL.")
