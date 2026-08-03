from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from unittest.mock import patch

from src.etl.generate_customer_order_items import (
    generate_customer_order_items,
    generate_ordered_quantity,
    get_line_status,
)
from src.etl.generate_production_orders import (
    generate_output_quantities,
    generate_production_orders,
)
from src.etl.generate_material_lots import (
    generate_available_quantity,
    generate_material_lots,
)
from src.etl.generate_production_order_materials import (
    calculate_required_material,
    generate_production_order_materials,
)
from src.etl.generate_production_runs import (
    ROUTES_BY_PRODUCT_FAMILY,
    generate_operation_quantities,
    get_run_statuses,
)
from src.etl.generate_quality_inspections import (
    generate_measured_value,
    generate_sample_results,
)
from src.etl.generate_quality_defects import (
    choose_disposition,
    generate_quality_defects,
    partition_defect_quantity,
)
from src.etl.generate_downtime_events import (
    DOWNTIME_RULES,
    build_downtime_event,
    generate_event_timestamps,
)
from src.etl.generate_maintenance_events import (
    build_maintenance_event,
    calculate_machine_hours,
)
from src.etl.generate_sensor_readings import (
    generate_sensor_readings,
    generate_sensor_value,
)


def test_line_status_matches_parent_order_status():
    assert get_line_status("Open") == "Open"
    assert get_line_status("Released") == "Allocated"
    assert get_line_status("Partially Fulfilled") == "Partially Fulfilled"
    assert get_line_status("Completed") == "Completed"
    assert get_line_status("Cancelled") == "Cancelled"


def test_ordered_quantities_follow_product_family_rules():
    for product_family in (
        "Solid Rivet",
        "Blind Rivet",
        "Blind Bolt",
        "Temporary Fastener",
        "Threaded Insert",
        "Installation Tool",
    ):
        quantity = generate_ordered_quantity(product_family)
        assert quantity > 0


@patch("src.etl.generate_customer_order_items.random.randint", return_value=2)
def test_generated_order_has_unique_products_and_sequential_lines(_):
    customer_orders = [
        {"customer_order_id": 1, "order_status": "Released"}
    ]
    products = [
        {
            "product_id": 10,
            "product_family": "Solid Rivet",
            "standard_unit_cost": Decimal("0.1200"),
        },
        {
            "product_id": 11,
            "product_family": "Blind Bolt",
            "standard_unit_cost": Decimal("4.7500"),
        },
    ]

    order_items = generate_customer_order_items(customer_orders, products)

    assert len(order_items) == 2
    assert {item["product_id"] for item in order_items} == {10, 11}
    assert [item["line_number"] for item in order_items] == [1, 2]
    assert all(item["line_status"] == "Allocated" for item in order_items)
    assert all(item["unit_price"] > 0 for item in order_items)


def test_completed_production_quantities_reconcile():
    completed, scrapped = generate_output_quantities(
        production_status="Completed",
        ordered_quantity=1000,
        planned_quantity=1025,
    )

    assert completed == 1000
    assert scrapped == 25
    assert completed + scrapped == 1025


def test_open_order_lines_do_not_generate_production_orders():
    customer_order_items = [
        {
            "customer_order_item_id": 1,
            "ordered_quantity": 1000,
            "line_status": "Open",
            "product_family": "Solid Rivet",
            "order_date": None,
            "requested_delivery_date": None,
        }
    ]

    production_orders = generate_production_orders(
        customer_order_items=customer_order_items,
        machines_by_operation={"Cold Heading": [1]},
    )

    assert production_orders == []


def test_unusable_material_lots_have_no_available_quantity():
    quantity_received = Decimal("1000.000")

    assert (
        generate_available_quantity("Depleted", quantity_received)
        == Decimal("0.000")
    )
    assert (
        generate_available_quantity("Rejected", quantity_received)
        == Decimal("0.000")
    )


def test_generated_material_lots_have_unique_numbers():
    materials = [
        {
            "material_id": 1,
            "material_form": "Wire",
            "unit_of_measure": "lb",
        }
    ]
    suppliers = [
        {
            "supplier_id": 1,
            "quality_rating": Decimal("98.50"),
        }
    ]

    material_lots = generate_material_lots(
        materials=materials,
        suppliers=suppliers,
        production_orders=[],
        start_date=date(2025, 1, 1),
        end_date=date(2025, 12, 31),
        background_lots_per_material=5,
    )

    lot_numbers = [lot["supplier_lot_number"] for lot in material_lots]

    assert len(material_lots) == 5
    assert len(set(lot_numbers)) == 5
    assert all(
        Decimal("0.000")
        <= lot["quantity_available"]
        <= lot["quantity_received"]
        for lot in material_lots
    )


def test_material_requirement_uses_product_dimensions():
    production_order = {
        "planned_quantity": 1000,
        "product_family": "Solid Rivet",
        "diameter_in": Decimal("0.1250"),
        "length_in": Decimal("0.5000"),
        "material_category": "Aluminum",
    }

    required_quantity = calculate_required_material(production_order)

    assert required_quantity > 0


def test_completed_material_allocations_use_fifo_and_preserve_inventory():
    production_orders = [
        {
            "production_order_id": 1,
            "production_order_number": "PO-100001",
            "planned_quantity": 1000,
            "production_status": "Completed",
            "scheduled_start_date": date(2025, 6, 1),
            "material_id": 1,
            "product_family": "Solid Rivet",
            "diameter_in": Decimal("0.1250"),
            "length_in": Decimal("0.5000"),
            "material_category": "Aluminum",
        }
    ]
    material_lots = [
        {
            "material_lot_id": 1,
            "material_id": 1,
            "received_date": date(2025, 1, 1),
            "quantity_received": Decimal("200.000"),
            "quantity_available": Decimal("100.000"),
            "lot_status": "Available",
        },
        {
            "material_lot_id": 2,
            "material_id": 1,
            "received_date": date(2025, 2, 1),
            "quantity_received": Decimal("200.000"),
            "quantity_available": Decimal("100.000"),
            "lot_status": "Available",
        },
    ]

    allocations, updated_lots = generate_production_order_materials(
        production_orders,
        material_lots,
    )

    assert allocations[0]["material_lot_id"] == 1
    assert allocations[0]["allocated_quantity"] > 0
    assert updated_lots[0]["quantity_available"] == Decimal("100.000")


def test_scheduled_material_allocations_reduce_current_inventory():
    production_orders = [
        {
            "production_order_id": 1,
            "production_order_number": "PO-100001",
            "planned_quantity": 1000,
            "production_status": "Scheduled",
            "scheduled_start_date": date(2025, 6, 1),
            "material_id": 1,
            "product_family": "Solid Rivet",
            "diameter_in": Decimal("0.1250"),
            "length_in": Decimal("0.5000"),
            "material_category": "Aluminum",
        }
    ]
    material_lots = [
        {
            "material_lot_id": 1,
            "material_id": 1,
            "received_date": date(2025, 1, 1),
            "quantity_received": Decimal("200.000"),
            "quantity_available": Decimal("100.000"),
            "lot_status": "Available",
        }
    ]

    _, updated_lots = generate_production_order_materials(
        production_orders,
        material_lots,
    )

    assert updated_lots[0]["quantity_available"] < Decimal("100.000")


def test_completed_production_run_quantities_reconcile_to_parent_order():
    production_order = {
        "completed_quantity": 1000,
        "scrapped_quantity": 25,
    }
    run_statuses = ["Completed"] * 5

    quantities = generate_operation_quantities(
        production_order,
        run_statuses,
    )

    assert all(
        input_quantity == good_quantity + scrap_quantity + rework_quantity
        for input_quantity, good_quantity, scrap_quantity, rework_quantity
        in quantities
    )
    assert quantities[-1][1] == 1000
    assert sum(quantity[2] for quantity in quantities) == 25


def test_in_production_route_has_one_running_operation():
    statuses = get_run_statuses("In Production", operation_count=5)

    assert statuses.count("Running") == 1
    assert all(
        status == "Completed"
        for status in statuses[:statuses.index("Running")]
    )
    assert all(
        status == "Planned"
        for status in statuses[statuses.index("Running") + 1:]
    )


def test_product_routes_begin_at_sequence_one_and_end_in_packaging():
    assert all(route for route in ROUTES_BY_PRODUCT_FAMILY.values())
    assert all(
        route[-1] == "Packaging"
        for route in ROUTES_BY_PRODUCT_FAMILY.values()
    )


def test_inspection_sample_quantities_balance():
    production_run = {
        "good_quantity": 950,
        "scrap_quantity": 30,
        "rework_quantity": 20,
    }

    sample_size, passed, failed, result = generate_sample_results(
        production_run
    )

    assert sample_size == passed + failed
    assert result in {"Pass", "Conditional", "Fail"}


def test_failed_measurement_falls_outside_specification():
    lower_limit = Decimal("0.1225")
    upper_limit = Decimal("0.1275")
    nominal = Decimal("0.1250")

    measured_value = generate_measured_value(
        lower_limit,
        upper_limit,
        nominal,
        "Fail",
    )

    assert measured_value < lower_limit or measured_value > upper_limit


def test_pending_measurement_has_no_recorded_value():
    measured_value = generate_measured_value(
        Decimal("0.1225"),
        Decimal("0.1275"),
        Decimal("0.1250"),
        "Pending",
    )

    assert measured_value is None


def test_partitioned_defect_quantities_reconcile():
    quantities = partition_defect_quantity(
        total_quantity=10,
        defect_count=3,
    )

    assert sum(quantities) == 10
    assert all(quantity > 0 for quantity in quantities)


def test_passed_inspections_do_not_generate_defects():
    inspections = [
        {
            "inspection_id": 1,
            "failed_quantity": 0,
            "inspection_result": "Pass",
            "measurement_type": "Diameter",
        }
    ]
    defect_types = [
        {
            "defect_type_id": 1,
            "defect_code": "DIM-001",
            "defect_category": "Dimensional",
            "severity": "Major",
        }
    ]

    assert generate_quality_defects(inspections, defect_types) == []


def test_conditional_defects_do_not_receive_scrap_disposition():
    dispositions = {
        choose_disposition("Major", "Conditional", "Dimensional")
        for _ in range(50)
    }

    assert "Scrap" not in dispositions


def test_downtime_duration_matches_timestamps():
    window_start = datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc)
    window_end = window_start + timedelta(hours=8)
    start, end, minutes = generate_event_timestamps(
        window_start,
        window_end,
        "Mechanical Failure",
    )

    assert window_start <= start < end <= window_end
    assert int((end - start).total_seconds() // 60) == minutes


def test_downtime_planned_flag_matches_category():
    start = datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc)
    end = start + timedelta(minutes=60)

    event = build_downtime_event(
        machine_id=1,
        production_run_id=None,
        category="Preventive Maintenance",
        start=start,
        end=end,
    )

    assert event["planned_flag"] is True
    assert event["downtime_minutes"] == 60
    assert DOWNTIME_RULES[event["downtime_category"]]["planned"] is True


def test_machine_hours_increase_with_service_date():
    machine = {
        "machine_id": 1,
        "install_date": date(2020, 1, 1),
    }
    earlier_service = datetime(2025, 1, 1, tzinfo=timezone.utc)
    later_service = datetime(2026, 1, 1, tzinfo=timezone.utc)

    assert calculate_machine_hours(
        machine,
        later_service,
    ) > calculate_machine_hours(machine, earlier_service)


def test_maintenance_event_has_valid_cost_and_timestamps():
    machine = {
        "machine_id": 1,
        "operation_type": "Cold Heading",
        "install_date": date(2020, 1, 1),
    }
    maintenance_start = datetime(2025, 1, 1, 8, 0, tzinfo=timezone.utc)
    maintenance_end = maintenance_start + timedelta(hours=2)

    event = build_maintenance_event(
        machine=machine,
        maintenance_type="Preventive",
        maintenance_start=maintenance_start,
        maintenance_end=maintenance_end,
        reported_timestamp=maintenance_start - timedelta(days=7),
        technician_names=["Technician One"],
    )

    assert event["maintenance_end"] >= event["maintenance_start"]
    assert event["reported_timestamp"] <= event["maintenance_start"]
    assert event["maintenance_cost"] >= 0
    assert event["machine_hours_at_service"] >= 0


def test_non_applicable_sensor_returns_null():
    assert generate_sensor_value(None, operating=True) is None


def test_sensor_readings_have_unique_machine_timestamps():
    start_timestamp = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    end_timestamp = start_timestamp + timedelta(minutes=15)
    machines = [
        {
            "machine_id": 1,
            "operation_type": "Cold Heading",
            "status": "Active",
        },
        {
            "machine_id": 2,
            "operation_type": "Inspection",
            "status": "Active",
        },
    ]

    readings = generate_sensor_readings(
        machines=machines,
        downtime_events=[],
        start_timestamp=start_timestamp,
        end_timestamp=end_timestamp,
    )
    keys = {
        (reading["machine_id"], reading["reading_timestamp"])
        for reading in readings
    }

    assert len(readings) == 8
    assert len(keys) == len(readings)
    assert all(reading["temperature_c"] >= 0 for reading in readings)
