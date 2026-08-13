"""Tests for planning-report summaries and CSV output."""

from datetime import date
from decimal import Decimal

from src.planning.report import summarize_plan, write_csv


def test_summary_combines_requirements_and_recommendations():
    material_requirements = [
        {
            "material_id": 1,
            "material_code": "MAT-1",
            "material_name": "Material One",
            "base_unit_of_measure": "KG",
            "gross_requirement": Decimal("100.000"),
        },
        {
            "material_id": 1,
            "material_code": "MAT-1",
            "material_name": "Material One",
            "base_unit_of_measure": "KG",
            "gross_requirement": Decimal("50.000"),
        },
    ]
    netted = [
        {"material_id": 1, "net_requirement": Decimal("40.000")},
        {"material_id": 1, "net_requirement": Decimal("20.000")},
    ]
    recommendations = [
        {
            "material_id": 1,
            "recommended_order_quantity": Decimal("100.000"),
            "estimated_purchase_cost": Decimal("250.00"),
            "urgency_status": "Past Due",
            "recommended_order_date": date(2026, 8, 1),
        }
    ]

    overall, materials = summarize_plan(
        material_requirements, netted, recommendations
    )

    assert overall["materials_planned"] == 1
    assert overall["materials_with_shortages"] == 1
    assert overall["past_due_recommendations"] == 1
    assert overall["estimated_purchase_cost"] == Decimal("250.00")
    assert materials[0]["gross_requirement"] == Decimal("150.000")
    assert materials[0]["cumulative_net_requirement"] == Decimal("60.000")


def test_write_csv_creates_reproducible_output(tmp_path):
    output_file = tmp_path / "planning.csv"
    rows = [{"material_code": "MAT-1", "quantity": Decimal("10.000")}]

    assert write_csv(output_file, rows) is True
    assert output_file.read_text(encoding="utf-8") == (
        "material_code,quantity\nMAT-1,10.000\n"
    )
