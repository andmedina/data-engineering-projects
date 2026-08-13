"""Run and export the complete BOM material-planning report."""

import csv
from collections import defaultdict
from datetime import date
from decimal import Decimal
from pathlib import Path

from sqlalchemy import create_engine

from src.config import DATABASE_URL

from .bom_explosion import get_bom_explosion, get_material_requirements
from .netting import get_netted_material_requirements
from .recommendations import get_purchase_recommendations


DEFAULT_OUTPUT_DIRECTORY = (
    Path(__file__).resolve().parents[2] / "outputs" / "planning"
)


def summarize_plan(material_requirements, netted_requirements, recommendations):
    """Return overall and material-level planning summaries."""
    requirements_by_material = defaultdict(lambda: Decimal("0"))
    net_by_material = defaultdict(lambda: Decimal("0"))
    recommendations_by_material = defaultdict(
        lambda: {
            "recommendation_count": 0,
            "recommended_quantity": Decimal("0"),
            "estimated_purchase_cost": Decimal("0"),
            "past_due_count": 0,
            "earliest_order_date": None,
        }
    )

    material_details = {}
    for row in material_requirements:
        material_id = row["material_id"]
        material_details[material_id] = row
        requirements_by_material[material_id] += row["gross_requirement"]

    for row in netted_requirements:
        net_by_material[row["material_id"]] += row["net_requirement"]

    for row in recommendations:
        summary = recommendations_by_material[row["material_id"]]
        summary["recommendation_count"] += 1
        summary["recommended_quantity"] += row[
            "recommended_order_quantity"
        ]
        summary["estimated_purchase_cost"] += row[
            "estimated_purchase_cost"
        ]
        summary["past_due_count"] += row["urgency_status"] == "Past Due"
        order_date = row["recommended_order_date"]
        if (
            summary["earliest_order_date"] is None
            or order_date < summary["earliest_order_date"]
        ):
            summary["earliest_order_date"] = order_date

    material_summary = []
    for material_id, detail in material_details.items():
        recommendation = recommendations_by_material[material_id]
        material_summary.append(
            {
                "material_code": detail["material_code"],
                "material_name": detail["material_name"],
                "base_unit_of_measure": detail["base_unit_of_measure"],
                "gross_requirement": requirements_by_material[material_id],
                "cumulative_net_requirement": net_by_material[material_id],
                **recommendation,
            }
        )

    material_summary.sort(
        key=lambda row: (
            -row["past_due_count"],
            -row["estimated_purchase_cost"],
            row["material_code"],
        )
    )

    overall_summary = {
        "materials_planned": len(material_details),
        "materials_with_shortages": sum(
            net_quantity > 0 for net_quantity in net_by_material.values()
        ),
        "recommendation_count": len(recommendations),
        "past_due_recommendations": sum(
            row["urgency_status"] == "Past Due" for row in recommendations
        ),
        "estimated_purchase_cost": sum(
            (row["estimated_purchase_cost"] for row in recommendations),
            Decimal("0"),
        ),
    }
    return overall_summary, material_summary


def write_csv(file_path, rows):
    """Write dictionaries to a CSV file and return whether a file was created."""
    if not rows:
        return False
    file_path.parent.mkdir(parents=True, exist_ok=True)
    with file_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    return True


def export_planning_results(
    bom_explosion,
    netted_requirements,
    recommendations,
    output_directory=DEFAULT_OUTPUT_DIRECTORY,
):
    """Export detailed planning datasets and return their created paths."""
    datasets = {
        "bom_explosion.csv": bom_explosion,
        "netted_material_requirements.csv": netted_requirements,
        "purchase_recommendations.csv": recommendations,
    }
    created_files = []
    for file_name, rows in datasets.items():
        file_path = output_directory / file_name
        if write_csv(file_path, rows):
            created_files.append(file_path)
    return created_files


def print_plan_summary(overall_summary, material_summary, planning_date):
    """Display a compact purchasing-oriented terminal report."""
    print("\nBOM MATERIAL PLANNING SUMMARY")
    print("=" * 80)
    print(f"Planning date:             {planning_date}")
    print(f"Materials planned:         {overall_summary['materials_planned']}")
    print(
        "Materials with shortages:  "
        f"{overall_summary['materials_with_shortages']}"
    )
    print(f"Purchase recommendations:  {overall_summary['recommendation_count']}")
    print(
        "Past-due recommendations:  "
        f"{overall_summary['past_due_recommendations']}"
    )
    print(
        "Estimated purchase cost:   "
        f"${overall_summary['estimated_purchase_cost']:,.2f}"
    )

    print("\nMATERIAL PURCHASING SUMMARY")
    print("=" * 115)
    print(
        f"{'Material':<18} {'UOM':<5} {'Gross req.':>13} {'Net req.':>13} "
        f"{'Orders':>7} {'Order qty':>13} {'Cost':>13} {'Past due':>9} "
        f"{'Order by':>12}"
    )
    print("-" * 115)
    for row in material_summary:
        earliest_order_date = row["earliest_order_date"] or "—"
        print(
            f"{row['material_code']:<18} "
            f"{row['base_unit_of_measure']:<5} "
            f"{row['gross_requirement']:>13,.3f} "
            f"{row['cumulative_net_requirement']:>13,.3f} "
            f"{row['recommendation_count']:>7} "
            f"{row['recommended_quantity']:>13,.3f} "
            f"${row['estimated_purchase_cost']:>12,.2f} "
            f"{row['past_due_count']:>9} "
            f"{str(earliest_order_date):>12}"
        )


def run_planning_report(engine, planning_date=None, output_directory=None):
    """Execute, summarize, print, and export the complete material plan."""
    planning_date = planning_date or date.today()
    bom_explosion = get_bom_explosion(engine)
    material_requirements = get_material_requirements(engine)
    netted_requirements = get_netted_material_requirements(engine)
    recommendations = get_purchase_recommendations(engine, planning_date)
    overall_summary, material_summary = summarize_plan(
        material_requirements,
        netted_requirements,
        recommendations,
    )

    print_plan_summary(overall_summary, material_summary, planning_date)
    created_files = export_planning_results(
        bom_explosion,
        netted_requirements,
        recommendations,
        output_directory or DEFAULT_OUTPUT_DIRECTORY,
    )
    print("\nPLANNING EXPORTS")
    print("=" * 80)
    for file_path in created_files:
        print(file_path)

    return overall_summary, material_summary, created_files


def main():
    """Run the reproducible planning report against PostgreSQL."""
    run_planning_report(create_engine(DATABASE_URL))


if __name__ == "__main__":
    main()
