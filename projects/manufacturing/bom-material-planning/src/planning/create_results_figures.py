"""Create reproducible figures for the material-planning results page."""

from collections import Counter
from datetime import date
from decimal import Decimal
from pathlib import Path

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

from src.config import DATABASE_URL

from .bom_explosion import get_material_requirements
from .netting import get_netted_material_requirements
from .recommendations import get_purchase_recommendations
from .report import summarize_plan


OUTPUT_DIRECTORY = Path(__file__).resolve().parents[2] / "docs" / "images"

NAVY = "#14233b"
BLUE = "#2f75b5"
CYAN = "#36a9ce"
AMBER = "#e6a23c"
RED = "#d9534f"
GRAY = "#64748b"
LIGHT_GRAY = "#e2e8f0"


def configure_plot_style():
    """Apply consistent, GitHub-readable figure styling."""
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.edgecolor": LIGHT_GRAY,
            "axes.labelcolor": NAVY,
            "axes.titlecolor": NAVY,
            "text.color": NAVY,
            "xtick.color": GRAY,
            "ytick.color": GRAY,
            "font.family": "sans-serif",
            "font.size": 10,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "grid.color": LIGHT_GRAY,
            "grid.alpha": 0.7,
        }
    )


def save_figure(figure, file_name):
    """Save a tightly cropped PNG and close the figure."""
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIRECTORY / file_name
    figure.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    return output_path


def create_shortage_exposure_figure(material_summary):
    """Plot net requirement as a percentage of gross demand by material."""
    rows = []
    for material in material_summary:
        gross = material["gross_requirement"]
        exposure = Decimal("0") if gross == 0 else (
            material["cumulative_net_requirement"] / gross * 100
        )
        rows.append((material["material_code"], float(exposure)))
    rows.sort(key=lambda row: row[1])

    figure, axis = plt.subplots(figsize=(12, 6.5))
    colors = [RED if exposure > 0 else CYAN for _, exposure in rows]
    bars = axis.barh(
        [material for material, _ in rows],
        [exposure for _, exposure in rows],
        color=colors,
    )
    axis.set_title("Material Shortage Exposure", fontsize=18, fontweight="bold")
    axis.set_xlabel("Cumulative net requirement as % of gross requirement")
    axis.set_xlim(0, max(60, max(exposure for _, exposure in rows) + 8))
    axis.grid(axis="x")
    axis.bar_label(bars, fmt="%.1f%%", padding=4, color=NAVY)
    figure.tight_layout()
    return save_figure(figure, "planning_shortage_exposure.png")


def create_recommended_spend_figure(material_summary):
    """Plot estimated recommended purchase cost by material."""
    rows = [
        (row["material_code"], float(row["estimated_purchase_cost"]))
        for row in material_summary
        if row["estimated_purchase_cost"] > 0
    ]
    rows.sort(key=lambda row: row[1])

    figure, axis = plt.subplots(figsize=(12, 6))
    bars = axis.barh(
        [material for material, _ in rows],
        [cost for _, cost in rows],
        color=BLUE,
    )
    axis.set_title("Recommended Purchase Spend", fontsize=18, fontweight="bold")
    axis.set_xlabel("Estimated cost (USD)")
    axis.grid(axis="x")
    axis.bar_label(bars, labels=[f"${cost:,.0f}" for _, cost in rows], padding=4)
    figure.tight_layout()
    return save_figure(figure, "planning_recommended_spend.png")


def create_order_timeline_figure(recommendations, planning_date):
    """Plot required order dates by material and urgency status."""
    materials = sorted({row["material_code"] for row in recommendations})
    material_positions = {material: index for index, material in enumerate(materials)}

    figure, axis = plt.subplots(figsize=(14, 7))
    for status, color in (("Past Due", RED), ("Due Today", AMBER), ("Future", BLUE)):
        rows = [row for row in recommendations if row["urgency_status"] == status]
        if not rows:
            continue
        axis.scatter(
            [row["recommended_order_date"] for row in rows],
            [material_positions[row["material_code"]] for row in rows],
            s=[
                45 + min(float(row["estimated_purchase_cost"]) / 100, 180)
                for row in rows
            ],
            color=color,
            alpha=0.78,
            edgecolor="white",
            linewidth=0.7,
            label=f"{status} ({len(rows)})",
        )

    axis.axvline(
        planning_date,
        color=NAVY,
        linestyle="--",
        linewidth=1.5,
        label=f"Planning date: {planning_date}",
    )
    axis.set_title("Recommended Order Timeline", fontsize=18, fontweight="bold")
    axis.set_xlabel("Recommended order date")
    axis.set_yticks(range(len(materials)), materials)
    axis.xaxis.set_major_locator(mdates.WeekdayLocator(interval=2))
    axis.xaxis.set_major_formatter(mdates.DateFormatter("%b %d"))
    axis.grid(axis="x")
    axis.legend(frameon=False, loc="upper left")
    figure.autofmt_xdate()
    figure.tight_layout()
    return save_figure(figure, "planning_order_timeline.png")


def main():
    """Query PostgreSQL and recreate every planning-results figure."""
    configure_plot_style()
    planning_date = date.today()
    engine = create_engine(DATABASE_URL)
    material_requirements = get_material_requirements(engine)
    netted_requirements = get_netted_material_requirements(engine)
    recommendations = get_purchase_recommendations(engine, planning_date)
    overall_summary, material_summary = summarize_plan(
        material_requirements, netted_requirements, recommendations
    )

    created_files = [
        create_shortage_exposure_figure(material_summary),
        create_recommended_spend_figure(material_summary),
        create_order_timeline_figure(recommendations, planning_date),
    ]
    urgency_counts = Counter(
        row["urgency_status"] for row in recommendations
    )

    print("\nPLANNING RESULT FIGURES")
    print("=" * 30)
    print(
        f"Recommendations: {overall_summary['recommendation_count']} | "
        f"Urgency: {dict(urgency_counts)}"
    )
    for file_path in created_files:
        print(file_path)


if __name__ == "__main__":
    main()
