"""Create reproducible figures for the GitHub analytics results report."""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from .analysis import (
    get_defect_analysis,
    get_downtime_causes,
    get_monthly_trends,
    get_product_family_kpis,
)
from .kpis import get_engine, get_machine_kpis


OUTPUT_DIRECTORY = Path(__file__).resolve().parents[2] / "docs" / "images"

NAVY = "#14233b"
BLUE = "#2f75b5"
CYAN = "#36a9ce"
GREEN = "#4f9d69"
AMBER = "#e6a23c"
RED = "#d9534f"
GRAY = "#64748b"
LIGHT_GRAY = "#e2e8f0"


def configure_plot_style():
    """Apply a consistent, readable style to every report figure."""
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
    """Save one tightly cropped, GitHub-ready PNG and close the figure."""
    OUTPUT_DIRECTORY.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIRECTORY / file_name
    figure.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    return output_path


def create_machine_performance_figure(rows):
    """Compare machine quality rates and downtime."""
    active_rows = [row for row in rows if row["first_pass_yield_pct"] is not None]
    machine_codes = [row["machine_code"] for row in active_rows]
    positions = np.arange(len(machine_codes))

    figure, (quality_axis, downtime_axis) = plt.subplots(1, 2, figsize=(15, 6))
    figure.suptitle("Machine Performance Comparison", fontsize=20, fontweight="bold")

    fpy_values = [row["first_pass_yield_pct"] for row in active_rows]
    loss_axis = quality_axis.twinx()
    quality_axis.plot(
        positions,
        fpy_values,
        marker="o",
        linewidth=2.5,
        color=BLUE,
        label="FPY",
    )
    loss_axis.plot(
        positions,
        [row["scrap_rate_pct"] for row in active_rows],
        marker="o",
        linewidth=2,
        color=RED,
        label="Scrap",
    )
    loss_axis.plot(
        positions,
        [row["rework_rate_pct"] for row in active_rows],
        marker="o",
        linewidth=2,
        color=GREEN,
        label="Rework",
    )
    quality_axis.set_title("Production rates by machine")
    quality_axis.set_ylabel("FPY (%)", color=BLUE)
    quality_axis.set_ylim(min(fpy_values) - 0.15, min(100, max(fpy_values) + 0.15))
    loss_axis.set_ylabel("Scrap and rework (%)", color=GRAY)
    loss_axis.set_ylim(0, 1.05)
    quality_axis.set_xticks(positions, machine_codes, rotation=45)
    quality_axis.grid(axis="y")
    quality_lines, quality_labels = quality_axis.get_legend_handles_labels()
    loss_lines, loss_labels = loss_axis.get_legend_handles_labels()
    quality_axis.legend(
        quality_lines + loss_lines,
        quality_labels + loss_labels,
        frameon=False,
        ncol=3,
        loc="upper center",
    )

    ordered_rows = sorted(rows, key=lambda row: row["downtime_minutes"])
    ordered_codes = [row["machine_code"] for row in ordered_rows]
    downtime_axis.barh(
        ordered_codes,
        [row["downtime_minutes"] for row in ordered_rows],
        color=LIGHT_GRAY,
        label="Total downtime",
    )
    downtime_axis.barh(
        ordered_codes,
        [row["unplanned_downtime_minutes"] for row in ordered_rows],
        color=AMBER,
        label="Unplanned downtime",
    )
    downtime_axis.set_title("Downtime by machine")
    downtime_axis.set_xlabel("Minutes")
    downtime_axis.grid(axis="x")
    downtime_axis.legend(frameon=False)

    figure.tight_layout(rect=(0, 0, 1, 0.92))
    return save_figure(figure, "analytics_machine_performance.png")


def create_product_family_figure(rows):
    """Compare product-family quality rates and average cycle time."""
    family_names = [row["product_family"] for row in rows]
    positions = np.arange(len(rows))
    width = 0.3

    figure, (rates_axis, cycle_axis) = plt.subplots(1, 2, figsize=(15, 6))
    figure.suptitle("Product-Family Performance", fontsize=20, fontweight="bold")

    fpy_values = [row["first_pass_yield_pct"] for row in rows]
    loss_axis = rates_axis.twinx()
    rates_axis.plot(
        positions,
        fpy_values,
        marker="o",
        linewidth=2.5,
        color=BLUE,
        label="FPY",
    )
    loss_axis.bar(
        positions - width / 2,
        [row["scrap_rate_pct"] for row in rows],
        width,
        color=RED,
        label="Scrap",
    )
    loss_axis.bar(
        positions + width / 2,
        [row["rework_rate_pct"] for row in rows],
        width,
        color=GREEN,
        label="Rework",
    )
    rates_axis.set_title("Quality rates")
    rates_axis.set_ylabel("FPY (%)", color=BLUE)
    rates_axis.set_ylim(min(fpy_values) - 0.15, min(100, max(fpy_values) + 0.15))
    loss_axis.set_ylabel("Scrap and rework (%)", color=GRAY)
    loss_axis.set_ylim(0, 1.0)
    rates_axis.set_xticks(positions, family_names, rotation=35, ha="right")
    rates_axis.grid(axis="y")
    quality_lines, quality_labels = rates_axis.get_legend_handles_labels()
    loss_bars, loss_labels = loss_axis.get_legend_handles_labels()
    rates_axis.legend(
        quality_lines + loss_bars,
        quality_labels + loss_labels,
        frameon=False,
        ncol=3,
        loc="upper center",
    )

    cycle_rows = sorted(rows, key=lambda row: row["average_cycle_time_seconds"])
    cycle_axis.barh(
        [row["product_family"] for row in cycle_rows],
        [row["average_cycle_time_seconds"] for row in cycle_rows],
        color=CYAN,
    )
    cycle_axis.set_title("Average production cycle time")
    cycle_axis.set_xlabel("Seconds")
    cycle_axis.grid(axis="x")

    figure.tight_layout(rect=(0, 0, 1, 0.92))
    return save_figure(figure, "analytics_product_family.png")


def create_loss_drivers_figure(defects, downtime_causes):
    """Visualize leading quality defects and downtime causes."""
    top_defects = list(reversed(defects[:10]))
    ordered_causes = sorted(
        downtime_causes, key=lambda row: row["downtime_minutes"]
    )

    figure, (defect_axis, downtime_axis) = plt.subplots(1, 2, figsize=(16, 7))
    figure.suptitle("Operational Loss Drivers", fontsize=20, fontweight="bold")

    defect_labels = [
        f"{row['machine_code']} | {row['defect_category']} | {row['severity']}"
        for row in top_defects
    ]
    defect_axis.barh(
        defect_labels,
        [row["defect_quantity"] for row in top_defects],
        color=RED,
    )
    defect_axis.set_title("Largest defect concentrations")
    defect_axis.set_xlabel("Recorded defect quantity")
    defect_axis.grid(axis="x")

    cause_colors = [
        AMBER if row["planned_flag"] else RED for row in ordered_causes
    ]
    downtime_axis.barh(
        [row["downtime_category"] for row in ordered_causes],
        [row["downtime_minutes"] for row in ordered_causes],
        color=cause_colors,
    )
    downtime_axis.set_title("Downtime minutes by cause")
    downtime_axis.set_xlabel("Minutes")
    downtime_axis.grid(axis="x")
    downtime_axis.text(
        0.98,
        0.03,
        "Amber = planned   Red = unplanned",
        transform=downtime_axis.transAxes,
        ha="right",
        color=GRAY,
        fontsize=9,
    )

    figure.tight_layout(rect=(0, 0, 1, 0.92))
    return save_figure(figure, "analytics_loss_drivers.png")


def create_monthly_downtime_figure(rows):
    """Plot monthly downtime not already visualized in Tableau."""
    months = [row["month"].strftime("%b %Y") for row in rows]
    positions = np.arange(len(rows))

    figure, downtime_axis = plt.subplots(figsize=(14, 6))
    figure.suptitle("Monthly Downtime Trend", fontsize=20, fontweight="bold")
    downtime_axis.bar(
        positions,
        [row["downtime_minutes"] for row in rows],
        color=LIGHT_GRAY,
        label="Total downtime",
    )
    downtime_axis.bar(
        positions,
        [row["unplanned_downtime_minutes"] for row in rows],
        color=AMBER,
        label="Unplanned downtime",
    )
    downtime_axis.set_ylabel("Minutes")
    downtime_axis.set_xticks(positions, months, rotation=35, ha="right")
    downtime_axis.grid(axis="y")
    downtime_axis.legend(frameon=False)

    figure.tight_layout(rect=(0, 0, 1, 0.92))
    return save_figure(figure, "analytics_monthly_downtime.png")


def main():
    """Query PostgreSQL and create every figure used by the results report."""
    configure_plot_style()
    engine = get_engine()

    created_files = [
        create_machine_performance_figure(get_machine_kpis(engine)),
        create_product_family_figure(get_product_family_kpis(engine)),
        create_loss_drivers_figure(
            get_defect_analysis(engine), get_downtime_causes(engine)
        ),
        create_monthly_downtime_figure(get_monthly_trends(engine)),
    ]

    print("\nANALYTICS RESULT FIGURES")
    print("=" * 30)
    for file_path in created_files:
        print(file_path)


if __name__ == "__main__":
    main()
