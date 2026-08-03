"""Create the portfolio image summarizing the ML experiment.

The values below are a recorded snapshot from the reproducible experiment in
``train_predictive_maintenance.py``. Keeping report rendering separate from
model training makes visual adjustments fast and avoids retraining three
models whenever presentation styling changes.

Run from the repository root with::

    python -m src.models.create_ml_results_report
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np


OUTPUT_PATH = Path("docs/images/predictive_maintenance_results.png")

MODEL_NAMES = [
    "Logistic\nRegression",
    "Random\nForest",
    "Histogram\nGradient Boosting",
]
VALIDATION_AVERAGE_PRECISION = [0.009, 0.088, 0.101]
VALIDATION_F1 = [0.052, 0.216, 0.170]

FINAL_METRICS = {
    "Precision": 0.082,
    "Recall": 0.389,
    "F1": 0.135,
    "Avg. Precision": 0.107,
    "ROC AUC": 0.693,
}

CONFUSION_MATRIX = np.array([[54_163, 157], [22, 14]])

FEATURE_IMPORTANCE = {
    "Max vibration (60m)": 0.3327,
    "Current vibration": 0.1905,
    "Mean vibration (60m)": 0.1240,
    "Max temperature (60m)": 0.0831,
    "Vibration variability (60m)": 0.0694,
    "Current pressure": 0.0678,
    "Mean pressure (60m)": 0.0664,
    "Max pressure (60m)": 0.0451,
    "Current RPM": 0.0281,
    "Max RPM (60m)": 0.0235,
}

NAVY = "#17324D"
BLUE = "#3977A8"
TEAL = "#2A9D8F"
ORANGE = "#E58B3A"
LIGHT_BLUE = "#DCEAF4"
LIGHT_GRAY = "#EEF1F4"
DARK_GRAY = "#44515C"


def label_bars(axis, bars, digits=3):
    """Write values above vertical bars."""
    for bar in bars:
        axis.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.004,
            f"{bar.get_height():.{digits}f}",
            ha="center",
            va="bottom",
            fontsize=9,
            color=DARK_GRAY,
        )


def style_axis(axis):
    """Apply consistent report styling to a chart axis."""
    axis.spines[["top", "right", "left"]].set_visible(False)
    axis.tick_params(axis="y", length=0, colors=DARK_GRAY)
    axis.tick_params(axis="x", length=0, colors=DARK_GRAY)
    axis.grid(axis="y", color=LIGHT_GRAY, linewidth=0.8)
    axis.set_axisbelow(True)


def create_report(output_path=OUTPUT_PATH):
    """Render and save the predictive-maintenance results summary."""
    figure = plt.figure(figsize=(16, 10), facecolor="white")
    grid = figure.add_gridspec(
        2,
        2,
        left=0.07,
        right=0.96,
        top=0.79,
        bottom=0.10,
        hspace=0.46,
        wspace=0.28,
    )

    figure.text(
        0.07,
        0.94,
        "Cold-Heading Predictive Maintenance",
        fontsize=25,
        fontweight="bold",
        color=NAVY,
    )
    figure.text(
        0.07,
        0.895,
        "Mechanical failure risk within 60 minutes  |  CH-01 and CH-02  |  Five-minute telemetry",
        fontsize=12,
        color=DARK_GRAY,
    )
    figure.text(
        0.07,
        0.852,
        "TRAIN  119,256 rows / 7 failures     VALIDATE  34,868 rows / 2 failures     TEST  54,356 rows / 3 failures",
        fontsize=10.5,
        color=BLUE,
        fontweight="bold",
    )

    # Candidate models are compared only on the chronological validation set.
    comparison_axis = figure.add_subplot(grid[0, 0])
    x_positions = np.arange(len(MODEL_NAMES))
    width = 0.34
    ap_bars = comparison_axis.bar(
        x_positions - width / 2,
        VALIDATION_AVERAGE_PRECISION,
        width,
        label="Average precision",
        color=TEAL,
    )
    f1_bars = comparison_axis.bar(
        x_positions + width / 2,
        VALIDATION_F1,
        width,
        label="F1",
        color=BLUE,
    )
    comparison_axis.set_title(
        "1. Compare models on validation data",
        loc="left",
        color=NAVY,
        fontweight="bold",
        pad=14,
    )
    comparison_axis.set_xticks(x_positions, MODEL_NAMES)
    comparison_axis.set_ylim(0, 0.26)
    comparison_axis.legend(frameon=False, loc="upper left")
    label_bars(comparison_axis, ap_bars)
    label_bars(comparison_axis, f1_bars)
    style_axis(comparison_axis)

    # Final test metrics use the model and threshold chosen on validation data.
    metrics_axis = figure.add_subplot(grid[0, 1])
    metric_names = list(FINAL_METRICS)
    metric_values = list(FINAL_METRICS.values())
    metric_colors = [TEAL if name == "Recall" else BLUE for name in metric_names]
    metric_bars = metrics_axis.bar(
        np.arange(len(metric_names)), metric_values, color=metric_colors
    )
    metrics_axis.set_title(
        "2. Evaluate selected model on untouched test data",
        loc="left",
        color=NAVY,
        fontweight="bold",
        pad=14,
    )
    metrics_axis.text(
        0,
        0.96,
        "Selected: Histogram Gradient Boosting  |  Threshold: 0.459",
        transform=metrics_axis.transAxes,
        fontsize=10,
        color=ORANGE,
        fontweight="bold",
        va="top",
    )
    metrics_axis.set_xticks(np.arange(len(metric_names)), metric_names, rotation=18)
    metrics_axis.set_ylim(0, 0.78)
    label_bars(metrics_axis, metric_bars)
    style_axis(metrics_axis)

    confusion_axis = figure.add_subplot(grid[1, 0])
    confusion_axis.imshow(CONFUSION_MATRIX, cmap="Blues", aspect="auto")
    confusion_axis.set_title(
        "3. Understand the alert tradeoff",
        loc="left",
        color=NAVY,
        fontweight="bold",
        pad=14,
    )
    confusion_axis.set_xticks([0, 1], ["Predicted normal", "Predicted warning"])
    confusion_axis.set_yticks([0, 1], ["Actually normal", "Failure within 60m"])
    for row in range(2):
        for column in range(2):
            value = CONFUSION_MATRIX[row, column]
            text_color = "white" if value > CONFUSION_MATRIX.max() / 2 else NAVY
            confusion_axis.text(
                column,
                row,
                f"{value:,}",
                ha="center",
                va="center",
                fontsize=16,
                fontweight="bold",
                color=text_color,
            )
    confusion_axis.tick_params(length=0, colors=DARK_GRAY)
    for spine in confusion_axis.spines.values():
        spine.set_visible(False)

    importance_axis = figure.add_subplot(grid[1, 1])
    feature_names = list(FEATURE_IMPORTANCE)[::-1]
    importance_values = list(FEATURE_IMPORTANCE.values())[::-1]
    importance_bars = importance_axis.barh(
        feature_names, importance_values, color=TEAL
    )
    importance_axis.set_title(
        "4. Explain the selected model",
        loc="left",
        color=NAVY,
        fontweight="bold",
        pad=14,
    )
    importance_axis.set_xlabel("Decrease in average precision when shuffled")
    importance_axis.set_xlim(0, 0.37)
    for bar in importance_bars:
        importance_axis.text(
            bar.get_width() + 0.006,
            bar.get_y() + bar.get_height() / 2,
            f"{bar.get_width():.3f}",
            va="center",
            fontsize=8.5,
            color=DARK_GRAY,
        )
    style_axis(importance_axis)

    figure.text(
        0.07,
        0.035,
        "Portfolio proof of concept using synthetic data. Results demonstrate workflow and tradeoffs—not production readiness or causation.",
        fontsize=9.5,
        color=DARK_GRAY,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    figure.savefig(output_path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close(figure)
    print(f"Saved ML results report to {output_path}")


if __name__ == "__main__":
    create_report()
