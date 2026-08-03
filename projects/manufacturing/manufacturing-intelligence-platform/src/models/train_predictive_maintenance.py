"""Train and compare predictive-maintenance classification models.

Run from the repository root with::

    python -m src.models.train_predictive_maintenance

The script builds features directly from PostgreSQL, compares candidates on a
chronological validation period, and evaluates the selected model on later
test readings. It does not write predictions or models to disk; this first
version is deliberately a reproducible experiment.
"""

import pandas as pd

from sklearn.ensemble import (
    HistGradientBoostingClassifier,
    RandomForestClassifier,
)
from sklearn.impute import SimpleImputer
from sklearn.inspection import permutation_importance
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    average_precision_score,
    confusion_matrix,
    f1_score,
    precision_recall_curve,
    precision_score,
    recall_score,
    roc_auc_score,
)
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from src.analytics.kpis import get_engine
from src.models.predictive_maintenance import (
    SENSOR_COLUMNS,
    build_predictive_maintenance_dataset,
    time_based_split,
)


VALIDATION_START = "2026-03-01"
TEST_START = "2026-05-01"
TARGET_COLUMN = "failure_within_60m"
IMPORTANCE_NEGATIVE_SAMPLE_SIZE = 10_000

# Machine identity and future-failure details are intentionally excluded. The
# model should learn sensor behavior, not memorize a machine or see its label.
FEATURE_COLUMNS = SENSOR_COLUMNS + [
    f"{column}_{stat}_60m"
    for column in SENSOR_COLUMNS
    for stat in ("mean", "std", "max", "change")
]


def create_models():
    """Return three complementary classifiers with imbalance handling.

    Logistic regression supplies an interpretable linear baseline. Random
    forest and histogram gradient boosting can learn nonlinear interactions.
    Class weighting makes rare failure rows matter during fitting without
    deleting the much more common normal-operation rows.
    """
    return {
        "Logistic Regression": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                ("scaler", StandardScaler()),
                (
                    "model",
                    LogisticRegression(
                        class_weight="balanced",
                        max_iter=1_000,
                        random_state=42,
                    ),
                ),
            ]
        ),
        "Random Forest": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    RandomForestClassifier(
                        n_estimators=200,
                        min_samples_leaf=2,
                        class_weight="balanced_subsample",
                        random_state=42,
                        n_jobs=-1,
                    ),
                ),
            ]
        ),
        "Histogram Gradient Boosting": Pipeline(
            [
                ("imputer", SimpleImputer(strategy="median")),
                (
                    "model",
                    HistGradientBoostingClassifier(
                        class_weight="balanced",
                        max_iter=200,
                        random_state=42,
                    ),
                ),
            ]
        ),
    }


def choose_alert_threshold(target, failure_probabilities):
    """Choose the probability cutoff with the highest validation F1 score.

    Threshold selection belongs on validation data. Choosing it from test
    results would make the final evaluation optimistically biased.
    """
    precision, recall, thresholds = precision_recall_curve(
        target, failure_probabilities
    )
    f1_scores = 2 * precision[:-1] * recall[:-1] / (
        precision[:-1] + recall[:-1] + 1e-12
    )
    return float(thresholds[f1_scores.argmax()])


def evaluate_model(model, features, target, threshold=0.50):
    """Calculate rare-event metrics at a specified alert threshold."""
    failure_probabilities = model.predict_proba(features)[:, 1]
    predicted_labels = (failure_probabilities >= threshold).astype(int)

    return {
        "precision": precision_score(target, predicted_labels, zero_division=0),
        "recall": recall_score(target, predicted_labels, zero_division=0),
        "f1": f1_score(target, predicted_labels, zero_division=0),
        "average_precision": average_precision_score(
            target, failure_probabilities
        ),
        "roc_auc": roc_auc_score(target, failure_probabilities),
        "confusion_matrix": confusion_matrix(target, predicted_labels),
    }


def print_dataset_summary(train, validation, test):
    """Print row and independent-event counts needed to interpret results."""
    print("Predictive-maintenance dataset")
    print(f"Training rows: {len(train):,}")
    print(f"Training positive rows: {int(train[TARGET_COLUMN].sum()):,}")
    print(
        "Training failure events: "
        f"{train.loc[train[TARGET_COLUMN].eq(1), 'failure_timestamp'].nunique()}"
    )
    print(f"Validation rows: {len(validation):,}")
    print(
        "Validation positive rows: "
        f"{int(validation[TARGET_COLUMN].sum()):,}"
    )
    print(
        "Validation failure events: "
        f"{validation.loc[validation[TARGET_COLUMN].eq(1), 'failure_timestamp'].nunique()}"
    )
    print(f"Test rows: {len(test):,}")
    print(f"Test positive rows: {int(test[TARGET_COLUMN].sum()):,}")
    print(
        "Test failure events: "
        f"{test.loc[test[TARGET_COLUMN].eq(1), 'failure_timestamp'].nunique()}"
    )


def calculate_permutation_importance(
    model,
    test,
    negative_sample_size=IMPORTANCE_NEGATIVE_SAMPLE_SIZE,
):
    """Estimate feature influence using a manageable test-data sample.

    All rare positive rows are retained. A reproducible sample of normal rows
    limits runtime while preserving enough negative examples to measure the
    change in average precision when each feature is shuffled.
    """
    positives = test[test[TARGET_COLUMN].eq(1)]
    negatives = test[test[TARGET_COLUMN].eq(0)]
    if len(negatives) > negative_sample_size:
        negatives = negatives.sample(negative_sample_size, random_state=42)

    importance_sample = pd.concat(
        [positives, negatives], ignore_index=True
    ).sample(frac=1, random_state=42)
    result = permutation_importance(
        model,
        importance_sample[FEATURE_COLUMNS],
        importance_sample[TARGET_COLUMN],
        scoring="average_precision",
        n_repeats=3,
        random_state=42,
        n_jobs=-1,
    )
    return sorted(
        zip(FEATURE_COLUMNS, result.importances_mean),
        key=lambda item: item[1],
        reverse=True,
    )


def main():
    """Compare three models, select one, and evaluate it on future data."""
    dataset = build_predictive_maintenance_dataset(get_engine())
    development, test = time_based_split(dataset, TEST_START)
    train, validation = time_based_split(development, VALIDATION_START)
    print_dataset_summary(train, validation, test)

    train_features = train[FEATURE_COLUMNS]
    train_target = train[TARGET_COLUMN]
    validation_features = validation[FEATURE_COLUMNS]
    validation_target = validation[TARGET_COLUMN]
    test_features = test[FEATURE_COLUMNS]
    test_target = test[TARGET_COLUMN]

    candidates = {}
    print("\nValidation comparison")
    for model_name, model in create_models().items():
        model.fit(train_features, train_target)
        validation_probabilities = model.predict_proba(validation_features)[:, 1]
        threshold = choose_alert_threshold(
            validation_target, validation_probabilities
        )
        metrics = evaluate_model(
            model,
            validation_features,
            validation_target,
            threshold=threshold,
        )
        candidates[model_name] = {
            "model": model,
            "threshold": threshold,
            "average_precision": metrics["average_precision"],
        }

        print(f"\n{model_name}")
        print(f"Selected threshold: {threshold:.3f}")
        print(f"Precision: {metrics['precision']:.3f}")
        print(f"Recall: {metrics['recall']:.3f}")
        print(f"F1: {metrics['f1']:.3f}")
        print(f"Average precision: {metrics['average_precision']:.3f}")
        print(f"ROC AUC: {metrics['roc_auc']:.3f}")
        print("Confusion matrix:")
        print(metrics["confusion_matrix"])

    selected_name = max(
        candidates,
        key=lambda name: candidates[name]["average_precision"],
    )
    selected = candidates[selected_name]

    # After model and threshold selection, refit the chosen model on all data
    # available before the test period. The test labels remain untouched.
    final_model = create_models()[selected_name]
    final_model.fit(development[FEATURE_COLUMNS], development[TARGET_COLUMN])
    test_metrics = evaluate_model(
        final_model,
        test_features,
        test_target,
        threshold=selected["threshold"],
    )

    print(f"\nSelected model: {selected_name}")
    print(f"Fixed validation threshold: {selected['threshold']:.3f}")
    print("Final untouched test results")
    print(f"Precision: {test_metrics['precision']:.3f}")
    print(f"Recall: {test_metrics['recall']:.3f}")
    print(f"F1: {test_metrics['f1']:.3f}")
    print(f"Average precision: {test_metrics['average_precision']:.3f}")
    print(f"ROC AUC: {test_metrics['roc_auc']:.3f}")
    print("Confusion matrix:")
    print(test_metrics["confusion_matrix"])

    feature_importance = calculate_permutation_importance(final_model, test)
    print("\nTop 10 permutation feature importances")
    for feature_name, importance in feature_importance[:10]:
        print(f"{feature_name}: {importance:.4f}")


if __name__ == "__main__":
    main()
