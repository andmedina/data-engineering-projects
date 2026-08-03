"""Tests for model evaluation decisions that must remain reproducible."""

import numpy as np

from src.models.train_predictive_maintenance import choose_alert_threshold


def test_alert_threshold_is_selected_from_validation_probabilities():
    target = np.array([0, 0, 1, 1])
    probabilities = np.array([0.05, 0.20, 0.70, 0.90])

    threshold = choose_alert_threshold(target, probabilities)

    assert threshold == 0.70
