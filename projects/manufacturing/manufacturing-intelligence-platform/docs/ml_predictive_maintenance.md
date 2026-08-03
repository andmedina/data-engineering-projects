# Predictive Maintenance Workflow

## Business Question

Can recent sensor behavior identify elevated risk that a cold-heading machine
will experience a mechanical failure within the next 60 minutes?

This is a synthetic proof of concept for a portfolio project. It demonstrates
the modeling workflow; it is not presented as a production maintenance model.

## Modeling Population

- Machines: `CH-01` and `CH-02`
- Machine family: Cold Heading
- Telemetry frequency: one reading every five minutes
- Historical window: 365 days through the latest completed five-minute interval

Keeping one machine family avoids treating unlike equipment as if it shared
the same operating behavior and failure mechanisms.

## Source Tables and Join Logic

| Table | Purpose | Join |
|---|---|---|
| `sensor_readings` | Predictor measurements | `machine_id` |
| `machines` | Select Cold Heading population | `machine_id` |
| `downtime_events` | Mechanical-failure timestamps and stopped intervals | `machine_id` and time |
| `maintenance_events` | Component associated with a corrective repair | `machine_id` and matching maintenance/downtime start |

The model receives temperature, vibration, power, pressure, and RPM readings.
It also receives trailing 60-minute mean, standard deviation, maximum, and
change features for each measurement.

## Target Definition

`failure_within_60m` equals 1 when a mechanical-failure downtime event begins
more than zero and no more than 60 minutes after a sensor reading. Otherwise,
it equals 0.

Readings taken during an existing downtime interval are removed. This prevents
the model from "predicting" a failure after the machine has already stopped.
Trailing feature windows also restart after each resulting telemetry gap, so a
single feature window never combines readings from before and after downtime.

## Evaluation Design

- Use chronological train, validation, and test periods rather than a random
  row split.
- Train the candidate models on readings before March 1, 2026.
- Select the model and alert threshold using March and April 2026 validation
  data.
- Perform the final evaluation on readings from May 1, 2026 onward.
- Evaluate precision, recall, F1, average precision, and the confusion matrix.
- Treat each mechanical failure as an independent event when discussing sample
  size, even though it creates several positive five-minute readings.

The chronological splits represent the real use case: learn from past data and
predict later events. They also prevent neighboring readings from one failure
window being randomly divided across development and evaluation periods.

## Synthetic Failure Signatures

The generator creates different pre-failure sensor patterns by repaired
component:

- Forming Die: sharply higher vibration and power, with moderately higher
  temperature.
- Hydraulic System: lower pressure with higher temperature and power.
- Feed System: lower RPM and power with higher vibration.

These patterns make the demonstration learnable and interpretable. They are
simulation assumptions, not empirically validated thresholds from a real
factory.

## Important Limitations

- Only two cold-heading machines are modeled.
- There are 13 historical mechanical-failure events as of August 3, 2026.
- The data and failure signals are synthetic, so model performance may appear
  stronger and cleaner than performance on real telemetry.
- Five-minute rows near the same failure are correlated and must not be
  described as independent failures.
- A production model would require more machines, more naturally observed
  failures, domain validation, drift monitoring, and cost-based alert
  thresholds.

The intended portfolio claim is therefore: the project demonstrates a
defensible predictive-maintenance pipeline and evaluation design, not a
production-ready failure prediction system.

## Code Walkthrough

The workflow is deliberately divided into two readable modules:

1. `src/models/predictive_maintenance.py` extracts the four source tables,
   calculates trailing features, creates the target, removes downtime leakage,
   and performs the chronological split.
2. `src/models/train_predictive_maintenance.py` defines the feature list,
   creates three classifiers, fits them, and reports test metrics.

Run the experiment from the repository root while the `data_engineering` Conda
environment and PostgreSQL database are available:

```bash
python -m src.models.train_predictive_maintenance
```

The three models serve different purposes:

- **Logistic Regression** is a simple, interpretable baseline.
- **Random Forest** captures nonlinear rules and interactions between sensors.
- **Histogram Gradient Boosting** builds a sequence of trees that progressively
  correct earlier errors and is efficient on tabular data.

Because failures are rare, accuracy is intentionally not the main reported
metric. A model that predicts "no failure" for every row would have very high
accuracy while providing no operational value. Recall measures how many actual
warning rows were detected, precision measures how many alerts were correct,
and average precision summarizes ranking quality across alert thresholds.

## Experiment Results

The final chronological experiment produced:

- 119,256 training rows, including 84 positive warning rows from 7 failures.
- 34,868 validation rows, including 24 positive warning rows from 2 failures.
- 54,356 test rows, including 36 positive warning rows from 3 failures.
- A test positive-row prevalence of approximately 0.066%.

Each candidate's alert threshold was selected on validation data by maximizing
F1. Model selection used validation average precision:

| Validation model | Threshold | Precision | Recall | F1 | Average precision | ROC AUC |
|---|---:|---:|---:|---:|---:|---:|
| Logistic Regression | 0.994 | 0.031 | 0.167 | 0.052 | 0.009 | 0.744 |
| Random Forest | 0.175 | 0.308 | 0.167 | 0.216 | 0.088 | 0.742 |
| Histogram Gradient Boosting | 0.459 | 0.174 | 0.167 | 0.170 | **0.101** | 0.606 |

Histogram Gradient Boosting had the highest validation average precision and
was therefore selected before examining final test performance. It was refit
on the combined training and validation periods while retaining its fixed
validation threshold of 0.459.

| Final test model | Precision | Recall | F1 | Average precision | ROC AUC |
|---|---:|---:|---:|---:|---:|
| Histogram Gradient Boosting | 0.082 | 0.389 | 0.135 | 0.107 | 0.693 |

The final confusion matrix contained 14 true-positive warning rows, 157 false
positives, 22 false negatives, and 54,163 true negatives. The modest results
are consistent with the extremely small number of independent failure events.
They also demonstrate why model selection and alert-threshold decisions matter
more than headline accuracy for rare-event maintenance prediction.

## Model Interpretation

Permutation importance was calculated for the selected model using every
positive test row and a reproducible sample of 10,000 negative rows. Each
feature was shuffled three times, and importance was measured as the resulting
decrease in average precision.

The ten most influential features were:

| Rank | Feature | Importance |
|---:|---|---:|
| 1 | 60-minute maximum vibration | 0.3327 |
| 2 | Current vibration | 0.1905 |
| 3 | 60-minute mean vibration | 0.1240 |
| 4 | 60-minute maximum temperature | 0.0831 |
| 5 | 60-minute vibration standard deviation | 0.0694 |
| 6 | Current pressure | 0.0678 |
| 7 | 60-minute mean pressure | 0.0664 |
| 8 | 60-minute maximum pressure | 0.0451 |
| 9 | Current RPM | 0.0281 |
| 10 | 60-minute maximum RPM | 0.0235 |

This ordering is consistent with the simulated component signatures: forming
die events create strong vibration behavior, hydraulic events affect pressure,
and feed-system events affect RPM. Permutation importance describes which
features the model relied upon; it does not prove that those measurements
caused a failure.
