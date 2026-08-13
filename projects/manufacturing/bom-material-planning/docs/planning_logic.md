# Material Planning Logic

## Purpose

This document explains how the project converts finished-product demand into
dated purchase recommendations. The calculations are deterministic and
auditable: an analyst can trace every recommendation back through material
netting, BOM requirements, and the original production demand.

## Calculation Sequence

```text
Open production demand
        ↓
Effective BOM revision
        ↓
Gross material requirements
        ↓
Usable inventory
        ↓
Timely open purchase receipts
        ↓
Time-phased net requirements
        ↓
Supplier MOQ and order multiple
        ↓
Recommended order quantity and date
```

## 1. Open Production Demand

Only demand with `Planned` or `Released` status enters material planning.
Completed and cancelled demand is excluded.

Each demand row supplies:

- finished product;
- required quantity;
- required date; and
- planning priority.

## 2. Effective BOM Selection

The engine selects an active BOM whose effective period contains the demand's
required date:

```text
effective_start_date <= required_date
and
effective_end_date is null or effective_end_date >= required_date
```

Planning fails when any open demand lacks an applicable active BOM. This avoids
silently understating material demand.

The first version supports single-level BOMs: each finished product references
purchased raw materials directly.

## 3. Gross Material Requirements

Each demand row is multiplied by every component quantity in its BOM. Expected
process loss is treated as input loss, so the input requirement is grossed up:

```text
gross requirement =
    demand quantity × quantity per unit
    ────────────────────────────────────
       1 - expected loss percentage
```

Example:

```text
31,000 titanium blind bolts
× 0.0125 kg titanium per bolt
÷ (1 - 4% expected loss)
= 403.646 kg gross titanium requirement
```

Requirements are aggregated by material and need date after retaining a
detailed demand-to-component audit dataset.

## 4. Usable Inventory

Inventory is protected before planning:

```text
usable inventory = max(
    on hand
    - reserved
    - restricted
    - safety stock,
    0
)
```

- Reserved inventory is already committed elsewhere.
- Restricted inventory is unavailable because of quality or usage controls.
- Safety stock remains protected against normal planned demand.

Usable balances are aggregated across eligible storage locations by material.

## 5. Scheduled Purchase Receipts

Only `Open` and `Partially Received` purchase-order lines can provide future
supply.

```text
open receipt quantity = ordered quantity - received quantity
```

Received lines are already reflected in inventory, and cancelled lines are
excluded. A scheduled receipt becomes available only when:

```text
expected receipt date <= material need date
```

A late receipt is never allowed to cover an earlier requirement.

## 6. Time-Phased Netting

Requirements are processed chronologically and independently for each
material. The projected balance is updated after every need date:

```text
projected supply before requirement =
    prior projected balance + newly available scheduled receipts

supply applied = min(projected supply, gross requirement)

net requirement = gross requirement - supply applied

projected supply after requirement = projected supply - supply applied
```

Supply consumed on one date cannot be reused. Unused supply carries forward.
When a net requirement is identified, the planning engine assumes purchasing
will cover it before later requirements are processed.

## 7. Supplier Selection

Every short material must have one preferred approved source. The source
provides:

- supplier identity;
- price per base unit;
- calendar-day lead time;
- minimum order quantity (MOQ); and
- required order multiple.

Planning fails instead of generating an incomplete recommendation when an
eligible preferred source is missing.

## 8. Constrained Order Quantity

Positive shortages are first raised to the supplier MOQ, then rounded upward
to the order multiple:

```text
constrained quantity = max(net requirement, MOQ)

recommended quantity =
    ceiling(constrained quantity / order multiple)
    × order multiple
```

Example:

```text
net requirement: 550 kg
MOQ:             500 kg
order multiple:  100 kg
recommended:     600 kg
```

If MOQ or multiple rounding creates excess supply, that excess is carried
forward and offsets later net requirements for the same material. This avoids
recommending the same supply twice.

## 9. Recommended Order Date and Urgency

```text
recommended order date = need date - supplier lead-time days
```

The result is classified relative to the planning date:

| Status | Rule |
|---|---|
| `Past Due` | Recommended order date is before the planning date |
| `Due Today` | Recommended order date equals the planning date |
| `Future` | Recommended order date is after the planning date |

Past-due recommendations indicate that standard supplier lead time no longer
supports the production need. A planner would need to expedite, use an
alternative source, revise the schedule, or accept shortage risk.

## 10. Estimated Purchase Cost

```text
estimated purchase cost = recommended order quantity × preferred unit price
```

This is a planning estimate, not a committed purchase-order value. Freight,
tax, price breaks, and contractual adjustments are outside the first-version
scope.

## Validation Layers

| Layer | Controls |
|---|---|
| PostgreSQL | Types, foreign keys, uniqueness, status and quantity checks |
| Seed data | One active BOM per product and one preferred approved source per material |
| Generators | Valid quantities, status consistency, reproducibility, master IDs |
| Planning SQL | Effective BOM dates, active records, eligible PO statuses |
| Python planning | Chronological supply consumption, MOQ carry-forward, urgency |
| pytest | Formula boundaries, invalid inputs, time ordering, supply reuse, exports |

## Limitations

- BOMs are single-level rather than recursive.
- Lead times are deterministic calendar days.
- Supplier capacity is not modeled.
- Open receipts are trusted at their current expected dates.
- Safety stock is a seeded planning parameter rather than statistically
  optimized.
- Recommendations are advisory and are not written automatically to an ERP.
- Demand is synthetic and does not yet come from a statistical forecast.
