"""Tests for deterministic synthetic scaling."""

# pylint: disable=missing-function-docstring

import random

from src.generate_data import (
    generate_scaled_bom,
    generate_scaled_inventory,
    generate_scaled_parts,
)


def test_scaled_parts_are_deterministic_and_unique(seed_parts):
    first = generate_scaled_parts(seed_parts, total_parts=12, rng=random.Random(7))
    second = generate_scaled_parts(seed_parts, total_parts=12, rng=random.Random(7))

    assert first.equals(second)
    assert len(first) == 12
    assert first["part_number"].is_unique
    assert (first["weight_kg"] > 0).all()


def test_scaled_inventory_covers_every_part(seed_parts):
    parts = generate_scaled_parts(seed_parts, total_parts=10)
    inventory = generate_scaled_inventory(parts)

    assert len(inventory) == len(parts)
    assert set(inventory["part_number"]) == set(parts["part_number"])
    assert (inventory[["stock_quantity", "reorder_level"]] >= 0).all().all()


def test_scaled_bom_has_valid_relationships(seed_parts):
    parts = generate_scaled_parts(seed_parts, total_parts=20)
    assemblies, bom = generate_scaled_bom(parts, total_assemblies=4)

    assert len(assemblies) == 4
    assert set(bom["part_number"]).issubset(set(parts["part_number"]))
    assert (bom["quantity"] > 0).all()
    assert not bom.duplicated(["assembly_id", "part_number"]).any()
