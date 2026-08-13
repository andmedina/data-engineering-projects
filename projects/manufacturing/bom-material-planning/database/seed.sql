-- ============================================================================
-- BOM Material Planning
-- Master-data seed
-- ============================================================================
-- Seeds the six master-data tables used by the planning engine:
--   products, materials, suppliers, bills_of_materials, bom_components,
--   supplier_materials
--
-- Relationship inserts use business-code lookups instead of assumed identity
-- values. ON CONFLICT clauses make the seed safe to rerun.
-- ============================================================================

BEGIN;

-- ============================================================================
-- PRODUCTS
-- ============================================================================

INSERT INTO products (
    product_code,
    product_name,
    product_family,
    base_unit_of_measure,
    active_flag
)
VALUES
    ('SR-AD-316', '3/16 in 2117-T4 Aluminum Solid Rivet', 'Solid Rivet', 'EA', TRUE),
    ('BR-MO-316', '3/16 in Monel Blind Rivet', 'Blind Rivet', 'EA', TRUE),
    ('BB-TI-250', '1/4 in Titanium Blind Bolt', 'Blind Bolt', 'EA', TRUE),
    ('TI-SS-M6', 'M6 Stainless Steel Threaded Insert', 'Threaded Insert', 'EA', TRUE),
    ('TF-ST-316', '3/16 in Alloy Steel Temporary Fastener', 'Temporary Fastener', 'EA', TRUE),
    ('SR-AD-250', '1/4 in 7050 Aluminum Solid Rivet', 'Solid Rivet', 'EA', TRUE)
ON CONFLICT (product_code) DO NOTHING;


-- ============================================================================
-- MATERIALS
-- ============================================================================

INSERT INTO materials (
    material_code,
    material_name,
    material_category,
    base_unit_of_measure,
    standard_unit_cost,
    active_flag
)
VALUES
    ('MAT-AL2117-WR', '2117-T4 Aluminum Wire', 'Metal Wire', 'KG', 8.7500, TRUE),
    ('MAT-AL7050-WR', '7050-T73 Aluminum Wire', 'Metal Wire', 'KG', 14.6000, TRUE),
    ('MAT-MONEL-WR', 'Monel 400 Wire', 'Metal Wire', 'KG', 31.2500, TRUE),
    ('MAT-TI6AL4V-WR', 'Ti-6Al-4V Titanium Wire', 'Metal Wire', 'KG', 46.8000, TRUE),
    ('MAT-SS286-WR', 'A-286 Stainless Steel Wire', 'Metal Wire', 'KG', 24.9000, TRUE),
    ('MAT-4140-WR', '4140 Alloy Steel Wire', 'Metal Wire', 'KG', 6.9500, TRUE),
    ('CHEM-ZN-NI', 'Zinc-Nickel Plating Solution', 'Process Chemical', 'L', 18.4000, TRUE),
    ('CHEM-PASS', 'Nitric Passivation Solution', 'Process Chemical', 'L', 12.7500, TRUE),
    ('PKG-TRAY-100', 'Reusable Fastener Tray - 100 Unit', 'Packaging', 'EA', 1.1500, TRUE),
    ('PKG-CARTON-1K', 'Aerospace Fastener Carton - 1,000 Unit', 'Packaging', 'EA', 2.8500, TRUE)
ON CONFLICT (material_code) DO NOTHING;


-- ============================================================================
-- SUPPLIERS
-- ============================================================================

INSERT INTO suppliers (
    supplier_code,
    supplier_name,
    supplier_status,
    quality_rating,
    on_time_delivery_pct
)
VALUES
    ('SUP-ALPHA', 'Alpha Aerospace Metals', 'Approved', 96.50, 94.20),
    ('SUP-PACIFIC', 'Pacific Specialty Alloys', 'Approved', 94.80, 91.70),
    ('SUP-TITAN', 'Titanium Source Partners', 'Approved', 98.10, 92.40),
    ('SUP-CHEM', 'AeroChem Process Materials', 'Approved', 95.20, 96.10),
    ('SUP-PACK', 'Precision Industrial Packaging', 'Approved', 93.70, 97.30),
    ('SUP-RESERVE', 'Western Metals Reserve', 'Conditional', 88.40, 86.50)
ON CONFLICT (supplier_code) DO NOTHING;


-- ============================================================================
-- BILL-OF-MATERIAL HEADERS
-- ============================================================================

INSERT INTO bills_of_materials (
    product_id,
    revision_code,
    effective_start_date,
    effective_end_date,
    bom_status
)
SELECT
    product_id,
    'A',
    DATE '2026-01-01',
    NULL,
    'Active'
FROM products
WHERE product_code IN (
    'SR-AD-316',
    'BR-MO-316',
    'BB-TI-250',
    'TI-SS-M6',
    'TF-ST-316',
    'SR-AD-250'
)
ON CONFLICT (product_id, revision_code) DO NOTHING;


-- ============================================================================
-- BOM COMPONENTS
-- ============================================================================
-- quantity_per_unit uses each material's base unit. Packaging quantities are
-- fractional because trays and cartons hold multiple finished fasteners.
-- ============================================================================

WITH component_seed (
    product_code,
    line_number,
    material_code,
    quantity_per_unit,
    expected_loss_pct
) AS (
    VALUES
        -- 3/16 in aluminum solid rivet
        ('SR-AD-316', 1, 'MAT-AL2117-WR', 0.001850::NUMERIC, 2.000::NUMERIC),
        ('SR-AD-316', 2, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('SR-AD-316', 3, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC),

        -- Monel blind rivet
        ('BR-MO-316', 1, 'MAT-MONEL-WR', 0.003800::NUMERIC, 3.000::NUMERIC),
        ('BR-MO-316', 2, 'CHEM-PASS', 0.000080::NUMERIC, 5.000::NUMERIC),
        ('BR-MO-316', 3, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('BR-MO-316', 4, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC),

        -- Titanium blind bolt
        ('BB-TI-250', 1, 'MAT-TI6AL4V-WR', 0.012500::NUMERIC, 4.000::NUMERIC),
        ('BB-TI-250', 2, 'CHEM-PASS', 0.000120::NUMERIC, 5.000::NUMERIC),
        ('BB-TI-250', 3, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('BB-TI-250', 4, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC),

        -- Stainless threaded insert
        ('TI-SS-M6', 1, 'MAT-SS286-WR', 0.008200::NUMERIC, 3.500::NUMERIC),
        ('TI-SS-M6', 2, 'CHEM-PASS', 0.000100::NUMERIC, 5.000::NUMERIC),
        ('TI-SS-M6', 3, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('TI-SS-M6', 4, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC),

        -- Alloy-steel temporary fastener
        ('TF-ST-316', 1, 'MAT-4140-WR', 0.006500::NUMERIC, 3.000::NUMERIC),
        ('TF-ST-316', 2, 'CHEM-ZN-NI', 0.000180::NUMERIC, 6.000::NUMERIC),
        ('TF-ST-316', 3, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('TF-ST-316', 4, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC),

        -- 1/4 in 7050 aluminum solid rivet
        ('SR-AD-250', 1, 'MAT-AL7050-WR', 0.003250::NUMERIC, 2.500::NUMERIC),
        ('SR-AD-250', 2, 'PKG-TRAY-100', 0.010000::NUMERIC, 0.500::NUMERIC),
        ('SR-AD-250', 3, 'PKG-CARTON-1K', 0.001000::NUMERIC, 0.500::NUMERIC)
)
INSERT INTO bom_components (
    bom_id,
    line_number,
    material_id,
    quantity_per_unit,
    expected_loss_pct
)
SELECT
    bom.bom_id,
    seed.line_number,
    material.material_id,
    seed.quantity_per_unit,
    seed.expected_loss_pct
FROM component_seed AS seed
JOIN products AS product
    ON product.product_code = seed.product_code
JOIN bills_of_materials AS bom
    ON bom.product_id = product.product_id
    AND bom.revision_code = 'A'
JOIN materials AS material
    ON material.material_code = seed.material_code
ON CONFLICT (bom_id, material_id) DO NOTHING;


-- ============================================================================
-- SUPPLIER-MATERIAL SOURCING RULES
-- ============================================================================
-- Every active material has exactly one preferred approved source. Selected
-- materials also have a nonpreferred alternative to support later sourcing
-- comparisons.
-- ============================================================================

WITH sourcing_seed (
    supplier_code,
    material_code,
    supplier_material_code,
    unit_price,
    lead_time_days,
    minimum_order_quantity,
    order_multiple,
    preferred_flag,
    source_status
) AS (
    VALUES
        ('SUP-ALPHA', 'MAT-AL2117-WR', 'AA-2117-T4-WR', 8.6000::NUMERIC, 21, 500.000::NUMERIC, 100.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-PACIFIC', 'MAT-AL2117-WR', 'PSA-2117-WIRE', 8.3500::NUMERIC, 28, 750.000::NUMERIC, 250.000::NUMERIC, FALSE, 'Approved'),

        ('SUP-PACIFIC', 'MAT-AL7050-WR', 'PSA-7050-WIRE', 14.3000::NUMERIC, 35, 400.000::NUMERIC, 100.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-RESERVE', 'MAT-AL7050-WR', 'WMR-7050-WR', 15.1000::NUMERIC, 42, 300.000::NUMERIC, 100.000::NUMERIC, FALSE, 'Conditional'),

        ('SUP-PACIFIC', 'MAT-MONEL-WR', 'PSA-M400-WIRE', 30.9000::NUMERIC, 42, 250.000::NUMERIC, 50.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-RESERVE', 'MAT-MONEL-WR', 'WMR-MONEL400', 32.4000::NUMERIC, 49, 200.000::NUMERIC, 50.000::NUMERIC, FALSE, 'Conditional'),

        ('SUP-TITAN', 'MAT-TI6AL4V-WR', 'TSP-TI64-WIRE', 46.2500::NUMERIC, 56, 200.000::NUMERIC, 25.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-RESERVE', 'MAT-TI6AL4V-WR', 'WMR-TI64-WR', 49.8000::NUMERIC, 70, 150.000::NUMERIC, 25.000::NUMERIC, FALSE, 'Conditional'),

        ('SUP-ALPHA', 'MAT-SS286-WR', 'AA-A286-WIRE', 24.5000::NUMERIC, 35, 300.000::NUMERIC, 50.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-RESERVE', 'MAT-SS286-WR', 'WMR-A286-WR', 26.1000::NUMERIC, 45, 200.000::NUMERIC, 50.000::NUMERIC, FALSE, 'Conditional'),

        ('SUP-ALPHA', 'MAT-4140-WR', 'AA-4140-WIRE', 6.8000::NUMERIC, 18, 750.000::NUMERIC, 250.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-PACIFIC', 'MAT-4140-WR', 'PSA-4140-WR', 6.6000::NUMERIC, 25, 1000.000::NUMERIC, 250.000::NUMERIC, FALSE, 'Approved'),

        ('SUP-CHEM', 'CHEM-ZN-NI', 'AC-ZNNI-20L', 18.1000::NUMERIC, 14, 100.000::NUMERIC, 20.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-CHEM', 'CHEM-PASS', 'AC-NITRIC-PASS', 12.5000::NUMERIC, 10, 80.000::NUMERIC, 20.000::NUMERIC, TRUE, 'Approved'),

        ('SUP-PACK', 'PKG-TRAY-100', 'PIP-TRAY100', 1.1000::NUMERIC, 12, 500.000::NUMERIC, 100.000::NUMERIC, TRUE, 'Approved'),
        ('SUP-PACK', 'PKG-CARTON-1K', 'PIP-CARTON1K', 2.7500::NUMERIC, 10, 250.000::NUMERIC, 50.000::NUMERIC, TRUE, 'Approved')
)
INSERT INTO supplier_materials (
    supplier_id,
    material_id,
    supplier_material_code,
    unit_price,
    lead_time_days,
    minimum_order_quantity,
    order_multiple,
    preferred_flag,
    source_status
)
SELECT
    supplier.supplier_id,
    material.material_id,
    seed.supplier_material_code,
    seed.unit_price,
    seed.lead_time_days,
    seed.minimum_order_quantity,
    seed.order_multiple,
    seed.preferred_flag,
    seed.source_status
FROM sourcing_seed AS seed
JOIN suppliers AS supplier
    ON supplier.supplier_code = seed.supplier_code
JOIN materials AS material
    ON material.material_code = seed.material_code
ON CONFLICT (supplier_id, material_id) DO NOTHING;

COMMIT;
