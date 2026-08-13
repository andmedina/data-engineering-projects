-- ============================================================================
-- BOM Material Planning
-- PostgreSQL schema
-- ============================================================================
--
-- This schema supports deterministic, single-level BOM material planning.
-- Run it inside a dedicated PostgreSQL database for this project.
--
-- The tables are created in dependency order:
--   master data -> BOM structure -> sourcing -> planning transactions
-- ============================================================================

BEGIN;

-- ============================================================================
-- MASTER DATA
-- ============================================================================

CREATE TABLE products (
    product_id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_code            VARCHAR(30) NOT NULL,
    product_name            VARCHAR(120) NOT NULL,
    product_family          VARCHAR(60) NOT NULL,
    base_unit_of_measure    VARCHAR(10) NOT NULL DEFAULT 'EA',
    active_flag             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_products_product_code
        UNIQUE (product_code),
    CONSTRAINT ck_products_code_not_blank
        CHECK (BTRIM(product_code) <> ''),
    CONSTRAINT ck_products_name_not_blank
        CHECK (BTRIM(product_name) <> ''),
    CONSTRAINT ck_products_family_not_blank
        CHECK (BTRIM(product_family) <> ''),
    CONSTRAINT ck_products_unit_of_measure
        CHECK (base_unit_of_measure = 'EA')
);


CREATE TABLE materials (
    material_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    material_code           VARCHAR(30) NOT NULL,
    material_name           VARCHAR(120) NOT NULL,
    material_category       VARCHAR(60) NOT NULL,
    base_unit_of_measure    VARCHAR(10) NOT NULL,
    standard_unit_cost      NUMERIC(14, 4) NOT NULL,
    active_flag             BOOLEAN NOT NULL DEFAULT TRUE,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_materials_material_code
        UNIQUE (material_code),
    CONSTRAINT ck_materials_code_not_blank
        CHECK (BTRIM(material_code) <> ''),
    CONSTRAINT ck_materials_name_not_blank
        CHECK (BTRIM(material_name) <> ''),
    CONSTRAINT ck_materials_category_not_blank
        CHECK (BTRIM(material_category) <> ''),
    CONSTRAINT ck_materials_unit_of_measure
        CHECK (base_unit_of_measure IN ('KG', 'L', 'EA')),
    CONSTRAINT ck_materials_standard_unit_cost
        CHECK (standard_unit_cost >= 0)
);


CREATE TABLE suppliers (
    supplier_id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_code           VARCHAR(20) NOT NULL,
    supplier_name           VARCHAR(120) NOT NULL,
    supplier_status         VARCHAR(20) NOT NULL,
    quality_rating          NUMERIC(5, 2) NOT NULL,
    on_time_delivery_pct    NUMERIC(5, 2) NOT NULL,
    created_at              TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT uq_suppliers_supplier_code
        UNIQUE (supplier_code),
    CONSTRAINT ck_suppliers_code_not_blank
        CHECK (BTRIM(supplier_code) <> ''),
    CONSTRAINT ck_suppliers_name_not_blank
        CHECK (BTRIM(supplier_name) <> ''),
    CONSTRAINT ck_suppliers_status
        CHECK (supplier_status IN ('Approved', 'Conditional', 'Inactive')),
    CONSTRAINT ck_suppliers_quality_rating
        CHECK (quality_rating BETWEEN 0 AND 100),
    CONSTRAINT ck_suppliers_on_time_delivery
        CHECK (on_time_delivery_pct BETWEEN 0 AND 100)
);


-- ============================================================================
-- BILL OF MATERIALS
-- ============================================================================

CREATE TABLE bills_of_materials (
    bom_id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id               BIGINT NOT NULL,
    revision_code            VARCHAR(20) NOT NULL,
    effective_start_date     DATE NOT NULL,
    effective_end_date       DATE,
    bom_status               VARCHAR(20) NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_bills_of_materials_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id),
    CONSTRAINT uq_bills_of_materials_product_revision
        UNIQUE (product_id, revision_code),
    CONSTRAINT ck_bills_of_materials_revision_not_blank
        CHECK (BTRIM(revision_code) <> ''),
    CONSTRAINT ck_bills_of_materials_effective_dates
        CHECK (
            effective_end_date IS NULL
            OR effective_end_date >= effective_start_date
        ),
    CONSTRAINT ck_bills_of_materials_status
        CHECK (bom_status IN ('Draft', 'Active', 'Obsolete'))
);


CREATE TABLE bom_components (
    bom_component_id         BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    bom_id                   BIGINT NOT NULL,
    line_number              INTEGER NOT NULL,
    material_id              BIGINT NOT NULL,
    quantity_per_unit        NUMERIC(16, 6) NOT NULL,
    expected_loss_pct        NUMERIC(6, 3) NOT NULL DEFAULT 0,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_bom_components_bom
        FOREIGN KEY (bom_id)
        REFERENCES bills_of_materials (bom_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_bom_components_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),
    CONSTRAINT uq_bom_components_line
        UNIQUE (bom_id, line_number),
    CONSTRAINT uq_bom_components_material
        UNIQUE (bom_id, material_id),
    CONSTRAINT ck_bom_components_line_number
        CHECK (line_number > 0),
    CONSTRAINT ck_bom_components_quantity_per_unit
        CHECK (quantity_per_unit > 0),
    CONSTRAINT ck_bom_components_expected_loss
        CHECK (expected_loss_pct >= 0 AND expected_loss_pct < 100)
);


-- ============================================================================
-- MATERIAL SOURCING
-- ============================================================================

CREATE TABLE supplier_materials (
    supplier_material_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    supplier_id              BIGINT NOT NULL,
    material_id              BIGINT NOT NULL,
    supplier_material_code   VARCHAR(40) NOT NULL,
    unit_price               NUMERIC(14, 4) NOT NULL,
    lead_time_days           INTEGER NOT NULL,
    minimum_order_quantity   NUMERIC(16, 3) NOT NULL,
    order_multiple           NUMERIC(16, 3) NOT NULL,
    preferred_flag           BOOLEAN NOT NULL DEFAULT FALSE,
    source_status            VARCHAR(20) NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_supplier_materials_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id),
    CONSTRAINT fk_supplier_materials_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),
    CONSTRAINT uq_supplier_materials_supplier_material
        UNIQUE (supplier_id, material_id),
    CONSTRAINT ck_supplier_materials_code_not_blank
        CHECK (BTRIM(supplier_material_code) <> ''),
    CONSTRAINT ck_supplier_materials_unit_price
        CHECK (unit_price >= 0),
    CONSTRAINT ck_supplier_materials_lead_time
        CHECK (lead_time_days >= 0),
    CONSTRAINT ck_supplier_materials_minimum_order
        CHECK (minimum_order_quantity >= 0),
    CONSTRAINT ck_supplier_materials_order_multiple
        CHECK (order_multiple > 0),
    CONSTRAINT ck_supplier_materials_status
        CHECK (source_status IN ('Approved', 'Conditional', 'Inactive'))
);

-- At most one preferred active sourcing option may exist for each material.
-- Seed/load validation will also require one for every active material.
CREATE UNIQUE INDEX uq_supplier_materials_preferred_material
    ON supplier_materials (material_id)
    WHERE preferred_flag = TRUE AND source_status = 'Approved';


-- ============================================================================
-- PLANNING AND TRANSACTIONAL DATA
-- ============================================================================

CREATE TABLE production_demand (
    demand_id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    demand_reference         VARCHAR(40) NOT NULL,
    product_id               BIGINT NOT NULL,
    required_date            DATE NOT NULL,
    demand_quantity          NUMERIC(16, 3) NOT NULL,
    demand_status            VARCHAR(20) NOT NULL,
    priority                 VARCHAR(15) NOT NULL DEFAULT 'Standard',
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_production_demand_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id),
    CONSTRAINT uq_production_demand_reference
        UNIQUE (demand_reference),
    CONSTRAINT ck_production_demand_reference_not_blank
        CHECK (BTRIM(demand_reference) <> ''),
    CONSTRAINT ck_production_demand_quantity
        CHECK (demand_quantity > 0),
    CONSTRAINT ck_production_demand_status
        CHECK (
            demand_status IN ('Planned', 'Released', 'Completed', 'Cancelled')
        ),
    CONSTRAINT ck_production_demand_priority
        CHECK (priority IN ('Standard', 'High', 'Critical'))
);


CREATE TABLE inventory_balances (
    inventory_balance_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    material_id              BIGINT NOT NULL,
    location_code            VARCHAR(20) NOT NULL,
    on_hand_quantity         NUMERIC(16, 3) NOT NULL,
    reserved_quantity        NUMERIC(16, 3) NOT NULL DEFAULT 0,
    restricted_quantity      NUMERIC(16, 3) NOT NULL DEFAULT 0,
    safety_stock_quantity    NUMERIC(16, 3) NOT NULL DEFAULT 0,
    last_counted_at           TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_inventory_balances_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),
    CONSTRAINT uq_inventory_balances_material_location
        UNIQUE (material_id, location_code),
    CONSTRAINT ck_inventory_balances_location_not_blank
        CHECK (BTRIM(location_code) <> ''),
    CONSTRAINT ck_inventory_balances_on_hand
        CHECK (on_hand_quantity >= 0),
    CONSTRAINT ck_inventory_balances_reserved
        CHECK (reserved_quantity >= 0),
    CONSTRAINT ck_inventory_balances_restricted
        CHECK (restricted_quantity >= 0),
    CONSTRAINT ck_inventory_balances_safety_stock
        CHECK (safety_stock_quantity >= 0),
    CONSTRAINT ck_inventory_balances_allocated_not_above_on_hand
        CHECK (reserved_quantity + restricted_quantity <= on_hand_quantity)
);


CREATE TABLE purchase_orders (
    purchase_order_id        BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    purchase_order_number    VARCHAR(30) NOT NULL,
    line_number              INTEGER NOT NULL,
    supplier_id              BIGINT NOT NULL,
    material_id              BIGINT NOT NULL,
    order_date               DATE NOT NULL,
    expected_receipt_date    DATE NOT NULL,
    ordered_quantity         NUMERIC(16, 3) NOT NULL,
    received_quantity        NUMERIC(16, 3) NOT NULL DEFAULT 0,
    unit_price               NUMERIC(14, 4) NOT NULL,
    purchase_order_status    VARCHAR(20) NOT NULL,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_purchase_orders_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id),
    CONSTRAINT fk_purchase_orders_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),
    CONSTRAINT uq_purchase_orders_number_line
        UNIQUE (purchase_order_number, line_number),
    CONSTRAINT ck_purchase_orders_number_not_blank
        CHECK (BTRIM(purchase_order_number) <> ''),
    CONSTRAINT ck_purchase_orders_line_number
        CHECK (line_number > 0),
    CONSTRAINT ck_purchase_orders_receipt_date
        CHECK (expected_receipt_date >= order_date),
    CONSTRAINT ck_purchase_orders_ordered_quantity
        CHECK (ordered_quantity > 0),
    CONSTRAINT ck_purchase_orders_received_quantity
        CHECK (
            received_quantity >= 0
            AND received_quantity <= ordered_quantity
        ),
    CONSTRAINT ck_purchase_orders_unit_price
        CHECK (unit_price >= 0),
    CONSTRAINT ck_purchase_orders_status
        CHECK (
            purchase_order_status IN (
                'Open',
                'Partially Received',
                'Received',
                'Cancelled'
            )
        ),
    CONSTRAINT ck_purchase_orders_status_quantity_consistency
        CHECK (
            (purchase_order_status = 'Open' AND received_quantity = 0)
            OR (
                purchase_order_status = 'Partially Received'
                AND received_quantity > 0
                AND received_quantity < ordered_quantity
            )
            OR (
                purchase_order_status = 'Received'
                AND received_quantity = ordered_quantity
            )
            OR purchase_order_status = 'Cancelled'
        )
);


-- ============================================================================
-- PLANNING-PATH INDEXES
-- ============================================================================

CREATE INDEX idx_bills_of_materials_product_effective
    ON bills_of_materials (
        product_id,
        bom_status,
        effective_start_date,
        effective_end_date
    );

CREATE INDEX idx_bom_components_bom
    ON bom_components (bom_id);

CREATE INDEX idx_supplier_materials_material_source
    ON supplier_materials (material_id, source_status, preferred_flag);

CREATE INDEX idx_production_demand_open_required_date
    ON production_demand (required_date, product_id)
    WHERE demand_status IN ('Planned', 'Released');

CREATE INDEX idx_inventory_balances_material
    ON inventory_balances (material_id);

CREATE INDEX idx_purchase_orders_open_receipts
    ON purchase_orders (material_id, expected_receipt_date)
    WHERE purchase_order_status IN ('Open', 'Partially Received');

COMMIT;
