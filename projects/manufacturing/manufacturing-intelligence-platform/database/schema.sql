-- ============================================================
-- Manufacturing Intelligence Platform
-- PostgreSQL Operational Database Schema
-- ============================================================

-- This schema models a simulated aerospace-fastener
-- manufacturing environment.
--
-- Main domains:
--   1. Master data
--   2. Order management
--   3. Material traceability
--   4. Manufacturing execution
--   5. Quality management
--   6. Equipment management
-- ============================================================


-- ============================================================
-- CLEANUP
-- ============================================================

DROP TABLE IF EXISTS sensor_readings CASCADE;
DROP TABLE IF EXISTS maintenance_events CASCADE;
DROP TABLE IF EXISTS downtime_events CASCADE;
DROP TABLE IF EXISTS quality_defects CASCADE;
DROP TABLE IF EXISTS quality_inspections CASCADE;
DROP TABLE IF EXISTS production_runs CASCADE;
DROP TABLE IF EXISTS production_order_materials CASCADE;
DROP TABLE IF EXISTS material_lots CASCADE;
DROP TABLE IF EXISTS production_orders CASCADE;
DROP TABLE IF EXISTS customer_order_items CASCADE;
DROP TABLE IF EXISTS customer_orders CASCADE;
DROP TABLE IF EXISTS defect_types CASCADE;
DROP TABLE IF EXISTS operators CASCADE;
DROP TABLE IF EXISTS machines CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS materials CASCADE;
DROP TABLE IF EXISTS suppliers CASCADE;
DROP TABLE IF EXISTS customers CASCADE;


-- ============================================================
-- 1. MASTER DATA
-- ============================================================


-- ------------------------------------------------------------
-- CUSTOMERS
-- One row per customer organization.
-- ------------------------------------------------------------

CREATE TABLE customers (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_name VARCHAR(150) NOT NULL,
    customer_type VARCHAR(50) NOT NULL,
    country VARCHAR(100) NOT NULL,
    industry_segment VARCHAR(100) NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_customers
        PRIMARY KEY (customer_id),

    CONSTRAINT uq_customers_name
        UNIQUE (customer_name),

    CONSTRAINT chk_customers_type
        CHECK (
            customer_type IN (
                'OEM',
                'Tier 1 Supplier',
                'Tier 2 Supplier',
                'Distributor',
                'Government',
                'Other'
            )
        ),

    CONSTRAINT chk_customers_industry
        CHECK (
            industry_segment IN (
                'Commercial Aviation',
                'Defense',
                'Space',
                'Maintenance Repair and Overhaul',
                'Distribution',
                'Other'
            )
        )
);


-- ------------------------------------------------------------
-- SUPPLIERS
-- One row per supplier.
-- ------------------------------------------------------------

CREATE TABLE suppliers (
    supplier_id BIGINT GENERATED ALWAYS AS IDENTITY,
    supplier_name VARCHAR(150) NOT NULL,
    supplier_category VARCHAR(75) NOT NULL,
    country VARCHAR(100) NOT NULL,
    approved_status VARCHAR(30) NOT NULL DEFAULT 'Approved',
    quality_rating NUMERIC(5,2),
    average_lead_time_days INTEGER,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_suppliers
        PRIMARY KEY (supplier_id),

    CONSTRAINT uq_suppliers_name
        UNIQUE (supplier_name),

    CONSTRAINT chk_suppliers_category
        CHECK (
            supplier_category IN (
                'Raw Material',
                'Coating',
                'Tooling',
                'Component',
                'Packaging',
                'Maintenance',
                'Other'
            )
        ),

    CONSTRAINT chk_suppliers_approved_status
        CHECK (
            approved_status IN (
                'Approved',
                'Conditional',
                'Suspended',
                'Inactive'
            )
        ),

    CONSTRAINT chk_suppliers_quality_rating
        CHECK (
            quality_rating IS NULL
            OR quality_rating BETWEEN 0 AND 100
        ),

    CONSTRAINT chk_suppliers_lead_time
        CHECK (
            average_lead_time_days IS NULL
            OR average_lead_time_days >= 0
        )
);


-- ------------------------------------------------------------
-- MATERIALS
-- One row per raw-material specification.
-- ------------------------------------------------------------

CREATE TABLE materials (
    material_id BIGINT GENERATED ALWAYS AS IDENTITY,
    material_code VARCHAR(50) NOT NULL,
    material_name VARCHAR(150) NOT NULL,
    material_category VARCHAR(50) NOT NULL,
    alloy VARCHAR(100),
    material_form VARCHAR(50) NOT NULL,
    unit_of_measure VARCHAR(25) NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_materials
        PRIMARY KEY (material_id),

    CONSTRAINT uq_materials_code
        UNIQUE (material_code),

    CONSTRAINT chk_materials_category
        CHECK (
            material_category IN (
                'Aluminum',
                'Titanium',
                'Stainless Steel',
                'Alloy Steel',
                'Nickel Alloy',
                'Other'
            )
        ),

    CONSTRAINT chk_materials_form
        CHECK (
            material_form IN (
                'Wire',
                'Rod',
                'Bar',
                'Sheet',
                'Coil',
                'Component',
                'Other'
            )
        ),

    CONSTRAINT chk_materials_uom
        CHECK (
            unit_of_measure IN (
                'lb',
                'kg',
                'ft',
                'in',
                'unit'
            )
        )
);


-- ------------------------------------------------------------
-- PRODUCTS
-- One row per manufactured part number.
-- ------------------------------------------------------------

CREATE TABLE products (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY,
    part_number VARCHAR(75) NOT NULL,
    product_name VARCHAR(150) NOT NULL,
    product_family VARCHAR(75) NOT NULL,
    material_id BIGINT NOT NULL,
    diameter_in NUMERIC(8,4),
    length_in NUMERIC(8,4),
    finish_type VARCHAR(75),
    aerospace_specification VARCHAR(100),
    standard_cycle_time_seconds NUMERIC(10,3) NOT NULL,
    standard_unit_cost NUMERIC(12,4) NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_products
        PRIMARY KEY (product_id),

    CONSTRAINT uq_products_part_number
        UNIQUE (part_number),

    CONSTRAINT fk_products_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),

    CONSTRAINT chk_products_family
        CHECK (
            product_family IN (
                'Solid Rivet',
                'Blind Rivet',
                'Blind Bolt',
                'Temporary Fastener',
                'Threaded Insert',
                'Installation Tool',
                'Other'
            )
        ),

    CONSTRAINT chk_products_diameter
        CHECK (
            diameter_in IS NULL
            OR diameter_in > 0
        ),

    CONSTRAINT chk_products_length
        CHECK (
            length_in IS NULL
            OR length_in > 0
        ),

    CONSTRAINT chk_products_cycle_time
        CHECK (
            standard_cycle_time_seconds > 0
        ),

    CONSTRAINT chk_products_unit_cost
        CHECK (
            standard_unit_cost >= 0
        )
);


-- ------------------------------------------------------------
-- MACHINES
-- One row per manufacturing machine.
-- ------------------------------------------------------------

CREATE TABLE machines (
    machine_id BIGINT GENERATED ALWAYS AS IDENTITY,
    machine_code VARCHAR(50) NOT NULL,
    machine_name VARCHAR(150) NOT NULL,
    operation_type VARCHAR(75) NOT NULL,
    production_line VARCHAR(75) NOT NULL,
    manufacturer VARCHAR(100),
    model VARCHAR(100),
    install_date DATE,
    rated_capacity_per_hour NUMERIC(12,2),
    status VARCHAR(30) NOT NULL DEFAULT 'Active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_machines
        PRIMARY KEY (machine_id),

    CONSTRAINT uq_machines_code
        UNIQUE (machine_code),

    CONSTRAINT chk_machines_operation_type
        CHECK (
            operation_type IN (
                'Cold Heading',
                'Thread Rolling',
                'Heat Treatment',
                'Surface Finishing',
                'Assembly',
                'Inspection',
                'Packaging',
                'Multi-Purpose'
            )
        ),

    CONSTRAINT chk_machines_capacity
        CHECK (
            rated_capacity_per_hour IS NULL
            OR rated_capacity_per_hour > 0
        ),

    CONSTRAINT chk_machines_status
        CHECK (
            status IN (
                'Active',
                'Idle',
                'Maintenance',
                'Retired'
            )
        )
);


-- ------------------------------------------------------------
-- OPERATORS
-- One row per employee involved in production or inspection.
-- ------------------------------------------------------------

CREATE TABLE operators (
    operator_id BIGINT GENERATED ALWAYS AS IDENTITY,
    employee_code VARCHAR(50) NOT NULL,
    operator_name VARCHAR(150) NOT NULL,
    shift VARCHAR(30) NOT NULL,
    role_type VARCHAR(50) NOT NULL DEFAULT 'Operator',
    experience_level VARCHAR(30) NOT NULL,
    hire_date DATE NOT NULL,
    certification_status VARCHAR(30) NOT NULL,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_operators
        PRIMARY KEY (operator_id),

    CONSTRAINT uq_operators_employee_code
        UNIQUE (employee_code),

    CONSTRAINT chk_operators_shift
        CHECK (
            shift IN (
                'First',
                'Second',
                'Third',
                'Weekend'
            )
        ),

    CONSTRAINT chk_operators_role_type
        CHECK (
            role_type IN (
                'Operator',
                'Inspector',
                'Technician',
                'Supervisor'
            )
        ),

    CONSTRAINT chk_operators_experience
        CHECK (
            experience_level IN (
                'Entry',
                'Intermediate',
                'Senior',
                'Lead'
            )
        ),

    CONSTRAINT chk_operators_certification
        CHECK (
            certification_status IN (
                'Current',
                'Expired',
                'Pending',
                'Not Required'
            )
        )
);


-- ------------------------------------------------------------
-- DEFECT TYPES
-- One row per standardized quality-defect category.
-- ------------------------------------------------------------

CREATE TABLE defect_types (
    defect_type_id BIGINT GENERATED ALWAYS AS IDENTITY,
    defect_code VARCHAR(30) NOT NULL,
    defect_name VARCHAR(150) NOT NULL,
    defect_category VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    description TEXT,
    active_flag BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_defect_types
        PRIMARY KEY (defect_type_id),

    CONSTRAINT uq_defect_types_code
        UNIQUE (defect_code),

    CONSTRAINT chk_defect_types_category
        CHECK (
            defect_category IN (
                'Dimensional',
                'Material',
                'Thread',
                'Surface',
                'Coating',
                'Assembly',
                'Packaging',
                'Other'
            )
        ),

    CONSTRAINT chk_defect_types_severity
        CHECK (
            severity IN (
                'Minor',
                'Major',
                'Critical'
            )
        )
);


-- ============================================================
-- INDEXES FOR MASTER DATA
-- ============================================================

CREATE INDEX idx_products_material_id
    ON products (material_id);

CREATE INDEX idx_products_family
    ON products (product_family);

CREATE INDEX idx_machines_operation_type
    ON machines (operation_type);

CREATE INDEX idx_machines_production_line
    ON machines (production_line);

CREATE INDEX idx_operators_shift
    ON operators (shift);

CREATE INDEX idx_defect_types_category
    ON defect_types (defect_category);

-- ============================================================
-- 2. ORDER MANAGEMENT
-- ============================================================

-- ------------------------------------------------------------
-- CUSTOMER ORDERS
-- One row per customer purchase order header.
-- ------------------------------------------------------------

CREATE TABLE customer_orders (
    customer_order_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_order_number VARCHAR(50) NOT NULL,
    customer_id BIGINT NOT NULL,
    order_date DATE NOT NULL,
    requested_delivery_date DATE NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'Standard',
    order_status VARCHAR(30) NOT NULL DEFAULT 'Open',
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_customer_orders
        PRIMARY KEY (customer_order_id),

    CONSTRAINT uq_customer_orders_order_number
        UNIQUE (customer_order_number),

    CONSTRAINT fk_customer_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers (customer_id),

    CONSTRAINT chk_customer_orders_priority
        CHECK (
            priority IN (
                'Low',
                'Standard',
                'High',
                'Rush'
            )
        ),

    CONSTRAINT chk_customer_orders_status
        CHECK (
            order_status IN (
                'Open',
                'Released',
                'Partially Fulfilled',
                'Completed',
                'Cancelled'
            )
        ),
    
    CONSTRAINT chk_customer_orders_delivery_date
        CHECK (
            requested_delivery_date >= order_date
        )
);


-- ------------------------------------------------------------
-- CUSTOMER ORDER INDEXES
-- ------------------------------------------------------------

CREATE INDEX idx_customer_orders_customer
    ON customer_orders (customer_id);

CREATE INDEX idx_customer_orders_status
    ON customer_orders (order_status);

CREATE INDEX idx_customer_orders_order_date
    ON customer_orders (order_date);