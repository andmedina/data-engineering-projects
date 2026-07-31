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

-- ------------------------------------------------------------
-- CUSTOMER ORDER ITEMS
-- One row per product line within a customer order.
-- ------------------------------------------------------------

CREATE TABLE customer_order_items (
    customer_order_item_id BIGINT GENERATED ALWAYS AS IDENTITY,
    customer_order_id BIGINT NOT NULL,
    line_number INTEGER NOT NULL,
    product_id BIGINT NOT NULL,
    ordered_quantity INTEGER NOT NULL,
    unit_price NUMERIC(12,4) NOT NULL,
    line_status VARCHAR(30) NOT NULL DEFAULT 'Open',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_customer_order_items
        PRIMARY KEY (customer_order_item_id),

    CONSTRAINT uq_customer_order_items_order_line
        UNIQUE (customer_order_id, line_number),

    CONSTRAINT fk_customer_order_items_order
        FOREIGN KEY (customer_order_id)
        REFERENCES customer_orders (customer_order_id),

    CONSTRAINT fk_customer_order_items_product
        FOREIGN KEY (product_id)
        REFERENCES products (product_id),

    CONSTRAINT chk_customer_order_items_line_number
        CHECK (
            line_number > 0
        ),

    CONSTRAINT chk_customer_order_items_quantity
        CHECK (
            ordered_quantity > 0
        ),

    CONSTRAINT chk_customer_order_items_unit_price
        CHECK (
            unit_price >= 0
        ),

    CONSTRAINT chk_customer_order_items_status
        CHECK (
            line_status IN (
                'Open',
                'Allocated',
                'Partially Fulfilled',
                'Completed',
                'Cancelled'
            )
        )
);


-- ------------------------------------------------------------
-- CUSTOMER ORDER ITEM INDEXES
-- ------------------------------------------------------------

CREATE INDEX idx_customer_order_items_order
    ON customer_order_items (customer_order_id);

CREATE INDEX idx_customer_order_items_product
    ON customer_order_items (product_id);

CREATE INDEX idx_customer_order_items_status
    ON customer_order_items (line_status);

-- ------------------------------------------------------------
-- PRODUCTION ORDERS
-- One row per manufacturing work order.
-- ------------------------------------------------------------

CREATE TABLE production_orders (
    production_order_id BIGINT GENERATED ALWAYS AS IDENTITY,
    production_order_number VARCHAR(50) NOT NULL,
    customer_order_item_id BIGINT NOT NULL,
    machine_id BIGINT,
    scheduled_start_date DATE,
    scheduled_end_date DATE,
    actual_start_timestamp TIMESTAMPTZ,
    actual_end_timestamp TIMESTAMPTZ,
    planned_quantity INTEGER NOT NULL,
    completed_quantity INTEGER NOT NULL DEFAULT 0,
    scrapped_quantity INTEGER NOT NULL DEFAULT 0,
    production_status VARCHAR(30) NOT NULL DEFAULT 'Released',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_production_orders
        PRIMARY KEY (production_order_id),

    CONSTRAINT uq_production_orders_number
        UNIQUE (production_order_number),

    CONSTRAINT fk_production_orders_customer_order_item
        FOREIGN KEY (customer_order_item_id)
        REFERENCES customer_order_items (customer_order_item_id),

    CONSTRAINT fk_production_orders_machine
        FOREIGN KEY (machine_id)
        REFERENCES machines (machine_id),

    CONSTRAINT chk_production_orders_planned_quantity
        CHECK (
            planned_quantity > 0
        ),

    CONSTRAINT chk_production_orders_completed_quantity
        CHECK (
            completed_quantity >= 0
        ),

    CONSTRAINT chk_production_orders_scrapped_quantity
        CHECK (
            scrapped_quantity >= 0
        ),

    CONSTRAINT chk_production_orders_schedule_dates
        CHECK (
            scheduled_end_date IS NULL
            OR scheduled_start_date IS NULL
            OR scheduled_end_date >= scheduled_start_date
        ),

    CONSTRAINT chk_production_orders_actual_dates
        CHECK (
            actual_end_timestamp IS NULL
            OR actual_start_timestamp IS NULL
            OR actual_end_timestamp >= actual_start_timestamp
        ),

    CONSTRAINT chk_production_orders_status
        CHECK (
            production_status IN (
                'Released',
                'Scheduled',
                'In Production',
                'Completed',
                'Cancelled'
            )
        )
);


-- ------------------------------------------------------------
-- PRODUCTION ORDER INDEXES
-- ------------------------------------------------------------

CREATE INDEX idx_production_orders_customer_order_item
    ON production_orders (customer_order_item_id);

CREATE INDEX idx_production_orders_machine
    ON production_orders (machine_id);

CREATE INDEX idx_production_orders_status
    ON production_orders (production_status);

CREATE INDEX idx_production_orders_scheduled_start
    ON production_orders (scheduled_start_date);

-- ------------------------------------------------------------
-- MATERIAL LOTS
-- One row per unique supplier material lot.
-- Quantities use the material's defined base unit of measure.
-- Assumption: Each supplier lot is represented by one receipt.
-- ------------------------------------------------------------

CREATE TABLE material_lots (
    material_lot_id BIGINT GENERATED ALWAYS AS IDENTITY,
    material_id BIGINT NOT NULL,
    supplier_id BIGINT NOT NULL,
    supplier_lot_number VARCHAR(100) NOT NULL,
    received_date DATE NOT NULL,
    quantity_received NUMERIC(14, 3) NOT NULL,
    quantity_available NUMERIC(14, 3) NOT NULL,
    lot_status VARCHAR(20) NOT NULL DEFAULT 'Available',
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_material_lots
        PRIMARY KEY (material_lot_id),

    CONSTRAINT uq_material_lots_supplier_lot
        UNIQUE (
            supplier_id,
            supplier_lot_number
        ),

    CONSTRAINT fk_material_lots_material
        FOREIGN KEY (material_id)
        REFERENCES materials (material_id),

    CONSTRAINT fk_material_lots_supplier
        FOREIGN KEY (supplier_id)
        REFERENCES suppliers (supplier_id),

    CONSTRAINT chk_material_lots_quantity_received
        CHECK (
            quantity_received > 0
        ),

    CONSTRAINT chk_material_lots_quantity_available
        CHECK (
            quantity_available >= 0
            AND quantity_available <= quantity_received
        ),

    CONSTRAINT chk_material_lots_status
        CHECK (
            lot_status IN (
                'Available',
                'On Hold',
                'Depleted',
                'Rejected'
            )
        )
);


-- ------------------------------------------------------------
-- MATERIAL LOT INDEXES
-- ------------------------------------------------------------

CREATE INDEX idx_material_lots_material
    ON material_lots (material_id);

CREATE INDEX idx_material_lots_supplier
    ON material_lots (supplier_id);

CREATE INDEX idx_material_lots_status
    ON material_lots (lot_status);

CREATE INDEX idx_material_lots_received_date
    ON material_lots (received_date);

-- ------------------------------------------------------------
-- PRODUCTION ORDER MATERIALS
-- One row per material lot allocated to a production order.
-- ------------------------------------------------------------

CREATE TABLE production_order_materials (
    production_order_id BIGINT NOT NULL,
    material_lot_id BIGINT NOT NULL,
    allocated_quantity NUMERIC(14, 3) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_production_order_materials
        PRIMARY KEY (
            production_order_id,
            material_lot_id
        ),

    CONSTRAINT fk_production_order_materials_production_order
        FOREIGN KEY (production_order_id)
        REFERENCES production_orders (production_order_id),

    CONSTRAINT fk_production_order_materials_material_lot
        FOREIGN KEY (material_lot_id)
        REFERENCES material_lots (material_lot_id),

    CONSTRAINT chk_production_order_materials_allocated_quantity
        CHECK (
            allocated_quantity > 0
        )
);


-- ------------------------------------------------------------
-- PRODUCTION ORDER MATERIAL INDEXES
-- ------------------------------------------------------------

CREATE INDEX idx_production_order_materials_material_lot
    ON production_order_materials (material_lot_id);


-- ============================================================================
-- production_runs
--
-- Purpose:
--     Stores the execution of individual manufacturing operations performed
--     as part of a production order.
--
-- Grain:
--     One row per continuous execution of a single manufacturing operation
--     for one production order on one machine.
-- ============================================================================

CREATE TABLE production_runs (

    production_run_id BIGINT GENERATED ALWAYS AS IDENTITY,

    production_order_id BIGINT NOT NULL,

    machine_id BIGINT NOT NULL,

    operator_id BIGINT NOT NULL,

    operation_sequence SMALLINT NOT NULL,

    operation_type VARCHAR(50) NOT NULL,

    start_timestamp TIMESTAMPTZ,

    end_timestamp TIMESTAMPTZ,

    planned_cycle_time_seconds NUMERIC(8,2),

    actual_cycle_time_seconds NUMERIC(8,2),

    input_quantity INTEGER NOT NULL,

    good_quantity INTEGER NOT NULL DEFAULT 0,

    scrap_quantity INTEGER NOT NULL DEFAULT 0,

    rework_quantity INTEGER NOT NULL DEFAULT 0,

    run_status VARCHAR(25) NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_production_runs
        PRIMARY KEY (production_run_id),

    CONSTRAINT fk_production_runs_production_orders
        FOREIGN KEY (production_order_id)
        REFERENCES production_orders (production_order_id),

    CONSTRAINT fk_production_runs_machines
        FOREIGN KEY (machine_id)
        REFERENCES machines (machine_id),

    CONSTRAINT fk_production_runs_operators
        FOREIGN KEY (operator_id)
        REFERENCES operators (operator_id),

    CONSTRAINT chk_production_runs_operation_sequence
        CHECK (operation_sequence > 0),

    CONSTRAINT chk_production_runs_cycle_times
        CHECK (
            planned_cycle_time_seconds IS NULL
            OR planned_cycle_time_seconds > 0
        ),

    CONSTRAINT chk_production_runs_actual_cycle_time
        CHECK (
            actual_cycle_time_seconds IS NULL
            OR actual_cycle_time_seconds > 0
        ),

    CONSTRAINT chk_production_runs_input_quantity
        CHECK (input_quantity >= 0),

    CONSTRAINT chk_production_runs_good_quantity
        CHECK (good_quantity >= 0),

    CONSTRAINT chk_production_runs_scrap_quantity
        CHECK (scrap_quantity >= 0),

    CONSTRAINT chk_production_runs_rework_quantity
        CHECK (rework_quantity >= 0),

    CONSTRAINT chk_production_runs_quantity_balance
        CHECK (
            input_quantity =
            good_quantity +
            scrap_quantity +
            rework_quantity
        ),

    CONSTRAINT chk_production_runs_timestamps
        CHECK (
            end_timestamp IS NULL
            OR start_timestamp IS NULL
            OR end_timestamp >= start_timestamp
        ),

    CONSTRAINT chk_production_runs_status
        CHECK (
            run_status IN (
                'Planned',
                'Running',
                'Completed',
                'Interrupted',
                'Cancelled'
            )
        )

);

CREATE INDEX idx_production_runs_production_order_id
    ON production_runs (production_order_id);

CREATE INDEX idx_production_runs_machine_id
    ON production_runs (machine_id);

CREATE INDEX idx_production_runs_operator_id
    ON production_runs (operator_id);

CREATE INDEX idx_production_runs_operation_type
    ON production_runs (operation_type);

CREATE INDEX idx_production_runs_start_timestamp
    ON production_runs (start_timestamp);

-- ============================================================================
-- quality_inspections
--
-- Purpose:
--     Stores inspections performed during or after a manufacturing operation.
--
-- Grain:
--     One row per inspection event performed for one production run.
-- ============================================================================

CREATE TABLE quality_inspections (

    inspection_id BIGINT GENERATED ALWAYS AS IDENTITY,

    production_run_id BIGINT NOT NULL,

    inspector_id BIGINT NOT NULL,

    inspection_timestamp TIMESTAMPTZ NOT NULL,

    sample_size INTEGER NOT NULL,

    passed_quantity INTEGER NOT NULL DEFAULT 0,

    failed_quantity INTEGER NOT NULL DEFAULT 0,

    inspection_result VARCHAR(20) NOT NULL,

    measurement_type VARCHAR(50) NOT NULL,

    measured_value NUMERIC(10,4),

    lower_spec_limit NUMERIC(10,4),

    upper_spec_limit NUMERIC(10,4),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_quality_inspections
        PRIMARY KEY (inspection_id),

    CONSTRAINT fk_quality_inspections_production_runs
        FOREIGN KEY (production_run_id)
        REFERENCES production_runs (production_run_id),

    CONSTRAINT fk_quality_inspections_operators
        FOREIGN KEY (inspector_id)
        REFERENCES operators (operator_id),

    CONSTRAINT chk_quality_inspections_sample_size
        CHECK (sample_size >= 0),

    CONSTRAINT chk_quality_inspections_passed_quantity
        CHECK (passed_quantity >= 0),

    CONSTRAINT chk_quality_inspections_failed_quantity
        CHECK (failed_quantity >= 0),

    CONSTRAINT chk_quality_inspections_quantity_balance
        CHECK (
            sample_size =
            passed_quantity +
            failed_quantity
        ),

    CONSTRAINT chk_quality_inspections_spec_limits
        CHECK (
            lower_spec_limit IS NULL
            OR upper_spec_limit IS NULL
            OR upper_spec_limit >= lower_spec_limit
        ),

    CONSTRAINT chk_quality_inspections_result
        CHECK (
            inspection_result IN (
                'Pass',
                'Fail',
                'Conditional',
                'Pending'
            )
        )

);

CREATE INDEX idx_quality_inspections_production_run_id
    ON quality_inspections (production_run_id);

CREATE INDEX idx_quality_inspections_inspector_id
    ON quality_inspections (inspector_id);

CREATE INDEX idx_quality_inspections_timestamp
    ON quality_inspections (inspection_timestamp);

CREATE INDEX idx_quality_inspections_measurement_type
    ON quality_inspections (measurement_type);

-- ============================================================================
-- quality_defects
--
-- Purpose:
--     Stores defect types identified during a quality inspection.
--
-- Grain:
--     One row per defect type identified during one quality inspection.
-- ============================================================================

CREATE TABLE quality_defects (

    quality_defect_id BIGINT GENERATED ALWAYS AS IDENTITY,

    inspection_id BIGINT NOT NULL,

    defect_type_id BIGINT NOT NULL,

    defect_quantity INTEGER NOT NULL,

    disposition VARCHAR(30) NOT NULL,

    root_cause_category VARCHAR(30) NOT NULL,

    corrective_action TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_quality_defects
        PRIMARY KEY (quality_defect_id),

    CONSTRAINT fk_quality_defects_quality_inspections
        FOREIGN KEY (inspection_id)
        REFERENCES quality_inspections (inspection_id),

    CONSTRAINT fk_quality_defects_defect_types
        FOREIGN KEY (defect_type_id)
        REFERENCES defect_types (defect_type_id),

    CONSTRAINT chk_quality_defects_quantity
        CHECK (defect_quantity >= 0),

    CONSTRAINT chk_quality_defects_disposition
        CHECK (
            disposition IN (
                'Scrap',
                'Rework',
                'Use As Is',
                'Return to Supplier',
                'Pending Review'
            )
        ),

    CONSTRAINT chk_quality_defects_root_cause
        CHECK (
            root_cause_category IN (
                'Machine',
                'Material',
                'Method',
                'Measurement',
                'Operator',
                'Environment',
                'Unknown'
            )
        )

);

CREATE INDEX idx_quality_defects_inspection_id
    ON quality_defects (inspection_id);

CREATE INDEX idx_quality_defects_defect_type_id
    ON quality_defects (defect_type_id);

CREATE INDEX idx_quality_defects_root_cause_category
    ON quality_defects (root_cause_category);

-- ============================================================================
-- downtime_events
--
-- Purpose:
--     Stores machine downtime events and production interruptions.
--
-- Grain:
--     One row per continuous downtime event for one machine.
-- ============================================================================

CREATE TABLE downtime_events (

    downtime_event_id BIGINT GENERATED ALWAYS AS IDENTITY,

    machine_id BIGINT NOT NULL,

    production_run_id BIGINT,

    downtime_start TIMESTAMPTZ NOT NULL,

    downtime_end TIMESTAMPTZ NOT NULL,

    downtime_minutes INTEGER NOT NULL,

    downtime_category VARCHAR(50) NOT NULL,

    downtime_reason TEXT,

    planned_flag BOOLEAN NOT NULL,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_downtime_events
        PRIMARY KEY (downtime_event_id),

    CONSTRAINT fk_downtime_events_machines
        FOREIGN KEY (machine_id)
        REFERENCES machines (machine_id),

    CONSTRAINT fk_downtime_events_production_runs
        FOREIGN KEY (production_run_id)
        REFERENCES production_runs (production_run_id),

    CONSTRAINT chk_downtime_events_minutes
        CHECK (downtime_minutes >= 0),

    CONSTRAINT chk_downtime_events_timestamps
        CHECK (downtime_end >= downtime_start),

    CONSTRAINT chk_downtime_events_category
        CHECK (
            downtime_category IN (
                'Mechanical Failure',
                'Tool Change',
                'Setup',
                'Material Shortage',
                'Quality Hold',
                'Preventive Maintenance',
                'Operator Unavailable',
                'Changeover',
                'Power Interruption'
            )
        )

);

CREATE INDEX idx_downtime_events_machine_id
    ON downtime_events (machine_id);

CREATE INDEX idx_downtime_events_production_run_id
    ON downtime_events (production_run_id);

CREATE INDEX idx_downtime_events_start
    ON downtime_events (downtime_start);

CREATE INDEX idx_downtime_events_category
    ON downtime_events (downtime_category);

-- ============================================================================
-- maintenance_events
--
-- Purpose:
--     Stores maintenance activities performed on manufacturing equipment.
--
-- Grain:
--     One row per maintenance event performed on one machine.
-- ============================================================================

CREATE TABLE maintenance_events (

    maintenance_event_id BIGINT GENERATED ALWAYS AS IDENTITY,

    machine_id BIGINT NOT NULL,

    maintenance_type VARCHAR(30) NOT NULL,

    reported_timestamp TIMESTAMPTZ NOT NULL,

    maintenance_start TIMESTAMPTZ NOT NULL,

    maintenance_end TIMESTAMPTZ NOT NULL,

    technician VARCHAR(100) NOT NULL,

    failure_component VARCHAR(100),

    maintenance_action TEXT,

    maintenance_cost NUMERIC(10,2),

    machine_hours_at_service NUMERIC(10,2),

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_maintenance_events
        PRIMARY KEY (maintenance_event_id),

    CONSTRAINT fk_maintenance_events_machines
        FOREIGN KEY (machine_id)
        REFERENCES machines (machine_id),

    CONSTRAINT chk_maintenance_events_type
        CHECK (
            maintenance_type IN (
                'Preventive',
                'Corrective',
                'Predictive',
                'Calibration',
                'Inspection'
            )
        ),

    CONSTRAINT chk_maintenance_events_timestamps
        CHECK (
            maintenance_end >= maintenance_start
        ),

    CONSTRAINT chk_maintenance_events_cost
        CHECK (
            maintenance_cost IS NULL
            OR maintenance_cost >= 0
        ),

    CONSTRAINT chk_maintenance_events_machine_hours
        CHECK (
            machine_hours_at_service IS NULL
            OR machine_hours_at_service >= 0
        )

);

CREATE INDEX idx_maintenance_events_machine_id
    ON maintenance_events (machine_id);

CREATE INDEX idx_maintenance_events_type
    ON maintenance_events (maintenance_type);

CREATE INDEX idx_maintenance_events_start
    ON maintenance_events (maintenance_start);

-- ============================================================================
-- sensor_readings
--
-- Purpose:
--     Stores time-series telemetry collected from manufacturing equipment.
--
-- Grain:
--     One row per machine per sensor-reading timestamp.
-- ============================================================================

CREATE TABLE sensor_readings (

    sensor_reading_id BIGINT GENERATED ALWAYS AS IDENTITY,

    machine_id BIGINT NOT NULL,

    reading_timestamp TIMESTAMPTZ NOT NULL,

    temperature_c NUMERIC(6,2),

    vibration_mm_s NUMERIC(8,3),

    power_kw NUMERIC(8,2),

    pressure_psi NUMERIC(8,2),

    rpm INTEGER,

    created_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT pk_sensor_readings
        PRIMARY KEY (sensor_reading_id),

    CONSTRAINT uq_sensor_readings_machine_timestamp
        UNIQUE (machine_id, reading_timestamp),

    CONSTRAINT fk_sensor_readings_machines
        FOREIGN KEY (machine_id)
        REFERENCES machines (machine_id),

    CONSTRAINT chk_sensor_readings_temperature
        CHECK (
            temperature_c IS NULL
            OR temperature_c >= -273.15
        ),

    CONSTRAINT chk_sensor_readings_vibration
        CHECK (
            vibration_mm_s IS NULL
            OR vibration_mm_s >= 0
        ),

    CONSTRAINT chk_sensor_readings_power
        CHECK (
            power_kw IS NULL
            OR power_kw >= 0
        ),

    CONSTRAINT chk_sensor_readings_pressure
        CHECK (
            pressure_psi IS NULL
            OR pressure_psi >= 0
        ),

    CONSTRAINT chk_sensor_readings_rpm
        CHECK (
            rpm IS NULL
            OR rpm >= 0
        )

);

CREATE INDEX idx_sensor_readings_machine_id
    ON sensor_readings (machine_id);

CREATE INDEX idx_sensor_readings_timestamp
    ON sensor_readings (reading_timestamp);

