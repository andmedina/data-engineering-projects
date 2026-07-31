-- ============================================================================
-- seed.sql
--
-- Purpose:
--     Populates the database with reference (master) data used throughout the
--     Manufacturing Intelligence Platform.
--
-- Tables are inserted in dependency order to satisfy foreign key constraints.
--
-- This file intentionally excludes transactional data, which is generated
-- separately by the synthetic data generator.
-- ============================================================================

-- ============================================================================
-- CUSTOMERS
-- ============================================================================

INSERT INTO customers (
    customer_name,
    customer_type,
    country,
    industry_segment,
    active_flag
)
VALUES
    ('Apex Aerospace Systems',      'OEM',             'United States', 'Commercial Aviation', TRUE),
    ('Orion Flight Technologies',   'OEM',             'United States', 'Defense', TRUE),
    ('Summit Aerostructures',       'Tier 1 Supplier', 'United States', 'Commercial Aviation', TRUE),
    ('Titan Precision Industries',  'Tier 1 Supplier', 'Canada',        'Defense', TRUE),
    ('Blue Horizon Spaceworks',     'OEM',             'United States', 'Space', TRUE),
    ('Nova Orbital Components',     'Tier 2 Supplier', 'United States', 'Space', TRUE),
    ('Falcon Industrial Supply',    'Distributor',     'United States', 'Distribution', TRUE),
    ('Pinnacle Fastener Group',     'Distributor',     'Mexico',        'Distribution', TRUE),
    ('Altitude Aircraft Services',  'Other',           'United States', 'Maintenance Repair and Overhaul', TRUE),
    ('SkyBridge MRO Solutions',     'Other',           'Canada',        'Maintenance Repair and Overhaul', TRUE),
    ('Atlas Defense Manufacturing', 'OEM',             'United States', 'Defense', TRUE),
    ('IronWing Engineering',        'Tier 1 Supplier', 'United Kingdom','Defense', TRUE),
    ('NorthStar Aerotech',          'Tier 2 Supplier', 'Germany',       'Commercial Aviation', TRUE),
    ('Vector Space Industries',     'OEM',             'France',        'Space', TRUE),
    ('Meridian Aircraft Systems',   'Tier 1 Supplier', 'Japan',         'Commercial Aviation', TRUE),
    ('Frontier Composite Works',    'Tier 2 Supplier', 'South Korea',   'Commercial Aviation', TRUE),
    ('Everest Defense Solutions',   'Government',      'United States', 'Defense', TRUE),
    ('Pacific Launch Systems',      'OEM',             'Australia',     'Space', TRUE),
    ('Sterling Aviation Supply',    'Distributor',     'United States', 'Distribution', TRUE),
    ('Helios Precision Aerospace',  'Tier 1 Supplier', 'Italy',         'Commercial Aviation', TRUE);

-- ============================================================================
-- SUPPLIERS
-- ============================================================================

INSERT INTO suppliers (
    supplier_name,
    supplier_category,
    country,
    approved_status,
    quality_rating,
    average_lead_time_days,
    active_flag
)
VALUES
    ('Precision Metals Group',      'Raw Material', 'United States', 'Approved',   98.50, 10, TRUE),
    ('Titan Alloy Supply',          'Raw Material', 'Canada',        'Approved',   97.20, 14, TRUE),
    ('Aero Steel International',    'Raw Material', 'Germany',       'Approved',   96.10, 21, TRUE),
    ('Global Aluminum Works',       'Raw Material', 'Japan',         'Approved',   95.40, 18, TRUE),
    ('Prime Coating Solutions',     'Coating',     'United States', 'Approved',   99.10,  7, TRUE),
    ('SurfaceTech Finishing',       'Coating',     'France',        'Conditional',91.80, 16, TRUE),
    ('Precision Tooling Systems',   'Tooling',     'United States', 'Approved',   97.90, 12, TRUE),
    ('Advanced Die Manufacturing',  'Tooling',     'South Korea',   'Approved',   94.60, 20, TRUE),
    ('Apex Component Supply',       'Component',   'Mexico',        'Approved',   96.80, 15, TRUE),
    ('Industrial Parts Network',    'Component',   'Italy',         'Approved',   93.70, 17, TRUE),
    ('Secure Packaging Group',      'Packaging',   'United States', 'Approved',   98.00,  5, TRUE),
    ('Continental Packaging',       'Packaging',   'United Kingdom','Approved',   95.30, 11, TRUE),
    ('MachineCare Services',        'Maintenance', 'United States', 'Approved',   94.20,  8, TRUE),
    ('Factory Reliability Partners','Maintenance', 'Canada',        'Conditional',90.50, 13, TRUE),
    ('Integrated Industrial Supply','Other',       'United States', 'Approved',   92.40,  9, TRUE);

-- ============================================================================
-- MATERIALS
-- ============================================================================

INSERT INTO materials (
    material_code,
    material_name,
    material_category,
    alloy,
    material_form,
    unit_of_measure,
    active_flag
)
VALUES
    ('MAT-001', 'Aluminum 2024 Wire',              'Aluminum',         '2024',         'Wire', 'lb', TRUE),
    ('MAT-002', 'Aluminum 7075 Bar',               'Aluminum',         '7075',         'Bar',  'lb', TRUE),
    ('MAT-003', 'Titanium Ti-6Al-4V Bar',          'Titanium',         'Ti-6Al-4V',    'Bar',  'lb', TRUE),
    ('MAT-004', 'Titanium Ti-6Al-4V Rod',          'Titanium',         'Ti-6Al-4V',    'Rod',  'lb', TRUE),
    ('MAT-005', 'Stainless Steel 304 Wire',        'Stainless Steel',  '304',          'Wire', 'lb', TRUE),
    ('MAT-006', 'Stainless Steel 316 Rod',         'Stainless Steel',  '316',          'Rod',  'lb', TRUE),
    ('MAT-007', 'Alloy Steel 4140 Bar',            'Alloy Steel',      '4140',         'Bar',  'lb', TRUE),
    ('MAT-008', 'Alloy Steel 4340 Rod',            'Alloy Steel',      '4340',         'Rod',  'lb', TRUE),
    ('MAT-009', 'Inconel 718 Bar',                 'Nickel Alloy',     'Inconel 718',  'Bar',  'lb', TRUE),
    ('MAT-010', 'Inconel 625 Wire',                'Nickel Alloy',     'Inconel 625',  'Wire', 'lb', TRUE),
    ('MAT-011', 'Aluminum 6061 Coil',              'Aluminum',         '6061',         'Coil', 'lb', TRUE),
    ('MAT-012', 'Stainless Steel 304 Sheet',       'Stainless Steel',  '304',          'Sheet','lb', TRUE);

-- ============================================================================
-- PRODUCTS
-- ============================================================================

INSERT INTO products (
    part_number,
    product_name,
    product_family,
    material_id,
    diameter_in,
    length_in,
    finish_type,
    aerospace_specification,
    standard_cycle_time_seconds,
    standard_unit_cost,
    active_flag
)
VALUES
('SR-1001','Solid Rivet 1/8 Aluminum','Solid Rivet',1,0.1250,0.5000,'Anodized','NAS1097',2.50,0.12,TRUE),
('SR-1002','Solid Rivet 5/32 Aluminum','Solid Rivet',2,0.1563,0.6250,'Anodized','NAS1097',2.70,0.15,TRUE),
('SR-1003','Solid Rivet Stainless','Solid Rivet',5,0.1250,0.5000,'Passivated','MS20470',3.10,0.22,TRUE),

('BR-2001','Blind Rivet Standard','Blind Rivet',5,0.1875,0.7500,'Zinc','NAS1738',4.20,0.48,TRUE),
('BR-2002','Blind Rivet Titanium','Blind Rivet',3,0.1875,0.8750,'Passivated','NAS1739',5.40,1.35,TRUE),
('BR-2003','Blind Rivet Alloy Steel','Blind Rivet',7,0.2500,1.0000,'Cadmium','NAS1740',4.80,0.92,TRUE),

('BB-3001','Titanium Blind Bolt','Blind Bolt',3,0.2500,1.2500,'Passivated','NAS1921',7.50,4.75,TRUE),
('BB-3002','Steel Blind Bolt','Blind Bolt',8,0.3125,1.5000,'Cadmium','NAS1922',6.80,3.95,TRUE),
('BB-3003','Inconel Blind Bolt','Blind Bolt',9,0.2500,1.2500,'Passivated','NAS1923',8.40,6.20,TRUE),

('TF-4001','Temporary Fastener Small','Temporary Fastener',2,0.1250,1.0000,'Anodized','NAS445',3.80,2.15,TRUE),
('TF-4002','Temporary Fastener Large','Temporary Fastener',7,0.2500,1.5000,'Cadmium','NAS446',4.60,2.95,TRUE),

('TI-5001','Threaded Insert Aluminum','Threaded Insert',11,0.2500,0.6250,'Anodized','NAS1394',4.70,0.85,TRUE),
('TI-5002','Threaded Insert Stainless','Threaded Insert',6,0.3125,0.7500,'Passivated','NAS1395',5.10,1.10,TRUE),
('TI-5003','Threaded Insert Titanium','Threaded Insert',4,0.3125,0.7500,'Passivated','NAS1396',6.00,2.30,TRUE),

('IT-6001','Blind Bolt Installation Tool','Installation Tool',12,NULL,NULL,NULL,'OEM-001',15.00,120.00,TRUE),
('IT-6002','Rivet Installation Tool','Installation Tool',12,NULL,NULL,NULL,'OEM-002',14.50,98.00,TRUE),

('SR-1004','High Strength Rivet','Solid Rivet',8,0.2500,1.0000,'Cadmium','MS20426',3.80,0.40,TRUE),
('BR-2004','Flush Head Blind Rivet','Blind Rivet',1,0.1563,0.6250,'Anodized','NAS1737',4.00,0.55,TRUE),
('BB-3004','High Temperature Blind Bolt','Blind Bolt',10,0.2500,1.5000,'Passivated','NAS1924',8.80,6.80,TRUE),
('TI-5004','Heavy Duty Threaded Insert','Threaded Insert',9,0.3750,0.8750,'Passivated','NAS1397',6.80,3.20,TRUE);

-- ============================================================================
-- MACHINES
-- ============================================================================

INSERT INTO machines (
    machine_code,
    machine_name,
    operation_type,
    production_line,
    manufacturer,
    model,
    install_date,
    rated_capacity_per_hour,
    status
)
VALUES
    ('CH-01', 'Cold Header 1',        'Cold Heading',       'Fastener Line A', 'National Machinery', 'FORMAX 2000', '2018-03-15', 3200.00, 'Active'),
    ('CH-02', 'Cold Header 2',        'Cold Heading',       'Fastener Line B', 'National Machinery', 'FORMAX 2500', '2020-07-10', 3600.00, 'Active'),

    ('TR-01', 'Thread Roller 1',      'Thread Rolling',     'Fastener Line A', 'Waterbury Farrel',    'TR-500',      '2017-11-02', 2800.00, 'Active'),
    ('TR-02', 'Thread Roller 2',      'Thread Rolling',     'Fastener Line B', 'Waterbury Farrel',    'TR-650',      '2021-04-19', 3100.00, 'Active'),

    ('HT-01', 'Heat Treat Furnace 1', 'Heat Treatment',     'Heat Treat Line', 'Ipsen',                'Titan H6',    '2016-09-08', 1200.00, 'Active'),
    ('SF-01', 'Surface Finishing 1',  'Surface Finishing',  'Finishing Line',  'Rösler',               'R 420',       '2019-06-21', 1800.00, 'Active'),

    ('AS-01', 'Assembly Cell 1',      'Assembly',           'Assembly Line A', 'Weber',                'SEV-P',       '2022-02-14', 1400.00, 'Active'),
    ('AS-02', 'Assembly Cell 2',      'Assembly',           'Assembly Line B', 'Weber',                'SEV-C',       '2023-05-09', 1600.00, 'Active'),

    ('IN-01', 'Inspection Station 1', 'Inspection',         'Quality Lab',     'Keyence',              'IM-8000',     '2021-08-30', 900.00,  'Active'),
    ('IN-02', 'Inspection Station 2', 'Inspection',         'Quality Lab',     'Zeiss',                'O-Inspect',   '2019-10-18', 700.00,  'Active'),

    ('PK-01', 'Packaging Line 1',     'Packaging',          'Packaging Area',  'Bosch',                'SVE 2520',    '2018-12-05', 2200.00, 'Active'),
    ('MP-01', 'Multi-Purpose Cell 1', 'Multi-Purpose',      'Prototype Line',  'Haas',                 'VF-2SS',      '2020-01-27', 500.00,  'Idle');

-- ============================================================================
-- OPERATORS
-- ============================================================================

INSERT INTO operators (
    employee_code,
    operator_name,
    shift,
    role_type,
    experience_level,
    hire_date,
    certification_status,
    active_flag
)
VALUES
('EMP-001','James Carter','First','Operator','Senior','2018-04-12','Current',TRUE),
('EMP-002','Maria Lopez','First','Operator','Intermediate','2020-06-01','Current',TRUE),
('EMP-003','Daniel Kim','First','Operator','Lead','2016-09-18','Current',TRUE),
('EMP-004','Sarah Johnson','Second','Operator','Intermediate','2021-01-11','Current',TRUE),
('EMP-005','Michael Brown','Second','Operator','Entry','2024-02-20','Pending',TRUE),

('EMP-006','Emily Davis','Third','Operator','Senior','2017-11-30','Current',TRUE),
('EMP-007','Kevin Martinez','Third','Operator','Intermediate','2019-08-15','Current',TRUE),
('EMP-008','Olivia Wilson','Weekend','Operator','Entry','2023-05-09','Current',TRUE),

('EMP-009','David Anderson','First','Inspector','Lead','2015-03-16','Current',TRUE),
('EMP-010','Sophia Garcia','Second','Inspector','Senior','2018-07-21','Current',TRUE),
('EMP-011','Ethan Moore','Third','Inspector','Intermediate','2020-12-01','Current',TRUE),
('EMP-012','Grace Thomas','Weekend','Inspector','Entry','2023-04-14','Pending',TRUE),

('EMP-013','Christopher Lee','First','Technician','Lead','2014-10-02','Current',TRUE),
('EMP-014','Isabella White','Second','Technician','Senior','2019-06-12','Current',TRUE),
('EMP-015','Benjamin Harris','Third','Technician','Intermediate','2021-08-24','Current',TRUE),

('EMP-016','Alexander Young','First','Supervisor','Lead','2012-01-09','Current',TRUE),
('EMP-017','Victoria Hall','Second','Supervisor','Lead','2013-09-27','Current',TRUE),

('EMP-018','Noah Allen','Weekend','Operator','Intermediate','2022-03-08','Current',TRUE),
('EMP-019','Ava Scott','First','Operator','Entry','2024-06-17','Pending',TRUE),
('EMP-020','Lucas Green','Second','Operator','Senior','2017-02-06','Current',TRUE);

-- ============================================================================
-- DEFECT TYPES
-- ============================================================================

INSERT INTO defect_types (
    defect_code,
    defect_name,
    defect_category,
    severity,
    description,
    active_flag
)
VALUES
    ('DIM-001', 'Diameter Out of Tolerance',      'Dimensional', 'Major',    'Measured diameter is outside the specified tolerance range.', TRUE),
    ('DIM-002', 'Length Out of Tolerance',        'Dimensional', 'Major',    'Overall part length is outside the specified tolerance range.', TRUE),
    ('DIM-003', 'Head Height Out of Tolerance',   'Dimensional', 'Minor',    'Fastener head height does not meet dimensional requirements.', TRUE),

    ('MAT-001', 'Material Certification Missing','Material',    'Critical', 'Required material certification is missing or incomplete.', TRUE),
    ('MAT-002', 'Incorrect Material Alloy',       'Material',    'Critical', 'Material alloy does not match the approved product specification.', TRUE),

    ('THR-001', 'Incomplete Thread',              'Thread',      'Major',    'Thread profile is incomplete or insufficiently formed.', TRUE),
    ('THR-002', 'Thread Damage',                  'Thread',      'Major',    'Threads contain deformation, dents, or other physical damage.', TRUE),
    ('THR-003', 'Thread Pitch Out of Tolerance',  'Thread',      'Critical', 'Thread pitch does not meet the required specification.', TRUE),

    ('SUR-001', 'Surface Scratch',                'Surface',     'Minor',    'Visible scratch or abrasion is present on the part surface.', TRUE),
    ('SUR-002', 'Crack Detected',                 'Surface',     'Critical', 'A crack or fracture indication is present on the part.', TRUE),
    ('SUR-003', 'Burr Present',                   'Surface',     'Major',    'Sharp or excess material remains after forming or machining.', TRUE),

    ('COA-001', 'Coating Thickness Out of Range', 'Coating',     'Major',    'Applied coating thickness is outside the approved range.', TRUE),
    ('COA-002', 'Uneven Coating',                 'Coating',     'Minor',    'Coating coverage is inconsistent across the part surface.', TRUE),

    ('ASM-001', 'Component Misalignment',         'Assembly',    'Major',    'Assembled components are not aligned correctly.', TRUE),
    ('ASM-002', 'Missing Component',              'Assembly',    'Critical', 'One or more required components are missing from the assembly.', TRUE),

    ('PKG-001', 'Incorrect Label',                'Packaging',   'Minor',    'Package label contains incorrect or incomplete information.', TRUE),
    ('PKG-002', 'Incorrect Quantity Packaged',    'Packaging',   'Major',    'Packaged quantity does not match the required quantity.', TRUE),
    ('PKG-003', 'Damaged Packaging',              'Packaging',   'Minor',    'Packaging is torn, crushed, or otherwise damaged.', TRUE);
