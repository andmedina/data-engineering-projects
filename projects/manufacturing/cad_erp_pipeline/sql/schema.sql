DROP TABLE IF EXISTS bom;
DROP TABLE IF EXISTS inventory;
DROP TABLE IF EXISTS parts;
DROP TABLE IF EXISTS suppliers;

CREATE TABLE suppliers (
    supplier_id VARCHAR(20) PRIMARY KEY,
    supplier_name VARCHAR(100) NOT NULL,
    country VARCHAR(50) NOT NULL,
    supplier_type VARCHAR(50) NOT NULL
);

CREATE TABLE parts (
    part_number VARCHAR(30) PRIMARY KEY,
    part_name VARCHAR(150) NOT NULL,
    revision VARCHAR(10) NOT NULL,
    material VARCHAR(100) NOT NULL,
    weight_kg NUMERIC(10, 2) NOT NULL CHECK (weight_kg > 0),
    cad_system VARCHAR(50) NOT NULL,
    engineering_status VARCHAR(30) NOT NULL CHECK (
        engineering_status IN ('released', 'in_review', 'obsolete')
    ),
    supplier_id VARCHAR(20) NOT NULL REFERENCES suppliers(supplier_id)
);

CREATE TABLE inventory (
    part_number VARCHAR(30) PRIMARY KEY REFERENCES parts(part_number),
    stock_quantity INTEGER NOT NULL CHECK (stock_quantity >= 0),
    reorder_level INTEGER NOT NULL CHECK (reorder_level >= 0),
    warehouse_location VARCHAR(20) NOT NULL,
    last_updated DATE NOT NULL,
    below_reorder_level BOOLEAN NOT NULL
);

CREATE TABLE bom (
    assembly_id VARCHAR(30) NOT NULL,
    assembly_name VARCHAR(150) NOT NULL,
    assembly_revision VARCHAR(10) NOT NULL,
    part_number VARCHAR(30) NOT NULL REFERENCES parts(part_number),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    PRIMARY KEY (assembly_id, part_number)
);
