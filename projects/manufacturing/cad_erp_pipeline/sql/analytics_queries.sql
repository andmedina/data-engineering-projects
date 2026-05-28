-- Parts below reorder threshold
SELECT
    part_number,
    stock_quantity,
    reorder_level,
    warehouse_location
FROM inventory
WHERE below_reorder_level = TRUE;


-- Count parts by supplier
SELECT
    supplier_id,
    COUNT(*) AS total_parts
FROM parts
GROUP BY supplier_id
ORDER BY total_parts DESC;


-- Assemblies with the most components
SELECT
    assembly_id,
    assembly_name,
    SUM(quantity) AS total_components
FROM bom
GROUP BY assembly_id, assembly_name
ORDER BY total_components DESC;


-- Engineering parts currently in review
SELECT
    part_number,
    part_name,
    revision
FROM parts
WHERE engineering_status = 'in_review';


-- Inventory distribution by warehouse
SELECT
    warehouse_location,
    COUNT(*) AS total_parts,
    SUM(stock_quantity) AS total_inventory
FROM inventory
GROUP BY warehouse_location
ORDER BY total_inventory DESC;


-- Suppliers supporting multiple engineering parts
SELECT
    supplier_id,
    COUNT(part_number) AS supported_parts
FROM parts
GROUP BY supplier_id
HAVING COUNT(part_number) > 5
ORDER BY supported_parts DESC;