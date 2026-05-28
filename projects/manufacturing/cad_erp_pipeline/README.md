## Data Sources

This project currently uses synthetically generated manufacturing and engineering metadata to simulate CAD-to-ERP workflows commonly found in aerospace and manufacturing environments.

In production environments, similar data is commonly sourced from:
- CAD/PLM platforms
- ERP systems
- manufacturing databases
- engineering BOM exports
- supplier/inventory systems

Example public datasets and references:
- https://www.kaggle.com/datasets/shivamb/machine-predictive-maintenance-classification
- https://www.kaggle.com/datasets/amirmotefaker/supply-chain-dataset

These will simulate:

* cad_parts_export.json → CAD/PLM part metadata
* assembly_bom_export.json → nested assembly/BOM structure
* suppliers_export.csv → ERP supplier data
* inventory_export.csv → inventory/warehouse data


