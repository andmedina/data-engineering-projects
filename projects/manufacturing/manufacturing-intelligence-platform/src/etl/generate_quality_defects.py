import random


DEFECT_CODES_BY_MEASUREMENT = {
    "Diameter": ["DIM-001"],
    "Length": ["DIM-002"],
    "Head Height": ["DIM-003"],
    "Thread Pitch": ["THR-003", "THR-001", "THR-002"],
    "Surface Finish": ["SUR-001", "SUR-002", "SUR-003"],
    "Coating Thickness": ["COA-001", "COA-002"],
    "Tensile Strength": ["MAT-002", "SUR-002"],
    "Assembly Gap": ["ASM-001", "ASM-002"],
    "Package Count": ["PKG-002", "PKG-001", "PKG-003"],
}

ROOT_CAUSES_BY_CATEGORY = {
    "Dimensional": (["Machine", "Method", "Operator", "Measurement"], [40, 30, 20, 10]),
    "Material": (["Material", "Method", "Measurement"], [70, 20, 10]),
    "Thread": (["Machine", "Method", "Operator"], [50, 35, 15]),
    "Surface": (["Machine", "Material", "Environment", "Method"], [35, 30, 20, 15]),
    "Coating": (["Method", "Environment", "Material"], [50, 30, 20]),
    "Assembly": (["Operator", "Method", "Machine"], [45, 35, 20]),
    "Packaging": (["Operator", "Method", "Unknown"], [50, 35, 15]),
    "Other": (["Unknown", "Method", "Operator"], [50, 30, 20]),
}

CORRECTIVE_ACTION_BY_ROOT_CAUSE = {
    "Machine": "Inspect machine setup and recalibrate affected equipment.",
    "Material": "Quarantine the material lot and initiate supplier review.",
    "Method": "Review the work instruction and correct the process method.",
    "Measurement": "Verify the gauge and repeat measurement-system checks.",
    "Operator": "Provide operator feedback and verify required training.",
    "Environment": "Correct environmental conditions and recheck the process.",
    "Unknown": "Open a root-cause investigation before final disposition.",
}


def partition_defect_quantity(total_quantity, defect_count):
    """Partition failed units into positive quantities across defect types."""

    if defect_count == 1:
        return [total_quantity]

    cut_points = sorted(
        random.sample(range(1, total_quantity), defect_count - 1)
    )
    boundaries = [0, *cut_points, total_quantity]
    return [
        boundaries[index + 1] - boundaries[index]
        for index in range(defect_count)
    ]


def choose_disposition(severity, inspection_result, defect_category):
    """Choose a disposition based on severity and inspection outcome."""

    if inspection_result == "Conditional":
        return random.choices(
            ["Rework", "Use As Is", "Pending Review"],
            weights=[45, 35, 20],
            k=1,
        )[0]

    if severity == "Critical":
        dispositions = ["Scrap", "Rework", "Return to Supplier", "Pending Review"]
        weights = [50, 20, 20, 10]
    elif severity == "Major":
        dispositions = ["Rework", "Scrap", "Use As Is", "Pending Review"]
        weights = [50, 25, 10, 15]
    else:
        dispositions = ["Rework", "Use As Is", "Scrap", "Pending Review"]
        weights = [40, 40, 10, 10]

    if defect_category != "Material" and "Return to Supplier" in dispositions:
        supplier_index = dispositions.index("Return to Supplier")
        dispositions[supplier_index] = "Scrap"

    return random.choices(dispositions, weights=weights, k=1)[0]


def choose_root_cause(defect_category):
    """Choose a root-cause category compatible with the defect category."""

    categories, weights = ROOT_CAUSES_BY_CATEGORY[defect_category]
    return random.choices(categories, weights=weights, k=1)[0]


def generate_quality_defects(quality_inspections, defect_types):
    """Generate classified defect records for failed inspection samples."""

    defect_types_by_code = {
        defect_type["defect_code"]: defect_type
        for defect_type in defect_types
    }
    quality_defects = []

    for inspection in quality_inspections:
        failed_quantity = inspection["failed_quantity"]

        if failed_quantity == 0:
            continue

        eligible_codes = DEFECT_CODES_BY_MEASUREMENT[
            inspection["measurement_type"]
        ]
        eligible_defects = [
            defect_types_by_code[code]
            for code in eligible_codes
            if code in defect_types_by_code
        ]

        if not eligible_defects:
            raise ValueError(
                "No compatible defect type found for measurement: "
                f"{inspection['measurement_type']}"
            )

        defect_count = min(
            len(eligible_defects),
            failed_quantity,
            random.choices([1, 2, 3], weights=[70, 25, 5], k=1)[0],
        )
        selected_defects = random.sample(eligible_defects, defect_count)
        defect_quantities = partition_defect_quantity(
            failed_quantity,
            defect_count,
        )

        for defect_type, defect_quantity in zip(
            selected_defects,
            defect_quantities,
        ):
            root_cause = choose_root_cause(defect_type["defect_category"])
            disposition = choose_disposition(
                defect_type["severity"],
                inspection["inspection_result"],
                defect_type["defect_category"],
            )

            quality_defects.append(
                {
                    "inspection_id": inspection["inspection_id"],
                    "defect_type_id": defect_type["defect_type_id"],
                    "defect_quantity": defect_quantity,
                    "disposition": disposition,
                    "root_cause_category": root_cause,
                    "corrective_action": CORRECTIVE_ACTION_BY_ROOT_CAUSE[
                        root_cause
                    ],
                }
            )

    return quality_defects
