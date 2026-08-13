"""Application configuration for the BOM material-planning project."""

import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2:///bom_material_planning",
)
