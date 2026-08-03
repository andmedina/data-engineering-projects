"""Shared application configuration.

The default PostgreSQL URL uses the current operating-system user and the
project database name. Set ``DATABASE_URL`` in the shell when different
credentials, a host, or a port are required.
"""

import os


DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2:///manufacturing_intelligence",
)
