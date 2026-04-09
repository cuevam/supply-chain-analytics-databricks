import os
from superset.db_engine_specs.base import BaseEngineSpec

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")

PREVENT_UNSAFE_DB_CONNECTIONS = False
ROW_LIMIT = 100000

# Register DuckDB as an allowed engine
ADDITIONAL_DATABASE_BACKENDS = [
    "duckdb",
]

class DuckDBEngineSpec(BaseEngineSpec):
    engine = "duckdb"
    engine_name = "DuckDB"
    default_driver = "duckdb_engine"

ADDITIONAL_DB_ENGINE_SPECS = [DuckDBEngineSpec]