import logging
from ninja import Router
from sqlalchemy import create_engine,text
from typing import List, Optional
from .utils import table_exists, DatabaseConnection
from .schemas import ErrorResponse, ColumnSchema, ForeignKeySchema, TableMetadata

logger = logging.getLogger(__name__)
router = Router(tags=["db-meta"])
connector = DatabaseConnection()

engine = create_engine(
    connector.settings_to_uri(for_sql_alchemy=True), 
    pool_pre_ping=True, 
    future=True
)

@router.get("/list",
    response={200: List[str], 500: ErrorResponse},
)
def list_tables(
    request, prefix: Optional[str] = "core_", exclude_substr: Optional[str] = "user"
):
    """
    List table names in a schema. You can filter by a prefix and/or exclude substring.
    """

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema='public'
                    ORDER BY table_name
                """),
            ).scalars().all()

        # Optional filters to mimic your original script
        if prefix:
            rows = [t for t in rows if t.startswith(prefix) or t.startswith('datasets')]
        if exclude_substr:
            rows = [t for t in rows if exclude_substr not in t]
        return rows
    
    except Exception as e:
        logger.error(str(e))
        return 500, ErrorResponse(detail=f"Failed to list tables")

@router.get("/table/{table_name}",
    response={200: TableMetadata, 404: ErrorResponse, 500: ErrorResponse},
)
def table_metadata(request, table_name: str, db_schema: str = "public"):
    """
    Return columns, primary keys, and foreign keys for a given table.
    """
    try:
        with engine.connect() as conn:
            # Verify table exists to avoid confusing 500s
            if not table_exists(conn, table_name):
                return 404, ErrorResponse(detail=f"Table '{db_schema}.{table_name}' not found")

            # Columns
            cols = conn.execute(
                text("""
                    SELECT
                        column_name,
                        data_type,
                        is_nullable,
                        column_default
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = :table
                    ORDER BY ordinal_position
                """),
                {"table": table_name},
            ).mappings().all()

            columns: List[ColumnSchema] = [
                ColumnSchema(
                    name=row["column_name"],
                    data_type=row["data_type"],
                    is_nullable=(row["is_nullable"] == "YES"),
                    default=row["column_default"],
                ) for row in cols
            ]

            # Primary keys
            pk_rows = conn.execute(
                text("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tco
                    JOIN information_schema.key_column_usage kcu
                      ON kcu.constraint_name = tco.constraint_name
                     AND kcu.table_schema = tco.table_schema
                    WHERE tco.constraint_type = 'PRIMARY KEY'
                      AND kcu.table_schema = 'public'
                      AND kcu.table_name = :table
                    ORDER BY kcu.ordinal_position
                """),
                {"table": table_name},
            ).scalars().all()

            # Foreign keys
            fk_rows = conn.execute(
                text("""
                    SELECT
                        kcu.column_name,
                        ccu.table_name AS foreign_table,
                        ccu.column_name AS foreign_column
                    FROM information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                      ON tc.constraint_name = kcu.constraint_name
                     AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                      ON ccu.constraint_name = tc.constraint_name
                     AND ccu.table_schema = tc.table_schema
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND kcu.table_schema = 'public'
                      AND kcu.table_name = :table
                    ORDER BY kcu.ordinal_position
                """),
                {"table": table_name},
            ).mappings().all()

            fks: List[ForeignKeySchema] = [
                ForeignKeySchema(
                    column=row["column_name"],
                    foreign_table=row["foreign_table"],
                    foreign_column=row["foreign_column"],
                ) for row in fk_rows
            ]

            return TableMetadata(
                table=table_name,
                columns=columns,
                primary_keys=pk_rows,
                foreign_keys=fks,
            )

    except Exception as e:
        return 500, ErrorResponse(detail="Failed to fetch table metadata")
