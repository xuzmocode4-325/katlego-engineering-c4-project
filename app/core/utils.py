from django.conf import settings
from urllib.parse import quote_plus
from sqlalchemy import text

class DatabaseConnection:
    def __init__(self, alias: str = "default"):
        self.alias = alias
        self.parse_table_name = lambda x: f"core_{x.replace("_", "")}"

    def settings_to_uri(self, for_sql_alchemy = False) -> str:
        db = settings.DATABASES[self.alias]
        user = quote_plus(db.get("USER", "") or "")
        pwd  = quote_plus(db.get("PASSWORD", "") or "")
        host = db.get("HOST") or "localhost"
        port = db.get("PORT") or "5432"
        name = db.get("NAME")
        if for_sql_alchemy:
            return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{name}"
        else:
            return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"
        
    def table_exists(self, conn, table: str) -> bool:
        row = conn.execute(
            text("""
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_name = :table
                LIMIT 1
            """),
            {"table": table},
        ).first()
        return bool(row)

    def _snake_to_pascal(self, snake_str):
        # Split by underscore, capitalize each word, then join
        return ''.join(word.capitalize() for word in snake_str.split('_'))


