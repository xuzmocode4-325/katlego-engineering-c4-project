import csv
from io import StringIO
from django.db import connection


def run_query_to_csv(query: str) -> str:
    """Executes a raw SQL query and returns CSV text."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()

    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()
