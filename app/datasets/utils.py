import csv
from io import StringIO
from typing import Dict
from django.db import connection



def run_query(query: str):
    """Run SQL query and return columns + rows."""
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [col[0] for col in cursor.description]
        rows = cursor.fetchall()
    return columns, rows

def rows_to_csv(columns, rows) -> str:
    """Convert query result to CSV string."""
    buffer = StringIO()
    writer = csv.writer(buffer)
    writer.writerow(columns)
    writer.writerows(rows)
    return buffer.getvalue()