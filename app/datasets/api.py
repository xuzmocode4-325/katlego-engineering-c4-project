import logging
import csv
from io import StringIO
from ninja import Router
from django.http import HttpResponse, JsonResponse
from .queries import QUERIES
from django.db import connection

logger = logging.getLogger(__name__)
router = Router(tags=["datasets"])

# -------------------------------
# Utility Functions
# -------------------------------

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

# -------------------------------
# Dynamic Dataset Endpoint
# -------------------------------

@router.get("/{dataset_name}")
def export_dataset(request, dataset_name: str, format: str = "csv"):
    """
    Expose any predefined dataset as CSV (default) or JSON.
    Example:
      GET /datasets/students_by_country?format=json
    """
    file_name = f"{dataset_name}.csv"

    try:
        if dataset_name not in QUERIES:
            logger.warning(f"Invalid dataset request: '{dataset_name}'")
            return HttpResponse("Invalid dataset", status=400)

        logger.info(f"Running dataset query: '{dataset_name}'")
        columns, rows = run_query(QUERIES[dataset_name])

        # Return JSON if requested
        if format.lower() == "json":
            data = [dict(zip(columns, row)) for row in rows]
            logger.info(f"Export successful: '{dataset_name}' as JSON")
            return JsonResponse(data, safe=False)

        # Default: CSV
        csv_data = rows_to_csv(columns, rows)
        logger.info(f"Export successful: '{file_name}'")
        response = HttpResponse(csv_data, content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="{file_name}"'
        return response

    except Exception as e:
        logger.error(f"Dataset export error for '{file_name}': {e}", exc_info=True)
        return HttpResponse(
            content=f"Failed to export dataset '{dataset_name}'. Details: {e}",
            status=500,
        )
