import logging
from ninja import Router
from typing import List
from django.db import DatabaseError
from django.http import HttpResponse, JsonResponse
from core.utils import DatabaseConnection
from sqlalchemy import create_engine

from .models import Dataset

from .utils import run_query, rows_to_csv


logger = logging.getLogger(__name__)
router = Router(tags=["datasets"])
conn = DatabaseConnection()
engine = create_engine(conn.settings_to_uri(for_sql_alchemy=True), pool_pre_ping=True, future=True)

@router.get("/list/")
def list_datasets(request):
    """
    Returns a list of available datasets with their descriptions.
    """
    try:
        datasets = Dataset.objects.all()
        if datasets.exists():
            return [
                {
                    "dataset_name": obj.name, 
                    "group": obj.group,
                    "description": obj.description
                } 
                for obj in datasets
            ]
        else:
            raise DatabaseError("Database connection is not available.")

    except DatabaseError as e:
        logger.error(f"Database error while listing datasets: {e}")
        return HttpResponse(
            content="Service temporarily unavailable due to a database error.",
            status=503
        )
    
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return HttpResponse(
            detail="An unexpected error occurred.",
            status=500
        )

@router.get("/single/{dataset_name}")
def export_dataset(request, dataset_name: str, format: str = "csv"):
    """
    Expose any predefined dataset as CSV (default) or JSON.
    Example:
      GET /datasets/students_by_country?format=json
    """
    file_name = f"{dataset_name}.csv"

    try:
        logger.info(f"Running dataset query: '{dataset_name}'")
        dataset = Dataset.objects.get(name=dataset_name)
        columns, rows = run_query(dataset.query )

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
    
    except Dataset.DoesNotExist:
            logger.warning(f"Invalid dataset request: '{dataset_name}'")
            return HttpResponse(f"No dataset object of name '{dataset_name}' exists in the database", status=400)

    except Exception as e:
        logger.error(f"Dataset export error for '{file_name}': {e}", exc_info=True)
        return HttpResponse(
            content=f"Failed to export dataset '{dataset_name}'. Details: {e}",
            status=500,
        )