# chat/report_api.py
from ninja import Router
from sqlalchemy import create_engine
from typing import Optional
from django.http import HttpResponse
from .utils import VisualReportGenerator 

from core.utils import DatabaseConnection

connector = DatabaseConnection()
engine = create_engine(connector.settings_to_uri(for_sql_alchemy=True))
gen = VisualReportGenerator(engine)

router = Router(tags=["report"])

@router.get("/download.png")
def download_png(request, dpi: int = 200, download: Optional[bool] = False):
    """
    Generate the visual report and return it as a PNG image.
    - dpi:    Render DPI (default 200)
    - download: if true, force download; otherwise display inline
    """
    
    png_bytes = gen.create_report(return_bytes=True, dpi=dpi)

    resp = HttpResponse(png_bytes.getvalue(), content_type="image/png")
    disp = 'attachment' if download else 'inline'
    resp["Content-Disposition"] = f'{disp}; filename="report.png"'
    # Optional cache headers (avoid caching if the data changes often)
    resp["Cache-Control"] = "no-store"
    return resp