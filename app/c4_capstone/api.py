import logging
from ninja import NinjaAPI, File
from ninja.files import UploadedFile
from core.api import router as db_meta_router
from datasets.api import router as dataset_router
from pipeline.api import router as etl_router
from chat.api import router as chat_router
from reports.api import router as report_router


logger =  logging.getLogger(__file__)

api = NinjaAPI()

api.add_router("/etl/", etl_router) 
api.add_router("/db-meta", db_meta_router) 
api.add_router("/datasets/", dataset_router)
api.add_router("/chat/", chat_router)
api.add_router("reports/", report_router)