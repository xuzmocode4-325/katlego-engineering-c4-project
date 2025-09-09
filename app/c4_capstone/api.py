import logging
from ninja import NinjaAPI, File
from ninja.files import UploadedFile
from datasets.api import router as dataset_router
from pipeline.api import router as etl_router


logger =  logging.getLogger(__file__)

api = NinjaAPI()

api.add_router("/datasets/", dataset_router)
api.add_router("/etl/", etl_router) 