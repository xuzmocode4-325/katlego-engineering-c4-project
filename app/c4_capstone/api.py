import logging
from ninja import NinjaAPI, File
from ninja.files import UploadedFile
from pipeline.utils import Extract, Transform, Load, transform_kwargs, category_columns
from django.core.files.uploadedfile import InMemoryUploadedFile
logger =  logging.getLogger(__file__)


api = NinjaAPI()
extractor = Extract()
transformer = Transform(**transform_kwargs)
loader = Load()


@api.post("/etl")
def extract_transform_load_pipeline(request, file: File[UploadedFile]):
    logger = logging.getLogger(__name__)
    file_name = file.name
    
    try:
        # Read file content directly or save temporarily if large
        logger.info("extracting data...")
        if isinstance(file, InMemoryUploadedFile):
            # Read directly from memory
            raw_data = extractor.merge_frames(file.file)
        else:
            # Temporary file on disk (handle large files)
            raw_data = extractor.merge_frames(file.temporary_file_path())
        
        logger.info("cleaning data...")
        clean_data = transformer.clean_data(raw_data)

        logger.info("loading data...")
        loader.load_data(clean_data, category_columns)
        return {
             "message": f"File '{file_name}' has been loaded to the database successfully."
        }
    except Exception as e:
        logger.error(f"ETL pipeline error for file '{file_name}': {e}", exc_info=True)
        return {
            "error": f"Failed to process file '{file_name}'.",
            "details": str(e)
        }