import logging
from ninja import Router, File
from ninja.files import UploadedFile
from django.core.files.uploadedfile import InMemoryUploadedFile
from .utils import Extract, Transform, Load, transform_kwargs, category_columns

# Initialize router with ETL tag
router = Router(tags=["etl"])

# Instantiate pipeline utilities
extractor = Extract()
transformer = Transform(**transform_kwargs)
loader = Load()

# Set up logger
logger = logging.getLogger(__name__)

@router.post("/")
def extract_transform_load_pipeline(request, file: UploadedFile):
    """
    Endpoint to run the full ETL pipeline on an uploaded file.
    Accepts small files in memory and large files on disk.
    """
    file_name = file.name

    try:
        logger.info("Extracting data...")
        if isinstance(file, InMemoryUploadedFile):
            # Read directly from memory
            raw_data = extractor.merge_frames(file.file)
        else:
            # Temporary file on disk (for large uploads)
            raw_data = extractor.merge_frames(file.temporary_file_path())

        logger.info("Cleaning data...")
        clean_data = transformer.clean_data(raw_data)

        logger.info("Loading data...")
        loader.load_data(clean_data, category_columns)

        logger.info(f"ETL pipeline completed successfully for file '{file_name}'")
        return {
            "message": f"File '{file_name}' has been loaded to the database successfully."
        }

    except Exception as e:
        logger.error(f"ETL pipeline error for file '{file_name}': {e}", exc_info=True)
        return {
            "error": f"Failed to process file '{file_name}'.",
            "details": str(e),
        }
