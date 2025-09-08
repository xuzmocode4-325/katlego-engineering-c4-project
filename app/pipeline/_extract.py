import logging
import pandas as pd
 
logger =  logging.getLogger(__file__)

class Extract:
    def __init__(self, data_path) -> None:
        self.data_path = data_path

    def merge_frames(self):
        logger.info("Standardising column names")

        try: 
            frames = self._extract()
            if type(frames) == list and len(frames) > 0: 
                frame_cols = [frame.columns for frame in frames]
                prime_frame_cols = frame_cols[0]
                prime_frame_cols_lower = [col.lower() for col in prime_frame_cols]

                new_frames = []

                flag = {}
                
                for i, frame in enumerate(frames):
                    if [col.lower() for col in prime_frame_cols] == prime_frame_cols_lower:
                        frame.columns = prime_frame_cols_lower
                        new_frames.append(frame)
                    else: 
                        flag[i] = frame

                new_frame = pd.concat(new_frames, axis=0)

                logger.info("Column names successfully standardised.")

                return new_frame
        except Exception as e:
            logger.error(f"Column name standardisation failed due to {str(e)}")


    def _extract(self):
        logger.info("Uploading data")
        try:
            xls_file = pd.ExcelFile(self.data_path)
            sheets = xls_file.sheet_names
            names = ", ".join(str(name) for name in xls_file.sheet_names)
            logger.info(f"Successfully uploaded {len(sheets)} named {names} from file.")
            frames = [xls_file.parse(sheet) for sheet in sheets]
            return frames
        except Exception as e: 
            logger.error("Cannot upload sheets from file")