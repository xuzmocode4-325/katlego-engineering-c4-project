import logging
import pandas as pd

logger =  logging.getLogger(__file__)

hours_mapping_dict = {
    "7-14 hours":  "7-14 hours",           
    "more than 14 hours": "More than 14 hours",
    "less than 6 hours": "Less than 7 hours", 
}

aims_mapping_dict = {
    'upskill': 'upskill', 
    'data': 'learn', 
    'connect': 'network',
    'build': 'enhance portfolio',
    'both': 'network & upskill',
    'more': 'learn & network'
}

standard_columns_list = [
    'timestamp', 'id', 'age_range_years', 'gender', 'country', 'referral', 
    'experience', 'track', 'hours_available', 'aim', 'motivation', 'skill_level',
    'completed_aptitude', 'aptitude_score', 'graduated'
]

transform_kwargs = {"hours_map":hours_mapping_dict, "aims_map":aims_mapping_dict, "standard_cols":standard_columns_list}


class EverythinDataTransform:
    def __init__(self, frame, hours_map, standard_cols, aims_map) -> None:
        self.frame = frame
        self.hours_map = hours_map
        self.standard_cols = standard_cols
        self.aims_map = aims_map

    def clean_data(self):
        short_names_df = self._map_short_column_names()
        std_cols_df = self._standardise_columns(short_names_df)
        split_skills_df = self._split_skill_level(std_cols_df)
        mapped_aims = self._map_aim_categories(split_skills_df)

        return mapped_aims
    
    def _map_short_column_names(self):
        logger.info("Shortening standardised column names")
        if len(self.frame.columns) == len(self.standard_cols):
            try:
                copy = self.frame.copy()
                columns = copy.columns
                map_dict = {col: columns[num] for num, col in enumerate(self.standard_cols)}
                copy.columns = [*map_dict.keys()]
                logger.info("Successfully shortened standardised column names.")
                return copy
            except Exception as e:
                logger.error(f"An error has occured attempting to shorten the standardised column names: {str(e)}")

    def _standardise_columns(self, frame): 
        logger.info("Standardising column values.")
        try: 
            copy = frame.copy()
            copy['referral'] = copy['referral'].apply(lambda row: row.replace(
                'through a geeks for geeks webinar', 'Geeks for Geeks')
            )
            copy['motivation'] = copy['motivation'].str.lower()
            copy['experience'] = copy['experience'].apply(
                lambda row: row.replace('six', '6')
            )
            copy['age_range_years'] =  copy['age_range_years'].apply(
                lambda row: row.replace('years', '')
            )
            copy['hours_available'] = copy['hours_available'].apply(
                lambda row: self.hours_map.get(row, row)
            )
            copy['track'] = copy['track'].str.lower()
            if 'timestamp' in copy.columns:
                copy['registration_date'] = copy['timestamp'].dt.date
                copy['registration_time'] = copy['timestamp'].dt.time
                copy = copy.drop(columns=['timestamp'])

            logger.info("Successfuly standardised column values.")
            return copy
    
        except Exception as e:
            logger.error(f"Failed to standardise column values due to the following error: {str(e)}")

    def _split_skill_level(self, frame):
        logger.info("Splitting the 'skill level' column into category and description.")
        try: 
            copy = frame.copy()
            skill_level = copy['skill_level']
            split_skill_level = skill_level.str.split('-')
            skill_label = split_skill_level.map(
                lambda x: x[0].strip()
            )
            skill_description = split_skill_level.map(
                lambda x: x[1].lower().strip()
            )
            copy.drop(columns=['skill_level'], inplace=True)
            copy['skill_level'] = skill_label.str.lower()
            copy['skill_level_description'] = skill_description
            new_standard_cols = ["registration_date", "registration_time"] + self.standard_cols[1:]
            copy = copy[new_standard_cols]

            logger.info("Successfully split the 'skill level' column.")
            return copy
        
        except Exception as e:
            logger.error(f"An error has occured attempting to split the 'skill level' column: {str(e)}")

    def _map_aim_categories(self, frame):
        logger.info("Mapping 'aim' column categories to standardised values")

        try:
            copy = frame.copy()
            extract_aim = lambda x:  x.split(" ")[1] if x.split(" ")[0] == 'Learn' else x.split(" ")[0]
            map_aims_func = lambda x: self.aims_map[extract_aim(x).lower()]
            copy['aim'] = copy['aim'].apply(lambda x: map_aims_func(x))

            logger.info("Successfully mapped 'aim' column categories" )
            return copy
        except Exception as e:
            logger.error(f"Failed to map 'aim' column categories: {str(e)}")
            