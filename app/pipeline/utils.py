import logging
import pandas as pd
from django_countries import countries
from django.db import transaction
from core.utils import snake_to_pascal
from core.models import (AgeRange, Country, Experience, Track, Referral, SkillLevel,
    Aim, Student, Motivation, HoursAvailable, Registration, Outcomes )
 
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
    'timestamp', 'id', 'age_range', 'gender', 'country', 'referral', 
    'experience', 'track', 'hours_available', 'aim', 'motivation', 'skill_level',
    'completed_aptitude', 'aptitude_score', 'graduated'
]

transform_kwargs = {
    "hours_map":hours_mapping_dict, 
    "aims_map":aims_mapping_dict, 
    "standard_cols":standard_columns_list
}

category_columns = [
    'age_range', 'country', 'experience', 'track', 'referral', 'skill_level', 'aim', 'hours_available'
]


class Extract:
    def __init__(self) -> None:
        pass
        
    def merge_frames(self, data_path):
        logger.info("Standardising column names")

        try: 
            frames = self._extract(data_path)
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


    def _extract(self, data_path):
        logger.info("Uploading data")
        try:
            xls_file = pd.ExcelFile(data_path)
            sheets = xls_file.sheet_names
            names = ", ".join(str(name) for name in xls_file.sheet_names)
            logger.info(f"Successfully uploaded {len(sheets)} named {names} from file.")
            frames = [xls_file.parse(sheet) for sheet in sheets]
            return frames
        except Exception as e: 
            logger.error("Cannot upload sheets from file")


class Transform:
    def __init__(self, hours_map, standard_cols, aims_map) -> None:
        self.frame = None
        self.hours_map = hours_map
        self.standard_cols = standard_cols
        self.aims_map = aims_map

    def clean_data(self, frame):
        short_names_df = self._map_short_column_names(frame)
        std_cols_df = self._standardise_columns(short_names_df)
        split_skills_df = self._split_skill_level(std_cols_df)
        mapped_aims_df = self._map_aim_categories(split_skills_df)
        if isinstance(mapped_aims_df, pd.DataFrame):
            mapped_aims_df.columns = mapped_aims_df.columns.str.strip()
            return mapped_aims_df
    
    def _map_short_column_names(self, frame):
        logger.info("Shortening standardised column names")
        if len(frame.columns) == len(self.standard_cols):
            try:
                copy = frame.copy()
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
            copy['age_range'] =  copy['age_range'].apply(
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
            copy['skill_description'] = skill_description
            new_standard_cols = ["registration_date", "registration_time"] + self.standard_cols[1:]
            new_standard_cols.insert(new_standard_cols.index("skill_level") + 1, "skill_description")
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
            
class Load: 
    def __init__(self) -> None:
        self.frame = None
        
    def _index_to_map(self, col):
        index = col.to_dict()
        index_map = {v: k for k, v in index.items()}
        return index_map
    
    def _prepare_cat_frame(self, cat):
        copy = self.frame.copy()
        frame = pd.DataFrame()

        if cat == "skill_level":
            cols = [col for col in copy.columns if "skill" in col]
            frame = copy[cols].drop_duplicates(
                subset=cols
            )
        else: 
            frame = pd.DataFrame(copy[cat].unique())
            frame.rename(columns={0: cat}, inplace=True)

        frame[cat] = frame[cat].astype('category')
        frame.index = pd.Index(range(1, len(frame.index) + 1))
        return frame
    
    def _prepare_students(self):
        copy = self.frame.copy()
        students = copy[['id', 'gender'] + self.cat_frames].copy()

        for col in self.cat_frames:
            cat_df = self._prepare_cat_frame(col)
            map_dict = {getattr(row, col): row.Index for row in cat_df.itertuples()}
            students[f'{col}_id'] = students[col].map(map_dict)

        students.drop(columns=self.cat_frames, inplace=True)
        return students

    def _prepare_motivation(self):
        copy = self.frame.copy()
        motivation = copy[['id', 'aim', 'motivation']].copy()
        motivation.rename(columns={'id': 'student_id'}, inplace=True)

        # Map Aim names to actual DB IDs
        aim_map = {obj.aim: obj.id for obj in Aim.objects.all()}
        motivation['aim_id'] = motivation['aim'].map(aim_map)
        motivation.drop(columns=['aim'], inplace=True)

        return motivation
    
    def _prepare_registrations(self):
        copy = self.frame.copy()
        registrations = copy[['id', 'registration_date', 'registration_time']].copy()
        registrations = registrations.rename(columns={'id':'student_id'})
        return registrations
    
    def _prepare_student_outcomes(self):
        copy = self.frame.copy()
        outcomes = copy[['id','completed_aptitude', 'aptitude_score', 'graduated']].copy()
        outcomes['completed_aptitude'] = outcomes['completed_aptitude'].map({'Yes': True, 'No': False})
        outcomes['graduated'] = outcomes['graduated'].map({'Yes': True, 'No': False})
        outcomes.rename(columns={'id':'student_id'}, inplace=True)
        return outcomes
    
    def _load_cat_frame(self, frame_dict: dict):
        key = list(frame_dict.keys())[0]
        frame = frame_dict[key]

        if not isinstance(frame, pd.DataFrame):
            raise ValueError(f"Expected DataFrame for {key}, got {type(frame)}")

        ModelName = snake_to_pascal(key)
        Model = globals()[ModelName]  # assumes model names match cat_frame title case

        for row in frame.itertuples(index=True, name='Row'):
            idx = row.Index
            if key not in row._fields:
                raise ValueError(f"Column '{key}' not found in row fields: {row._fields}")

            prime_raw = getattr(row, key)
            prime_value = countries.by_name(prime_raw) if ModelName == "Country" else prime_raw

            lookup = {"id": idx}
            defaults = {key: prime_value}

            # Add other columns
            for col_name in row._fields[1:]:
                if col_name != key:
                    defaults[col_name] = getattr(row, col_name)

            Model.objects.update_or_create(
                **lookup,
                defaults=defaults
            )

    def _load_student(self):
        # Load Students
        students_df = self._prepare_students()

         # Validate FK integrity before inserting
        self._validate_foreign_keys(students_df)

        for _, row in students_df.iterrows():
            try:
                Student.objects.update_or_create(
                    student_id=row['id'],  # lookup field
                    defaults={
                        'gender': row['gender'],
                        'age_range_id': int(row['age_range_id']),
                        'country_id': int(row['country_id']),
                        'referral_id': int(row['referral_id']),
                        'experience_id': int(row['experience_id']),
                        'track_id': int(row['track_id']),
                        'skill_level_id': int(row['skill_level_id']),
                        'hours_available_id':int(row['hours_available_id'])
                    }
                )
            except Exception as e:
                logger.error(f"Failed to insert student {row['id']}: {str(e)}")
                raise

    def _load_motivation(self):
        # Load Motivation
        motivation_df = self._prepare_motivation()
        for _, row in motivation_df.iterrows():
            student_obj = Student.objects.get(student_id=row['student_id'])
            aim_obj = Aim.objects.get(id=row['aim_id']) 
            Motivation.objects.update_or_create(
                student=student_obj,   
                aim=aim_obj,
                defaults={'motivation': row['motivation']}
            )

    def _load_registration(self):
        # Load Registrations
        registrations_df = self._prepare_registrations()
        for _, row in registrations_df.iterrows():
            student_obj = Student.objects.get(student_id=row['student_id'])
            Registration.objects.update_or_create(
                student=student_obj,  
                defaults={
                    'date': row['registration_date'],
                    'time': row['registration_time']
                }
            )

    def _load_outcomes(self):
        # Load Outcomes
        outcomes_df = self._prepare_student_outcomes()
        for _, row in outcomes_df.iterrows():
            student_obj = Student.objects.get(student_id=row['student_id'])
            Outcomes.objects.update_or_create(
                student=student_obj,  
                defaults={
                    'completed_aptitude': row['completed_aptitude'],
                    'aptitude_score': row['aptitude_score'],
                    'graduated': row['graduated']
                }
            )

    def _validate_foreign_keys(self, students_df):
    # Collect all foreign key ids from students_df
        fk_fields = {
            'age_range_id': AgeRange,
            'country_id': Country,
            'experience_id': Experience,
            'track_id': Track,
            'referral_id': Referral,
            'skill_level_id': SkillLevel,
            'hours_available_id': HoursAvailable
        }

        for fk_field, Model in fk_fields.items():
            if fk_field in students_df.columns:
                unique_keys = students_df[fk_field].dropna().unique()
                existing_keys = set(Model.objects.filter(id__in=unique_keys).values_list('id', flat=True))
                missing_keys = set(unique_keys) - existing_keys
                if missing_keys:
                    raise ValueError(
                        f"Foreign key values {missing_keys} for field '{fk_field}' are missing in {Model.__name__} table."
                    )
    
    def load_data(self, frame, cat_frames):
        self.frame = frame
        self.cat_frames = cat_frames
        cat_frames = [{name: self._prepare_cat_frame(name)} for name in self.cat_frames]
        
        with transaction.atomic():
        # Load categorical frames (AgeRange, Country, Experience, etc.)
            for frame_dict in cat_frames:
                self._load_cat_frame(frame_dict)

        self._load_student()
        self._load_motivation()
        self._load_registration()
        self._load_outcomes()