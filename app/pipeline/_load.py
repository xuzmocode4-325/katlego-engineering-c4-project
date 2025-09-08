import pandas as pd
import logging

logger =  logging.getLogger(__file__)

class EverythinDataLoad: 
    def __init__(self, frame) -> None:
        self.frame = frame
        
    def _index_to_map(self, col):
        index = col.to_dict()
        index_map = {v: k for k, v in index.items()}
        return index_map

    def _prepare_age_range(self):
        try:
            copy = self.frame.copy()
            age_range = pd.DataFrame(copy['age_range'].unique())
            age_range.rename(columns={0: 'age_range'}, inplace=True)
            return age_range
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'age' dataframe: {str(e)}")
            
    def _prepare_country(self):
        try: 
            copy = self.frame.copy()
            country = pd.DataFrame(copy['country'].unique())
            country.rename(columns={0: 'country'}, inplace=True)
            return country
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'country' dataframe: {str(e)}")
    
    def _prepare_experience(self):
        try: 
            copy = self.frame.copy()
            experience = pd.DataFrame(copy['experience'].unique())
            experience.rename(columns={0: 'experience_level'}, inplace=True)
            return experience
        except Exception as e: 
            logger.error(f"An error has occured attempting to prepare the 'experience' dataframe: {str(e)}")

    def _prepare_track(self):
        try:
            copy = self.frame.copy()
            tracks = pd.DataFrame(copy['track'].unique())
            tracks.rename(columns={0: 'track'}, inplace=True)
            return tracks
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'track' dataframe: {str(e)}")
    
    def _prepare_referral(self):
        try: 
            copy = self.frame.copy()
            referral = pd.DataFrame(copy['referral'].unique())
            referral.rename(columns={0: 'source'}, inplace=True)
            return referral
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'referral' dataframe: {str(e)}")

    def _prepare_skills(self):
        try:
            copy = self.frame.copy()
            skills = copy[['skill_level', 'skill_level_description']].drop_duplicates(
                subset=['skill_level', 'skill_level_description']
            )
            skills.index = pd.Index(range(len(skills.index)))
            return skills.copy()
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'skill' dataframe: {str(e)}")

    def _prepare_aims(self):
        try:
            copy = self.frame.copy()
            aims = pd.DataFrame(copy['aim'].unique())
            return aims
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'aims' dataframe: {str(e)}")
    
    def _prepare_registrations(self):
        try:
            copy = self.frame.copy()
            registrations = copy[['id', 'registration_date', 'registration_time']]
            registrations = registrations.copy()
            registrations = registrations.rename(columns={'id', 'student_id'})
            return registrations
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'registrations' dataframe: {str(e)}")
    
    def _prepare_student_outcomes(self):
        try: 
            copy = self.frame.copy()
            outcomes = copy[['id','completed_aptitude', 'aptitude_score', 'graduated']]
            outcomes = outcomes.copy()
            outcomes.rename(columns={'id':'student_id'}, inplace=True)
            return outcomes
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'student outcomes' dataframe: {str(e)}")
    
    def _prepare_motivation(self):
        try:
            copy = self.frame.copy()
            mappings = self._prepare_key_mappings('aim')['aim']
            motivation = copy[['id', 'aim', 'motivation']] 
            motivation = motivation.copy()
            motivation['aim_id'] = motivation['aim'].apply(lambda x: mappings['aim'].get(x))
            motivation.drop(columns=['aim'], inplace=True)
            motivation.rename(columns={'id':'student_id'}, inplace=True)
            return motivation
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'motivation' dataframe: {str(e)}")
            
    
    def _prepare_students(self):
        try: 
            copy = self.frame.copy()
            foreign_key_columns = ['age_range', 'country', 'experience', 'track', 'referral', 'skill_level']
            mappings = self._prepare_key_mappings(foreign_key_columns)
            students = copy[['id', 'gender', 'age_range', 'country', 'experience', 'track', 'referral', 'skill_level']]
            students = students.copy()
            students['age_range_id'] = students['age_range'].apply(lambda x: mappings['age_range'].get(x))
            students['country_id'] = students['country'].apply(lambda x: mappings['country'].get(x))
            students['experience_id'] = students['experience'].apply(lambda x: mappings['experience'].get(x))
            students['track_id'] = students['track'].apply(lambda x: mappings['track'].get(x))
            students['referral_id'] = students['referral'].apply(lambda x: mappings['referral'].get(x))
            students['skill_level_id'] = students['skill_level'].apply(lambda x: mappings['skill_level'].get(x))
            students.drop(columns=foreign_key_columns, inplace=True)
            return students
        except Exception as e:
            logger.error(f"An error has occured attempting to prepare the 'student' dataframe: {str(e)}")
    
    def _prepare_key_mappings(self, *args):
        mappings = {} 
        
        if "aim" in args:
            aim = self._prepare_aims()
            if type(aim) == pd.DataFrame:
                aim_keys = self._index_to_map(aim['aim'])
                mappings["aims"] = aim_keys
        if "age_range" in args:
            age_range = self._prepare_age_range()
            if type(age_range) == pd.DataFrame:
                age_keys = self._index_to_map(age_range['age_range'])
                mappings["age_range"] = age_keys
        if "country" in args:
            country = self._prepare_country()
            if type(country) == pd.DataFrame:
                country_keys = self._index_to_map(country['country'])
                mappings["country"] = country_keys
        if "experience" in args:
            experience = self._prepare_experience()
            if type(experience) == pd.DataFrame:
                experience_keys = self._index_to_map(experience['experience_level'])
                mappings["experience"] = experience_keys
        if "track" in args:
            track = self._prepare_track()
            if type(track) == pd.DataFrame:
                track_keys = self._index_to_map(track['track'])
                mappings["track"] = track_keys
        if "referral" in args:
            referral = self._prepare_referral()
            if type(referral) == pd.DataFrame: 
                referral_keys = self._index_to_map(referral['source'])
                mappings["referral"] = referral_keys
        if "skill" in args: 
            skill = self._prepare_skills()
            if type(skill) == pd.DataFrame:
                skill_keys = self._index_to_map(skill['skill_level'])
                mappings["skill"] = skill_keys
        return mappings