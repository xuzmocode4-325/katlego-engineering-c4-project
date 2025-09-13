
from django.conf import settings

import os

import psycopg
from psycopg.rows import dict_row
from langgraph.checkpoint.postgres import PostgresSaver
from urllib.parse import quote_plus



class CustomSaver:
    def __init__(self, alias: str = "default"):
        self.alias = alias

    def settings_to_uri(self) -> str:
        db = settings.DATABASES[self.alias]
        user = quote_plus(db.get("USER", "") or "")
        pwd  = quote_plus(db.get("PASSWORD", "") or "")
        host = db.get("HOST") or "localhost"
        port = db.get("PORT") or "5432"
        name = db.get("NAME")
        return f"postgresql://{user}:{pwd}@{host}:{port}/{name}"

    def checkpoint_connection(self):
        uri = self.settings_to_uri()
        saver = PostgresSaver.from_conn_string(uri) 
        return saver