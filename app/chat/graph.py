import os
from typing import Literal
from sqlalchemy import create_engine, inspect
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langgraph.prebuilt import create_react_agent
from langchain_groq import ChatGroq  # or any tool-calling LLM

from core.utils import DatabaseConnection

connector = DatabaseConnection()
llm = ChatGroq(model_name="openai/gpt-oss-120b", temperature=0)

engine = create_engine(connector.settings_to_uri(for_sql_alchemy=True), pool_pre_ping=True, future=True)
inspector = inspect(engine)

allowed_schema = "public"
allowed = [
    t for t in inspector.get_table_names(schema=allowed_schema)
    if t.startswith("core_") 
    or t.startswith("datasets") 
    and "user" not in t
]

db = SQLDatabase.from_uri(
    connector.settings_to_uri(),
    include_tables=allowed, 
    sample_rows_in_table_info=0, 
    view_support=False
)

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()


system_prompt = f"""
You are an agent designed to interact with a SQL database (dialect: {db.dialect}).
The database contains information records related to the past participants in "Everything Data" programme. 

Goal: answer the user's question about the "Everything Data" in clear, non-technical language.

Process:
- Look at available tables, then fetch schemas of relevant tables.
- Write a syntactically correct query (limit to 5 rows unless told otherwise).
- ALWAYS double-check the query before executing.
- Run the query and use the results to answer.

Rules:
- Never perform DML (INSERT/UPDATE/DELETE/DROP/CREATE).
- Never SELECT *; pick relevant columns.
- In your FINAL answer: do NOT show SQL; respond in plain language only.
- Consult the 'dataset_datasets' table for prebuilt queries.
"""


agent = create_react_agent(llm, tools, prompt=system_prompt)