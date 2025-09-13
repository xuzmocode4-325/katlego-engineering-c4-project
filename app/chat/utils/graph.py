import os
import psycopg

from typing import Literal
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import SQLDatabaseToolkit

from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    ToolMessage,
    SystemMessage,
    RemoveMessage,
)

from langchain_groq import ChatGroq
from langgraph.graph import START, END, StateGraph
from langgraph.graph import MessagesState
from langgraph.prebuilt import ToolNode

from .saver import CustomSaver 


saver = CustomSaver()
llm = ChatGroq(model_name="openai/gpt-oss-20b", temperature=0)
db = SQLDatabase.from_uri(saver.settings_to_uri())

toolkit = SQLDatabaseToolkit(db=db, llm=llm)
tools = toolkit.get_tools()

get_schema_tool = next(t for t in tools if t.name == "sql_db_schema")
run_query_tool = next(t for t in tools if t.name == "sql_db_query")
list_tables_tool = next(t for t in tools if t.name == "sql_db_list_tables")

get_schema_node = ToolNode([get_schema_tool])
run_query_node = ToolNode([run_query_tool])
list_tables_node = ToolNode([list_tables_tool])


class State(MessagesState):
    summary: str

# --- Node: fabricate a tool call for list_tables and let ToolNode run it ---
def prep_list_tables(state: State):
    call_id = "list_tables-1"
    # Return an AI message that *requests* the tool; ToolNode will execute it next
    return {
        "messages": [
            AIMessage(
                content="",
                tool_calls=[{"name": "sql_db_list_tables", "args": {}, "id": call_id}],
            )
        ]
    }

# Optional: format the list tables result into a friendly AI message
def show_tables(state: State):
    # The last message should be the ToolMessage from the ToolNode
    tm = state["messages"][-1]
    if isinstance(tm, ToolMessage):
        return {"messages": [AIMessage(content=f"Available tables: {tm.content}")]}
    return {}

# --- Node: call get_schema via the model (the ToolNode will execute it) ---
def call_get_schema(state: State):
    llm_with_tools = llm.bind_tools([get_schema_tool], tool_choice="any")
    response = llm_with_tools.invoke(state["messages"])
    return {"messages": [response]}

# --- Node: generate SQL (may or may not call the run_query tool) ---
generate_query_system_prompt = (
    "You are an agent designed to interact with a SQL database.\n"
    f"Given an input question, create a syntactically correct {db.dialect} query to run,\n"
    "then look at the results of the query and return the answer. Unless the user\n"
    "specifies a specific number of examples they wish to obtain, always limit your\n"
    "query to at most 5 results.\n\n"
    "You can order the results by a relevant column to return the most interesting examples.\n"
    "Never SELECT *; only ask for relevant columns. Do not perform DML.\n"
)

def generate_query(state: State):
    llm_with_tools = llm.bind_tools([run_query_tool])  # let model decide to call or not
    msgs = [SystemMessage(content=generate_query_system_prompt), *state["messages"]]
    response = llm_with_tools.invoke(msgs)
    return {"messages": [response]}

# --- Node: optionally rewrite/validate SQL before execution ---
check_query_system_prompt = (
    "You are a SQL expert. Double-check the query for:\n"
    "- NOT IN with NULLs\n- UNION vs UNION ALL\n- BETWEEN exclusivity\n"
    "- Type mismatches\n- Quoted identifiers\n- Function arg counts\n"
    "- Correct casting\n- Proper join keys\n\n"
    "If issues exist, rewrite the query; otherwise return the original.\n"
    "Then call the execution tool."
)

def check_query(state: State):
    last = state["messages"][-1]
    tool_call = last.tool_calls[0]  # guarded by should_continue
    sql = tool_call["args"].get("query") or tool_call["args"].get("sql")
    llm_with_tools = llm.bind_tools([run_query_tool], tool_choice="any")
    response = llm_with_tools.invoke(
        [SystemMessage(content=check_query_system_prompt), HumanMessage(content=sql)]
    )
    # Keep the same id so the tool response threads correctly
    response.id = last.id
    return {"messages": [response]}

# --- Node: summarize + trim history ---
def summarize_conversation(state: State):
    prior = state.get("summary", "")
    prompt = (
        f"This is summary of the conversation to date: {prior}\n\n"
        "Extend the summary by taking into account the new messages above:"
        if prior
        else "Create a summary of the conversation above:"
    )
    msgs = [*state["messages"], HumanMessage(content=prompt)]
    response = llm.invoke(msgs)

    # keep only last 2 messages
    to_delete = [RemoveMessage(id=m.id) for m in state["messages"][:-2]]
    return {"summary": response.content, "messages": to_delete}

# --- Routing ---
def should_continue(state: State) -> Literal["check_query", "summarize_conversation"]:
    last = state["messages"][-1]
    return "check_query" if getattr(last, "tool_calls", None) else "summarize_conversation"
    

def get_builder() -> StateGraph:
    builder = StateGraph(State)

    builder.add_node("prep_list_tables", prep_list_tables)
    builder.add_node("list_tables", list_tables_node)
    builder.add_node("show_tables", show_tables)

    builder.add_node("call_get_schema", call_get_schema)
    builder.add_node("get_schema", get_schema_node)

    builder.add_node("generate_query", generate_query)
    builder.add_node("check_query", check_query)
    builder.add_node("run_query", run_query_node)

    builder.add_node("summarize_conversation", summarize_conversation)

    builder.add_edge(START, "prep_list_tables")
    builder.add_edge("prep_list_tables", "list_tables")
    builder.add_edge("list_tables", "show_tables")
    builder.add_edge("show_tables", "call_get_schema")
    builder.add_edge("call_get_schema", "get_schema")
    builder.add_edge("get_schema", "generate_query")

    builder.add_conditional_edges("generate_query", should_continue)
    builder.add_edge("check_query", "run_query")
    builder.add_edge("run_query", "generate_query")
    builder.add_edge("summarize_conversation", END)

    return builder

