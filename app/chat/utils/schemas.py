from ninja import  Schema
from typing import List, Literal, Optional, Any, Dict
from langchain_core.messages import (
    AIMessage,
    HumanMessage,
    SystemMessage,
)

class Message(Schema):
    role: Literal["user", "assistant", "system"]
    content: str

class RunRequest(Schema):
    messages: List[Message]
    thread_id: str
    checkpoint_ns: Optional[str] = None

class RunResponse(Schema):
    # return the final assistant text plus any metadata you want
    answer: Optional[str] = None
    messages: List[Dict[str, Any]]

def to_lc_messages(msgs: List[Message]):
    out = []
    for m in msgs:
        if m.role == "user":
            out.append(HumanMessage(content=m.content))
        elif m.role == "assistant":
            out.append(AIMessage(content=m.content))
        elif m.role == "system":
            out.append(SystemMessage(content=m.content))
    return out