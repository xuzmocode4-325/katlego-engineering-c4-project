import logging
from ninja import Router
from typing import List, Dict, Any, Union
from django.http import HttpResponse
from psycopg import OperationalError, ProgrammingError, Error as PsycopgError


from .utils.saver import CustomSaver
from .utils.graph import get_builder
from .utils.schemas import RunRequest, RunResponse, to_lc_messages

router = Router(tags=["agent"])
logger = logging.getLogger(__name__)
saver = CustomSaver()  # uses DATABASES["default"]

@router.post("/get_response")
def run_agent(request, payload: RunRequest):
    try:
        with saver.checkpoint_connection() as checkpointer:
            checkpointer.setup()  # safe to call every time

            builder = get_builder()
            graph = builder.compile(checkpointer=checkpointer)

            config = {"configurable": {
                "thread_id": payload.thread_id,
                "checkpoint_ns": payload.checkpoint_ns or "",
            }}

            state = graph.invoke({"messages": to_lc_messages(payload.messages)}, config)
            msgs = state.get("messages", [])

            answer = next(
                (getattr(m, "content", None) for m in reversed(msgs) if getattr(m, "type", "") == "ai"),
                None
            )

            formatted_messages = [
                m.dict() if hasattr(m, "dict") else {
                    "type": getattr(m, "type", "unknown"),
                    "content": getattr(m, "content", None),
                }
                for m in msgs
            ]
            return RunResponse(answer=answer, messages=formatted_messages)

    except (OperationalError, ProgrammingError, PsycopgError) as e:
        logger.error(f"Database error in run_agent: {e}")
        return HttpResponse(
            content="Service temporarily unavailable due to a database error.",
            status=503
        )
    except Exception as e:
        logger.error(f"An unexpected error occurred in run_agent: {e}", exc_info=True)
        return HttpResponse(
            content="An internal server error occurred.",
            status=500
        )
