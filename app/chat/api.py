import logging
from ninja import Router
from django.http import HttpResponse

from .graph import agent
from .utils.schemas import RunRequest, RunResponse

router = Router(tags=["agent"])
logger = logging.getLogger(__name__)

@router.post("/get_response")
def run_agent(request, payload: RunRequest):
    try:
        last_user = next((m.content for m in reversed(payload.messages) if m.role == "user"), "")
        state = agent.invoke({"messages": [{"role": "user", "content": last_user}]})
        msgs = state.get("messages", [])
        # last AI message = natural-language answer
        answer = next((m.content for m in reversed(msgs) if getattr(m, "type", "") == "ai"), None)
        # optional: serialize for visibility
        serial = [
            {"type": getattr(m, "type", ""), "name": getattr(m, "name", None), "content": getattr(m, "content", None)}
            for m in msgs
        ]
        return RunResponse(answer=answer or "", messages=serial)


    except Exception as e:
        logger.error(f"An unexpected error occurred in run_agent: {e}", exc_info=True)
        return HttpResponse(
            content="An internal server error occurred.",
            status=500
        )
