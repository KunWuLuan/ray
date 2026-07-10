"""Client utilities for FuseModelDeployment.

Provides HTTP helpers for sending requests and Ray-based helpers
for control-plane operations (switch, stats, etc.).
"""

import logging
from typing import Any, Dict

logger = logging.getLogger(__name__)

BASE_URL = "http://localhost:8000"


# ---------------------------------------------------------------------------
# HTTP client (no Ray required)
# ---------------------------------------------------------------------------


def send_chat_request(
    model: str,
    message: str,
    base_url: str = BASE_URL,
    stream: bool = False,
    **kwargs: Any,
) -> Dict[str, Any]:
    """Send a chat completion request to the FuseModelDeployment.

    If the requested model is currently sleeping, the request will
    block (up to 5 minutes) until the controller switches to it.
    """
    import requests

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": message}],
        "stream": stream,
        **kwargs,
    }

    if stream:
        return requests.post(
            f"{base_url}/v1/chat/completions",
            json=payload,
            stream=True,
            timeout=300,
        )
    response = requests.post(
        f"{base_url}/v1/chat/completions",
        json=payload,
        timeout=300,
    )
    response.raise_for_status()
    return response.json()


def send_streaming_request(
    model: str,
    message: str,
    base_url: str = BASE_URL,
) -> Any:
    """Send a streaming chat request and yield response chunks.

    Usage::

        for chunk in send_streaming_request("llama-8b", "Hello!"):
            print(chunk, end="", flush=True)
    """
    import json

    import requests

    payload = {
        "model": model,
        "messages": [{"role": "user", "content": message}],
        "stream": True,
    }
    response = requests.post(
        f"{base_url}/v1/chat/completions",
        json=payload,
        stream=True,
        timeout=300,
    )
    response.raise_for_status()

    for line in response.iter_lines(decode_unicode=True):
        if line and line.startswith("data: "):
            data = line[6:]
            if data == "[DONE]":
                break
            chunk = json.loads(data)
            delta = chunk.get("choices", [{}])[0].get("delta", {})
            content = delta.get("content", "")
            if content:
                yield content


# ---------------------------------------------------------------------------
# Ray-based control-plane operations
# ---------------------------------------------------------------------------


def force_switch(controller_handle: Any, target_model: str) -> bool:
    """Force the controller to switch to a specific model."""
    import ray

    return ray.get(controller_handle.force_switch.remote(target_model))


def force_switch_with_level(
    controller_handle: Any, target_model: str, sleep_level: int
) -> bool:
    """Force switch with a specific sleep level for the current model."""
    import ray

    return ray.get(
        controller_handle.force_switch.remote(target_model, sleep_level=sleep_level)
    )


def clear_override(controller_handle: Any) -> None:
    """Clear the manual override and resume automatic mode."""
    import ray

    ray.get(controller_handle.clear_override.remote())


def get_status(controller_handle: Any) -> Dict[str, Any]:
    """Get the full status from the controller (QPS, stats, etc.)."""
    import ray

    return ray.get(controller_handle.get_status.remote())


def get_deployment_stats(handle: Any) -> Dict[str, Any]:
    """Get deployment-level stats (model states, in-flight, etc.)."""
    import ray

    return ray.get(handle.get_stats.remote())
