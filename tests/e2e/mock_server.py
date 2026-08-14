from __future__ import annotations

import asyncio
from typing import Any

from aiohttp import web

LAST_ECHO_KEY: web.AppKey[dict[str, Any]] = web.AppKey("last_echo")
FLAKY_STATE_KEY: web.AppKey[dict[str, bool]] = web.AppKey("flaky_state")


async def handle_status(request: web.Request) -> web.Response:
    code = int(request.match_info["code"])
    return web.Response(status=code, text=f"status {code}")


async def handle_delay(request: web.Request) -> web.Response:
    seconds = float(request.match_info["seconds"])
    await asyncio.sleep(seconds)
    return web.Response(status=200, text=f"delayed {seconds}s")


async def handle_echo(request: web.Request) -> web.Response:
    body = None
    content_type = request.content_type or ""

    if "json" in content_type:
        try:
            body = await request.json()
        except (ValueError, Exception):
            body = None
    else:
        raw = await request.read()
        if raw:
            body = raw.decode("utf-8", errors="replace")

    result = {
        "method": request.method,
        "headers": {k.lower(): v for k, v in request.headers.items()},
        "body": body,
    }
    request.app[LAST_ECHO_KEY]["value"] = result

    return web.json_response(result)


async def handle_last_echo(request: web.Request) -> web.Response:
    return web.json_response(request.app[LAST_ECHO_KEY].get("value") or {})


async def handle_flaky(request: web.Request) -> web.Response:
    state = request.app[FLAKY_STATE_KEY]
    current = state["value"]
    state["value"] = not current
    if current:
        return web.Response(status=200, text="ok")
    return web.Response(status=500, text="internal error")


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    async for msg in ws:
        if msg.type == web.WSMsgType.TEXT:
            await ws.send_str(msg.data)
        elif msg.type in (web.WSMsgType.CLOSED, web.WSMsgType.ERROR):
            break
    return ws


def create_app() -> web.Application:
    app = web.Application()
    app[LAST_ECHO_KEY] = {}
    app[FLAKY_STATE_KEY] = {"value": False}
    app.router.add_route("*", "/echo", handle_echo)
    app.router.add_get("/last-echo", handle_last_echo)
    app.router.add_get("/status/{code}", handle_status)
    app.router.add_get("/delay/{seconds}", handle_delay)
    app.router.add_get("/flaky", handle_flaky)
    app.router.add_get("/ws", handle_websocket)
    return app
