from __future__ import annotations

import asyncio
from typing import Any

from aiohttp import web

LAST_ECHO_KEY: web.AppKey[dict[str, Any]] = web.AppKey("last_echo")
FLAKY_STATE_KEY: web.AppKey[dict[str, bool]] = web.AppKey("flaky_state")
BROADCAST_KEY: web.AppKey[set[web.WebSocketResponse]] = web.AppKey("broadcast_clients")


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
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                await ws.send_str(msg.data)
            elif msg.type == web.WSMsgType.BINARY:
                await ws.send_bytes(msg.data)
            elif msg.type in (web.WSMsgType.CLOSED, web.WSMsgType.ERROR):
                break
    except Exception:
        pass
    finally:
        await ws.close()
    return ws


async def handle_ws_discard(request: web.Request) -> web.WebSocketResponse:
    """WebSocket endpoint that accepts and discards all messages (no echo)."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    try:
        async for msg in ws:
            if msg.type in (web.WSMsgType.CLOSED, web.WSMsgType.ERROR):
                break
    except Exception:
        pass
    finally:
        await ws.close()
    return ws


async def handle_broadcast(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    clients: set[web.WebSocketResponse] = request.app[BROADCAST_KEY]
    clients.add(ws)
    try:
        async for msg in ws:
            if msg.type in (web.WSMsgType.TEXT, web.WSMsgType.BINARY):
                dead: list[web.WebSocketResponse] = []
                for client in clients:
                    if client is not ws and not client.closed:
                        try:
                            if msg.type == web.WSMsgType.TEXT:
                                await client.send_str(msg.data)
                            else:
                                await client.send_bytes(msg.data)
                        except Exception:
                            dead.append(client)
                for client in dead:
                    clients.discard(client)
    finally:
        clients.discard(ws)
    return ws


async def handle_sse(request: web.Request) -> web.StreamResponse:
    """SSE endpoint that streams events. Query param ?count=N limits events."""
    count = int(request.query.get("count", "0"))
    response = web.StreamResponse(
        status=200,
        headers={
            "Content-Type": "text/event-stream",
            "Cache-Control": "no-cache",
        },
    )
    await response.prepare(request)
    i = 0
    try:
        while count == 0 or i < count:
            await response.write(f"data: event-{i}\n\n".encode())
            i += 1
            await asyncio.sleep(0.05)
    except (asyncio.CancelledError, ConnectionResetError):
        pass
    return response


def create_app() -> web.Application:
    app = web.Application()
    app[LAST_ECHO_KEY] = {}
    app[FLAKY_STATE_KEY] = {"value": False}
    app[BROADCAST_KEY] = set()
    app.router.add_route("*", "/echo", handle_echo)
    app.router.add_get("/last-echo", handle_last_echo)
    app.router.add_get("/status/{code}", handle_status)
    app.router.add_get("/delay/{seconds}", handle_delay)
    app.router.add_get("/flaky", handle_flaky)
    app.router.add_get("/ws", handle_websocket)
    app.router.add_get("/ws/discard", handle_ws_discard)
    app.router.add_get("/ws/broadcast", handle_broadcast)
    app.router.add_get("/sse", handle_sse)
    return app
