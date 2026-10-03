import asyncio
import shutil
import socket
import sys
import threading
from collections.abc import Generator
from pathlib import Path

import pytest
from aiohttp import web

from .mock_server import create_app


def pytest_collection_modifyitems(config, items):
    # Automatically mark tests under the tests/e2e/ folder
    for item in items:
        if "e2e" in Path(item.fspath).parts:
            item.add_marker("e2e")

    # Skip e2e tests by default if --e2e option is missing
    if not config.getoption("--e2e"):
        skip_e2e = pytest.mark.skip(reason="need --e2e option to run")
        for item in items:
            if "e2e" in item.keywords:
                item.add_marker(skip_e2e)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def mock_server():
    port = _free_port()
    app = create_app()
    loop = asyncio.new_event_loop()

    runner = web.AppRunner(app)

    def _run():
        asyncio.set_event_loop(loop)
        loop.run_until_complete(runner.setup())
        site = web.TCPSite(runner, "127.0.0.1", port)
        loop.run_until_complete(site.start())
        loop.run_forever()

    thread = threading.Thread(target=_run, daemon=True)
    thread.start()

    yield f"http://127.0.0.1:{port}"

    loop.call_soon_threadsafe(loop.stop)
    thread.join(timeout=5)
    loop.run_until_complete(runner.cleanup())
    loop.close()


def _find_cli_bin() -> str:
    path = shutil.which("strobengine")
    if path:
        return path

    bin_dir = "Scripts" if sys.platform == "win32" else "bin"
    name = "strobengine.exe" if sys.platform == "win32" else "strobengine"
    return str(Path(sys.prefix) / bin_dir / name)


@pytest.fixture(scope="session")
def cli_bin() -> str:
    return _find_cli_bin()


@pytest.fixture
def blackhole_server() -> Generator[str]:
    """A TCP listener that accepts connections but never responds.

    Reproduces a blackholed target: clients that connect (e.g. a WebSocket
    handshake) wait forever for a reply that never comes.
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", 0))
    srv.listen(16)
    srv.settimeout(0.5)
    port = srv.getsockname()[1]
    stop = threading.Event()
    held: list[socket.socket] = []

    def _accept_loop() -> None:
        while not stop.is_set():
            try:
                conn, _ = srv.accept()
                held.append(conn)
            except TimeoutError:
                continue

    thread = threading.Thread(target=_accept_loop, daemon=True)
    thread.start()

    yield f"ws://127.0.0.1:{port}"

    stop.set()
    thread.join(timeout=5)
    for conn in held:
        conn.close()
    srv.close()


class RawSseServer:
    """Raw local SSE server for connection-behavior tests.

    Reads each request fully (so the close sends a clean FIN, not an RST),
    writes one response per connection, then closes the connection.

    Modes:
        "eof":         valid SSE response with one event, then clean close.
        "short_body":  Content-Length: 1000 with no body, then clean close
                       -- HTTP clients report a premature-close error with
                       zero events delivered.
    """

    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.accepts = 0
        self._stop = threading.Event()
        self._srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(64)
        self._srv.settimeout(0.5)
        port = self._srv.getsockname()[1]
        self.url = f"http://127.0.0.1:{port}/sse"
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._srv.accept()
            except TimeoutError:
                continue
            self.accepts += 1
            try:
                self._handle(conn)
            except OSError:
                pass
            finally:
                conn.close()

    def _handle(self, conn: socket.socket) -> None:
        conn.settimeout(2.0)
        data = b""
        while b"\r\n\r\n" not in data:
            chunk = conn.recv(4096)
            if not chunk:
                return
            data += chunk
        if self.mode == "short_body":
            conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Length: 1000\r\n\r\n")
        else:
            conn.sendall(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/event-stream\r\n"
                b"Connection: close\r\n\r\n"
                b"data: e1\n\n"
            )

    def close(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5)
        self._srv.close()


@pytest.fixture
def sse_eof_server() -> Generator[RawSseServer]:
    server = RawSseServer("eof")
    yield server
    server.close()


@pytest.fixture
def sse_short_body_server() -> Generator[RawSseServer]:
    server = RawSseServer("short_body")
    yield server
    server.close()
