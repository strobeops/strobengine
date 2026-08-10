import asyncio
import shutil
import socket
import sys
import threading
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
