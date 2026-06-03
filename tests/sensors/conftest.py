"""Pytest fixtures shared across sensor tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from tests.sensors.sftp_server import SFTPTestServer


@pytest.fixture
def sftp_server(tmp_path: Path) -> Iterator[SFTPTestServer]:
    root = tmp_path / "sftp-root"
    root.mkdir()
    server = SFTPTestServer(root=root, username="sensor", password="sensor")
    server.start()
    try:
        yield server
    finally:
        server.stop()
