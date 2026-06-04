"""Tests for the SFTP sensor."""

from __future__ import annotations

import json
import time
from pathlib import Path

import pytest

from prefect_sensor.sensors.sftp import SFTPSensor


@pytest.mark.asyncio
async def test_sftp_sensor_e2e(sftp_server) -> None:
    remote_dir = sftp_server.root / "incoming" / "reports"
    remote_dir.mkdir(parents=True)
    report = remote_dir / "report.csv"
    report.write_text("version 1\n")

    s = SFTPSensor(
        SFTPSensor.Config(
            name="s",
            hostname=sftp_server.host,
            port=sftp_server.port,
            username=sftp_server.username,
            password=sftp_server.password,
            remote_directories=["/incoming/reports"],
            poll_interval_seconds=0,
        )
    )
    await s.setup()
    try:
        agen = s.observe().__aiter__()
        obs = await agen.__anext__()
        assert obs.event_type == "file.appeared"
        assert (
            obs.resource_id == f"sftp:{sftp_server.host}:/incoming/reports/report.csv"
        )
        assert obs.payload["remote_path"] == "/incoming/reports/report.csv"
        assert obs.payload["size"] == len("version 1\n")

        report.write_text("version 2 with more bytes\n")
        report.touch()

        agen = s.observe().__aiter__()
        obs = await agen.__anext__()
        assert obs.event_type == "file.changed"
        assert obs.payload["remote_path"] == "/incoming/reports/report.csv"
    finally:
        await s.teardown()


@pytest.mark.asyncio
async def test_sftp_state_skips_files_before_hwm(sftp_server, tmp_path: Path) -> None:
    remote_dir = sftp_server.root / "incoming" / "reports"
    remote_dir.mkdir(parents=True)
    old = remote_dir / "old.csv"
    old.write_text("old\n")

    state_file = tmp_path / "sftp-state.json"
    hwm = int(old.stat().st_mtime)
    state_file.write_text(json.dumps({"state": hwm}))

    cfg_kwargs = dict(
        name="s",
        hostname=sftp_server.host,
        port=sftp_server.port,
        username=sftp_server.username,
        password=sftp_server.password,
        remote_directories=["/incoming/reports"],
        poll_interval_seconds=0,
        state_file=str(state_file),
    )

    s = SFTPSensor(SFTPSensor.Config(**cfg_kwargs))
    await s.setup()
    try:
        # Old file mtime <= HWM, must not refire as appeared.
        obs_list = [obs async for obs in s.observe()]
        assert obs_list == []

        # A newer file should fire appeared.
        time.sleep(2.1)
        fresh = remote_dir / "fresh.csv"
        fresh.write_text("fresh\n")

        obs_list = [obs async for obs in s.observe()]
        events = [o.event_type for o in obs_list]
        assert "file.appeared" in events
        appeared = next(o for o in obs_list if o.event_type == "file.appeared")
        assert appeared.payload["remote_path"] == "/incoming/reports/fresh.csv"
    finally:
        await s.teardown()

    persisted = json.loads(state_file.read_text())
    assert persisted["state"] >= int(fresh.stat().st_mtime)
