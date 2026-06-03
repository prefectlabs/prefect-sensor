"""Tests for the SFTP sensor."""

from __future__ import annotations

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
