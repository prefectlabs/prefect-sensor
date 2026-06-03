"""Tests for the SQL sensor."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from prefect_sensor.sensors.sql import SQLSensor


async def _collect(sensor: SQLSensor) -> list:
    return [obs async for obs in sensor.observe()]


async def _seed_orders(db_url: str) -> None:
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE orders ("
                "id INTEGER PRIMARY KEY, "
                "customer TEXT, "
                "updated_at TIMESTAMP)"
            )
        )
    await engine.dispose()


async def _insert_orders(db_url: str, rows: list[tuple]) -> None:
    engine = create_async_engine(db_url)
    async with engine.begin() as conn:
        for row in rows:
            await conn.execute(
                text(
                    "INSERT INTO orders (id, customer, updated_at) "
                    "VALUES (:id, :customer, :updated_at)"
                ),
                {"id": row[0], "customer": row[1], "updated_at": row[2]},
            )
    await engine.dispose()


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"


@pytest.mark.asyncio
async def test_sql_sensor_integer_tracking(tmp_path: Path) -> None:
    db_url = _sqlite_url(tmp_path)
    await _seed_orders(db_url)
    await _insert_orders(
        db_url,
        [
            (i, f"cust-{i}", datetime(2024, 1, i, tzinfo=timezone.utc))
            for i in (1, 2, 3)
        ],
    )

    s = SQLSensor(
        SQLSensor.Config(
            name="q",
            connection_string=db_url,
            query="SELECT id, customer FROM orders WHERE id > :hwm ORDER BY id",
            tracking_column="id",
            emit_existing=True,
            poll_interval_seconds=0,
        )
    )
    await s.setup()
    try:
        obs = await _collect(s)
        assert [o.payload["id"] for o in obs] == [1, 2, 3]
        assert obs[0].event_type == "row.detected"
        assert obs[2].resource_id == "sql:id:3"
        assert s._hwm == 3

        await _insert_orders(
            db_url,
            [
                (4, "cust-4", datetime(2024, 1, 4, tzinfo=timezone.utc)),
                (5, "cust-5", datetime(2024, 1, 5, tzinfo=timezone.utc)),
            ],
        )
        obs = await _collect(s)
        assert [o.payload["id"] for o in obs] == [4, 5]
        assert s._hwm == 5
    finally:
        await s.teardown()


@pytest.mark.asyncio
async def test_sql_sensor_timestamp_tracking(tmp_path: Path) -> None:
    db_url = _sqlite_url(tmp_path)
    await _seed_orders(db_url)
    await _insert_orders(
        db_url,
        [(i, f"cust-{i}", datetime(2024, 1, i)) for i in (1, 2, 3)],
    )

    s = SQLSensor(
        SQLSensor.Config(
            name="q",
            connection_string=db_url,
            query=(
                "SELECT id, customer, updated_at FROM orders "
                "WHERE updated_at > :hwm ORDER BY updated_at"
            ),
            tracking_column="updated_at",
            tracking_type="timestamp",
            start_value="datetime(2024, 1, 2, tzinfo=timezone.utc)",
            poll_interval_seconds=0,
        )
    )
    await s.setup()
    try:
        obs = await _collect(s)
        assert [o.payload["id"] for o in obs] == [3]
        assert isinstance(obs[0].payload["updated_at"], datetime)
        assert obs[0].payload["updated_at"].tzinfo is not None
        assert s._hwm == datetime(2024, 1, 3, tzinfo=timezone.utc)
    finally:
        await s.teardown()


@pytest.mark.asyncio
async def test_sql_sensor_state_persistence(tmp_path: Path) -> None:
    db_url = _sqlite_url(tmp_path)
    state_file = str(tmp_path / "state.json")
    await _seed_orders(db_url)
    await _insert_orders(
        db_url,
        [
            (i, f"cust-{i}", datetime(2024, 1, i, tzinfo=timezone.utc))
            for i in (1, 2, 3)
        ],
    )

    cfg_kwargs = dict(
        name="q",
        connection_string=db_url,
        query="SELECT id, customer FROM orders WHERE id > :hwm ORDER BY id",
        tracking_column="id",
        emit_existing=True,
        state_file=state_file,
        poll_interval_seconds=0,
    )

    s1 = SQLSensor(SQLSensor.Config(**cfg_kwargs))
    await s1.setup()
    try:
        obs = await _collect(s1)
        assert [o.payload["id"] for o in obs] == [1, 2, 3]
    finally:
        await s1.teardown()

    s2 = SQLSensor(SQLSensor.Config(**cfg_kwargs))
    await s2.setup()
    try:
        assert s2._hwm == 3
        obs = await _collect(s2)
        assert obs == []

        await _insert_orders(
            db_url,
            [(4, "cust-4", datetime(2024, 1, 4, tzinfo=timezone.utc))],
        )
        obs = await _collect(s2)
        assert [o.payload["id"] for o in obs] == [4]
    finally:
        await s2.teardown()


@pytest.mark.asyncio
async def test_sql_sensor_start_value_skips_existing(tmp_path: Path) -> None:
    db_url = _sqlite_url(tmp_path)
    await _seed_orders(db_url)
    await _insert_orders(
        db_url,
        [
            (i, f"cust-{i}", datetime(2024, 1, i, tzinfo=timezone.utc))
            for i in (1, 2, 3)
        ],
    )

    s = SQLSensor(
        SQLSensor.Config(
            name="q",
            connection_string=db_url,
            query="SELECT id, customer FROM orders WHERE id > :hwm ORDER BY id",
            tracking_column="id",
            start_value="2",
            poll_interval_seconds=0,
        )
    )
    await s.setup()
    try:
        obs = await _collect(s)
        assert [o.payload["id"] for o in obs] == [3]
    finally:
        await s.teardown()


def test_sql_sensor_query_missing_hwm_rejected() -> None:
    with pytest.raises(ValueError, match=":hwm"):
        SQLSensor.Config(
            name="q",
            connection_string="sqlite+aiosqlite:///:memory:",
            query="SELECT id FROM orders",
            tracking_column="id",
            emit_existing=True,
        )


def test_sql_sensor_requires_initial_mode() -> None:
    with pytest.raises(ValueError, match="start_value or emit_existing"):
        SQLSensor.Config(
            name="q",
            connection_string="sqlite+aiosqlite:///:memory:",
            query="SELECT id FROM orders WHERE id > :hwm",
            tracking_column="id",
        )


def test_sql_sensor_rejects_both_initial_modes() -> None:
    with pytest.raises(ValueError, match="not both"):
        SQLSensor.Config(
            name="q",
            connection_string="sqlite+aiosqlite:///:memory:",
            query="SELECT id FROM orders WHERE id > :hwm",
            tracking_column="id",
            start_value="0",
            emit_existing=True,
        )
