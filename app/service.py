"""业务规则：读数版本裁决、逐小时结算生成与查询。"""

from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone

from pydantic import BaseModel

from .database import read_tx, write_tx
from .models import (
    MAX_HOURLY_USAGE,
    ROLLOVER,
    ConflictItem,
    IngestItemResult,
    IngestResponse,
    MeterView,
    PublishProblem,
    ReadingInput,
    ReadingSnapshot,
    ReadingView,
    SettlementDetail,
    SettlementEvent,
    SettlementEventPage,
    SettlementInterval,
    SettlementSummary,
    normalize_utc_hour,
)


class ApiError(Exception):
    def __init__(self, status_code: int, code: str, message: str, **extra: object):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message
        self.extra = extra

    def detail(self) -> dict:
        return {"code": self.code, "message": self.message, **self._jsonable(self.extra)}

    @staticmethod
    def _jsonable(value: object) -> object:
        if isinstance(value, BaseModel):
            return value.model_dump(mode="json", exclude_none=True)
        if isinstance(value, dict):
            return {k: ApiError._jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [ApiError._jsonable(v) for v in value]
        return value


def utc_hour_text(value: datetime) -> str:
    return normalize_utc_hour(value).strftime("%Y-%m-%dT%H:00:00Z")


def parse_hour_text(value: str) -> datetime:
    return datetime.strptime(value, "%Y-%m-%dT%H:00:00Z").replace(tzinfo=timezone.utc)


def utc_now_text() -> str:
    now = datetime.now(timezone.utc)
    return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _meter_not_found(meter_id: str) -> ApiError:
    return ApiError(404, "meter_not_found", f"电表 {meter_id!r} 不存在", meter_id=meter_id)


def _record_invalidations(
    conn: sqlite3.Connection,
    meter_id: str,
    corrections: list[tuple[str, int]],
    occurred_at: str,
) -> None:
    """为读数更正登记“失效”事件，与业务写入同事务成败。

    corrections 是本次批次中接受了更高修订的 (hour, 旧 revision) 列表。
    引用该时刻旧修订、且仍在变更流中生效（已发布、未失效）的每个结算
    记一次 invalidated；同一批次更正多个边界时同一结算只记一次。
    """
    affected: dict[str, sqlite3.Row] = {}
    for hour_text, old_revision in corrections:
        rows = conn.execute(
            """
            SELECT s.settlement_no, s.meter_id, s.start_hour, s.end_hour, s.hours
            FROM settlement_reading_refs r
            JOIN settlements s ON s.settlement_no = r.settlement_no
            WHERE r.meter_id = ? AND r.hour = ? AND r.revision = ?
              -- 变更流仅覆盖启用后发布（已有 published 事件）的结算
              AND EXISTS (
                  SELECT 1 FROM settlement_events e
                  WHERE e.settlement_no = r.settlement_no
                    AND e.event_type = 'published'
              )
              -- 已撤下的结算不重复失效
              AND NOT EXISTS (
                  SELECT 1 FROM settlement_events e
                  WHERE e.settlement_no = r.settlement_no
                    AND e.event_type = 'invalidated'
              )
            """,
            (meter_id, hour_text, old_revision),
        ).fetchall()
        for row in rows:
            affected.setdefault(row["settlement_no"], row)

    # 按结算号排序，保证同一批次内事件顺序确定
    for settlement_no in sorted(affected):
        row = affected[settlement_no]
        conn.execute(
            "INSERT INTO settlement_events "
            "(event_type, settlement_no, meter_id, start_hour, end_hour, hours, occurred_at) "
            "VALUES ('invalidated', ?, ?, ?, ?, ?, ?)",
            (
                settlement_no,
                row["meter_id"],
                row["start_hour"],
                row["end_hour"],
                row["hours"],
                occurred_at,
            ),
        )


def _meter_view(row: sqlite3.Row) -> MeterView:
    return MeterView(
        meter_id=row["meter_id"],
        name=row["name"],
        created_at=row["created_at"],
        hour_count=row["hour_count"],
        reading_versions=row["reading_versions"],
        latest_hour=row["latest_hour"],
    )


_METER_VIEW_SQL = """
SELECT m.meter_id, m.name, m.created_at,
       COUNT(DISTINCT r.hour) AS hour_count,
       COUNT(r.hour)         AS reading_versions,
       MAX(r.hour)           AS latest_hour
FROM meters m
LEFT JOIN readings r ON r.meter_id = m.meter_id
"""


def _fetch_meter_view(conn: sqlite3.Connection, meter_id: str) -> MeterView | None:
    row = conn.execute(
        _METER_VIEW_SQL + " WHERE m.meter_id = ? GROUP BY m.meter_id",
        (meter_id,),
    ).fetchone()
    return _meter_view(row) if row else None


def create_meter(
    conn: sqlite3.Connection, meter_id: str, name: str | None
) -> tuple[bool, MeterView]:
    now = utc_now_text()
    with write_tx(conn):
        cursor = conn.execute(
            "INSERT INTO meters (meter_id, name, created_at) VALUES (?, ?, ?) "
            "ON CONFLICT(meter_id) DO NOTHING",
            (meter_id, name, now),
        )
        created = cursor.rowcount == 1
        view = _fetch_meter_view(conn, meter_id)
    assert view is not None
    return created, view


def get_meter(conn: sqlite3.Connection, meter_id: str) -> MeterView:
    with read_tx(conn):
        view = _fetch_meter_view(conn, meter_id)
    if view is None:
        raise _meter_not_found(meter_id)
    return view


def list_meters(conn: sqlite3.Connection) -> list[MeterView]:
    with read_tx(conn):
        rows = conn.execute(
            _METER_VIEW_SQL + " GROUP BY m.meter_id ORDER BY m.meter_id"
        ).fetchall()
    return [_meter_view(row) for row in rows]


def ingest_readings(
    conn: sqlite3.Connection, meter_id: str, readings: list[ReadingInput]
) -> IngestResponse:
    """整批原子写入：任一旧版/同版异值/批内同时刻异内容都会使整批回滚。

    批内完全相同 (hour, revision, value) 的重复条目按一条幂等条目处理。
    """
    grouped: dict[str, list[ReadingInput]] = {}
    for item in readings:
        grouped.setdefault(utc_hour_text(item.hour), []).append(item)

    conflicting_hours = {
        hour_text
        for hour_text, items in grouped.items()
        if len({(i.revision, i.value) for i in items}) > 1
    }

    with write_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        conflicts: list[ConflictItem] = []
        current_by_hour: dict[str, sqlite3.Row] = {}
        for hour_text, items in grouped.items():
            if hour_text in conflicting_hours:
                continue
            item = items[0]
            current = conn.execute(
                "SELECT revision, value FROM readings "
                "WHERE meter_id = ? AND hour = ? "
                "ORDER BY revision DESC LIMIT 1",
                (meter_id, hour_text),
            ).fetchone()
            if current is None:
                continue
            current_by_hour[hour_text] = current
            if item.revision < current["revision"]:
                conflicts.append(
                    ConflictItem(
                        hour=hour_text,
                        reason="stale_revision",
                        provided_revision=item.revision,
                        provided_value=item.value,
                        current_revision=current["revision"],
                        current_value=current["value"],
                    )
                )
            elif item.revision == current["revision"] and item.value != current["value"]:
                conflicts.append(
                    ConflictItem(
                        hour=hour_text,
                        reason="same_revision_different_value",
                        provided_revision=item.revision,
                        provided_value=item.value,
                        current_revision=current["revision"],
                        current_value=current["value"],
                    )
                )

        for hour_text in conflicting_hours:
            first = grouped[hour_text][0]
            conflicts.append(
                ConflictItem(
                    hour=hour_text,
                    reason="duplicate_hour_in_batch",
                    provided_revision=first.revision,
                    provided_value=first.value,
                )
            )

        if conflicts:
            conflicts.sort(key=lambda c: (c.hour, c.reason))
            raise ApiError(
                409,
                "reading_conflict",
                "读数批次与已提交版本冲突，整批未写入",
                atomic=True,
                conflicts=conflicts,
            )

        received_at = utc_now_text()
        results: list[IngestItemResult] = []
        corrections: list[tuple[str, int]] = []
        accepted = 0
        unchanged = 0
        for hour_text, items in grouped.items():
            item = items[0]
            current = current_by_hour.get(hour_text)
            if current is None:
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (meter_id, hour_text, item.revision, item.value, received_at),
                )
                status = "accepted"
                accepted += 1
            elif item.revision == current["revision"]:
                # 同版同值重传：幂等，不新增版本，也不产生变更流事件。
                status = "unchanged"
                unchanged += 1
            else:
                # 更高修订：追加新版本，旧版本保留但不再是当前值；
                # 引用旧修订的结算随之失效（同值高修订同样使旧依据失效）。
                conn.execute(
                    "INSERT INTO readings (meter_id, hour, revision, value, received_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (meter_id, hour_text, item.revision, item.value, received_at),
                )
                corrections.append((hour_text, current["revision"]))
                status = "accepted"
                accepted += 1
            results.append(
                IngestItemResult(
                    hour=hour_text,
                    value=item.value,
                    revision=item.revision,
                    status=status,
                )
            )

        if corrections:
            _record_invalidations(conn, meter_id, corrections, received_at)

    results.sort(key=lambda r: r.hour)
    return IngestResponse(
        meter_id=meter_id,
        accepted=accepted,
        unchanged=unchanged,
        readings=results,
    )


def _current_readings(
    conn: sqlite3.Connection,
    meter_id: str,
    start_hour: str,
    end_hour: str,
) -> dict[str, sqlite3.Row]:
    rows = conn.execute(
        """
        SELECT r.hour, r.value, r.revision
        FROM readings r
        JOIN (
            SELECT hour, MAX(revision) AS revision
            FROM readings
            WHERE meter_id = ? AND hour BETWEEN ? AND ?
            GROUP BY hour
        ) latest ON latest.hour = r.hour AND latest.revision = r.revision
        WHERE r.meter_id = ?
        """,
        (meter_id, start_hour, end_hour, meter_id),
    ).fetchall()
    return {row["hour"]: row for row in rows}


def _missing_problems(
    required: list[datetime], present: dict[str, sqlite3.Row]
) -> list[PublishProblem]:
    """把连续缺失的读数时刻合并成半开区间。"""
    problems: list[PublishProblem] = []
    run: list[datetime] = []
    for hour in required + [None]:  # 哨兵，冲刷最后一段
        if hour is not None and utc_hour_text(hour) not in present:
            run.append(hour)
            continue
        if run:
            first, last = run[0], run[-1] + timedelta(hours=1)
            problems.append(
                PublishProblem(
                    code="missing_reading",
                    start_hour=utc_hour_text(first),
                    end_hour=utc_hour_text(last),
                    message=(
                        f"缺少 {utc_hour_text(first)} 至 {utc_hour_text(last)} "
                        "区间内的小时读数"
                    ),
                    missing_readings=[utc_hour_text(h) for h in run],
                )
            )
            run = []
    return problems


def publish_settlement(
    conn: sqlite3.Connection,
    meter_id: str,
    start: datetime,
    hours: int,
) -> SettlementDetail:
    start_dt = normalize_utc_hour(start)
    end_dt = start_dt + timedelta(hours=hours)
    start_text = utc_hour_text(start_dt)
    end_text = utc_hour_text(end_dt)
    required = [start_dt + timedelta(hours=i) for i in range(hours + 1)]

    with write_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        # 发布与录入共用同一个写锁；这里看到的是某一个完整提交状态。
        present = _current_readings(conn, meter_id, start_text, end_text)
        problems = _missing_problems(required, present)

        intervals: list[SettlementInterval] = []
        total = 0
        for seq in range(1, hours + 1):
            hour_start = start_dt + timedelta(hours=seq - 1)
            hour_end = hour_start + timedelta(hours=1)
            start_row = present.get(utc_hour_text(hour_start))
            end_row = present.get(utc_hour_text(hour_end))
            if start_row is None or end_row is None:
                continue

            start_value = start_row["value"]
            end_value = end_row["value"]
            usage = (
                end_value - start_value
                if end_value >= start_value
                else end_value + ROLLOVER - start_value
            )
            total += usage
            if usage > MAX_HOURLY_USAGE:
                problems.append(
                    PublishProblem(
                        code="hourly_usage_exceeds_limit",
                        start_hour=utc_hour_text(hour_start),
                        end_hour=utc_hour_text(hour_end),
                        message=(
                            f"{utc_hour_text(hour_start)} 至 {utc_hour_text(hour_end)} "
                            f"用量 {usage} 超过单小时上限 {MAX_HOURLY_USAGE}"
                        ),
                        usage=usage,
                        limit=MAX_HOURLY_USAGE,
                    )
                )

            intervals.append(
                SettlementInterval(
                    seq=seq,
                    hour_start=utc_hour_text(hour_start),
                    hour_end=utc_hour_text(hour_end),
                    usage=usage,
                    start_reading=ReadingSnapshot(
                        value=start_value, revision=start_row["revision"]
                    ),
                    end_reading=ReadingSnapshot(
                        value=end_value, revision=end_row["revision"]
                    ),
                )
            )

        if problems:
            problems.sort(key=lambda p: (p.start_hour, p.code))
            raise ApiError(
                409,
                "publish_rejected",
                "结算未发布：存在缺失读数或超限用量",
                atomic=True,
                meter_id=meter_id,
                start_hour=start_text,
                end_hour=end_text,
                hours=hours,
                problems=problems,
            )

        published_at = utc_now_text()
        settlement_no = (
            "STL-" + start_dt.strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex
        )
        conn.execute(
            "INSERT INTO settlements "
            "(settlement_no, meter_id, start_hour, end_hour, hours, total_usage, published_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                settlement_no,
                meter_id,
                start_text,
                end_text,
                hours,
                total,
                published_at,
            ),
        )
        conn.executemany(
            "INSERT INTO settlement_intervals "
            "(settlement_no, seq, hour_start, hour_end, usage, "
            " start_value, start_revision, end_value, end_revision) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    settlement_no,
                    item.seq,
                    item.hour_start,
                    item.hour_end,
                    item.usage,
                    item.start_reading.value,
                    item.start_reading.revision,
                    item.end_reading.value,
                    item.end_reading.revision,
                )
                for item in intervals
            ],
        )
        # 固化每个边界整点引用的读数版本，供更正时反查受影响结算。
        conn.executemany(
            "INSERT INTO settlement_reading_refs "
            "(settlement_no, meter_id, hour, revision) VALUES (?, ?, ?, ?)",
            [
                (
                    settlement_no,
                    meter_id,
                    boundary_text,
                    present[boundary_text]["revision"],
                )
                for boundary_text in (utc_hour_text(h) for h in required)
            ],
        )
        # 变更流：发布事件，与结算数据同一事务提交。
        conn.execute(
            "INSERT INTO settlement_events "
            "(event_type, settlement_no, meter_id, start_hour, end_hour, hours, occurred_at) "
            "VALUES ('published', ?, ?, ?, ?, ?, ?)",
            (settlement_no, meter_id, start_text, end_text, hours, published_at),
        )

    return SettlementDetail(
        settlement_no=settlement_no,
        meter_id=meter_id,
        start_hour=start_text,
        end_hour=end_text,
        hours=hours,
        total_usage=total,
        published_at=published_at,
        intervals=intervals,
    )


def list_readings(
    conn: sqlite3.Connection,
    meter_id: str,
    start: datetime | None,
    end: datetime | None,
    include_history: bool,
) -> list[ReadingView]:
    if start is not None and end is not None and start > end:
        raise ApiError(422, "invalid_hour_range", "start_hour 不能晚于 end_hour")

    clauses = ["meter_id = ?"]
    params: list[object] = [meter_id]
    if start is not None:
        clauses.append("hour >= ?")
        params.append(utc_hour_text(start))
    if end is not None:
        clauses.append("hour <= ?")
        params.append(utc_hour_text(end))
    where = " AND ".join(clauses)

    with read_tx(conn):
        if conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)

        if include_history:
            rows = conn.execute(
                f"SELECT hour, value, revision, received_at FROM readings "
                f"WHERE {where} ORDER BY hour, revision",
                params,
            ).fetchall()
        else:
            rows = conn.execute(
                f"""
                SELECT r.hour, r.value, r.revision, r.received_at
                FROM readings r
                JOIN (
                    SELECT hour, MAX(revision) AS revision
                    FROM readings WHERE {where} GROUP BY hour
                ) latest ON latest.hour = r.hour AND latest.revision = r.revision
                WHERE r.meter_id = ?
                ORDER BY r.hour
                """,
                (*params, meter_id),
            ).fetchall()

    return [
        ReadingView(
            hour=row["hour"],
            value=row["value"],
            revision=row["revision"],
            received_at=row["received_at"],
        )
        for row in rows
    ]


def get_settlement(conn: sqlite3.Connection, settlement_no: str) -> SettlementDetail:
    with read_tx(conn):
        row = conn.execute(
            "SELECT * FROM settlements WHERE settlement_no = ?", (settlement_no,)
        ).fetchone()
        if row is None:
            raise ApiError(
                404,
                "settlement_not_found",
                f"结算号 {settlement_no!r} 不存在",
                settlement_no=settlement_no,
            )
        interval_rows = conn.execute(
            "SELECT * FROM settlement_intervals "
            "WHERE settlement_no = ? ORDER BY seq",
            (settlement_no,),
        ).fetchall()

    return SettlementDetail(
        settlement_no=row["settlement_no"],
        meter_id=row["meter_id"],
        start_hour=row["start_hour"],
        end_hour=row["end_hour"],
        hours=row["hours"],
        total_usage=row["total_usage"],
        published_at=row["published_at"],
        intervals=[
            SettlementInterval(
                seq=item["seq"],
                hour_start=item["hour_start"],
                hour_end=item["hour_end"],
                usage=item["usage"],
                start_reading=ReadingSnapshot(
                    value=item["start_value"], revision=item["start_revision"]
                ),
                end_reading=ReadingSnapshot(
                    value=item["end_value"], revision=item["end_revision"]
                ),
            )
            for item in interval_rows
        ],
    )


def list_settlements(
    conn: sqlite3.Connection, meter_id: str | None, limit: int
) -> list[SettlementSummary]:
    with read_tx(conn):
        if meter_id is not None and conn.execute(
            "SELECT 1 FROM meters WHERE meter_id = ?", (meter_id,)
        ).fetchone() is None:
            raise _meter_not_found(meter_id)
        if meter_id is None:
            rows = conn.execute(
                "SELECT * FROM settlements "
                "ORDER BY published_at DESC, settlement_no DESC LIMIT ?",
                (limit,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM settlements WHERE meter_id = ? "
                "ORDER BY published_at DESC, settlement_no DESC LIMIT ?",
                (meter_id, limit),
            ).fetchall()
    return [
        SettlementSummary(
            settlement_no=row["settlement_no"],
            meter_id=row["meter_id"],
            start_hour=row["start_hour"],
            end_hour=row["end_hour"],
            hours=row["hours"],
            total_usage=row["total_usage"],
            published_at=row["published_at"],
        )
        for row in rows
    ]


def list_settlement_events(
    conn: sqlite3.Connection, after: int, limit: int
) -> SettlementEventPage:
    """按游标增量读取变更流。

    event_id 在串行化的写事务内分配，顺序即提交顺序；`WHERE event_id > after
    ORDER BY event_id` 的键集分页因此不重不漏。多取一条用于判断 has_more。
    """
    with read_tx(conn):
        rows = conn.execute(
            "SELECT event_id, event_type, settlement_no, meter_id, "
            "       start_hour, end_hour, hours, occurred_at "
            "FROM settlement_events WHERE event_id > ? "
            "ORDER BY event_id LIMIT ?",
            (after, limit + 1),
        ).fetchall()

    has_more = len(rows) > limit
    rows = rows[:limit]
    events = [
        SettlementEvent(
            cursor=row["event_id"],
            event_type=row["event_type"],
            settlement_no=row["settlement_no"],
            meter_id=row["meter_id"],
            start_hour=row["start_hour"],
            end_hour=row["end_hour"],
            hours=row["hours"],
            occurred_at=row["occurred_at"],
        )
        for row in rows
    ]
    return SettlementEventPage(
        events=events,
        count=len(events),
        next_cursor=events[-1].cursor if events else after,
        has_more=has_more,
    )
