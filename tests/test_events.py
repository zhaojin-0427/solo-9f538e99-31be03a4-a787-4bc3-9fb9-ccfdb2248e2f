"""结算变更流：发布/失效事件、游标分页、原子性与并发不变量。"""

from __future__ import annotations

import threading

from fastapi.testclient import TestClient

from conftest import post_readings, reading, register

from app.database import connect
from app.main import create_app


def seed(client, values, revision: int = 1, meter_id: str = "M-001"):
    register(client, meter_id)
    readings = [
        {
            "hour": f"2026-11-16T{hour:02d}:00:00Z",
            "value": value,
            "revision": revision,
        }
        for hour, value in enumerate(values)
    ]
    response = post_readings(client, readings, meter_id=meter_id)
    assert response.status_code == 200, response.text


def publish(client, hours=2, start_hour=0, meter_id: str = "M-001"):
    return client.post(
        f"/meters/{meter_id}/settlements",
        json={
            "start_hour": f"2026-11-16T{start_hour:02d}:00:00Z",
            "hours": hours,
        },
    )


def all_events(client, page_size: int = 1000) -> list[dict]:
    """按游标翻页拉取全部事件；校验分页本身不重不漏。"""
    events: list[dict] = []
    after = 0
    while True:
        page = client.get(
            "/settlement-events", params={"after": after, "limit": page_size}
        ).json()
        events.extend(page["events"])
        if page["events"]:
            assert page["next_cursor"] == page["events"][-1]["cursor"]
        else:
            assert page["next_cursor"] == after
        if not page["has_more"]:
            return events
        after = page["next_cursor"]


def by_type(events, event_type):
    return [e for e in events if e["event_type"] == event_type]


def test_publish_emits_published_event(client):
    seed(client, [100, 110, 130])
    settlement = publish(client, 2).json()

    events = all_events(client)
    assert len(events) == 1
    event = events[0]
    assert event["event_type"] == "published"
    assert event["settlement_no"] == settlement["settlement_no"]
    assert event["meter_id"] == "M-001"
    assert event["start_hour"] == "2026-11-16T00:00:00Z"
    assert event["end_hour"] == "2026-11-16T02:00:00Z"
    assert event["hours"] == 2
    assert event["cursor"] >= 1
    assert event["occurred_at"] == settlement["published_at"]


def test_correction_invalidates_settlement_and_republish_emits_new_events(client):
    seed(client, [100, 110, 130])
    first = publish(client, 2).json()

    # 更正 01 点（两个区间的公共边界）→ 旧结算失效
    assert post_readings(client, [reading(1, 120, 2)]).status_code == 200

    events = all_events(client)
    assert [e["event_type"] for e in events] == ["published", "invalidated"]
    invalidated = events[1]
    assert invalidated["settlement_no"] == first["settlement_no"]
    assert invalidated["start_hour"] == first["start_hour"]
    assert invalidated["end_hour"] == first["end_hour"]
    assert invalidated["hours"] == first["hours"]
    assert invalidated["cursor"] > events[0]["cursor"]

    # 原结算号仍可查冻结明细
    fetched = client.get(f"/settlements/{first['settlement_no']}").json()
    assert fetched == first

    # 重新发布得到新结算及新事件
    second = publish(client, 2).json()
    assert second["settlement_no"] != first["settlement_no"]
    events = all_events(client)
    assert [e["event_type"] for e in events] == [
        "published",
        "invalidated",
        "published",
    ]
    assert events[2]["settlement_no"] == second["settlement_no"]


def test_same_value_higher_revision_still_invalidates(client):
    seed(client, [100, 110, 130])
    publish(client, 2)

    # 同值高修订：旧依据同样失效
    response = post_readings(client, [reading(1, 110, 2)])
    assert response.status_code == 200
    assert response.json()["accepted"] == 1

    events = all_events(client)
    assert [e["event_type"] for e in events] == ["published", "invalidated"]


def test_same_revision_same_value_retransmit_emits_nothing(client):
    seed(client, [100, 110, 130])
    publish(client, 2)

    response = post_readings(client, [reading(1, 110, 1)])
    assert response.status_code == 200
    assert response.json()["unchanged"] == 1

    assert [e["event_type"] for e in all_events(client)] == ["published"]


def test_multi_boundary_correction_in_one_batch_invalidates_once(client):
    seed(client, [100, 110, 130])
    settlement = publish(client, 2).json()

    # 同批更正该结算的两个边界（00 点和 01 点）
    response = post_readings(client, [reading(0, 100, 2), reading(1, 115, 2)])
    assert response.status_code == 200
    assert response.json()["accepted"] == 2

    invalidated = by_type(all_events(client), "invalidated")
    assert len(invalidated) == 1
    assert invalidated[0]["settlement_no"] == settlement["settlement_no"]


def test_unaffected_settlements_stay_valid(client):
    seed(client, [100, 110, 130], meter_id="M-001")
    seed(client, [100, 110, 130], meter_id="M-002")
    own = publish(client, 2, meter_id="M-001").json()
    other_meter = publish(client, 2, meter_id="M-002").json()
    other_range = publish(client, 1, start_hour=1, meter_id="M-001").json()

    # 只更正 M-001 的 00 点：仅 [00,01) 边界引用它的结算失效
    assert post_readings(client, [reading(0, 105, 2)]).status_code == 200

    invalidated = by_type(all_events(client), "invalidated")
    assert {e["settlement_no"] for e in invalidated} == {own["settlement_no"]}
    assert other_meter["settlement_no"] not in {e["settlement_no"] for e in invalidated}
    assert other_range["settlement_no"] not in {e["settlement_no"] for e in invalidated}


def test_invalidated_settlement_is_not_invalidated_twice(client):
    seed(client, [100, 110, 130])
    settlement = publish(client, 2).json()

    # 第一次更正 01 点 → 失效
    assert post_readings(client, [reading(1, 120, 2)]).status_code == 200
    # 第二次更正同一结算的另一边界 02 点 → 不再重复失效
    assert post_readings(client, [reading(2, 130, 2)]).status_code == 200

    invalidated = by_type(all_events(client), "invalidated")
    assert [e["settlement_no"] for e in invalidated] == [settlement["settlement_no"]]


def test_settlement_published_after_correction_uses_new_revision(client):
    seed(client, [100, 110, 130])
    old = publish(client, 2).json()
    assert post_readings(client, [reading(1, 120, 2)]).status_code == 200

    # 更正后发布的结算引用新修订，不受那次更正影响
    new = publish(client, 2).json()
    invalidated = by_type(all_events(client), "invalidated")
    assert [e["settlement_no"] for e in invalidated] == [old["settlement_no"]]

    # 下一次更正使新结算失效
    assert post_readings(client, [reading(1, 125, 3)]).status_code == 200
    invalidated = by_type(all_events(client), "invalidated")
    assert {e["settlement_no"] for e in invalidated} == {
        old["settlement_no"],
        new["settlement_no"],
    }


def test_correction_of_unreferenced_hour_emits_no_invalidation(client):
    seed(client, [100, 110, 130, 150])
    publish(client, 1)  # 只覆盖 [00, 01)

    # 03 点没有任何结算引用
    assert post_readings(client, [reading(3, 155, 2)]).status_code == 200
    assert [e["event_type"] for e in all_events(client)] == ["published"]


def test_failed_writes_emit_no_events(client):
    seed(client, [100, 110, 130])
    publish(client, 2)
    assert len(all_events(client)) == 1

    # 发布失败（缺读数时段）不产生事件
    rejected = client.post(
        "/meters/M-001/settlements",
        json={"start_hour": "2026-11-16T04:00:00Z", "hours": 2},
    )
    assert rejected.status_code == 409

    # 录入冲突整批回滚：更高修订那一条也不会留下失效事件
    conflict = post_readings(
        client,
        [reading(1, 999, 1), reading(2, 135, 2)],  # 旧版冲突 + 本可生效的更正
    )
    assert conflict.status_code == 409

    # 同版异值冲突也不产生事件
    conflict2 = post_readings(client, [reading(1, 111, 1)])
    assert conflict2.status_code == 409

    events = all_events(client)
    assert [e["event_type"] for e in events] == ["published"]
    # 冲突批次整体回滚，02 点没有被写入
    current = client.get("/meters/M-001/readings").json()["readings"]
    assert [(r["hour"], r["revision"]) for r in current] == [
        ("2026-11-16T00:00:00Z", 1),
        ("2026-11-16T01:00:00Z", 1),
        ("2026-11-16T02:00:00Z", 1),
    ]


def test_cursor_pagination_is_complete_ordered_and_gapless_to_client(client):
    seed(client, [i * 10 for i in range(5)])
    for _ in range(3):
        publish(client, 2)
    post_readings(client, [reading(1, 15, 2)])  # 3 份结算全部失效

    expected = all_events(client)
    assert len(expected) == 6  # 3 published + 3 invalidated
    cursors = [e["cursor"] for e in expected]
    assert cursors == sorted(cursors)
    assert len(set(cursors)) == len(cursors)

    # 小页翻页结果与一次性读取完全一致
    for page_size in (1, 2, 5):
        assert all_events(client, page_size=page_size) == expected

    # 从中间游标继续：只拿到之后的事件
    mid = expected[2]["cursor"]
    page = client.get(
        "/settlement-events", params={"after": mid, "limit": 100}
    ).json()
    assert [e["cursor"] for e in page["events"]] == cursors[3:]
    assert page["has_more"] is False
    assert page["next_cursor"] == cursors[-1]

    # 游标超过最大事件：空页且游标不变
    empty = client.get(
        "/settlement-events", params={"after": cursors[-1], "limit": 10}
    ).json()
    assert empty == {
        "events": [],
        "count": 0,
        "next_cursor": cursors[-1],
        "has_more": False,
    }


def test_event_query_params_validated(client):
    register(client)
    assert client.get("/settlement-events", params={"after": -1}).status_code == 422
    assert client.get("/settlement-events", params={"limit": 0}).status_code == 422
    assert client.get("/settlement-events", params={"limit": 1001}).status_code == 422
    ok = client.get("/settlement-events")
    assert ok.status_code == 200
    assert ok.json() == {"events": [], "count": 0, "next_cursor": 0, "has_more": False}


def test_stream_covers_only_settlements_published_after_enablement(client, db_path):
    """功能启用前已存在的结算（无 published 事件、无引用记录）不进变更流。"""
    seed(client, [100, 110, 130])
    # 模拟启用前遗留的结算：直接写库，不产生事件与引用记录
    conn = connect(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "INSERT INTO settlements "
            "(settlement_no, meter_id, start_hour, end_hour, hours, total_usage, published_at) "
            "VALUES ('STL-LEGACY', 'M-001', '2026-11-16T00:00:00Z', "
            "        '2026-11-16T02:00:00Z', 2, 30, '2026-11-16T00:00:00.000Z')"
        )
        conn.execute(
            "INSERT INTO settlement_intervals "
            "(settlement_no, seq, hour_start, hour_end, usage, "
            " start_value, start_revision, end_value, end_revision) "
            "VALUES ('STL-LEGACY', 1, '2026-11-16T00:00:00Z', '2026-11-16T01:00:00Z', "
            "        10, 100, 1, 110, 1)"
        )
        conn.execute("COMMIT")
    finally:
        conn.close()

    new = publish(client, 2).json()
    assert post_readings(client, [reading(1, 120, 2)]).status_code == 200

    events = all_events(client)
    assert {e["settlement_no"] for e in events} == {new["settlement_no"]}
    assert [e["event_type"] for e in events] == ["published", "invalidated"]
    # 遗留结算本身仍可查
    assert client.get("/settlements/STL-LEGACY").status_code == 200


def test_events_survive_restart_and_cursor_continues(db_path):
    with TestClient(create_app(db_path)) as first:
        seed(first, [100, 110, 130])
        settlement = publish(first, 2).json()
        assert post_readings(first, [reading(1, 120, 2)]).status_code == 200
        before = all_events(first)
        assert [e["event_type"] for e in before] == ["published", "invalidated"]

    with TestClient(create_app(db_path)) as second:
        # 重启后事件不丢、游标不回退
        assert all_events(second) == before
        republished = publish(second, 2).json()
        assert republished["settlement_no"] != settlement["settlement_no"]
        after = all_events(second)
        assert after[:2] == before
        assert after[2]["event_type"] == "published"
        assert after[2]["cursor"] > before[-1]["cursor"]


def run_threads(targets):
    errors: list[BaseException] = []

    def wrapper(fn):
        try:
            fn()
        except BaseException as exc:  # noqa: BLE001 - 收集所有线程异常
            errors.append(exc)

    threads = [threading.Thread(target=wrapper, args=(fn,)) for fn in targets]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)
    assert all(not t.is_alive() for t in threads)
    if errors:
        raise errors[0]


def test_concurrent_publish_and_correct_stream_invariants(db_path, monkeypatch):
    """并发录入与发布：变更流不漏项、不乱序、不重复失效。"""
    monkeypatch.setenv("ENERGY_DB_PATH", db_path)
    app = create_app(db_path)
    with TestClient(app) as client:
        seed(client, [100, 110, 130, 150])

        def publisher():
            for _ in range(8):
                response = client.post(
                    "/meters/M-001/settlements",
                    json={"start_hour": "2026-11-16T00:00:00Z", "hours": 3},
                )
                assert response.status_code == 201, response.text

        def corrector(revision: int):
            def run():
                # 各线程用不同修订号更正同一边界；部分会因后到而 409，属正常裁决
                response = post_readings(
                    client, [reading(1, 110 + revision, revision)]
                )
                assert response.status_code in (200, 409), response.text

            return run

        targets = [publisher, publisher] + [corrector(rev) for rev in range(2, 8)]
        run_threads(targets)

        events = all_events(client, page_size=3)  # 小页翻页同样不重不漏
        cursors = [e["cursor"] for e in events]
        assert cursors == sorted(cursors) and len(set(cursors)) == len(cursors)

        published = by_type(events, "published")
        invalidated = by_type(events, "invalidated")

        # 每份成功发布的结算恰好一条 published，与结算表一一对应（不漏项）
        listing = client.get("/settlements", params={"limit": 1000}).json()
        assert {e["settlement_no"] for e in published} == {
            item["settlement_no"] for item in listing["items"]
        }
        assert len(published) == listing["count"] == 16  # 2 个发布线程 × 各 8 次

        # 每份结算至多一条 invalidated，且失效必在发布之后
        published_cursor = {e["settlement_no"]: e["cursor"] for e in published}
        seen_invalidated: set[str] = set()
        for event in invalidated:
            no = event["settlement_no"]
            assert no not in seen_invalidated, "同一结算不得重复失效"
            seen_invalidated.add(no)
            assert event["cursor"] > published_cursor[no]
