"""FastAPI 接口测试（需要 fastapi/httpx，随 requirements-dev.txt 安装）：

    pytest tests/test_api.py -v
"""
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

fastapi = pytest.importorskip("fastapi")
from fastapi.testclient import TestClient  # noqa: E402

from app.config import Settings  # noqa: E402
from app.main import create_app  # noqa: E402
from app.service import _decode_event_cursor  # noqa: E402


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "api.db"),
        silence_interval_seconds=60.0,
    )
    with TestClient(create_app(settings)) as c:
        yield c


def test_submit_and_get_segment(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "warning",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["deduplicated"] is False
    seg = body["segment"]
    assert seg["event_count"] == 1
    assert seg["revision"] == 1
    assert seg["max_severity"] == "warning"
    assert seg["started_at"] == seg["ended_at"]

    got = client.get(f"/segments/{seg['id']}")
    assert got.status_code == 200
    assert got.json() == seg


def test_duplicate_submission_keeps_count(client):
    payload = {
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
    }
    first = client.post("/events", json=payload).json()
    second = client.post("/events", json=payload).json()
    assert second["deduplicated"] is True
    assert second["segment"]["event_count"] == first["segment"]["event_count"] == 1


def test_late_out_of_order_event_bumps_revision(client):
    client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:30Z", "severity": "info",
    })
    late = client.post("/events", json={
        "source": "gw-01", "event_id": "e2",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "error",
    }).json()
    seg = late["segment"]
    assert seg["revision"] == 2
    assert seg["max_severity"] == "error"
    # pydantic 不同版本对 UTC 可能输出 Z 或 +00:00，统一归一化比较
    assert seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:00:30+00:00"
    assert seg["event_count"] == 2


def test_list_segments_filter_by_source(client):
    for source, eid in [("a", "e1"), ("b", "e2"), ("a", "e3")]:
        client.post("/events", json={
            "source": source, "event_id": eid,
            "occurred_at": "2026-09-20T08:00:00Z", "severity": "info",
        })
    assert len(client.get("/segments").json()) == 2
    segs = client.get("/segments", params={"source": "a"}).json()
    assert len(segs) == 1
    assert segs[0]["event_count"] == 2


def test_invalid_severity_rejected(client):
    resp = client.post("/events", json={
        "source": "gw-01", "event_id": "e1",
        "occurred_at": "2026-09-20T08:00:00Z", "severity": "fatal-error",
    })
    assert resp.status_code == 422


def test_missing_field_rejected(client):
    resp = client.post("/events", json={"source": "gw-01", "event_id": "e1"})
    assert resp.status_code == 422


def test_get_missing_segment_404(client):
    assert client.get("/segments/9999").status_code == 404


def _post(client, source, event_id, occurred_at, severity="info"):
    resp = client.post("/events", json={
        "source": source, "event_id": event_id,
        "occurred_at": occurred_at, "severity": severity,
    })
    assert resp.status_code == 200
    return resp.json()


def test_segment_changes_empty_initially(client):
    assert client.get("/segment-changes").json() == {"changes": [], "next_after": 0}


def test_segment_changes_upsert_rules(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")  # 创建 → upsert
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 边界+级别 → upsert
    _post(client, "gw-01", "e3", "2026-09-20T08:00:10Z", "debug")    # 仅计数 → 无变更
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")    # 幂等重放 → 无变更

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "upsert"]
    first, second = page["changes"]
    assert first["change_id"] < second["change_id"]
    assert first["revision"] == 1
    assert second["revision"] == 2
    assert second["segment"]["max_severity"] == "error"
    assert second["segment"]["event_count"] == 2  # 变更时刻的快照（e3 未产生记录）
    assert page["next_after"] == second["change_id"]

    seg = client.get(f"/segments/{second['segment_id']}").json()
    assert seg["event_count"] == 3  # 段最终计数包含 e3


def test_segment_changes_merge_tombstone(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")["segment"]  # 超静默 → 新段
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")                 # 桥接合并

    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == [
        "upsert", "upsert", "merged", "upsert",
    ]
    tomb = page["changes"][2]
    assert tomb["segment_id"] == b["id"]
    assert tomb["merged_into"] == a["id"]
    assert tomb["segment"] is None
    final = page["changes"][3]
    assert final["segment_id"] == a["id"]
    assert final["segment"]["event_count"] == 3


def test_segment_changes_pagination_while_ingesting(client):
    for i in range(3):  # 不同 source → 3 个独立段 → 3 条 upsert
        _post(client, f"gw-{i:02d}", "e1", "2026-09-20T08:00:00Z")

    p1 = client.get("/segment-changes", params={"after": 0, "limit": 2}).json()
    assert len(p1["changes"]) == 2
    assert p1["next_after"] == p1["changes"][-1]["change_id"]

    _post(client, "gw-03", "e1", "2026-09-20T08:00:00Z")  # 分页期间新写入

    p2 = client.get("/segment-changes", params={"after": p1["next_after"], "limit": 100})
    p2 = p2.json()
    ids = [c["change_id"] for c in p1["changes"] + p2["changes"]]
    assert ids == sorted(ids) and len(set(ids)) == 4  # 不漏读、不重复
    assert p2["next_after"] == ids[-1]

    # 空页：next_after 保持不变，可直接继续轮询
    p3 = client.get("/segment-changes", params={"after": p2["next_after"]}).json()
    assert p3 == {"changes": [], "next_after": p2["next_after"]}


def test_segment_changes_param_validation(client):
    assert client.get("/segment-changes", params={"after": -1}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 0}).status_code == 422
    assert client.get("/segment-changes", params={"limit": 1001}).status_code == 422


def test_preview_split_merge_and_unchanged(client):
    # silence=60：08:00:00、08:00:30 同段；08:02:00、08:02:30 另一段
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "critical")
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "info")
    _post(client, "gw-01", "e4", "2026-09-20T08:02:30Z", "debug")

    # 更小间隔 15s：两段各被拆开 → 4 个模拟段，2 个 split
    split = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 15},
    )
    assert split.status_code == 200
    body = split.json()
    assert body["source"] == "gw-01"
    assert body["silence_interval_seconds"] == 15.0
    assert body["event_count"] == 4
    sims = body["simulated_segments"]
    assert [s["temp_id"] for s in sims] == [1, 2, 3, 4]
    assert all(s["event_count"] == 1 for s in sims)
    assert sims[0]["max_severity"] == "warning"
    assert sims[1]["max_severity"] == "critical"
    assert [m["relation"] for m in body["mappings"]] == ["split", "split"]
    assert body["mappings"][0]["simulated_temp_ids"] == [1, 2]
    assert body["mappings"][1]["simulated_temp_ids"] == [3, 4]
    summary = body["summary"]
    assert summary["split_existing_segment_count"] == 2
    assert summary["merged_existing_segment_count"] == 0
    assert summary["unchanged_existing_segment_count"] == 0
    assert summary["changed_existing_segment_count"] == 2
    assert summary["existing_segment_count"] == 2
    assert summary["simulated_segment_count"] == 4

    # 更大间隔 600s：合并为 1 个模拟段，2 个 merged
    merged = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 600},
    ).json()
    assert len(merged["simulated_segments"]) == 1
    merged_seg = merged["simulated_segments"][0]
    assert merged_seg["temp_id"] == 1
    assert merged_seg["event_count"] == 4
    assert merged_seg["max_severity"] == "critical"
    assert merged_seg["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert merged_seg["ended_at"].replace("Z", "+00:00") == "2026-09-20T08:02:30+00:00"
    assert [m["relation"] for m in merged["mappings"]] == ["merged", "merged"]
    assert all(m["simulated_temp_ids"] == [1] for m in merged["mappings"])
    assert merged["summary"]["merged_existing_segment_count"] == 2
    assert merged["summary"]["changed_existing_segment_count"] == 2

    # 与生效间隔相同（60s）：全部 unchanged
    same = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 60},
    ).json()
    assert len(same["simulated_segments"]) == 2
    assert all(m["relation"] == "unchanged" for m in same["mappings"])
    assert same["summary"]["changed_existing_segment_count"] == 0
    assert same["summary"]["unchanged_existing_segment_count"] == 2


def test_preview_deterministic_and_read_only(client):
    for i, ts in enumerate(
        ["08:00:00", "08:00:30", "08:02:00", "08:02:30", "08:05:00"]
    ):
        _post(client, "gw-01", f"e{i}", f"2026-09-20T{ts}Z")

    params = {"source": "gw-01", "silence_interval_seconds": 45}
    first = client.get("/segments/resegmentation-preview", params=params).json()
    second = client.get("/segments/resegmentation-preview", params=params).json()
    assert first == second  # 重复请求确定一致

    # 预演不改变现有段与变更流
    segs = client.get("/segments", params={"source": "gw-01"}).json()
    changes = client.get("/segment-changes").json()
    client.get("/segments/resegmentation-preview", params=params)
    client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 1},
    )
    assert client.get("/segments", params={"source": "gw-01"}).json() == segs
    assert client.get("/segment-changes").json() == changes


def test_preview_unknown_source_empty(client):
    body = client.get(
        "/segments/resegmentation-preview",
        params={"source": "ghost", "silence_interval_seconds": 60},
    ).json()
    assert body["event_count"] == 0
    assert body["simulated_segments"] == []
    assert body["mappings"] == []
    assert body["summary"]["existing_segment_count"] == 0
    assert body["summary"]["simulated_segment_count"] == 0


def test_preview_param_validation(client):
    # 缺 source
    r = client.get(
        "/segments/resegmentation-preview",
        params={"silence_interval_seconds": 60},
    )
    assert r.status_code == 422
    # 负间隔
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": -1},
    )
    assert r.status_code == 422
    # 非数字间隔
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": "soon"},
    )
    assert r.status_code == 422
    # 零允许（同刻事件才同段）
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 0},
    )
    assert r.status_code == 200


def test_preview_route_not_captured_by_segment_id(client):
    # 静态路径必须优先于 /segments/{segment_id} 匹配，不能返回 422
    r = client.get(
        "/segments/resegmentation-preview",
        params={"source": "gw-01", "silence_interval_seconds": 60},
    )
    assert r.status_code == 200


def test_healthz(client):
    assert client.get("/healthz").json() == {"status": "ok"}


# ---- 误报事件撤回 DELETE /events/{source}/{event_id} ----

def _delete(client, source, event_id):
    return client.delete(f"/events/{source}/{event_id}")


def test_withdraw_unknown_event_returns_404(client):
    resp = _delete(client, "gw-01", "ghost")
    assert resp.status_code == 404
    # 存在该 event_id 但 source 不同：同样 404
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    before = client.get("/segment-changes").json()
    assert _delete(client, "gw-02", "e1").status_code == 404
    # 404 不产生任何变更
    assert client.get("/segment-changes").json() == before


def test_withdraw_sole_event_deletes_segment(client):
    seg = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")["segment"]
    resp = _delete(client, "gw-01", "e1")
    assert resp.status_code == 200
    assert resp.json() == {
        "source": "gw-01",
        "event_id": "e1",
        "result": "deleted_segment",
        "segments": [],
    }
    # 原段已不存在；重复撤回返回 404
    assert client.get(f"/segments/{seg['id']}").status_code == 404
    assert _delete(client, "gw-01", "e1").status_code == 404
    page = client.get("/segment-changes").json()
    assert [c["type"] for c in page["changes"]] == ["upsert", "deleted"]
    tomb = page["changes"][-1]
    assert tomb["segment_id"] == seg["id"]
    assert tomb["merged_into"] is None
    assert tomb["segment"] is None
    # 撤回后可重新提交同一事件（幂等键已释放）
    again = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    assert again["deduplicated"] is False


def test_withdraw_inner_event_shrinks_segment_and_bumps_revision(client):
    first = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "critical")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "info")
    _post(client, "gw-01", "e3", "2026-09-20T08:00:10Z", "debug")  # 段内，rev 不变
    resp = _delete(client, "gw-01", "e3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "shrunk_segment"
    seg = body["segments"][0]
    assert seg["id"] == first["segment"]["id"]  # 沿用原 id
    assert seg["revision"] == 3                  # rev=2 → 3（边界不变也必须递增）
    assert seg["event_count"] == 2
    assert seg["max_severity"] == "critical"
    assert client.get(f"/segments/{seg['id']}").json()["event_count"] == 2


def test_withdraw_recomputes_max_severity(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "critical")
    body = _delete(client, "gw-01", "e2").json()
    assert body["segments"][0]["max_severity"] == "info"


def test_withdraw_bridge_event_splits_segment(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "error")["segment"]
    b = _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "warning")["segment"]
    bridged = _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")["segment"]

    resp = _delete(client, "gw-01", "e3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["result"] == "split_segment"
    kept, new = body["segments"]
    # 最早子段沿用原 id，revision 在桥接合并后的值上递增
    assert kept["id"] == a["id"]
    assert kept["revision"] == bridged["revision"] + 1
    assert kept["event_count"] == 1
    assert kept["max_severity"] == "error"
    assert kept["started_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    # 其余子段新建（不用被合并段 B 的 id），revision 从 1 开始
    assert new["id"] not in (a["id"], b["id"])
    assert new["revision"] == 1
    assert new["event_count"] == 1
    assert new["max_severity"] == "warning"
    assert new["started_at"].replace("Z", "+00:00") == "2026-09-20T08:02:00+00:00"

    segs = client.get("/segments", params={"source": "gw-01"}).json()
    assert len(segs) == 2

    # 变更流：保留段 upsert 在前、新段 upsert 在后，change_id 有序
    page = client.get("/segment-changes").json()
    tail = page["changes"][-2:]
    assert [c["type"] for c in tail] == ["upsert", "upsert"]
    assert tail[0]["segment_id"] == kept["id"]
    assert tail[0]["revision"] == kept["revision"]
    assert tail[1]["segment_id"] == new["id"]
    assert tail[1]["revision"] == 1
    assert tail[0]["change_id"] < tail[1]["change_id"]


def test_withdraw_is_isolated_by_source(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")
    other = _post(client, "gw-02", "e9", "2026-09-20T08:00:05Z", "critical")
    _delete(client, "gw-01", "e3")  # 拆 gw-01
    seg = client.get(f"/segments/{other['segment']['id']}").json()
    assert seg == other["segment"]


def test_withdraw_then_reingest_rebridges(client):
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z")
    _delete(client, "gw-01", "e3")
    again = _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")
    assert again["deduplicated"] is False
    assert again["segment"]["id"] == a["id"]
    assert again["segment"]["event_count"] == 3
    assert len(client.get("/segments", params={"source": "gw-01"}).json()) == 1


def test_withdraw_path_params_are_url_segments(client):
    # 路径段支持 source/event_id 允许的 URL 安全字符
    _post(client, "edge-gateway.01", "evt-0001_x", "2026-09-20T08:00:00Z")
    resp = client.delete("/events/edge-gateway.01/evt-0001_x")
    assert resp.status_code == 200
    assert resp.json()["result"] == "deleted_segment"
    # 缺失路径段不会误匹配到该路由（交由 404/405 语义）
    assert client.delete("/events/edge-gateway.01").status_code in (404, 405)


# ---- 事件审计查询 GET /events ----

def test_get_events_requires_source(client):
    assert client.get("/events").status_code == 422
    assert client.get("/events", params={"source": ""}).status_code == 422


def test_get_events_empty(client):
    assert client.get("/events", params={"source": "gw-01"}).json() == {
        "events": [],
        "next_after": None,
    }


def test_get_events_orders_and_returns_fields_with_segment_id(client):
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "error")
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    seg = _post(client, "gw-01", "b", "2026-09-20T08:01:00Z", "warning")["segment"]
    _post(client, "gw-01", "a", "2026-09-20T08:01:00Z", "debug")  # 同刻，event_id 决胜

    body = client.get("/events", params={"source": "gw-01"}).json()
    assert [e["event_id"] for e in body["events"]] == ["e1", "a", "b", "e3"]
    # 非空页 next_after 指向末行（e3, 08:02:00）游标
    cursor_ts, cursor_eid = _decode_event_cursor(body["next_after"])
    assert cursor_eid == "e3"
    assert cursor_ts == datetime(
        2026, 9, 20, 8, 2, 0, tzinfo=timezone.utc
    ).timestamp()
    e1 = body["events"][0]
    assert set(e1) == {"source", "event_id", "occurred_at", "severity", "segment_id"}
    assert e1["source"] == "gw-01"
    assert e1["event_id"] == "e1"
    assert e1["occurred_at"].replace("Z", "+00:00") == "2026-09-20T08:00:00+00:00"
    assert e1["severity"] == "info"
    assert e1["segment_id"] == seg["id"]
    # 同刻事件次序由 event_id 决定
    assert body["events"][1]["event_id"] == "a"
    assert body["events"][2]["event_id"] == "b"


def test_get_events_isolated_by_source(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z")
    _post(client, "gw-02", "e2", "2026-09-20T08:00:00Z")
    body = client.get("/events", params={"source": "gw-01"}).json()
    assert [e["event_id"] for e in body["events"]] == ["e1"]


def test_get_events_time_range_filter_inclusive(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:01:00Z", "info")
    _post(client, "gw-01", "e3", "2026-09-20T08:02:00Z", "info")
    body = client.get("/events", params={
        "source": "gw-01",
        "start_at": "2026-09-20T08:00:00Z",
        "end_at": "2026-09-20T08:01:00Z",
    }).json()
    assert [e["event_id"] for e in body["events"]] == ["e1", "e2"]  # 闭区间


def test_get_events_naive_time_assumed_utc(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:30Z", "info")
    body = client.get("/events", params={
        "source": "gw-01", "start_at": "2026-09-20T08:00:00",
    }).json()
    assert [e["event_id"] for e in body["events"]] == ["e1"]


def test_get_events_start_after_end_rejected(client):
    resp = client.get("/events", params={
        "source": "gw-01",
        "start_at": "2026-09-20T08:02:00Z",
        "end_at": "2026-09-20T08:00:00Z",
    })
    assert resp.status_code == 422


def test_get_events_severity_multi_filter_normalized(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "warning")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")
    # 重复参数：多值过滤；大小写与别名（fatal→critical）归一化
    body = client.get("/events", params=[
        ("source", "gw-01"),
        ("severity", "WARN"),
        ("severity", "fatal"),
    ]).json()
    assert [e["event_id"] for e in body["events"]] == ["e2", "e3"]


def test_get_events_invalid_severity_rejected(client):
    resp = client.get("/events", params={"source": "gw-01", "severity": "bogus"})
    assert resp.status_code == 422


def test_get_events_pagination_no_miss_no_dup_and_empty_page_echoes_cursor(client):
    for i in range(5):
        _post(client, "gw-01", f"e{i}", f"2026-09-20T08:00:0{i}Z")

    p1 = client.get("/events", params={"source": "gw-01", "limit": 2}).json()
    assert [e["event_id"] for e in p1["events"]] == ["e0", "e1"]
    assert p1["next_after"]

    p2 = client.get("/events", params={
        "source": "gw-01", "after": p1["next_after"], "limit": 2,
    }).json()
    assert [e["event_id"] for e in p2["events"]] == ["e2", "e3"]

    p3 = client.get("/events", params={
        "source": "gw-01", "after": p2["next_after"], "limit": 100,
    }).json()
    assert [e["event_id"] for e in p3["events"]] == ["e4"]

    # 空页：next_after 与入参 after 完全相同，游标不前进
    p4 = client.get("/events", params={
        "source": "gw-01", "after": p3["next_after"],
    }).json()
    assert p4 == {"events": [], "next_after": p3["next_after"]}

    ids = [e["event_id"] for p in (p1, p2, p3) for e in p["events"]]
    assert ids == [f"e{i}" for i in range(5)] and len(set(ids)) == 5


def test_get_events_pagination_with_filters(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "info")
    _post(client, "gw-01", "e2", "2026-09-20T08:00:30Z", "error")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "error")
    params = [
        ("source", "gw-01"),
        ("severity", "error"),
        ("start_at", "2026-09-20T08:00:00Z"),
        ("limit", "1"),
    ]
    p1 = client.get("/events", params=params).json()
    assert [e["event_id"] for e in p1["events"]] == ["e2"]
    p2 = client.get("/events", params=[*params, ("after", p1["next_after"])]).json()
    assert [e["event_id"] for e in p2["events"]] == ["e3"]


def test_get_events_limit_validation(client):
    assert client.get("/events", params={"source": "gw-01", "limit": 0}).status_code == 422
    assert client.get("/events", params={"source": "gw-01", "limit": 1001}).status_code == 422


def test_get_events_invalid_cursor_rejected(client):
    resp = client.get("/events", params={"source": "gw-01", "after": "%%%not-base64"})
    assert resp.status_code == 422


def test_get_events_segment_id_reflects_current_segment(client):
    # 查询返回的是当前 segment_id：桥接合并且撤回拆段后随之变化
    a = _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "error")["segment"]
    _post(client, "gw-01", "e2", "2026-09-20T08:02:00Z", "warning")
    _post(client, "gw-01", "e3", "2026-09-20T08:01:00Z", "critical")  # 桥接
    merged = client.get("/events", params={"source": "gw-01"}).json()
    assert all(e["segment_id"] == a["id"] for e in merged["events"])

    client.delete("/events/gw-01/e3")  # 拆回两段
    split = client.get("/events", params={"source": "gw-01"}).json()
    owners = {e["event_id"]: e["segment_id"] for e in split["events"]}
    assert owners["e1"] == a["id"]
    assert owners["e2"] != a["id"]


def test_get_events_is_read_only(client):
    _post(client, "gw-01", "e1", "2026-09-20T08:00:00Z", "warning")
    segs_before = client.get("/segments").json()
    changes_before = client.get("/segment-changes").json()
    for params in (
        {"source": "gw-01"},
        {"source": "gw-01", "severity": "warning", "limit": 1},
        {"source": "gw-01", "start_at": "2026-09-20T08:00:00Z"},
    ):
        client.get("/events", params=params)
    assert client.get("/segments").json() == segs_before
    assert client.get("/segment-changes").json() == changes_before

