"""事件归段核心逻辑（仅依赖标准库，便于独立测试）。

归段规则：
- 同一 source 下，事件 occurred_at 与某事件段的 [start, end] 相距不超过
  silence 秒（含段外前后各 silence 的扩展窗口）即归入该段；
- 乱序/迟到事件可能同时落在多个段的扩展窗口内，或桥接相邻段，
  此时把这些段合并为一段（链式传导，直到不再扩大）；
- (source, event_id) 唯一，重复提交直接返回原段，不改变任何计数；
- revision 初始为 1，仅当新事件导致段的起止边界或最高级别发生变化时递增，
  仅事件数增加不递增。

变更流（供下游增量同步）：
- 每次创建段、边界或最高级别变化（revision 递增）、多段合并，都在事件写入
  事务内向 segment_changes 追加记录，change_id 单调递增；
- 合并时为每个被删除段写一条指向保留段的 merged 墓碑；
- 仅事件数增加（revision 不变）不写 upsert；event_id 幂等重放不写任何记录。

误报事件撤回（withdraw_event，与写入同级别单事务）：
- 删除指定 (source, event_id) 事件后，按当前静默间隔对原段的剩余事件重新切分；
- 无剩余事件（唯一事件）→ 删除原段并写一条 deleted 墓碑；
- 桥接事件移除导致拆段时，最早子段沿用原段 id（revision 在原值上 +1），
  其余子段新建（revision=1），各写一条 upsert；未拆段时同样 +1 并写 upsert；
- 所有删除/更新/新建与变更流追加在同一事务内，任一步失败由调用方整体回滚。

重分段预演（preview_resegmentation，纯读）：
- 在同一只读事务内读取某 source 全部原始事件，按 (occurred_at, event_id)
  稳定排序后以候选静默间隔重新切分，得到不落库的模拟段与临时编号；
- 依据事件成员关系给出每个现有段到模拟段的 unchanged/split/merged 映射；
- 不写任何表（调用方开启 query_only 双重保险），结果对重复请求确定一致。
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime, timezone

from . import severity as sev


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _row_to_segment(row: sqlite3.Row) -> dict:
    return {
        "id": row["id"],
        "source": row["source"],
        "started_at": _iso(row["start_ts"]),
        "ended_at": _iso(row["end_ts"]),
        "max_severity": row["max_severity"],
        "event_count": row["event_count"],
        "revision": row["revision"],
    }


def get_segment(conn: sqlite3.Connection, segment_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (segment_id,)
    ).fetchone()
    return _row_to_segment(row) if row else None


def list_segments(conn: sqlite3.Connection, source: str | None = None) -> list[dict]:
    if source is None:
        rows = conn.execute("SELECT * FROM segments ORDER BY id").fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM segments WHERE source = ? ORDER BY id", (source,)
        ).fetchall()
    return [_row_to_segment(r) for r in rows]


def _record_change(
    conn: sqlite3.Connection,
    *,
    change_type: str,
    segment_id: int,
    now: float,
    merged_into: int | None = None,
    revision: int | None = None,
    source: str | None = None,
    start_ts: float | None = None,
    end_ts: float | None = None,
    max_severity: str | None = None,
    event_count: int | None = None,
) -> None:
    """在事件写入事务内追加一条变更记录（change_id 由 AUTOINCREMENT 保证单调递增）。"""
    conn.execute(
        """
        INSERT INTO segment_changes
            (type, segment_id, merged_into, revision, source,
             start_ts, end_ts, max_severity, event_count, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            change_type, segment_id, merged_into, revision, source,
            start_ts, end_ts, max_severity, event_count, now,
        ),
    )


def _row_to_change(row: sqlite3.Row) -> dict:
    change = {
        "change_id": row["change_id"],
        "type": row["type"],
        "segment_id": row["segment_id"],
        "merged_into": row["merged_into"],
        "revision": row["revision"],
        "segment": None,
    }
    if row["type"] == "upsert":
        change["segment"] = {
            "id": row["segment_id"],
            "source": row["source"],
            "started_at": _iso(row["start_ts"]),
            "ended_at": _iso(row["end_ts"]),
            "max_severity": row["max_severity"],
            "event_count": row["event_count"],
            "revision": row["revision"],
        }
    return change


def list_segment_changes(
    conn: sqlite3.Connection, *, after: int = 0, limit: int = 100
) -> dict:
    """按 change_id 升序返回变更流的一页：{"changes": [...], "next_after": int}。

    键集分页（change_id > after）：change_id 单调递增，分页期间新写入的变更
    只会获得更大的 change_id、出现在后续页，因此不漏读也不重复。
    next_after 可直接作为下一页的 after；空页时与入参 after 相同，便于轮询。
    """
    rows = conn.execute(
        """
        SELECT * FROM segment_changes
        WHERE change_id > ?
        ORDER BY change_id
        LIMIT ?
        """,
        (after, limit),
    ).fetchall()
    changes = [_row_to_change(r) for r in rows]
    next_after = changes[-1]["change_id"] if changes else after
    return {"changes": changes, "next_after": next_after}


def _find_merge_candidates(
    conn: sqlite3.Connection, source: str, occurred_ts: float, silence: float
) -> list[sqlite3.Row]:
    """找出应与新事件合并的所有段。

    新事件的初始窗口为 [occurred - silence, occurred + silence]；
    每并入一个段，窗口随该段边界继续向外扩展，循环直至收敛，
    从而正确处理一个迟到事件桥接多个段的链式情形。
    """
    lo, hi = occurred_ts - silence, occurred_ts + silence
    merged: dict[int, sqlite3.Row] = {}
    while True:
        rows = conn.execute(
            """
            SELECT * FROM segments
            WHERE source = ? AND start_ts <= ? AND end_ts >= ?
            ORDER BY id
            """,
            (source, hi, lo),
        ).fetchall()
        fresh = {r["id"]: r for r in rows if r["id"] not in merged}
        if not fresh:
            return [merged[k] for k in sorted(merged)]
        merged.update(fresh)
        lo = min([lo] + [r["start_ts"] - silence for r in fresh.values()])
        hi = max([hi] + [r["end_ts"] + silence for r in fresh.values()])


def ingest_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    occurred_ts: float,
    severity: str,
    silence: float,
) -> dict:
    """写入一个事件并返回 {"deduplicated": bool, "segment": {...}}。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化。
    """
    now = time.time()

    dup = conn.execute(
        "SELECT segment_id FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if dup:
        return {"deduplicated": True, "segment": get_segment(conn, dup["segment_id"])}

    candidates = _find_merge_candidates(conn, source, occurred_ts, silence)

    if not candidates:
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, 1, 1, ?, ?)
            """,
            (source, occurred_ts, occurred_ts, severity, now, now),
        )
        segment_id = cur.lastrowid
        _record_change(
            conn,
            change_type="upsert",
            segment_id=segment_id,
            now=now,
            revision=1,
            source=source,
            start_ts=occurred_ts,
            end_ts=occurred_ts,
            max_severity=severity,
            event_count=1,
        )
    else:
        survivor = candidates[0]  # id 最小者作为保留段
        segment_id = survivor["id"]

        start_ts = min([occurred_ts] + [c["start_ts"] for c in candidates])
        end_ts = max([occurred_ts] + [c["end_ts"] for c in candidates])
        max_severity = sev.highest(severity, *[c["max_severity"] for c in candidates])
        event_count = 1 + sum(c["event_count"] for c in candidates)

        changed = (
            start_ts != survivor["start_ts"]
            or end_ts != survivor["end_ts"]
            or max_severity != survivor["max_severity"]
        )
        revision = max(c["revision"] for c in candidates) + (1 if changed else 0)

        others = [c["id"] for c in candidates[1:]]
        if others:
            marks = ",".join("?" * len(others))
            conn.execute(
                f"UPDATE events SET segment_id = ? WHERE segment_id IN ({marks})",
                [segment_id, *others],
            )
            conn.execute(f"DELETE FROM segments WHERE id IN ({marks})", others)
            # 每个被删除段一条 merged 墓碑，指向保留段
            for deleted_id in others:
                _record_change(
                    conn,
                    change_type="merged",
                    segment_id=deleted_id,
                    merged_into=segment_id,
                    now=now,
                )

        conn.execute(
            """
            UPDATE segments
            SET start_ts = ?, end_ts = ?, max_severity = ?,
                event_count = ?, revision = ?, updated_at = ?
            WHERE id = ?
            """,
            (start_ts, end_ts, max_severity, event_count, revision, now, segment_id),
        )
        if changed:
            # 边界或最高级别变化（revision 递增）才写 upsert；
            # 仅事件数增加时不产生变更记录
            _record_change(
                conn,
                change_type="upsert",
                segment_id=segment_id,
                now=now,
                revision=revision,
                source=source,
                start_ts=start_ts,
                end_ts=end_ts,
                max_severity=max_severity,
                event_count=event_count,
            )

    conn.execute(
        """
        INSERT INTO events (segment_id, source, event_id, occurred_at, severity, received_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (segment_id, source, event_id, occurred_ts, severity, now),
    )
    return {"deduplicated": False, "segment": get_segment(conn, segment_id)}


def preview_resegmentation(
    conn: sqlite3.Connection, *, source: str, silence: float
) -> dict:
    """按候选静默间隔对某 source 的全部原始事件重新归段（纯读预演）。

    - 仅执行 SELECT：不写 events / segments / segment_changes；调用方还应使用
      query_only 的只读事务（见 db.read_only_session）双重保证；
    - 重新归段等价于把事件按 (occurred_at, event_id) 稳定排序后，在相邻事件
      间隔大于 silence 处切分——这正是增量归段规则收敛后的最终分段；
    - 排序与临时编号均为确定性行为，相同输入重复请求结果逐字节一致；
    - 映射依据事件成员关系给出：现有段与模拟段一一对应为 unchanged，
      一对多为 split，多对一为 merged。
    """
    # 同一读事务内一次性读取该来源全部原始事件；Python 侧按
    # (occurred_at, event_id) 稳定排序，event_id 作为同刻事件的确定次序
    rows = conn.execute(
        """
        SELECT event_id, occurred_at, severity, segment_id
        FROM events
        WHERE source = ?
        """,
        (source,),
    ).fetchall()
    events = sorted(rows, key=lambda r: (r["occurred_at"], r["event_id"]))

    # ---- 按候选间隔重算模拟段 ----
    simulated: list[dict] = []
    # 事件 event_id -> 所属模拟段 temp_id（event_id 在同一 source 内唯一）
    event_sim_index: dict[str, int] = {}

    for event in events:
        ts = event["occurred_at"]
        if simulated and ts <= simulated[-1]["end_ts"] + silence:
            # 与上一个模拟段相邻（含恰好等于间隔的边界）→ 归入
            seg = simulated[-1]
            seg["end_ts"] = ts
            seg["max_severity"] = sev.highest(seg["max_severity"], event["severity"])
            seg["event_count"] += 1
        else:
            # 与上一段间隔超过 silence → 开新段；temp_id 从 1 起按时间序编号
            simulated.append(
                {
                    "temp_id": len(simulated) + 1,
                    "source": source,
                    "start_ts": ts,
                    "end_ts": ts,
                    "max_severity": event["severity"],
                    "event_count": 1,
                }
            )
        event_sim_index[event["event_id"]] = simulated[-1]["temp_id"]

    # ---- 依据事件成员关系构建现有段 -> 模拟段映射（同一读事务内读取）----
    existing_rows = conn.execute(
        "SELECT id FROM segments WHERE source = ? ORDER BY id", (source,)
    ).fetchall()
    existing_ids = [r["id"] for r in existing_rows]

    # 模拟段 temp_id -> 含多少个不同现有段（用于反向判定 merged）
    sim_to_existing: dict[int, set[int]] = {s["temp_id"]: set() for s in simulated}
    # 现有段 id -> 覆盖到的模拟段 temp_id 有序集合
    existing_to_sims: dict[int, list[int]] = {sid: [] for sid in existing_ids}
    existing_counts: dict[int, int] = {sid: 0 for sid in existing_ids}

    for event in events:
        sid = event["segment_id"]
        temp_id = event_sim_index[event["event_id"]]
        sim_to_existing[temp_id].add(sid)
        if sid in existing_to_sims and (
            not existing_to_sims[sid] or existing_to_sims[sid][-1] != temp_id
        ):
            # 事件已按时间排序，落入的模拟段 temp_id 单调不减，去重追加即可
            existing_to_sims[sid].append(temp_id)
        existing_counts[sid] = existing_counts.get(sid, 0) + 1

    mappings: list[dict] = []
    split_count = merged_count = unchanged_count = 0
    for sid in existing_ids:  # 按现有段 id 升序，保证输出确定
        temp_ids = existing_to_sims.get(sid, [])
        if len(temp_ids) == 1 and len(sim_to_existing[temp_ids[0]]) == 1:
            relation = "unchanged"
            unchanged_count += 1
        elif len(temp_ids) > 1:
            relation = "split"
            split_count += 1
        else:
            relation = "merged"
            merged_count += 1
        mappings.append(
            {
                "segment_id": sid,
                "relation": relation,
                "simulated_temp_ids": temp_ids,
                "event_count": existing_counts.get(sid, 0),
            }
        )

    return {
        "source": source,
        "silence_interval_seconds": silence,
        "event_count": len(events),
        "simulated_segments": [
            {
                "temp_id": s["temp_id"],
                "source": s["source"],
                "started_at": _iso(s["start_ts"]),
                "ended_at": _iso(s["end_ts"]),
                "max_severity": s["max_severity"],
                "event_count": s["event_count"],
            }
            for s in simulated
        ],
        "mappings": mappings,
        "summary": {
            "changed_existing_segment_count": split_count + merged_count,
            "split_existing_segment_count": split_count,
            "merged_existing_segment_count": merged_count,
            "unchanged_existing_segment_count": unchanged_count,
            "existing_segment_count": len(existing_ids),
            "simulated_segment_count": len(simulated),
        },
    }


def withdraw_event(
    conn: sqlite3.Connection,
    *,
    source: str,
    event_id: str,
    silence: float,
) -> dict | None:
    """撤回一个误报事件并按静默间隔重算其原段；事件不存在时返回 None（调用方回 404）。

    调用方需持有事务（建议 BEGIN IMMEDIATE）并保证串行化；本函数的所有删除、
    更新、新建与变更流追加均在该事务内执行，异常向上抛出由调用方整体回滚。

    - 唯一事件移除（原段清空）：删除原段，写一条 deleted 墓碑；
    - 未拆段：原段边界/最高级别重算，沿用原 id，revision 一律 +1，写 upsert；
    - 桥接事件移除导致拆段：剩余事件按时间序切分，最早子段沿用原 id
      （revision 在原值上 +1），其余子段新建（revision=1），各写一条 upsert。
      change_id 顺序：保留段 upsert 在前，新建子段按时间序在后。
    """
    now = time.time()

    event = conn.execute(
        "SELECT segment_id, occurred_at, severity FROM events "
        "WHERE source = ? AND event_id = ?",
        (source, event_id),
    ).fetchone()
    if event is None:
        return None

    old_segment_id = event["segment_id"]
    old = conn.execute(
        "SELECT * FROM segments WHERE id = ?", (old_segment_id,)
    ).fetchone()
    if old is None:  # 理论不可达（外键），仍显式处理
        return None

    # 原段其余事件（排除被撤回事件），按时间序稳定排序后重切分
    remaining = conn.execute(
        """
        SELECT event_id, occurred_at, severity
        FROM events
        WHERE segment_id = ? AND NOT (source = ? AND event_id = ?)
        ORDER BY occurred_at, event_id
        """,
        (old_segment_id, source, event_id),
    ).fetchall()

    if not remaining:
        # 唯一事件移除：删事件 + 删原段，原段写 deleted 墓碑
        conn.execute(
            "DELETE FROM events WHERE source = ? AND event_id = ?",
            (source, event_id),
        )
        conn.execute("DELETE FROM segments WHERE id = ?", (old_segment_id,))
        _record_change(
            conn,
            change_type="deleted",
            segment_id=old_segment_id,
            now=now,
        )
        return {
            "source": source,
            "event_id": event_id,
            "result": "deleted_segment",
            "segments": [],
        }

    # 与 preview 相同的切分：相邻事件间隔 > silence 处切分
    groups: list[list[sqlite3.Row]] = []
    for row in remaining:
        if groups and row["occurred_at"] <= groups[-1][-1]["occurred_at"] + silence:
            groups[-1].append(row)
        else:
            groups.append([row])

    def _group_fields(group: list[sqlite3.Row]) -> tuple[float, float, str, int]:
        start_ts = group[0]["occurred_at"]
        end_ts = group[-1]["occurred_at"]
        max_severity = sev.highest(*[g["severity"] for g in group])
        return start_ts, end_ts, max_severity, len(group)

    # 先删除被撤回事件，再更新/新建段（同事务，任一步失败整体回滚）
    conn.execute(
        "DELETE FROM events WHERE source = ? AND event_id = ?",
        (source, event_id),
    )

    # 最早子段沿用原 id（含未拆段的唯一组），revision 在原值上 +1
    start_ts, end_ts, max_severity, event_count = _group_fields(groups[0])
    revision = old["revision"] + 1  # 撤回后保留段的 revision 必须递增
    conn.execute(
        """
        UPDATE segments
        SET start_ts = ?, end_ts = ?, max_severity = ?,
            event_count = ?, revision = ?, updated_at = ?
        WHERE id = ?
        """,
        (start_ts, end_ts, max_severity, event_count, revision, now,
         old_segment_id),
    )
    _record_change(
        conn,
        change_type="upsert",
        segment_id=old_segment_id,
        now=now,
        revision=revision,
        source=source,
        start_ts=start_ts,
        end_ts=end_ts,
        max_severity=max_severity,
        event_count=event_count,
    )
    result_segments = [get_segment(conn, old_segment_id)]

    # 拆出的其余子段：新建段（revision=1）并把事件归属改到新段；
    # 变更记录在保留段 upsert 之后按时间序追加，change_id 天然有序
    for group in groups[1:]:
        start_ts, end_ts, max_severity, event_count = _group_fields(group)
        cur = conn.execute(
            """
            INSERT INTO segments
                (source, start_ts, end_ts, max_severity, event_count, revision,
                 created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (source, start_ts, end_ts, max_severity, event_count, now, now),
        )
        new_id = cur.lastrowid
        conn.executemany(
            "UPDATE events SET segment_id = ? WHERE source = ? AND event_id = ?",
            [(new_id, source, g["event_id"]) for g in group],
        )
        _record_change(
            conn,
            change_type="upsert",
            segment_id=new_id,
            now=now,
            revision=1,
            source=source,
            start_ts=start_ts,
            end_ts=end_ts,
            max_severity=max_severity,
            event_count=event_count,
        )
        result_segments.append(get_segment(conn, new_id))

    result = "shrunk_segment" if len(groups) == 1 else "split_segment"
    return {
        "source": source,
        "event_id": event_id,
        "result": result,
        "segments": result_segments,
    }
