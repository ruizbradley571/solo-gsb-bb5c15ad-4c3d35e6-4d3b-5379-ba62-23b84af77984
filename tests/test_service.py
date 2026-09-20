"""核心归段逻辑测试：仅依赖标准库，可直接运行：

    python3 -m unittest tests.test_service -v
"""
import os
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.db import init_db, read_only_session, session  # noqa: E402
from app import service  # noqa: E402

SILENCE = 10.0


class ServiceTestCase(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "test.db")
        init_db(self.db_path)

    def tearDown(self):
        self.tmp.cleanup()

    def submit(self, source, event_id, occurred_ts, severity="info"):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=source,
                event_id=event_id,
                occurred_ts=occurred_ts,
                severity=severity,
                silence=SILENCE,
            )

    def withdraw(self, source, event_id):
        with session(self.db_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.withdraw_event(
                conn, source=source, event_id=event_id, silence=SILENCE
            )

    def get(self, segment_id):
        with session(self.db_path) as conn:
            return service.get_segment(conn, segment_id)

    def changes(self, after=0, limit=100):
        with session(self.db_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    def test_first_event_creates_segment(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        self.assertFalse(r["deduplicated"])
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)
        self.assertEqual(seg["max_severity"], "warning")
        self.assertEqual(seg["started_at"], seg["ended_at"])

    def test_duplicate_event_id_is_idempotent(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e1", 1000.0)
        self.assertTrue(r2["deduplicated"])
        seg = self.get(r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 1)

    def test_same_event_id_different_source_not_duplicate(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e1", 1000.0)
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["source"], "s2")

    def test_out_of_order_event_extends_start_and_bumps_revision(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 995.0)  # 更早到达的乱序事件
        self.assertEqual(r2["segment"]["id"], r1["segment"]["id"])
        seg = r2["segment"]
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["revision"], 2)  # 边界变化 → 递增
        self.assertEqual(seg["started_at"], "1970-01-01T00:16:35+00:00")  # 995.0

    def test_inner_event_lower_severity_keeps_revision(self):
        self.submit("s1", "e1", 1000.0, "error")
        r = self.submit("s1", "e2", 1010.0, "info")  # 扩展 end → rev=2
        self.assertEqual(r["segment"]["revision"], 2)
        r = self.submit("s1", "e3", 1005.0, "debug")  # 严格位于段内、级别更低
        self.assertEqual(r["segment"]["event_count"], 3)
        self.assertEqual(r["segment"]["revision"], 2)  # 边界与级别未变
        self.assertEqual(r["segment"]["max_severity"], "error")

    def test_inner_event_higher_severity_bumps_revision(self):
        self.submit("s1", "e1", 1000.0, "info")
        r = self.submit("s1", "e2", 1003.0, "critical")
        self.assertEqual(r["segment"]["revision"], 2)
        self.assertEqual(r["segment"]["max_severity"], "critical")

    def test_event_beyond_silence_creates_new_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE + 0.1)
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_event_at_exact_silence_boundary_joins_segment(self):
        r1 = self.submit("s1", "e1", 1000.0)
        r2 = self.submit("s1", "e2", 1000.0 + SILENCE)
        self.assertEqual(r1["segment"]["id"], r2["segment"]["id"])

    def test_late_event_bridges_two_segments(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A [0, 0]
        r2 = self.submit("s1", "e2", 11.0)         # 11 > 0+10 → 段 B [11, 11]
        self.assertNotEqual(r1["segment"]["id"], r2["segment"]["id"])
        r3 = self.submit("s1", "e3", 5.0)          # 桥接 A 与 B
        seg = r3["segment"]
        self.assertEqual(seg["id"], r1["segment"]["id"])
        self.assertEqual(seg["event_count"], 3)
        self.assertEqual(seg["revision"], 2)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_chain_merge_via_fixpoint(self):
        self.submit("s1", "e1", 0.0)               # 段 A
        self.submit("s1", "e2", 11.0)              # 段 B
        self.submit("s1", "e3", 22.0)              # 22 > 11+10 → 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B → [0, 11]
        r = self.submit("s1", "e5", 16.0)          # 桥接 [0,11] 与 [22,22]
        seg = r["segment"]
        self.assertEqual(seg["event_count"], 5)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
        self.assertEqual(len(segs), 1)
        self.assertTrue(segs[0]["ended_at"].startswith("1970-01-01T00:00:22"))

    def test_sources_are_isolated(self):
        self.submit("s1", "e1", 1000.0)
        r = self.submit("s2", "e2", 1001.0)  # 时间接近但 source 不同
        self.assertEqual(r["segment"]["event_count"], 1)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn)), 2)

    def test_revision_never_decreases_after_merge(self):
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 1.0, "critical")   # 段 A rev=2
        self.submit("s1", "e3", 21.0, "info")      # 21 > 1+10 → 段 B rev=1
        r = self.submit("s1", "e4", 11.0)          # 桥接 A、B
        self.assertGreaterEqual(r["segment"]["revision"], 3)

    # ---- 变更流（segment_changes）----

    def test_create_emits_upsert_change(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        page = self.changes()
        self.assertEqual(len(page["changes"]), 1)
        ch = page["changes"][0]
        self.assertEqual(ch["change_id"], 1)
        self.assertEqual(ch["type"], "upsert")
        self.assertEqual(ch["segment_id"], r["segment"]["id"])
        self.assertEqual(ch["revision"], 1)
        self.assertIsNone(ch["merged_into"])
        self.assertEqual(ch["segment"], r["segment"])  # 快照与段状态一致
        self.assertEqual(page["next_after"], 1)

    def test_boundary_or_severity_change_emits_upsert(self):
        self.submit("s1", "e1", 1000.0, "info")
        self.submit("s1", "e2", 1005.0, "error")   # 扩 end + 提级 → rev 2
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "upsert"])
        second = page["changes"][1]
        self.assertEqual(second["revision"], 2)
        self.assertEqual(second["segment"]["max_severity"], "error")
        self.assertEqual(second["segment"]["event_count"], 2)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertLess(ids[0], ids[1])            # 单调递增

    def test_count_only_increase_emits_no_change(self):
        self.submit("s1", "e1", 1000.0, "error")
        self.submit("s1", "e2", 1010.0)            # 扩边界 → upsert
        before = self.changes()["changes"]
        self.assertEqual(len(before), 2)
        self.submit("s1", "e3", 1005.0, "debug")   # 仅事件数 +1，revision 不变
        self.assertEqual(self.changes()["changes"], before)  # 无新增变更

    def test_duplicate_event_id_emits_no_change(self):
        self.submit("s1", "e1", 1000.0)
        before = self.changes()["changes"]
        r = self.submit("s1", "e1", 1000.0)        # 幂等重放
        self.assertTrue(r["deduplicated"])
        self.assertEqual(self.changes()["changes"], before)

    def test_merge_emits_tombstone_per_deleted_segment(self):
        r1 = self.submit("s1", "e1", 0.0)          # 段 A
        r2 = self.submit("s1", "e2", 11.0)         # 段 B
        r3 = self.submit("s1", "e3", 22.0)         # 段 C
        self.submit("s1", "e4", 5.0)               # 合并 A+B
        self.submit("s1", "e5", 16.0)              # 桥接 [A+B] 与 C → 三段合并
        page = self.changes()
        types = [c["type"] for c in page["changes"]]
        # 3 次创建 + (B 墓碑 + A upsert) + (C 墓碑 + A upsert)
        self.assertEqual(
            types,
            ["upsert", "upsert", "upsert", "merged", "upsert", "merged", "upsert"],
        )
        tombs = [c for c in page["changes"] if c["type"] == "merged"]
        self.assertEqual(
            {t["segment_id"] for t in tombs},
            {r2["segment"]["id"], r3["segment"]["id"]},
        )
        for t in tombs:                            # 每个墓碑都指向保留段 A
            self.assertEqual(t["merged_into"], r1["segment"]["id"])
            self.assertIsNone(t["segment"])
        final = page["changes"][-1]
        self.assertEqual(final["type"], "upsert")
        self.assertEqual(final["segment_id"], r1["segment"]["id"])
        self.assertEqual(final["segment"]["event_count"], 5)
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))         # 全局单调递增
        self.assertEqual(len(set(ids)), len(ids))

    def test_pagination_no_miss_no_dup_while_ingesting(self):
        for i in range(5):                         # 5 个独立段 → 5 条 upsert
            self.submit("s1", f"e{i}", 1000.0 + i * 100.0)

        page1 = self.changes(after=0, limit=2)
        self.assertEqual([c["change_id"] for c in page1["changes"]], [1, 2])
        self.assertEqual(page1["next_after"], 2)

        self.submit("s1", "e5", 2000.0)            # 分页期间又有新事件

        page2 = self.changes(after=page1["next_after"], limit=2)
        page3 = self.changes(after=page2["next_after"], limit=100)
        seen = [c["change_id"] for p in (page1, page2, page3) for c in p["changes"]]
        self.assertEqual(seen, [1, 2, 3, 4, 5, 6])  # 不漏读
        self.assertEqual(len(seen), len(set(seen)))  # 不重复

        empty = self.changes(after=page3["next_after"])  # 已读完：空页
        self.assertEqual(empty["changes"], [])
        self.assertEqual(empty["next_after"], page3["next_after"])  # 游标不动

    def test_empty_changes_next_after_echoes_after(self):
        page = self.changes(after=0)
        self.assertEqual(page, {"changes": [], "next_after": 0})
        page = self.changes(after=42)
        self.assertEqual(page, {"changes": [], "next_after": 42})

    # ---- 静默间隔重分段预演（纯读）----

    def preview(self, source, silence):
        with read_only_session(self.db_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence
            )

    def test_preview_smaller_interval_splits_segments(self):
        # 当前 SILENCE=10：0、8、20、28、40 → 三段 [0,8]、[20,28]、[40]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)
        c = self.submit("s1", "e5", 40.0)["segment"]

        r = self.preview("s1", 5.0)
        # 间隔 5：8→20(12)、28→40(12) 切开；0→8(8) 切开 → 五段
        sims = r["simulated_segments"]
        self.assertEqual([s["temp_id"] for s in sims], [1, 2, 3, 4, 5])
        self.assertEqual(
            [s["event_count"] for s in sims], [1, 1, 1, 1, 1]
        )
        self.assertTrue(all(s["source"] == "s1" for s in sims))

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "split")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1, 2])
        self.assertEqual(by_id[b["id"]]["relation"], "split")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [3, 4])
        self.assertEqual(by_id[c["id"]]["relation"], "unchanged")
        self.assertEqual(by_id[c["id"]]["simulated_temp_ids"], [5])

        summary = r["summary"]
        self.assertEqual(summary["split_existing_segment_count"], 2)
        self.assertEqual(summary["merged_existing_segment_count"], 0)
        self.assertEqual(summary["unchanged_existing_segment_count"], 1)
        self.assertEqual(summary["changed_existing_segment_count"], 2)
        self.assertEqual(summary["existing_segment_count"], 3)
        self.assertEqual(summary["simulated_segment_count"], 5)
        self.assertEqual(r["event_count"], 5)

    def test_preview_larger_interval_merges_segments(self):
        # 0、8、20、28 → 现有两段 [0,8]、[20,28]
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]
        self.submit("s1", "e4", 28.0)

        r = self.preview("s1", 100.0)
        sims = r["simulated_segments"]
        self.assertEqual(len(sims), 1)
        self.assertEqual(sims[0]["temp_id"], 1)
        self.assertEqual(sims[0]["event_count"], 4)
        self.assertEqual(sims[0]["started_at"], "1970-01-01T00:00:00+00:00")
        self.assertEqual(sims[0]["max_severity"], "info")

        by_id = {m["segment_id"]: m for m in r["mappings"]}
        self.assertEqual(by_id[a["id"]]["relation"], "merged")
        self.assertEqual(by_id[a["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(by_id[b["id"]]["relation"], "merged")
        self.assertEqual(by_id[b["id"]]["simulated_temp_ids"], [1])
        self.assertEqual(r["summary"]["merged_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 2)
        self.assertEqual(r["summary"]["split_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 0)

    def test_preview_equal_interval_all_unchanged(self):
        a = self.submit("s1", "e1", 0.0)["segment"]
        self.submit("s1", "e2", 8.0)
        b = self.submit("s1", "e3", 20.0)["segment"]

        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertTrue(all(m["relation"] == "unchanged" for m in r["mappings"]))
        self.assertEqual(
            [m["simulated_temp_ids"] for m in r["mappings"]], [[1], [2]]
        )
        self.assertEqual(r["summary"]["changed_existing_segment_count"], 0)
        self.assertEqual(r["summary"]["unchanged_existing_segment_count"], 2)
        # 模拟段与现有段边界一致
        sims = {s["temp_id"]: s for s in r["simulated_segments"]}
        self.assertEqual(sims[1]["event_count"], 2)
        self.assertEqual(sims[2]["event_count"], 1)

    def test_preview_boundary_gap_equal_to_interval_joins(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", SILENCE)  # 恰好等于间隔 → 同段
        r = self.preview("s1", SILENCE)
        self.assertEqual(len(r["simulated_segments"]), 1)

    def test_preview_max_severity_and_timestamps(self):
        self.submit("s1", "e1", 100.0, "info")
        self.submit("s1", "e2", 103.0, "critical")
        self.submit("s1", "e3", 95.0, "warning")  # 乱序到达
        r = self.preview("s1", 1000.0)
        seg = r["simulated_segments"][0]
        self.assertEqual(seg["started_at"], "1970-01-01T00:01:35+00:00")  # 95
        self.assertEqual(seg["ended_at"], "1970-01-01T00:01:43+00:00")    # 103
        self.assertEqual(seg["max_severity"], "critical")
        self.assertEqual(seg["event_count"], 3)

    def test_preview_stable_sort_tiebreaks_same_timestamp_by_event_id(self):
        self.submit("s1", "b", 0.0)
        self.submit("s1", "a", 0.0)    # 同刻事件
        self.submit("s1", "c", 100.0)  # 超静默 → 第二段
        r = self.preview("s1", 5.0)
        self.assertEqual(
            [s["temp_id"] for s in r["simulated_segments"]], [1, 2]
        )
        # 同刻事件必在同一段；temp_id 编号按 (occurred_at, event_id) 次序确定
        self.assertEqual(r["simulated_segments"][0]["event_count"], 2)

    def test_preview_is_isolated_by_source(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s2", "e2", 0.0)
        self.submit("s2", "e3", 100.0)
        r = self.preview("s2", 5.0)
        self.assertEqual(r["source"], "s2")
        self.assertEqual(r["event_count"], 2)
        self.assertEqual(len(r["simulated_segments"]), 2)
        self.assertEqual(r["summary"]["existing_segment_count"], 2)
        self.assertTrue(all(s["source"] == "s2" for s in r["simulated_segments"]))

    def test_preview_unknown_source_is_empty(self):
        self.submit("s1", "e1", 0.0)
        r = self.preview("nope", 10.0)
        self.assertEqual(r["event_count"], 0)
        self.assertEqual(r["simulated_segments"], [])
        self.assertEqual(r["mappings"], [])
        self.assertEqual(
            r["summary"],
            {
                "changed_existing_segment_count": 0,
                "split_existing_segment_count": 0,
                "merged_existing_segment_count": 0,
                "unchanged_existing_segment_count": 0,
                "existing_segment_count": 0,
                "simulated_segment_count": 0,
            },
        )

    def test_preview_repeated_requests_are_identical(self):
        for i in range(6):
            self.submit("s1", f"e{i}", float(i) * 7)
        r1 = self.preview("s1", 5.0)
        r2 = self.preview("s1", 5.0)
        self.assertEqual(r1, r2)

    def test_preview_does_not_mutate_data(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 8.0)
        self.submit("s1", "e3", 20.0)
        with session(self.db_path) as conn:
            segs_before = service.list_segments(conn)
            changes_before = service.list_segment_changes(conn)
            events_before = conn.execute("SELECT * FROM events ORDER BY id").fetchall()

        for candidate in (0.0, 5.0, 100.0):
            self.preview("s1", candidate)

        with session(self.db_path) as conn:
            segs_after = service.list_segments(conn)
            changes_after = service.list_segment_changes(conn)
            events_after = conn.execute("SELECT * FROM events ORDER BY id").fetchall()
        self.assertEqual(segs_after, segs_before)
        self.assertEqual(changes_after, changes_before)
        self.assertEqual(
            [tuple(e) for e in events_after], [tuple(e) for e in events_before]
        )

    def test_preview_read_only_session_rejects_writes(self):
        from app.db import read_only_session

        self.submit("s1", "e1", 0.0)
        with read_only_session(self.db_path) as conn:
            with self.assertRaises(sqlite3.OperationalError):
                conn.execute(
                    "INSERT INTO segments "
                    "(source, start_ts, end_ts, max_severity, event_count, "
                    " revision, created_at, updated_at) "
                    "VALUES ('x', 0, 0, 'info', 1, 1, 0, 0)"
                )
        # 回滚释放后库仍可正常写入
        self.submit("s1", "e2", 8.0)

    # ---- 误报事件撤回（withdraw_event）----

    def test_withdraw_unknown_event_returns_none(self):
        self.submit("s1", "e1", 0.0)
        self.assertIsNone(self.withdraw("s1", "ghost"))
        # 同 event_id、不同 source 也视为未知
        self.assertIsNone(self.withdraw("other", "e1"))

    def test_withdraw_unknown_event_changes_nothing(self):
        self.submit("s1", "e1", 0.0)
        with session(self.db_path) as conn:
            segs = service.list_segments(conn)
            changes = service.list_segment_changes(conn)
            events = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertIsNone(self.withdraw("s1", "missing"))
        with session(self.db_path) as conn:
            self.assertEqual(service.list_segments(conn), segs)
            self.assertEqual(service.list_segment_changes(conn), changes)
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], events
            )

    def test_withdraw_sole_event_deletes_segment_and_writes_deleted_tombstone(self):
        r = self.submit("s1", "e1", 1000.0, "warning")
        sid = r["segment"]["id"]
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "deleted_segment")
        self.assertEqual(w["segments"], [])
        with session(self.db_path) as conn:
            self.assertIsNone(service.get_segment(conn, sid))
            self.assertEqual(
                conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 0
            )
        page = self.changes()
        self.assertEqual([c["type"] for c in page["changes"]], ["upsert", "deleted"])
        tomb = page["changes"][-1]
        self.assertEqual(tomb["segment_id"], sid)
        self.assertIsNone(tomb["merged_into"])
        self.assertIsNone(tomb["segment"])
        # 删除后再删一次：未知事件，404 语义
        self.assertIsNone(self.withdraw("s1", "e1"))

    def test_withdraw_inner_event_shrinks_segment_without_split(self):
        a = self.submit("s1", "e1", 0.0, "error")
        self.submit("s1", "e2", 5.0, "info")     # 扩 end → rev=2
        self.submit("s1", "e3", 8.0, "info")     # 扩 end → rev=3
        w = self.withdraw("s1", "e2")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(len(w["segments"]), 1)
        seg = w["segments"][0]
        self.assertEqual(seg["id"], a["segment"]["id"])  # 沿用原 id
        self.assertEqual(seg["revision"], 4)             # rev 必须递增
        self.assertEqual(seg["event_count"], 2)
        self.assertEqual(seg["max_severity"], "error")
        self.assertTrue(seg["ended_at"].startswith("1970-01-01T00:00:08"))
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)
            self.assertEqual(
                conn.execute(
                    "SELECT event_id FROM events WHERE segment_id = ? ORDER BY event_id",
                    (a["segment"]["id"],),
                ).fetchall().__len__(),
                2,
            )

    def test_withdraw_recomputes_max_severity(self):
        # 提级事件在段内部：撤回后最高级别必须重算回落
        self.submit("s1", "e1", 0.0, "info")
        self.submit("s1", "e2", 5.0, "critical")  # 提级 → rev=2
        w = self.withdraw("s1", "e2")
        seg = w["segments"][0]
        self.assertEqual(seg["max_severity"], "info")
        self.assertEqual(seg["event_count"], 1)
        self.assertEqual(seg["revision"], 3)

    def test_withdraw_shrinks_when_new_first_event(self):
        # 撤回段首事件后边界前移但未拆段：仍沿用原 id
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 5.0)
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(w["segments"][0]["id"], a["segment"]["id"])
        self.assertTrue(
            w["segments"][0]["started_at"].startswith("1970-01-01T00:00:05")
        )
        self.assertEqual(w["segments"][0]["event_count"], 1)

    def test_withdraw_bridge_event_splits_segment(self):
        a = self.submit("s1", "e1", 0.0, "error")    # 段 A [0,0]
        b = self.submit("s1", "e2", 20.0, "warning")  # 段 B [20,20]
        c = self.submit("s1", "e3", 10.0, "critical")  # 桥接 → A rev=2
        self.assertEqual(c["segment"]["id"], a["segment"]["id"])
        w = self.withdraw("s1", "e3")
        self.assertEqual(w["result"], "split_segment")
        self.assertEqual(len(w["segments"]), 2)
        kept, new = w["segments"]
        # 最早子段沿用原 id，revision 在原值上递增
        self.assertEqual(kept["id"], a["segment"]["id"])
        self.assertEqual(kept["revision"], 3)
        self.assertEqual(kept["event_count"], 1)
        self.assertEqual(kept["max_severity"], "error")
        self.assertTrue(kept["started_at"].startswith("1970-01-01T00:00:00"))
        # 其余子段新建：revision=1，不复用被合并段 B 的 id
        self.assertNotIn(new["id"], (a["segment"]["id"], b["segment"]["id"]))
        self.assertEqual(new["revision"], 1)
        self.assertEqual(new["event_count"], 1)
        self.assertEqual(new["max_severity"], "warning")
        self.assertTrue(new["started_at"].startswith("1970-01-01T00:00:20"))
        with session(self.db_path) as conn:
            segs = service.list_segments(conn, "s1")
            self.assertEqual(len(segs), 2)
            rows = conn.execute(
                "SELECT event_id, segment_id FROM events ORDER BY event_id"
            ).fetchall()
            self.assertEqual(
                {r["event_id"]: r["segment_id"] for r in rows},
                {"e1": kept["id"], "e2": new["id"]},
            )

    def test_withdraw_split_writes_ordered_upserts_only(self):
        a = self.submit("s1", "e1", 0.0)
        b = self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 桥接：B 写 merged 墓碑
        before = self.changes()["changes"]
        w = self.withdraw("s1", "e3")
        new_id = w["segments"][1]["id"]
        page = self.changes()
        added = page["changes"][len(before):]
        # 拆段只追加 upsert（保留段在前、新段在后），不写墓碑
        self.assertEqual([c["type"] for c in added], ["upsert", "upsert"])
        self.assertEqual(added[0]["segment_id"], a["segment"]["id"])
        self.assertEqual(added[0]["revision"], 3)
        self.assertEqual(added[0]["segment"]["event_count"], 1)
        self.assertEqual(added[1]["segment_id"], new_id)
        self.assertEqual(added[1]["revision"], 1)
        self.assertLess(added[0]["change_id"], added[1]["change_id"])
        ids = [c["change_id"] for c in page["changes"]]
        self.assertEqual(ids, sorted(ids))

    def test_withdraw_split_at_silence_boundary_rule(self):
        # 0、10 同段（恰好等于间隔）；撤回中间事件后 0 与 20 间隔 20 > 10 → 拆开
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 10 同时桥接两段
        w = self.withdraw("s1", "e3")
        self.assertEqual(w["result"], "split_segment")
        self.assertEqual(len(w["segments"]), 2)

    def test_withdraw_same_timestamp_events_stay_one_segment(self):
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 0.0)
        w = self.withdraw("s1", "e1")
        self.assertEqual(w["result"], "shrunk_segment")
        self.assertEqual(w["segments"][0]["id"], a["segment"]["id"])
        self.assertEqual(w["segments"][0]["event_count"], 1)
        w2 = self.withdraw("s1", "e2")
        self.assertEqual(w2["result"], "deleted_segment")

    def test_withdraw_does_not_touch_other_sources(self):
        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)
        other = self.submit("s2", "e9", 5.0, "critical")
        before = self.get(other["segment"]["id"])
        self.withdraw("s1", "e3")
        self.assertEqual(self.get(other["segment"]["id"]), before)

    def test_withdraw_then_reingest_bridges_again(self):
        # 撤回 → 拆段后，以相同 id 重新提交应重新桥接，数据自洽
        a = self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)
        self.withdraw("s1", "e3")
        r = self.submit("s1", "e3", 10.0, "critical")
        self.assertFalse(r["deduplicated"])
        self.assertEqual(r["segment"]["id"], a["segment"]["id"])
        self.assertEqual(r["segment"]["event_count"], 3)
        with session(self.db_path) as conn:
            self.assertEqual(len(service.list_segments(conn, "s1")), 1)

    def test_withdraw_revision_increases_even_when_bounds_unchanged(self):
        # 撤回严格位于段内的事件：边界/级别都不变，revision 仍必须 +1 并写 upsert
        self.submit("s1", "e1", 0.0, "critical")
        self.submit("s1", "e2", 5.0, "info")    # rev=2
        self.submit("s1", "e3", 3.0, "warning")  # 段内，rev 不变仍为 2
        w = self.withdraw("s1", "e3")
        seg = w["segments"][0]
        self.assertEqual(seg["revision"], 3)
        self.assertEqual(seg["max_severity"], "critical")
        page = self.changes()
        self.assertEqual(page["changes"][-1]["type"], "upsert")
        self.assertEqual(page["changes"][-1]["revision"], 3)

    def test_withdraw_failure_rolls_back_entire_transaction(self):
        from unittest import mock

        self.submit("s1", "e1", 0.0)
        self.submit("s1", "e2", 20.0)
        self.submit("s1", "e3", 10.0)  # 桥接成一段（撤回应拆段）
        with session(self.db_path) as conn:
            snapshot = {
                "events": [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT segment_id, event_id FROM events ORDER BY id"
                    )
                ],
                "segments": [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id, start_ts, end_ts, max_severity, event_count, "
                        "revision FROM segments ORDER BY id"
                    )
                ],
                "changes": conn.execute(
                    "SELECT COUNT(*) FROM segment_changes"
                ).fetchone()[0],
            }

        real_record = service._record_change
        calls = {"n": 0}

        def flaky(conn, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:  # 保留段 upsert 之后、新段 upsert 时失败
                raise RuntimeError("simulated failure")
            return real_record(conn, **kwargs)

        with mock.patch.object(service, "_record_change", flaky):
            with self.assertRaises(RuntimeError):
                self.withdraw("s1", "e3")

        with session(self.db_path) as conn:
            self.assertEqual(
                [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT segment_id, event_id FROM events ORDER BY id"
                    )
                ],
                snapshot["events"],
            )
            self.assertEqual(
                [
                    tuple(r)
                    for r in conn.execute(
                        "SELECT id, start_ts, end_ts, max_severity, event_count, "
                        "revision FROM segments ORDER BY id"
                    )
                ],
                snapshot["segments"],
            )
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) FROM segment_changes"
                ).fetchone()[0],
                snapshot["changes"],
            )

    # ---- 旧库迁移（segment_changes CHECK 增加 deleted）----

    def test_init_db_migrates_legacy_segment_changes_check(self):
        import sqlite3

        legacy = os.path.join(self.tmp.name, "legacy.db")
        conn = sqlite3.connect(legacy)
        conn.executescript(
            """
            CREATE TABLE segment_changes (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                type TEXT NOT NULL CHECK (type IN ('upsert','merged')),
                segment_id INTEGER NOT NULL, merged_into INTEGER, revision INTEGER,
                source TEXT, start_ts REAL, end_ts REAL, max_severity TEXT,
                event_count INTEGER, created_at REAL NOT NULL);
            """
        )
        conn.execute(
            "INSERT INTO segment_changes "
            "(type, segment_id, revision, created_at) VALUES ('upsert', 7, 1, 1.0)"
        )
        conn.commit()
        conn.close()

        from app.db import init_db as _init

        _init(legacy)  # 迁移
        _init(legacy)  # 再次执行必须幂等

        conn = sqlite3.connect(legacy)
        try:
            sql = conn.execute(
                "SELECT sql FROM sqlite_master "
                "WHERE type='table' AND name='segment_changes'"
            ).fetchone()[0]
            self.assertIn("'deleted'", sql)
            # 历史记录与 change_id 原样保留
            row = conn.execute(
                "SELECT change_id, type, segment_id, revision "
                "FROM segment_changes"
            ).fetchone()
            self.assertEqual(tuple(row), (1, "upsert", 7, 1))
            conn.execute(
                "INSERT INTO segment_changes (type, segment_id, created_at) "
                "VALUES ('deleted', 7, 2.0)"
            )
            with self.assertRaises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO segment_changes (type, segment_id, created_at) "
                    "VALUES ('bogus', 7, 3.0)"
                )
            conn.commit()
        finally:
            conn.close()

        # 迁移后的库可直接完成完整撤回流程
        from app import service as svc

        with session(legacy) as c:
            c.execute(
                "INSERT INTO segments "
                "(id, source, start_ts, end_ts, max_severity, event_count, "
                " revision, created_at, updated_at) "
                "VALUES (7, 's', 0, 0, 'info', 1, 1, 1, 1)"
            )
            c.execute(
                "INSERT INTO events "
                "(segment_id, source, event_id, occurred_at, severity, received_at) "
                "VALUES (7, 's', 'only', 0, 'info', 1)"
            )
            w = svc.withdraw_event(c, source="s", event_id="only", silence=SILENCE)
        self.assertEqual(w["result"], "deleted_segment")


if __name__ == "__main__":
    unittest.main()
