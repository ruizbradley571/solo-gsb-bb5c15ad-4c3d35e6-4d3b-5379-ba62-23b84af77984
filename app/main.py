"""FastAPI 应用入口。"""
from __future__ import annotations

import threading
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from .config import Settings
from .db import init_db, read_only_session, session
from .schemas import (
    EventIn,
    IngestResponse,
    ResegmentationPreviewResponse,
    SegmentChangesPage,
    SegmentOut,
    WithdrawResponse,
)
from . import service

# SQLite 单写者：进程内串行化归段写入（含撤回），配合事务保证合并/拆段一致性
_ingest_lock = threading.Lock()


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.database_path)  # 启动即完成数据库初始化（幂等）
        yield

    app = FastAPI(
        title="设备告警事件段聚合服务",
        version="1.1.0",
        lifespan=lifespan,
    )

    @app.get("/healthz")
    def healthz() -> dict:
        return {"status": "ok"}

    @app.post("/events", response_model=IngestResponse)
    def submit_event(event: EventIn) -> dict:
        """提交告警事件；按 source + 静默间隔归段，event_id 幂等去重。"""
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            return service.ingest_event(
                conn,
                source=event.source,
                event_id=event.event_id,
                occurred_ts=event.occurred_at.timestamp(),
                severity=event.severity,
                silence=settings.silence_interval_seconds,
            )

    @app.delete("/events/{source}/{event_id}", response_model=WithdrawResponse)
    def withdraw_event(source: str, event_id: str) -> dict:
        """撤回误报事件：单事务内删除事件并按静默间隔重算其原段。

        - 未知 (source, event_id) 返回 404；
        - 唯一事件移除 → 删除原段并写 deleted 墓碑；
        - 桥接事件移除导致拆段 → 最早子段沿用原 id（revision+1，upsert），
          其余子段新建（revision=1，upsert）；
        - 未拆段 → 保留原 id，边界/最高级别重算，revision+1，upsert；
        - 任一步失败整体回滚，事件、段与变更流均保持撤回前状态。
        """
        with _ingest_lock, session(settings.database_path) as conn:
            conn.execute("BEGIN IMMEDIATE")
            result = service.withdraw_event(
                conn,
                source=source,
                event_id=event_id,
                silence=settings.silence_interval_seconds,
            )
        if result is None:
            raise HTTPException(status_code=404, detail="event not found")
        return result

    @app.get(
        "/segments/resegmentation-preview",
        response_model=ResegmentationPreviewResponse,
    )
    def preview_resegmentation(
        source: str = Query(min_length=1, max_length=256),
        silence_interval_seconds: float = Query(ge=0),
    ) -> dict:
        """静默间隔重分段预演：用候选间隔在只读事务内重算该 source 的分段。

        全程纯读（query_only 连接 + 无任何写 SQL），不改写事件、现有段或
        变更流；返回模拟段（带临时编号）、每个现有段到模拟段的
        unchanged/split/merged 映射及受影响段汇总。
        """
        with read_only_session(settings.database_path) as conn:
            return service.preview_resegmentation(
                conn, source=source, silence=silence_interval_seconds
            )

    @app.get("/segments/{segment_id}", response_model=SegmentOut)
    def get_segment(segment_id: int) -> dict:
        """查询事件段详情：起止时间、最高级别、事件数、revision。"""
        with session(settings.database_path) as conn:
            segment = service.get_segment(conn, segment_id)
        if segment is None:
            raise HTTPException(status_code=404, detail="segment not found")
        return segment

    @app.get("/segments", response_model=list[SegmentOut])
    def list_segments(source: str | None = Query(default=None)) -> list[dict]:
        """列出事件段，可按 source 过滤。"""
        with session(settings.database_path) as conn:
            return service.list_segments(conn, source)

    @app.get("/segment-changes", response_model=SegmentChangesPage)
    def list_segment_changes(
        after: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
    ) -> dict:
        """增量同步变更流：按 change_id 升序返回 upsert/merged 记录。

        用上一页的 next_after 作为下一页的 after；change_id 单调递增，
        分页期间新写入的变更不会导致漏读或重复。
        """
        with session(settings.database_path) as conn:
            return service.list_segment_changes(conn, after=after, limit=limit)

    return app


app = create_app(Settings.from_env())
