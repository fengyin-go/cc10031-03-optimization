"""质量工作台接口：采集质量谱系查询、隔离恢复、记录更正。

独立于通用资源路由：质量谱系跨模块聚合，不挂在单一业务模块下。
所有路由以 /api/quality 为前缀，须在 /api/{module} 动态路由之前注册。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Query
from pydantic import BaseModel, Field

from app.services.report_dashboard import ReportDashboardService
from app.services.result_verify import ResultVerifyService
from app.store import store

router = APIRouter(prefix="/api/quality", tags=["质量工作台"])


class RecoverPayload(BaseModel):
    record_keys: list[str] = Field(default_factory=list)
    record_ids: list[int] = Field(default_factory=list)
    recovered_to: str | None = None


class CorrectPayload(BaseModel):
    record_key: str
    corrected_fields: dict[str, Any] = Field(default_factory=dict)
    mode: str = "rebuild"  # rebuild / recalc_event


@router.get("/lineage")
def quality_lineage(batch_ref: str | None = Query(default=None)) -> dict[str, Any]:
    """去重后的采集质量谱系数据集（脱敏）。"""
    service = ResultVerifyService()
    rows = service.quality_lineage(batch_ref=batch_ref)
    return {"count": len(rows), "items": rows}


@router.get("/record")
def quality_record(key: str = Query(..., description="记录键")) -> dict[str, Any]:
    """按记录键查看当前隔离原因、恢复时间和历史重算记录。"""
    return store.dirty_vault.record_history(key)


@router.get("/summary")
def quality_summary() -> dict[str, Any]:
    """质量汇总：按恢复状态区分当前问题与历史隔离，附隔离原因明细。"""
    return ReportDashboardService().quality_summary()


@router.post("/recover")
def quality_recover(payload: RecoverPayload) -> dict[str, Any]:
    """批量恢复：只更新仍处于当前隔离状态的记录，并发重复提交只算一次。"""
    service = ResultVerifyService()
    result = service.recover_records(
        record_keys=payload.record_keys,
        record_ids=payload.record_ids,
        recovered_to=payload.recovered_to,
    )
    return result


@router.post("/correct")
def quality_correct(payload: CorrectPayload) -> dict[str, Any]:
    """原始记录更正：rebuild 重建当前隔离清单，或 recalc_event 追加重算事件。"""
    service = ResultVerifyService()
    if payload.mode not in ("rebuild", "recalc_event"):
        return {"ok": False, "message": f"未知更正模式：{payload.mode}"}
    return service.correct_record(
        payload.record_key, payload.corrected_fields, mode=payload.mode)
