"""内存数据仓库：给每个业务模块准备一份可筛选、可流转的示例数据。

真实项目里这里会换成数据库访问层；当前实现只依赖标准库，保证克隆下来就能起。

跨模块数据流能力：
- DataChannel: 服务间数据发布/消费通道，支持 topic 订阅、快照读、消费确认
- DirtyDataVault: 脏数据隔离区，存储校验不通过/解析失败的脏数据记录；
  按记录键去重，恢复后的记录键不会再次进入同一隔离批次
- FailureRecovery: 失败恢复管理器，记录失败步骤并支持重试
- CollectRecord: 采集记录存储，记录每次采集产出的结构化数据；
  处理状态（待校验/已通过/已隔离/已恢复）与隔离回执同步推进
"""
from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fingerprint_fields(fields: dict[str, Any]) -> str:
    """采集记录内容指纹：用于没有显式记录键时的去重键。"""
    blob = json.dumps(fields, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()[:12]

# ---------------------------------------------------------------------------
# 跨模块数据通道 (DataChannel) — 服务间发布/消费
# ---------------------------------------------------------------------------

@dataclass
class ChannelMessage:
    """数据通道中的单条消息，带 topic、来源、负载和确认状态。"""
    msg_id: int
    topic: str
    source_module: str
    payload: dict[str, Any]
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    consumed_by: list[str] = field(default_factory=list)
    confirmed: bool = False

    def mark_consumed(self, consumer: str) -> None:
        if consumer not in self.consumed_by:
            self.consumed_by.append(consumer)


class DataChannel:
    """跨模块数据通道：支持 topic 发布/订阅，服务间数据流转的桥梁。

    用法::

        store.channel.publish("rpa.bot_created", "bot_definition", bot_data)
        msgs = store.channel.consume("step_editor", "rpa.bot_created")
        store.channel.confirm(msg_id, "step_editor")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._messages: list[ChannelMessage] = []

    @property
    def total_messages(self) -> int:
        with self._lock:
            return len(self._messages)

    def publish(self, topic: str, source: str, payload: dict[str, Any]) -> ChannelMessage:
        with self._lock:
            self._seq += 1
            msg = ChannelMessage(
                msg_id=self._seq, topic=topic,
                source_module=source, payload=payload,
            )
            self._messages.append(msg)
            return msg

    def consume(self, consumer: str, topic: str | None = None,
                since_msg_id: int | None = None) -> list[ChannelMessage]:
        """获取指定 consumer 尚未消费的消息。"""
        with self._lock:
            results: list[ChannelMessage] = []
            for msg in self._messages:
                if topic and msg.topic != topic:
                    continue
                if since_msg_id is not None and msg.msg_id <= since_msg_id:
                    continue
                if consumer not in msg.consumed_by:
                    results.append(msg)
                    msg.mark_consumed(consumer)
            return results

    def confirm(self, msg_id: int, consumer: str) -> bool:
        with self._lock:
            for msg in self._messages:
                if msg.msg_id == msg_id:
                    msg.confirmed = True
                    return True
            return False

    def snapshot(self, topic: str | None = None) -> list[dict[str, Any]]:
        """返回消息快照，供跨模块分析/看板消费。"""
        with self._lock:
            msgs = self._messages if topic is None else [
                m for m in self._messages if m.topic == topic
            ]
            return [{
                "msg_id": m.msg_id, "topic": m.topic,
                "source": m.source_module, "payload": m.payload,
                "created_at": m.created_at, "consumed_by": m.consumed_by,
                "confirmed": m.confirmed,
            } for m in msgs]

    def latest(self, topic: str) -> dict[str, Any] | None:
        with self._lock:
            for msg in reversed(self._messages):
                if msg.topic == topic:
                    return {
                        "msg_id": msg.msg_id, "topic": msg.topic,
                        "source": msg.source_module, "payload": msg.payload,
                        "created_at": msg.created_at, "confirmed": msg.confirmed,
                    }
            return None

    def uncomfirmed_count(self) -> int:
        with self._lock:
            return sum(1 for m in self._messages if not m.confirmed)


# ---------------------------------------------------------------------------
# 脏数据隔离区 (DirtyDataVault)
# ---------------------------------------------------------------------------

@dataclass
class DirtyRecord:
    """脏数据记录：校验失败或解析异常的数据行。"""
    record_id: int
    source_module: str
    batch_ref: str
    raw_data: dict[str, Any]
    reason: str  # 如 "字段缺失", "格式非法", "解析超时"
    record_key: str = ""  # 记录键：同一批次内唯一标识一条采集记录
    collect_entry_id: int | None = None  # 关联的采集记录 entry_id
    detected_at: str = field(default_factory=_utc_now)
    quarantined: bool = True
    recovered_to: str | None = None
    recovered_at: str | None = None  # 恢复时间
    superseded: bool = False  # 被更正重建取代，转为历史隔离

    @property
    def effective(self) -> bool:
        """是否为当前有效隔离（未恢复、未被取代）。"""
        return self.quarantined and not self.superseded

    @property
    def effective_status(self) -> str:
        if self.superseded:
            return "已更正"
        return "隔离中" if self.quarantined else "已恢复"


@dataclass
class QuarantineReceipt:
    """隔离回执：一次隔离写入的结果凭据。

    失败记录的处理状态必须与隔离回执同步推进：
    调用方拿到回执后应立即推进采集记录的处理状态。
    """
    record_key: str
    status: str  # created / duplicate / blocked_recovered / cleared
    record: DirtyRecord | None = None

    @property
    def ok(self) -> bool:
        """回执是否覆盖了该记录键（新建或命中既有有效隔离）。"""
        return self.status in ("created", "duplicate")

    @property
    def created_new(self) -> bool:
        return self.status == "created"


class DirtyDataVault:
    """脏数据隔离区：被校验规则或解析器标记为问题数据后移入此处。

    去重与恢复稳定性约定：
    - 同一记录键在任一时刻只保留一条当前有效隔离原因；
      重复隔离命中既有有效记录，返回 duplicate 回执，不新增记录。
    - 恢复后的记录键登记在册，不会再次进入同一隔离批次（blocked_recovered）。
    - 原始记录更正可走「重建当前隔离清单」或「追加重算事件」两套边界，
      两套边界都只保留一条有效隔离原因。

    用法::

        vault = store.dirty_vault
        receipt = vault.quarantine("data_extract", "BATC-001", row, "缺少必填字段",
                                   record_key="BATC-001#7")
        dirty = vault.list_by_source("data_extract")
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._records: list[DirtyRecord] = []
        self._recovered_keys: set[str] = set()
        self._recalc_events: list[dict[str, Any]] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._records)

    @staticmethod
    def _normalize_key(batch_ref: str, raw_data: dict[str, Any],
                       record_key: str | None) -> str:
        if record_key:
            return record_key
        return f"{batch_ref}#{fingerprint_fields(raw_data)}"

    def _find_effective_locked(self, record_key: str) -> DirtyRecord | None:
        for r in self._records:
            if r.record_key == record_key and r.effective:
                return r
        return None

    def quarantine(self, source: str, batch_ref: str,
                   raw_data: dict[str, Any], reason: str,
                   record_key: str | None = None,
                   collect_entry_id: int | None = None,
                   allow_recovered: bool = False) -> QuarantineReceipt:
        """写入隔离记录（按记录键去重），返回隔离回执。

        - 记录键已恢复：默认拒绝再次进入同一隔离批次（blocked_recovered）；
          仅更正重建等显式处理边界可传 allow_recovered=True 覆盖。
        - 记录键已有当前有效隔离：返回既有记录（duplicate），不重复写入。
        """
        key = self._normalize_key(batch_ref, raw_data, record_key)
        with self._lock:
            if key in self._recovered_keys and not allow_recovered:
                return QuarantineReceipt(record_key=key, status="blocked_recovered")
            existing = self._find_effective_locked(key)
            if existing is not None:
                return QuarantineReceipt(record_key=key, status="duplicate",
                                         record=existing)
            self._recovered_keys.discard(key)
            self._seq += 1
            rec = DirtyRecord(
                record_id=self._seq, source_module=source,
                batch_ref=batch_ref, raw_data=raw_data, reason=reason,
                record_key=key, collect_entry_id=collect_entry_id,
            )
            self._records.append(rec)
            return QuarantineReceipt(record_key=key, status="created", record=rec)

    def list_by_source(self, source: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.source_module == source]

    def list_by_batch(self, batch_ref: str) -> list[DirtyRecord]:
        with self._lock:
            return [r for r in self._records if r.batch_ref == batch_ref]

    def find_by_key(self, record_key: str) -> list[DirtyRecord]:
        """按记录键取全部隔离记录（当前 + 历史）。"""
        with self._lock:
            return [r for r in self._records if r.record_key == record_key]

    def get(self, record_id: int) -> DirtyRecord | None:
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    return r
            return None

    def effective_records(self) -> list[DirtyRecord]:
        """当前有效隔离记录（未恢复、未被更正取代）。"""
        with self._lock:
            return [r for r in self._records if r.effective]

    def mark_recovered(self, record_id: int, recovered_to: str) -> bool:
        """恢复一条隔离记录；幂等 —— 并发重复提交只生效一次。"""
        with self._lock:
            for r in self._records:
                if r.record_id == record_id:
                    if not r.quarantined:
                        return False  # 已恢复过，不重复累计
                    r.quarantined = False
                    r.recovered_to = recovered_to
                    r.recovered_at = _utc_now()
                    if r.record_key:
                        self._recovered_keys.add(r.record_key)
                    return True
            return False

    def batch_recover(self, record_ids: list[int],
                      recovered_to: str) -> dict[str, Any]:
        """批量恢复：只更新仍处于当前隔离状态的记录，重复提交只算一次。"""
        recovered: list[int] = []
        skipped: list[dict[str, Any]] = []
        with self._lock:
            by_id = {r.record_id: r for r in self._records}
            for rid in record_ids:
                rec = by_id.get(rid)
                if rec is None:
                    skipped.append({"record_id": rid, "status": "missing"})
                    continue
                if not rec.quarantined:
                    skipped.append({"record_id": rid, "status": "already_recovered"})
                    continue
                rec.quarantined = False
                rec.recovered_to = recovered_to
                rec.recovered_at = _utc_now()
                if rec.record_key:
                    self._recovered_keys.add(rec.record_key)
                recovered.append(rid)
        return {"recovered": recovered, "skipped": skipped}

    def rebuild_by_correction(self, record_key: str, source: str,
                              corrected_raw: dict[str, Any],
                              new_reason: str | None) -> QuarantineReceipt:
        """原始记录更正到达：按更正重建当前隔离清单。

        旧的当前有效记录转为历史（superseded），更正后仍有问题的
        只新建一条有效隔离记录；更正后无问题则不再保留有效隔离。
        """
        entry_id: int | None = None
        with self._lock:
            for r in self._records:
                if r.record_key == record_key and r.effective:
                    r.superseded = True
                    r.quarantined = False
                    entry_id = entry_id or r.collect_entry_id
        if not new_reason:
            return QuarantineReceipt(record_key=record_key, status="cleared")
        return self.quarantine(source, self._batch_of(record_key), corrected_raw,
                               new_reason, record_key=record_key,
                               collect_entry_id=entry_id, allow_recovered=True)

    def append_recalc_event(self, record_key: str, reason: str,
                            detail: str, delay_summary: bool = True) -> dict[str, Any]:
        """追加重算事件并延迟发布汇总：不新增隔离记录，只留重算轨迹。"""
        event = {
            "record_key": record_key,
            "reason": reason,
            "detail": detail,
            "event_at": _utc_now(),
            "summary_deferred": delay_summary,
        }
        with self._lock:
            self._recalc_events.append(event)
        return event

    def recalc_history(self, record_key: str) -> list[dict[str, Any]]:
        """某记录键的历史重算记录。"""
        with self._lock:
            return [dict(e) for e in self._recalc_events
                    if e["record_key"] == record_key]

    def record_history(self, record_key: str) -> dict[str, Any]:
        """质量工作台按记录键查询：当前隔离原因、恢复时间和历史重算记录。"""
        with self._lock:
            records = [r for r in self._records if r.record_key == record_key]
            current = next((r for r in records if r.effective), None)
            latest_recovered = next(
                (r for r in reversed(records) if r.recovered_at), None)
            events = [dict(e) for e in self._recalc_events
                      if e["record_key"] == record_key]
        return {
            "record_key": record_key,
            "current_status": current.effective_status if current else (
                latest_recovered.effective_status if latest_recovered else "无隔离记录"),
            "current_reason": current.reason if current else None,
            "current_record_id": current.record_id if current else None,
            "recovered_at": latest_recovered.recovered_at if latest_recovered else None,
            "recovered_to": latest_recovered.recovered_to if latest_recovered else None,
            "history": [
                {"record_id": r.record_id, "status": r.effective_status,
                 "reason": r.reason, "detected_at": r.detected_at,
                 "recovered_at": r.recovered_at}
                for r in records
            ],
            "recalc_events": events,
        }

    def _batch_of(self, record_key: str) -> str:
        with self._lock:
            for r in self._records:
                if r.record_key == record_key:
                    return r.batch_ref
        return record_key.split("#", 1)[0]

    def lineage(self) -> list[dict[str, Any]]:
        """采集质量谱系：按记录键去重，每个记录键只保留一条有效隔离原因。"""
        with self._lock:
            records = list(self._records)
        by_key: dict[str, DirtyRecord] = {}
        for r in records:
            current = by_key.get(r.record_key)
            if current is None or (r.effective and not current.effective) or (
                    r.effective == current.effective
                    and r.record_id > current.record_id):
                by_key[r.record_key] = r
        return [
            {
                "record_key": key,
                "record_id": rec.record_id,
                "batch_ref": rec.batch_ref,
                "source": rec.source_module,
                "status": rec.effective_status,
                "reason": rec.reason if rec.effective else None,
                "detected_at": rec.detected_at,
                "recovered_at": rec.recovered_at,
                "recovered_to": rec.recovered_to,
            }
            for key, rec in sorted(by_key.items(),
                                   key=lambda item: item[1].record_id)
        ]

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            quarantined = sum(1 for r in self._records if r.effective)
            historical = len(self._records) - quarantined
            by_reason: dict[str, int] = {}
            by_reason_current: dict[str, int] = {}
            reason_detail: dict[str, dict[str, Any]] = {}
            for r in self._records:
                by_reason[r.reason] = by_reason.get(r.reason, 0) + 1
                detail = reason_detail.setdefault(
                    r.reason, {"reason": r.reason, "current": 0,
                               "historical": 0, "record_keys": []})
                if r.effective:
                    by_reason_current[r.reason] = \
                        by_reason_current.get(r.reason, 0) + 1
                    detail["current"] += 1
                    if r.record_key not in detail["record_keys"]:
                        detail["record_keys"].append(r.record_key)
                else:
                    detail["historical"] += 1
            return {
                "total": len(self._records),
                "quarantined": quarantined,
                "recovered": sum(1 for r in self._records
                                 if not r.quarantined and not r.superseded),
                "historical": historical,
                "by_reason": by_reason,
                "by_reason_current": by_reason_current,
                "reason_details": sorted(
                    reason_detail.values(),
                    key=lambda d: (-d["current"], d["reason"])),
                "recalc_events": len(self._recalc_events),
            }


# ---------------------------------------------------------------------------
# 采集记录 (CollectRecord) — 每次采集产出的结构化数据
# ---------------------------------------------------------------------------

# 采集记录处理状态：待校验 → 已通过 / 已隔离 → 已恢复
COLLECT_STATES = ("待校验", "已通过", "已隔离", "已恢复")


@dataclass
class CollectEntry:
    entry_id: int
    batch_ref: str
    source_module: str
    fields: dict[str, Any]
    collected_at: str = field(default_factory=_utc_now)
    validated: bool = False
    quality_score: float = 1.0
    process_state: str = "待校验"
    record_key: str = ""


class CollectRecordStore:
    """采集记录存储：每次采集流程产出的结构化数据行。

    处理状态与隔离回执同步推进：校验失败写入隔离区的同时，
    记录状态从「待校验」推进到「已隔离」，下一轮消费不会再拿到同一失败项。

    用法::

        rec = store.collect_records.add("BATC-001", "data_extract", {"字段1": "值1", ...})
        store.collect_records.mark_validated(rec.entry_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[CollectEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def add(self, batch_ref: str, source: str,
            fields: dict[str, Any]) -> CollectEntry:
        with self._lock:
            self._seq += 1
            entry = CollectEntry(
                entry_id=self._seq, batch_ref=batch_ref,
                source_module=source, fields=dict(fields),
                record_key=f"{batch_ref}#{self._seq}",
            )
            self._entries.append(entry)
            return entry

    def get(self, entry_id: int) -> CollectEntry | None:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    return e
            return None

    def find_by_key(self, record_key: str) -> CollectEntry | None:
        with self._lock:
            for e in self._entries:
                if e.record_key == record_key:
                    return e
            return None

    def list_by_batch(self, batch_ref: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.batch_ref == batch_ref]

    def list_all(self) -> list[CollectEntry]:
        with self._lock:
            return list(self._entries)

    def list_unvalidated(self, batch_ref: str) -> list[CollectEntry]:
        """读取尚未验证的采集记录（待校验状态），重跑不会重复拿到失败项。"""
        with self._lock:
            return [e for e in self._entries
                    if e.batch_ref == batch_ref and e.process_state == "待校验"]

    def list_by_source(self, source: str) -> list[CollectEntry]:
        with self._lock:
            return [e for e in self._entries if e.source_module == source]

    def mark_validated(self, entry_id: int, score: float = 1.0) -> bool:
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    e.validated = True
                    e.quality_score = score
                    e.process_state = "已通过"
                    return True
            return False

    def mark_quarantined(self, entry_id: int) -> bool:
        """隔离回执确认后同步推进：待校验 → 已隔离。"""
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    if e.process_state == "待校验":
                        e.process_state = "已隔离"
                    return True
            return False

    def mark_recovered(self, entry_id: int, score: float = 1.0) -> bool:
        """恢复后推进：待校验/已隔离 → 已恢复，不再进入同一隔离批次。"""
        with self._lock:
            for e in self._entries:
                if e.entry_id == entry_id:
                    if e.process_state not in ("待校验", "已隔离"):
                        return False
                    e.process_state = "已恢复"
                    e.validated = True
                    e.quality_score = score
                    return True
            return False

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            validated = sum(1 for e in self._entries if e.validated)
            by_source: dict[str, int] = {}
            by_state: dict[str, int] = {s: 0 for s in COLLECT_STATES}
            for e in self._entries:
                by_source[e.source_module] = by_source.get(e.source_module, 0) + 1
                by_state[e.process_state] = by_state.get(e.process_state, 0) + 1
            return {
                "total": len(self._entries),
                "validated": validated,
                "unvalidated": len(self._entries) - validated,
                "by_source": by_source,
                "by_state": by_state,
            }


# ---------------------------------------------------------------------------
# 失败恢复管理 (FailureRecovery)
# ---------------------------------------------------------------------------

@dataclass
class RecoveryEntry:
    recovery_id: int
    module: str
    batch_ref: str
    step: str
    error_detail: str
    attempt: int = 0
    max_attempts: int = 3
    resolved: bool = False
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


class FailureRecovery:
    """失败恢复管理器：记录失败步骤并支持自动重试。

    用法::

        rec = store.recovery.log("step_editor", "BATC-001", "元素定位",
                                  "选择器超时未匹配")
        store.recovery.retry(rec.recovery_id)
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._seq = 0
        self._entries: list[RecoveryEntry] = []

    @property
    def count(self) -> int:
        with self._lock:
            return len(self._entries)

    def log(self, module: str, batch_ref: str, step: str,
            error_detail: str, max_attempts: int = 3) -> RecoveryEntry:
        with self._lock:
            self._seq += 1
            entry = RecoveryEntry(
                recovery_id=self._seq, module=module,
                batch_ref=batch_ref, step=step,
                error_detail=error_detail, max_attempts=max_attempts,
            )
            self._entries.append(entry)
            return entry

    def retry(self, recovery_id: int) -> tuple[bool, str]:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    if e.resolved:
                        return False, "已恢复，无需重试"
                    e.attempt += 1
                    if e.attempt > e.max_attempts:
                        return False, f"已达最大重试次数 {e.max_attempts}"
                    return True, f"重试 {e.attempt}/{e.max_attempts}"
            return False, "恢复记录不存在"

    def resolve(self, recovery_id: int) -> bool:
        with self._lock:
            for e in self._entries:
                if e.recovery_id == recovery_id:
                    e.resolved = True
                    return True
            return False

    def pending_count(self) -> int:
        with self._lock:
            return sum(1 for e in self._entries if not e.resolved)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            pending = sum(1 for e in self._entries if not e.resolved)
            return {
                "total": len(self._entries),
                "resolved": len(self._entries) - pending,
                "pending": pending,
            }


# ---------------------------------------------------------------------------
# 增强版 Store — 整合通道、脏数据、采集记录、恢复
# ---------------------------------------------------------------------------

class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        # 跨模块基础设施
        self.channel = DataChannel()
        self.dirty_vault = DirtyDataVault()
        self.collect_records = CollectRecordStore()
        self.recovery = FailureRecovery()

    # ---- 基础数据操作 ----

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        return {"cards": cards, "modules": modules}


store = Store()