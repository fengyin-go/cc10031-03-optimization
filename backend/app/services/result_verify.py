"""结果校验领域服务 — 校验采集结果并触发回调。

数据工程核心：数据校验与去重策略执行 — 跨模块数据产物

数据流:
    channel("rpa.data_collected") ← data_extract
    → result_verify 消费校验 + DirtyDataVault（按记录键去重）
    → channel("rpa.validation_complete") → callback_hook

隔离与恢复稳定性约定：
- 只读取尚未验证（待校验）的采集记录，重跑不会重复消费同一失败项；
- 校验失败写入隔离区的同时推进采集记录处理状态（与隔离回执同步）；
- 恢复后的记录键不会再次进入同一隔离批次，不重复累计同一隔离原因；
- 输出的采集质量谱系数据集按记录键去重，且只含脱敏后的校验结果。
"""
from __future__ import annotations

import random
from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


def _safe_float(value: str, default: float = 1.0) -> float:
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def _desensitize_fields(fields: dict[str, Any]) -> dict[str, str]:
    """采集记录脱敏：不输出原始值，只保留字段名与类型/长度指纹。"""
    masked: dict[str, str] = {}
    for name, value in (fields or {}).items():
        text = "" if value is None else str(value)
        masked[name] = f"{type(value).__name__}:{len(text)}"
    return masked


class ResultVerifyService(BaseModuleService):
    MODULE = "result_verify"
    ENTITY = "校验规则"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["规则名称", "校验字段", "校验条件", "期望模式", "异常动作", "抽样比例", "关联流程"]
    REQUIRED_FIELDS = ["规则名称", "校验字段", "校验条件"]
    STATUS_ORDER = ["草稿", "已启用", "已关闭"]
    ACTION_RULES = {"启用规则": "草稿", "关闭规则": "已启用"}
    NEGATIVE_ACTIONS = []

    # ---- 跨模块业务方法 ----

    def verify_collected_data(self) -> dict[str, Any]:
        """消费 data_extract 发布的采集结果，执行校验。

        从 channel("rpa.data_collected") 获取采集结果批次，
        按已启用的校验规则逐条校验「尚未验证」的采集记录。

        不通过的记录移入 DirtyDataVault（按记录键去重），
        采集记录处理状态与隔离回执同步推进为「已隔离」，
        下一轮消费不会再拿到同一失败项。

        Returns:
            {"verified": int, "passed": int, "failed": int,
             "duplicates_skipped": int, "batches": [...],
             "lineage": 去重后的采集质量谱系数据集（脱敏）}
        """
        msgs = store.channel.consume(
            consumer=self.MODULE,
            topic="rpa.data_collected",
        )
        if not msgs:
            return {"verified": 0, "passed": 0, "failed": 0,
                    "duplicates_skipped": 0, "batches": [],
                    "lineage": self.quality_lineage()}

        batches = []
        total_passed = 0
        total_failed = 0
        total_duplicates = 0

        for msg in msgs:
            batch_data = msg.payload
            batch_ref = batch_data.get("batch_ref", "")

            # 获取已启用的校验规则
            verify_rules = self._get_active_rules()
            if not verify_rules:
                store.channel.confirm(msg.msg_id, self.MODULE)
                continue

            # 只读取尚未验证的采集记录：已隔离/已恢复的记录不会重复消费
            records = store.collect_records.list_unvalidated(batch_ref)
            passed, failed, duplicates = 0, 0, 0

            for record_entry in records:
                quality = self._validate_record(
                    record_entry.fields, verify_rules)
                if quality["passed"]:
                    store.collect_records.mark_validated(
                        record_entry.entry_id, quality["score"])
                    passed += 1
                    continue

                failed += 1
                # 写入隔离区（按记录键去重），拿到隔离回执
                receipt = store.dirty_vault.quarantine(
                    source=self.MODULE,
                    batch_ref=batch_ref,
                    raw_data=dict(record_entry.fields),
                    reason=quality["reason"],
                    record_key=record_entry.record_key,
                    collect_entry_id=record_entry.entry_id,
                )
                if receipt.status == "duplicate":
                    duplicates += 1
                if receipt.ok:
                    # 失败记录状态与隔离回执同步推进
                    store.collect_records.mark_quarantined(record_entry.entry_id)
                else:
                    # 记录键已恢复过：不再进入同一隔离批次，状态同步到已恢复
                    store.collect_records.mark_recovered(record_entry.entry_id)

            total_passed += passed
            total_failed += failed
            total_duplicates += duplicates

            # 发布校验完成事件
            store.channel.publish(
                topic="rpa.validation_complete",
                source=self.MODULE,
                payload={
                    "batch_ref": batch_ref,
                    "site": batch_data.get("site"),
                    "passed": passed,
                    "failed": failed,
                    "duplicates_skipped": duplicates,
                    "rules_applied": len(verify_rules),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                },
            )
            store.channel.confirm(msg.msg_id, self.MODULE)

            batches.append({
                "batch_ref": batch_ref,
                "passed": passed,
                "failed": failed,
                "duplicates_skipped": duplicates,
                "total": len(records),
            })

        return {
            "verified": total_passed + total_failed,
            "passed": total_passed,
            "failed": total_failed,
            "duplicates_skipped": total_duplicates,
            "batches": batches,
            "lineage": self.quality_lineage(),
        }

    def quality_lineage(self, batch_ref: str | None = None) -> list[dict[str, Any]]:
        """生成去重后的采集质量谱系数据集（脱敏）。

        解析每条采集记录的有效校验状态，与隔离区当前有效记录对齐：
        同一记录键只保留一条有效隔离原因，原始字段值脱敏后输出。
        """
        vault_rows = {
            row["record_key"]: row for row in store.dirty_vault.lineage()
        }
        entries = store.collect_records.list_by_batch(batch_ref) if batch_ref \
            else store.collect_records.list_all()

        lineage: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for entry in entries:
            key = entry.record_key or f"{entry.batch_ref}#{entry.entry_id}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            vault_row = vault_rows.get(key)
            lineage.append({
                "record_key": key,
                "batch_ref": entry.batch_ref,
                "source": entry.source_module,
                "status": entry.process_state,
                "effective_reason": (
                    vault_row["reason"] if vault_row and vault_row["status"] == "隔离中"
                    else None
                ),
                "quality_score": entry.quality_score,
                "validated": entry.validated,
                "recovered_at": vault_row["recovered_at"] if vault_row else None,
                "fields_masked": _desensitize_fields(entry.fields),
            })

        # 隔离区中存在但采集记录已不在修订范围内的键，也按谱系保留一行
        # 状态词与采集记录处理状态对齐：隔离中→已隔离
        vault_status_map = {"隔离中": "已隔离", "已恢复": "已恢复", "已更正": "已更正"}
        for key, row in vault_rows.items():
            if key in seen_keys:
                continue
            if batch_ref and row["batch_ref"] != batch_ref:
                continue
            lineage.append({
                "record_key": key,
                "batch_ref": row["batch_ref"],
                "source": row["source"],
                "status": vault_status_map.get(row["status"], row["status"]),
                "effective_reason": row["reason"],
                "quality_score": None,
                "validated": row["status"] == "已恢复",
                "recovered_at": row["recovered_at"],
                "fields_masked": {},
            })
        return lineage

    def recover_records(self, record_keys: list[str] | None = None,
                        record_ids: list[int] | None = None,
                        recovered_to: str | None = None) -> dict[str, Any]:
        """批量恢复隔离记录：只更新仍处于当前隔离状态的记录。

        幂等：并发/重复提交只算一次；恢复后采集记录状态同步推进为
        「已恢复」，记录键不会再次进入同一隔离批次。
        """
        target = recovered_to or self.MODULE
        ids: list[int] = list(record_ids or [])
        for key in record_keys or []:
            records = store.dirty_vault.find_by_key(key)
            if not records:
                continue
            # 当前有效记录优先；否则取该键最新一条（由批量恢复回报为已恢复跳过）
            current = next((r for r in records if r.effective), None)
            ids.append((current or records[-1]).record_id)
        ids = sorted(set(ids))

        result = store.dirty_vault.batch_recover(ids, target)

        # 同步推进关联采集记录的处理状态
        synced = 0
        for rid in result["recovered"]:
            rec = store.dirty_vault.get(rid)
            entry_id = rec.collect_entry_id if rec else None
            if entry_id is not None and store.collect_records.mark_recovered(entry_id):
                synced += 1

        return {
            "ok": True,
            "requested": len(ids),
            "recovered": len(result["recovered"]),
            "skipped": result["skipped"],
            "collect_synced": synced,
            "lineage": self.quality_lineage(),
        }

    def correct_record(self, record_key: str,
                       corrected_fields: dict[str, Any],
                       mode: str = "rebuild") -> dict[str, Any]:
        """原始记录更正：与隔离回执同时到达时的两套处理边界。

        - rebuild: 按更正重建当前隔离清单（旧记录转历史，只保留一条有效原因）；
        - recalc_event: 追加重算事件并延迟发布汇总（不新增隔离记录）。
        """
        quality = self._validate_record(corrected_fields, self._get_active_rules())
        new_reason = None if quality["passed"] else quality["reason"]

        if mode == "recalc_event":
            event = store.dirty_vault.append_recalc_event(
                record_key, new_reason or "更正后通过",
                detail="原始记录更正：追加重算事件，延迟发布汇总",
                delay_summary=True,
            )
            return {
                "ok": True, "mode": mode, "record_key": record_key,
                "effective_reason": new_reason, "recalc_event": event,
                "lineage": self.quality_lineage(),
            }

        receipt = store.dirty_vault.rebuild_by_correction(
            record_key, self.MODULE, dict(corrected_fields), new_reason)
        if receipt.status == "cleared":
            # 更正后无有效隔离：采集记录同步推进为已恢复，谱系与隔离区保持一致
            entry = store.collect_records.find_by_key(record_key)
            if entry is not None:
                store.collect_records.mark_recovered(entry.entry_id)
        return {
            "ok": True, "mode": "rebuild", "record_key": record_key,
            "receipt": receipt.status, "effective_reason": new_reason,
            "lineage": self.quality_lineage(),
        }

    def _get_active_rules(self) -> list[dict[str, Any]]:
        """获取已启用的校验规则。"""
        rows = store.rows(self.MODULE)
        return [
            {
                "id": r.get("id"),
                "name": r.get("规则名称"),
                "field": r.get("校验字段"),
                "condition": r.get("校验条件"),
                "pattern": r.get("期望模式", "non_empty"),
                "action_on_fail": r.get("异常动作", "quarantine"),
                "sample_rate": _safe_float(r.get("抽样比例", "1.0")),
                "bot_ref": r.get("关联流程", ""),
            }
            for r in rows if r.get("status") == "已启用"
        ]

    def _validate_record(
        self,
        fields: dict[str, Any],
        rules: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """按规则校验单条采集记录。

        Returns:
            {"passed": bool, "score": float, "reason": str}
        """
        if not fields:
            return {"passed": False, "score": 0.0, "reason": "空记录"}

        applied = 0
        failed = 0
        reasons = []

        for rule in rules:
            if random.random() > rule["sample_rate"]:
                continue  # 抽样跳过

            field_name = rule["field"]
            condition = rule["condition"]

            # 模拟校验逻辑
            if field_name in fields:
                value = fields[field_name]
                applied += 1

                if condition == "non_empty" and not str(value).strip():
                    failed += 1
                    reasons.append(f"字段[{field_name}]为空")
                elif condition == "numeric" and not str(value).replace(".", "").isdigit():
                    failed += 1
                    reasons.append(f"字段[{field_name}]不是数值")
                elif condition == "length_gt_0" and len(str(value)) == 0:
                    failed += 1
                    reasons.append(f"字段[{field_name}]长度为零")
                # 模拟额外场景
                elif random.random() < 0.08:
                    failed += 1
                    reasons.append(f"字段[{field_name}]格式异常")
            else:
                # 字段缺失
                failed += 1
                reasons.append(f"缺少字段[{field_name}]")

        if applied == 0:
            # 无匹配字段视为通过
            return {"passed": True, "score": 1.0, "reason": "无校验规则匹配"}

        passed = failed == 0
        score = 1.0 - (failed / max(applied, 1)) if applied > 0 else 0.0
        return {
            "passed": passed,
            "score": round(score, 2),
            "reason": "; ".join(reasons) if reasons else "通过",
        }

    def run_single_validate(
        self,
        record: dict[str, Any],
        rule_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        """对单条记录执行指定规则校验。

        Args:
            record: 待校验记录
            rule_ids: 要使用的规则 ID 列表（None = 所有已启用）

        Returns:
            校验结果
        """
        all_rules = self._get_active_rules()
        if rule_ids:
            rules = [r for r in all_rules if r["id"] in rule_ids]
        else:
            rules = all_rules

        return self._validate_record(record, rules)

    def get_validation_stats(self) -> dict[str, Any]:
        """获取校验统计概览。"""
        validate_events = store.channel.snapshot(topic="rpa.validation_complete")
        total_passed = sum(
            e.get("payload", {}).get("passed", 0) for e in validate_events)
        total_failed = sum(
            e.get("payload", {}).get("failed", 0) for e in validate_events)
        return {
            "total_validation_events": len(validate_events),
            "total_passed": total_passed,
            "total_failed": total_failed,
            "dirty_vault": store.dirty_vault.snapshot(),
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        stats = self.get_validation_stats()
        lineage = self.quality_lineage()
        return {
            **base, **stats,
            "lineage_total": len(lineage),
            "lineage_quarantined": sum(
                1 for row in lineage if row["status"] == "已隔离"),
        }
