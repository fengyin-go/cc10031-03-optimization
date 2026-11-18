"""运行概览领域服务 — 聚合展示 RPA 平台运行统计，输出 domain_snapshot。

数据工程核心：数据标准化与字段映射转换 — 跨模块数据产物
"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ReportDashboardService(BaseModuleService):
    MODULE = "report_dashboard"
    ENTITY = "运行数据"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["统计日期", "总执行数", "成功数", "失败数", "平均耗时", "采集总量", "可用率"]
    REQUIRED_FIELDS = ["统计日期", "总执行数", "成功数"]
    STATUS_ORDER = ["正常", "下降", "异常"]
    ACTION_RULES = {"刷新概览": "正常"}
    NEGATIVE_ACTIONS = []

    def aggregate_platform_stats(self) -> dict[str, Any]:
        """跨模块聚合平台运行统计。

        从多个模块的数据汇总生成运行概览指标：
        - 采集记录统计（collect_records）
        - 任务队列状态（task_queue）
        - 异常日志（anomaly_log）
        - 脏数据统计（dirty_vault）
        - 通道消息流量（channel）
        """
        collect = store.collect_records.snapshot()
        dirty = store.dirty_vault.snapshot()
        recovery = store.recovery.snapshot()

        # 任务队列状态
        task_rows = store.rows("task_queue")
        task_completed = sum(1 for r in task_rows if r.get("status") == "已完成")
        task_failed = sum(1 for r in task_rows if r.get("status") == "执行失败")

        # 执行日志
        exec_rows = store.rows("execution_log")
        exec_success = sum(1 for r in exec_rows if r.get("status") == "执行成功")

        # 通道流量
        total_msgs = store.channel.total_messages
        unconfirmed = store.channel.uncomfirmed_count()

        return {
            "collect_records": collect["total"],
            "validated_records": collect["validated"],
            "dirty_records": dirty["total"],
            "recovery_pending": recovery["pending"],
            "tasks_completed": task_completed,
            "tasks_failed": task_failed,
            "exec_success": exec_success,
            "channel_messages": total_msgs,
            "channel_unconfirmed": unconfirmed,
            "platform_health": "正常" if task_failed < task_completed / 2 else "下降",
        }

    def quality_summary(self) -> dict[str, Any]:
        """质量汇总：核对采集质量谱系数据集。

        按恢复状态区分当前问题与历史隔离：
        - 当前问题：隔离区仍有效的记录（隔离中）；
        - 历史隔离：已恢复 / 已被更正取代的记录；
        并保留一份隔离原因明细，核对谱系与隔离区计数一致。
        """
        from app.services.result_verify import ResultVerifyService

        lineage = ResultVerifyService().quality_lineage()
        vault = store.dirty_vault.snapshot()

        current = [row for row in lineage if row["status"] == "已隔离"]
        historical = [row for row in lineage if row["status"] in ("已恢复", "已更正")]
        reason_details = vault["reason_details"]

        # 核对：谱系当前问题数必须等于隔离区当前有效数（每键一条有效原因）
        reconciled = len(current) == vault["quarantined"]

        return {
            "lineage_total": len(lineage),
            "current_issues": len(current),
            "historical_quarantine": len(historical),
            "recovered": sum(1 for row in lineage if row["status"] == "已恢复"),
            "corrected": sum(1 for row in lineage if row["status"] == "已更正"),
            "reason_details": reason_details,
            "vault_quarantined": vault["quarantined"],
            "vault_recovered": vault["recovered"],
            "reconciled": reconciled,
        }

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        agg = self.aggregate_platform_stats()
        return {**base, **agg, "quality_summary": self.quality_summary()}